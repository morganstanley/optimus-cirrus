/*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package optimus.buildtool.cache.silverking

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.UUID
import com.ms.silverking.cloud.dht.ForwardingMode
import com.ms.silverking.cloud.dht.RetrievalType
import com.ms.silverking.cloud.dht.client.AsyncKeyedOperation
import com.ms.silverking.cloud.dht.client.AsyncOperation
import com.ms.silverking.cloud.dht.client.AsyncOperationListener
import com.ms.silverking.cloud.dht.client.DHTClient
import com.ms.silverking.cloud.dht.client.DHTSession
import com.ms.silverking.cloud.dht.client.FailureCause
import com.ms.silverking.cloud.dht.client.serialization.SerializationRegistry
import com.ms.silverking.cloud.dht.common.OpResult
import optimus.buildtool.files.Asset
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.dal.silverking.SkUtils
import optimus.dal.silverking.client.SkClientKerberosUtils
import optimus.graph.Node
import optimus.graph.NodePromise
import optimus.graph.NodeTaskInfo
import optimus.platform.annotations.{createNodeTrait, nodeSync}
import optimus.platform.util.Log
import optimus.platform._
import optimus.scalacompat.collection._
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.crumbs.Properties.obtCategory
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.buildtool.artifacts.CachedArtifactType
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.RelativePath
import optimus.buildtool.trace.ObtCrumbSource
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.SilverkingOperation
import optimus.buildtool.trace.TaskTrace
import optimus.core.ChainedNodeID
import optimus.dal.silverking.client.TraceableAsyncNamespacePerspective
import optimus.dal.silverking.client.TraceableSkConverters
import optimus.dsi.trace.TraceId
import optimus.graph.NodeAwaiter
import optimus.graph.NodeTask

import scala.jdk.CollectionConverters._
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.TimeoutException
import scala.reflect._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object SilverKingOperations {
  import SilverKingStore._

  private[cache] sealed trait RequestInfo {
    def summary(uuid: UUID, n: Namespace, requestType: String): String

    protected def str(relativePath: Option[RelativePath]): String =
      relativePath.map(p => s"${p.pathString}:").getOrElse("")
  }
  private[cache] object RequestInfo {
    def apply(key: ArtifactKey): RequestInfo =
      SimpleRequestInfo(key.id, key.tpe, None)
    def apply(key: ArtifactKey, relativePath: Option[RelativePath]): RequestInfo =
      SimpleRequestInfo(key.id, key.tpe, relativePath)

    def apply(keys: Iterable[ArtifactKey]): RequestInfo = {
      keys.map(k => (k.id, k.tpe)).toSeq.distinct match {
        case Seq((id, tpe)) => SimpleRequestInfo(id, tpe, None)
        case idTpes         => CompoundRequestInfo(idTpes, None)
      }
    }

    def merge(infos: Seq[RequestInfo]): RequestInfo = infos match {
      case Seq(i) => i
      case is =>
        val idTpes = is.flatMap {
          case SimpleRequestInfo(id, tpe, _)  => Seq((id, tpe))
          case CompoundRequestInfo(idTpes, _) => idTpes
        }
        val suffix = is
          .flatMap {
            case SimpleRequestInfo(_, _, suffix) => suffix
            case CompoundRequestInfo(_, suffix)  => suffix
          }
          .distinct
          .singleOption
        CompoundRequestInfo(idTpes, suffix)
    }
  }

  private final case class SimpleRequestInfo private (
      id: ScopeId,
      tpe: CachedArtifactType,
      relativePath: Option[RelativePath]
  ) extends RequestInfo {
    override def summary(uuid: UUID, n: Namespace, requestType: String): String =
      s"[$id:$tpe:$requestType:$n:${str(relativePath)}$uuid]"
  }

  private final case class CompoundRequestInfo private (
      idTpes: Seq[(ScopeId, CachedArtifactType)],
      relativePath: Option[RelativePath]
  ) extends RequestInfo {
    override def summary(uuid: UUID, n: Namespace, requestType: String): String =
      s"[($idTpesStr):$requestType:$n:${str(relativePath)}$uuid]"

    private def idTpesStr: String =
      idTpes.map { case (id, tpe) => s"($id:$tpe)" }.mkString(",")
  }

  sealed trait Namespace {
    type Key <: StoredKey
    type Value
  }
  object Namespace {
    case object Artifact extends Namespace {
      type Key = ArtifactKey
      type Value = AssetContent
    }
    case object Directory extends Namespace {
      type Key = DirectoryFileKey
      type Value = DirectoryFile
    }
    case object File extends Namespace {
      type Key = FileSegmentKey
      type Value = ByteBuffer
    }
  }

  val DefaultTimeoutMillis = 60000

}

// Deliberately not an entity, since we want to avoid caching large byte-arrays as arguments or return values of @nodes.
private[cache] trait SilverKingOperations extends Log {
  import SilverKingOperations._
  import SilverKingStore._

  protected def maxDataSize: Int

  @async private[cache] def getArtifact(key: ArtifactKey): Option[AssetContent] =
    read(RequestInfo(key), Namespace.Artifact)(key)
  @async private[cache] def getValidKeys(keys: Iterable[ArtifactKey]): Set[ArtifactKey] =
    validKeys(RequestInfo(keys), Namespace.Artifact)(keys)

  @async private[cache] def getContentData(
      key: ArtifactKey,
      content: AssetContent,
      asset: Asset
  ): Option[Seq[(FileAsset, ByteBuffer)]] = {
    content match {
      case fc @ FileContent(_) =>
        val fileAsset = asset.asInstanceOf[FileAsset]
        getFileContentData(key, fc, None).map(bytes => Seq((fileAsset, bytes)))
      case DirectoryContent(numFiles) =>
        log.debug(s"Loading $numFiles files for key: $key")
        val rootDir = asset.asInstanceOf[Directory]
        val keys = (0 until numFiles).map { fileID =>
          DirectoryFileKey(key, fileID)
        }
        val fileMap = readAll(RequestInfo(key), Namespace.Directory)(keys)
        val files = keys.flatMap(fileMap(_))

        if (keys.size == files.size) {
          val fileBytes = files.apar.flatMap { f =>
            val fileAsset = rootDir.resolveFile(f.relativePath)
            getFileContentData(
              key,
              f.fileContent,
              Some(f.relativePath)
            ).map((fileAsset, _))
          }
          if (keys.size == fileBytes.size) Some(fileBytes)
          else None // No need to log here, since we'll already have logged for the individual files
        } else {
          log.debug(s"Missing ${keys.size - files.size} file(s) for key: $key")
          None
        }
    }
  }

  @async private def getFileContentData(
      key: ArtifactKey,
      content: FileContent,
      relativePath: Option[RelativePath]
  ): Option[ByteBuffer] = {
    val numSegments = content.segments
    val msgSuffix = relativePath.map(p => s" [${p.pathString}]").getOrElse("")
    log.debug(s"Loading $numSegments segments for key: $key$msgSuffix")
    val keys = (0 until numSegments).map { segmentID =>
      FileSegmentKey(key, relativePath, segmentID)
    }
    val segmentMap = readAll(RequestInfo(key, relativePath), Namespace.File)(keys)
    val segments = keys.flatMap(segmentMap(_))

    if (keys.size == segments.size) {
      segments match {
        case Seq(segment) =>
          // avoid copying into a buffer if we've just got a single segment. we know segment won't be reused,
          // so it's safe to skip the deep copy.
          Some(segment)
        case s =>
          val buffer = ByteBuffer.allocateDirect(segments.map(_.remaining).sum)
          s.foreach(buffer.put)
          buffer.flip()
          Some(buffer)
      }
    } else {
      // This can happen if some of the segments for the file have been evicted from SK due to eviction policy
      log.debug(s"Missing ${keys.size - segments.size} segment(s) for key: $key$msgSuffix")
      None
    }
  }

  @async private[cache] def putArtifact(key: ArtifactKey, content: AssetContent): Unit =
    write(RequestInfo(key), Namespace.Artifact)(key, content)

  @async private[cache] def putContentData(key: ArtifactKey, path: Path): (AssetContent, Long) = {
    if (Files.isRegularFile(path)) {
      putFileContentData(key, FileAsset(path), None)
    } else if (Files.isDirectory(path)) {
      val uuid = UUID.randomUUID()
      val dir = Directory(path)
      // SilverKing puts are only expected to be used for RT artifacts, so we're safe to call findFilesUnsafe here
      val files = Directory.findFilesUnsafe(dir)
      log.debug(s"Storing ${files.size} files for key: $key, uuid: $uuid")
      val (keyedFiles, fileSizes) = files.zipWithIndex.apar.map { case (f, i) =>
        val relativeFilePath = dir.relativize(f)
        val (content, size) = putFileContentData(key, f, Some(relativeFilePath))
        (DirectoryFileKey(key, i) -> DirectoryFile(relativeFilePath, content), size)
      }.unzip
      writeAll(RequestInfo(key), Namespace.Directory)(keyedFiles)
      (DirectoryContent(files.size), fileSizes.sum)
    } else {
      throw new IllegalArgumentException(s"$path is not a valid file or directory")
    }
  }

  @async private def putFileContentData(
      key: ArtifactKey,
      file: FileAsset,
      relativePath: Option[RelativePath]
  ): (FileContent, Long) = {
    val msgSuffix = relativePath.map(p => s" [${p.pathString}]").getOrElse("")
    // TODO (OPTIMUS-27148): We're always writing the byte array as a separate
    //  reference; should we ever inline the data in FileContent (which would require finding an efficient JSON
    //  representation)?
    val channel = FileChannel.open(file.path)
    val fileSize = channel.size
    // Assume we're not going to be dealing with multi-petabyte files, so `toInt` is safe
    // SK currently doesn't support serializing direct byte buffers so these need to be heap-allocated for now
    val fullSegments = Seq.fill((fileSize / maxDataSize).toInt)(ByteBuffer.allocate(maxDataSize))
    val partialSegmentSize = (fileSize % maxDataSize).toInt
    val segments =
      if (partialSegmentSize > 0) fullSegments :+ ByteBuffer.allocate(partialSegmentSize) else fullSegments
    try {
      segments.foreach { s =>
        channel.read(s)
        s.flip()
      }
    } finally channel.close()
    log.debug(s"Storing ${segments.size} segments for key: $key$msgSuffix")
    val keyedSegments = segments.zipWithIndex.map { case (segment, i) =>
      FileSegmentKey(key, relativePath, i) -> segment
    }
    writeAll(RequestInfo(key, relativePath), Namespace.File)(keyedSegments)
    (FileContent(keyedSegments.size), fileSize)
  }

  @async private[cache] def invalidateArtifact(key: ArtifactKey): Unit = {
    invalidate(RequestInfo(key), Namespace.Artifact)(key)
  }

  @async protected def read(r: RequestInfo, n: Namespace)(key: n.Key): Option[n.Value]

  @async protected def readAll(r: RequestInfo, n: Namespace)(keys: Seq[n.Key]): Map[n.Key, Option[n.Value]] =
    keys.apar.map { k =>
      k -> read(r, n)(k)
    }.toMap

  @async protected def validKeys(r: RequestInfo, n: Namespace)(
      keys: Iterable[n.Key]
  ): Set[n.Key]

  @async protected def write(r: RequestInfo, n: Namespace)(key: n.Key, value: n.Value): Unit

  @async protected def writeAll(r: RequestInfo, n: Namespace)(entries: Seq[(n.Key, n.Value)]): Unit =
    entries.apar.foreach { case (k, v) =>
      write(r, n)(k, v)
    }

  @async protected def invalidate(r: RequestInfo, n: Namespace)(key: n.Key): Unit

  def close(): Unit = ()
}

private[cache] object SilverKingOperationsImpl extends Log {
  private val awaitTaskInfo = new NodeTaskInfo(s"SilverKingCache.await", NodeTaskInfo.DONT_CACHE)

  object Config {
    private val prefix = SilverKingStoreConfig.prefix

    // defaults to `false`
    val debug: Boolean = sys.props.get(s"$prefix.debug").exists(_.toBoolean)
    // defaults to `false`
    val batchRequests: Boolean = sys.props.get(s"$prefix.batchRequests").exists(_.toBoolean)
    val maxAutoBatchSize: Int = sys.props.get(s"$prefix.maxAutoBatchSize").map(_.toInt).getOrElse(100)
    val advancedRequestBatching: Boolean = sys.props.get(s"$prefix.advancedRequestBatching").exists(_.toBoolean)

    private val Gig = 1024L * 1024 * 1024
    // Note: Prefix for versioned namespace is deliberately lowercase - this is only used by the legacy root locator
    val artifactNamespace: String =
      sys.props.getOrElse(s"$prefix.namespace", "OBT")
    val artifactCapacityBytes: Long =
      sys.props.get(s"$prefix.artifactCapacityBytes").map(_.toLong).getOrElse(10L * Gig) // 10GB
    val fileNamespace: String =
      sys.props.getOrElse(s"$prefix.fileNamespace", s"$artifactNamespace-file")
    val fileCapacityBytes: Long =
      sys.props
        .get(s"$prefix.fileCapacityBytes")
        .map(_.toLong)
        .getOrElse((1.2 * 1024 * Gig).toLong) // 1.2TB
    val directoryNamespace: String =
      sys.props.getOrElse(s"$prefix.directoryNamespace", s"$artifactNamespace-directory")
    val directoryCapacityBytes: Long =
      sys.props.get(s"$prefix.directoryCapacityBytes").map(_.toLong).getOrElse(10L * Gig) // 10GB

  }

  private val registry = {
    import ArtifactJsonProtocol._
    import SilverKingStore._

    val r = SerializationRegistry.createDefaultRegistry
    r.addSerDes(classOf[ArtifactKey], new JsonSerDes[ArtifactKey])
    r.addSerDes(classOf[AssetContent], new JsonSerDes[AssetContent](AssetContent.empty))
    r.addSerDes(classOf[FileSegmentKey], new JsonSerDes[FileSegmentKey])
    r.addSerDes(classOf[ByteBuffer], new DirectByteBufferSerDes)
    r.addSerDes(classOf[DirectoryFileKey], new JsonSerDes[DirectoryFileKey])
    r.addSerDes(classOf[DirectoryFile], new JsonSerDes[DirectoryFile])
    r
  }

  SilverKingBatching.setup()

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  private[cache] def apply(
      config: SilverKingConfig,
      clusterType: ClusterType,
      debug: Boolean = Config.debug,
      forward: Boolean = true
  ): SilverKingOperationsImpl = {
    // Timeouts on SK connection are notoriously fickle, so protect ourselves here
    // by only ever waiting a set maximum period of time
    try Await.result(create(config, clusterType, debug, forward), SilverKingOperations.DefaultTimeoutMillis millis)
    catch {
      case _: TimeoutException =>
        throw new TimeoutException(
          s"SilverKing $clusterType connection timed out after ${SilverKingOperations.DefaultTimeoutMillis} millis"
        )
    }
  }

  private[this] def create(
      config: SilverKingConfig,
      clusterType: ClusterType,
      debug: Boolean,
      forward: Boolean
  ): Future[SilverKingOperationsImpl] = {
    if (SkClientKerberosUtils.enabled != config.kerberized) {
      SkClientKerberosUtils.setEnabled(config.kerberized)
    }

    val dhtClient = new DHTClient(registry)

    val artifactSession = newSession(config, dhtClient, clusterType, "artifact")
    val pathSession = newSession(config, dhtClient, clusterType, "path")

    for {
      as <- artifactSession
      ps <- pathSession
    } yield {
      val ops = new SilverKingOperationsImpl(config, clusterType, debug, forward, as, ps)

      val props = Map(
        "cluster" -> clusterType.toString,
        "artifactSession" -> as.getServer.toString,
        "pathSession" -> ps.getServer.toString
      )
      Breadcrumbs.info(
        ChainedNodeID.nodeID,
        PropertiesCrumb(
          _,
          ObtCrumbSource,
          props,
          obtCategory -> "SilverKingConnection"
        )
      )
      ops
    }
  }

  private[this] def newSession(
      config: SilverKingConfig,
      dhtClient: DHTClient,
      clusterType: ClusterType,
      tpe: String
  ): Future[DHTSession] = Future {
    log.debug(s"Establishing $clusterType $tpe session")
    val s = config.newSession(dhtClient)
    log.debug(s"$clusterType $tpe session established with server ${s.getServer.toString}")
    s
  }
}

private[cache] class SilverKingOperationsImpl private (
    config: SilverKingConfig,
    clusterType: ClusterType,
    debug: Boolean,
    forward: Boolean,
    artifactSession: DHTSession,
    pathSession: DHTSession
) extends SilverKingOperations {
  import SilverKingOperations._
  import SilverKingStore._
  import SilverKingOperationsImpl.Config._

  protected val maxDataSize: Int = 5 * 1024 * 1024

  type Perspective[K, V] = TraceableAsyncNamespacePerspective[K, V]

  // ensure dev cluster is created at half-size
  private val capacityDivisor = if (clusterType == ClusterType.Dev) 2 else 1

  private[cache] val artifactPerspective: Perspective[ArtifactKey, AssetContent] = {
    log.debug(s"Using $clusterType artifact namespace '$artifactNamespace'")
    perspective(artifactSession, artifactNamespace, artifactCapacityBytes / capacityDivisor)
  }

  private val filePerspective: Perspective[FileSegmentKey, ByteBuffer] =
    perspective(pathSession, fileNamespace, fileCapacityBytes / capacityDivisor)

  private val directoryPerspective: Perspective[DirectoryFileKey, DirectoryFile] =
    perspective(pathSession, directoryNamespace, directoryCapacityBytes / capacityDivisor)

  private def perspective[K: ClassTag, V: ClassTag](
      session: DHTSession,
      name: String,
      capacityBytes: Long
  ): Perspective[K, V] = {
    if (!namespaceExists(session, name))
      SkUtils.createMutableLooseNamespaceLrw(session, name, checkExistence = false, capacityBytes)
    val rawPerspective = session.openAsyncNamespacePerspective(name, classTag[K].runtimeClass, classTag[V].runtimeClass)
    if (!TraceableSkConverters.serverSupportsTrace(rawPerspective))
      throw new IllegalStateException(s"Trace unsupported for $rawPerspective")
    TraceableSkConverters.asTraceable(rawPerspective).asInstanceOf[Perspective[K, V]]
  }

  private def namespaceExists(session: DHTSession, namespace: String): Boolean =
    SkUtils.namespaceExists(session, namespace)

  @async override protected def read(r: RequestInfo, n: Namespace)(key: n.Key): Option[n.Value] =
    readUntyped(r, n)(key).asInstanceOf[Option[n.Value]]

  // Remove namespace-based typing here to allow for node batching
  @createNodeTrait @async private def readUntyped(r: RequestInfo, n: Namespace)(key: StoredKey): Option[Any] = {
    val typedKey = key.asInstanceOf[n.Key]
    val p = perspective(n)
    val traceId = newTraceId(n, "read", r)(typedKey)
    if (debug || !forward) {
      var options = p.asVanilla.getOptions.getDefaultGetOptions
      if (debug) options = options.retrievalType(RetrievalType.VALUE_AND_META_DATA)
      if (!forward) options = options.forwardingMode(ForwardingMode.DO_NOT_FORWARD)
      val op = p.getWithTrace(typedKey, options, traceId)
      await(traceId, op) {
        Option(op.getStoredValue).map { sv =>
          log.info(s"Creation time for ${str(traceId)}: ${Instant.ofEpochMilli(sv.getCreationTime.inMillis)}")
          sv.getValue
        }
      }
    } else {
      val op = p.getWithTrace(typedKey, traceId)
      await(traceId, op)(Option(op.getValue))
    }
  }

  @async override protected def readAll(r: RequestInfo, n: Namespace)(keys: Seq[n.Key]): Map[n.Key, Option[n.Value]] =
    if (batchRequests) readAllUntyped(r, n)(keys).asInstanceOf[Map[n.Key, Option[n.Value]]]
    else super.readAll(r, n)(keys)

  // Remove namespace-based typing here to allow for node batching
  @async private[cache] def readAllUntyped(r: RequestInfo, n: Namespace)(
      keys: Seq[StoredKey]
  ): Map[StoredKey, Option[Any]] = {
    val typedKeys = keys.asInstanceOf[Seq[n.Key]]
    val p = perspective(n)
    val traceId = newBatchTraceId(n, "readAll", r)(typedKeys)
    val op = p.getWithTrace(typedKeys.toSet.asJava, traceId)
    await(traceId, op)(op.getValues.asScala.mapValuesNow(Option(_)).toMap)
  }

  @async override protected def validKeys(r: RequestInfo, n: Namespace)(keys: Iterable[n.Key]): Set[n.Key] =
    validKeysUntyped(r, n)(keys).asInstanceOf[Set[n.Key]]

  // Remove namespace-based typing here to allow for node batching
  @createNodeTrait @async private def validKeysUntyped(r: RequestInfo, n: Namespace)(
      keys: Iterable[StoredKey]): Set[StoredKey] =
    _validKeysUntyped(r, n)(keys)

  @async private[cache] def _validKeysUntyped(r: RequestInfo, n: Namespace)(keys: Iterable[StoredKey]): Set[StoredKey] =
    if (keys.nonEmpty) {
      val typedKeys = keys.asInstanceOf[Iterable[n.Key]]
      val p = perspective(n)
      val traceId = newBatchTraceId(n, "validKeys", r)(typedKeys)
      val existenceOnly = p.asVanilla.getOptions.getDefaultGetOptions.retrievalType(RetrievalType.EXISTENCE)
      val op = p.retrieveWithTrace(typedKeys.toSet.asJava, existenceOnly, traceId)
      await(traceId, op)(op.getStoredValues.keySet.asScala.toSet)
    } else Set.empty

  @async override protected def write(r: RequestInfo, n: Namespace)(key: n.Key, value: n.Value): Unit =
    writeUntyped(r, n)(key, value)

  // Remove namespace-based typing here to allow for node batching
  @createNodeTrait @async private def writeUntyped(r: RequestInfo, n: Namespace)(key: StoredKey, value: Any): Unit = {
    val typedKey = key.asInstanceOf[n.Key]
    val typedValue = value.asInstanceOf[n.Value]
    val p = perspective(n)
    val traceId = newTraceId(n, "write", r)(typedKey)
    val op = p.putWithTrace(typedKey, typedValue, traceId)
    await(traceId, op)(())
  }

  @async override protected def writeAll(r: RequestInfo, n: Namespace)(
      entries: Seq[(n.Key, n.Value)]
  ): Unit =
    if (batchRequests) writeAllUntyped(r, n)(entries)
    else super.writeAll(r, n)(entries)

  // Remove namespace-based typing here to allow for node batching
  @async private[cache] def writeAllUntyped(r: RequestInfo, n: Namespace)(
      entries: Seq[(StoredKey, Any)]
  ): Unit = {
    val typedEntries = entries.asInstanceOf[Seq[(n.Key, n.Value)]]
    val p = perspective(n)
    val traceId = newBatchTraceId(n, "writeAll", r)(typedEntries.map(_._1))
    val op = p.putWithTrace(typedEntries.toMap.asJava, traceId)
    await(traceId, op)(())
  }

  @async override protected def invalidate(r: RequestInfo, n: Namespace)(key: n.Key): Unit = {
    val p = perspective(n)
    val traceId = newTraceId(n, "invalidate", r)(key)
    val op = p.invalidateWithTrace(key, traceId)
    await(traceId, op)(())
  }

  private def perspective[A](n: Namespace): Perspective[n.Key, n.Value] = {
    val p = n match {
      case Namespace.Artifact  => artifactPerspective
      case Namespace.Directory => directoryPerspective
      case Namespace.File      => filePerspective
    }
    // It makes me sad that I can't do this without needing `asInstanceOf`
    p.asInstanceOf[Perspective[n.Key, n.Value]]
  }

  private def newTraceId(n: Namespace, requestType: String, r: RequestInfo)(key: n.Key) =
    TraceId(r.summary(UUID.randomUUID, n, requestType), ChainedNodeID.nodeID)

  private def newBatchTraceId(n: Namespace, requestType: String, r: RequestInfo)(keys: Iterable[n.Key]) =
    TraceId(r.summary(UUID.randomUUID, n, requestType), ChainedNodeID.nodeID)

  private def str(traceId: TraceId): String = s"[${traceId.requestId}, ${traceId.chainedId}]"

  @async private def await[A <: AsyncKeyedOperation[_], B](
      traceId: TraceId,
      op: A,
      timeoutMillis: Int = DefaultTimeoutMillis
  )(
      f: => B
  ): B = {
    val requestId = str(traceId)
    asyncResult {
      _await(traceId, op, timeoutMillis)(f)
    }.valueOrElse {
      case e: TimeoutException =>
        throw new IllegalStateException(s"SilverKing $clusterType operation timed out for $requestId ($op)", e)
      case e =>
        throw e
    }
  }

  private class TimeoutTaskCompleter(task: TaskTrace, traceId: TraceId, startTime: Instant) extends NodeAwaiter {
    override def onChildCompleted(eq: EvaluationQueue, node: NodeTask): Unit = {
      val e = node.exception
      // in other cases, task should have been completed by `op.addListener` below
      if (e.isInstanceOf[TimeoutException]) {
        task.complete(Failure(e))
        val finishTime = patch.MilliInstant.now
        val awaitTimeMillis = finishTime.toEpochMilli - startTime.toEpochMilli
        writeErrorCrumb(traceId, startTime, finishTime, awaitTimeMillis, e)
      }
    }
  }

  @nodeSync
  private def _await[A <: AsyncKeyedOperation[_], B](
      traceId: TraceId,
      op: A,
      timeoutMillis: Int
  )(
      f: => B
  ): B =
    _await$queued(traceId, op, timeoutMillis)(f).get
  def _await$queued[A <: AsyncKeyedOperation[_], B](
      traceId: TraceId,
      op: A,
      timeoutMillis: Int
  )(
      f: => B
  ): Node[B] = {
    val startTime = patch.MilliInstant.now
    val requestId = str(traceId)
    log.debug(s"Awaiting SilverKing $clusterType completion for $requestId [${op.getNumKeys} in batch] ($op)")
    val task = ObtTrace.startTask(RootScopeId, SilverkingOperation(clusterType, requestId), startTime)
    val promise = NodePromise.withTimeout[B](SilverKingOperationsImpl.awaitTaskInfo, timeoutMillis)
    op.addListener {
      new AsyncOperationListener {
        override def asyncOperationUpdated(asyncOperation: AsyncOperation): Unit = {
          val finishTime = patch.MilliInstant.now
          val awaitTimeMillis = finishTime.toEpochMilli - startTime.toEpochMilli
          log.debug(s"SilverKing $clusterType operation completed for $requestId in $awaitTimeMillis ms ($op)")
          // op.getState incorrectly returns "FAILED" if some (but not all) of the keys are missing for a retrieval
          // operation, so use op.getOpResultMap instead
          val opResults = op.getOpResultMap.asScala
          val (wasIncomplete, taskResult) = {
            // OpResult meanings:
            // - SUCCEEDED: Operation was successful and (if a read) returned a value
            // - NO_SUCH_VALUE: Read operation was successful but no value was found
            // - INVALID_VERSION: Write operation failed because a value was already present - we can get this if two
            //     separate builds attempt to write the same artifact at similar times (it's similar to a
            //     DAL OutdatedVersionException)
            if (
              opResults.values.forall(s =>
                s == OpResult.SUCCEEDED || s == OpResult.NO_SUCH_VALUE || s == OpResult.INVALID_VERSION)
            ) {
              (promise.complete(Try(f)), Success(Nil))
            } else if (op.getFailureCause == FailureCause.SESSION_CLOSED) {
              val failure = Failure(
                // Rethrow as a separate exception type we can catch elsewhere and trigger a reconnection
                new SilverKingSessionClosedException(
                  s"SilverKing $clusterType session is closed for $requestId: $opResults"
                )
              )
              (promise.complete(failure), failure)
            } else {
              val summary =
                opResults.groupBy(_._2).map { case (res, ops) => s"$res: ${ops.size}" }.mkString("[", ", ", "]")
              val failure = Failure(
                new RuntimeException(
                  s"SilverKing $clusterType operation failed with ${op.getFailureCause} for $requestId: $opResults $summary"
                )
              )
              (promise.complete(failure), failure)
            }
          }

          if (wasIncomplete) {
            task.complete(taskResult, time = finishTime)
            taskResult.failed.foreach(t => writeErrorCrumb(traceId, startTime, finishTime, awaitTimeMillis, t))
          } else {
            log.warn(
              s"Received silverKing $clusterType completion callback on already completed node for $requestId. State: ${op.getState}")
          }
          ()
        }
      }
    }
    promise.node.continueWith(new TimeoutTaskCompleter(task, traceId, startTime), EvaluationContext.current)
    promise.node
  }

  private def writeErrorCrumb(
      traceId: TraceId,
      start: Instant,
      end: Instant,
      awaitTimeMillis: Long,
      t: Throwable
  ): Unit = {
    val props = Map(
      "cluster" -> clusterType.toString,
      "reqId" -> traceId.requestId,
      "start" -> start.toString,
      "end" -> end.toString,
      "awaitTimeMillis" -> awaitTimeMillis.toString,
      "error" -> t.toString
    )
    Breadcrumbs.info(
      traceId.chainedId,
      PropertiesCrumb(
        _,
        ObtCrumbSource,
        props,
        obtCategory -> "SilverKingOperation"
      )
    )
  }

  override def close(): Unit =
    try artifactSession.close()
    finally pathSession.close()
}
