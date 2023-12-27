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

import java.net.URL
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.crumbs.Properties.obtCategory
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.buildtool.artifacts.CachedArtifactType
import optimus.buildtool.artifacts.CompilerMessagesArtifact
import optimus.buildtool.artifacts.IncrementalArtifact
import optimus.buildtool.cache.ArtifactStoreBase
import optimus.buildtool.artifacts.CompilationMessage.Warning
import optimus.buildtool.cache.RemoteAssetStore
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.Pathed
import optimus.buildtool.files.RelativePath
import optimus.buildtool.trace._
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.CompilePathBuilder
import optimus.buildtool.utils.Utils
import optimus.core.ChainedNodeID
import optimus.graph.NodeAwaiter
import optimus.graph.NodeTask
import optimus.graph.diagnostics.GraphDiagnostics
import optimus.platform.{Query => _, _}
import optimus.platform.annotations.createNodeTrait
import optimus.platform.util.Log

import scala.annotation.tailrec

object SilverKingStore extends Log {

  object Config {
    private val prefix = SilverKingStoreConfig.prefix

    // defaults to "external"
    val externalArtifactVersion: String = sys.props.get(s"$prefix.externalArtifactVersion").getOrElse("external-1.1")
  }

  private[cache] sealed trait StoredKey

  final case class ArtifactKey(
      id: ScopeId,
      fingerprintHash: String,
      tpe: CachedArtifactType,
      discriminator: Option[String],
      artifactVersion: String
  ) extends StoredKey

  sealed trait AssetContent
  object AssetContent {
    private[cache] val empty: AssetContent = FileContent(0)
  }

  final case class FileContent(segments: Int) extends AssetContent
  // relativePath is None for single files, but Some(...) for directory files
  final case class FileSegmentKey(artifact: ArtifactKey, relativePath: Option[RelativePath], segment: Int)
      extends StoredKey
  // FileSegment is stored as Array[Byte]

  final case class DirectoryContent(files: Int) extends AssetContent
  final case class DirectoryFileKey(artifact: ArtifactKey, index: Int) extends StoredKey
  final case class DirectoryFile(relativePath: RelativePath, fileContent: FileContent)

  private val RetryDelta = Duration.ofMinutes(1)
  private val MaxRetryDelay = Duration.ofHours(1)

  private[silverking] def retryDelta(retryCount: Int): Duration =
    if (retryCount == 0) {
      // the first time, we retry immediately
      Duration.ZERO
    } else {
      // after the first time, we retry after 1 minute, 2 minutes, 4 minutes, 8 minutes, ...up to 1h
      val multiplier = Math.pow(2, retryCount - 1).toLong
      val delta = RetryDelta.multipliedBy(multiplier)
      if (delta.compareTo(MaxRetryDelay) < 0) delta else MaxRetryDelay
    }

  private[cache] def clusterStr(clusterType: ClusterType) = s"Distributed artifact cache (SilverKing $clusterType)"

  sealed trait Connection
  object Connection {
    final case class Disconnected(message: String, t: Throwable, retryTime: Instant, retryCount: Int) extends Connection
    final case class Connected(ops: SilverKingOperations) extends Connection
  }

  def apply(
      pathBuilder: CompilePathBuilder,
      config: SilverKingConfig,
      artifactVersion: String,
      writeArtifacts: Boolean,
      offlinePuts: Boolean = true
  ): SilverKingStore = {
    val clusterType = ClusterType.forLookup(config)
    val connector = new Connector(() => SilverKingOperationsImpl(config, clusterType), clusterType)
    val disconnect = Some(() => connector.disconnect())
    val distributedSwitch = ZookeeperSwitch(config, clusterType, onTrigger = disconnect)
    val failuresSwitch = new MaxFailuresReadWriteSwitch(onTrigger = disconnect)
    new SilverKingStore(
      pathBuilder,
      clusterType,
      artifactVersion,
      writeArtifacts = writeArtifacts,
      offlinePuts = offlinePuts,
      connector,
      distributedSwitch,
      failuresSwitch
    )
  }
}

class SilverKingStore private[cache] (
    pathBuilder: CompilePathBuilder,
    clusterType: ClusterType = ClusterType.QA,
    artifactVersion: String,
    writeArtifacts: Boolean,
    offlinePuts: Boolean,
    connector: Connector,
    distributedSwitch: Variable[Map[OperationType, String]],
    failureSwitch: MaxFailuresReadWriteSwitch
) extends ArtifactStoreBase
    with RemoteAssetStore {
  import SilverKingStore._
  import SilverKingStore.Connection._
  import OperationType._

  override protected val cacheType: String = s"SilverKing $clusterType"
  override protected val stat: ObtStats.Cache = ObtStats.SilverKing

  private val clusterStr = SilverKingStore.clusterStr(clusterType)
  private val statusLogger = new StatusLogger(this, clusterStr)

  def operations(opType: OperationType): Either[String, SilverKingOperations] =
    _operations(opType, connectIfNeeded = true, alwaysLog = false)

  def logStatus(): Seq[String] = {
    _operations(Read, connectIfNeeded = false, alwaysLog = true).left.toSeq ++
      _operations(Write, connectIfNeeded = false, alwaysLog = true).left.toSeq
  }

  // Returns either the operations or an informational message, and also logs the current status if relevant
  private def _operations(
      opType: OperationType,
      connectIfNeeded: Boolean,
      alwaysLog: Boolean
  ): Either[String, SilverKingOperations] = {
    @tailrec def ops(connection: Connection): Either[String, SilverKingOperations] = connection match {
      case Connected(ops) =>
        statusLogger.succeeded(opType)
        Right(ops)
      case Disconnected(_, _, retryTime, retryCount) if shouldRetry(retryTime) =>
        if (connectIfNeeded)
          ops(connector.reconnect(retryCount))
        else
          // Don't log anything - this is not an erroneous state, it just means the connection hasn't yet
          // been re-established after failure (but connection will be reattempted the next time it's needed).
          Left(s"Pending reconnection")
      case Disconnected(msg, _, retryTime, _) =>
        statusLogger.failed(alwaysLog, opType, msg, Some(retryTime))
        Left(msg)
    }

    (connector.connectionOption, distributedSwitch.state.get(opType), failureSwitch.state(opType)) match {
      case (_, Some(msg), _) =>
        statusLogger.failed(alwaysLog, opType, msg)
        Left(msg)
      case (_, _, false) =>
        val msg = s"Cache disabled due to repeated query failures"
        statusLogger.failed(alwaysLog, opType, msg, failureSwitch.retryTime(opType))
        Left(msg)
      case (Some(conn), _, _) =>
        ops(conn)
      case (None, _, _) if connectIfNeeded =>
        ops(connector.connection())
      case (None, _, _) =>
        // Don't log anything - this is not an erroneous state, it just means the connection hasn't yet
        // been established (connectIfNeeded is false).
        Left(s"Connection not initialized")
    }
  }

  protected def shouldRetry(retryTime: Instant): Boolean = retryTime.isBefore(patch.MilliInstant.now)

  protected[silverking] def warningTrace(msg: String): Unit = ObtTrace.warn(msg)
  protected[silverking] def infoTrace(msg: String): Unit = ObtTrace.info(msg)

  private val pendingWrites = new PendingWrites

  private val _incompleteReads = new AtomicInteger(0)
  def incompleteReads: Int = _incompleteReads.get

  private val _incompleteWrites = new AtomicInteger(0)
  def incompleteWrites: Int = _incompleteWrites.get

  private def recordIncomplete(opType: OperationType): Unit = opType match {
    case Read  => _incompleteReads.incrementAndGet()
    case Write => _incompleteWrites.incrementAndGet()
  }

  @async override def get[A <: CachedArtifactType](
      id: ScopeId,
      fingerprintHash: String,
      tpe: A,
      discriminator: Option[String]
  ): Option[A#A] = {
    val key = ArtifactKey(id, fingerprintHash, tpe, discriminator, artifactVersion)
    // Safe to set incremental=false here, since we don't allow incremental artifacts in SK
    val asset = pathBuilder.outputPathFor(key.id, key.fingerprintHash, key.tpe, key.discriminator, incremental = false)

    val storedAsset = _get(key, ArtifactCacheTraceType(tpe), asset)
    storedAsset.map(tpe.fromAsset(id, _))
  }

  @async override def get(url: URL, destination: FileAsset): Option[FileAsset] = {
    val key = ArtifactKey(RootScopeId, url.toString, null, None, Config.externalArtifactVersion)
    _get(key, RemoteAssetCacheTraceType(url), destination)
  }

  @async def _get[A <: CachedArtifactType](
      key: ArtifactKey,
      tpe: CacheTraceType,
      asset: FileAsset): Option[FileAsset] = {
    val id = key.id
    safely[Option[FileAsset]](Read, id, None, s"Failed to get artifact for $tpe") { ops =>
      val (timeNanos, storedContent: Option[Seq[(FileAsset, ByteBuffer)]]) = AdvancedUtils.timed {
        val content = ObtTrace.traceTask(id, Query(clusterType, tpe), failureSeverity = Warning) {
          ops.getArtifact(key)
        }
        for {
          c <- content
          contentBuffer <- ObtTrace.traceTask(id, Fetch(clusterType, tpe), failureSeverity = Warning) {
            ops.getContentData(key, c, asset)
          }
        } yield contentBuffer
      }

      storedContent.foreach { files =>
        val contentSize = files.map { case (_, contentBytes) => contentBytes.limit() }.sum
        logFound(id, tpe, s"$key ($contentSize content bytes in ${timeNanos / 1000000L} ms)")
        ObtTrace.addToStat(stat.ReadBytes, contentSize)
        files.foreach { case (file, contentBuffer) =>
          Utils.createDirectories(file.parent)
          AssetUtils.atomicallyWrite(file) { p =>
            val channel = FileChannel.open(p, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
            try {
              channel.write(contentBuffer)
            } finally channel.close()
          }
        }
      }

      if (storedContent.isEmpty) {
        logNotFound(id, tpe, s"$key (${timeNanos / 1000000L} ms)")
        None
      } else {
        Some(asset)
      }
    }
  }

  @async def check[A <: CachedArtifactType](
      id: ScopeId,
      fingerprintHashes: Set[String],
      tpe: A,
      discriminator: Option[String]
  ): Set[String] =
    safely(Read, id, Set[String](), s"Failed to check fingerprint hashes for $tpe") { ops =>
      debug(id, s"Checking for $tpe: $fingerprintHashes")
      val keys = fingerprintHashes.map(ArtifactKey(id, _, tpe, discriminator, artifactVersion))
      val (timeNanos, validInputsHashes) = AdvancedUtils.timed {
        ObtTrace.traceTask(id, KeyQuery(clusterType, ArtifactCacheTraceType(tpe)), failureSeverity = Warning) {
          ops.getValidKeys(keys).map(_.fingerprintHash)
        }
      }
      debug(
        id,
        s"Found ${validInputsHashes.size}/${fingerprintHashes.size} fingerprint hashes for $tpe in ${timeNanos / 1000000L} ms"
      )
      validInputsHashes
    }

  @async def check(url: URL): Boolean =
    safely[Boolean](Read, RootScopeId, false, s"Failed to check silverking cache for url: $url") { ops =>
      val key = ArtifactKey(RootScopeId, url.toString, null, None, Config.externalArtifactVersion)
      val checkedArtifact =
        ObtTrace.traceTask(key.id, Query(clusterType, RemoteAssetCacheTraceType(url)), failureSeverity = Warning) {
          ops.getValidKeys(Seq(key))
        }
      checkedArtifact.nonEmpty
    }

  @async override protected def write[A <: CachedArtifactType](
      tpe: A)(id: ScopeId, fingerprintHash: String, discriminator: Option[String], artifact: A#A): A#A = {
    artifact match {
      case _ if !writeArtifacts =>
      // don't write artifacts if we're configured not to
      case cma: CompilerMessagesArtifact if cma.hasErrors =>
      // don't write failure artifacts, to protect against non-rt build failures
      case ia: IncrementalArtifact if ia.incremental =>
        throw new IllegalArgumentException(s"Incremental artifacts not permitted in SilverKing: $artifact")
      case _ =>
        val key = ArtifactKey(id, fingerprintHash, tpe, discriminator, artifactVersion)
        maybeOfflinePut(key, ArtifactCacheTraceType(tpe), artifact.path)
    }
    artifact
  }

  @async override def put(url: URL, file: FileAsset): FileAsset = {
    val key = ArtifactKey(RootScopeId, url.toString, null, None, Config.externalArtifactVersion)
    maybeOfflinePut(key, RemoteAssetCacheTraceType(url), file.path)
    file
  }

  @async def maybeOfflinePut(key: ArtifactKey, tpe: CacheTraceType, file: Path): Unit = {
    if (offlinePuts) {
      // we don't need the put to complete before we move on to compiling the next jar, so execute this without
      // waiting on the result
      val n = queuedNodeOf(_put(key, tpe, file))
      pendingWrites.add(n)
    } else
      _put(key, tpe, file)
  }

  @createNodeTrait @async private def _put(key: ArtifactKey, tpe: CacheTraceType, file: Path): Unit = {
    val id = key.id
    safely(Write, id, (), s"Failed to put artifact for $tpe") { ops =>
      val (timeNanos, contentSize) = AdvancedUtils.timed {
        ObtTrace.traceTask(id, Put(clusterType, tpe), failureSeverity = Warning) {
          val (content, size) = ops.putContentData(key, file)
          ops.putArtifact(key, content)
          size
        }
      }

      logWritten(id, tpe, s"$key ($contentSize content bytes in ${timeNanos / 1000000L} ms)")
      ObtTrace.addToStat(stat.WriteBytes, contentSize)
    }
  }

  override def flush(timeoutMillis: Long): Unit = {
    val remainingWrites = pendingWrites.remaining
    if (remainingWrites.nonEmpty) {
      debug(s"Awaiting completion of ${remainingWrites.size} operations...")
      val waiter = new BlockingWaiter(remainingWrites.size)
      remainingWrites.foreach(_.node.continueOnCurrentContextWith(waiter))
      if (waiter.await(timeoutMillis)) debug("Operations complete")
      else {
        warn(s"Flush timed out after ${timeoutMillis}ms with ${waiter.remaining} operations incomplete")
        debug(GraphDiagnostics.getGraphState)
        debug(GraphDiagnostics.getDependencyTrackerWorkInfo(includeUTracks = true))
        remainingWrites.foreach { w =>
          val props = Map(
            "user" -> sys.props.getOrElse("user.name", "unknown"),
            "sysLoc" -> sys.env.getOrElse("SYS_LOC", "unknown"),
            "cluster" -> clusterType.toString,
            "scopeId" -> w.key.id.properPath,
            "hash" -> w.key.fingerprintHash,
            "type" -> w.key.tpe.name,
            "path" -> Pathed.pathString(w.file),
            "complete" -> w.node.isDone.toString
          )
          Breadcrumbs.info(
            ChainedNodeID.nodeID,
            PropertiesCrumb(
              _,
              ObtCrumbSource,
              props,
              obtCategory -> "SilverKingIncompleteWrite"
            )
          )
        }

      }
    }
  }

  private class BlockingWaiter(count: Int) extends NodeAwaiter {
    private val latch = new CountDownLatch(count)
    override def onChildCompleted(eq: EvaluationQueue, node: NodeTask): Unit = latch.countDown()
    def await(timeoutMillis: Long): Boolean = latch.await(timeoutMillis, TimeUnit.MILLISECONDS)
    def remaining: Long = latch.getCount
  }

  @async private def safely[A](opType: OperationType, id: ScopeId, empty: A, failureMessage: String)(
      f: SilverKingOperations => A
  ): A = safely$NF(opType, id, empty, failureMessage)(asNode(f(_)))
  @async private def safely$NF[A](opType: OperationType, id: ScopeId, empty: A, failureMessage: String)(
      f: NodeFunction1[SilverKingOperations, A]
  ): A = operations(opType) match {
    case Right(ops) =>
      asyncResult {
        f(ops)
      } match {
        case NodeSuccess(v) => v
        case NodeFailure(t) =>
          t match {
            case _: SilverKingSessionClosedException =>
              // Don't count this into the switch as some other request will have a different exception
              // that describes the root cause for closing the session.
              recordIncomplete(opType)
              // SK dropped our session, so we need to disconnect. This may fail some other reqs in flight,
              // but they are going to get a SessionClosed failure anyway, as Sk has kicked us out.
              // The next op will have to reconnect, giving us a new open session.
              connector.disconnect()
              empty
            case _ =>
              recordIncomplete(opType)
              val failureState = failureSwitch.reportFailure(opType)
              if (failureState != FailureState.Failed) warn(id, failureMessage, t)
              else debug(id, s"$failureMessage: $t")
              empty
          }
      }
    case Left(errorMessage) =>
      recordIncomplete(opType)
      val msg = s"$failureMessage: $errorMessage"
      debug(id, msg) // associating the message to its scope id in the logs
      warnDedupped(msg) // but not in console, so that we can deduplicate it
      empty
  }

  @async override def close(): Unit = {
    connector.disconnect()
    closeDedupedMessages()
  }
}
