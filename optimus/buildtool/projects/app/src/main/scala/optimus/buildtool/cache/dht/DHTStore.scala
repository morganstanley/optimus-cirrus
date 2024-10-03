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

package optimus.buildtool.cache.dht

import optimus.buildtool.artifacts.CachedArtifactType
import optimus.buildtool.artifacts.CompilerMessagesArtifact
import optimus.buildtool.artifacts.IncrementalArtifact
import optimus.buildtool.artifacts.Severity.Warning
import optimus.buildtool.cache.ArtifactStoreBase
import optimus.buildtool.cache.ComparableArtifactStore
import optimus.buildtool.cache.RemoteAssetStore
import optimus.buildtool.cache.silverking.ClusterType
import optimus.buildtool.cache.silverking.SilverKingStore.Config.externalArtifactVersion
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.files.FileAsset
import optimus.buildtool.trace.ArtifactCacheTraceType
import optimus.buildtool.trace.CacheTraceType
import optimus.buildtool.trace.Fetch
import optimus.buildtool.trace.KeyQuery
import optimus.buildtool.trace.ObtStats
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.Put
import optimus.buildtool.trace.Query
import optimus.buildtool.trace.RemoteAssetCacheTraceType
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.CompilePathBuilder
import optimus.buildtool.utils.Utils
import optimus.buildtool.utils.Utils.durationStringNanos
import optimus.dht.client.api.DHTClient
import optimus.dht.client.api.DHTClientBuilder
import optimus.dht.client.api.kv.KVClient
import optimus.dht.client.api.kv.KVKey
import optimus.dht.client.api.kv.KVLargeEntry
import optimus.dht.client.api.kv.KVLargeValue
import optimus.dht.client.api.replication.SimpleReplicationStrategy
import optimus.dht.common.api.Keyspace
import optimus.platform._
import optimus.platform.util.Log

import java.net.URL
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.time.Duration
import scala.util.Failure
import scala.util.Try
import scala.util.Using

object DHTStore extends Log {
  import optimus.buildtool.artifacts.JsonImplicits._
  import spray.json.DefaultJsonProtocol._
  import spray.json._
  implicit val keyFormat: JsonFormat[ArtifactKey] =
    jsonFormat[ScopeId, String, CachedArtifactType, Option[String], String, ArtifactKey](
      ArtifactKey.apply,
      "id",
      "fingerprintHash",
      "tpe",
      "discriminator",
      "artifactVersion")
  implicit val keyFormat2: JsonFormat[AssetKey] =
    jsonFormat[URL, String, AssetKey](AssetKey.apply, "url", "externalArtifactVersion")
  implicit val keyFormat3: JsonFormat[StoredKey] = new JsonFormat[StoredKey] {
    override def read(json: JsValue): StoredKey = json match {
      case x if x.asJsObject.fields.contains("id")  => x.convertTo[ArtifactKey]
      case x if x.asJsObject.fields.contains("url") => x.convertTo[AssetKey]
      case _                                        => throw new IllegalArgumentException(s"Unknown key type: $json")
    }
    override def write(obj: StoredKey): JsValue = obj match {
      case x: ArtifactKey => x.toJson(keyFormat)
      case x: AssetKey    => x.toJson(keyFormat2)
    }
  }

  implicit def storedToKV(stored: StoredKey): KVKey = new KVKey(stored.toJson(keyFormat3).compactPrint.getBytes())
  implicit def kvToStored(kv: KVKey): StoredKey = new String(kv.key()).parseJson.convertTo[StoredKey]

  private val zkClusterTypePattern = """.*/obt-build-cache-(dev|qa)/.*""".r
  def zkClusterType(zkPath: String): ClusterType = zkPath match {
    case zkClusterTypePattern("dev") => ClusterType.Dev
    case zkClusterTypePattern("qa")  => ClusterType.QA
    case _                           => ClusterType.Custom
  }

  def ZkBuilder(zkPath: String): DHTClientBuilder =
    DHTClientBuilder.create
      .zkPath(zkPath)
      .kerberos(true)
      .replicationStrategy(new SimpleReplicationStrategy(2))
      .defaultOperationTimeout(
        Duration.ofSeconds(System.getProperty("obt.dht.defaultOperationTimeoutSeconds", "60").toLong))
      .ioThreads(System.getProperty("obt.dht.ioThreads", "12").toInt)

  private[cache] sealed trait StoredKey {
    val id: ScopeId
    val traceType: CacheTraceType
    val keyspace: Keyspace
  }

  final case class ArtifactKey(
      override val id: ScopeId,
      fingerprintHash: String,
      tpe: CachedArtifactType,
      discriminator: Option[String],
      artifactVersion: String
  ) extends StoredKey {
    override val traceType: CacheTraceType = ArtifactCacheTraceType(tpe)
    override val keyspace: Keyspace = Keyspace.of("obt-artifacts")

  }

  final case class AssetKey(url: URL, externalArtifactVersion: String) extends StoredKey {
    override val id: ScopeId = RootScopeId
    override val traceType: CacheTraceType = RemoteAssetCacheTraceType(url)
    override val keyspace: Keyspace = Keyspace.of("obt-assets")
  }

  private def debugString(keys: Set[StoredKey]): String = {
    val suffix =
      if (keys.size >= 3) ""
      else
        keys
          .map {
            case k: ArtifactKey => k.fingerprintHash
            case k: AssetKey    => k.url.toString
          }
          .mkString(", ")
    (keys.headOption match {
      case Some(ArtifactKey(id, _, tpe, discriminator, version)) => s"Artifact:$id:$tpe:$discriminator:$version"
      case Some(AssetKey(_, version))                            => s"$version"
      case _                                                     => ""
    }) + suffix
  }

  val MappedBufferThreshold: Long = 1024 * 1024 // 1MB

}

class DHTStore(
    pathBuilder: CompilePathBuilder,
    clusterType: ClusterType = ClusterType.QA,
    artifactVersion: String,
    writeArtifacts: Boolean,
    clientBuilder: DHTClientBuilder)
    extends ArtifactStoreBase
    with RemoteAssetStore
    with ComparableArtifactStore
    with Log {
  import DHTStore._
  override protected def cacheType: String = s"DHT $clusterType"
  override protected def stat: ObtStats.Cache = ObtStats.DHT
  private val client: DHTClient = clientBuilder.build()

  override def toString: String = client.getServerConnectionsManager.toString
  private val lvClient = {
    waitForConsistentRegistry(Duration.ofSeconds(System.getProperty("obt.dht.initTimeoutSeconds", "45").toInt))
    new AsyncGraphLargeValueClient(client.getModule(classOf[KVClient[KVKey]]))
  }

  private def waitForConsistentRegistry(timeout: Duration): Unit = {
    Try {
      client.waitForConsistentRegistry(timeout)
    } match {
      case Failure(e) =>
        log.warn(s"Failed to get consistent registry for DHT after $timeout: ${e.getMessage}")
      case _ =>
    }
  }

  @async private[dht] def readAsset(path: FileAsset): Array[ByteBuffer] = {
    Using.resource(FileChannel.open(path.path, StandardOpenOption.READ)) { channel =>
      val fSize = channel.size()
      if (fSize < DHTStore.MappedBufferThreshold) {
        val buffer = ByteBuffer.allocate(fSize.toInt)
        channel.read(buffer)
        Array(buffer.flip())
      } else {
        Array(channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size()))
      }
    }
  }

  @async private def writeAsset(asset: FileAsset, buffers: Array[ByteBuffer]): Unit = {
    Utils.createDirectories(asset.parent)
    AssetUtils.atomicallyWrite(asset) { p =>
      Using.resource(FileChannel.open(p, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) { channel =>
        buffers.foreach { b =>
          if (b.hasRemaining)
            channel.write(b)
        }
      }
    }
  }

  @async private def _put(key: StoredKey, asset: FileAsset): Unit = {
    AdvancedUtils.timed {
      ObtTrace.traceTask(key.id, Put(clusterType, key.traceType), failureSeverity = Warning) {
        val content = readAsset(asset)
        (
          content.map(_.remaining()).sum,
          lvClient.putLarge(key.keyspace, new KVLargeEntry(key, new KVLargeValue(content)), key.toString))
      }
    } match {
      case (timeNanos, (contentSize, overwritten)) =>
        logWritten(
          key.id,
          key.traceType,
          s"$key ($contentSize content bytes in ${durationStringNanos(timeNanos)}) (overwrote existing: $overwritten)")
        ObtTrace.addToStat(stat.WriteBytes, contentSize)
    }
  }

  // private admin method - not meant for general use
  @async private[dht] def _remove(key: StoredKey): Boolean = {
    AdvancedUtils.timed {
      lvClient.removeLarge(key.keyspace, key, key.toString)
    } match {
      case (timeNanos, result) =>
        logRemoved(key.id, key.traceType, s"$key (removed=$result) in ${durationStringNanos(timeNanos)}")
        result
    }
  }

  @async private def _get(key: StoredKey, destination: FileAsset): Option[FileAsset] = {
    AdvancedUtils.timed {
      ObtTrace.traceTask(key.id, Fetch(clusterType, key.traceType), failureSeverity = Warning) {
        lvClient.getLarge(key.keyspace, key, key.toString)
      }
    } match {
      case (timeNanos, Some(r)) =>
        val contentSize = r.buffers().map(_.remaining()).sum
        logFound(key.id, key.traceType, s"$key ($contentSize content bytes in ${durationStringNanos(timeNanos)}")
        ObtTrace.addToStat(stat.ReadBytes, contentSize)
        writeAsset(destination, r.buffers())
        Some(destination)
      case (_, _) => None
    }
  }

  @async private def _check(keys: Set[StoredKey]): Set[StoredKey] = {
    if (keys.nonEmpty) {
      lvClient
        .contains(
          keys.head.keyspace,
          keys.map(storedToKV),
          debugString(keys)
        )
        .map(_.map(kvToStored))
        .getOrElse(Set.empty)
    } else {
      Set.empty
    }
  }

  @async override protected[buildtool] def write[A <: CachedArtifactType](
      tpe: A)(id: ScopeId, fingerprintHash: String, discriminator: Option[String], artifact: A#A): A#A = {
    artifact match {
      case _ if !writeArtifacts                           => // don't write if we're not configured to
      case cma: CompilerMessagesArtifact if cma.hasErrors => // don't write failure artifacts, may be non-rt failure
      case ia: IncrementalArtifact if ia.incremental =>
        throw new IllegalArgumentException(
          s"Incremental artifacts not permitted in DHT remote artifact store: $artifact")
      case _ =>
        _put(
          ArtifactKey(id, fingerprintHash, tpe, discriminator, artifactVersion),
          FileAsset(artifact.path)
        )
    }
    artifact
  }

  @async override def get[A <: CachedArtifactType](
      id: ScopeId,
      fingerprintHash: String,
      tpe: A,
      discriminator: Option[String]): Option[A#A] = {
    val key = ArtifactKey(id, fingerprintHash, tpe, discriminator, artifactVersion)
    val storedAsset = _get(key, pathBuilder.outputPathFor(id, fingerprintHash, tpe, discriminator, incremental = false))
    tpe.fromRemoteAsset(storedAsset, id, key.toString, stat)
  }

  @async override def check[A <: CachedArtifactType](
      id: ScopeId,
      fingerprintHashes: Set[String],
      tpe: A,
      discriminator: Option[String]): Set[String] = {
    AdvancedUtils.timed {
      ObtTrace.traceTask(id, KeyQuery(clusterType, ArtifactCacheTraceType(tpe)), failureSeverity = Warning) {
        _check(fingerprintHashes.map(f => ArtifactKey(id, f, tpe, discriminator, artifactVersion))).map {
          case k: ArtifactKey => k.fingerprintHash
          case _              => throw new IllegalArgumentException("Unexpected key type") // shouldn't happen :)
        }
      }
    } match {
      case (timeNanos, validHashes) =>
        debug(
          id,
          s"Found ${validHashes.size}/${fingerprintHashes.size} fingerprint hashes for $tpe in ${durationStringNanos(timeNanos)}")
        validHashes
    }
  }

  @async override def get(url: URL, destination: FileAsset): Option[FileAsset] = {
    _get(AssetKey(url, externalArtifactVersion), destination)
  }

  @async override def put(url: URL, file: FileAsset): FileAsset = {
    _put(AssetKey(url, externalArtifactVersion), file)
    file
  }

  // private admin method - not meant for general use
  @async private[dht] def remove(url: URL): Boolean = {
    _remove(AssetKey(url, externalArtifactVersion))
  }

  @async override def check(url: URL): Boolean = {
    AdvancedUtils.timed {
      ObtTrace.traceTask(RootScopeId, Query(clusterType, RemoteAssetCacheTraceType(url)), failureSeverity = Warning) {
        _check(Set(AssetKey(url, externalArtifactVersion)))
      }
    } match {
      case (timeNanos, validKeys) =>
        debug(s"Found ${validKeys.size} artifacts for url: '$url', in ${durationStringNanos(timeNanos)}")
        validKeys.nonEmpty
    }
  }

  override def logStatus(): Seq[String] = Seq()
  override def incompleteWrites: Int = lvClient.getFailedWrites.toInt
  @async override def close(): Unit = {
    client.shutdown(true)
  }
}
