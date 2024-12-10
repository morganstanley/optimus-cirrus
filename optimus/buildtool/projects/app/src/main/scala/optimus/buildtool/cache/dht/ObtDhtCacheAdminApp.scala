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

import com.opencsv.CSVWriter
import msjava.zkapi.internal.ZkaData
import optimus.buildtool.OptimusBuildTool
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.CachedArtifactType
import optimus.buildtool.cache.CacheMode
import optimus.buildtool.cache.dht.DHTStore._
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Pathed
import optimus.buildtool.utils.CompilePathBuilder
import optimus.dht.client.internal.registry.ZookeeperRegistryObserver
/* import optimus.dht.common.util.registry.ZKUtils
import optimus.dht.common.util.registry.ZkConfig
import optimus.dht.server.api.registry.RegistryConsts
import optimus.dht.server.management.DHTManagementInterface */
import optimus.platform._
import optimus.platform.util.ArgHandlers.StringOptionOptionHandler
import optimus.platform.util.Log
import optimus.utils.app.ScalaEnumerationOptionHandler
import org.apache.curator.utils.ZKPaths
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.OptionDef
import org.kohsuke.args4j.spi.Setter
import spray.json._

import java.io.FileWriter
import java.net.URI
import java.nio.file.Paths
import javax.management.MBeanServerInvocationHandler
import javax.management.ObjectName
import javax.management.remote.JMXConnectorFactory
import javax.management.remote.JMXServiceURL
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.Using

class ObtDhtCacheAdminAppCmdLine extends OptimusAppCmdLine {
  import org.kohsuke.args4j.{Option => ArgOption}

  @ArgOption(
    name = "--zk-path",
    required = true,
    usage = "Zookeeper path to DHT cluster"
  )
  val zkPath: String = ""

  @ArgOption(
    name = "--operation",
    required = true,
    handler = classOf[ObtDhtCachedAdminOperationOptionHandler],
    usage = "Operation to perform - get all keys, get specific key, or remove a key"
  )
  val operation: ObtDhtCacheAdminOperation.Value = ObtDhtCacheAdminOperation.GetKey

  @ArgOption(
    name = "--keyspace-name",
    required = true,
    usage = "Keyspace name of interest"
  )
  val keyspaceName: String = ""

  @ArgOption(
    name = "--out-csv-path",
    handler = classOf[StringOptionOptionHandler],
    usage = "If choosing to get all keys, path to where to generate CSV output file"
  )
  val outCsvPath: Option[String] = None

  @ArgOption(
    name = "--artifact-version",
    usage = "Custom artifact version. Defaults to latest artifact version"
  )
  val artifactVersion: String = OptimusBuildTool.DefaultArtifactVersionNumber

  @ArgOption(
    name = "--scope",
    usage = "Scope ID of key (when getting/removing specific key)"
  )
  val scope: String = ""

  @ArgOption(
    name = "--hash",
    usage = "Hash of the artifact key (when getting/removing specific key)"
  )
  val hash: String = ""

  @ArgOption(
    name = "--type",
    usage = "Type of artifact key (when getting/removing specific key)"
  )
  val artifactType: String = ""

  @ArgOption(
    name = "--discriminator",
    handler = classOf[StringOptionOptionHandler],
    usage = "Discriminator of artifact key (when getting/removing specific key)"
  )
  val discriminator: Option[String] = None

  @ArgOption(
    name = "--build-dir",
    usage = "Build directory for where to save artifact key entry (when getting specific key)"
  )
  val buildDir: String = "."
}

object ObtDhtCacheAdminApp extends OptimusApp[ObtDhtCacheAdminAppCmdLine] with Log {
  private lazy val _cmdLine = cmdLine
  import _cmdLine._

  private lazy val jmxUrls: Seq[String] = {
    val urls = getJmxUrls
    log.info(s"found ${urls.size} nodes: ${urls.mkString("\n\t", "\n\t", "")}")
    urls
  }

  private lazy val keyConverter: Array[Byte] => StoredKey = (key: Array[Byte]) =>
    keyspaceName match {
      case "obt-assets"    => new String(key).parseJson.convertTo[AssetKey]
      case "obt-artifacts" => new String(key).parseJson.convertTo[ArtifactKey]
      case other           => throw new IllegalArgumentException(s"Unknown OBT key type: $other")
    }

  private def getArtifactKey: StoredKey =
    keyspaceName match {
      case "obt-assets" => AssetKey(new URI(scope).toURL, artifactVersion)
      case "obt-artifacts" =>
        ArtifactKey(scopeId, hash, cachedArtifactType, discriminator, artifactVersion)
      case other => throw new IllegalArgumentException(s"Unknown OBT key type: $other")
    }

  private lazy val scopeId = ScopeId.parse(scope)
  private lazy val cachedArtifactType =
    ArtifactType.parse(artifactType).asInstanceOf[CachedArtifactType]

  private lazy val dhtStore =
    new DHTStore(
      CompilePathBuilder(Directory(Paths.get("."))),
      DHTStore.zkClusterType(zkPath),
      artifactVersion,
      cacheMode = CacheMode.ReadWrite, // admin needs to be able to read and delete
      /* DHTStore.ZkBuilder(zkPath) */
    )

  @entersGraph override def run(): Unit = {
    import ObtDhtCacheAdminOperation._
    operation match {
      case GetKey =>
        getKey
      case GetAllKeys =>
        getAllKeys
      case RemoveKey =>
        removeKey()
      case other =>
        throw new IllegalArgumentException(s"Unknown operation: $other")
    }
  }

  @async private def getKey: Option[Pathed] = {
    val pathBuilder = CompilePathBuilder(Directory(Paths.get(buildDir)))

    val store =
      new DHTStore(
        pathBuilder,
        DHTStore.zkClusterType(zkPath),
        artifactVersion,
        cacheMode = CacheMode.ReadOnly,
        /* DHTStore.ZkBuilder(zkPath) */)

    val maybeResult = getArtifactKey match {
      case artifactKey: ArtifactKey =>
        store.get(artifactKey.id, hash, cachedArtifactType, discriminator)
      case assetKey: AssetKey =>
        store.get(assetKey.url, pathBuilder.outputDir.resolveFile(scope))
    }

    log.info(s"Result of key: ${maybeResult.getOrElse("NOT FOUND")}")

    maybeResult
  }

  @async private def removeKey(): Boolean = {
    val key = getArtifactKey
    getKey match {
      case Some(res) =>
        log.info(s"[before removal] Query result: $res")
        val result = dhtStore._remove(key)
        log.info(s"Result of key removal: $result")
        result
      case None =>
        log.error(s"Failed to find any entry, so nothing to invalidate, for key: $key")
        false
    }
  }

  private def getAllKeys: Seq[StoredKey] = {

    val keys: Seq[StoredKey] = ??? /* jmxUrls.flatMap { jmxUrl =>
      withDhtManagementBean(jmxUrl) { bean =>
        bean.returnKeyspaceKeys(keyspaceName, "", false).toSeq.map { case Array(maybeKey, _, _) =>
          Option(maybeKey).flatMap {
            case key: Array[Byte] =>
              Some(keyConverter(key))
            case other =>
              log.info(s"unknown key format: $other")
              None
          }
        }
      }
    }.flatten */

    log.info(s"found ${keys.size} keys in total")
    if (keys.nonEmpty) {
      log.info(s"preview of keys: ${keys.take(10).mkString("\n\t", "\n\t", "")}")
    }

    def toCsvRowHeader(storedKey: StoredKey): Array[String] = storedKey match {
      case _: ArtifactKey =>
        Array("id", "fingerprint_hash", "type", "discriminator", "artifact_version")
      case _: AssetKey =>
        Array("id", "url", "artifact_version")
    }
    def toCsvRow(storedKey: StoredKey): Array[String] = storedKey match {
      case key: ArtifactKey =>
        Array(key.id.toString, key.fingerprintHash, key.tpe.name, key.discriminator.getOrElse(""), key.artifactVersion)
      case key: AssetKey =>
        Array(key.id.toString, key.url.toString, key.externalArtifactVersion)
    }

    outCsvPath.foreach { outPath =>
      if (keys.nonEmpty) {
        Using(new CSVWriter(new FileWriter(outPath))) { writer =>
          keys.lift(1).foreach { key => writer.writeNext(toCsvRowHeader(key)) }
          writer.writeAll(keys.map(key => toCsvRow(key)).asJava)

          log.info(s"saved ${keys.size} keys to: $outPath")
        }
      } else {
        log.warn("no keys found: nothing to save to file")
      }

    }

    keys
  }

  private def getJmxUrls: Seq[String] = ??? /* {
    val zkConfig = ZkConfig.parse(zkPath, true)
    val curatorFramework = ZKUtils.getFrameworkBuilder(zkConfig).build
    curatorFramework.start()
    curatorFramework.blockUntilConnected()

    val basePath = ZKPaths.makePath(zkConfig.getBasePath, ZookeeperRegistryObserver.SERVERS_PATH)
    val children = curatorFramework.getChildren.forPath(basePath).asScala.toSeq

    children.flatMap { child =>
      val childData = curatorFramework.getData.forPath(ZKPaths.makePath(basePath, child))
      val zkaData = ZkaData.fromBytes(childData)

      Option(zkaData.get(RegistryConsts.JMX_URL_KEY).asInstanceOf[String])
    }
  } */

  private val dhtManagementObjectName = "com.dht:type=DHTManagement"

  /* private def withDhtManagementBean[T](jmxUrlStr: String)(f: DHTManagementInterface => T): T = {
    val conn = Try {
      log.info(s"Connecting to DHT jmx interface with url: $jmxUrlStr")
      val jmxUrl = new JMXServiceURL(jmxUrlStr)
      JMXConnectorFactory.connect(jmxUrl)
    } match {
      case Success(res) => res
      case Failure(exc) =>
        log.error(s"Some error occurred while creating a JMX connection to URL: $jmxUrlStr")
        throw exc
    }
    log.info(s"Successfully connected to DHT jmx interface.")

    try {
      log.info(s"Retrieving DHT MBean with object name: $dhtManagementObjectName.")
      val mBeanServerConn = conn.getMBeanServerConnection
      val objectName = new ObjectName(dhtManagementObjectName)
      val manager = MBeanServerInvocationHandler
        .newProxyInstance[DHTManagementInterface](mBeanServerConn, objectName, classOf[DHTManagementInterface], false)
      f(manager)
    } finally {
      conn.close()
    }
  } */

}

object ObtDhtCacheAdminOperation extends Enumeration {
  val GetAllKeys, GetKey, RemoveKey = Value
}
class ObtDhtCachedAdminOperationOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Any])
    extends ScalaEnumerationOptionHandler(ObtDhtCacheAdminOperation, parser, option, setter)
