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

import java.io.File
import java.net.URI
import java.time.Duration

import com.ms.silverking.cloud.dht.SessionOptions
import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration
import com.ms.silverking.cloud.dht.client.DHTClient
import com.ms.silverking.cloud.dht.client.DHTSession
import com.ms.silverking.cloud.dht.client.SimpleSessionEstablishmentTimeoutController
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration
import com.ms.zookeeper.clientutils.ZkEnv
import optimus.dal.silverking.SilverKingPlantConfig
import optimus.dal.silverking.SkUtils
import optimus.dal.silverking.SysLoc
import optimus.dal.silverking.client.PrcServiceConfig
import optimus.platform.dal.config.DalServicesSilverKingLookup

sealed trait SilverKingConfig {
  def newSession(client: DHTClient): DHTSession
  def kerberized: Boolean = true
}
object SilverKingConfig {
  private[silverking] val MaxFailures = 5
  private[silverking] val RetryTime = Duration.ofHours(1)

  // Never retry internally on session creation failures
  private val TimeoutController = new SimpleSessionEstablishmentTimeoutController(0, 0, 0)

  trait ZookeeperConfig extends SilverKingConfig {
    def env: ZkEnv
    def clusterName: String
  }

  object ZookeeperLookup {
    def apply(cfg: SilverKingPlantConfig): ZookeeperLookup =
      ZookeeperLookup(cfg.zkEnv, cfg.skClusterName, cfg.skDhtName, cfg.skVersion)
  }

  final case class ZookeeperLookup(env: ZkEnv, clusterName: String, dhtName: String, version: String = "0000000000")
      extends ZookeeperConfig {
    private val cfg = SilverKingPlantConfig(env, clusterName, dhtName, version, SysLoc.current)

    override def newSession(client: DHTClient): DHTSession =
      SkUtils.createSkConnection(client, cfg.getConnectionInfo(), TimeoutController)
  }

  final case class FileConfig(serverPath: File, serverName: String, serverIP: String) extends SilverKingConfig {
    override def newSession(client: DHTClient): DHTSession = client.openSession(
      new SessionOptions(SKGridConfiguration.parseFile(serverPath, serverName), serverIP, TimeoutController)
    )
  }

  final case class ServiceLookup(lookup: DalServicesSilverKingLookup, hostPort: Option[(String, Int)])
      extends ZookeeperConfig {
    private val cfg = PrcServiceConfig.readPlantConfig(lookup)

    override def env: ZkEnv = cfg.zkEnv
    override def clusterName: String = cfg.skClusterName
    def name: String = lookup.env.underlying

    override def newSession(client: DHTClient): DHTSession = {
      val initialInfo = cfg.getConnectionInfo()
      val connInfo = hostPort match {
        case Some((h, p)) =>
          initialInfo.copy(hostList = Seq(h), fixedPort = p)
        case None => initialInfo
      }
      SkUtils.createSkConnection(client, connInfo, TimeoutController)
    }

    override def toString: String = s"${getClass.getSimpleName}(${lookup.env}, ${lookup.instance}, ${lookup.plantName})"
  }

  final case class Explicit(dhtName: String, dhtPort: Int, ensemble: String, override val kerberized: Boolean = true)
      extends SilverKingConfig {
    override def newSession(client: DHTClient): DHTSession = {
      client.openSession(new SessionOptions(new ClientDHTConfiguration(dhtName, dhtPort, ensemble)))
    }
    def uriString: String = s"sk://$dhtName:$dhtPort/$ensemble${if (!kerberized) "?krb=0" else ""}"
    override def toString = s"Explicit($uriString)"
  }
  object Explicit {
    def fromUri(uri: URI): Explicit = {
      assert(uri.getScheme == "sk", uri)
      val kerberized = {
        val krbQuery = """krb=(\d)""".r
        uri.getQuery match {
          case krbQuery(v) => v.toInt != 0
          case _           => true
        }
      }
      val (dhtName, dhtPort, ensemble) = (uri.getHost, uri.getPort, uri.getPath stripPrefix "/")
      new Explicit(dhtName, dhtPort, ensemble, kerberized)
    }
  }

  // see unit tests for more examples
  private val ZkFormat = """zk://([^/]+)/(.+)/([^/]+)/([0-9]+)""".r // eg. zk://qa/qa_obt/SK_Qa_OBT/0000000000
  private val ShortZkFormat = """zk://([^/]+)/(.+)/([^/]+)""".r // eg. zk://qa/qa_obt/SK_Qa_OBT
  private val FileFormat = """sk://([^/]+)/(.+)/([^/]+)""".r
  private val ExplicitFormat =
    """sk://.*:.*/.*""".r
  private val LookupWithHostFormat =
    s"""(${DalServicesSilverKingLookup.UriScheme}://.+)\\?host=(.+):(.+)""".r
  private val LookupFormat =
    s"""(${DalServicesSilverKingLookup.UriScheme}://.+)""".r // eg. skClient://obtqa/global/obtqa_obt or skClient://obtdev/global/obtdev_obt

  def apply(cfg: String): SilverKingConfig = cfg match {
    case ZkFormat(envStr, cluster, dht, version) => ZookeeperLookup(ZkEnv.valueOf(envStr), cluster, dht, version)
    case ShortZkFormat(envStr, cluster, dht)     => ZookeeperLookup(ZkEnv.valueOf(envStr), cluster, dht)
    case FileFormat(serverIP, path, serverName)  => FileConfig(new File(path), serverName, serverIP)
    case uriStr @ ExplicitFormat(_*)             => Explicit.fromUri(new URI(uriStr))
    case LookupWithHostFormat(uriStr, host, port) =>
      ServiceLookup(DalServicesSilverKingLookup.fromUri(new URI(uriStr)), Some((host, port.toInt)))
    case LookupFormat(uriStr) => ServiceLookup(DalServicesSilverKingLookup.fromUri(new URI(uriStr)), None)
  }
}
