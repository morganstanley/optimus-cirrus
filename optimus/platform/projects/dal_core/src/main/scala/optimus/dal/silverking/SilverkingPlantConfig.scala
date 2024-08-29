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
package optimus.dal.silverking

import java.io.ByteArrayInputStream

import com.ms.silverking.cloud.dht.SessionOptions
import com.ms.silverking.cloud.dht.SessionPolicyOnDisconnect
import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration
import com.ms.silverking.cloud.dht.client.ClientDHTConfigurationProvider
import com.ms.silverking.cloud.dht.client.SessionEstablishmentTimeoutController
import com.ms.silverking.cloud.dht.meta.DHTConfiguration
import com.ms.silverking.cloud.dht.meta.{MetaPaths => DhtMetaPaths}
import com.ms.silverking.cloud.meta.CloudConfiguration
import com.ms.silverking.cloud.meta.{MetaPaths => CloudMetaPaths}
import com.ms.silverking.cloud.topology.Node
import com.ms.silverking.cloud.topology.NodeClass
import com.ms.silverking.cloud.topology.TopologyParser
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient
import com.ms.zookeeper.clientutils.ZkClientUtils
import com.ms.zookeeper.clientutils.ZkEnv
import optimus.config.MissingConfigPropertyException
import optimus.config.RuntimeConfiguration
import optimus.dsi.zookeeper.CuratorZkOperations
import optimus.dsi.zookeeper.ZNode
import optimus.platform.dal.config.BrokerConfigSilverKingLookup
import optimus.platform.dal.config.DalEnv
import optimus.platform.runtime.ZkUtils

import scala.jdk.CollectionConverters._
import scala.util.Random

final case class SilverKingConnectionInfo private (
    dhtName: String,
    hostList: Seq[String],
    fixedPort: Int,
    zkLoc: String
) {
  // this should be included in plant config somewhere but for now it is always true so it can be hard-coded
  def kerberized: Boolean = true
}

final case class DHTConnectionInfo(
    dhtCfgProvider: ClientDHTConfigurationProvider,
    proxyCandidates: Seq[String],
    timeoutController: SessionEstablishmentTimeoutController = SessionOptions.getDefaultTimeoutController) {

  def getSKSessionOptions: SessionOptions = {
    // select a random host as proxy.
    val proxy = Random.shuffle(proxyCandidates).head
    // we want session to be closed on disconnect so that we can do a retry at prc client level and create a new client
    new SessionOptions(dhtCfgProvider, proxy, timeoutController, SessionPolicyOnDisconnect.CloseSession)
  }
}

object SilverKingPlantConfig {
  private val ipPattern = "([0-9]{1,3}.){3}[0-9]{1,3}".r

  private val SkPlantsPropertyBrokerConfig = "optimus.dsimappings.silverking.plants"

  def getSkRootPath(clusterName: String): String = s"/silverking/$clusterName"
  def getSkRootZNode(clusterName: String): ZNode = ZNode(getSkRootPath(clusterName))

  def apply(
      zkEnv: ZkEnv,
      skClusterName: String,
      skDhtName: String,
      skVersion: String,
      sysLoc: Option[SysLoc]
  ) = new SilverKingPlantConfig(zkEnv, skClusterName, skDhtName, skVersion, sysLoc)

  private[optimus] def fromBrokerConfig(
      conf: RuntimeConfiguration,
      brokerLookup: BrokerConfigSilverKingLookup,
      sysLoc: Option[SysLoc] = SysLoc.current
  ): SilverKingPlantConfig = {
    val env = DalEnv(conf.env)

    def getSkParameter(name: String): String = {
      val path = s"$SkPlantsPropertyBrokerConfig.${brokerLookup.plantName}.$name"
      conf.getString(path).getOrElse(throw new MissingConfigPropertyException(path))
    }

    val zkEnv = ZkUtils.getZkEnv(env)
    val skClusterName = getSkParameter("skClusterName")
    val skDhtName = getSkParameter("skDhtName")
    val skVersion = getSkParameter("version")

    SilverKingPlantConfig(zkEnv, skClusterName, skDhtName, skVersion, sysLoc)
  }
}

final case class SysLoc(building: String, city: String, continent: String)
object SysLoc {
  def current: Option[SysLoc] = sys.env.get("SYS_LOC").map(_.split("\\.").toSeq).collect {
    case _ :+ building :+ city :+ continent => SysLoc(building, city, continent)
  }
}

/**
 * NOTE: We always use these fixed names to deploy our silverking cluster
 */
object SilverKingPlantConfigFixedNames {
  /* Cloud 1.1: Zk node names */
  val topologyZkName = "topology"
  val exclusionsZkName = "exclusions"
  val hostGroupTableZkName = "host_groups"
  /* Cloud 1.2: Names inside config */
  val hostGroupName = "Host.VM"

  /* Toporing 1.1: Zk node names */
  val storagePolicyGroupZkName = "storagePolicy"
  val weightsZkName = "weights"
  // used by silverking cluster auto-deployment
  val ringZkName = "ring"
  // ring names below are used by silverking clusters that are manually deployed
  val legacyDevdevPayloadRingZkName = "ring_dal"
  val legacyDevdevPrcRingZkName = "ring_dal"
  val legacyDevNyPayloadRingZkName = "payload_dal"
  val legacyQaObtRingZkName = "ring_obt_global"
  /* Toporing 1.2: Names inside config */
  val storagePolicyRootName = "GlobalPolicy_pri"

  /* Dht */
  val classVarsZkName = "host_vars"
}

class SilverKingPlantConfig(
    val zkEnv: ZkEnv,
    val skClusterName: String,
    val skDhtName: String,
    val skVersion: String,
    val sysLoc: Option[SysLoc]
) {

  import SilverKingPlantConfig.getSkRootZNode
  import SilverKingPlantConfig.ipPattern
  import SilverKingPlantConfigFixedNames._

  val rootNode = getSkRootZNode(skClusterName).path
  private val cloudMetaPaths = new CloudMetaPaths(
    new CloudConfiguration(
      topologyZkName,
      exclusionsZkName,
      hostGroupTableZkName
    ))
  protected def clientDhtConfig(zkLoc: String): ClientDHTConfiguration =
    new ClientDHTConfiguration(skDhtName, zkLoc)

  // TODO (OPTIMUS-30238): Replace these ZkPath and the parsing impls below with Silverking's internal MetaClient impl (skVersion is actually not needed)
  private val allHostsZkPath = s"$rootNode${cloudMetaPaths.getHostGroupPath}/$skVersion"
  private val dhtConfigZkPath = s"$rootNode/cloud/dht/instances/$skDhtName/config/$skVersion"
  private val topologyZkPath = s"$rootNode${cloudMetaPaths.getTopologyPath}/$skVersion"
  private val exclusionsZkPath = s"$rootNode${DhtMetaPaths.getInstanceExclusionsPath(skDhtName)}"
  private val suspectsZkPath = s"$rootNode${DhtMetaPaths.getInstanceSuspectsPath(skDhtName)}"

  private val Continent = NodeClass.continent
  private val Region = NodeClass.region
  private val Building = NodeClass.forName("Building")

  // no need to instantiate if we are passing ZK ops in for test purposes
  private lazy val (skZkConnectionString, zkClient) = getZkConnStrAndClient

  private[optimus] final def hasExcludedHosts() = getExcludedHosts().nonEmpty

  private[optimus] def getExcludedHosts(zkClient: CuratorZkOperations = zkClient): Set[String] = {
    val versions = zkClient.getChildren(ZNode(exclusionsZkPath))
    if (versions.isEmpty)
      Set.empty
    else {
      val latestVersion = versions.map(_.toLong).max
      val excludedPath = SilverKingZooKeeperClient.padVersionPath(exclusionsZkPath, latestVersion)
      val hostsString = new String(zkClient.getData(ZNode(excludedPath)))
      ipPattern.findAllIn(hostsString).toSet
    }
  }

  private[optimus] def getSuspectedHosts(): Set[String] = {
    val nodes = zkClient.getChildren(ZNode(suspectsZkPath))
    val nodePatterns: Seq[Set[String]] = nodes.map { node =>
      val suspectedPath = suspectsZkPath + "/" + node
      val hostsString = new String(zkClient.getData(ZNode(suspectedPath)))
      if (hostsString == "<empty>") {
        Set.empty[String]
      } else {
        val ipAddresses = hostsString.split(",").toList
        val patterns = ipAddresses.map { ip =>
          ipPattern.findFirstIn(ip).getOrElse(throw new IllegalArgumentException(s"$ip is not an IP"))
        }.toSet
        patterns
      }
    }
    nodePatterns.flatten.toSet
  }

  def createSkgc(): ClientDHTConfiguration = {
    new ClientDHTConfiguration(skDhtName, ZkUtils.getSilverKingZkConfig(zkEnv, Some(getSkRootZNode(skClusterName))))
  }

  def getHosts(
      zkClient: CuratorZkOperations = zkClient,
      ignoreSysLoc: Boolean,
      withExcluded: Boolean = false): Set[String] = {
    val excludedHosts = if (withExcluded) Set.empty[String] else getExcludedHosts(zkClient)
    def parseWithoutExcluded(node: Option[Node]) = {
      node.flatMap(n => {
        val remainingHosts = ipPattern.findAllIn(n.toStructuredString).toSet.diff(excludedHosts)
        if (remainingHosts.nonEmpty) Some(remainingHosts) else None
      })
    }

    def allUnexludedHosts =
      ipPattern.findAllIn(new String(zkClient.getData(ZNode(allHostsZkPath)))).toSet.diff(excludedHosts)

    val hosts = sysLoc match {
      case _ if ignoreSysLoc => allUnexludedHosts
      case None              => allUnexludedHosts
      case Some(SysLoc(building, city, continent)) =>
        val topologyBytes = zkClient.getData(ZNode(topologyZkPath))
        val topology = TopologyParser.parse(new ByteArrayInputStream(topologyBytes))
        def node(nc: NodeClass, id: String): Option[Node] =
          Option(topology.getNodesByClass(nc)).flatMap(_.asScala.find(n => n.getIDString == id))
        val hostsOpt = parseWithoutExcluded(node(Building, building)) orElse parseWithoutExcluded(
          node(Region, city)) orElse parseWithoutExcluded(node(Continent, continent)) orElse parseWithoutExcluded(
          Some(topology.getRoot))
        hostsOpt.getOrElse(throw new IllegalStateException("All host in cluster have been exluded"))
    }
    require(hosts.nonEmpty, "hosts should exist on silverking hostgroups or topology config")
    hosts
  }

  protected def getZkConnStrAndClient: (String, CuratorZkOperations) = {
    val zkContext = ZkUtils.getRootContext(zkEnv)
    val connStr = ZkClientUtils.getConnectionString(zkEnv, zkContext.getRootNode.substring(1))
    (connStr.substring(0, connStr.lastIndexOf('@')), new CuratorZkOperations(zkContext.getCurator))
  }

  def parseDhtCfg(zkClient: CuratorZkOperations = zkClient): DHTConfiguration = {
    val infoString = new String(zkClient.getData(ZNode(dhtConfigZkPath)))
    DHTConfiguration.parse(infoString, skVersion.toLong)
  }

  def getConnectionInfo(
      curatorZkOperations: CuratorZkOperations = zkClient,
      connStr: String = skZkConnectionString,
      ignoreSysLoc: Boolean = false,
      overrideHost: Option[String] = None): SilverKingConnectionInfo = {
    val cfg = parseDhtCfg(curatorZkOperations)
    val hosts = getHosts(curatorZkOperations, ignoreSysLoc).toSeq.sorted
    SilverKingConnectionInfo(
      skDhtName,
      overrideHost.collect { case h if hosts.contains(h) => Seq(h) }.getOrElse(hosts),
      cfg.getPort,
      s"$connStr/optimus/silverking/$skClusterName"
    )
  }

}
