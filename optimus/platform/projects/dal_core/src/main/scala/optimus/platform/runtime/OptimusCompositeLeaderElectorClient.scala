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
package optimus.platform.runtime

import java.io.IOException
import java.util.concurrent.CountDownLatch

import msjava.base.slr.internal.NotificationPolicies
import msjava.base.sr.ServiceAttributes.CommonKey
import msjava.hdom.Document
import msjava.hdom.Element
import msjava.msxml.xpath.MSXPathUtils
import msjava.slf4jutils.scalalog.getLogger
import msjava.zkapi.internal.ZkaContext
/* import msjava.zkapi.ZkaDirectoryWatcher
import msjava.zkapi.leader.LeaderSubscriber
import msjava.zkapi.leader.LeaderSubscriberListener
import msjava.zkapi.leader.internal.ZkaLeaderSubscriber */
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.config.OptimusConfigurationException
import optimus.dsi.cli.ZkStoreUtils
import optimus.graph.DiagnosticSettings
import optimus.platform.AdvancedUtils.timed
import optimus.platform.dal.config.DalEnv
import optimus.platform.dsi.DalZkCrumbSource
import optimus.platform.internal.SimpleStateHolder
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.ChildData
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener

import scala.collection.mutable
import scala.util.Failure
import scala.util.Random
import scala.util.Success
import scala.util.Try

final case class Brokers(writeBroker: Option[String], readBrokers: List[String])

final class LoadBalancingFeature {
  import LoadBalancingFeature._
  private[optimus] var value = default
}
object LoadBalancingFeature extends SimpleStateHolder(() => new LoadBalancingFeature) {
  val flag = "optimus.platform.runtime.optimusCompositeLeaderElectorClient.loadBalancingFeature"
  private def default = DiagnosticSettings.getBoolProperty(flag, true)

  def set(value: Boolean) = getState.synchronized { getState.value = value }
  def reset: Unit = getState.synchronized { getState.value = default }
  def value = getState.value
}

trait LeaderElectorClientListener {
  def update(brokers: Brokers): Unit
}

private[optimus] final case class BrokerData(address: String, score: Option[Double]) {
  val exhausted = UriUtils.queryContainsBooleanKey(address, "exhausted")
}

object OptimusCiSplitZkClientConstants {
  import optimus.dsi.cli.ZkPaths.ZK_SESSION_NODE

  // By convention, ci means to choose one of the ci envs (ci0, ci1, and so on)
  // indicated by hotpointer and coldpointer. dal_client should use hotpointer
  // when CI_HOT is true and coldpointer otherwise.
  val CiHotProperty: String = "CI_HOT"
  val HotPointerFilterEnvs: Set[DalEnv] = Set(DalEnv("ci"))
  val DsiNode: String = "/config/optimus/dsi"
  val HotPointerName: String = "hotpointer"
  val ColdPointerName: String = "coldpointer"
  val HotPointerNodeName: (DalEnv) => String = (env: DalEnv) => s"$ZK_SESSION_NODE/${env.underlying}/$HotPointerName"
}

object OptimusCompositeLeaderElectorClient extends ZkClientFactory[OptimusCompositeLeaderElectorClient] {
  import OptimusCiSplitZkClientConstants.CiHotProperty

  override protected type CacheKeyType = (String, ZkaContext, Boolean)

  private val log = getLogger(this)
  private val lock = new Object

  override def createClient(k: CacheKeyType): OptimusCompositeLeaderElectorClient = {
    val (instance, rootContext, shouldLoadBalance) = k
    OptimusCompositeLeaderElectorClient(
      instance,
      rootContext,
      DiagnosticSettings.getBoolProperty(CiHotProperty, false),
      shouldLoadBalance)
  }

  def getClient(instance: String, shouldLoadBalance: Boolean = false): OptimusCompositeLeaderElectorClient = {
    getClient(instance, ZkUtils.getRootContextForUriAuthority(instance), shouldLoadBalance)
  }

  def getClient(
      instance: String,
      rootContext: ZkaContext,
      shouldLoadBalance: Boolean): OptimusCompositeLeaderElectorClient = {
    getClient((instance, rootContext, shouldLoadBalance))
  }

  private[optimus /*platform*/ ] def apply(
      instance: String,
      rootContext: ZkaContext,
      ci_hot: Boolean,
      shouldLoadBalance: Boolean = false): OptimusCompositeLeaderElectorClient = {
    val client = new OptimusCompositeLeaderElectorClient(instance, rootContext, ci_hot, shouldLoadBalance)
    client.ensureInitialized()
    client
  }

  private val brokerProviderResolver = new BrokerProviderResolver {
    override def resolve(instance: String): BrokerProvider =
      getClient(instance, shouldLoadBalance = false)
    override def resolveWithLoadBalance(instance: String): BrokerProvider =
      getClient(instance, shouldLoadBalance = true)
  }

  def asBrokerProviderResolver: BrokerProviderResolver = brokerProviderResolver

  def getBrokerProviderResolverBasedOn(rootContext: ZkaContext): BrokerProviderResolver = new BrokerProviderResolver {
    override def resolve(instance: String): BrokerProvider =
      getClient(instance, rootContext, shouldLoadBalance = false)
    override def resolveWithLoadBalance(instance: String): BrokerProvider =
      getClient(instance, rootContext, shouldLoadBalance = true)
  }
}

/**
 * This class is a bridge from how we've traditionally done broker resolution via zookeeper to how we want to do it in
 * the future. It is able to read from both legacy and new-style locations. If a legacy instance is specified, it will
 * use the legacy data first. If, and only if, the legacy data is empty will it rely on the new style data.
 *
 * @param instance
 *   A string containing the instance name.
 */
class OptimusCompositeLeaderElectorClient protected (
    val instance: String,
    protected val rootContext: ZkaContext,
    private val ci_hot: Boolean,
    private val shouldLoadBalance: Boolean)
    extends ZkClient
    with BrokerProvider {
  import OptimusCiSplitZkClientConstants.HotPointerFilterEnvs
  import OptimusCompositeLeaderElectorClient.lock
  import OptimusCompositeLeaderElectorClient.log

  import scala.jdk.CollectionConverters._

  private[this] val env: DalEnv = {
    val e: DalEnv = DalEnv(instance)
    if (HotPointerFilterEnvs.contains(e)) {
      val zksu: ZkStoreUtils = new ZkStoreUtils(rootContext.getCurator, (s: String) => log.info(s))
      val data: Try[Document] = zksu.readFromZooKeeper(OptimusCiSplitZkClientConstants.HotPointerNodeName(e))
      data match {
        case Success(doc) =>
          log.info(s"found ${e.underlying} hotpointer node ${OptimusCiSplitZkClientConstants.HotPointerNodeName(e)}")
          val pointerName: String =
            if (ci_hot) OptimusCiSplitZkClientConstants.HotPointerName
            else OptimusCiSplitZkClientConstants.ColdPointerName
          // val compiledxPath = MSXPathUtils.compile(s"${OptimusCiSplitZkClientConstants.DsiNode}/$pointerName")
          val elements: List[Element] = ???
          val pointer: String = ???
          log.info(s"found pointer: $pointer")
          DalEnv(pointer)
        case Failure(ex) =>
          throw new OptimusConfigurationException(
            Some(s"Could not get hotpointer node for instance $instance."),
            Some(ex))
      }
    } else {
      e
    }
  }

  private[optimus /*platform*/ ] def getEnv: DalEnv = env

  // TODO (OPTIMUS-13179): Remove IgnoreExhaustedBrokers property once we are comfortable
  // with the feature of marking/unmarking exhausted brokers
  private[this] val ignoreExhaustedBrokers =
    DiagnosticSettings.getBoolProperty("optimus.dsi.ignoreExhaustedBrokers", true)

  private[this] val urlKey: String = ???
  private[this] val random = new Random

  private[this] val listeners = mutable.Set.empty[LeaderElectorClientListener]

  def addListener(listener: LeaderElectorClientListener, immediateSynchronization: Boolean): Unit = {
    lock synchronized {
      listeners += listener
    }
    if (immediateSynchronization) {
      lock synchronized {
        listener.update(getAllBrokers)
      }
    }
  }

  def removeListener(listener: LeaderElectorClientListener): Unit = {
    lock synchronized {
      listeners -= listener
    }
  }

  protected def getModernLePath(env: DalEnv): String =
    s"/${ZkUtils.modernZkLeaderElectionBaseNode}/${env.underlying}"

  private[this] val modernLePath = getModernLePath(env)
  private[this] val modernLeContext = ZkUtils.createZkaContext(rootContext, modernLePath, ensurePathCreated = false)

  private[this] var writeBrokerDataRaw: Option[BrokerData] = None
  private[this] def writeBrokerData: Option[BrokerData] =
    if (ignoreExhaustedBrokers) writeBrokerDataRaw.filter(!_.exhausted) else writeBrokerDataRaw
  private[this] var readBrokerData: List[BrokerData] = List.empty

  private[this] val initCountDownLatch = new CountDownLatch(1)

  /* private[this] val leaderSubscriberListener = new LeaderSubscriberListener {
    override def onLeaderUpdate(leaderSubscriber: LeaderSubscriber): Unit = update(leaderSubscriber)
    override def onInitialized(leaderSubscriber: LeaderSubscriber): Unit = update(leaderSubscriber)
    override def onCandidateUpdate(leaderSubscriber: LeaderSubscriber): Unit = update(leaderSubscriber)

    private[this] def update(leaderSubscriber: LeaderSubscriber): Unit = {
      lock synchronized {
        val iter = leaderSubscriber.getLeaders.iterator()
        val cl = if (iter.hasNext) {
          val leader = iter.next()
          require(!iter.hasNext, "There is more than one leader!")
          Some(leader)
        } else None

        val writer = cl.map(_.get(urlKey).toString)
        val readers = leaderSubscriber.getNonLeaders.asScala.map(_.get(urlKey).toString)

        writeBrokerDataRaw = writer.map(ClientBrokerDataParser.parseWriteBrokerData)
        readBrokerData = {
          val readBrokerDataRaw = ClientBrokerDataParser.parseReadBrokerData(readers.toList)
          if (ignoreExhaustedBrokers) readBrokerDataRaw.filter(d => !d.exhausted) else readBrokerDataRaw
        }

        initCountDownLatch.countDown()
        updateListeners()

        log.debug(s"modern ZK mode lead writer: $writeBrokerData")
        log.debug(s"modern ZK mode read brokers: ${readBrokerData.mkString(", ")}")
      }
    }
  } */

  private[this] var legacyWriteBrokerDataRaw: Option[BrokerData] = None
  private[this] def legacyWriteBrokerData: Option[BrokerData] =
    if (ignoreExhaustedBrokers) legacyWriteBrokerDataRaw.filter(!_.exhausted) else legacyWriteBrokerDataRaw
  private[this] var legacyReadBrokerData: List[BrokerData] = List.empty

  private[this] val legacyListener: PathChildrenCacheListener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      val nodes: List[ChildData] = ???

      val writer = if (nodes.isEmpty) "" else new String(nodes.head.getData)
      val readers = if (nodes.isEmpty) nodes else nodes.drop(1)

      lock synchronized {
        legacyWriteBrokerDataRaw =
          if (writer.isEmpty) None else Some(ClientBrokerDataParser.parseWriteBrokerData(writer))

        legacyReadBrokerData = {
          val readBrokerAddresses = readers.map(r => new String(r.getData))
          val legacyReadBrokerDataRaw = ClientBrokerDataParser.parseReadBrokerData(readBrokerAddresses)
          if (ignoreExhaustedBrokers) legacyReadBrokerDataRaw.filter(!_.exhausted) else legacyReadBrokerDataRaw
        }

        initCountDownLatch.countDown()
        updateListeners()

        log.debug(s"legacy ZK mode lead writer: $legacyWriteBrokerData")
        log.debug(s"legacy ZK mode read brokers: ${legacyReadBrokerData.mkString(", ")}")
      }
    }
  }

  private[this] def updateListeners(): Unit = {
    lock synchronized {
      listeners.foreach { _.update(getAllBrokers) }
    }
  }

  def ensureInitialized(): Unit = {
    // since we already wait in the ctor for ZK watcher to initialize itself, we expect 'initialized' state
    // will be very soon without further IO
    initCountDownLatch.await()
  }

  // A custom sorter for elections nodes.
  // The path for the election znodes in zk (using LeaderLatch) has the following pattern:
  // _c_$UUID-$session_id-latch-$sequence_number, i.e.:
  // _c_f261bcda-2df8-4bf0-b67e-063205c27182-latch-0000000000
  // '_c_' and the 'latch' are hard coded identifiers in LeaderLatch
  //
  // Furthermore, this sorter is compatible with the current election node pattern
  // which allows for N out of M nodes in a cluster to be a leader at any time. In
  // the case of the DAL there will only ever be one leader.
  private[this] val legacyElectionNodeSorter: (ChildData, ChildData) => Boolean = { (node1, node2) =>
    val path1 = node1.getPath
    val path2 = node2.getPath
    path1.substring(path1.length - 10).toLong < path2.substring(path2.length - 10).toLong
  }

  private[this] val legacyElectionNodeFilter: ChildData => Boolean = { node =>
    ZkUtils.legacyElectionNodeFilter(node.getPath)
  }

  private def legacyGetBrokersBaseNode(mode: String): String = {
    val topNode = ZkUtils.legacyZkLeaderElectionBaseNode
    if (ZkUtils.isTestMode(mode)) {
      s"/$topNode/test/$mode"
    } else {
      s"/$topNode/$mode"
    }
  }

  private[this] lazy val legacyBrokerBaseNode: String = legacyGetBrokersBaseNode(env.underlying)

  /* private[this] val (leaderSubscriber, zkaDirectoryWatcher) = {
    if (rootContext.exists(modernLePath, "/semaphore/locks") ne null)
      (
        Some(new ZkaLeaderSubscriber(modernLeContext, leaderSubscriberListener, NotificationPolicies.PASS_ALL_POLICY)),
        None)
    else (None, Some(new ZkaDirectoryWatcher(rootContext, legacyBrokerBaseNode, legacyListener)))
  } */

  /* val (zkaDirectoryWatcherTime, _): (Long, Unit) = timed {
    if (
      zkaDirectoryWatcher.isDefined && !ZkUtils.waitZdwForInitialization(
        zkaDirectoryWatcher.get,
        legacyBrokerBaseNode,
        rootContext,
        ZkUtils.watcherTimeout)
    ) {
      try {
        zkaDirectoryWatcher.foreach(_.close())
      } catch {
        case ex: Exception =>
          log.error("Error closing ZkaDirectoryWatcher", ex)
      }
      throw new IOException(
        s"Timed out on initializing directory watcher for $legacyBrokerBaseNode. " +
          s"Error is fatal, cannot resolve read or write brokers.")
    }
  } */
  /* Breadcrumbs.info(
    ChainedID.root,
    new PropertiesCrumb(_, DalZkCrumbSource, ("DalClientZkaDirectoryWatcher", zkaDirectoryWatcherTime.toString))) */

  val (leaderSubscriberTime, _): (Long, Unit) = ???
  Breadcrumbs.info(
    ChainedID.root,
    new PropertiesCrumb(_, DalZkCrumbSource, ("DalClientLeaderSubscriber", leaderSubscriberTime.toString)))

  override def close(): Unit = {
    // zkaDirectoryWatcher foreach { _.close() }
    // leaderSubscriber foreach { _.close() }
  }

  private def obtainBiasedRandomBrokerFromList(brokersList: List[BrokerData]): BrokerData = {
    val zeroScoreBrokerList = brokersList.filter(_.score.getOrElse(Double.MaxValue) <= Double.MinPositiveValue)
    brokersList.foreach(a => {
      log.debug(s"address : ${a.address} , score : ${a.score.getOrElse(0.0d)} :score is empty? : ${!a.score.isDefined}")
    })
    // improvised logic to pick random from score of '0'
    if (zeroScoreBrokerList.nonEmpty) {
      zeroScoreBrokerList(random.nextInt(zeroScoreBrokerList.size))
    } else {
      val cumulativeWeightList: Seq[Double] = brokersList
        .scanLeft(0.0)((sumSoFar, brokerData) => sumSoFar + (1.0 / brokerData.score.get))
        .drop(1) // since we want less loaded brokers,  weight = 1/load and then generate a cumulative weight list
      val biasedRand: Double = random
        .nextDouble() * cumulativeWeightList.last // gives a value in the range [0.0, cumulativeWeightList.last)
      val pickedIndex = cumulativeWeightList.indexWhere(
        biasedRand < _
      ) // find the first index where the weight sum is less than the biasedRandom number, because the larger the weight the larger the chance of getting picked
      brokersList(pickedIndex)
    }
  }

  private[optimus] def randomFromList(brokersList: List[BrokerData]): String = {
    require(brokersList.nonEmpty, "Input broker list for random broker selection cannot be empty")
    if (shouldLoadBalance && LoadBalancingFeature.value && brokersList.forall(data => data.score.isDefined)) {
      log.info("Using biased randomization for selecting Read Broker")
      obtainBiasedRandomBrokerFromList(brokersList).address
    } else {
      log.info("Using unbiased randomization for selecting Read Broker")
      brokersList(random.nextInt(brokersList.size)).address
    }
  }

  def getReadBrokers: List[String] = getReadBrokersData.map(_.address)

  def withReadBroker[T](f: Option[String] => T): T = {
    lock synchronized {
      val readBrokers = getReadBrokersData.filter(_.address.nonEmpty)
      val broker = if (readBrokers.isEmpty) None else Some(randomFromList(readBrokers))
      f(broker)
    }
  }

  def getReadBroker: Option[String] = withReadBroker(identity)

  def withWriteBroker[T](f: Option[String] => T): T = {
    lock synchronized {
      f(getWriteBrokerData.map(_.address))
    }
  }

  def getWriteBroker: Option[String] = withWriteBroker(identity)

  def getWriteBrokerRaw: Option[String] = getWriteBrokerDataRaw.map(_.address)

  def withRandomBroker[T](f: Option[String] => T): T = {
    lock synchronized {
      val brokersWithData = getAllBrokersWithData
      val broker = if (brokersWithData.isEmpty) None else Some(randomFromList(brokersWithData))
      f(broker)
    }
  }

  def getRandomBroker: Option[String] = withRandomBroker(identity)

  def getAllBrokers: Brokers = {
    lock synchronized {
      val writeBroker = getWriteBroker
      val readBrokers = getReadBrokers

      Brokers(writeBroker, readBrokers)
    }
  }

  private def getWriteBrokerDataRaw: Option[BrokerData] = {
    lock synchronized {
      if (legacyWriteBrokerDataRaw.exists(_.address.nonEmpty)) legacyWriteBrokerDataRaw
      else if (writeBrokerDataRaw.exists(_.address.nonEmpty)) writeBrokerDataRaw
      else None
    }
  }

  def getWriteBrokerData: Option[BrokerData] = {
    lock synchronized {
      if (legacyWriteBrokerData.exists(_.address.nonEmpty)) legacyWriteBrokerData
      else if (writeBrokerData.exists(_.address.nonEmpty)) writeBrokerData
      else None
    }
  }

  def getReadBrokersData: List[BrokerData] = {
    lock synchronized {
      if (legacyReadBrokerData.nonEmpty) legacyReadBrokerData
      else if (legacyWriteBrokerData.exists(_.address.nonEmpty)) legacyWriteBrokerData.toList
      else if (readBrokerData.nonEmpty) readBrokerData
      else if (writeBrokerData.exists(_.address.nonEmpty)) writeBrokerData.toList
      else Nil
    }
  }

  def getAllBrokersWithData: List[BrokerData] = lock synchronized {
    (getWriteBrokerData.toList ++ getReadBrokersData).filter(_.address.nonEmpty)
  }

  def getBrokerNodesAndData: List[(String, String)] = {
    try {
      /* zkaDirectoryWatcher map { zkaDirWatcher =>
        zkaDirWatcher.getCurrentData.asScala.toList
          .filter(legacyElectionNodeFilter)
          .sortWith(legacyElectionNodeSorter) map { node =>
          (node.getPath, new String(node.getData))
        }
      } getOrElse {
        leaderSubscriber map { ls =>
          val allCandidates = (ls.getLeaders.asScala ++ ls.getNonLeaders.asScala).toList
          val allNodesAndData = allCandidates map (node =>
            (s"${rootContext.getRootNode}${modernLeContext.getBasePath}", node.asScala.getOrElse(urlKey, "").toString))
          allNodesAndData
        } getOrElse Nil
      } */
      Nil
    } catch {
      case e: Exception =>
        OptimusCompositeLeaderElectorClient.log
          .warn(s"Could not obtain the brokers under [${env.underlying}] from ZooKeeper: ", e)
        Nil
    }
  }

  def isExhaustedNode(data: String): Boolean = UriUtils.queryContainsBooleanKey(data, "exhausted")

}

class DalServiceDiscoveryLeaderElectorClient private (
    instance: String,
    leName: String,
    rootContext: ZkaContext,
    ciHot: Boolean)
    extends OptimusCompositeLeaderElectorClient(instance, rootContext, ciHot, false) {
  // The place at which modern LE registration happens, in our case something like dalServices/<mode>/discovery/<name>
  // See ZkUtils.getServiceDiscoveryLeaderElectionBaseNode.
  override protected def getModernLePath(env: DalEnv): String =
    ZkUtils.getServiceDiscoveryLeaderElectionBaseNode(env.mode, leName)
}

object DalServiceDiscoveryLeaderElectorClient extends ZkClientFactory[DalServiceDiscoveryLeaderElectorClient] {
  override protected type CacheKeyType = (String, String, ZkaContext)

  override def createClient(k: (String, String, ZkaContext)): DalServiceDiscoveryLeaderElectorClient = {
    val (instance, leName, rootContext) = k
    val client = new DalServiceDiscoveryLeaderElectorClient(
      instance,
      leName,
      rootContext,
      DiagnosticSettings.getBoolProperty(OptimusCiSplitZkClientConstants.CiHotProperty, false))
    client.ensureInitialized()
    client
  }

  def getClient(instance: String, leName: String): DalServiceDiscoveryLeaderElectorClient = {
    getClient((instance, leName, ZkUtils.getRootContextForUriAuthority(instance)))
  }
}
