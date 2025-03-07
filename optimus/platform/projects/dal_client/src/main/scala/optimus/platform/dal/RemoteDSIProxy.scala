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
package optimus.platform.dal

import msjava.slf4jutils.scalalog.getLogger
import optimus.core.CoreAPI.asyncResult
import optimus.dsi.partitioning.Partition
import optimus.dsi.partitioning.PartitionMap
import optimus.dsi.session.EstablishSession
import optimus.dsi.session.EstablishedClientSession
import optimus.graph.DiagnosticSettings
import optimus.platform.dal.DALPubSub.ClientStreamId
import optimus.platform._
import optimus.platform.dal.config.DalAppId
import optimus.platform.dal.config.DalAsyncConfig
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.DalZoneId
import optimus.platform.dal.pubsub.PubSubClientHandler
import optimus.platform.dsi.SupportedFeatures
import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.protobufutils.DALProtoClient
import optimus.platform.dsi.protobufutils.DalBrokerClient
import optimus.platform.dsi.protobufutils.PubSubProtoClient
import optimus.platform.runtime.BrokerProvider
import optimus.platform.runtime.BrokerProviderResolver
import optimus.platform.runtime.Brokers
import optimus.platform.runtime.LeaderElectorClientListener
import optimus.platform.runtime.OptimusCompositeLeaderElectorClient

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable.ArrayBuffer

abstract class AbstractRemoteDSIProxy(ctx: ClientBrokerContext) extends DSIClient(ctx) {
  protected def createBrokerClient(): DalBrokerClient

  override def getSender(): DalBrokerClient = {
    val protoClient = createBrokerClient()
    protoClient.start()
    protoClient
  }
}

object AbstractRemoteDSIProxyWithLeaderElector {
  private val log = getLogger(this)
}

abstract class AbstractRemoteDSIProxyWithLeaderElector(
    ctx: ClientBrokerContext,
    val leaderElectorClient: BrokerProvider)
    extends AbstractRemoteDSIProxy(ctx)
    with LeaderElectorClientListener {

  protected[this] var connStr: Option[String] = None

  leaderElectorClient.addListener(this)

  final override def update(brokers: Brokers): Unit = {
    this synchronized {
      if (connStr.nonEmpty && shouldCloseClient(brokers)) {
        AbstractRemoteDSIProxyWithLeaderElector.log.info(
          "Closing broker client due to remote broker update causing a mismatch.")
        close(false) // false; close the dal connection and not the full dsi
        connStr = None
      }
    }
  }

  protected def shouldCloseClient(brokers: Brokers): Boolean

  override def close(shutdown: Boolean): Unit = {
    super.close(shutdown)
    if (shutdown) leaderElectorClient.removeListener(this)
  }
}

class RemoteDSIProxy(
    ctx: ClientBrokerContext,
    host: String,
    port: Int,
    kerberos: Boolean,
    override val partitionMap: PartitionMap,
    secureTransport: Boolean)
    extends AbstractRemoteDSIProxy(ctx) {
  override protected def createBrokerClient() =
    new DALProtoClient(
      s"$host:$port",
      kerberos,
      ctx.clientContext,
      requestLimiter,
      secureTransport,
      writeRequestLimiter,
      batchRetryManager,
      Some(new DALProtoClient.Configuration(ctx.asyncConfig))
    )
}

class AwsDsiProxy(ctx: ClientBrokerContext, kerberos: Boolean, override val partitionMap: PartitionMap = PartitionMap())
    extends AbstractRemoteDSIProxy(ctx) {
  override protected def createBrokerClient() =
    new DALProtoClient(
      ctx.broker,
      kerberos,
      ctx.clientContext,
      requestLimiter,
      false,
      writeRequestLimiter,
      batchRetryManager,
      Some(new DALProtoClient.Configuration(ctx.asyncConfig))
    )
}

class LeaderRemoteDSIProxy(
    ctx: ClientBrokerContext,
    brokerProvider: BrokerProvider,
    servingPartition: Partition,
    override val partitionMap: PartitionMap,
    val secureTransport: Boolean)
    extends AbstractRemoteDSIProxyWithLeaderElector(ctx.leadCtx, brokerProvider) {
  def this(
      ctx: ClientBrokerContext,
      brokerProviderResolver: BrokerProviderResolver,
      servingPartition: Partition,
      partitionMap: PartitionMap,
      secureTransport: Boolean) =
    this(ctx, brokerProviderResolver.resolve(ctx.broker), servingPartition, partitionMap, secureTransport)
  override protected def createBrokerClient(): DalBrokerClient = leaderElectorClient.withWriteBroker { writeBroker =>
    this synchronized {
      connStr = Some(
        writeBroker.getOrElse(
          throw new DALRetryableActionException(s"No write broker registered of type: ${ctx.broker}", None)))
      DALProtoClient(
        connStr.get,
        ctx.clientContext,
        requestLimiter,
        batchRetryManager,
        Some(new DALProtoClient.Configuration(ctx.asyncConfig, batchRetryManagerOpt = batchRetryManager)),
        Some(servingPartition),
        secureTransport,
        writeRequestLimiter
      )
    }
  }

  override protected def createDALRetryEvent(t: Exception): DALRetryEvent = new DALLeaderRetryEvent(t, ctx.broker)

  override protected def shouldCloseClient(brokers: Brokers): Boolean = {
    val writeBroker = brokers.writeBroker
    if (writeBroker.isDefined) {
      connStr.isDefined && writeBroker.get != connStr.get
    } else connStr.isDefined
  }
}

class ReadRemoteDSIProxy(
    ctx: ClientBrokerContext,
    brokerProvider: BrokerProvider,
    override val partitionMap: PartitionMap,
    val secureTransport: Boolean)
    extends AbstractRemoteDSIProxyWithLeaderElector(ctx, brokerProvider) {
  def this(
      ctx: ClientBrokerContext,
      brokerProviderResolver: BrokerProviderResolver,
      partitionMap: PartitionMap,
      shouldLoadBalance: Boolean,
      secureTransport: Boolean) =
    this(
      ctx,
      if (shouldLoadBalance) brokerProviderResolver.resolveWithLoadBalance(ctx.broker)
      else brokerProviderResolver.resolve(ctx.broker),
      partitionMap,
      secureTransport
    )
  override protected def createBrokerClient(): DalBrokerClient = leaderElectorClient.withReadBroker { readBroker =>
    this synchronized {
      connStr = Some(readBroker getOrElse {
        throw new DALRetryableActionException(s"No broker registered of type: ${ctx.broker}", None)
      })
      DALProtoClient(
        connStr.get,
        ctx.clientContext,
        requestLimiter,
        batchRetryManager,
        Some(new DALProtoClient.Configuration(ctx.asyncConfig, batchRetryManagerOpt = batchRetryManager)),
        None,
        secureTransport,
        writeRequestLimiter
      )
    }
  }

  override protected def createDALRetryEvent(t: Exception): DALRetryEvent = new DALReadOnlyRetryEvent(t, ctx.broker)

  override protected def shouldCloseClient(brokers: Brokers): Boolean = {
    val writeBroker = brokers.writeBroker
    val readBrokers = brokers.readBrokers
    writeBroker.isDefined && connStr.isDefined && !readBrokers.contains(connStr.get)
  }
}

class RandomRemoteDSIProxy(
    ctx: ClientBrokerContext,
    brokerProvider: BrokerProvider,
    override val partitionMap: PartitionMap,
    val secureTransport: Boolean)
    extends AbstractRemoteDSIProxyWithLeaderElector(ctx, brokerProvider) {
  def this(
      ctx: ClientBrokerContext,
      brokerProviderResolver: BrokerProviderResolver,
      partitionMap: PartitionMap,
      shouldLB: Boolean,
      secureTransport: Boolean) =
    this(
      ctx,
      if (shouldLB) brokerProviderResolver.resolveWithLoadBalance(ctx.broker)
      else brokerProviderResolver.resolve(ctx.broker),
      partitionMap,
      secureTransport)
  override protected def createBrokerClient(): DalBrokerClient = leaderElectorClient.withRandomBroker { randomBroker =>
    this synchronized {
      connStr = Some(randomBroker getOrElse {
        throw new DALRetryableActionException(s"No broker registered of type: ${ctx.broker}", None)
      })
      DALProtoClient(
        connStr.get,
        ctx.clientContext,
        requestLimiter,
        batchRetryManager,
        Some(new DALProtoClient.Configuration(ctx.asyncConfig, batchRetryManagerOpt = batchRetryManager)),
        None,
        secureTransport,
        writeRequestLimiter
      )
    }
  }

  override protected def shouldCloseClient(brokers: Brokers): Boolean = {
    val writeBroker = brokers.writeBroker
    val readBrokers = brokers.readBrokers
    writeBroker.isDefined && connStr.isDefined &&
    writeBroker.get != connStr.get && !readBrokers.contains(connStr.get)
  }

}

class MultiReadBrokersDSIProxy(
    val baseContext: Context,
    val brokerProviderResolver: BrokerProviderResolver,
    val replicaBroker: String,
    val partitionMap: PartitionMap,
    val zone: DalZoneId,
    val appId: DalAppId,
    val cmdLimit: Int,
    val asyncConfig: DalAsyncConfig,
    val env: DalEnv,
    val shouldLoadBalance: Boolean,
    val secureTransport: Boolean,
    val maxBrokersAllowedForConnectionFromURI: Int = 1)
    extends ClientSideDSI {

  private val log = getLogger(this)

  lazy val brokerProvider = brokerProviderResolver.resolve(replicaBroker)

  lazy val fixedReadRemoteDSIProxyList: Option[FixedReadRemoteDSIProxyList] = {
    if (maxBrokersAllowedForConnectionFromURI > 1) {
      Some(
        new FixedReadRemoteDSIProxyList(
          brokerProvider,
          maxBrokersAllowedForConnectionFromURI,
          ClientBrokerContext(
            baseContext,
            replicaBroker,
            zone,
            appId,
            cmdLimit,
            asyncConfig,
            brokerVirtualHostname(env)),
          partitionMap,
          secureTransport,
          shouldLoadBalance
        ))
    } else None
  }

  lazy val fixedReplica = new RandomRemoteDSIProxy(
    ClientBrokerContext(baseContext, replicaBroker, zone, appId, cmdLimit, asyncConfig, brokerVirtualHostname(env)),
    brokerProviderResolver,
    partitionMap,
    shouldLoadBalance,
    secureTransport
  )

  protected[optimus] def replica = {
    if (maxBrokersAllowedForConnectionFromURI == 1) {
      fixedReplica
    } else {
      fixedReadRemoteDSIProxyList.get.chosenReplica
    }
  }

  private[optimus] def getDSIClient = {
    replica
  }

  private[optimus] def connectionSession(): Option[EstablishedClientSession] =
    getDSIClient.sessionCtx.connectionSession()

  @async def executeReplicaReadOnlyCommands(
      chosenReplica: DSIClient,
      replicaCmds: Seq[ReadOnlyCommand],
      attempts: Int): Seq[Result] = {
    asyncResult(chosenReplica.executeReadOnlyCommands(replicaCmds)) match {
      case NodeFailure(ex) =>
        if (ex.isInstanceOf[DalBrokerClientInitializedException]) {
          log.error(
            s"Got exception with read broker - ${chosenReplica}, removing from the list of connected read brokers")

          fixedReadRemoteDSIProxyList.get.removeReplicaFromAvailableConnections(chosenReplica)

          if (attempts > 0 && fixedReadRemoteDSIProxyList.get.getAvailableConnections.size > 0) {
            log.info("Choosing different read broker to serve this request")
            executeReplicaReadOnlyCommands(replica, replicaCmds, attempts - 1)
          } else {
            log.error("Either max retry is exhausted or no brokers are available to serve this request")
            throw ex
          }
        } else
          throw ex
      case NodeSuccess(results) => results
    }
  }

  @async def executeRoundRobinReadOnlyCommands(replicaCmds: Seq[ReadOnlyCommand]): Seq[Result] = {
    val chosenReplica = replica.asInstanceOf[FixedReadRemoteDSIProxy]
    executeReplicaReadOnlyCommands(chosenReplica, replicaCmds, chosenReplica.maxRetryCount)
  }

  @async override def executeReadOnlyCommands(replicaCmds: Seq[ReadOnlyCommand]): Seq[Result] = {
    if (maxBrokersAllowedForConnectionFromURI > 1) {
      executeRoundRobinReadOnlyCommands(replicaCmds)
    } else replica.executeReadOnlyCommands(replicaCmds)
  }

  @async override def executeLeadWriterCommands(cmds: Seq[LeadWriterCommand]): Seq[Result] = {
    throw new UnsupportedOperationException(s"LeadWriterCommands are not supported!")
  }

  override def sessionData: optimus.platform.dal.session.ClientSessionContext.SessionData =
    replica.sessionCtx.sessionData

  override private[optimus] def bindSessionFetcher(sessionFetcher: SessionFetcher): Unit = {
    if (fixedReadRemoteDSIProxyList.isDefined) {
      fixedReadRemoteDSIProxyList.get.bindSessionFetcher(sessionFetcher)
    } else { fixedReplica.bindSessionFetcher(sessionFetcher) }
  }

  override protected[optimus] def close(shutdown: Boolean): Unit = {
    if (fixedReadRemoteDSIProxyList.isDefined) {
      fixedReadRemoteDSIProxyList.get.getAvailableConnections.foreach(replica => replica.close(shutdown))
    } else fixedReplica.close(shutdown)
  }

  override private[optimus] def setEstablishSession(establishSession: => EstablishSession): Unit = {
    if (fixedReadRemoteDSIProxyList.isDefined) {
      fixedReadRemoteDSIProxyList.get.setEstablishSession(establishSession)
    } else { fixedReplica.setEstablishSession(establishSession) }
  }

  override protected[optimus] def serverFeatures(): SupportedFeatures = replica.serverFeatures()
}

class DalBrokerClientInitializedException(message: String, cause: Throwable, client: Option[DalBrokerClient])
    extends DALNonRetryableActionException(message, cause, client) {
  def this(msg: String, client: Option[DalBrokerClient]) = this(msg, null, client)
}

class FixedReadRemoteDSIProxy(
    ctx: ClientBrokerContext,
    brokerProvider: BrokerProvider,
    override val partitionMap: PartitionMap,
    val secureTransport: Boolean,
    readBroker: Option[String],
    shouldLB: Boolean
) extends AbstractRemoteDSIProxyWithLeaderElector(ctx, brokerProvider) {
  val maxRetryCount = retryAttempts
  var initializedBefore = false
  def this(
      ctx: ClientBrokerContext,
      brokerProviderResolver: BrokerProviderResolver,
      partitionMap: PartitionMap,
      secureTransport: Boolean,
      readBroker: Option[String],
      shouldLB: Boolean) =
    this(
      ctx,
      if (shouldLB) brokerProviderResolver.resolveWithLoadBalance(ctx.broker)
      else brokerProviderResolver.resolve(ctx.broker),
      partitionMap,
      secureTransport,
      readBroker,
      shouldLB
    )
  override protected def createBrokerClient(): DalBrokerClient = {
    this synchronized {
      if (initializedBefore)
        throw new DalBrokerClientInitializedException(
          "This Broker Client has already been initialized before, will retry with other broker client",
          None)

      initializedBefore = true
      connStr = Some(readBroker getOrElse {
        throw new DALRetryableActionException(s"No broker registered of type: ${ctx.broker}", None)
      })

      DALProtoClient(
        connStr.get,
        ctx.clientContext,
        requestLimiter,
        batchRetryManager,
        Some(new DALProtoClient.Configuration(ctx.asyncConfig, batchRetryManagerOpt = batchRetryManager)),
        None,
        secureTransport,
        writeRequestLimiter
      )
    }
  }

  override protected def shouldCloseClient(brokers: Brokers): Boolean = {
    val writeBroker = brokers.writeBroker
    val readBrokers = brokers.readBrokers
    writeBroker.isDefined && connStr.isDefined &&
    writeBroker.get != connStr.get && !readBrokers.contains(connStr.get)
  }

  def getReadBrokerConnectionString: Option[String] = readBroker

}

class FixedReadRemoteDSIProxyList(
    brokerProvider: BrokerProvider,
    maxBrokersAllowedForConnectionFromURI: Int = 1,
    val ctx: ClientBrokerContext,
    val partitionMap: PartitionMap,
    secureTransport: Boolean,
    shouldLB: Boolean) {

  private val log = getLogger(this)

  lazy val readRemoteDSIProxyList = {
    brokerProvider.withMultiReadBrokers(maxBrokersAllowedForConnectionFromURI) { readBroker =>
      {
        new FixedReadRemoteDSIProxy(ctx, brokerProvider, partitionMap, secureTransport, readBroker, shouldLB)
      }
    }
  }

  private lazy val availableConnections: ArrayBuffer[DSIClient] = ArrayBuffer(readRemoteDSIProxyList: _*)

  def addReplicaToAvailableConnections(replica: DSIClient): Unit = this synchronized {
    availableConnections += replica
  }

  def removeReplicaFromAvailableConnections(replica: DSIClient): Unit = {
    this synchronized {
      availableConnections -= replica
    }
    electionClientListener.update(brokerProvider.asInstanceOf[OptimusCompositeLeaderElectorClient].getAllBrokers)
  }

  def getAvailableConnections = this synchronized {
    availableConnections.toSeq
  }

  private var roundRobinIndex = -1

  def chosenReplica = {
    this synchronized {
      roundRobinIndex = roundRobinIndex + 1
      getAvailableConnections(roundRobinIndex % getAvailableConnections.size)
    }
  }

  val establishedSession: AtomicReference[EstablishSession] = new AtomicReference[EstablishSession]()
  val establishedSessionFetcher: AtomicReference[SessionFetcher] = new AtomicReference[SessionFetcher]()

  val electionClientListener = new LeaderElectorClientListener {
    override def update(brokers: Brokers): Unit = {
      val listOfAllBrokers: List[String] = brokers.readBrokers ++ List(brokers.writeBroker.getOrElse(""))
      val currentConnections = getAvailableConnections
      if (
        maxBrokersAllowedForConnectionFromURI > currentConnections.size && currentConnections.size < listOfAllBrokers.size
      ) {
        val currentConnectedBrokers = currentConnections.map(conn =>
          conn
            .asInstanceOf[FixedReadRemoteDSIProxy]
            .getReadBrokerConnectionString
            .getOrElse({
              log.error(
                s"No broker registered for ${conn} connection, removing from the current connected brokers list")
              removeReplicaFromAvailableConnections(conn)
            }))

        val availableBrokersToConnect = listOfAllBrokers.filter(currentBroker => {
          !currentConnectedBrokers.contains(currentBroker)
        })

        val numBrokersDeficit = maxBrokersAllowedForConnectionFromURI - currentConnectedBrokers.size
        val numBrokersToConnect =
          if (availableBrokersToConnect.size < numBrokersDeficit) availableBrokersToConnect.size
          else numBrokersDeficit

        if (numBrokersToConnect == 0)
          log.info(
            s"No more brokers to connect or ${maxBrokersAllowedForConnectionFromURI} number of brokers are already connected as required")
        else {
          val brokersListToConnect = availableBrokersToConnect.take(numBrokersToConnect)
          log.info(
            s"Creating ${numBrokersToConnect} more read broker connection(s) as maxBrokersAllowedForConnectionFromURI value is greater than the current connections and read brokers are available")
          val newConnections = brokersListToConnect.map(broker => {
            new FixedReadRemoteDSIProxy(ctx, brokerProvider, partitionMap, secureTransport, Some(broker), shouldLB)
          })
          newConnections.foreach(conn => {
            conn.setEstablishSession(establishedSession.get())
            conn.bindSessionFetcher(establishedSessionFetcher.get())
            addReplicaToAvailableConnections(conn)
          })
        }
      }
    }
  }

  brokerProvider.addListener(electionClientListener, immediateSynchronization = true)

  private[optimus] def setEstablishSession(command: EstablishSession): Unit = {
    establishedSession.set(command)
    readRemoteDSIProxyList.foreach(_.setEstablishSession(command))
  }

  private[optimus] def bindSessionFetcher(sessionFetcher: SessionFetcher): Unit = {
    establishedSessionFetcher.set(sessionFetcher)
    readRemoteDSIProxyList.foreach(_.bindSessionFetcher(sessionFetcher))
  }
}

object PubSubDsiProxy {
  private val OperationRetryAttempts: Int =
    DiagnosticSettings.getIntProperty(
      "optimus.dsi.pubsub.retryAttempts",
      DiagnosticSettings.getIntProperty("optimus.dsi.retryAttempts", 720))
  private val OperationRetryBackoffTime: Long =
    DiagnosticSettings.getLongProperty(
      "optimus.dsi.pubsub.retryBackoffTimeMs",
      DiagnosticSettings.getLongProperty("optimus.dsi.retryBackoffTimeMs", 1000L))
  def shouldCloseIfConnStrIsNoLongerInLeaderOrFollower(
      connStr: Option[String],
      leader: Option[String],
      followers: List[String]): Boolean = {
    connStr exists { s =>
      leader.forall(_ != s) && !followers.contains(s)
    }
  }
}

class PubSubDsiProxy(
    ctx: ClientBrokerContext,
    brokerProvider: BrokerProvider,
    override val partitionMap: PartitionMap,
    val secureTransport: Boolean)
    extends AbstractRemoteDSIProxyWithLeaderElector(ctx, brokerProvider)
    with PubSubConnectionInternals {
  def this(
      ctx: ClientBrokerContext,
      brokerProviderResolver: BrokerProviderResolver,
      partitionMap: PartitionMap,
      secureTransport: Boolean) =
    this(ctx, brokerProviderResolver.resolve(ctx.broker), partitionMap, secureTransport)

  private[optimus] def getPubSubStreamManager: DsiClientPubSubStreamRetryManager = {
    require(pubSubRetryManager.nonEmpty)
    pubSubRetryManager.get
  }

  override def getRetryAttempts: Int = PubSubDsiProxy.OperationRetryAttempts
  override def getRetryBackoffTime: Long = PubSubDsiProxy.OperationRetryBackoffTime
  override protected def createBrokerClient(): DalBrokerClient = leaderElectorClient.withRandomBroker { randomBroker =>
    this synchronized {
      connStr = Some(randomBroker getOrElse {
        throw new DALRetryableActionException(s"No broker registered of type: ${ctx.broker}", None)
      })
      PubSubProtoClient(
        connStr.get,
        ctx.clientContext,
        Some(new DALProtoClient.Configuration(ctx.asyncConfig, name = "PubSubProtoClient")),
        pubSubClientHandler,
        secureTransport)
    }
  }

  // Exposed for tests
  protected val pubSubClientHandler: PubSubClientHandler = new PubSubClientHandler(
    sessionCtx,
    pubSubRetryManager.get.handlePubSubResult,
    pubSubRetryManager.get.handlePubSubException,
    pubSubRetryManager.get.checkStreamExists,
    pubSubRetryManager.get.remoteStreamCleanup,
    () => pubSubRetryManager.get.onConnect()
  )

  override protected def shouldCloseClient(brokers: Brokers): Boolean = {
    val leader = brokers.writeBroker
    val followers = brokers.readBrokers
    PubSubDsiProxy.shouldCloseIfConnStrIsNoLongerInLeaderOrFollower(connStr, leader, followers)
  }

  override protected def getPubSubRetryManager(
      retryAttempts: ClientStreamId,
      retryBackoffTime: Long): Option[DsiClientPubSubStreamRetryManager] =
    Some(new DsiClientPubSubStreamRetryManager(retryAttempts, retryBackoffTime))

  // Exposed for tests
  private[optimus] def getServerSideIdFromClientId(id: ClientStreamId) = {
    require(pubSubRetryManager.isDefined, "pubSubRetryMgr should be defined in PubSubDsiProxy")
    pubSubRetryManager.get.getServerSideIdFromClientId(id)
  }
}

trait PubSubConnectionInternals { dsiClient: DSIClient =>

  private[optimus] def enableReading: Unit = {
    getCurrentSender() match {
      case client: PubSubProtoClient => client.enableReading()
      case _                         => // do nothing
    }
  }

  private[optimus] def disableReading: Unit = {
    getCurrentSender() match {
      case client: PubSubProtoClient => client.disableReading()
      case _                         => // do nothing
    }
  }

  private[optimus] def disconnectConnection: Unit = {
    getCurrentSender() match {
      case client: PubSubProtoClient => client.disconnectConnection()
      case _                         => // do nothing
    }
  }
}

class PubSubTcpDsiProxy(
    ctx: ClientBrokerContext,
    host: String,
    port: Int,
    kerberos: Boolean,
    override val partitionMap: PartitionMap,
    val secureTransport: Boolean)
    extends AbstractRemoteDSIProxy(ctx)
    with PubSubConnectionInternals {

  override protected def createBrokerClient() =
    new PubSubProtoClient(
      s"$host:$port",
      kerberos,
      ctx.clientContext,
      Some(createBrokerClientConfiguration("PubSubProtoClient")),
      pubSubClientHandler,
      secureTransport)

  protected def createBrokerClientConfiguration(name: String): DALProtoClient.Configuration =
    new DALProtoClient.Configuration(ctx.asyncConfig, name = name)

  protected val pubSubClientHandler: PubSubClientHandler = new PubSubClientHandler(
    sessionCtx,
    pubSubRetryManager.get.handlePubSubResult,
    pubSubRetryManager.get.handlePubSubException,
    pubSubRetryManager.get.checkStreamExists,
    pubSubRetryManager.get.remoteStreamCleanup,
    () => pubSubRetryManager.get.onConnect()
  )

  override protected def getPubSubRetryManager(
      retryAttempts: Int,
      retryBackoffTime: Long): Option[DsiClientPubSubStreamRetryManager] =
    Some(new DsiClientPubSubStreamRetryManager(retryAttempts, retryBackoffTime))
}

/*
 * This class is responsible just for the mediation between a client and the Service Discovery broker. It does not
 * attempt to handle requests to be sent to a broker (or other DAL service) which was discovered via DAL service
 * discovery -- that is handled by DalServiceDiscoveryDsiProxy.
 */
class DalServiceDiscoveryBrokerProxy(
    ctx: ClientBrokerContext,
    le: BrokerProvider,
    override val partitionMap: PartitionMap)
    extends AbstractRemoteDSIProxyWithLeaderElector(ctx, le) {

  override protected def createBrokerClient(): DalBrokerClient = leaderElectorClient.withRandomBroker { brokerOpt =>
    val broker = brokerOpt.getOrElse(
      throw new DALRetryableActionException(s"No service discovery broker registered of type: ${ctx.broker}", None))
    this synchronized {
      connStr = Some(broker)
      DALProtoClient(
        broker,
        ctx.clientContext,
        requestLimiter,
        batchRetryManager,
        Some(new DALProtoClient.Configuration(ctx.asyncConfig, batchRetryManagerOpt = batchRetryManager)),
        None,
        false,
        writeRequestLimiter
      )
    }
  }

  override protected def shouldCloseClient(brokers: Brokers): Boolean = {
    connStr.exists(connected => !brokers.writeBroker.contains(connected) && !brokers.readBrokers.contains(connected))
  }
}
