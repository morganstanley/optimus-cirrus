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
import optimus.dsi.partitioning.Partition
import optimus.dsi.partitioning.PartitionMap
import optimus.graph.DiagnosticSettings
import optimus.platform.dal.DALPubSub.ClientStreamId
import optimus.platform._
import optimus.platform.dal.pubsub.PubSubClientHandler
import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.protobufutils
import optimus.platform.dsi.protobufutils.DALProtoClient
import optimus.platform.dsi.protobufutils.DalBrokerClient
import optimus.platform.dsi.protobufutils.PubSubProtoClient
import optimus.platform.runtime.BrokerProvider
import optimus.platform.runtime.BrokerProviderResolver
import optimus.platform.runtime.Brokers
import optimus.platform.runtime.LeaderElectorClientListener
import optimus.platform.runtime.OptimusCompositeLeaderElectorClient

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
