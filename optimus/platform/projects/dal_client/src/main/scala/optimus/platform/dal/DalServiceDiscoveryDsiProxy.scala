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
import optimus.dsi.partitioning.DefaultPartition
import optimus.dsi.partitioning.NamedPartition
import optimus.dsi.partitioning.PartitionBrokerLookup
import optimus.dsi.partitioning.PartitionMap
import optimus.dsi.session.EstablishSession
import optimus.dsi.session.EstablishSessionSuccess
import optimus.dsi.session.EstablishedClientSession
import optimus.platform._
import optimus.platform.dal.config.DalAppId
import optimus.platform.dal.config.DalAsyncConfig
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.DalServicesSilverKingLookup
import optimus.platform.dal.config.DalZoneId
import optimus.platform.dal.pubsub.PubSubClientRequest
import optimus.platform.dal.pubsub.PubSubClientResponse
import optimus.platform.dal.servicediscovery.DalServiceLookupType
import optimus.platform.dal.servicediscovery.DalServiceProviderType
import optimus.platform.dal.servicediscovery.DalServiceType
import optimus.platform.dal.servicediscovery.ServiceDiscoveryElement
import optimus.platform.dal.session.ClientSessionContext
import optimus.platform.dsi.SupportedFeatures
import optimus.platform.dsi.bitemporal.Context
import optimus.platform.dsi.bitemporal.DiscoverServices
import optimus.platform.dsi.bitemporal.DiscoverServicesFailure
import optimus.platform.dsi.bitemporal.DiscoverServicesSuccess
import optimus.platform.dsi.bitemporal.LeadWriterCommand
import optimus.platform.dsi.bitemporal.MessagesCommand
import optimus.platform.dsi.bitemporal.MessagesResult
import optimus.platform.dsi.bitemporal.ReadOnlyCommand
import optimus.platform.dsi.bitemporal.Result
import optimus.platform.dsi.bitemporal.ServiceDiscoveryCommand
import optimus.platform.dsi.bitemporal.ServiceDiscoveryUnsupportedException
import optimus.platform.runtime.DalServiceDiscoveryLeaderElectorClient
import optimus.platform.runtime.OptimusCompositeLeaderElectorClient
import optimus.platform.runtime.RuntimeConfigurationFactory

/*
 * There is a conscious design decision made here, which is to encapsulate DAL service discovery handling within this
 * class rather than expose it to a higher level. The reason we make this decision is that currently there is an
 * expectation in the client code that DsiProxies implement the ClientSideDSI interface in order to expose datastore
 * operations to higher levels of the client (via executeReadOnlyCommands / executeLeadWriterCommands). As a result,
 * if we want the service discovery logic to live at higher levels of the client, we currently need to expose it in
 * ClientSideDSI itself, which isn't desirable as not all ClientSideDSIs will support service discovery. When we later
 * want to unpick this decision, we will have to expose an alternative interface via the existing DSIFactoryImpl /
 * EntityResolverFactory APIs. This will likely take place as part of later work to alter the way that fresh Optimus
 * runtimes establish DAL connections if and when we choose to do that.
 */
class DalServiceDiscoveryDsiProxy(
    env: DalEnv,
    override val baseContext: Context,
    override val partitionMap: PartitionMap,
    instance: String,
    subInstance: String,
    zoneId: DalZoneId,
    appId: DalAppId,
    cmdLimit: Int,
    asyncConfig: DalAsyncConfig
) extends ClientSideDSI {
  import DalServiceDiscoveryDsiProxy._

  private var state: State = DalServiceDiscoveryDsiProxy.State.Uninitialized(None)

  private val serviceDiscoveryCtx =
    ClientBrokerContext(
      baseContext,
      s"$instance/$subInstance",
      zoneId,
      appId,
      cmdLimit,
      asyncConfig,
      brokerVirtualHostname(env))
  private val serviceDiscoveryProxy = new DalServiceDiscoveryBrokerProxy(
    serviceDiscoveryCtx,
    DalServiceDiscoveryLeaderElectorClient.getClient(instance, subInstance),
    partitionMap
  )

  private def shutdownPartitionedProxy(state: State, reason: String): Unit = {
    state.partitionedProxyOpt.foreach { proxy =>
      log.info(s"Shutting down DAL connections due to $reason...")
      proxy.close(shutdown = true)
    }
  }

  override def setEstablishSession(establishSession: => EstablishSession): Unit = synchronized {
    state match {
      case _: State.Uninitialized | _: State.Failed =>
        // If we are Uninitialized, we may not be able to immediately execute session establishment with the service
        // discovery broker. The reason is that this entails executing an async call (executeServiceDiscoveryCommands)
        // but in the Uninitialized state the optimus runtime may not yet have been initialized so we may not be able
        // to make async calls. As such we defer this to later by capturing the establishment to be executed later
        // once the runtime initialization has advanced further and the initial time tweaks are getting calculated.
        state = State.Uninitialized(Some(establishSession))
      case i: State.Initialized =>
        // If we are Initialized, we should be safe to immediately perform this new session establishment and
        // reconfigure the underlying proxies based on the result.
        val newState = executeServiceDiscovery(
          serviceDiscoveryProxy,
          establishSession,
          env,
          baseContext,
          partitionMap,
          zoneId,
          appId,
          cmdLimit,
          asyncConfig)
        // since we're re-configuring, we should shut down the old partitioned proxy now, otherwise we might leave
        // connections hanging around
        shutdownPartitionedProxy(i, "successful service discovery")
        state = newState
    }
  }

  private def initializedProxy: PartitionedDsiProxy = synchronized {
    initializeIfPossible()
    state.partitionedProxyOpt.getOrElse(throw new IllegalArgumentException("Service Discovery not yet complete."))
  }

  private def initializeIfPossible(): Unit = synchronized {
    state match {
      case State.Uninitialized(Some(es)) =>
        // we have session establishment information which we can use to perform service discovery, but haven't yet
        // done so
        state = executeServiceDiscovery(
          serviceDiscoveryProxy,
          es,
          env,
          baseContext,
          partitionMap,
          zoneId,
          appId,
          cmdLimit,
          asyncConfig)
      case State.Failed(ex) =>
        throw new IllegalArgumentException(s"DAL service discovery failed", ex)
      case State.Uninitialized(None) | _: State.Initialized =>
    }
  }

  @async override def executeReadOnlyCommands(reads: Seq[ReadOnlyCommand]): Seq[Result] =
    initializedProxy.executeReadOnlyCommands(reads)

  @async override def executeLeadWriterCommands(cmds: Seq[LeadWriterCommand]): Seq[Result] =
    initializedProxy.executeLeadWriterCommands(cmds)

  @async override def executeServiceDiscoveryCommands(sds: Seq[ServiceDiscoveryCommand]): Seq[Result] =
    serviceDiscoveryProxy.executeServiceDiscoveryCommands(sds)

  override def executePubSubRequest(request: PubSubClientRequest): PubSubClientResponse =
    initializedProxy.executePubSubRequest(request)

  override def executePubSubRequestBatch(requests: Seq[PubSubClientRequest]): Seq[PubSubClientResponse] =
    initializedProxy.executePubSubRequestBatch(requests)

  @async override def executeMessagesCommands(cmds: Seq[MessagesCommand]): Seq[MessagesResult] =
    initializedProxy.executeMessagesCommands(cmds)

  override def sessionData: ClientSessionContext.SessionData = synchronized {
    state match {
      case i: State.Initialized => ClientSessionContext.SessionData(Some(i.session), None, Set.empty[Int])
      case State.Uninitialized(establishSessionOpt) =>
        ClientSessionContext.SessionData(None, establishSessionOpt.map(SessionId -> _), Set.empty[Int])
      case State.Failed(_) => ClientSessionContext.SessionData(None, None, Set.empty[Int])
    }
  }

  override protected[optimus] def close(shutdown: Boolean): Unit = {
    serviceDiscoveryProxy.close(shutdown)
    // If we are being asked to shutdown, we forget about the underlying proxies. This means that if we want to reuse
    // this instance after a close(shutdown=true) call, we will have to call setEstablishSession(...) again in order to
    // trigger another service discovery & reconfiguration cycle. If we're not being asked to shut down, we don't
    // actually want to propagate the close call to underlying service proxies: if we do, we will disconnect from them,
    // which isn't desirable behaviour (most likely we're just failing over between service discovery brokers or
    // something)
    if (shutdown) {
      val oldState = synchronized {
        val oldState = state
        state = State.Uninitialized(None)
        oldState
      }
      oldState.partitionedProxyOpt.foreach(_.close(shutdown))
    }
  }

  // in future we will want something more fine-grained than this like serverFeatures(serviceType)
  override protected[optimus] def serverFeatures(): SupportedFeatures = synchronized {
    initializeIfPossible()
    state match {
      case State.Initialized(_, _, esc) => esc.serverFeatures
      case _: State.Uninitialized | _: State.Failed =>
        throw new IllegalArgumentException(s"Could not get supported server features from server")
    }
  }
  private[optimus] override def bindSessionFetcher(sessionFetcher: SessionFetcher): Unit = {}
}

object DalServiceDiscoveryDsiProxy {
  private val log = getLogger(DalServiceDiscoveryDsiProxy)
  private val SessionId = 1

  private sealed trait State {
    def partitionedProxyOpt: Option[PartitionedDsiProxy]
    def services: Seq[ServiceDiscoveryElement]
  }

  private object State {
    // In the Uninitialized state we haven't got valid service discovery data or session establishment. As a result
    // we can't use the proxy to execute commands.
    final case class Uninitialized(establishSession: Option[EstablishSession]) extends State {
      override def partitionedProxyOpt: Option[PartitionedDsiProxy] = None
      override def services: Seq[ServiceDiscoveryElement] = Nil
    }

    // The service discovery proxy has discovered services and is currently in a normal running state
    final case class Initialized(
        services: Seq[ServiceDiscoveryElement],
        partitionedProxy: PartitionedDsiProxy,
        session: EstablishedClientSession)
        extends State {
      override def partitionedProxyOpt: Option[PartitionedDsiProxy] = Some(partitionedProxy)
    }

    final case class Failed(ex: Exception) extends State {
      override def partitionedProxyOpt: Option[PartitionedDsiProxy] = None
      override def services: Seq[ServiceDiscoveryElement] = Nil
    }
  }

  private def executeServiceDiscovery(
      serviceDiscoveryProxy: DalServiceDiscoveryBrokerProxy,
      es: => EstablishSession,
      env: DalEnv,
      baseContext: Context,
      partitionMap: PartitionMap,
      zoneId: DalZoneId,
      appId: DalAppId,
      cmdLimit: Int,
      asyncConfig: DalAsyncConfig): State = {
    log.info("Performing DAL service discovery...")
    val results =
      serviceDiscoveryProxy.executeServiceDiscoveryCommands(
        Seq(DiscoverServices(es, env, RuntimeConfigurationFactory.sysLoc)))
    val newState = results match {
      case Seq(r) =>
        EntityResolverReadImpl
          .mapResult(r) {
            case DiscoverServicesSuccess(discoveredServices, establishSessionSuccess) =>
              consumeDiscoverServicesSuccess(
                discoveredServices,
                es,
                establishSessionSuccess,
                baseContext,
                partitionMap,
                zoneId,
                appId,
                cmdLimit,
                asyncConfig,
                env)
            case dsf: DiscoverServicesFailure =>
              consumeDiscoverServicesFailure(es, dsf)
              State.Uninitialized(None)
          }
          .getOrElse(throw new GeneralDALException(s"Unable to configure DAL client through DAL Service Discovery"))
      case o =>
        log.debug(s"Expected exactly one service discovery result, but got ${o.length}: $o")
        throw new GeneralDALException(s"Expected exactly one service discovery result, but got ${o.length}")
    }
    newState match {
      case i: State.Initialized =>
        log.debug(s"DAL Service Discovery successful. Will close (but not shut down) service discovery proxy...")
        serviceDiscoveryProxy.close(shutdown = false)
        // the underlying partitioned proxy will also need to perform session establishment later on
        i.partitionedProxy.setEstablishSession(es)
      case _: State.Uninitialized | _: State.Failed =>
        log.info("No longer initialized after performing service discovery. Shutting down...")
        serviceDiscoveryProxy.close(shutdown = true)
    }
    newState
  }

  private def consumeDiscoverServicesSuccess(
      services: Seq[ServiceDiscoveryElement],
      es: EstablishSession,
      ess: EstablishSessionSuccess,
      baseContext: Context,
      partitionMap: PartitionMap,
      zoneId: DalZoneId,
      appId: DalAppId,
      cmdLimit: Int,
      asyncConfig: DalAsyncConfig,
      env: DalEnv): State.Initialized = {
    def ignoreService(service: ServiceDiscoveryElement): Unit = {
      log.warn(s"DAL service discovery ignoring service: $service")
    }

    def firstLookupString(services: Seq[ServiceDiscoveryElement]): Option[String] = {
      if (services.nonEmpty) services.tail.foreach(ignoreService)
      services.headOption.map(_.lookupStr)
    }

    log.info(s"DAL service discovery reconfiguring from ${services.length} discovered service(s)...")
    log.debug(s"All services: ${services.mkString(", ")}")

    // if we choose to support lookup not through ZK, we will have to alter this logic.
    val (zkServices, nonZkServices) = services.partition(_.lookupType == DalServiceLookupType.ZooKeeper)
    nonZkServices.foreach(ignoreService)

    val servicesByType = zkServices.groupBy(s => (s.serviceType, s.serviceProviderType))

    var replicaOpt: Option[String] = None
    var leadWriterOpt: Option[String] = None
    val writePartitionLookupMappingBld = Map.newBuilder[NamedPartition, String]
    var pubsubOpt: Option[String] = None
    val pubsubPartitionLookupMappingBld = Map.newBuilder[NamedPartition, String]
    var acc: Option[String] = None
    var prc: Option[String] = None
    var messages: Option[String] = None
    val messagesBrk: Option[String] = None

    servicesByType.foreach {
      case ((DalServiceType.Read, DalServiceProviderType.Broker), readBrokers) =>
        replicaOpt = firstLookupString(readBrokers)
      case ((DalServiceType.Write, DalServiceProviderType.Broker), writeBrokers) =>
        writeBrokers.foreach { writeBroker =>
          writeBroker.partition
            .map {
              case DefaultPartition  => leadWriterOpt = Some(writeBroker.lookupStr)
              case n: NamedPartition => writePartitionLookupMappingBld += (n -> writeBroker.lookupStr)
            }
            .getOrElse(ignoreService(writeBroker))
        }
      case ((DalServiceType.PubSub, DalServiceProviderType.Broker), pubSubBrokers) =>
        pubSubBrokers.foreach { pubsubBroker =>
          pubsubBroker.partition
            .map {
              case DefaultPartition  => pubsubOpt = Some(pubsubBroker.lookupStr)
              case n: NamedPartition => pubsubPartitionLookupMappingBld += (n -> pubsubBroker.lookupStr)
            }
            .getOrElse(ignoreService(pubsubBroker))
        }
      case ((DalServiceType.Accelerated, DalServiceProviderType.Broker), accBrokers) =>
        acc = firstLookupString(accBrokers)
      case ((DalServiceType.Read, DalServiceProviderType.Prc), prcBrokers) =>
        prc = firstLookupString(prcBrokers)
      case ((DalServiceType.Messages, DalServiceProviderType.Broker), messagesBroker) =>
        messages = firstLookupString(messagesBroker)
      case x =>
        throw new MatchError(x)
    }

    val replica = replicaOpt.getOrElse(
      throw new ServiceDiscoveryUnsupportedException("Cannot construct a DSI proxy without a replica broker lookup"))
    val leadWriter = leadWriterOpt.getOrElse(
      throw new ServiceDiscoveryUnsupportedException(
        "Cannot construct a DSI proxy without a lead writer broker lookup"))

    val pubsubLookup: Map[PartitionBrokerLookup.BrokerAddress, PartitionBrokerLookup.PartitionToBrokerMap] =
      pubsubOpt
        .map { defaultAddr =>
          Map(defaultAddr -> pubsubPartitionLookupMappingBld.result())
        }
        .getOrElse(Map.empty)
    val writeLookup = writePartitionLookupMappingBld.result()
    val partitionToLookupMapping = PartitionBrokerLookup(writeLookup, pubsubLookup)

    val unavailablePartitions = DSIClient.getUnavailablePartitionsFromReplica(
      OptimusCompositeLeaderElectorClient.asBrokerProviderResolver,
      replica)
    if (unavailablePartitions.nonEmpty)
      log.info(s"unavailable partitions: ${unavailablePartitions.mkString(",")}")
    val newPartitionMap = PartitionMap(partitionMap, unavailablePartitions)
    val newPartitionedProxy = new PartitionedDsiProxy(
      baseContext,
      OptimusCompositeLeaderElectorClient.asBrokerProviderResolver,
      replica,
      leadWriter,
      pubsubOpt,
      acc,
      messagesBrk,
      prc.flatMap(DalServicesSilverKingLookup.fromString(env, _)),
      newPartitionMap,
      partitionToLookupMapping,
      zoneId,
      appId,
      cmdLimit,
      asyncConfig,
      env,
      // TODO (OPTIMUS-47658): Revisit this when service discovery logic is enabled
      false,
      // TODO (OPTIMUS-0000): Revisit this when service discovery logic is enabled
      false
    )

    log.trace("Consuming EstablishSession result...")
    val session = ClientSessionContext.handleEstablishSessionSuccess(es, ess)
    State.Initialized(services, newPartitionedProxy, session)
  }

  private def consumeDiscoverServicesFailure(es: EstablishSession, dsf: DiscoverServicesFailure): Unit = dsf match {
    case DiscoverServicesFailure.CannotEstablishSession(establishFailure) =>
      val (ex, _) = ClientSessionContext.handleEstablishSessionFailure(es, establishFailure)
      log.error("Could not establish session with DAL Service Discovery broker", ex)
    case DiscoverServicesFailure.CannotDiscoverServices(msg) =>
      log.error(s"Could not discover services from DAL Service Discovery broker: $msg")
    case DiscoverServicesFailure.GenericFailure(msg) =>
      log.error(s"Received unknown error from DAL Service Discovery broker: $msg")
  }
}
