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
import optimus.dsi.partitioning.Partition
import optimus.dsi.partitioning.PartitionBrokerLookup
import optimus.dsi.partitioning.PartitionMap
import optimus.dsi.session.EstablishSession
import optimus.dsi.trace.DalRequestId
import optimus.platform.TimeInterval
import optimus.platform._
import optimus.platform.dal.client.PrcKeyProvider
import optimus.platform.dal.config._
import optimus.platform.dal.messages.MessagesBrokerDsiProxy
import optimus.platform.dal.pubsub.PubSubClientRequest
import optimus.platform.dal.pubsub.PubSubClientResponse
import optimus.platform.dal.session.ClientSessionContext.SessionData
import optimus.platform.dsi.Feature.LeadWriterPartitioning
import optimus.platform.dsi.SupportedFeatures
import optimus.platform.dsi.bitemporal._
import optimus.platform.runtime.BrokerProviderResolver
import optimus.platform.storable.EventStateFlag.NEW_EVENT
import optimus.platform.storable.EventStateFlag.RESTATE
import optimus.scalacompat.collection._

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

class PartitionedDsiProxy(
    val baseContext: Context,
    brokerProviderResolver: BrokerProviderResolver,
    val replicaBroker: String,
    val leadBroker: String,
    pubSubBroker: Option[String],
    val accBroker: Option[String],
    val messagesBroker: Option[String],
    prcSkLoc: Option[SilverKingLookup],
    override val partitionMap: PartitionMap,
    val partitionBrokerLookup: PartitionBrokerLookup,
    val zone: DalZoneId,
    val appId: DalAppId,
    cmdLimit: Int,
    asyncConfig: DalAsyncConfig,
    val env: DalEnv,
    shouldLoadBalance: Boolean,
    secureTransport: Boolean,
    val maxBrokersAllowedForConnectionFromURI: Int = 1
) extends PrcBrokerMultiplexingProxy
    with ClientSideDSI {
  import PartitionedDsiProxy._

  log.info(
    s"Partition map received by PartitionedDsiProxy: " +
      s"allPartitions = ${partitionMap.allPartitions}, partitionToLookupMapping = $partitionBrokerLookup")

  protected[optimus] lazy val leadWriter: ClientSideDSI =
    getOrCreateLeaderRemoteDsiProxy(leadBroker, DefaultPartition)
  private val proxies: ConcurrentHashMap[String, DSIClient] = new ConcurrentHashMap
  protected[optimus] lazy val partitionProxies: Map[NamedPartition, ClientSideDSI] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    partitionMap.partitions
      .map { partition =>
        Future {
          val partitionLeader = partitionBrokerLookup.getWriteBroker(partition, leadBroker)
          partition -> getOrCreateLeaderRemoteDsiProxy(partitionLeader, partition)
        }
      }
      .map(fut => Await.result(fut, Duration(10, TimeUnit.MINUTES)))
      .toMap
  }

  protected[optimus] override lazy val replica: ClientSideDSI = new MultiReadBrokersDSIProxy(
    baseContext: Context,
    brokerProviderResolver,
    replicaBroker,
    partitionMap,
    zone,
    appId,
    cmdLimit,
    asyncConfig,
    env,
    shouldLoadBalance,
    secureTransport,
    maxBrokersAllowedForConnectionFromURI
  )

  protected[optimus] override lazy val prcProxyOpt = getPrcRemoteDsiProxy(prcSkLoc)

  private[optimus] lazy val messagesBrokerProxy: Option[MessagesBrokerDsiProxy] = messagesBroker.map { messagesUri =>
    new MessagesBrokerDsiProxy(
      ClientBrokerContext(baseContext, messagesUri, zone, appId, Int.MaxValue, asyncConfig, brokerVirtualHostname(env)),
      brokerProviderResolver,
      env)
  }

  protected[optimus] lazy val accProxyOpt =
    accBroker.filter(_ != replicaBroker).map(accUri => getReplicaRemoteDsiProxy(accUri, cmdLimit, false))

  private[optimus] lazy val pubSubProxyOpt = pubSubBroker.map(getOrCreatePubSubRemoteDsiProxy)
  private lazy val pubsubproxies: ConcurrentHashMap[String, PubSubDsiProxy] = new ConcurrentHashMap
  private[optimus] lazy val pubSubPartitionProxies: Map[NamedPartition, Option[PubSubDsiProxy]] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    partitionMap.partitions
      .map { partition =>
        val proxyFutOpt =
          partitionBrokerLookup
            .getPubSubBroker(partition, pubSubBroker)
            .map { addr =>
              Future {
                getOrCreatePubSubRemoteDsiProxy(addr)
              }
            }
        partition -> proxyFutOpt
      }
      .toMap
      .mapValuesNow(_.map(fut => Await.result(fut, Duration(10, TimeUnit.MINUTES))))
  }

  protected def getOrCreatePubSubRemoteDsiProxy(brokeralias: String): PubSubDsiProxy = {
    pubsubproxies.computeIfAbsent(
      brokeralias,
      _ => {
        new PubSubDsiProxy(
          ClientBrokerContext(
            baseContext,
            brokeralias,
            zone,
            appId,
            Int.MaxValue,
            asyncConfig,
            brokerVirtualHostname(env)),
          brokerProviderResolver,
          partitionMap,
          secureTransport
        )
      }
    )
  }

  private[optimus] def getLeaderRemoteDsiProxy(servingPartition: Partition): Option[DSI] = {
    servingPartition match {
      case DefaultPartition      => Some(leadWriter)
      case named: NamedPartition => partitionProxies.get(named)
    }
  }

  protected def getOrCreateLeaderRemoteDsiProxy(broker: String, servingPartition: Partition): ClientSideDSI = {
    proxies.computeIfAbsent(
      broker,
      _ => {
        new LeaderRemoteDSIProxy(
          ClientBrokerContext(baseContext, broker, zone, appId, Int.MaxValue, asyncConfig, brokerVirtualHostname(env)),
          brokerProviderResolver,
          servingPartition,
          partitionMap,
          secureTransport
        )
      }
    )
  }

  protected def getReplicaRemoteDsiProxy(broker: String, cmdLimit: Int, shouldLoadBalance: Boolean): DSIClient =
    new RandomRemoteDSIProxy(
      ClientBrokerContext(baseContext, broker, zone, appId, cmdLimit, asyncConfig, brokerVirtualHostname(env)),
      brokerProviderResolver,
      partitionMap,
      shouldLoadBalance,
      secureTransport
    )

  protected def getPrcRemoteDsiProxy(skLocOpt: Option[SilverKingLookup]): Option[PrcRemoteDsiProxy] = {
    if (PrcRemoteDsiProxy.enabled && PrcRemoteDsiProxy.supportsContext(baseContext)) {
      try {
        log.debug(s"PRC proxy is enabled; using URI ${skLocOpt.getOrElse("None")}")
        skLocOpt.map(skLoc =>
          new PrcRemoteDsiProxy(
            ClientBrokerContext(
              baseContext,
              skLoc.connectionString,
              zone,
              appId,
              cmdLimit,
              asyncConfig,
              brokerVirtualHostname(env)),
            partitionMap,
            skLoc,
            new PrcKeyProvider(baseContext),
            replica.asInstanceOf[MultiReadBrokersDSIProxy].getDSIClient
          ))
      } catch {
        case NonFatal(ex) =>
          log.error("Failed to initialise PrcRemoteDsiProxy, will fall back to DAL brokers only", ex)
          None
      }
    } else if (!PrcRemoteDsiProxy.supportsContext(baseContext)) {
      log.debug(s"Context $baseContext is not supported by PRC, will fall back to DAL brokers only")
      None
    } else {
      log.debug(s"PRC is not enabled, will fall back to DAL brokers only")
      None
    }
  }

  override def sessionData: SessionData = replica.sessionData

  override private[optimus] def setEstablishSession(command: => EstablishSession): Unit = {
    val establishSession = command
    (Set(leadWriter) ++ partitionProxies.values.toSet ++ Set(replica)).foreach(_.setEstablishSession(establishSession))
    (pubSubProxyOpt.toSet ++ pubSubPartitionProxies.values.flatten.toSet) foreach { proxy =>
      proxy.setEstablishSession(establishSession)
    }
    prcProxyOpt.foreach(_.setEstablishSession(establishSession))
    accProxyOpt.foreach { proxy =>
      proxy.setEstablishSession(establishSession)
    }
    messagesBrokerProxy.foreach(_.setEstablishSession(establishSession))
  }

  private[optimus] override def bindSessionFetcher(sessionFetcher: SessionFetcher): Unit = {
    (Set(leadWriter) ++ partitionProxies.values.toSet ++ Set(replica)).foreach { case clientSideDSI: ClientSideDSI =>
      clientSideDSI.bindSessionFetcher(sessionFetcher)
    }
    (pubSubProxyOpt.toSet ++ pubSubPartitionProxies.values.flatten.toSet).foreach(_.bindSessionFetcher(sessionFetcher))
    prcProxyOpt.foreach(_.bindSessionFetcher(sessionFetcher))
    accProxyOpt.foreach(_.bindSessionFetcher(sessionFetcher))
    messagesBrokerProxy.foreach(_.bindSessionFetcher(sessionFetcher))
  }

  override protected[optimus] def serverFeatures(): SupportedFeatures = replica.serverFeatures()

  override def executePubSubRequest(request: PubSubClientRequest): PubSubClientResponse = {
    val streamId = request.cmd.streamId

    def featureNotEnabledMsg = "Looks like pubsub feature is not enabled. Please contact DAL team."

    def findPartition(streamId: String): Partition = {
      pubSubPartitionProxies
        .filter(_._2.nonEmpty)
        .find(_._2.get.getPubSubStreamManager.checkStreamExists(streamId))
        .getOrElse(throw new IllegalStateException(s"Stream: $streamId does not exist!"))
        ._1
    }

    val partition = request match {
      case request: PubSubClientRequest.CreateStream                                    => request.partition
      case _: PubSubClientRequest.CloseStream | _: PubSubClientRequest.ChangeSubRequest => findPartition(streamId)
    }

    val proxyOpt = partition match {
      case partition: NamedPartition => pubSubPartitionProxies(partition)
      case DefaultPartition          => pubSubProxyOpt
    }

    require(proxyOpt.isDefined, featureNotEnabledMsg)
    proxyOpt.get.executePubSubRequest(request)
  }

  @async override def executeAccCommands(reads: Seq[ReadOnlyCommand]): Seq[Result] = {
    accProxyOpt match {
      case Some(acc) => acc.executeReadOnlyCommands(reads)
      case None      => executeReadOnlyCommands(reads)
    }
  }

  @async override def executeMessagesCommands(cmds: Seq[MessagesCommand]): Seq[MessagesResult] = {
    messagesBrokerProxy match {
      case Some(proxy) => proxy.executeMessagesCommands(cmds)
      case None =>
        val ex = new UnsupportedOperationException("No client found to execute messages commands!")
        cmds.map(_ => MessagesErrorResult(ex))
    }
  }

  @async override def executeLeadWriterCommands(cmds: Seq[LeadWriterCommand]): Seq[Result] = {
    import PartitionedDsiProxy._

    if (checkEmptyTransaction(cmds)) {
      log.warn(s"Empty transaction received as part of cmds $cmds. Ignoring...")
      val paes = cmds.collect { case p: PutApplicationEvent => p }
      paes.map(p =>
        PutApplicationEventResult.empty(
          TimeInterval.NegInfinity,
          p.application,
          p.contentOwner,
          DalClientInfo.Admin,
          DalRequestId.Admin.underlying,
          Set.empty,
          None))
    } else if (!serverFeatures().supports(LeadWriterPartitioning)) {
      log.warn(
        "Server features don't support LeadWriterPartitioning. Sending request to default partition's lead writer.")
      executeWrite(DefaultPartition, cmds)
    } else {
      log.debug(s"Server features support LeadWriterPartitioning. Resolving partition for cmds $cmds")
      executeWriteCommandsWithPartitions(cmds)
    }
  }

  @async private def executeWriteCommandsWithPartitions(cmds: Seq[LeadWriterCommand]): Seq[Result] = {
    import PartitionedDsiProxy._

    val allPartitions: Set[Partition] = partitionMap.allPartitions
    val sysCommand = cmds.collect { case c: SystemCommand => c }
    val oblCommand = cmds.collect { case o: Obliterate => o }
    if (sysCommand.nonEmpty) {
      require(
        cmds.size == 1,
        s"Only one system command is expected if contained in the list of write commands. Command list = $cmds")
      log.debug(s"Cmd $cmds is a system command. Sending it to allPartitions $allPartitions")
      executeWriteAcrossPartitions(cmds)
    } else if (oblCommand.nonEmpty) {
      require(cmds.size == 1, s"Only one obliterate command is expected in a batch. Command list = $cmds")
      val resolvedPartition = getPartitionForCommands(cmds, partitionMap)
      log.debug(s"Resolved partition for $cmds is $resolvedPartition")
      executeWrite(resolvedPartition, cmds)
    } else {
      val resolvedPartition = validateAndGetSinglePartitionForCommands(partitionMap, cmds)
      log.debug(s"Resolved partition for $cmds is $resolvedPartition")
      executeWrite(resolvedPartition, cmds)
    }
  }

  @async private def executeWrite(partition: Partition, cmds: Seq[LeadWriterCommand]): Seq[Result] = {
    partition match {
      case DefaultPartition =>
        leadWriter.executeLeadWriterCommands(cmds)
      case n: NamedPartition =>
        partitionProxies(n).executeLeadWriterCommands(cmds)
    }
  }

  // Method to be used in case a command is authorized to write across partitions
  private def executeWriteAcrossPartitions(cmds: Seq[LeadWriterCommand]): Seq[Result] = {
    cmds.foreach(c =>
      require(
        c.isInstanceOf[SystemCommand],
        s"Only system commands are authorized to write across partitions. Got commands ${cmds}"))
    val unqRmtToPartitionMap =
      ((partitionProxies.iterator ++ Map(DefaultPartition -> leadWriter)).toMap)
        .groupBy { case (_, v) => v }
        .mapValuesNow { i =>
          i.keySet
        }
    unqRmtToPartitionMap flatMap { case (dsi, partitions) =>
      val partStr = partitions.mkString("", ", ", "")
      val res = dsi.executeLeadWriterCommands(cmds)
      res map { h =>
        h match {
          case SystemCommandResult(m) =>
            SystemCommandResult(s"partitions $partStr -> $m")
          case x: ErrorResult =>
            x.copy(
              error = new IllegalStateException(
                s"partitions $partStr -> System command $cmds failed, exception = ${x.error}, " +
                  s"message = ${Option(x.error.getCause).map(_.getMessage).getOrElse("<no cause could be found>")}",
                x.error
              ))
          case e =>
            ErrorResult(new IllegalArgumentException(s"partitions $partStr -> Unexpected result received $e"))
        }
      }
    } toSeq
  }

  // shutdown=false would allow cleaning/renewing some state to keep reusing the dsi
  override protected[optimus] def close(shutdown: Boolean): Unit = {
    leadWriter.close(shutdown)
    replica.close(shutdown)
    partitionProxies.values.toSet foreach { i: DSI =>
      i.close(shutdown)
    }
    (pubSubProxyOpt.toSet ++ pubSubPartitionProxies.values.flatten.toSet).foreach { proxy =>
      proxy.close(shutdown)
    }
    accProxyOpt.foreach { proxy =>
      proxy.close(shutdown)
    }
    prcProxyOpt.foreach { proxy =>
      proxy.close(shutdown)
    }
    messagesBrokerProxy.foreach { proxy =>
      proxy.close(shutdown)
    }
  }
}

object PartitionedDsiProxy {
  private val log = getLogger(this)

  def validateAndGetSinglePartitionForCommands(partitionMap: PartitionMap, cmds: Seq[LeadWriterCommand]): Partition = {
    if (checkEmptyTransaction(cmds)) {
      log.warn("Empty transaction received as part of lead writer commands.")
      DefaultPartition
    } else {
      val commandToPartitionMap = splitLeadWriterCommandsBasedOnPartition(partitionMap, cmds)
      commandToPartitionMap.foreach { case (cmd, parts) =>
        require(
          parts.size == 1,
          s"Write command cannot span across partitions. Got command = ${cmd} spanning across ${parts}")
      }

      val partitionForCommands = commandToPartitionMap.values.flatten.toSet
      require(
        partitionForCommands.size == 1,
        s"All write commands must goto the same partition. Found command(s) accessing multiple partitions. " +
          s"Problematic commands = ${commandToPartitionMap}, " +
          s"resolvedPartitions = ${partitionForCommands}"
      )

      partitionForCommands.head
    }
  }

  def checkEmptyTransaction(cmds: Seq[LeadWriterCommand]): Boolean = {
    cmds.forall {
      case p: PutApplicationEvent =>
        p.bes.isEmpty || p.bes.forall(i =>
          i.state == RESTATE && i.puts.isEmpty && i.invalidates.isEmpty && i.reverts.isEmpty && i.putSlots.isEmpty)
      case _ => false
    }
  }

  private def splitLeadWriterCommandsBasedOnPartition(
      partitionMap: PartitionMap,
      cmds: Seq[LeadWriterCommand]): Map[LeadWriterCommand, Set[Partition]] = {
    def throwMissingClassException(prop: String, obj: Any) = {
      throw new IllegalArgumentException(
        s"Classname must be defined for $prop in order to use partitioning. " +
          s"Got $prop = $obj containing className as None")
    }

    cmds flatMap { cmd =>
      cmd match {
        case c: PutApplicationEvent =>
          val classNamesForWrites = c.bes flatMap { be =>
            be.puts.map(i => i.ent.className) ++
              be.invalidates.map(i => i.cn) ++
              be.reverts.map(i => i.cn)
          } toSet

          val classNameForBusinessEvents = c.bes.filter(_.state == NEW_EVENT) map { i =>
            i.className().getOrElse(throwMissingClassException("BusinessEvent", c.bes))
          } toSet

          val cns = classNameForBusinessEvents ++ classNamesForWrites
          Some(c -> cns.map(partitionMap.partitionForType))

        case a: AbstractRawCommand =>
          throw new IllegalArgumentException(
            s"Admin command is not excepted to originate from dal client. Got command ${a}"
          )

        case g: GeneratedAppEvent =>
          throw new IllegalArgumentException(
            s"GeneratedAppEvent is a server side construct which cannot be resolved on client side. Got event ${g}"
          )

        case c: CreateSlots =>
          val cns = c.className().getOrElse(throwMissingClassException("CreateSlots", c))
          Some(c -> Set(partitionMap.partitionForType(cns)))

        case av: AssertValid =>
          val assertValids = cmds.collect { case cm: AssertValid => cm }
          if (assertValids.length == cmds.length)
            Some(av -> Set[Partition](DefaultPartition))
          else None

        case f: FillSlot =>
          val cns = f.className().getOrElse(throwMissingClassException("FillSlot", f))
          Some(f -> Set(partitionMap.partitionForType(cns)))

        case i: InvalidateAfter =>
          val cns = i.className().getOrElse(throwMissingClassException("InvalidateAfter", i))
          Some(i -> Set(partitionMap.partitionForType(cns)))

        case i: InvalidateAllCurrent =>
          val cns = i.className().getOrElse(throwMissingClassException("InvalidateAllCurrent", i))
          Some(i -> Set(partitionMap.partitionForType(cns)))

        case i: InvalidateAllCurrentByRefs =>
          val cns = i.className().getOrElse(throwMissingClassException("InvalidateAllCurrentByRefs", i))
          Some(i -> Set(partitionMap.partitionForType(cns)))

        case p: Put =>
          val cns = p.className().getOrElse(throwMissingClassException("Put", p))
          Some(p -> Set(partitionMap.partitionForType(cns)))

        case r: ResolveKeys =>
          val cns = r.keys.map(i => i.typeName).toSet
          Some(r -> cns.map(partitionMap.partitionForType))

        case _: SystemCommand => None

        case a: AccMetadataCommand =>
          val cns = a.className().getOrElse(throwMissingClassException("AccMetadataCommand", a))
          Some(a -> Set(partitionMap.partitionForType(cns)))

        case w: WriteBusinessEvent =>
          throw new IllegalArgumentException(
            s"Unexpected txn Action ${w}. WriteBusinessEvent is always expected to be part of PutApplicationEvent"
          )

        case o: Obliterate =>
          val cnsOpt = resolveObliterateClassNames(o)
          cnsOpt.foreach(i =>
            require(
              i.isDefined,
              s"Classname for every obliterate command must be defined. Got classname ${i} from command ${o}"))
          val cns = cnsOpt.flatten
          Some(o -> cns.map(partitionMap.partitionForType))
      }
    } toMap

  }

  def getPartitionForCommands(cmds: Seq[LeadWriterCommand], partitionMap: PartitionMap): Partition = {
    val oblCommands = cmds.collect { case c: Obliterate => c }
    val oblRefCommands: Set[LeadWriterCommand] = oblCommands flatMap { o =>
      o.queries flatMap { query =>
        query match {
          case e @ (EventCmReferenceQuery(_, _) | EntityCmReferenceQuery(_, _) | ReferenceQuery(_, _, None) |
              EventReferenceQuery(_)) if e.className.isEmpty =>
            Some(o)

          case _ => None
        }
      }
    } toSet

    if (oblRefCommands.nonEmpty) DefaultPartition
    else validateAndGetSinglePartitionForCommands(partitionMap, cmds)
  }

  private def resolveObliterateClassNames(o: Obliterate): Set[Option[String]] = {
    o.queries.map { query =>
      query match {
        case e: EntityClassQuery            => e.className
        case e: EntityClassAppIdUserIdQuery => e.className
        case e: EventClassQuery             => e.className
        case s: SerializedKeyQuery          => Some(s.key.typeName)
        case e: EntityCmReferenceQuery      => e.className
        case r: ReferenceQuery              => r.className
        case e: EventCmReferenceQuery       => e.className
        case e: EventReferenceQuery         => e.className
        case s: EventSerializedKeyQuery     => Some(s.key.typeName)
        case l: LinkageQuery                => l.className
      }
    } toSet
  }
}
