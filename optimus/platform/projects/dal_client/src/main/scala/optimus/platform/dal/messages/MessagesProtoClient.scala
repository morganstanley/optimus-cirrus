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
package optimus.platform.dal.messages

import com.google.protobuf.MessageLite
import msjava.msnet.MSNetMessage
import msjava.protobufutils.server.BackendException
import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.RequestsStallInfo
import optimus.breadcrumbs.crumbs.StallPlugin
import optimus.core.CoreAPI
import optimus.core.needsPlugin
import optimus.dsi.base.UriSchemeChecker
import optimus.dsi.session.EstablishSessionResult
import optimus.graph._
import optimus.platform._
import optimus.platform.dal.ClientBrokerContext
import optimus.platform.dal.ClientContext
import optimus.platform.dal.ClientRequestLimiter
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.messages.MessagesSubscription.EventClassName
import optimus.platform.dal.session.ClientSessionContext
import optimus.platform.dsi.DSIResponse
import optimus.platform.dsi.Response
import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.bitemporal.proto.CommandProtoSerialization
import optimus.platform.dsi.bitemporal.proto.Dsi.DSIRequestProto
import optimus.platform.dsi.bitemporal.proto.Dsi.DalResponseProto
/* import optimus.platform.dsi.bitemporal.proto.Envelope.DalProtocolStatus
import optimus.platform.dsi.bitemporal.proto.Envelope.DalRequestResponseEnvelope */
import optimus.platform.dsi.connection.DalProtocolVersion
import optimus.platform.dsi.protobufutils.BatchContext.BatchContextImpl
import optimus.platform.dsi.protobufutils._

import java.net.URI
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

object MessagesProtoClient extends CommandProtoSerialization {
  private val log = getLogger(this)
  private[MessagesProtoClient] val clientSerial = new AtomicInteger(0)
  type RequestId = String

  def apply(
      uriString: String,
      clientBrkContext: ClientBrokerContext,
      config: Option[DALProtoClient.Configuration],
      listener: MessagesClientEventsListener,
      sessionCtx: ClientSessionContext,
      env: DalEnv): MessagesProtoClient = {

    val (hostPort, kerberized) = {
      val uri = new URI(uriString)
      (s"${uri.getHost}:${uri.getPort}", UriSchemeChecker.isKerberized(uri))
    }

    new MessagesProtoClient(
      hostPort = hostPort,
      kerberized = kerberized,
      clientBrkCtx = clientBrkContext,
      configOpt = config,
      listener = listener,
      clientSessionCtx = sessionCtx,
      reqLimiter = new ClientRequestLimiter(clientBrkContext.cmdLimit),
      env = env
    )
  }

  private[messages] def sendRequest(
      msgsBatchContext: BatchContext,
      tcpRequestSender: TcpRequestSender,
      seqId: Int,
      ctx: ClientContext
  ): Unit = {
    tcpRequestSender.assertConnected()
    val request = buildRequest(msgsBatchContext, tcpRequestSender, ctx)
    val lvl = if (log.isDebugEnabled()) CmdLogLevelHelper.DebugLevel else CmdLogLevelHelper.InfoLevel
    val cmdDetails = msgsBatchContext.cmdDetails(lvl)
    cmdDetails foreach { details =>
      log.info(s"${logPrefix(msgsBatchContext.requestUuid, seqId)} $details, $toString")
    }
    val byteMessage: Array[Byte] = tcpRequestSender.buildMsnetMessageImpl(request.toByteString, seqId)
    tcpRequestSender.asyncSend(byteMessage)
  }

  private def buildRequest(msgsBatchContext: BatchContext, sender: TcpRequestSender, ctx: ClientContext): MessageLite =
    sender.buildRequest(
      msgsBatchContext.chainedId,
      msgsBatchContext.requestUuid,
      msgsBatchContext.clientRequests,
      msgsBatchContext.clientSessionContext.sessionData,
      toProto(ctx.context),
      toProto(ClientAppIdentifier(ctx.zoneId, ctx.appId)),
      msgsBatchContext.requestType,
      Seq.empty,
      Seq.empty,
      minAssignableTtOpt = None,
      msgsBatchContext.lastWitnessedTxTimeOpt,
      msgsBatchContext.redirectionInfo
    )
}

@entity object PublishCommandScheduler {

  @node(exposeArgTypes = true) @scenarioIndependent def syncPublishCmd(
      protoClient: MessagesProtoClient,
      cmd: MessagesPublishCommandBase,
      clientBrkCtx: ClientBrokerContext,
      clientSesCtx: ClientSessionContext,
      requestBatcher: ClientRequestBatcher
  ): Response = needsPlugin

  override val log: Logger = getLogger(this)

  val batchSenderPlugin: SchedulerPlugin = new SchedulerPlugin {

    override val pluginType: PluginType = PluginType.DAL
    override val stallReason = "waiting for command execution on DAL messages broker"

    override def stallRequests(node: NodeTaskInfo): Option[RequestsStallInfo] = {
      val requests = ClientRequestTracker.getRequestsInFlight.toSeq
      Some(RequestsStallInfo(StallPlugin.DAL, requests.length, requests))
    }

    override def adapt(v: NodeTask, ec: OGSchedulerContext): Boolean = ??? /* {
      v match {
        case node: PublishCommandScheduler.syncPublishCmd$node =>
          try {
            val nodeWithScheduler = NodeWithScheduler(ec.scheduler, node, 0L, NoPreCalc)
            // batchScope exit happens in MessageContexts when we completeWithException/completeWithResult
            node.scenarioStack.batchScope.enterDSI(node)
            val payload = MessagesClient.generateMessagesPayload(node.cmd, node.clientBrkCtx)
            val req =
              ClientRequest.write(
                completable = nodeWithScheduler,
                nodeID = payload.publisherChainId,
                clientSessionCtx = node.clientSesCtx,
                commands = Seq(node.cmd),
                redirectionInfo = None,
                lastWitnessedTxTimeOpt = None,
                callerDetail = None
              )
            node.protoClient.tcpRequestSender.assertConnected()
            node.requestBatcher.offer(req)
          } catch {
            case e: Exception =>
              log.error(
                s"Caught exception while sync send from MessagesProtoClient ${e.getClass.getName} - ${e.getMessage}",
                e)
              node.completeWithException(e, ec)
          }
          true
        case _ =>
          log.warn("Received unsupported task type " + v)
          false
      }
    } */
  }

  syncPublishCmd_info.setCacheable(false)
  syncPublishCmd_info.setPlugin(batchSenderPlugin)
}

class MessagesProtoClient(
    val hostPort: String,
    val kerberized: Boolean,
    override val clientBrkCtx: ClientBrokerContext,
    configOpt: Option[DALProtoClient.Configuration] = None,
    override val listener: MessagesClientEventsListener,
    val clientSessionCtx: ClientSessionContext,
    reqLimiter: ClientRequestLimiter,
    override val env: DalEnv
) extends BatchedDalBrokerClient(
      reqLimiter = reqLimiter,
      clientCtx = clientBrkCtx.clientContext,
      batchRetryManager = None,
      writeServingPartitionOpt = None
    )
    with MessagesClient
    with CommandProtoSerialization
    with CoreAPI {
  outer =>
  import MessagesProtoClient._

  private lazy val protoConfig: DALProtoClient.Configuration =
    configOpt.getOrElse(new DALProtoClient.Configuration(clientBrkCtx.clientContext.asyncConfig))

  override protected def createNewRequestSender: RequestSender = new RequestSender(
    this,
    protoConfig.mkSenderFactory(
      hostPort,
      kerberized,
      clientName,
      this,
      onMessage,
      () => shutdown(None),
      onConnect _,
      false),
    clientCtx.context,
    ClientAppIdentifier(clientCtx.zoneId, clientCtx.appId)
  )

  // Needs to be lazy because requestSender is lazy.
  private[messages] lazy val tcpRequestSender: TcpRequestSender = {
    require(requestSender.impl.isInstanceOf[TcpRequestSender], "Require BatchedRequestSender of type tcpRequestSender")
    requestSender.impl.asInstanceOf[TcpRequestSender]
  }

  override protected def createNewRequestBatcher(requestSender: RequestSender): ClientRequestBatcher =
    new ClientRequestBatcher(
      requestSender,
      protoConfig.batchingQueue,
      RequestBatcher.Configuration(clientName, async = true, protoConfig.largeBatchBreakdownCallback _),
      None)

  private[this] val serial = clientSerial.incrementAndGet
  override val toString = s"$clientName:$hostPort"
  override def clientName: String = s"${protoConfig.name}#$serial"

  // Needs to be lazy because creating the TimeoutDetector might be side-effecting, for instance by spawning threads
  // etc. As such initialization of the timeoutDetector should not happen until we call start()

  override protected def createNewTimeoutDetector: TimeoutDetector =
    new TimeoutDetector(this, clientName, protoConfig.timeoutDetectorConfig)

  override def connectionString: String = hostPort

  override def createStream(cmd: CreateMessagesStream): Unit = sendStreamRequest(cmd)

  override def closeStream(cmd: CloseMessagesStream): Unit = sendStreamRequest(cmd)

  override def updateSubscription(
      cmd: ChangeMessagesSubscription,
      subs: Set[EventClassName],
      unsubs: Set[EventClassName]): Unit = sendStreamRequest(cmd)

  override def commitStream(cmd: CommitMessagesStream): Unit = sendStreamRequest(cmd)

  override def checkEntitlementAndSetACLs(cmd: StreamsACLsCommand): MessagesResult = {
    sendSyncRequest(cmd)
    StreamsACLsCommandSuccessResult(cmd.streamAppId)
  }

  private def getMsgCtx[T <: Command](cmd: T, p: Promise[Response] = Promise()) = ??? /* {
    val chainId = ChainedID.root
    BatchContextImpl(
      requestUuid = UUID.randomUUID().toString,
      chainedId = chainId,
      requestType = DSIRequestProto.Type.MESSAGES,
      clientSessionContext = clientSessionCtx,
      clientRequests = Vector(ClientRequest.read(CompletablePromise(p), chainId, clientSessionCtx, Seq(cmd))),
      token = None,
      redirectionInfo = None
    )
  } */

  private def sendStreamRequest(cmd: MessagesStreamCommand): Unit = {
    // As we are not batching these requests, seqId set is -1, seqI
    sendRequest(getMsgCtx(cmd), tcpRequestSender, -1, clientBrkCtx.clientContext)
  }

  // Reuse sendBatch to implement SyncRequest is because it maintained (seq => batch) mapping
  // In this way, we can retrieve the ctx using getBatch(seqId) for result notification.
  private def sendSyncRequest[T <: Command](cmd: T): Unit = {
    val promise = Promise[Response]()
    val ctx = getMsgCtx(cmd, promise)
    // requestSender.sendBatch(ctx)
    Await.result(promise.future, Duration(protoConfig.maxWaitingTimeDALRequest, TimeUnit.MILLISECONDS))
  }

  @async override def publish(
      msgs: Seq[MessagesPublishCommandBase]
  ): Seq[MessagesResult] =
    msgs.aseq.map { msg =>
      msg.option match {
        case MessagesPublishCommandBase.Option.Ack =>
          PublishCommandScheduler.syncPublishCmd(this, msg, clientBrkCtx, clientSessionCtx, requestBatcher)
        case MessagesPublishCommandBase.Option.IgnoreAck =>
          requestBatcher.offer(
            ClientRequest.write(
              completable = CompletablePromise(Promise()),
              nodeID = ChainedID.root,
              clientSessionCtx = clientSessionCtx,
              commands = msgs,
              redirectionInfo = None,
              lastWitnessedTxTimeOpt = None
            ))
      }
      MessagesPublishSuccessResult(msg.identifier)
    }

  override def shutdown(cause: Option[DalBrokerClient.ShutdownCause]): Unit =
    if (isRunning) {
      super.shutdown(cause)
      // to make sure we do not execute the shutdown logic multiple times
      // we do this stopIfRunning and have this whole method body wrapped in isRunning check
      log.info(s"Shutting down ${outer}")
      log.info(s"${protoConfig.name} shut down completed.")
      cause match {
        // We do not propagate this shutdown further in case of duplicate proto client creation because
        // DSIClient is expected to handle this particular scenario gracefully.
        // See optimus.platform.dal.DSIClient#getDSIRequestSender
        case Some(DalBrokerClient.ShutdownCause.DuplicateCreation) => // Do nothing.
        case None                                                  =>
          // We invoke the callback, so that, in case there are any Streams associated with
          // this ProtoClient, they can be retried. If there are no Streams, then it is a noop.
          val excCause = new MessagesStreamDisconnectException(s"Client shutdown: ${toString}")
          listener.handleError(Some(this), new DALRetryableActionException(excCause.getMessage, excCause, Some(this)))
      }
    }

  def onMessage(message: MSNetMessage, dalProtocolOpt: Option[DalProtocolVersion]): Unit = ???
    /* withRShutdownLock {
      if (isRunning) {
        val envelope = DalRequestResponseEnvelope.parseFrom(message.getBytes)
        val header = envelope.getHeader
        val response = DalResponseProto.parseFrom(ProtoBufUtils.getCodedInputStream(envelope.getPayload))

        val sessionEstRes =
          if (response.hasEstablishSessionResult) {
            val res = fromProto(response.getEstablishSessionResult)
            listener.consumeEstablishSessionResult(res, clientSessionCtx)
            Some(res)
          } else
            None

        require(
          header.getStatus.getCode != DalProtocolStatus.Code.SUCCEEDED || envelope.hasPayload,
          "invalid response: no error, no payload")

        if (response.hasDsiResponse) {
          val reqUuid = response.getDsiResponse.getRequestUuid
          // Since publish commands are batched we expect unique seqId set for them
          if (header.hasSeqId && header.getSeqId != -1) {
            val seqId = header.getSeqId
            getBatch(seqId) match {
              case Some(batchCtx) =>
                handleBatchCmdResponse(response, sessionEstRes, batchCtx, seqId)
              case None =>
                log.error(
                  s"Received but not expected responses to request with sequence id $seqId, perhaps it's completed already? reqId  ${reqUuid}")
            }
          } else handleStreamCmdResponse(response)
        }
      }
    } */

  private def handleBatchCmdResponse(
      response: DalResponseProto,
      sessEstRes: Option[EstablishSessionResult],
      ctx: BatchContext,
      seqId: Int
  ): Unit = ??? /* {

    val dsiResponse = response.getDsiResponse
    val requestId = dsiResponse.getRequestUuid
    def failPendingRequest(throwable: Throwable, request: ClientRequest): Unit = {
      log.info(s"Failing request $request with exception: ${throwable.getMessage} ")
      request.completable.completeWithException(throwable)
    }

    val results: Seq[Result] = dsiResponse.getResultsList.asScala.map(fromProto)
    try {
      val requests = ctx.clientRequests
      require(requests.size == results.size, "Response result received must be equal to the requests sent for Messages")
      results.zip(requests).foreach { case (res, req) =>
        try {
          res match {
            case MessagesPublishSuccessResult(_) =>
              req.completable.completeWithResult(DSIResponse(results, sessEstRes, Some(this)))
            case StreamsACLsCommandSuccessResult(_) =>
              req.completable.completeWithResult(DSIResponse(results, sessEstRes, Some(this)))
            case ErrorResult(t: DsiSpecificTransientError, _) =>
              log.error(s"$toString received retryable error response!", t)
              failPendingRequest(new DALRetryableActionException(t.getMessage, t, Some(this)), req)
            case ErrorResult(t: MessagesPublishException, _) =>
              log.debug(s"$toString received error response!", t)
              failPendingRequest(t, req)
            case ErrorResult(t: StreamsACLsCommandException, _) =>
              log.debug(s"$toString received acls command error response!", t)
              failPendingRequest(t, req)
            case other =>
              log.error(s"$toString received some other response!", other)
              failPendingRequest(new BackendException(s"expected an ErrorResult, got: $other"), req)
          }
        } catch {
          case ex: Throwable =>
            log.error(s"Request with commands ${req.commands} in batch reqId ${ctx.requestUuid} failed")
            failPendingRequest(ex, req)
        }
        onBatchComplete(seqId)
        ClientRequestTracker.markComplete(requestId)
        val now = System.currentTimeMillis
        lazy val elapsed = now - ctx.started
        log.info(s"$requestId batch completed in $elapsed ms")
      }
    } catch {
      case ex: Throwable =>
        ctx.completeAllWithException(Some(this), ex)
        onBatchComplete(seqId)
        ClientRequestTracker.markComplete(requestId)
        log.info(s"$requestId batch failed ")
    }
  } */

  private def handleStreamCmdResponse(response: DalResponseProto): Unit = ??? /* {
    val dsiResponse = response.getDsiResponse
    val results: Seq[Result] = dsiResponse.getResultsList.asScala.map(fromProto)
    val filteredRes = results.flatMap {
      case msr: MessagesStreamResult       => Some(msr)
      case err: MessagesErrorResult        => Some(err)
      case err: ErrorResult                => Some(err)
      case _: MessagesPublishSuccessResult =>
        // This is the case where user don't care about publish ack.
        // This will also be triggered for publish Ack requests arriving after timeout
        None
      case otherResult =>
        log.error(s"$toString::Other result received: $otherResult")
        None // ignore all other results
    }
    if (filteredRes.nonEmpty) listener.handleResults(filteredRes)
  } */

  // TODO (OPTIMUS-52127): Handle Connect Disconnect events in reactive side
  def onDisconnect(): Unit = {
    log.error(s"$toString Received disconnect")
    shutdown(None)
  }

  // TODO (OPTIMUS-52127): Handle Connect Disconnect events in reactive side
  def onConnect(): Unit = {}

  override def brokerVirtualHostname: Option[String] = clientBrkCtx.brokerVirtualHostname
}
