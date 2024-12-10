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
package optimus.platform.dsi.protobufutils

import com.google.protobuf.ByteString
import com.google.protobuf.MessageLite
// import msjava.base.spring.lifecycle.BeanState
import msjava.msnet._
import msjava.msnet.internal.MSNetProtocolConnectionConfigurationSupport
// import msjava.msnet.ssl.SSLEstablisherFactory
/* import msjava.protobufutils.generated.proto.Eai.RequestResponseEnvelope
import msjava.protobufutils.generated.proto.Eai.RequestResponseHeader */
import msjava.slf4jutils.scalalog.getLogger
import net.iharder.Base64
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.BreadcrumbsSendLimit.OnceByCrumbEquality
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.CrumbNodeType
import optimus.breadcrumbs.crumbs.EdgeType
import optimus.breadcrumbs.crumbs.EventCrumb
import optimus.breadcrumbs.crumbs.LogPropertiesCrumb
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.core.ChainedNodeID
import optimus.dal.ssl.DalSSLConfig
import optimus.graph.DiagnosticSettings._
import optimus.graph.Edges
import optimus.graph.PluginType
import optimus.graph.Settings
import optimus.graph.diagnostics.gridprofiler.GridProfiler
import optimus.graph.diagnostics.gridprofiler.CustomRegressionMetrics
import optimus.graph.diagnostics.messages.BookmarkCounter
import optimus.platform.dal.client.BatchedRequestSender
import optimus.platform.dal.client.BatchedRequestSenderFactory
import optimus.platform.dal.client.DalClientBatchTracker
import optimus.platform.dal.session.ClientSessionContext
import optimus.platform.dal.session.ClientSessionContext.SessionData
import optimus.platform.dsi.DalClientCrumbSource
import optimus.platform.dsi.DalEvents
import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.bitemporal.proto.CommandProtoSerialization
import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform.dsi.bitemporal.proto._
import optimus.platform.dsi.connection.DalProtocolVersion
import optimus.platform.dsi.connection.DalProtocolVersionEstablisher
import optimus.platform.dsi.connection.GpbWithDalRequest
import optimus.platform.dsi.protobufutils.DALProtoClient.DsiServer
import optimus.platform.dsi.protobufutils.DALProtoClient.VersioningServer
import optimus.platform.dsi.versioning.VersioningRedirectionInfo
import optimus.platform.internal.TemporalSource
import optimus.platform.util.PrettyStringBuilder
import optimus.utils.PropertyUtils
import optimus.utils.misc.Color

import java.io.File
import java.io.FileOutputStream
import java.io.PrintWriter
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

object RequestSender {
  final val RecordDALCmdFilePropName = "optimus.dsi.security.recordDALCmdFile"
  final val publishDalCmds = PropertyUtils.get("requestsender.publish.cmds", default = false)
}

final class RequestSender(
    client: DalClientBatchTracker with DalBrokerClient,
    senderFactory: BatchedRequestSenderFactory,
    baseContext: Context,
    clientAppId: ClientAppIdentifier)
    extends CommandProtoSerialization {
  import RequestSender.RecordDALCmdFilePropName
  // Not on companion because "overriding method log in trait Entity of type => msjava.slf4jutils.scalalog.Logger; value
  // log has weaker access privileges; it should not be private"
  private[this] val log = getLogger(this)
  private[this] val sequence = new AtomicInteger()
  private[this] val contextProto = toProto(baseContext)
  private[this] val clientAppIdProto = toProto(clientAppId)
  private[platform] val impl: BatchedRequestSender = senderFactory.createSender()

  private[this] lazy val dalCmdRecorderFileOpt: Option[File] = {
    def initRecordFile(fileName: String): File = {
      log.info(s"DAL Commands will be recorded in file: $fileName")
      val file = new File(fileName)
      if (file.exists) {
        log.warn(s"Cmd dump file exists with name $fileName. Deleting it.")
        file.delete
      }
      file
    }
    val fileName = getStringProperty(RecordDALCmdFilePropName, "")
    if (fileName.isEmpty) {
      // user didn't give the env var, hence not recording
      None
    } else {
      val file = initRecordFile(fileName)
      Some(file)
    }
  }

  def start(): Unit = {
    impl.start()
  }

  def shutdown(): Unit = {
    log.info("Shutting down RequestSender")
    impl.shutdown()
  }

  private[optimus] def assertConnected: Unit = {
    impl.assertConnected()
  }

  private[optimus] def sendBatch(
      clientSessionContext: ClientSessionContext,
      clientRequests: Vector[ClientRequest],
      requestType: DSIRequestProto.Type,
      token: Option[InFlightRequestToken] = None,
      redirectionInfo: Option[VersioningRedirectionInfo] = None
  ): Unit = ??? /* {

    val requestUuid = UUID.randomUUID
    val requestId = requestUuid.toString

    /*
      (on why it's acceptable to take first node ID from the client requests and use that for the entire batch)

      Even if there are multiple chainIDs in the request, we still generate edges to make the graph unambiguous at this line:
      nodeIDs.foreach { nid => Edges.ensureTracked(requestChainedId, nid, "sendBatch", CrumbNodeType.DataRequest, EdgeType.RequestedBy) }
      So if, for example, the two ids in the request are ABC#1 and ABC#2, we'll end up with a sub-graph like
         ABC#1    ABC#2
           \    /
          ABC#1#2
      In the graph database, these ids are just unique identifiers, so there's no information lost. The only thing we lose is
      the ability to infer topology by visually examining the strings. Since the most common mechanical inspection of the strings will be
      for the purpose of filtering the crumb stream to a single application, we're still OK, as all the ids still share the same root.
      Moreover, it's not going to be very common for there to be more than one chainID in a given request; there are basically
      two ways it could happen.
      One is that someone writes code with DAL requests embedded in track in a tight loop, e.g. keys.apar.map {k => track { Foo.get(k) }},
      leading to many chainIDs caught in a single batch.
      There's no good reason to do this, and the unnecessary scenario stack churn is significant enough to count as a bug.
      The less culpable way the situation could arise is if there are two simultaneous sequences of requests that get interleaved asynchronously,
      e.g.
        @node def getFoos(ks: String) = track("Getting Foos") { ks.apar.map(Foo.get(_)) }
        @node def getBars(ks: String) = track("Getting some Bars") {ks.apar.map(Bar.get(_)) }
        @node def getBoth = getFoos ++ getBars
      Again, this could happen, but in fact you have to try pretty hard to make it happen reliably, and as noted above,
      the consequences are purely visual.
      By contrast, transmitting a chainID along with each ClientRequest is problematic. If it's done naively, then, in the usual case
      where all the ids are the same anyway, we'd be transmitting an identical string many times in the same DAL request and on the broker side
      deserializing them into distinct ChainedID objects that compare equal. So then someone will suggest an elaborate interning scheme,
      which would introduce what I would consider to be unnecessary complexity.
     */
    val requestChainedId = clientRequests.headOption.map(_.nodeID.child).getOrElse(new ChainedID(requestUuid))

    val batchRetryManager = client.batchRetryManager
    val requestContext =
      BatchContext(
        requestId,
        requestChainedId,
        requestType,
        clientSessionContext,
        clientRequests,
        token,
        redirectionInfo,
        batchRetryManager)
    sendBatch(requestContext)
  } */

  /* private[optimus] def sendBatch(rc: BatchContext): Unit = {

    val commandLocations = CommandLocations.extract(rc.clientRequests)
    val commandTags = CommandAppNameTags.extract(rc.clientRequests)

    /*
     * we have to build the message as soon as possible so we can abort and throw if it's too big before all the
     * administration below begins - the less stuff we have to undo the better
     */
    val seqId = sequence.incrementAndGet
    val message = impl.buildMessage(seqId, rc, contextProto, clientAppIdProto, commandLocations, commandTags)

    val nodes = rc.clientRequests.map(_.completable).collect { case ns: NodeWithScheduler =>
      ns.node
    }
    PluginType.fire(nodes)

    // NB: this is about 2x faster (on my machine) than val nodeIDs = clientRequests.map(_.nodeID).toSet
    val nidArr = Array.ofDim[ChainedID](rc.clientRequests.length)
    var i = 0
    while (i < rc.clientRequests.length) { nidArr(i) = rc.clientRequests(i).nodeID; i += 1 }
    val nodeIDs = nidArr.toSet

    if (nodeIDs.size > 1)
      log.info(
        s"Multiple node ID's found in batch, will use chained ID ${rc.chainedId} for node ID's ${nodeIDs mkString ","}")

    try {
      nodeIDs.foreach { nid =>
        Edges.ensureTracked(rc.chainedId, nid, "sendBatch", CrumbNodeType.DataRequest, EdgeType.RequestedBy)
      }

      /*as we check commandLocation is nonEmpty, we should always get a clientRequest hence clientRequests.head is safe
       * We need clientRequests.head.completable to pass into customCounter for jobID and scope. The method is called in a
       * dal thread which doesn't have EvaluationContext.currentNode hence we need to borrow a clientRequest node.
       * */
      val requestCommandLocationProp =
        if (commandLocations.nonEmpty) {
          val completable = rc.clientRequests
            .find(_.completable.isInstanceOf[NodeWithScheduler])
            .map(_.completable.asInstanceOf[NodeWithScheduler])
          completable.foreach(n =>
            commandLocations.foreach(loc => {
              val locKey =
                if (scala.util.Properties.isLinux) loc.location.drop(1).dropRight(1) else loc.location.split(",").head
              GridProfiler.recordCustomCounter(
                CustomRegressionMetrics.dalPriqlCustomMetric,
                Map(locKey -> 1),
                node = n.node)
              BookmarkCounter.report(s"DAL Priql event at $loc", Color.ORANGE.getRGB)
            }))
          log.info(
            s"requestID=${rc.requestUuid} location of priql queries=${commandLocations map { _.location } mkString ","}")
          Some(Properties.requestCommandLocations -> CommandLocations.generateSummary(commandLocations))
        } else None
      val writeReqSpecificProps =
        if (rc.requestType == DSIRequestProto.Type.WRITE) {
          val anyWriteReq = rc.clientRequests.collectFirst { case req: WriteClientRequest => req }
          val re = anyWriteReq.flatMap(_.completable.env)
          val maybeEnv = re.flatMap(r => Option(r.config)).map(_.runtimeConfig.mode)
          Seq(Properties.`type` -> "write") ++ maybeEnv.map(env => Properties.env -> env)
        } else Seq.empty

      if (requestCommandLocationProp.nonEmpty || writeReqSpecificProps.nonEmpty) {
        val elems = Seq(Properties.dalReqUuid -> rc.requestUuid, Properties.batchSize -> rc.clientRequests.length) ++
          requestCommandLocationProp ++ writeReqSpecificProps
        Breadcrumbs.info(
          rc.chainedId,
          PropertiesCrumb(
            _,
            DalClientCrumbSource,
            elems: _*
          )
        )
      }

      rc.token.foreach(_.setUUID(rc.requestUuid))

      logDetails()
      if (Settings.traceDalStacksEnabled) traceDALStacks()
      dalCmdRecorderFileOpt.foreach(file => recordDALCmdToFile(rc.clientRequests, file))
      if (RequestSender.publishDalCmds) rc.clientRequests.foreach { req =>
        Breadcrumbs.info(
          req.nodeID,
          LogPropertiesCrumb(
            _,
            DalClientCrumbSource,
            Properties.requestId -> rc.chainedId,
            Properties.commands -> req.commands.map(_.toString)))
      }

      val totalCommands = rc.clientRequests.flatMap(_.commands)
      if (totalCommands.size == 1) {
        rc.clientRequests
          .find(_.completable.isInstanceOf[NodeWithScheduler])
          .foreach(req =>
            GridProfiler.recordCustomCounter(
              CustomRegressionMetrics.dal1of1CustomMetric,
              1,
              node = req.completable.asInstanceOf[NodeWithScheduler].node))
        BookmarkCounter.report(s"DAL(1of1) ${totalCommands.head.logDisplay(2)}", Color.BLUE.getRGB)
      }

      client.onBatchStart(seqId, rc)
      ClientRequestTracker.add(rc)

      assertConnected
      Breadcrumbs.trace(
        OnceByCrumbEquality,
        rc.chainedId,
        EventCrumb(_, DalClientCrumbSource, DalEvents.Client.RequestSubmitted))
      impl.asyncSend(message)

    } catch {
      case nr: DALNonRetryableActionException     => handleRequestSendFailure(nr)
      case mtl: DALClientMessageTooLargeException => throw mtl
      case ex: InterruptedException =>
        handleRequestSendFailure(new DALRetryableActionException(ex.toString, ex, Some(client)))
        throw ex
      case ex: Throwable =>
        handleRequestSendFailure(new DALRetryableActionException(ex.toString, ex, Some(client)))
    }

    def handleRequestSendFailure(t: Throwable): Unit = {
      log.error(logPrefix(rc.requestUuid, seqId) + "Send failed, setting all client requests to failed", t)
      client.onBatchComplete(seqId)
      rc.completeAllWithException(Some(client), t)
      ClientRequestTracker.markComplete(rc.requestUuid)
    }

    def traceDALStacks(): Unit = {
      val actualNodes = rc.clientRequests.map(_.completable).collect { case NodeWithScheduler(_, node, _, _) => node }
      val numNodes = actualNodes.size
      if (numNodes > 0 && numNodes <= Settings.traceDalStacksMaxBatch) {
        val psb = new PrettyStringBuilder
        (0 until numNodes) map { i =>
          psb.append(s"Node Stack for ${i + 1} of $numNodes \n")
          actualNodes(i).waitersToFullMultilineNodeStack(false, psb)
          log.info(psb.toString)
          psb.underlying.setLength(0)
        }
      }
    }

    def logDetails(): Unit = {
      if (log.isInfoEnabled()) {
        nodeIDs.foreach {
          ChainedNodeID.withNodeIDOverride(_) { log.info(s"requestID=${rc.requestUuid} chainedID=${rc.chainedId}") }
        }
        val lvl = if (log.isDebugEnabled()) CmdLogLevelHelper.DebugLevel else CmdLogLevelHelper.InfoLevel
        val cmdDetails = rc.cmdDetails(lvl)
        cmdDetails foreach { details =>
          log.info(logPrefix(rc.requestUuid, seqId) + details)
        }
      }
    }

    def recordDALCmdToFile(requests: Seq[ClientRequest], file: File): Unit = {
      def saveToFile(cmds: Seq[String], file: File): Unit = {
        log.debug(s"Writing command to file ${file.getAbsolutePath}")
        val p = new PrintWriter(new FileOutputStream(file, true))

        val tryWrite = Try(cmds.foreach(p.println))
        if (tryWrite.isFailure)
          log.warn(s"Writing command to file ${file.getAbsolutePath} failed", tryWrite.failed.get)
        p.close()
      }

      val commands = requests.flatMap(_.commandProtos)
      val encodedCmds = commands map { cmd =>
        Base64.encodeBytes(cmd.toByteArray)
      }
      saveToFile(encodedCmds, file)
    }
  } */
}

object TcpRequestSender {
  private val log = getLogger(this)

  val dalProtocolVersionEstablisherEnabled: Boolean =
    !java.lang.Boolean.getBoolean(
      "optimus.platform.dsi.protobufutils.TcpRequestSender.dalProtocolVersionEstablisherDisabled")

  val socksProxy: String =
    getStringProperty("optimus.platform.dsi.socksproxy", "")
  type MessageType = Array[Byte]
}

class DALClientMessageTooLargeException(size: Int, maxSize: Int)
    extends Exception(s"DAL request size ($size) bytes exceeds the predefined max size ($maxSize) bytes")

private[optimus] class TcpRequestSender(
    client: DalBrokerClient,
    config: DALProtoClient.Configuration,
    configSupport: MSNetProtocolConnectionConfigurationSupport,
    secureTransport: Boolean,
    msgListener: Option[DalProtocolVersion] => MSNetConnectionListenerAdapter,
    clientDalProtocolVersion: Option[DalProtocolVersion])
    extends BatchedRequestSender
    with CommandProtoSerialization
    with VersioningProtoSerialization
    with TemporalContextSerialization {
  import TcpRequestSender._

  override type MessageType = TcpRequestSender.MessageType

  private[this] var connection: MSNetTCPConnection = null
  private[this] var remoteProtocolVersion: Option[DalProtocolVersion] = null
  // private[this] val state = new BeanState()
  private[optimus] def hostPort = ??? // configSupport.getHostPort

  private[this] def createConnection() = ??? /* {
    log.info(s"Creating new connection to $hostPort${if (secureTransport) " (secure)" else ""}")
    val connection = config.createConnection(configSupport)
    if (dalProtocolVersionEstablisherEnabled && !secureTransport) {
      clientDalProtocolVersion foreach { v =>
        connection.addEstablisher(DalProtocolVersionEstablisher(v))
      }
    }

    if (socksProxy.nonEmpty) {
      log.info(s"Will use socks proxy $socksProxy")
      connection.setSOCKSProxyAddress(new MSNetInetAddress(socksProxy))
    }

    // If this is a kerberised connection, ignore the SSL config
    if (!connection.getKerberos && DalSSLConfig.enabledClientSSL) {
      connection.addEstablisher(new SSLEstablisherFactory().createEstablisher())
    }
    if (secureTransport) {
      require(!DalSSLConfig.enabledClientSSL)
      val serviceHostname = client.brokerVirtualHostname.getOrElse {
        hostPort.lastIndexOf(':') match {
          case -1  => hostPort
          case pos => hostPort.slice(0, pos)
        }
      }
      connection.addEstablisher(new SSLEstablisherFactory(true, serviceHostname).createEstablisher())
      // regex-ignore-line (open-sourcing) has to persist unless lib is updated
      connection.setMasterEstablisherMode(MasterEstablisherMode.OFF)
    }
    // state.initializeIfNotInitialized()
    if (!connection.syncConnect(config.defaultConnectTimeout)) {
      throw new DALRetryableActionException(s"Failed to connect to $hostPort", Option(client))
    }
    remoteProtocolVersion =
      if (secureTransport) Some(GpbWithDalRequest)
      else DalProtocolVersionEstablisher.getRemoteDalProtocolVersion(connection)
    connection.addListener(msgListener(remoteProtocolVersion))
    connection.setImmutable(true)
    connection
  } */

  private[optimus] def start(): Unit =
    try {
      connection = createConnection()
      // state.startIfNotRunning()
    } catch {
      case NonFatal(ex) =>
        log.error("Exception caught in start(), about to shutdown()", ex)
        shutdown()
        throw ex
    }

  private[optimus] def shutdown(): Unit = ??? /* {
    Option(connection) foreach (_.close())
    Option(configSupport.getLoop) foreach (_.quit())
    Option(configSupport.getCallbackExecutor) foreach {
      case e: ExecutorService => e.shutdown()
      case _                  =>
    }
    // state.destroyIfNotDestroyed()
  } */

  private[optimus] def asyncSend(message: MessageType): Unit = ??? /* {
    val msg = new MSNetStringMessage(message)
    connection.asyncSend(msg)
  } */

  private[optimus] def assertConnected(): Unit = ??? /* {
    try {
      // state.throwIfNotRunning()
      if (!connection.isConnected && !connection.getRetryFlag) throw new MSNetException("DAL is not connected.")
    } catch {
      case e: IllegalStateException => {
        log.error(s"The DAL broker is not responding; will retry in a few seconds")
        throw new DALRetryableActionException(e.toString, Option(client))
      }
      case e: Exception => {
        log.error(s"Caught exception when preparing to send the DAL request ${e.getClass.getName} - ${e.getMessage}", e)
        throw new DALRetryableActionException(e.toString, Option(client))
      }
    }
  } */

  // necessary for tests
  protected[optimus] def dalRequestRemoteProtocolVersion: Boolean =
    remoteProtocolVersion.exists(_.isSince(GpbWithDalRequest))

  // This doesn't take a DSIRequestProto because if it did we'd have to strip a bunch of stuff off the DSIRequestProto and move it to
  // a DalRequestProto in the case that the client & server support DalRequestProto.
  // NB it is not necessary that requests.nonEmpty because we want to be able to support DalRequestProtos without a DSIRequestProto on.
  private[optimus] def buildRequest(
      chainedId: ChainedID,
      requestUuid: String,
      requests: Vector[ClientRequest],
      sessionData: SessionData,
      contextProto: ContextProto,
      clientAppIdProto: ClientAppIdentifierProto,
      tpe: DSIRequestProto.Type,
      commandLocations: Seq[CommandLocation],
      commandTags: Seq[CommandAppNameTag],
      minAssignableTtOpt: Option[Instant],
      lastWitnessedTxTimeOpt: Option[Instant],
      redirectionInfo: Option[VersioningRedirectionInfo] = None): MessageLite = ??? /* {

    val cmdProtos = requests.flatMap(_.commandProtos)
    val commandLocationsProto = commandLocations map { CommandLocationSerializer.toProto } asJava
    val commandAppNameTagProto = commandTags map { CommandAppNameTagSerialization.toProto } asJava

    lazy val canBeSentWithoutSession = cmdProtos.size == 1 &&
      (cmdProtos.head.hasGetProtofileProperties || cmdProtos.head.hasDiscoverServices)
    require(
      sessionData.existingSession.isDefined || sessionData.establishSession.isDefined || canBeSentWithoutSession,
      "Cannot construct requests without an associated session."
    )

    if (dalRequestRemoteProtocolVersion) {
      // Build a DalRequestProto
      val builder = DalRequestProto.newBuilder
      if (cmdProtos.nonEmpty) {
        val dsiRequestBuilder = DSIRequestProto.newBuilder
          .addAllCommands(cmdProtos.asJava)
          .setType(tpe)
          .addAllCommandLocations(commandLocationsProto)
          .addAllCommandAppNameTags(commandAppNameTagProto)
        minAssignableTtOpt.foreach(tt => dsiRequestBuilder.setMinAssignableTt(toProto(tt)))
        lastWitnessedTxTimeOpt.foreach(tt => dsiRequestBuilder.setLastWitnessedTxTime(toProto(tt)))
        val dsiRequestProto = dsiRequestBuilder.build()
        config.serverType match {
          case DsiServer =>
            builder.setDsiRequest(dsiRequestProto)

          case VersioningServer =>
            val versioningRequestProto = VersioningRequestProto.newBuilder
              .setDsiRequest(dsiRequestProto)
              .setRedirectionInfo(toProto(redirectionInfo.get))
              .setClientTemporalContext(toProto(TemporalSource.loadContext))
            builder.setVersioningRequest(versioningRequestProto)
            sessionData.existingSession foreach { e =>
              builder.setEstablishSession(toProto((e.sessionId, e.reestablishCommand)))
            }
        }
      }
      builder.setChainedId(chainedId.toString)
      builder.setChainedIdLevel(chainedId.crumbLevel)
      builder.setChainedId2(toProto(chainedId))
      builder.setRequestUuid(requestUuid)
      sessionData.existingSession foreach { s =>
        builder.setSessionIdentifier(s.sessionId)
      }
      sessionData.establishSession foreach { e =>
        builder.setEstablishSession(toProto(e))
      }
      builder.addAllCloseSessionIdentifiers(sessionData.closeSessions.map(Integer.valueOf).toSeq.asJava)
      builder.build
    } else {
      require(cmdProtos.nonEmpty)
      // Build a DSIRequestProto. NB we can retire this option when all clients use DalRequestProto.
      val builder = DSIRequestProto.newBuilder
        .addAllCommands(cmdProtos.asJava)
        .setType(tpe)
        .setRequestUuid(requestUuid)
        .setContext(contextProto)
        .setClientAppIdentifier(clientAppIdProto)
        .addAllCloseSessionIdentifiers(sessionData.closeSessions.map(Integer.valueOf).toSeq.asJava)
        .addAllCommandLocations(commandLocationsProto)
        .addAllCommandAppNameTags(commandAppNameTagProto)
      minAssignableTtOpt.foreach(tt => builder.setMinAssignableTt(toProto(tt)))
      lastWitnessedTxTimeOpt.foreach(tt => builder.setLastWitnessedTxTime(toProto(tt)))
      sessionData.existingSession foreach { s =>
        builder.setSessionIdentifier(s.sessionId)
      }
      sessionData.establishSession foreach { e =>
        builder.setEstablishSession(toProto(e))
      }
      builder.build
    }
  } */

  override private[platform] def buildMessage(
      seqId: Int,
      rc: BatchContext,
      contextProto: ContextProto,
      clientAppIdProto: ClientAppIdentifierProto,
      commandLocations: Seq[CommandLocation],
      commandTags: Seq[CommandAppNameTag]): MessageType = {
    val req = buildRequest(
      rc.chainedId,
      rc.requestUuid,
      rc.clientRequests,
      rc.clientSessionContext.sessionData,
      contextProto,
      clientAppIdProto,
      rc.requestType,
      commandLocations,
      commandTags,
      rc.minAssignableTtOpt,
      rc.lastWitnessedTxTimeOpt,
      rc.redirectionInfo
    )
    buildMsnetMessageImpl(req.toByteString, seqId)
  }

  private[optimus] def buildMsnetMessageImpl(payload: ByteString, seqId: Int): Array[Byte] = ??? /* {
    val header = RequestResponseHeader.newBuilder
      .setSeqId(seqId)
      .build
    val envelope = RequestResponseEnvelope.newBuilder
      .setHeader(header)
      .setPayload(payload)
      .build
    val bytes = envelope.toByteArray
    assertMessageSizeUnderLimit(bytes.length)
    bytes
  } */

  private[this] def assertMessageSizeUnderLimit(size: Int): Unit = {
    val maxSize = config.maxMessageSize
    if (size > maxSize) {
      throw new DALClientMessageTooLargeException(size, maxSize)
    }
  }

  private[optimus] def enableReading(): Unit = ??? /* {
    Option(connection).foreach(_.enableReading)
  } */

  private[optimus] def disableReading(): Unit = ??? /* {
    Option(connection).foreach(_.disableReading)
  } */

  private[optimus] def disconnectConnection(): Unit = ??? /* {
    Option(connection).foreach(_.disconnect)
  } */
}
