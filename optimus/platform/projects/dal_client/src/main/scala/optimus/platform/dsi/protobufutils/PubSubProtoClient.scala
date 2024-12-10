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

import java.net.URI
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.google.protobuf.ByteString
// import msjava.base.spring.lifecycle.BeanState
import msjava.msnet.MSNetMessage
import msjava.protobufutils.server.BackendException
import msjava.slf4jutils.scalalog.getLogger
import optimus.dsi.base.UriSchemeChecker
import optimus.dsi.notification.NotificationEntry
import optimus.dsi.pubsub.PubSubBrokerConnectEvent
import optimus.platform._
import optimus.platform.dal._
import optimus.platform.dal._
import optimus.platform.dal.pubsub.PubSubCallback
import optimus.platform.dal.session.ClientSessionContext
import optimus.platform.dsi.Response
import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.bitemporal.proto.CommandProtoSerialization
import optimus.platform.dsi.bitemporal.proto.Dsi._
/* import optimus.platform.dsi.bitemporal.proto.Envelope.DalProtocolStatus
import optimus.platform.dsi.bitemporal.proto.Envelope.DalRequestResponseEnvelope */
import optimus.platform.dsi.bitemporal.proto.NotificationEntrySerialization
import optimus.platform.dsi.connection.DalProtocolVersion
import optimus.platform.dsi.versioning.VersioningRedirectionInfo

import scala.jdk.CollectionConverters._
import optimus.dal.ssl.DalSSLConfig
import optimus.platform.runtime.UriUtils

object PubSubProtoClient {
  private val log = getLogger(this)
  private[PubSubProtoClient] val clientSerial = new AtomicInteger(0)

  def apply(
      uriString: String,
      clientContext: ClientContext,
      config: Option[DALProtoClient.Configuration],
      pubSubCallback: PubSubCallback,
      secureTransport: Boolean): PubSubProtoClient = {

    val actSecureTransport = secureTransport && DalSSLConfig.enabledZoneBasedClientSSL

    val (hostPort, kerberized) = {

      val uri = new URI(uriString)
      if (actSecureTransport)
        (
          s"${uri.getHost}:${Integer.parseInt(UriUtils.getQueryMap(uri)("sslPort"))}",
          UriSchemeChecker.isKerberized(uri))
      else
        (s"${uri.getHost}:${uri.getPort}", UriSchemeChecker.isKerberized(uri))
    }

    new PubSubProtoClient(hostPort, kerberized, clientContext, config, pubSubCallback, actSecureTransport)
  }

  object AuxiliaryDataProcessor extends AuxiliaryDataProcessor

  abstract class AuxiliaryDataProcessor extends NotificationEntrySerialization {
    protected def handleNotificationEntries(entryData: Iterable[ByteString]): Seq[NotificationEntry] = ??? /* {
      entryData.map(NotificationEntryProto.parseFrom).map(fromProto(_)).toVector
    } */

    def inline(results: Seq[Result], auxiliaryData: Iterable[ByteString]): Seq[Result] =
      results
        .scanLeft(auxiliaryData, None: Option[Result]) { case ((auxData, _), result) =>
          result match {
            case rnot: ReferentialPubSubNotificationResult =>
              val (entryData, remAuxData) = auxData.splitAt(rnot.numberOfOutOfLineEntries)
              val entries = handleNotificationEntries(entryData)
              val cnot = DirectPubSubNotificationResult(
                rnot.streamId,
                rnot.writeReqId,
                rnot.txTime,
                rnot.entries.map(r => PubSubNotificationResult.SimpleEntry(r.subId, entries(r.index)))
              )
              remAuxData -> Some(cnot)
            case rnotp: ReferentialPubSubNotificationPartialResult =>
              val (entryData, remAuxData) = auxData.splitAt(rnotp.numberOfOutOfLineEntries)
              val entries = handleNotificationEntries(entryData)
              val cnot = DirectPubSubNotificationPartialResult(
                rnotp.streamId,
                rnotp.writeReqId,
                rnotp.txTime,
                rnotp.entries.map(r => PubSubNotificationResult.SimpleEntry(r.subId, entries(r.index))),
                rnotp.isLast
              )
              remAuxData -> Some(cnot)
            case _ =>
              auxData -> Some(result)
          }
        }
        .flatMap(_._2)
  }
}

class PubSubProtoClient(
    val hostPort: String,
    val kerberized: Boolean,
    val clientCtx: ClientContext,
    configOpt: Option[DALProtoClient.Configuration] = None,
    pubSubCallback: PubSubCallback,
    val secureTransport: Boolean)
    extends DalBrokerClient
    with CommandProtoSerialization {
  outer =>
  import PubSubProtoClient._

  private val config: DALProtoClient.Configuration =
    configOpt.getOrElse(new DALProtoClient.Configuration(clientCtx.asyncConfig))

  private[this] val sequence = new AtomicInteger

  private[this] val serial = clientSerial.incrementAndGet
  override val toString = s"$clientName:$hostPort"
  override def clientName: String = s"${config.name}#$serial"

  // necessary to prevent deadlock on shutdown call
  private val rwShutdownLock: ReadWriteLock = new ReentrantReadWriteLock
  private val rShutdownLock = rwShutdownLock.readLock
  private val wShutdownLock = rwShutdownLock.writeLock

  // private[this] val state = new BeanState()
  // state.initializeIfNotInitialized()

  private[this] val configSupport = config.createConfigSupport(hostPort, kerberized, clientName, secureTransport)
  private[this] val msgListener = (dalProtocolOpt: Option[DalProtocolVersion]) =>
    new DALClientMessageListener(toString, dalProtocolOpt, onMessage, onDisconnect _, onConnect _)

  private[optimus] final val tcpRequestSender: TcpRequestSender =
    config.createTcpRequestSender(this, configSupport, secureTransport, msgListener)

  override def connectionString: String = hostPort

  @async override def request(
      cmds: Seq[Command],
      requestType: DSIRequestProto.Type,
      clientSessionCtx: ClientSessionContext,
      redirectionInfo: Option[VersioningRedirectionInfo] = None,
      separateWriteBatcher: Boolean): Response =
    throw new UnsupportedOperationException(s"PubSubProtoClient doesn't support request, but received cmds: $cmds")

  override private[optimus] def sendBatch(batchContext: BatchContext): Unit = {
    tcpRequestSender.assertConnected()

    val request = tcpRequestSender.buildRequest(
      batchContext.chainedId,
      batchContext.requestUuid,
      batchContext.clientRequests,
      batchContext.clientSessionContext.sessionData,
      toProto(clientCtx.context),
      toProto(ClientAppIdentifier(clientCtx.zoneId, clientCtx.appId)),
      batchContext.requestType,
      Seq.empty,
      Seq.empty,
      minAssignableTtOpt = None,
      batchContext.lastWitnessedTxTimeOpt,
      batchContext.redirectionInfo
    )

    val seqId = sequence.incrementAndGet()

    // Log command details.
    val lvl = if (log.isDebugEnabled()) CmdLogLevelHelper.DebugLevel else CmdLogLevelHelper.InfoLevel
    val cmdDetails = batchContext.cmdDetails(lvl)
    cmdDetails foreach { details =>
      log.info(s"${logPrefix(batchContext.requestUuid, seqId)} $details, $toString")
    }

    val byteMessage: Array[Byte] = tcpRequestSender.buildMsnetMessageImpl(request.toByteString, seqId)
    try {
      tcpRequestSender.asyncSend(byteMessage)
    } catch {
      case e: Exception =>
        log.error(s"Caught exception while asyncSend from PubSubProtoClient ${e.getClass.getName} - ${e.getMessage}", e)
        throw new DALRetryableActionException(e.toString, Some(this))
    }
  }

  override def start(): Unit = {
    tcpRequestSender.start()
    // state.startIfNotRunning()
  }

  def shutdown(cause: Option[DalBrokerClient.ShutdownCause]): Unit = {
    if (isRunning) {
      wShutdownLock.lock
      try {
        if (isRunning) {
          // to make sure we do not execute the shutdown logic multiple times
          // we do this stopIfRunning and have this whole method body wrapped in isRunning check
          // state.stopIfRunning()
          log.info(s"Shutting down ${outer}")
          tcpRequestSender.shutdown()
          log.info(s"${config.name} shut down")
          cause match {
            // We do not propagate this shutdown further in case of duplicate proto client creation because
            // DSIClient is expected to handle this particular scenario gracefully.
            // See optimus.platform.dal.DSIClient#getDSIRequestSender
            case Some(DalBrokerClient.ShutdownCause.DuplicateCreation) => // Do nothing.
            case None                                                  =>
              // We invoke the callback, so that, in case there are any Streams associated with
              // this ProtoClient, they can be retried. If there are no Streams, then it is a noop.
              val excCause = new PubSubStreamDisconnectedException(s"Client shutdown: ${toString}")
              pubSubCallback.handlePubSubException(
                this,
                new DALRetryableActionException(excCause.getMessage, excCause, Some(this)))
          }
        }
      } finally {
        wShutdownLock.unlock
        // state.destroyIfNotDestroyed
      }
    }
  }
  def isRunning: Boolean = ??? // state.isRunning

  /* private def checkAndThrowHeaderError(envelope: DalRequestResponseEnvelope): Unit = {
    val response = DalResponseProto.parseFrom(ProtoBufUtils.getCodedInputStream(envelope.getPayload))
    if (response.hasDsiResponse) {
      val dsiResponse = response.getDsiResponse
      val results: Seq[Result] = dsiResponse.getResultsList.asScala.map(fromProto(_))
      results match {
        case Seq(ErrorResult(t: PubSubTransientException, _)) =>
          log.error(s"$toString received retryable error response!", t)
          throw new DALRetryableActionException(t.getMessage, t, Some(this))
        case Seq(ErrorResult(t, _)) =>
          log.debug(s"$toString received error response!", t)
          throw t
        case other =>
          log.error(s"$toString received some other response!", other)
          throw new BackendException(s"expected an ErrorResult, got: $other")
      }
    } else {
      throw new BackendException(envelope.getHeader.getStatus.getMessage)
    }
  }

  private def extractResults(
      dsiResponseProto: DSIResponseProto,
      auxiliaryData: Iterable[ByteString]): Seq[(String, Result)] = {
    val results: Seq[Result] =
      AuxiliaryDataProcessor.inline(dsiResponseProto.getResultsList.asScala.map(fromProto(_)), auxiliaryData)
    results.flatMap {
      case psr: PubSubResult if pubSubCallback.streamValid(psr.streamId)          => Some(psr.streamId -> psr)
      case psr: PubSubResult if !psr.isInstanceOf[ClosePubSubStreamSuccessResult] =>
        // this is the case when the stream needs to be cleaned up on the broker
        pubSubCallback.remoteStreamCleanup(psr.streamId)
        None
      case otherResult =>
        log.error(s"${toString}::Other result received: ${otherResult}")
        None // ignore all other results
    }
  } */

  def onMessage(message: MSNetMessage, dalProtocolOpt: Option[DalProtocolVersion]): Unit = ??? /* {
    rShutdownLock.lock
    try {
      if (isRunning) {
        val envelope = DalRequestResponseEnvelope.parseFrom(message.getBytes)
        val header = envelope.getHeader
        require(
          header.getStatus.getCode != DalProtocolStatus.Code.SUCCEEDED || envelope.hasPayload,
          "invalid response: no error, no payload")
        if (header.getStatus.getCode != DalProtocolStatus.Code.SUCCEEDED) {
          checkAndThrowHeaderError(envelope)
        } else {
          val dalResponseProto = DalResponseProto.parseFrom(ProtoBufUtils.getCodedInputStream(envelope.getPayload))

          if (dalResponseProto.hasEstablishSessionResult) {
            pubSubCallback.consumeEstablishSessionResult(fromProto(dalResponseProto.getEstablishSessionResult))
          }

          if (dalResponseProto.hasDsiResponse) {
            val dsiResponseProto = dalResponseProto.getDsiResponse
            val streamIdWithResult = extractResults(dsiResponseProto, envelope.getAuxiliaryDataList.asScala)
            require(dsiResponseProto.hasRequestUuid, "DSIResponse received without Request Uuid")
            val requestUuid = dsiResponseProto.getRequestUuid
            log.trace(s"Received results for uuid: ${requestUuid}, results: ${streamIdWithResult}")
            pubSubCallback.handlePubSubResult(streamIdWithResult)
          }
        }
      }
    } catch {
      case t: Throwable => pubSubCallback.handlePubSubException(this, t)
    } finally {
      rShutdownLock.unlock
    }
  } */

  def onDisconnect(): Unit = {
    log.error(s"${toString} Received disconnect")
    shutdown(None)
  }

  def onConnect(): Unit = {
    pubSubCallback.handlePubSubEvent(PubSubBrokerConnectEvent)
  }

  private[optimus] def enableReading(): Unit = {
    tcpRequestSender.enableReading()
  }

  private[optimus] def disableReading(): Unit = {
    tcpRequestSender.disableReading()
  }

  private[optimus] def disconnectConnection(): Unit = {
    tcpRequestSender.disconnectConnection()
  }

  override def brokerVirtualHostname: Option[String] = clientCtx.brokerVirtualHostname
}
