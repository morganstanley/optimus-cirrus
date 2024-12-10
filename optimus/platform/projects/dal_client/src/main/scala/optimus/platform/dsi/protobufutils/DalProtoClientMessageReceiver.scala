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
import msjava.msnet.MSNetMessage
// import msjava.protobufutils.generated.proto.Eai.RequestResponseEnvelope
import msjava.protobufutils.server.BackendException
import msjava.slf4jutils.scalalog.getLogger
import optimus.graph.DiagnosticSettings
import optimus.platform.dal.client.DalServiceResponse
import optimus.platform.dal.client.DalServiceResponseMessage
import optimus.platform.dal.client.MessageReceiver
import optimus.platform.dal.config.DalEnv
import optimus.platform.dsi.bitemporal.ErrorResult
import optimus.platform.dsi.bitemporal.Result
import optimus.platform.dsi.bitemporal.proto.CommandProtoSerialization
import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform.dsi.connection.DalProtocolVersion
import optimus.platform.dsi.connection.GpbWithDalRequest

import scala.jdk.CollectionConverters._

// TODO (OPTIMUS-15272): remove this
object DebugZeroResult {
  lazy val value = DiagnosticSettings.getBoolProperty("optimus.platform.dsi.protobufutils.debugZeroResult", false)
}

private[platform] final class DalBrokerResponseMessage(
    message: MSNetMessage,
    val protocolVersionOpt: Option[DalProtocolVersion])
    extends DalServiceResponseMessage {
  /* private lazy val envelope = RequestResponseEnvelope.parseFrom(ProtoBufUtils.getCodedInputStream(message.getBytes)) */
  /* private lazy val header = envelope.getHeader */

  override def hasError: Boolean = ??? // header.hasError
  override def hasPayload: Boolean = ??? // envelope.hasPayload
  override def seqId: Int = ??? // header.getSeqId
  def errorMessage: String = ??? // header.getError.getMessage
  def payload: ByteString = ??? // envelope.getPayload
  def getDalResponseProto: DalResponseProto = ??? /* DalResponseProto.parseFrom(ProtoBufUtils.getCodedInputStream(payload)) */
}

private[platform] final case class DsiResponseWrapper(underlying: DSIResponseProto)
    extends AnyVal
    with DalServiceResponse {
  override def isPartial: Boolean = ??? // underlying.getIsPartial
  override def results: Seq[ResultProto] = ??? // underlying.getResultsList.asScala
  override def commandIndices: Seq[Int] = ??? // underlying.getCommandIndicesList.asScala.map(_.intValue)
  override def establishSessionResult: Option[EstablishSessionResultProto] = ???
    /* if (underlying.hasEstablishSessionResult) Some(underlying.getEstablishSessionResult)
    else None */
  override def timings: Option[TimingsProto] = ???
    /* if (underlying.hasTimings) Some(underlying.getTimings)
    else None */
}

object DalProtoClientMessageReceiver {
  private val log = getLogger(DalProtoClientMessageReceiver)
}

class DalProtoClientMessageReceiver(
    env: DalEnv,
    client: DALProtoClient,
    connectionDescription: String,
    enableOutOfOrderCompletion: Boolean)
    extends MessageReceiver[DalBrokerResponseMessage](env, client, connectionDescription, enableOutOfOrderCompletion)
    with CommandProtoSerialization {
  import DalProtoClientMessageReceiver.log

  override protected def unsafeParsePayload(
      requestUuid: String,
      seqId: Int,
      batchContext: BatchContext,
      now: Long,
      elapsed: => Long,
      message: DalBrokerResponseMessage): Unit = ??? /* {
    if (message.protocolVersionOpt.exists(_.isSince(GpbWithDalRequest))) {
      log.trace(s"${logPrefix(requestUuid, seqId)} parsing DALResponseProto")
      // parse a DalResponseProto
      val response = DalResponseProto.parseFrom(ProtoBufUtils.getCodedInputStream(message.payload))
      if (response.hasEstablishSessionResult) {
        log.trace(s"${logPrefix(requestUuid, seqId)} consuming EstablishSessionResult within DAl response")
        batchContext.clientSessionContext.consumeEstablishSessionResult(fromProto(response.getEstablishSessionResult))
      }
      if (response.hasDsiResponse) {
        log.trace(s"${logPrefix(requestUuid, seqId)} parsing DsiResponseProto within Dal")
        unsafeParseResponse(requestUuid, seqId, now, elapsed, batchContext, DsiResponseWrapper(response.getDsiResponse))
      } else if (response.hasVersioningResponse) {
        unsafeParseVersioningResponse(requestUuid, seqId, now, elapsed, batchContext, response.getVersioningResponse)
      }
    } else {
      log.trace(s"${logPrefix(requestUuid, seqId)} parsing DsiResponseProto")
      // parse a DSIResponseProto
      val response = DSIResponseProto.parseFrom(ProtoBufUtils.getCodedInputStream(message.payload))
      if (response.hasEstablishSessionResult) {
        log.trace(s"${logPrefix(requestUuid, seqId)} consuming EstablishSessionResult within DSI response")
        batchContext.clientSessionContext.consumeEstablishSessionResult(fromProto(response.getEstablishSessionResult))
      }
      unsafeParseResponse(requestUuid, seqId, now, elapsed, batchContext, DsiResponseWrapper(response))
    }
  } */

  override protected def unsafeParseError(response: DalBrokerResponseMessage): Unit = ??? /* {
    val dalResponse = response.getDalResponseProto
    if (dalResponse.hasDsiResponse) {
      val dsiResponse = dalResponse.getDsiResponse
      val results: Seq[Result] = dsiResponse.getResultsList.asScala.map(fromProto)
      results match {
        case Seq(ErrorResult(t, _)) => throw t
        case other                  => throw new BackendException(s"expected an ErrorResult, got: $other")
      }
    } else {
      throw new BackendException(response.errorMessage)
    }
  } */
}
