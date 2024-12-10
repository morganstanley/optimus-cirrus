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
package optimus.platform.dal.client.prc

import msjava.slf4jutils.scalalog.getLogger
import optimus.platform.dal.client.DalServiceResponse
import optimus.platform.dal.client.DalServiceResponseMessage
import optimus.platform.dal.client.MessageReceiver
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.prc.PrcExecutionException
import optimus.platform.dsi.bitemporal.proto.Dsi
import optimus.platform.dsi.bitemporal.proto.Dsi.ResultProto
import optimus.platform.dsi.bitemporal.proto.Prc.PrcSingleKeyResponseProto
import optimus.platform.dsi.protobufutils.BatchContext

import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/*
 * Wraps the response received from a PRC server for a single DHT Key. NB this may correspond to multiple "commands",
 * for instance we might batch together two Selects for the same ReferenceQuery at different temporalities. By design,
 * these will map to the same DHTKey (since the ReferenceQuery is the same), and so we will receive two results for
 * that single DHTKey (one per temporality) both within the PrcSingleKeyResponseProto. The wrapper also contains the
 * commandIndices to which those results correspond in the originating batch, and an isPartial marker for the results.
 * We expect the invariant to hold here that responseProto.getResultsCount == commandIndices.size.
 */
final case class PrcResponseWrapper(
    isPartial: Boolean,
    responseProto: PrcSingleKeyResponseProto,
    commandIndices: Seq[Int])
    extends DalServiceResponse {
  override def results: Seq[ResultProto] = ??? // responseProto.getResultsList.asScala
  override def establishSessionResult: Option[Dsi.EstablishSessionResultProto] = None
  override def timings: Option[Dsi.TimingsProto] = None
}

/*
 * Logically this serves as a response message received from the PRC server for a single request, analogous to a
 * DSIResponseProto we might get from a broker. In reality the situation is more complicated than this because of
 * SilverKing's architecture: a single batch of retrievals sent to SilverKing doesn't necessarily correspond to a
 * single request to a SilverKing server beyond the SK API level. The Try here wraps the possibility of some failure
 * either at the SK level (unexpected SK exceptions) or at the level of our client code (logic errors which might
 * result in unexpected exceptions).
 */
final case class DalPrcResponseMessage(seqId: Int, payload: Try[PrcResponseWrapper]) extends DalServiceResponseMessage {
  override def hasError: Boolean = payload.isFailure
  override def hasPayload: Boolean = payload.isSuccess
}

class PrcMessageReceiver(env: DalEnv, client: DalPrcClient, connectionDescription: String)
    extends MessageReceiver[DalPrcResponseMessage](
      env,
      client,
      connectionDescription,
      enableOutOfOrderCompletion = true) {
  import PrcMessageReceiver.log
  override def messageCallback(response: DalPrcResponseMessage): Unit = client.withRShutdownLock {
    super.messageCallback(response)
  }

  override protected def unsafeParsePayload(
      requestUuid: String,
      seqId: Int,
      batchContext: BatchContext,
      now: Long,
      elapsed: => Long,
      message: DalPrcResponseMessage): Unit = message match {
    case DalPrcResponseMessage(seqId, Success(payload)) =>
      unsafeParseResponse(requestUuid, seqId, System.currentTimeMillis, elapsed, batchContext, payload)
    case d @ DalPrcResponseMessage(_, Failure(_)) =>
      unsafeParseError(d)
  }

  override protected def unsafeParseError(response: DalPrcResponseMessage): Unit = {
    response.payload match {
      case Success(response) =>
        val msg = s"Tried to parse PRC error when a response was returned: $response"
        log.error(msg)
        throw new PrcExecutionException(msg)
      case Failure(ex) =>
        throw new PrcExecutionException("Error during PRC execution", ex)
    }
  }
}

object PrcMessageReceiver {
  private val log = getLogger(PrcMessageReceiver)
}
