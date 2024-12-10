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
package optimus.platform.dal.pubsub

import optimus.breadcrumbs.ChainedID
import optimus.dsi.pubsub.GlobalEvent
import optimus.dsi.pubsub.PubSubBrokerConnectEvent
import optimus.dsi.session.EstablishSessionResult
import optimus.platform.dal.session.ClientSessionContext
import optimus.platform.dsi.Response
import optimus.platform.dsi.bitemporal.PubSubCommand
import optimus.platform.dsi.bitemporal.Result
import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform.dsi.protobufutils.BatchContext
import optimus.platform.dsi.protobufutils.ClientRequest
import optimus.platform.dsi.protobufutils.CompletablePromise
import optimus.platform.dsi.protobufutils.DalBrokerClient
import optimus.platform.dsi.protobufutils.InFlightRequestToken
import optimus.platform.dsi.versioning.VersioningRedirectionInfo

import scala.concurrent.Promise

/**
 * This impl of BatchContext is actually a placeholder for PubSubCommands. Ideally, we should abstract out the minimum
 * requirements of Read and PubSubCommands into a trait above BatchContext
 */
class PubSubBatchContext(
    val cmds: Seq[PubSubCommand],
    override val requestUuid: String,
    override val chainedId: ChainedID,
    override val clientSessionContext: ClientSessionContext,
    appNameTag: Option[String])
    extends BatchContext {

  override val requestType: DSIRequestProto.Type = ??? // DSIRequestProto.Type.PUBSUB
  override val clientRequests: Vector[ClientRequest] = {
    val p: Promise[Response] = Promise()
    Vector(ClientRequest.read(CompletablePromise(p), chainedId, clientSessionContext, cmds, appNameTag = appNameTag))
  }
  override val token: Option[InFlightRequestToken] = None
  override val redirectionInfo: Option[VersioningRedirectionInfo] = None
  override val started: Long = System.currentTimeMillis()
  override val numResults: Int = 0
  override def expired(timeout: Int): Boolean = false

  override def completeAllWithException(client: Option[DalBrokerClient], ex: Throwable): Unit =
    throw new UnsupportedOperationException("PubSubBatchContext doesn't support this method!")
  override def completeResponses(
      client: DalBrokerClient,
      seqId: Int,
      now: Long,
      partial: Boolean,
      enableOutOfOrderCompletion: Boolean): Boolean =
    throw new UnsupportedOperationException("PubSubBatchContext doesn't support this method!")
  override def commandIdToClientRequestContext(i: Int): BatchContext.ClientRequestContext =
    throw new UnsupportedOperationException("PubSubBatchContext doesn't support this method!")
}

trait PubSubCallback {
  def handlePubSubResult(streamIdWithResult: Seq[(String, Result)]): Unit
  def handlePubSubException(failedClient: DalBrokerClient, th: Throwable): Unit
  def consumeEstablishSessionResult(result: EstablishSessionResult): Unit
  def streamValid(streamId: String): Boolean
  def remoteStreamCleanup(streamId: String): Unit
  def handlePubSubEvent(evt: GlobalEvent): Unit
}

class PubSubClientHandler(
    clientSessionContext: ClientSessionContext,
    onSuccess: Seq[(String, Result)] => Unit,
    onError: (DalBrokerClient, Throwable) => Unit,
    checkStreamValid: String => Boolean,
    doRemoteStreamCleanup: String => Unit,
    pubsubOnConnect: () => Unit
) extends PubSubCallback {
  override def handlePubSubResult(streamIdWithResult: Seq[(String, Result)]): Unit = onSuccess(streamIdWithResult)
  override def handlePubSubException(failedClient: DalBrokerClient, th: Throwable): Unit = onError(failedClient, th)
  override def consumeEstablishSessionResult(result: EstablishSessionResult): Unit =
    clientSessionContext.consumeEstablishSessionResult(result)
  override def streamValid(streamId: String) = checkStreamValid(streamId)
  override def remoteStreamCleanup(streamId: String): Unit = doRemoteStreamCleanup(streamId)
  override def handlePubSubEvent(evt: GlobalEvent): Unit = {
    evt match {
      case PubSubBrokerConnectEvent => pubsubOnConnect()
      case _                        =>
    }
  }
}

final case class PubSubContextMetadata(chainedID: ChainedID, appNameTag: Option[String])
