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

import optimus.breadcrumbs.ChainedID
import optimus.dsi.session.EstablishSessionResult
import optimus.logging.LoggingInfo
import optimus.platform.async
import optimus.platform.dal.ClientBrokerContext
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.messages.MessagesDsiProxyBase._
import optimus.platform.dal.messages.MessagesSubscription.EventClassName
import optimus.platform.dal.session.ClientSessionContext
import optimus.platform.dsi.Response
import optimus.platform.dsi.bitemporal.ChangeMessagesSubscription
import optimus.platform.dsi.bitemporal.ClientAppIdentifier
import optimus.platform.dsi.bitemporal.CloseMessagesStream
import optimus.platform.dsi.bitemporal.Command
import optimus.platform.dsi.bitemporal.CommitMessagesStream
import optimus.platform.dsi.bitemporal.CreateMessagesStream
import optimus.platform.dsi.bitemporal.StreamsACLsCommand
import optimus.platform.dsi.bitemporal.MessagesPublishCommand
import optimus.platform.dsi.bitemporal.MessagesPublishCommandBase
import optimus.platform.dsi.bitemporal.MessagesPublishTransactionCommand
import optimus.platform.dsi.bitemporal.MessagesResult
import optimus.platform.dsi.bitemporal.Result
import optimus.platform.dsi.bitemporal.proto.Dsi.DSIRequestProto
import optimus.platform.dsi.protobufutils.BatchContext
import optimus.platform.dsi.protobufutils.DalBrokerClient
import optimus.platform.dsi.versioning.VersioningRedirectionInfo

import java.time.Instant
import java.util.UUID

object MessagesClient {

  private[optimus] def generateMessagesPayload(
      cmd: MessagesPublishCommandBase,
      ctx: ClientBrokerContext
  ): MessagesPayloadBase = cmd match {
    case publishEvent: MessagesPublishCommand =>
      MessagesPayload(
        publishTime = Instant.now,
        clientAppId = ClientAppIdentifier(ctx.zoneId, ctx.appId),
        user = LoggingInfo.getUser,
        publisherChainId = ChainedID.root,
        rId = UUID.randomUUID().toString,
        message = publishEvent.msg
      )
    case publishTransaction: MessagesPublishTransactionCommand =>
      MessagesTransactionPayload(
        publishTime = Instant.now,
        clientAppId = ClientAppIdentifier(ctx.zoneId, ctx.appId),
        user = LoggingInfo.getUser,
        publisherChainId = ChainedID.root,
        rId = UUID.randomUUID().toString,
        message = publishTransaction.msg
      )
  }
}

trait MessagesClient extends DalBrokerClient {

  def env: DalEnv
  def listener: MessagesClientEventsListener
  def clientBrkCtx: ClientBrokerContext
  def clientName: String

  // TODO (OPTIMUS-64719): only in memory supports this right now, redesign and implement for non-mock environments
  private[optimus] def setStreamsConfig(classNames: Seq[String]): Unit = ()

  @async def publish(msgs: Seq[MessagesPublishCommandBase]): Seq[MessagesResult]

  def createStream(cmd: CreateMessagesStream): Unit
  def closeStream(cmd: CloseMessagesStream): Unit
  def updateSubscription(cmd: ChangeMessagesSubscription, subs: Set[EventClassName], unsubs: Set[EventClassName]): Unit
  def commitStream(cmd: CommitMessagesStream): Unit
  def checkEntitlementAndSetACLs(cmd: StreamsACLsCommand): MessagesResult

  @async override final def request(
      commands: Seq[Command],
      requestType: DSIRequestProto.Type,
      clientSessionCtx: ClientSessionContext,
      redirectionInfo: Option[VersioningRedirectionInfo],
      separateWriteBatcher: Boolean
  ): Response =
    throw new NotImplementedError("Not implemented request method for Messages Client")
}

trait MessagesClientEventsListener {
  def handleResults(results: Seq[Result]): Unit
  def handleError(failedClient: Option[DalBrokerClient], th: Throwable): Unit
  def consumeEstablishSessionResult(result: EstablishSessionResult, clientSessionCtx: ClientSessionContext): Unit
}
