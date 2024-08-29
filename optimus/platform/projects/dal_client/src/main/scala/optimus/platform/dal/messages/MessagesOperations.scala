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

import optimus.platform._
import optimus.platform.dal.ClientSideDSI
import optimus.platform.dal.ContainedEventSerializer
import optimus.platform.dal.DSIResolver
import optimus.platform.dal.Transaction
import optimus.platform.dal.UpsertableTransactionSerializer
import optimus.platform.dsi.Feature
import optimus.platform.dsi.bitemporal.StreamsACLsCommandException
import optimus.platform.dsi.bitemporal.CreateMessagesClientStream
import optimus.platform.dsi.bitemporal.CreateMessagesStream
import optimus.platform.dsi.bitemporal.MessagesCommand
import optimus.platform.dsi.bitemporal.MessagesCreateClientStreamSuccessResult
import optimus.platform.dsi.bitemporal.StreamsACLsCommand
import optimus.platform.dsi.bitemporal.MessagesErrorResult
import optimus.platform.dsi.bitemporal.MessagesPublishCommand
import optimus.platform.dsi.bitemporal.MessagesPublishCommandBase
import optimus.platform.dsi.bitemporal.MessagesPublishException
import optimus.platform.dsi.bitemporal.MessagesPublishTransactionCommand
import optimus.platform.dsi.bitemporal.MessagesResult

import java.time.Instant

@entity
trait MessagesOperations { this: DSIResolver =>

  @async def publishEvent(
      event: BusinessEvent with ContainedEvent,
      option: MessagesPublishCommandBase.Option
  ): Unit = {
    val serMsg = ContainedEventSerializer.serialize(event)
    val results = executeMessagesCommands(MessagesPublishCommand(serMsg, option) :: Nil)
    results.foreach {
      case res: MessagesErrorResult => throw new MessagesPublishException(res.error.getMessage)
      case _                        =>
    }
  }

  @async def publishTransaction(
      transaction: Transaction
  ): Unit = {
    val serializedMsg = UpsertableTransactionSerializer.serialize(transaction)

    //// txn content check - fail-fast on client-side
    // 1) check txn content -- note: same is verified on dal broker side as well !
    serializedMsg.validate()

    // 2) txn content (writes/upserts) must goto the same DAL partition - must be enforced before publish
    // for now this is only done on dal broker side - there is no fail-fast client side check to do same

    val results = executeMessagesCommands(MessagesPublishTransactionCommand(serializedMsg) :: Nil)
    results.foreach {
      case res: MessagesErrorResult =>
        transaction.dispose()
        throw new MessagesPublishException(res.error.getMessage)
      case _ =>
        transaction.dispose()
    }
  }

  @async def createMessagesStream(
      streamId: String,
      subscriptions: Set[MessagesSubscription],
      callback: MessagesNotificationCallback,
      env: RuntimeEnvironment,
      consumerId: Option[String],
      startTime: Option[Instant] = None
  ): MessagesNotificationStream = {
    val createStream = CreateMessagesStream(streamId, subscriptions, consumerId, startTime)
    executeCreateStream(createStream, callback, env)
  }

  @async private def executeCreateStream(
      createStream: CreateMessagesStream,
      callback: MessagesNotificationCallback,
      env: RuntimeEnvironment
  ): MessagesNotificationStream = {
    val streamId = createStream.streamId
    val result = executeMessagesCommands(CreateMessagesClientStream(createStream, callback, env) :: Nil).head
    result match {
      case MessagesErrorResult(error) => throw error
      case MessagesCreateClientStreamSuccessResult(id, stream: MessagesNotificationStream) =>
        require(id == streamId, s"NotificationStream returned for another streamId: $id, expected for $streamId")
        stream
      case _ => throw new IllegalArgumentException("Unexpected result type")
    }
  }

  @async private def executeMessagesCommands(
      cmds: Seq[MessagesCommand]
  ): Seq[MessagesResult] = {
    dsi match {
      case clientSideDSI: ClientSideDSI => clientSideDSI.executeMessagesCommands(cmds)
      case serverDsi                    => serverDsi.executeMessagesCommands(cmds)
    }
  }

  @async def checkEntitlementAndSetACLs(appId: String, acls: Seq[StreamsACLs]): Unit = {
    require(dsi.serverFeatures().supports(Feature.SetStreamsACLs), "Cannot run StreamsACLsCommand against this broker")
    val results = executeMessagesCommands(StreamsACLsCommand(appId, acls) :: Nil)
    results foreach {
      case res: MessagesErrorResult => throw new StreamsACLsCommandException(res.error.getMessage)
      case _                        =>
    }
  }
}
