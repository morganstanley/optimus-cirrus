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

  /**
   * For consistency, reactive tests need to know whether messages were published in different test steps. This is
   * straightforward for DAL pubsub, because DAL pubsub is bitemporal. It is sufficient to wait for a tt tick to know
   * that we have received everything before that tick.
   *
   * This is more complicated for messages. We need to have some way of telling ReactiveSequence that it should wait to
   * receive things, but we don't want to create dependency on reactive in dal_client... but we also want things to be
   * testable easily without a complex test setup, because ReactiveSequence tests are done in user code (as they should!)
   * So we use tweaks, the standard optimus approaches to mutate global state.
   *
   * In prod, in most cases this won't be tweaked, which means that it is nearly free to call (due to the
   * WAS_NEVER_TWEAKED optimization). Accessing the tweak shouldn't cause scenario dependence issues either, because
   * publishing is impure.
   */
  @node(tweak = true) final def publishedMessagesListener: Option[MessagesPublishCommandBase => Unit] = None

  @async def publishEvent(
      event: BusinessEvent with ContainedEvent,
      option: MessagesPublishCommandBase.Option
  ): Unit = {
    val serMsg = ContainedEventSerializer.serialize(event)
    val command = MessagesPublishCommand(serMsg, option)
    val result = executeMessagesCommand(MessagesPublishCommand(serMsg, option))

    result match {
      case MessagesErrorResult(error) =>
        throw new MessagesPublishException(error.getMessage, error)
      case _ =>
        publishedMessagesListener.foreach(_.apply(command))
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
    val command = MessagesPublishTransactionCommand(serializedMsg)
    val result = executeMessagesCommand(command)

    result match {
      case res: MessagesErrorResult =>
        transaction.dispose()
        throw new MessagesPublishException(res.error.getMessage)
      case _ =>
        transaction.dispose()
        publishedMessagesListener.foreach(_.apply(command))
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
    val result = executeMessagesCommand(CreateMessagesClientStream(createStream, callback, env))
    result match {
      case MessagesErrorResult(error) => throw error
      case MessagesCreateClientStreamSuccessResult(id, stream: MessagesNotificationStream) =>
        require(id == streamId, s"NotificationStream returned for another streamId: $id, expected for $streamId")
        stream
      case _ => throw new IllegalArgumentException("Unexpected result type")
    }
  }

  @async private def executeMessagesCommand(
      cmd: MessagesCommand
  ): MessagesResult = {
    val result = dsi match {
      case clientSideDSI: ClientSideDSI => clientSideDSI.executeMessagesCommands(cmd :: Nil)
      case serverDsi                    => serverDsi.executeMessagesCommands(cmd :: Nil)
    }
    assert(result.length == 1, "sent one command, expected one result")
    result.head
  }

  @async def checkEntitlementAndSetACLs(streamAppId: String, acls: Seq[StreamsACLs]): Unit = {
    require(dsi.serverFeatures().supports(Feature.SetStreamsACLs), "Cannot run StreamsACLsCommand against this broker")
    val results = executeMessagesCommand(StreamsACLsCommand(streamAppId, acls))
    results match {
      case res: MessagesErrorResult => throw new StreamsACLsCommandException(res.error.getMessage)
      case _                        =>
    }
  }
}
