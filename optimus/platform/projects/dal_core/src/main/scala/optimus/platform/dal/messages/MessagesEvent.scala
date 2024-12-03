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

import optimus.dsi.pubsub.Subscription
import optimus.platform.dal.NotificationStreamCallback
import optimus.platform.storable.SerializedContainedEvent
import optimus.platform.storable.SerializedUpsertableTransaction

sealed trait MessagesEvent

object MessagesEvent {

  sealed trait GlobalEvent extends MessagesEvent
  sealed trait StreamEvent extends MessagesEvent {
    val streamId: String
  }
  sealed trait StreamErrorEvent extends StreamEvent {
    val error: Throwable
  }

  final case class StreamCreationSucceeded(streamId: String) extends StreamEvent
  final case class StreamCreationFailed(streamId: String, error: Throwable) extends StreamErrorEvent

  final case class StreamCloseSucceeded(streamId: String) extends StreamEvent

  final case class SubscriptionChangeSucceeded(changeId: Int, streamId: String) extends StreamEvent
  final case class SubscriptionChangeFailed(changeId: Int, streamId: String, error: Throwable) extends StreamErrorEvent

  sealed trait StreamNotificationEvent extends StreamEvent {
    def commitId: Long
    def className: String
  }

  // used by DAL.publishEvents API
  final case class MessagesDataNotification(
      streamId: String,
      eventClassName: String,
      payload: SerializedContainedEvent,
      commitId: Long,
      pubReqId: String
  ) extends StreamNotificationEvent {
    override def className: String = eventClassName
  }

  // used by DAL.publishTransactions API
  final case class MessagesTransactionNotification(
      streamId: String,
      className: String, // BusinessEvent className
      payload: SerializedUpsertableTransaction,
      commitId: Long,
      pubReqId: String,
      subscriptionId: Subscription.Id
  ) extends StreamNotificationEvent {
    def classNames: Seq[String] = payload.topLevelSerializedEntityClassNames
  }

  final case class MessagesStreamDisconnect(streamId: String, error: Throwable) extends StreamEvent

  final case class MessagesStreamError(streamId: String, error: Throwable) extends StreamErrorEvent

  /**
   * message is received when there is disconnection with messages broker
   */
  case object MessagesBrokerConnectEvent extends GlobalEvent

  /**
   * message is received when connection is successful with with messages broker
   */
  case object MessagesBrokerDisconnectEvent extends GlobalEvent
}

trait MessagesNotificationCallback
    extends NotificationStreamCallback[MessagesEvent.GlobalEvent, MessagesEvent.StreamEvent, String] {
  override def notifyGlobalEvent(id: String, msg: MessagesEvent.GlobalEvent): Unit
  override def notifyStreamEvent(id: String, msg: MessagesEvent.StreamEvent): Unit
}
