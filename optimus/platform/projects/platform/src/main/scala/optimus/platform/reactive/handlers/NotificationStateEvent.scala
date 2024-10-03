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
package optimus.platform.reactive.handlers

import optimus.graph.tracking._
import optimus.platform.dal.DALPubSub.ClientStreamId
import optimus.platform.reactive.handlers.StateChangeEvent.StatusError

/**
 * The super class of all kinds of state event which usually indicates warning/error By handling its subclasses,
 * reactive application especially UI ones can show error information to users rather than check logs to figure out what
 * happened
 *
 * Examples of how to define handler and the input binding, please
 * @see
 *   optimus.platform.graph.ticking.NotificationStatusHandlerTests#
 *   bindGlobalStatusSourceToHandler(optimus.platform.graph.ticking.HandlerEntity)
 * @see
 *   optimus.platform.graph.ticking.HandlerEntity
 */
sealed trait GlobalStateEvent extends WithEventCause {
  override def eventCause = Option.empty[EventCause]
}

/**
 * The super class of all kinds of reactive evaluator state events
 */
sealed trait ExecutionStateEvent extends GlobalStateEvent

/**
 * The super class of all kinds of notification state events
 */
sealed trait NotificationStateEvent extends GlobalStateEvent

/**
 * implementation of this represent a notification/reactive error which indicates
 *   1. an unrecoverable issue, or 2. recoverable but not handled exception, need investigation
 */
sealed trait ErrorEvent extends GlobalStateEvent

final case class NotificationClearEvent(
    original: NotificationWarningEvent
) extends NotificationStateEvent {
  override def toString: String = original.toClearString
}

/**
 * implementation of this represent a notification/reactive warning which indicates recoverable error
 */
sealed trait NotificationWarningEvent extends NotificationStateEvent {
  def toClearString: String

  def clear() = NotificationClearEvent(this)
}

/**
 * implementation of this represent an info
 */
sealed trait InfoEvent extends GlobalStateEvent

sealed trait StreamFailedEvent extends ExecutionStateEvent with ErrorEvent {
  val id: Option[Any]
  val reason: String
  val exception: Option[Throwable]
}
final case class InputFailedEvent(id: Option[Any], reason: String, error: StatusError) extends StreamFailedEvent {
  override def eventCause: Option[EventCause] = error.eventCause
  override val exception: Option[Throwable] = Some(error.reason)
}
final case class OutputFailedEvent(id: Option[Any], reason: String, exception: Option[Throwable])
    extends StreamFailedEvent
final case class DeferredFailedEvent(id: Option[Any], reason: String, exception: Option[Throwable])
    extends StreamFailedEvent
final case class PubSubStreamFailedEvent(id: ClientStreamId, exception: Throwable)
    extends NotificationStateEvent
    with ErrorEvent

// DAL Messaging / Contained Events
final case class ContainedEventStreamFailedEvent(
    id: String,
    exception: Throwable
) extends NotificationStateEvent
    with ErrorEvent
final case class ContainedEventSubscriptionFailedEvent(
    id: String,
    exception: Throwable
) extends NotificationStateEvent
    with ErrorEvent

// DAL Messaging / Upsertable Transaction
final case class UpsertableTransactionStreamFailedEvent(
    id: String,
    exception: Throwable
) extends NotificationStateEvent
    with ErrorEvent
final case class UpsertableTransactionSubscriptionFailedEvent(
    id: String,
    exception: Throwable
) extends NotificationStateEvent
    with ErrorEvent

final case class BrokerResetState(reason: String) extends NotificationWarningEvent {
  override def toString = s"Broker is unstable for $reason, need get the gap (maybe slow for a while)"
  override def toClearString = "Broker Reset state restored"
}

case object WorkerStarted extends NotificationStateEvent with InfoEvent {
  override def toString: String =
    "Notification Stream created successfully"
}

case object WorkerStopped extends NotificationStateEvent with InfoEvent {
  override def toString: String = "Notification worker thread stopped"
}

// Stream level events which are sent to each datasource's status handler
sealed trait StreamStatusEvent extends StateChangeEvent

object StreamStatusEvent {
  case object StreamCreated extends StreamStatusEvent {
    override def toString: String =
      "Notification Stream created"
  }

  case object StreamClosed extends StreamStatusEvent {
    override def toString: String =
      "Notification Stream closed"
  }

  case object StreamNotCreated extends StreamStatusEvent {
    override def toString: String =
      "Notification Stream creation failed"
  }

  case object StreamSubscriptionChanged extends StreamStatusEvent {
    override def toString: String =
      "Notification Stream subscriptions changed"
  }

  case object StreamSubscriptionNotChanged extends StreamStatusEvent {
    override def toString: String =
      "Notification Stream subscriptions change failed"
  }

  case object SowStarted extends StreamStatusEvent {
    override def toString: String =
      "Notification Stream SOW started"
  }

  case object SowCompleted extends StreamStatusEvent {
    override def toString: String =
      "Notification Stream SOW completed"
  }

  case object CatchupStarted extends StreamStatusEvent {
    override def toString: String =
      "Notification Stream catchup started"
  }

  case object CatchupCompleted extends StreamStatusEvent {
    override def toString: String =
      "Notification Stream catchup completed"
  }
}
