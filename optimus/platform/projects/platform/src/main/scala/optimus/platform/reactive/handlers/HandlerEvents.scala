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

import optimus.dsi.notification.NotificationEntry
import optimus.dsi.notification.NotificationType
import optimus.graph.tracking.EventCause
import optimus.platform.BusinessEvent
import optimus.platform.ContainedEvent
import optimus.platform.TimeInterval
import optimus.platform.storable.BusinessEventReference
import optimus.platform.storable.EntityReference
import optimus.platform.storable.StorableReference
import optimus.platform.storable.Entity
import optimus.platform.storable.PersistentEntity

import java.time.Instant

/**
 * this is the root of the parameter taken by a @handle
 */
/**
 * the base type of all event data that is passed to handlers
 */
sealed trait ReactiveEvent

/**
 * base for all "true" events. Excludes [[SignalAsEvent]]
 */
sealed trait BaseReactiveEvent extends ReactiveEvent

/**
 * events generated outside of the core optimus platform should inherit from this type, which is not sealed
 */
trait UserReactiveEvent extends BaseReactiveEvent

/**
 * SignalAsEvent covers the case where the handler views the impact of the event in terms of the changes to some data
 * typically expressed in terms of a PriQL expression.
 *
 * @tparam V
 *   is the value at the location L.
 */
sealed trait SignalAsEvent[+V] extends ReactiveEvent {
  def data: Seq[ChangedSignal[V]]
}

/**
 * an update containing the full dataset. This is provided when the data is first available
 */
final case class StateOfWorldUpdate[+V](
    eventTT: Instant,
    data: Seq[InsertedSignal[V]]
) extends SignalAsEvent[V] {
  override def toString: String =
    s"StateOfWorldUpdate(${if (data.isEmpty) "empty" else "first data: " + data.head}, dalTT: $eventTT)"
}

final case class SignalUpdated[+V](eventTT: Instant, data: Seq[ChangedSignal[V]]) extends SignalAsEvent[V]

sealed trait ContainedEventChangedSignal[+V <: BusinessEvent with ContainedEvent] {

  /**
   * the identity of the value. if updates are tracked over time this can be used to identify the object e.g. of V is a
   * Self-contained Event, this would be the BusinessEventReference
   */
  val id: BusinessEventReference
}

final case class ContainedEventInsertedSignal[V <: BusinessEvent with ContainedEvent](
    id: BusinessEventReference,
    value: V,
) extends ContainedEventChangedSignal[V]

/**
 * Event definition for @event(contained=true) use case.
 */
sealed trait ContainedEventSignalAsEvent[+V <: BusinessEvent with ContainedEvent] extends ReactiveEvent {
  def data: ContainedEventChangedSignal[V]
  private[reactive] def commitId: Long
}

/**
 * This update is from @event(contained=true) subscription.
 */
final case class ContainedEventSignalUpdated[+V <: BusinessEvent with ContainedEvent](
    data: ContainedEventChangedSignal[V],
    private[reactive] val commitId: Long
) extends ContainedEventSignalAsEvent[V]

/** Entity from Upsertable (delayed) Transaction. */
sealed trait TransactionEntitySignalAsEvent[+V] extends ReactiveEvent {
  def data: Seq[TransactionEntityChangedSignal[V]]
  private[reactive] def commitId: Long
}

/**
 * This update is from @stored @entity subscription from Upsertable (delayed) Transaction published as Message.
 */
final case class TransactionEntitySignalUpdate[+V <: Entity](
    streamId: String,
    tt: Instant,
    override val data: Seq[TransactionEntityChangedSignal[V]],
    private[reactive] val commitId: Long
) extends TransactionEntitySignalAsEvent[V]

/** BusinessEvent from Upsertable (delayed) Transaction. */
sealed trait TransactionEventSignalAsEvent[+V <: BusinessEvent] extends ReactiveEvent {
  def data: TransactionEventChangedSignal[V]
  private[reactive] def commitId: Long
}

/**
 * This update is from @stored @entity subscription from Upsertable (delayed) Transaction published as Message.
 */
final case class TransactionEventSignalUpdate[+V <: BusinessEvent](
    streamId: String,
    tt: Instant,
    override val data: TransactionEventChangedSignal[V],
    protected[reactive] val commitId: Long
) extends TransactionEventSignalAsEvent[V]

sealed trait TransactionEventChangedSignal[+V <: BusinessEvent] {

  /**
   * the identity of the value. if updates are tracked over time this can be used to identify the object e.g. of V is a
   * Transaction Event, this would be the BusinessEventReference
   */
  val id: BusinessEventReference
}

final case class TransactionEventInsertedSignal[V <: BusinessEvent](
    id: BusinessEventReference,
    value: V,
) extends TransactionEventChangedSignal[V]

sealed trait TransactionEntityChangedSignal[+V] {

  /**
   * the identity of the value. if updates are tracked over time this can be used to identify the object e.g. of V is a
   * Entity published via UpsertableTransaction, this would be the EntityReference
   */
  val id: EntityReference
}

final case class TransactionEntityInsertedSignal[+V <: Entity](
    eventId: BusinessEventReference,
    value: V,
    protected[reactive] val commitId: Long
) extends TransactionEntityChangedSignal[V] {
  override val id: EntityReference = value.dal$entityRef
}

sealed trait StreamEvent[+R]
object StreamEvent {

  /**
   * Single value received over a stream.
   */
  final case class Value[R](r: R) extends StreamEvent[R]

  // TODO (OPTIMUS-75668): Add other events of interests here, if needed.
}

object SimpleEvents {
  final case class SimpleValueEvent[T](value: T) extends BaseReactiveEvent
}

trait StateChangeEvent extends ReactiveEvent
object StateChangeEvent {
  final case object StatusInitialising extends StateChangeEvent
  final case object StatusStarted extends StateChangeEvent
  final case object StatusOperational extends StateChangeEvent
  final case object StatusStopped extends StateChangeEvent
  final case class StatusError(reason: Throwable, event: Option[Any] = None) extends StateChangeEvent
  object StatusError {
    // TODO (OPTIMUS-65703): Will remove the now unused parameter in a later PR.
    def unapply(x: StatusError): Option[(Throwable, Null, Option[Any])] = Some((x.reason, null, x.event))
  }
}

trait GlobalStatusEvent extends ReactiveEvent {
  type DetailType <: GlobalStateEvent
  val detail: DetailType
}

final case class InputFailedStatusEvent(override val detail: InputFailedEvent) extends GlobalStatusEvent {
  override type DetailType = InputFailedEvent
}

/**
 * indicates that the ticking TxTimeProvider's subscription has failed, so TickingTransactionTime.tickingNow may be
 * unavailable
 */
final case class PubSubTickingTxTimeFailedEvent(override val detail: InputFailedEvent) extends GlobalStatusEvent {
  type DetailType = InputFailedEvent
}

sealed trait NotificationUpdate extends ReactiveEvent with Ordered[NotificationUpdate] {
  def tt: Instant
  final override def compare(that: NotificationUpdate) = this.tt.compareTo(that.tt)
}
final case class EntityNotificationUpdate(
    tt: Instant,
    changedEntities: List[PersistentEntity],
    deletedEntities: List[PersistentEntity])
    extends NotificationUpdate
object EntityNotificationUpdate {
  def fromNotificationEntries(tt: Instant, entries: Seq[NotificationEntry]): EntityNotificationUpdate = {
    val (updates, deletes) = entries.foldLeft((List[PersistentEntity](), List[PersistentEntity]())) {
      case ((curUpdates, curDeletes), entry) =>
        entry.tpe match {
          case NotificationType.ADD_ENTITY =>
            (toPersistentEntity(entry, entry.txTime) :: curUpdates, curDeletes)
          case NotificationType.INVALIDATE_ENTITY =>
            (curUpdates, toPersistentEntity(entry, entry.segmentTtFrom) :: curDeletes)
          case _ =>
            throw new IllegalArgumentException(s"Unexpected NotificationType: ${entry.tpe}")
        }
    }
    new EntityNotificationUpdate(tt, updates, deletes)
  }

  private def toPersistentEntity(entry: NotificationEntry, ttFrom: Instant): PersistentEntity = {
    entry.segment.data.toPersistentEntity(
      entry.slotRef.vref,
      entry.lockToken,
      entry.segment.vtInterval,
      TimeInterval(ttFrom),
      None)
  }
}
