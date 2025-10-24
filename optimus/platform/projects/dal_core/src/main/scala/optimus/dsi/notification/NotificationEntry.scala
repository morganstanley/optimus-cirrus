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
package optimus.dsi.notification

import optimus.dsi.base.SlottedVersionedReference
import optimus.dsi.base.dalQuantum
import optimus.dsi.partitioning.Partition
import optimus.dsi.trace.TraceId
import optimus.platform.TimeInterval
import optimus.platform.ValidTimeInterval
import optimus.platform.bitemporal.ValidSegment
import optimus.platform.dsi.bitemporal.Context
import optimus.platform.dsi.bitemporal.proto.Dsi.NotificationEntryProto
import optimus.platform.storable._

import java.time.Instant

// The enumeration entries should be one-on-one mapped to the types in NotificationEntryProto
object NotificationType extends Enumeration {
  type NotificationType = Value
  val ADD_ENTITY = Value(1)
  val INVALIDATE_ENTITY = Value(2)
}

sealed trait NotificationMessage {
  def txTime: Instant
  def context: Context
  def primarySeq: Long
  def messageType: NotificationMessageType
  def partition: Partition
  lazy val uuid: String = s"${getClass.getSimpleName}-${txTime}"
}
sealed trait TimeSequence {
  val txTime: Instant

  /**
   * Whether this TimeSequence Message tick TS forward only. Currently for transactions comes from sync up, we need
   * re-wind TS's time to release them correctly.
   */
  def forwardOnly: Boolean
}

/**
 * This message is used to re-wind reactive barrier's time to release sync up result properly
 */
final case class TimeReset(txTime: Instant) extends TimeSequence {
  override def forwardOnly: Boolean = false
}
final case class NotifyHeartBeat(txTime: Instant, context: Context, primarySeq: Long, partition: Partition)
    extends NotificationMessage
    with TimeSequence {
  override def forwardOnly: Boolean = true
  override def messageType: NotificationMessageType = NotificationMessageType.HeartBeat
}

// NB: The context of ResetState may be null, which is for reset of all contexts. This may happen
//     when the server starts, or restart from failure.
final case class ResetState private (
    txTime: Instant,
    context: Context,
    message: String,
    primarySeq: Long,
    partition: Partition)
    extends NotificationMessage {
  override def messageType: NotificationMessageType = NotificationMessageType.ResetState
}
final case class BeginTransaction(txTime: Instant, context: Context, primarySeq: Long, partition: Partition)
    extends NotificationMessage {
  override def messageType: NotificationMessageType = NotificationMessageType.BeginTransaction
}
final case class EndTransaction(
    txTime: Instant,
    context: Context,
    primarySeq: Long,
    partition: Partition,
    forwardOnly: Boolean = true)
    extends NotificationMessage
    with TimeSequence {
  override def messageType: NotificationMessageType = NotificationMessageType.EndTransaction
}

// XXX: collect all use case of ResetState
object ResetState {
  def raw(txTime: Instant, context: Context, message: String, primarySeq: Long, partition: Partition) =
    this(txTime, context, message, primarySeq, partition)

  def serverStart(txTime: Instant, context: Context, primarySeq: Long, partition: Partition) =
    this(txTime, context, "Server started", primarySeq, partition)
  def serverSideCPSChange(txTime: Instant, primarySeq: Long, partition: Partition) =
    this(txTime, null, "Server side CPS change", primarySeq, partition)
  def gapDetected(txTime: Instant, context: Context, primarySeq: Long, partition: Partition) =
    this(txTime, context, "Gap detected", primarySeq, partition)
  def transformException(txTime: Instant, context: Context, primarySeq: Long, partition: Partition) =
    this(txTime, context, "Unexpected exception during transforming/publishing replication unit", primarySeq, partition)
  def resubscribe(txTime: Instant, context: Context, primarySeq: Long, partition: Partition) =
    this(txTime, context, "Resubscribe notification", primarySeq, partition)
  def queueFull(txTime: Instant, context: Context, primarySeq: Long, partition: Partition) =
    this(txTime, context, "Notification queue is full. Discard all units queued on server.", primarySeq, partition)
  def publishFailed(txTime: Instant, context: Context, primarySeq: Long, partition: Partition) =
    this(txTime, context, "Failed to publish notification message", primarySeq, partition)
}

sealed trait NotificationEntry extends NotificationMessage {
  val tpe: NotificationType.Value
  val context: Context
  // XXX: If we expand txTime to [txTime, Inf), the following fields contains all fields of a PersistentEntity.
  //      In case that in future we may extend more related fields, we keep it flattened and not use PersistentEntity
  //      directly.
  val txTime: Instant
  val affectedVtInterval: ValidTimeInterval
  val slotRef: SlottedVersionedReference
  val lockToken: Long
  val segment: ValidSegment[SerializedEntity]
  val segmentTtFromOption: Option[Instant]
  val primarySeq: Long

  override def messageType: NotificationMessageType = tpe match {
    case NotificationType.ADD_ENTITY        => NotificationMessageType.AddEntry
    case NotificationType.INVALIDATE_ENTITY => NotificationMessageType.InvalidateEntry
  }

  // When segmentTtFromOption doesn't contain a value, we use 'txTime - dalQuantum' instead, so that the initiating
  // event of the entity version can always be found at (tt=segmentTtFrom, vt=segment.vtInterval.vtFrom).
  def segmentTtFrom: Instant = segmentTtFromOption.getOrElse(txTime.minus(dalQuantum))

  def copy(
      tpe: NotificationType.Value = tpe,
      context: Context = context,
      txTime: Instant = txTime,
      affectedVtInterval: ValidTimeInterval = affectedVtInterval,
      slotRef: SlottedVersionedReference = slotRef,
      lockToken: Long = lockToken,
      segment: ValidSegment[SerializedEntity] = segment,
      segmentTtFromOption: Option[Instant] = segmentTtFromOption,
      primarySeq: Long = primarySeq,
      partition: Partition = partition): NotificationEntry = {
    NotificationEntry.NotificationEntryImpl(
      tpe,
      context,
      txTime,
      affectedVtInterval,
      slotRef,
      lockToken,
      segment,
      segmentTtFromOption,
      primarySeq,
      partition)
  }
  @volatile private[this] var cachedProto: NotificationEntryProto = null
  def proto(serialize: => NotificationEntryProto): NotificationEntryProto = {
    if (cachedProto eq null) {
      synchronized {
        if (cachedProto eq null) cachedProto = serialize
      }
    }
    cachedProto
  }

  private[optimus] final def toPersistentEntity: PersistentEntity =
    segment.data.toPersistentEntity(slotRef.vref, lockToken, segment.vtInterval, TimeInterval(txTime))
}

object NotificationEntry {
  object AddEntity {
    def apply(
        context: Context,
        txTime: Instant,
        slotRef: SlottedVersionedReference,
        lockToken: Long,
        segment: ValidSegment[SerializedEntity],
        primarySeq: Long,
        partition: Partition): NotificationEntry = {
      NotificationEntry(
        NotificationType.ADD_ENTITY,
        context,
        txTime,
        segment.vtInterval,
        slotRef,
        lockToken,
        segment,
        txTime,
        primarySeq,
        partition)
    }

    def unapply(arg: NotificationEntry): Option[
      (Context, Instant, SlottedVersionedReference, Long, ValidSegment[SerializedEntity], Long, Partition)
    ] =
      Some(arg)
        .filter(_.tpe == NotificationType.ADD_ENTITY)
        .map(ne => (ne.context, ne.txTime, ne.slotRef, ne.lockToken, ne.segment, ne.primarySeq, ne.partition))
  }

  def apply(
      tpe: NotificationType.Value,
      context: Context,
      txTime: Instant,
      slotRef: SlottedVersionedReference,
      lockToken: Long,
      segment: ValidSegment[SerializedEntity],
      primarySeq: Long,
      partition: Partition): NotificationEntry = {
    NotificationEntryImpl(
      tpe,
      context,
      txTime,
      segment.vtInterval,
      slotRef,
      lockToken,
      segment,
      None,
      primarySeq,
      partition)
  }

  // New code should not use this method. When all CloseEntityTimeSlice carries 'ttFrom', this method can be removed,
  // then we can be sure that 'segmentFromTt' is always defined.
  def apply(
      tpe: NotificationType.Value,
      context: Context,
      txTime: Instant,
      affectedVtInterval: ValidTimeInterval,
      slotRef: SlottedVersionedReference,
      lockToken: Long,
      segment: ValidSegment[SerializedEntity],
      primarySeq: Long,
      partition: Partition): NotificationEntry = {
    NotificationEntryImpl(
      tpe,
      context,
      txTime,
      affectedVtInterval,
      slotRef,
      lockToken,
      segment,
      None,
      primarySeq,
      partition)
  }

  def deserialized(
      tpe: NotificationType.Value,
      context: Context,
      txTime: Instant,
      affectedVtInterval: ValidTimeInterval,
      slotRef: SlottedVersionedReference,
      lockToken: Long,
      segment: ValidSegment[SerializedEntity],
      primarySeq: Long,
      partition: Partition,
      referenceOnly: Boolean): NotificationEntry = {
    if (referenceOnly) {
      ReferenceOnlyNotificationEntry(
        tpe,
        context,
        txTime,
        affectedVtInterval,
        slotRef,
        lockToken,
        ValidSegment(segment.data, segment.vtInterval),
        None,
        primarySeq,
        partition)
    } else {
      NotificationEntryImpl(
        tpe,
        context,
        txTime,
        affectedVtInterval,
        slotRef,
        lockToken,
        ValidSegment(segment.data, segment.vtInterval),
        None,
        primarySeq,
        partition)
    }
  }

  def apply(
      tpe: NotificationType.Value,
      context: Context,
      txTime: Instant,
      affectedVtInterval: ValidTimeInterval,
      slotRef: SlottedVersionedReference,
      lockToken: Long,
      segment: ValidSegment[SerializedEntity],
      segmentTtFrom: Instant,
      primarySeq: Long,
      partition: Partition): NotificationEntry = {
    NotificationEntryImpl(
      tpe,
      context,
      txTime,
      affectedVtInterval,
      slotRef,
      lockToken,
      segment,
      Some(segmentTtFrom),
      primarySeq,
      partition)
  }

  def deserialized(
      tpe: NotificationType.Value,
      context: Context,
      txTime: Instant,
      affectedVtInterval: ValidTimeInterval,
      slotRef: SlottedVersionedReference,
      lockToken: Long,
      segment: ValidSegment[SerializedEntity],
      segmentTtFrom: Instant,
      primarySeq: Long,
      partition: Partition,
      referenceOnly: Boolean): NotificationEntry = {
    if (referenceOnly) {
      ReferenceOnlyNotificationEntry(
        tpe,
        context,
        txTime,
        affectedVtInterval,
        slotRef,
        lockToken,
        ValidSegment(segment.data, segment.vtInterval),
        Some(segmentTtFrom),
        primarySeq,
        partition)
    } else {
      NotificationEntryImpl(
        tpe,
        context,
        txTime,
        affectedVtInterval,
        slotRef,
        lockToken,
        ValidSegment(segment.data, segment.vtInterval),
        Some(segmentTtFrom),
        primarySeq,
        partition)
    }
  }

  def unapply(ne: NotificationEntry): Option[(
      NotificationType.Value,
      Context,
      Instant,
      SlottedVersionedReference,
      Long,
      ValidSegment[SerializedEntity],
      Long,
      Partition)] = {
    Some(ne.tpe, ne.context, ne.txTime, ne.slotRef, ne.lockToken, ne.segment, ne.primarySeq, ne.partition)
  }

  private[optimus] final case class LazyNotificationEntry(
      override val tpe: NotificationType.Value,
      override val context: Context,
      override val txTime: Instant,
      override val affectedVtInterval: ValidTimeInterval,
      override val slotRef: SlottedVersionedReference,
      override val lockToken: Long,
      override val segment: ValidSegment[SerializedEntity],
      override val segmentTtFromOption: Option[Instant],
      override val primarySeq: Long,
      partition: Partition,
      containsBlobRefOnly: Boolean,
      traceId: TraceId)
      extends NotificationEntry {
    def toEntry(svref: SlottedVersionedReference, se: SerializedEntity): NotificationEntry = {
      require(svref.vref == slotRef.vref, s"Illegal payload passed to toEntry: ${se}, ${svref}, ${slotRef}")
      NotificationEntryImpl(
        tpe,
        context,
        txTime,
        affectedVtInterval,
        svref,
        lockToken,
        ValidSegment(se, segment.vtInterval),
        segmentTtFromOption,
        primarySeq,
        partition)
    }
  }

  private[optimus] final case class NotificationEntryImpl private[NotificationEntry] (
      override val tpe: NotificationType.Value,
      override val context: Context,
      override val txTime: Instant,
      override val affectedVtInterval: ValidTimeInterval,
      override val slotRef: SlottedVersionedReference,
      override val lockToken: Long,
      override val segment: ValidSegment[SerializedEntity],
      override val segmentTtFromOption: Option[Instant],
      override val primarySeq: Long,
      partition: Partition)
      extends NotificationEntry

  /**
   * This type is only for DAL PubSub internal use. It should be converted to normal entry  before passing to client callback.
   * The properties of the serialised entity should be removed
   * The entity is retained so that type refs are avaliable
   */
  private[optimus] final case class ReferenceOnlyNotificationEntry private[NotificationEntry] (
      override val tpe: NotificationType.Value,
      override val context: Context,
      override val txTime: Instant,
      override val affectedVtInterval: ValidTimeInterval,
      override val slotRef: SlottedVersionedReference,
      override val lockToken: Long,
      override val segment: ValidSegment[SerializedEntity],
      override val segmentTtFromOption: Option[Instant],
      override val primarySeq: Long,
      partition: Partition)
      extends NotificationEntry {
    def toEntry(se: SerializedEntity): NotificationEntry = {
      NotificationEntryImpl(
        tpe,
        context,
        txTime,
        affectedVtInterval,
        slotRef,
        lockToken,
        ValidSegment(se, segment.vtInterval),
        segmentTtFromOption,
        primarySeq,
        partition)
    }
  }
}

object NotificationEntryOrder {
  val txTimeAndTpe = new Ordering[NotificationEntry] {
    override def compare(leftNE: NotificationEntry, rightNE: NotificationEntry): Int = {
      val txCompare = leftNE.txTime.compareTo(rightNE.txTime)
      if (txCompare == 0) leftNE.tpe compare rightNE.tpe else txCompare
    }
  }
}

// There's two types of obliteration:
//    1) By EntityReference
//       obliterateClass will be translated into a sequence of obliterate by entity reference actions
//    2) By Key
//       Generally, Key represents both @key and @indexed. However, for obliteration, only @key is allowed.
//       And, Key is one-on-one mapped to EntityReference, if there is.
//
// XXX: Assumption: entity references are only used for client side cache removal, and types are only used for
//                  CPS filtering and entitlement checking.
final case class ObliterateMessage(
    entityReferences: Seq[EntityReference],
    types: Seq[SerializedEntity.TypeRef],
    txTime: Instant,
    context: Context,
    primarySeq: Long,
    partition: Partition)
    extends NotificationMessage {
  override def messageType: NotificationMessageType = NotificationMessageType.Obliterate
}
