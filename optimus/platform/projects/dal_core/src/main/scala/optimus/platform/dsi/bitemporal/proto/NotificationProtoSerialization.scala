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
package optimus.platform.dsi.bitemporal.proto

import optimus.dsi.notification._
import net.iharder.Base64
import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform.storable._

import scala.jdk.CollectionConverters._
import optimus.platform.bitemporal.ValidSegment
import optimus.dsi.base.SlottedVersionedReference
import optimus.dsi.partitioning.DefaultPartition
import optimus.dsi.partitioning.NamedPartition
import optimus.dsi.partitioning.PartitionHelper

trait NotificationProtoSerializationBase extends BasicProtoSerialization {
  implicit val notifyHeartbeatSerializer: NotifyHeartbeatSerializer.type = NotifyHeartbeatSerializer
  implicit val resetStateSerialier: ResetStateSerializer.type = ResetStateSerializer
  implicit val beginTxnSerializer: BeginTransactionSerializer.type = BeginTransactionSerializer
  implicit val endTxnSerializer: EndTransactionSerializer.type = EndTransactionSerializer
  implicit val notificatinoEntrySerializer: NotificationEntrySerializer.type = NotificationEntrySerializer
  implicit val obliterateMessageSerializer: ObliterateMessageSerializer.type = ObliterateMessageSerializer
}

trait NotificationProtoSerialization extends NotificationProtoSerializationBase {
  final def toProto(msg: NotificationMessage): NotificationMessageProto = NotificationMessageSerializer.serialize(msg)
  final def fromProto(proto: NotificationMessageProto): NotificationMessage =
    NotificationMessageSerializer.deserialize(proto)
}

object NotificationMessageSerializer
    extends NotificationProtoSerializationBase
    with ProtoSerializer[NotificationMessage, NotificationMessageProto] {

  override def serialize(nm: NotificationMessage): NotificationMessageProto = ??? /* {
    val builder = NotificationMessageProto.newBuilder
    nm match {
      case hb: NotifyHeartBeat =>
        builder.setType(NotificationMessageProto.Type.HEARTBEAT).setHeartBeat(toProto(hb)).build
      case rs: ResetState =>
        builder.setType(NotificationMessageProto.Type.RESET_STATE).setResetState(toProto(rs)).build
      case bt: BeginTransaction =>
        builder.setType(NotificationMessageProto.Type.BEGIN_TRANSACTION).setBeginTransaction(toProto(bt)).build
      case et: EndTransaction =>
        builder.setType(NotificationMessageProto.Type.END_TRANSACTION).setEndTransaction(toProto(et)).build
      case e: NotificationEntry =>
        builder.setType(NotificationMessageProto.Type.NOTIFICATION_ENTRY).setNotificationEntry(toProto(e)).build
      case om: ObliterateMessage =>
        builder.setType(NotificationMessageProto.Type.OBLITERATE_MESSAGE).setObliterateMessage(toProto(om)).build
    }
  } */

  override def deserialize(nmp: NotificationMessageProto): NotificationMessage = ??? /* {
    nmp.getType match {
      case NotificationMessageProto.Type.HEARTBEAT          => fromProto(nmp.getHeartBeat)
      case NotificationMessageProto.Type.RESET_STATE        => fromProto(nmp.getResetState)
      case NotificationMessageProto.Type.BEGIN_TRANSACTION  => fromProto(nmp.getBeginTransaction)
      case NotificationMessageProto.Type.END_TRANSACTION    => fromProto(nmp.getEndTransaction)
      case NotificationMessageProto.Type.NOTIFICATION_ENTRY => fromProto(nmp.getNotificationEntry)
      case NotificationMessageProto.Type.OBLITERATE_MESSAGE => fromProto(nmp.getObliterateMessage)
    }
  } */
}

private[proto] object NotifyHeartbeatSerializer
    extends NotificationProtoSerializationBase
    with ProtoSerializer[NotifyHeartBeat, NotifyHeartBeatProto] {

  override def serialize(hb: NotifyHeartBeat): NotifyHeartBeatProto = ??? /* {
    val builder = NotifyHeartBeatProto.newBuilder
    builder
      .setTxTime(toProto(hb.txTime))
      .setContext(toProto(hb.context))
      .setPrimarySeq(hb.primarySeq)
    PartitionHelper.getPartitionNameOption(hb.partition).foreach(builder.setPartitionName)
    builder.build
  } */

  override def deserialize(proto: NotifyHeartBeatProto): NotifyHeartBeat = ??? /* {
    NotifyHeartBeat(
      fromProto(proto.getTxTime),
      if (proto.hasContext) fromProto(proto.getContext) else null,
      proto.getPrimarySeq,
      if (proto.hasPartitionName) PartitionHelper.getPartitionForString(proto.getPartitionName) else DefaultPartition
    ) // set to default value 0 if not found in proto
  } */
}

// NB: For ResetState, null context is for reset on all Context's from the server side.
//     It's sent when server starts, or restarts from failure.
private[proto] object ResetStateSerializer
    extends NotificationProtoSerializationBase
    with ProtoSerializer[ResetState, ResetStateProto] {

  override def serialize(rs: ResetState): ResetStateProto = ??? /* {
    val builder = ResetStateProto.newBuilder

    builder
      .setTxTime(toProto(rs.txTime))
      .setMessage(rs.message)
      .setPrimarySeq(rs.primarySeq)
    PartitionHelper.getPartitionNameOption(rs.partition).foreach(builder.setPartitionName)

    if (rs.context ne null)
      builder.setContext(toProto(rs.context))

    builder.build
  } */

  override def deserialize(proto: ResetStateProto): ResetState = ??? /* {
    ResetState.raw(
      fromProto(proto.getTxTime),
      if (proto.hasContext) fromProto(proto.getContext) else null,
      proto.getMessage,
      proto.getPrimarySeq,
      if (proto.hasPartitionName) PartitionHelper.getPartitionForString(proto.getPartitionName) else DefaultPartition
    )
  } */
}

private[proto] object BeginTransactionSerializer
    extends NotificationProtoSerializationBase
    with ProtoSerializer[BeginTransaction, BeginTransactionProto] {

  override def serialize(bt: BeginTransaction): BeginTransactionProto = ??? /* {
    val builder = BeginTransactionProto.newBuilder

    builder
      .setTxTime(toProto(bt.txTime))
      .setContext(toProto(bt.context))
      .setPrimarySeq(bt.primarySeq)
    PartitionHelper.getPartitionNameOption(bt.partition).foreach(builder.setPartitionName)

    builder.build
  } */

  override def deserialize(proto: BeginTransactionProto): BeginTransaction = ??? /* {
    BeginTransaction(
      fromProto(proto.getTxTime),
      if (proto.hasContext) fromProto(proto.getContext) else null,
      proto.getPrimarySeq,
      if (proto.hasPartitionName) PartitionHelper.getPartitionForString(proto.getPartitionName) else DefaultPartition
    )
  } */
}

private[proto] object EndTransactionSerializer
    extends NotificationProtoSerializationBase
    with ProtoSerializer[EndTransaction, EndTransactionProto] {

  override def serialize(et: EndTransaction): EndTransactionProto = ??? /* {
    val builder = EndTransactionProto.newBuilder
    builder
      .setTxTime(toProto(et.txTime))
      .setContext(toProto(et.context))
      .setPrimarySeq(et.primarySeq)
    PartitionHelper.getPartitionNameOption(et.partition).foreach(builder.setPartitionName)
    builder.build
  } */

  override def deserialize(proto: EndTransactionProto): EndTransaction = ??? /* {
    EndTransaction(
      fromProto(proto.getTxTime),
      if (proto.hasContext) fromProto(proto.getContext) else null,
      proto.getPrimarySeq,
      if (proto.hasPartitionName) PartitionHelper.getPartitionForString(proto.getPartitionName) else DefaultPartition
    )
  } */
}

// TODO (OPTIMUS-28487): Revert commit 2208323463741454f4109e1fd72c811069389272
private[optimus /*dsi*/ ] object NotificationEntrySerializer
    extends NotificationProtoSerializationBase
    with ProtoSerializer[NotificationEntry, NotificationEntryProto] {
  private val distinguishedSegmentVtIntervalBit = 1
  private val segmentTtFromPresenceBit = 2

  override def serialize(entry: NotificationEntry): NotificationEntryProto =
    ??? /* entry.proto({
      val builder = NotificationEntryProto.newBuilder

      builder
        .setNotificationType(NotificationEntryProto.NotificationType.forNumber(entry.tpe.id))
        .setTxTime(toProto(entry.txTime))
        .setVersionedRef(toProto(entry.slotRef.vref))
        .setSlot(entry.slotRef.slot)
        .setLockToken(entry.lockToken)
        .setContext(toProto(entry.context))
        .setPrimarySeq(entry.primarySeq)

      PartitionHelper.getPartitionNameOption(entry.partition).foreach(builder.setPartitionName)

      entry.segmentTtFromOption foreach { t =>
        builder.setSegmentTtFrom(toProto(t))
        builder.setFormat(builder.getFormat | segmentTtFromPresenceBit)
      }

      builder.setSegment({
        val affectedVtIntervalAndSegmentProtoBuilder =
          NotificationEntryProto.AffectedVtIntervalAndSegmentProto
            .newBuilder()
            .setVtInterval(toProto(entry.affectedVtInterval))
            .setSerializedEntity(toProto(entry.segment.data))

        if (entry.segment.vtInterval != entry.affectedVtInterval) {
          builder.setFormat(builder.getFormat | distinguishedSegmentVtIntervalBit)
          affectedVtIntervalAndSegmentProtoBuilder.setSegmentVtInterval(toProto(entry.segment.vtInterval))
        }
        affectedVtIntervalAndSegmentProtoBuilder
      })

      builder.build
    }) */

  override def deserialize(proto: NotificationEntryProto): NotificationEntry = ??? /* {
    val vref_ =
      if (proto.hasVersionedRef) fromProto(proto.getVersionedRef)
      else null
    val slot = if (proto.hasSlot) proto.getSlot else 0
    val notificationType = NotificationType(proto.getNotificationType.getNumber)
    val context = if (proto.hasContext) fromProto(proto.getContext) else null
    val txTime = fromProto(proto.getTxTime)
    val affectedVtInterval = fromProto(proto.getSegment.getVtInterval)
    val vref = SlottedVersionedReference(vref_, slot)
    val segment = ValidSegment[SerializedEntity](
      fromProto(proto.getSegment.getSerializedEntity),
      if ((proto.getFormat & distinguishedSegmentVtIntervalBit) == 0)
        affectedVtInterval
      else
        fromProto(proto.getSegment.getSegmentVtInterval)
    )
    val segmentTtFromOption =
      if ((proto.getFormat & segmentTtFromPresenceBit) == 0) None else Some(fromProto(proto.getSegmentTtFrom))
    val partition =
      if (proto.hasPartitionName) PartitionHelper.getPartitionForString(proto.getPartitionName) else DefaultPartition
    segmentTtFromOption match {
      case Some(segmentTtFrom) =>
        NotificationEntry(
          notificationType,
          context,
          txTime,
          affectedVtInterval,
          vref,
          proto.getLockToken,
          segment,
          segmentTtFrom,
          proto.getPrimarySeq,
          partition
        )
      case None =>
        NotificationEntry(
          notificationType,
          context,
          txTime,
          affectedVtInterval,
          vref,
          proto.getLockToken,
          segment,
          proto.getPrimarySeq,
          partition
        )
    }
  } */
}

private[proto] object ObliterateMessageSerializer
    extends NotificationProtoSerializationBase
    with ProtoSerializer[ObliterateMessage, ObliterateMessageProto] {

  override def serialize(om: ObliterateMessage): ObliterateMessageProto = ??? /* {
    val builder = ObliterateMessageProto.newBuilder
    builder
      .addAllEntityReferenceStrings((om.entityReferences map (c => Base64.encodeBytes(c.data))).asJava)
      .addAllTypes(om.types.asJava)
      .setTxTime(toProto(om.txTime))
      .setContext(toProto(om.context))
      .setPrimarySeq(om.primarySeq)
    PartitionHelper.getPartitionNameOption(om.partition).foreach(builder.setPartitionName)
    builder.build
  } */

  override def deserialize(o: ObliterateMessageProto): ObliterateMessage = ??? /* {
    ObliterateMessage(
      o.getEntityReferenceStringsList.asScala map (s => EntityReference.fromString(s)) toSeq,
      o.getTypesList.asScala toSeq,
      fromProto(o.getTxTime),
      if (o.hasContext) fromProto(o.getContext) else null,
      o.getPrimarySeq,
      if (o.hasPartitionName) NamedPartition(o.getPartitionName) else DefaultPartition
    )
  } */

}
