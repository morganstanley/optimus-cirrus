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

import java.time.Instant

import optimus.dsi.base.actions.PutAppEvent
import optimus.dsi.base.actions.PutBusinessEventTimeSlice
import optimus.dsi.base.actions.PutEntityTimeSlice
import optimus.dsi.base.actions.TxnAction
import optimus.platform.dsi.bitemporal._

import optimus.platform.dsi.bitemporal.proto.Dsi.UOWApplicationEventProto
import optimus.platform.dsi.bitemporal.proto.Dsi.UOWNotificationProto
import optimus.platform.dsi.bitemporal.proto.Dsi.PartitionedUOWNotificationProto
import optimus.platform.dsi.bitemporal.proto.Dsi.PartitionedUOWNotificationKeyProto

import scala.jdk.CollectionConverters._

private[proto] trait UnitOfWorkSerialization extends ProtoSerialization {
  implicit val instantSerializer: InstantSerializer.type = InstantSerializer
  implicit val businessEventReferenceSerializer: BusinessEventReferenceSerializer.type =
    BusinessEventReferenceSerializer
  implicit val appEventReferenceSerializer: AppEventReferenceSerializer.type = AppEventReferenceSerializer

  implicit val uowApplicationEventSerializer: UOWApplicationEventSerializer.type = UOWApplicationEventSerializer
  implicit val uowNotificationSerializer: UOWNotificationSerializer.type = UOWNotificationSerializer
  implicit val partitionedUowNotificationSerializer: PartitionedUOWNotificationSerializer.type =
    PartitionedUOWNotificationSerializer
  implicit val partitionedUOWNotificationKeyInfoSerializer: PartitionedUOWNotificationKeyInfoSerializer.type =
    PartitionedUOWNotificationKeyInfoSerializer
}

object UnitOfWorkSerialization extends UnitOfWorkSerialization {

  def prepareUnitOfWorkMessageB2B(
      actions: Seq[TxnAction],
      txTime: Instant,
      excludePrevBusEvent: Boolean): UOWNotificationProto = ??? /* {
    toProto(prepareUnitOfWorkMessageB2BUOWNotification(actions, txTime, excludePrevBusEvent))
  } */

  private[optimus] def prepareUnitOfWorkMessageB2BUOWNotification(
      actions: Seq[TxnAction],
      txTime: Instant,
      excludePrevBusEvent: Boolean): UOWNotification = {
    val appEvents = actions collect { case x: PutAppEvent => x }
    val uowAppEvents = appEvents.map { appEvent =>
      val appEventRef = appEvent.id

      def isTimeSliceWithBusinessEvent(putEntityTS: PutEntityTimeSlice): Boolean = {
        putEntityTS.seg.data match {
          // Excluding business event refs from cloneWithClosedVT timeslices
          case data: EvtTimeSliceData if excludePrevBusEvent => data.creationAppEvt == appEventRef
          // TODO (OPTIMUS-14120): Remove the option to use the legacy behavior and remove excludePrevBusEvents flag
          case data: EvtTimeSliceData => true
          case _                      => false
        }
      }

      val beRefs = actions.collect {
        case x: PutEntityTimeSlice if isTimeSliceWithBusinessEvent(x) => x.seg.data.asInstanceOf[EvtTimeSliceData].evtId
      }.distinct
      // This is the case where receive empty business event with no entity update inside it
      val bes = if (beRefs.isEmpty) actions.collect {
        case bts: PutBusinessEventTimeSlice if bts.appEventId == appEventRef => bts.eventRef
      }.distinct
      else beRefs
      UOWApplicationEvent(appEventRef, bes)
    }

    UOWNotification(uowAppEvents, txTime)
  }

  def preparePartitionedUOWNotificationProto(
      actions: Seq[TxnAction],
      txTime: Instant,
      excludePrevBusEvent: Boolean,
      numPartitionKeys: Option[Int]): PartitionedUOWNotificationProto = ??? /* {
    toProto(preparePartitionedUOWNotification(actions, txTime, excludePrevBusEvent, numPartitionKeys))
  } */

  private[optimus] def preparePartitionedUOWNotification(
      actions: Seq[TxnAction],
      txTime: Instant,
      excludePrevBusEvent: Boolean,
      numPartitionKeys: Option[Int]): PartitionedUOWNotification = {
    val uowNotification = prepareUnitOfWorkMessageB2BUOWNotification(actions, txTime, excludePrevBusEvent)
    require(
      uowNotification.appEvents.size == 1,
      s"expected 1 application event in a UowNotification but found: ${uowNotification.appEvents.size}, tt: $txTime details: ${uowNotification.appEvents
          .mkString(";")}"
    )
    val beRefs = uowNotification.appEvents.flatMap(_.beRefs)
    PartitionedUOWNotification(beRefs, txTime, numPartitionKeys)
  }

  def preparePartitionedUOWNotificationKeyInfoProto(
      partitionedUOWNotificationKey: PartitionedUOWNotificationKey): PartitionedUOWNotificationKeyProto =
    toProto(partitionedUOWNotificationKey)

}

object UOWApplicationEventSerializer
    extends UnitOfWorkSerialization
    with ProtoSerializer[UOWApplicationEvent, UOWApplicationEventProto] {
  override def serialize(appEvent: UOWApplicationEvent): UOWApplicationEventProto = ??? /* {
    UOWApplicationEventProto
      .newBuilder()
      .setEventRef(toProto(appEvent.id))
      .addAllBeRefs((appEvent.beRefs.map(beRef => toProto(beRef))).asJava)
      .build()
  } */

  override def deserialize(proto: UOWApplicationEventProto): UOWApplicationEvent = ??? /* {
    val beRefs = proto.getBeRefsList().asScala.map(beproto => fromProto(beproto)).toSeq
    UOWApplicationEvent(fromProto(proto.getEventRef()), beRefs)
  } */
}

object UOWNotificationSerializer
    extends UnitOfWorkSerialization
    with ProtoSerializer[UOWNotification, UOWNotificationProto] {
  override def serialize(uowNotification: UOWNotification): UOWNotificationProto = ??? /* {
    UOWNotificationProto
      .newBuilder()
      .setTxTime(toProto(uowNotification.tt))
      .addAllAppEvents((uowNotification.appEvents.map(ae => toProto(ae))).asJava)
      .build()
  } */

  override def deserialize(proto: UOWNotificationProto): UOWNotification = ??? /* {
    val appEventProtos = proto.getAppEventsList().asScala
    val uowAppEvents = appEventProtos.map(proto => fromProto(proto)).toSeq
    val tt = fromProto(proto.getTxTime())
    UOWNotification(uowAppEvents, tt)
  } */
}

object PartitionedUOWNotificationSerializer
    extends UnitOfWorkSerialization
    with ProtoSerializer[PartitionedUOWNotification, PartitionedUOWNotificationProto] {
  override def serialize(partitionedUowNotification: PartitionedUOWNotification): PartitionedUOWNotificationProto = ??? /* {
    val builder = PartitionedUOWNotificationProto
      .newBuilder()
      .addAllBeRefs((partitionedUowNotification.beRefs.map(be => toProto(be)).asJava))
      .setTxTime(toProto(partitionedUowNotification.tt))
    partitionedUowNotification.numPartitionKeys foreach { key =>
      builder.setNumPartitionKeys(key)
    }
    builder.build()
  } */

  override def deserialize(proto: PartitionedUOWNotificationProto): PartitionedUOWNotification = ??? /* {
    val beRefProtos = proto.getBeRefsList().asScala
    val beRefs = beRefProtos.map(proto => fromProto(proto)).toSeq
    val tt = fromProto(proto.getTxTime())
    val numPartitionKeys =
      if (proto.hasNumPartitionKeys) Some(proto.getNumPartitionKeys) else None
    PartitionedUOWNotification(beRefs, tt, numPartitionKeys)
  } */
}

object PartitionedUOWNotificationKeyInfoSerializer
    extends ProtoSerializer[PartitionedUOWNotificationKey, PartitionedUOWNotificationKeyProto] {
  override def serialize(notificationKey: PartitionedUOWNotificationKey): PartitionedUOWNotificationKeyProto = ??? /* {
    PartitionedUOWNotificationKeyProto
      .newBuilder()
      .setClassName(notificationKey.className)
      .setKeyName(notificationKey.keyName)
      .setKeyValue(notificationKey.keyValue)
      .build()
  } */

  override def deserialize(proto: PartitionedUOWNotificationKeyProto): PartitionedUOWNotificationKey =
    ??? // PartitionedUOWNotificationKey(proto.getClassName(), proto.getKeyName(), proto.getKeyValue())
}
