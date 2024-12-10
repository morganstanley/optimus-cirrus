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

import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform.storable.SerializedBusinessEvent
import scala.jdk.CollectionConverters._

private[proto] trait EventCommandProtoSerialization extends GetCommandProtoSerialization with BasicProtoSerialization {
  implicit val getBusinessEventByKeySerializer: GetBusinessEventByKeySerializer.type = GetBusinessEventByKeySerializer
  implicit val getBusinessEventsByClassSerializer: GetBusinessEventsByClassSerializer.type =
    GetBusinessEventsByClassSerializer
  implicit val getBusinessEventSerializer: GetBusinessEventSerializer.type = GetBusinessEventSerializer
  implicit val getInitiatingEventSerializer: GetInitiatingEventSerializer.type = GetInitiatingEventSerializer
  implicit val getEntityEventTimelineSerializer: GetEntityEventTimelineSerializer.type =
    GetEntityEventTimelineSerializer
  implicit val getEntityEventValidTimelineSerializer: GetEntityEventValidTimelineSerializer.type =
    GetEntityEventValidTimelineSerializer
  implicit val getEventTimelineSerializer: GetEventTimelineSerializer.type = GetEventTimelineSerializer
  implicit val getEventTransactionsSerializer: GetEventTransactionsSerializer.type = GetEventTransactionsSerializer
  implicit val getAssociatedEntitiesSerializer: GetAssociatedEntitiesSerializer.type = GetAssociatedEntitiesSerializer
  implicit val getBusinessEventResultSerializer: GetBusinessEventResultSerializer.type =
    GetBusinessEventResultSerializer
  implicit val partialGetBusinessEventResultSerializer: PartialGetBusinessEventResultSerializer.type =
    PartialGetBusinessEventResultSerializer
  implicit val getBusinessEventWithTTToResultSerializer: GetBusinessEventWithTTToResultSerializer.type =
    GetBusinessEventWithTTToResultSerializer
  implicit val getInitiatingEventResultSerializer: GetInitiatingEventResultSerializer.type =
    GetInitiatingEventResultSerializer
  implicit val getEntityEventTimelineResultSerializer: GetEntityEventTimelineResultSerializer.type =
    GetEntityEventTimelineResultSerializer
  implicit val partialGetEntityEventTimelineResultSerializer: PartialGetEntityEventTimelineResultSerializer.type =
    PartialGetEntityEventTimelineResultSerializer
  implicit val getEntityEventValidTimelineResultSerializer: GetEntityEventValidTimelineResultSerializer.type =
    GetEntityEventValidTimelineResultSerializer
  implicit val getEntityEventValidTimelineResultLazyLoadSerializer
      : GetEntityEventValidTimelineLazyLoadResultSerializer.type = GetEntityEventValidTimelineLazyLoadResultSerializer
}

object GetEntityEventTimelineResultSerializer
    extends EventCommandProtoSerialization
    with ProtoSerializer[GetEntityEventTimelineResult, SelectBusinessEventTimelineResultProto] {

  override def deserialize(proto: SelectBusinessEventTimelineResultProto): GetEntityEventTimelineResult = ??? /* {
    val serializedEvents = proto.getEntityBusinessEventComboList().asScala.toSeq
    val x = serializedEvents map { k =>
      val be: SerializedBusinessEvent = fromProto(k.getBusinessEvent())
      val pe =
        if (k.hasPersistentEntity())
          Some(fromProto(k.getPersistentEntity()))
        else None
      (be, pe)
    }
    GetEntityEventTimelineResult(x)
  } */

  override def serialize(result: GetEntityEventTimelineResult): SelectBusinessEventTimelineResultProto = ??? /* {
    val combos = result.eventsEntitiesCombo.map { k =>
      val builder = EntityBusinessEventComboProto.newBuilder.setBusinessEvent(toProto(k._1))
      if (k._2.isDefined)
        builder.setPersistentEntity(toProto(k._2.get))
      builder.build()
    }
    SelectBusinessEventTimelineResultProto.newBuilder
      .addAllEntityBusinessEventCombo(combos.asJava)
      .build

  } */
}

object PartialGetEntityEventTimelineResultSerializer
    extends EventCommandProtoSerialization
    with ProtoSerializer[PartialGetEntityEventTimelineResult, PartialSelectBusinessEventTimelineResultProto] {

  override def deserialize(
      proto: PartialSelectBusinessEventTimelineResultProto): PartialGetEntityEventTimelineResult = ??? /* {
    val serializedEvents = proto.getEntityBusinessEventComboList().asScala.toSeq
    val x = serializedEvents map { k =>
      val be: SerializedBusinessEvent = fromProto(k.getBusinessEvent())
      val pe =
        if (k.hasPersistentEntity())
          Some(fromProto(k.getPersistentEntity()))
        else None
      (be, pe)
    }
    PartialGetEntityEventTimelineResult(x, proto.getIsLast)
  } */

  override def serialize(result: PartialGetEntityEventTimelineResult): PartialSelectBusinessEventTimelineResultProto = ??? /* {
    val combos = result.eventsEntitiesCombo.map { k =>
      val builder = EntityBusinessEventComboProto.newBuilder.setBusinessEvent(toProto(k._1))
      if (k._2.isDefined)
        builder.setPersistentEntity(toProto(k._2.get))
      builder.build()
    }
    PartialSelectBusinessEventTimelineResultProto.newBuilder
      .addAllEntityBusinessEventCombo(combos.asJava)
      .setIsLast(result.isLast)
      .build

  } */
}

object GetEntityEventValidTimelineResultSerializer
    extends EventCommandProtoSerialization
    with ProtoSerializer[GetEntityEventValidTimelineResult, SelectBusinessEventValidTimelineResultProto] {

  override def deserialize(proto: SelectBusinessEventValidTimelineResultProto): GetEntityEventValidTimelineResult = ??? /* {
    val serializedEvents = proto.getEntityBusinessEventComboList().asScala.toSeq
    val x = serializedEvents map { k =>
      val be =
        if (k.hasBusinessEvent())
          Some(fromProto(k.getBusinessEvent()))
        else None
      val pe =
        if (k.hasPersistentEntity())
          Some(fromProto(k.getPersistentEntity()))
        else None
      (be, pe)
    }
    GetEntityEventValidTimelineResult(x)
  } */

  override def serialize(result: GetEntityEventValidTimelineResult): SelectBusinessEventValidTimelineResultProto = ??? /* {
    val combos = result.eventsEntitiesCombo.map { k =>
      val builder = EntityBusinessEventWithTTToComboProto.newBuilder
      if (k._1.isDefined) builder.setBusinessEvent(toProto(k._1.get))
      if (k._2.isDefined) builder.setPersistentEntity(toProto(k._2.get))
      builder.build()
    }
    SelectBusinessEventValidTimelineResultProto.newBuilder
      .addAllEntityBusinessEventCombo(combos.asJava)
      .build

  } */
}

object GetEntityEventValidTimelineLazyLoadResultSerializer
    extends EventCommandProtoSerialization
    with ProtoSerializer[
      GetEntityEventValidTimelineLazyLoadResult,
      SelectBusinessEventValidTimelineLazyLoadResultProto] {

  override def deserialize(
      proto: SelectBusinessEventValidTimelineLazyLoadResultProto): GetEntityEventValidTimelineLazyLoadResult = ??? /* {
    val brefVtfs = proto.getBusinessEventValidTimeFromComboList().asScala.toSeq
    val x = brefVtfs map { c =>
      val bref =
        if (c.hasBusinessEvent())
          Some(fromProto(c.getBusinessEvent()))
        else None
      val vtf = fromProto(c.getValidTimeFrom())
      (bref, vtf)
    }
    GetEntityEventValidTimelineLazyLoadResult(x)
  } */

  override def serialize(
      result: GetEntityEventValidTimelineLazyLoadResult): SelectBusinessEventValidTimelineLazyLoadResultProto = ??? /* {
    val combos = result.brefVtfsCombo.map { k =>
      val builder = BusinessEventValidTimeFromComboProto.newBuilder
      if (k._1.isDefined) builder.setBusinessEvent(toProto(k._1.get))
      builder.setValidTimeFrom(toProto(k._2))
      builder.build()
    }
    SelectBusinessEventValidTimelineLazyLoadResultProto.newBuilder
      .addAllBusinessEventValidTimeFromCombo(combos.asJava)
      .build

  } */
}
