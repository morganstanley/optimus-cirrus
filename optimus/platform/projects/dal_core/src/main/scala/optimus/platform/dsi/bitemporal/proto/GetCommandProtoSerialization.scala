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

import com.google.protobuf.ByteString
import msjava.base.com.google.flatbuffers.FlatBufferBuilder.HeapByteBufferFactory
import msjava.slf4jutils.scalalog.getLogger
import net.jpountz.lz4.LZ4Factory
import optimus.dsi.base.RegisteredIndexConfig
import optimus.dsi.partitioning.DefaultPartition
import optimus.dsi.partitioning.NamedPartition
import optimus.dsi.partitioning.Partition
import optimus.dsi.partitioning.PartitionHelper
import optimus.dsi.session.JarPathT.ExistingNonStorableJarPath
import optimus.dsi.session._
import optimus.entity.EntityAuditInfo
import optimus.platform.ImmutableArray
import optimus.platform.dal.ClientMachineIdentifier
import optimus.platform.dal.OnBehalfTokenType
import optimus.platform.dal.TreadmillNode
import optimus.platform.dal.UnremarkableClientMachine
import optimus.platform.dal.UnremarkableClientMachineWithHostPort
import optimus.platform.dal.config._
import optimus.platform.dal.servicediscovery.DalServiceLookupType
import optimus.platform.dal.servicediscovery.DalServiceProviderType
import optimus.platform.dal.servicediscovery.DalServiceType
import optimus.platform.dal.servicediscovery.ServiceDiscoveryElement
import optimus.platform.dal.session.RolesetMode
import optimus.platform.dsi.Feature
import optimus.platform.dsi.SupportedFeatures
import optimus.platform.dsi.bitemporal.DSIQueryTemporality._
import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform.dsi.expressions.proto.ExpressionSerialization
import optimus.platform.pickling.ImmutableByteArray
import optimus.platform.runtime.ZkUtils
import optimus.platform.storable._
import org.apache.commons.lang3.SerializationUtils

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

private[proto] trait GetCommandProtoSerialization extends QueryProtoSerialization with RolesetSerialization {
  implicit val countGroupingsSerializer: CountGroupingsSerializer.type = CountGroupingsSerializer
  implicit val countSerializer: CountSerializer.type = CountSerializer
  implicit val countResultSerializer: CountResultSerializer.type = CountResultSerializer
  implicit val selectSerializer: SelectSerializer.type = SelectSerializer
  implicit val selectResultSerializer: SelectResultSerializer.type = SelectResultSerializer
  implicit val partialSelectResultSerializer: PartialSelectResultSerializer.type = PartialSelectResultSerializer
  implicit val prcPartialSelectResultSerializer: PrcPartialSelectResultSerializer.type =
    PrcPartialSelectResultSerializer
  implicit val getAuditInfoSerializer: GetAuditInfoSerializer.type = GetAuditInfoSerializer
  implicit val getAuditInfoResultSerializer: GetAuditInfoResultSerializer.type = GetAuditInfoResultSerializer
  implicit val partialGetAuditInfoResultSerializer: PartialGetAuditInfoResultSerializer.type =
    PartialGetAuditInfoResultSerializer
  implicit val establishSessionSerializer: EstablishSessionSerializer.type = EstablishSessionSerializer
  implicit val sessionTokenRequestSerializer: SessionTokenRequestSerializer.type = SessionTokenRequestSerializer
  implicit val sessionTokenResultSerializer: SessionTokenResultSerializer.type = SessionTokenResultSerializer
  implicit val heartbeatSerializer: HeartbeatSerializer.type = HeartbeatSerializer
  implicit val heartbeatResultSerializer: HeartbeatResultSerializer.type = HeartbeatResultSerializer
  implicit val getProtoFilePropertiesSerializer: GetProtoFilePropertiesSerializer.type =
    GetProtoFilePropertiesSerializer
  implicit val getProtoFilePropertiesResultSerializer: GetProtoFilePropertiesResultSerializer.type =
    GetProtoFilePropertiesResultSerializer
  implicit val temporalityQueryCommandSerializer: TemporalityQueryCommandSerializer.type =
    TemporalityQueryCommandSerializer
  implicit val queryReferenceResultSerializer: QueryReferenceResultSerializer.type = QueryReferenceResultSerializer
  implicit val partialQueryReferenceResultSerializer: PartialQueryReferenceResultSerializer.type =
    PartialQueryReferenceResultSerializer
  implicit val featureInfoSerializer: FeatureInfoSerializer.type = FeatureInfoSerializer
  implicit val getInfoResultSerializer: GetInfoResultSerializer.type = GetInfoResultSerializer
  implicit val roleMembershipQuerySerializer: RoleMembershipQuerySerializer.type = RoleMembershipQuerySerializer
  implicit val roleMembershipResultSerializer: RoleMembershipResultSerializer.type = RoleMembershipResultSerializer
  implicit val slotMapSerializer: SlotMapSerializer.type = SlotMapSerializer
  implicit val getRefsNotAtSlotSerializer: GetRefsNotAtSlotSerializer.type = GetRefsNotAtSlotSerializer
  implicit val getRefsNotAtSlotResultSerializer: GetRefsNotAtSlotResultSerializer.type =
    GetRefsNotAtSlotResultSerializer
  implicit val getSlotsSerializer: GetSlotsSerializer.type = GetSlotsSerializer
  implicit val getSlotsResultSerializer: GetSlotsResultSerializer.type = GetSlotsResultSerializer
  implicit val selectSpaceSerializer: SelectSpaceSerializer.type = SelectSpaceSerializer
  implicit val selectSpaceResultSerializer: SelectSpaceResultSerializer.type = SelectSpaceResultSerializer
  implicit val partialSelectSpaceResultSerializer: PartialSelectSpaceResultSerializer.type =
    PartialSelectSpaceResultSerializer
  implicit val jarPathSerializer: JarPathSerializer.type = JarPathSerializer
  implicit val expressionQuerySerializer: ExpressionQuerySerializer.type = ExpressionQuerySerializer
  implicit val partialQueryResultSerializer: PartialQueryResultSerializer.type = PartialQueryResultSerializer
  implicit val queryResultSerializer: QueryResultSerializer.type = QueryResultSerializer
  implicit val clientMachineIdentifierSerializer: ClientMachineIdentifierSerializer.type =
    ClientMachineIdentifierSerializer
  implicit val canPerformActionSerializer: CanPerformActionSerializer.type = CanPerformActionSerializer
  implicit val booleanResultSerializer: BooleanResultSerializer.type = BooleanResultSerializer
  implicit val resultStatsSerializer: ResultStatsSerializer.type = ResultStatsSerializer
  implicit val createNewSesssionSerializer: CreateNewSessionSerializer.type = CreateNewSessionSerializer
  implicit val createNewSesssionResultSerializer: CreateNewSessionResultSerializer.type =
    CreateNewSessionResultSerializer
  implicit def dsSerializer: ProtoSerializer[DiscoverServices, DiscoverServicesProto] = DiscoverServicesSerializer
  implicit def dsrSerializer: ProtoSerializer[DiscoverServicesResult, DiscoverServicesResultProto] =
    DiscoverServicesResultSerializer

  final def fromProto(proto: QueryDataType): DALDataType.DALDataType = {
    val dalDataType =
      if (proto == QueryDataType.BUSINESS_EVENT)
        DALDataType.BusinessEvent
      else DALDataType.Entity
    dalDataType
  }

  final def toProto(req: DALDataType.DALDataType): QueryDataType = {
    val data = if (DALDataType.BusinessEvent == req) QueryDataType.BUSINESS_EVENT else QueryDataType.ENTITY
    data
  }

  final def fromProto(proto: EstablishSessionResultProto) = EstablishSessionResultSerializer.deserialize(proto)
  final def toProto(result: EstablishSessionResult) = EstablishSessionResultSerializer.serialize(result)
}

object CountGroupingsSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[CountGroupings, CountGroupingsProto] {
  override def serialize(count: CountGroupings): CountGroupingsProto = {
    val bld = CountGroupingsProto.newBuilder
    val builder = count.query match {
      case q: EntityClassQuery => bld.setEntityClassQuery(toProto(q))
      case q: EventClassQuery  => bld.setEventClassQuery(toProto(q))
      case q =>
        throw new UnsupportedOperationException(s"CountGroupingsProto serialization for query $q not supported.")
    }
    builder.setReadTxTime(toProto(count.readTxTime)).build()
  }

  override def deserialize(proto: CountGroupingsProto): CountGroupings = {
    val query =
      if (proto.hasEntityClassQuery)
        fromProto(proto.getEntityClassQuery)
      else if (proto.hasEventClassQuery)
        fromProto(proto.getEventClassQuery)
      else
        throw new UnsupportedOperationException

    CountGroupings(query, fromProto(proto.getReadTxTime))
  }
}

object CountSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[Count, CountProto]
    with DSIQueryTemporalitySerialization {
  protected override val log = getLogger(this)

  override def serialize(count: Count): CountProto = {
    val bld = CountProto.newBuilder

    val builder = count.temporality match {
      case All(readTT)           => bld.setReadTxTime(toProto(readTT))
      case TxTime(tx)            => bld.setTxTime(toProto(tx))
      case ValidTime(vt, readTT) => bld.setValidTime(toProto(vt)).setReadTxTime(toProto(readTT))
      case At(vt, tx)            => bld.setTxTime(toProto(tx)).setValidTime(toProto(vt))
      case TxRange(range)        => bld.setTxRange(toProto(range))
      case BitempRange(_, _, _) =>
        throw new UnsupportedOperationException("BitempRange is not supported for Count serialization.")
      case OpenVtTxRange(_) =>
        throw new UnsupportedOperationException("OpenVtTxRange is not supported for Count serialization.")
    }
    (count.query match {
      case q: SerializedKeyQuery => builder.setSerializedKeyQuery(toProto(q))
      case q: EntityClassQuery   => builder.setEntityClassQuery(toProto(q))
      case q: EventClassQuery    => builder.setEventClassQuery(toProto(q))
      case _ =>
        throw new UnsupportedOperationException(s"CountProto serialization for query ${count.query} not supported.")
    }).build
  }

  override def deserialize(proto: CountProto): Count = {
    val query =
      if (proto.hasEntityClassQuery)
        fromProto(proto.getEntityClassQuery)
      else if (proto.hasSerializedKeyQuery)
        fromProto(proto.getSerializedKeyQuery)
      else if (proto.hasEventClassQuery)
        fromProto(proto.getEventClassQuery)
      else
        throw new UnsupportedOperationException

    val temporality = getTemporality(proto)

    Count(query, temporality)
  }
}

object SelectSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[Select, SelectProto]
    with DSIQueryTemporalitySerialization {
  protected override val log = getLogger(this)

  override def serialize(select: Select): SelectProto = {
    val bld = SelectProto.newBuilder

    val builder = select.temporality match {
      case All(readTT)           => bld.setReadTxTime(toProto(readTT))
      case TxTime(tx)            => bld.setTxTime(toProto(tx))
      case ValidTime(vt, readTT) => bld.setValidTime(toProto(vt)).setReadTxTime(toProto(readTT))
      case At(vt, tx)            => bld.setTxTime(toProto(tx)).setValidTime(toProto(vt))
      case TxRange(range)        => bld.setTxRange(toProto(range))
      case BitempRange(vtRange, ttRange, inRange) =>
        bld.setBitempRange(
          BitempRangeProto
            .newBuilder()
            .setVtRange(toProto(vtRange))
            .setTtRange(toProto(ttRange))
            .setInRange(inRange)
            .build)
      case OpenVtTxRange(_) =>
        throw new UnsupportedOperationException("OpenVtTxRange is not supported for Select serialization.")
    }
    (select.query match {
      case q: ReferenceQuery          => builder.setReferenceQuery(toProto(q))
      case q: SerializedKeyQuery      => builder.setSerializedKeyQuery(toProto(q))
      case q: EntityClassQuery        => builder.setEntityClassQuery(toProto(q))
      case q: LinkageQuery            => builder.setLinkageQuery(toProto(q))
      case q: EntityCmReferenceQuery  => builder.setEntityCmReferenceQuery(toProto(q))
      case q: EventCmReferenceQuery   => builder.setEventCmReferenceQuery(toProto(q))
      case q: EventSerializedKeyQuery => builder.setEventKeyQuery(toProto(q))
      case _: EventClassQuery | _: EventReferenceQuery | _: EntityClassAppIdUserIdQuery =>
        throw new UnsupportedOperationException(s"SelectProto serialization for query ${select.query} not supported.")
    }).build
  }

  override def deserialize(proto: SelectProto): Select = {
    val query =
      if (proto.hasReferenceQuery)
        fromProto(proto.getReferenceQuery)
      else if (proto.hasEntityClassQuery)
        fromProto(proto.getEntityClassQuery)
      else if (proto.hasSerializedKeyQuery)
        fromProto(proto.getSerializedKeyQuery)
      else if (proto.hasLinkageQuery)
        fromProto(proto.getLinkageQuery)
      else if (proto.hasEntityCmReferenceQuery)
        fromProto(proto.getEntityCmReferenceQuery)
      else if (proto.hasEventCmReferenceQuery)
        fromProto(proto.getEventCmReferenceQuery)
      else if (proto.hasEventKeyQuery)
        fromProto(proto.getEventKeyQuery)
      else
        throw new UnsupportedOperationException

    val temporality = getTemporality(proto)

    Select(query, temporality)
  }
}

object SelectSpaceSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[SelectSpace, SelectSpaceProto]
    with DSIQueryTemporalitySerialization {
  protected override val log = getLogger(this)

  override def serialize(cmd: SelectSpace): SelectSpaceProto = {
    val bld = SelectSpaceProto.newBuilder

    val builder = cmd.temporality match {
      case All(readTT)           => bld.setReadTxTime(toProto(readTT))
      case TxTime(tx)            => bld.setTxTime(toProto(tx))
      case ValidTime(vt, readTT) => bld.setValidTime(toProto(vt)).setReadTxTime(toProto(readTT))
      case At(vt, tx)            => bld.setTxTime(toProto(tx)).setValidTime(toProto(vt))
      case TxRange(range)        => bld.setTxRange(toProto(range))
      case BitempRange(vtRange, ttRange, inRange) =>
        bld.setBitempRange(
          BitempRangeProto
            .newBuilder()
            .setVtRange(toProto(vtRange))
            .setTtRange(toProto(ttRange))
            .setInRange(inRange)
            .build)
      case OpenVtTxRange(_) =>
        throw new UnsupportedOperationException("OpenVtTxRange is not supported for SelectSpace serialization.")
    }

    cmd.query match {
      case q: ReferenceQuery     => builder.setReferenceQuery(toProto(q))
      case q: SerializedKeyQuery => builder.setSerializedKeyQuery(toProto(q))
      case _: EntityClassQuery | _: LinkageQuery | _: EventClassQuery | _: EventReferenceQuery |
          _: EventSerializedKeyQuery | _: EntityCmReferenceQuery | _: EventCmReferenceQuery =>
        throw new UnsupportedOperationException(s"SelectSpace serialization for query type ${cmd.query} not supported")
      case EntityClassAppIdUserIdQuery(typeName, appId, userId) =>
        val qb = EntityClassAppIdUserIdQueryProto.newBuilder().setClassName(typeName)
        if (appId.isDefined) qb.setAppId(appId.get)
        if (userId.isDefined) qb.setUserId(userId.get)
        builder.setClassAppidUseridQuery(qb.build)
    }

    builder.build()
  }

  override def deserialize(proto: SelectSpaceProto): SelectSpace = {
    val temporality = getTemporality(proto)
    val query =
      if (proto.hasReferenceQuery) fromProto(proto.getReferenceQuery)
      else if (proto.hasSerializedKeyQuery) fromProto(proto.getSerializedKeyQuery)
      else if (proto.hasClassAppidUseridQuery()) {
        val typeName = proto.getClassAppidUseridQuery().getClassName()
        val appId =
          if (proto.getClassAppidUseridQuery().hasAppId()) Some(proto.getClassAppidUseridQuery().getAppId()) else None
        val userId =
          if (proto.getClassAppidUseridQuery().hasUserId()) Some(proto.getClassAppidUseridQuery().getUserId()) else None
        EntityClassAppIdUserIdQuery(typeName, appId, userId)
      } else
        throw new UnsupportedOperationException(s"Cannot deserialize SelectSpace command due to unknown query type")
    SelectSpace(query, temporality)
  }
}

object SelectSpaceRectangleSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[SelectSpaceResult.Rectangle, SelectSpaceResultRectangleProto] {
  override def deserialize(proto: SelectSpaceResultRectangleProto): SelectSpaceResult.Rectangle = {
    SelectSpaceResult.Rectangle(
      txInterval = fromProto(proto.getTimeInterval),
      vtInterval = fromProto(proto.getValidTimeInterval),
      vref = fromProto(proto.getVersionedReference),
      eref = fromProto(proto.getEntityReference)
    )
  }
  override def serialize(obj: SelectSpaceResult.Rectangle): SelectSpaceResultRectangleProto = {
    SelectSpaceResultRectangleProto.newBuilder
      .setTimeInterval(toProto(obj.txInterval))
      .setValidTimeInterval(toProto(obj.vtInterval))
      .setVersionedReference(toProto(obj.vref))
      .setEntityReference(toProto(obj.eref))
      .build()
  }
}

object SelectSpaceResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[SelectSpaceResult, SelectSpaceResultProto] {
  override def serialize(res: SelectSpaceResult): SelectSpaceResultProto = {
    val rects = res.value.map { SelectSpaceRectangleSerializer.serialize }
    SelectSpaceResultProto.newBuilder.addAllRectangles(rects.asJava).build()
  }

  override def deserialize(proto: SelectSpaceResultProto): SelectSpaceResult = {
    val rects = proto.getRectanglesList.asScala.map { SelectSpaceRectangleSerializer.deserialize }
    SelectSpaceResult(rects)
  }
}

object PartialSelectSpaceResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[PartialSelectSpaceResult, PartialSelectSpaceResultProto] {
  override def serialize(res: PartialSelectSpaceResult): PartialSelectSpaceResultProto = {
    val rects = res.value.map { case SelectSpaceResult.Rectangle(txInterval, vtInterval, vref, eref, _) =>
      SelectSpaceResultRectangleProto.newBuilder
        .setTimeInterval(toProto(txInterval))
        .setValidTimeInterval(toProto(vtInterval))
        .setVersionedReference(toProto(vref))
        .setEntityReference(toProto(eref))
        .build()
    }
    PartialSelectSpaceResultProto.newBuilder.addAllRectangles(rects.asJava).setIsLast(res.isLast).build()
  }

  override def deserialize(proto: PartialSelectSpaceResultProto): PartialSelectSpaceResult = {
    val rects = proto.getRectanglesList.asScala.map { rect =>
      SelectSpaceResult.Rectangle(
        txInterval = fromProto(rect.getTimeInterval),
        vtInterval = fromProto(rect.getValidTimeInterval),
        vref = fromProto(rect.getVersionedReference),
        eref = fromProto(rect.getEntityReference)
      )
    }
    PartialSelectSpaceResult(rects, proto.getIsLast)
  }
}

object GetAuditInfoSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[GetAuditInfo, GetAuditInfoProto]
    with DSIQueryTemporalitySerialization {
  protected override val log = getLogger(this)

  override def serialize(getAuditInfo: GetAuditInfo): GetAuditInfoProto = {
    val bld = GetAuditInfoProto.newBuilder

    val builder = getAuditInfo.temporality match {
      case All(readTT)           => bld.setReadTxTime(toProto(readTT))
      case TxTime(tx)            => bld.setTxTime(toProto(tx))
      case ValidTime(vt, readTT) => bld.setValidTime(toProto(vt)).setReadTxTime(toProto(readTT))
      case At(vt, tx)            => bld.setTxTime(toProto(tx)).setValidTime(toProto(vt))
      case TxRange(range)        => bld.setTxRange(toProto(range))
      case BitempRange(_, _, _) =>
        throw new UnsupportedOperationException("BitempRange is not supported for GetAuditInfo serialization.")
      case OpenVtTxRange(_) =>
        throw new UnsupportedOperationException("OpenVtTxRange is not supported for GetAuditInfo serialization.")
    }

    val bd = builder.setEntityReference(toProto(getAuditInfo.entityRef))

    (bd.setClassName(getAuditInfo.clazzName)).build()
  }

  override def deserialize(proto: GetAuditInfoProto): GetAuditInfo = {
    val entityRef =
      if (proto.hasEntityReference)
        fromProto(proto.getEntityReference)
      else
        throw new UnsupportedOperationException

    // TODO (OPTIMUS-13040): This code is copy-pasted from other places
    val temporality = getTemporality(proto)

    val className =
      if (proto.hasClassName())
        proto.getClassName()
      else
        ""

    GetAuditInfo(entityRef, className, temporality)
  }
}

object EntityAuditInfoSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[EntityAuditInfo, EntityAuditInfoProto] {
  override def serialize(auditInfo: EntityAuditInfo): EntityAuditInfoProto = {
    val builder = EntityAuditInfoProto.newBuilder
      .setEffectiveUser(auditInfo.effectiveUser)
      .setRealUser(auditInfo.realUser)
    auditInfo.versionedRef.foreach(vref => builder.setVersionedReference(toProto(vref)))
    auditInfo.elevatedForUser.foreach(builder.setElevatedForUser)
    auditInfo.appId.foreach(builder.setAppId)
    auditInfo.host.foreach(builder.setHost)
    auditInfo.reqId.foreach(builder.setRequestUuid)
    auditInfo.zoneId.foreach(builder.setZoneId)
    builder.build
  }

  override def deserialize(proto: EntityAuditInfoProto): EntityAuditInfo = {
    EntityAuditInfo(
      if (proto.hasRealUser) proto.getRealUser else proto.getEffectiveUser,
      if (proto.hasVersionedReference) Some(fromProto(proto.getVersionedReference)) else None,
      proto.getEffectiveUser,
      if (proto.hasElevatedForUser) Some(proto.getElevatedForUser) else None,
      if (proto.hasAppId) Some(proto.getAppId) else None,
      if (proto.hasHost) Some(proto.getHost) else None,
      if (proto.hasRequestUuid) Some(proto.getRequestUuid) else None,
      if (proto.hasZoneId) Some(proto.getZoneId) else None
    )
  }
}

object GetBusinessEventByKeySerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[GetBusinessEventByKey, SelectBusinesEventByKeyProto] {

  override def serialize(byKey: GetBusinessEventByKey): SelectBusinesEventByKeyProto = {
    SelectBusinesEventByKeyProto.newBuilder
      .setSerializedKey(toProto(byKey.key))
      .setReadTxTime(toProto(byKey.tt))
      .build
  }

  override def deserialize(proto: SelectBusinesEventByKeyProto): GetBusinessEventByKey = {
    GetBusinessEventByKey(fromProto(proto.getSerializedKey()), fromProto(proto.getReadTxTime()))
  }
}

object GetBusinessEventsByClassSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[GetBusinessEventsByClass, SelectBusinesEventByEventClassProto] {

  override def serialize(byClass: GetBusinessEventsByClass): SelectBusinesEventByEventClassProto = {
    SelectBusinesEventByEventClassProto.newBuilder
      .setEventClassQuery(toProto(byClass.eventClassQuery))
      .setReadTxTime(toProto(byClass.tt))
      .build
  }

  override def deserialize(proto: SelectBusinesEventByEventClassProto): GetBusinessEventsByClass = {
    GetBusinessEventsByClass(fromProto(proto.getEventClassQuery()), fromProto(proto.getReadTxTime()))
  }
}

object GetBusinessEventSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[GetBusinessEvent, SelectBusinesEventByEventRefProto] {

  override def serialize(byEventRef: GetBusinessEvent): SelectBusinesEventByEventRefProto = {
    SelectBusinesEventByEventRefProto.newBuilder
      .setEventReference(toProto(byEventRef.ref))
      .setReadTxTime(toProto(byEventRef.tt))
      .build
  }

  override def deserialize(proto: SelectBusinesEventByEventRefProto): GetBusinessEvent = {
    GetBusinessEvent(fromProto(proto.getEventReference()), fromProto(proto.getReadTxTime()))
  }
}

object GetInitiatingEventSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[GetInitiatingEvent, GetInitiatingEventProto] {

  override def serialize(gie: GetInitiatingEvent): GetInitiatingEventProto = {
    GetInitiatingEventProto.newBuilder
      .setEntityReference(toProto(gie.ref))
      .setReadVtTime(toProto(gie.vt))
      .setReadTxTime(toProto(gie.tt))
      .build
  }

  override def deserialize(proto: GetInitiatingEventProto): GetInitiatingEvent = {
    GetInitiatingEvent(
      fromProto(proto.getEntityReference),
      fromProto(proto.getReadVtTime),
      fromProto(proto.getReadTxTime))
  }
}

object GetEntityEventTimelineSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[GetEntityEventTimeline, SelectBusinesEventByEntityRefProto] {

  override def serialize(byEntityRef: GetEntityEventTimeline): SelectBusinesEventByEntityRefProto = {
    SelectBusinesEventByEntityRefProto.newBuilder
      .setEntityReference(toProto(byEntityRef.ref))
      .setReadTxTime(toProto(byEntityRef.tt))
      .build
  }

  override def deserialize(proto: SelectBusinesEventByEntityRefProto): GetEntityEventTimeline = {
    GetEntityEventTimeline(fromProto(proto.getEntityReference()), fromProto(proto.getReadTxTime()))
  }
}

object GetEntityEventValidTimelineSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[GetEntityEventValidTimeline, SelectEntityEventValidTimelineProto] {

  override def serialize(byEntityRef: GetEntityEventValidTimeline): SelectEntityEventValidTimelineProto = {
    val builder = SelectEntityEventValidTimelineProto.newBuilder
    builder.setEntityReference(toProto(byEntityRef.ref))
    builder.setReadTxTime(toProto(byEntityRef.tt))
    if (byEntityRef.validTimeInterval.isDefined)
      builder.setValidTimeInterval(toProto(byEntityRef.validTimeInterval.get))
    builder.build
  }

  override def deserialize(proto: SelectEntityEventValidTimelineProto): GetEntityEventValidTimeline = {
    GetEntityEventValidTimeline(
      fromProto(proto.getEntityReference()),
      fromProto(proto.getReadTxTime()),
      if (proto.hasValidTimeInterval) Some(fromProto(proto.getValidTimeInterval)) else None
    )
  }
}

object GetEventTimelineSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[GetEventTimeline, SelectEventTimelineProto] {

  override def serialize(byEventRef: GetEventTimeline): SelectEventTimelineProto = {
    SelectEventTimelineProto.newBuilder
      .setEventReference(toProto(byEventRef.ref))
      .setReadTxTime(toProto(byEventRef.tt))
      .build
  }

  override def deserialize(proto: SelectEventTimelineProto): GetEventTimeline = {
    GetEventTimeline(fromProto(proto.getEventReference()), fromProto(proto.getReadTxTime()))
  }
}

object GetEventTransactionsSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[GetEventTransactions, GetEventTransactionsProto] {

  override def serialize(eventWithTt: GetEventTransactions): GetEventTransactionsProto = {
    GetEventTransactionsProto.newBuilder
      .setEventReference(toProto(eventWithTt.ref))
      .setReadTxTime(toProto(eventWithTt.tt))
      .build
  }

  override def deserialize(proto: GetEventTransactionsProto): GetEventTransactions = {
    GetEventTransactions(fromProto(proto.getEventReference), fromProto(proto.getReadTxTime))
  }
}

object GetAssociatedEntitiesSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[GetAssociatedEntities, SelectBusinessEventAssociatedEntitiesProto] {

  override def serialize(byEventRef: GetAssociatedEntities): SelectBusinessEventAssociatedEntitiesProto = {
    val builder = SelectBusinessEventAssociatedEntitiesProto.newBuilder
    builder.setEventReference(toProto(byEventRef.ref))
    builder.setReadVtTime(toProto(byEventRef.vt))
    builder.setReadTxTime(toProto(byEventRef.tt))
    if (byEventRef.typeName.isDefined)
      builder.setTypeName(byEventRef.typeName.get)

    builder.build
  }

  override def deserialize(proto: SelectBusinessEventAssociatedEntitiesProto): GetAssociatedEntities = {
    GetAssociatedEntities(
      fromProto(proto.getEventReference()),
      fromProto(proto.getReadVtTime()),
      fromProto(proto.getReadTxTime()),
      if (proto.hasTypeName) Some(proto.getTypeName) else None
    )
  }
}

object JarPathSerializer extends ProtoSerializer[ExistingNonStorableJarPath, JarPathProto] {
  override def serialize(jp: ExistingNonStorableJarPath): JarPathProto = {
    val builder = JarPathProto.newBuilder
    jp match {
      case AbsoluteJarPath(p) =>
        builder.setType(JarPathProto.Type.ABSOLUTE)
        builder.setPath(p)
      case MprJarPath(m, p, r, rp) =>
        builder.setType(JarPathProto.Type.MPR)
        builder.setMetaproject(m)
        builder.setProject(p)
        builder.setRelease(r)
        builder.setPath(rp)
    }
    builder.build()
  }

  override def deserialize(proto: JarPathProto): ExistingNonStorableJarPath = proto.getType() match {
    case JarPathProto.Type.ABSOLUTE => AbsoluteJarPath(proto.getPath)
    case JarPathProto.Type.MPR => MprJarPath(proto.getMetaproject, proto.getProject, proto.getRelease, proto.getPath)
  }
}

object ClientMachineIdentifierSerializer
    extends ProtoSerializer[ClientMachineIdentifier, ClientMachineIdentifierProto] {
  override def serialize(clientMachineIdentifier: ClientMachineIdentifier): ClientMachineIdentifierProto = {
    val proto: ClientMachineIdentifierProto.Builder = ClientMachineIdentifierProto.newBuilder()
    clientMachineIdentifier match {
      case UnremarkableClientMachine =>
        proto.setType(ClientMachineIdentifierProto.Type.UNREMARKABLE_CLIENT_MACHINE)
      case cm: TreadmillNode =>
        proto.setType(ClientMachineIdentifierProto.Type.TREADMILL)
        proto.setTreadmillApp(cm.treadmillApp.underlying)
        proto.setTreadmillInstanceId(cm.treadmillInstanceId.underlying)
      case cm: UnremarkableClientMachineWithHostPort =>
        proto.setType(ClientMachineIdentifierProto.Type.UNREMARKABLE_CLIENT_MACHINE_WITH_HOSTPORT)
        proto.setHost(cm.hostport.host.underlying)
        proto.setPort(cm.hostport.port.underlying)
    }
    proto.build
  }

  override def deserialize(proto: ClientMachineIdentifierProto): ClientMachineIdentifier = {
    proto.getType match {
      case cm if cm.equals(ClientMachineIdentifierProto.Type.TREADMILL) =>
        TreadmillNode(TreadmillApp(proto.getTreadmillApp), TreadmillInstanceId(proto.getTreadmillInstanceId))
      case cm if cm.equals(ClientMachineIdentifierProto.Type.UNREMARKABLE_CLIENT_MACHINE_WITH_HOSTPORT) => {
        UnremarkableClientMachineWithHostPort(HostPort(Host(proto.getHost), Port(proto.getPort)))
      }
      case _ => UnremarkableClientMachine
    }
  }
}

object EstablishSessionSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[(Int, EstablishSession), EstablishSessionProto] {

  def deserializeTxTime(proto: EstablishSessionProto): Option[Instant] = {
    if (proto.hasEstablishmentTime) Some(fromProto(proto.getEstablishmentTime)) else None
  }

  override def deserialize(proto: EstablishSessionProto): (Int, EstablishSession) = {
    val appIdentifier = fromProto(proto.getApplicationIdentifier())
    val context = fromProto(proto.getContext())
    val features = proto.getFeatureIdentifiersList().asScala map { Feature.fromValue(_) }
    val rolesetMode = {
      val rolesetProtos = proto.getRolesetsList().asScala
      val rolesets = rolesetProtos map { fromProto(_) } toList
      val establishAllRoles = if (proto.hasEstablishAllRoles) proto.getEstablishAllRoles() else false
      (rolesets, establishAllRoles) match {
        case (Nil, true)  => RolesetMode.AllRoles
        case (Nil, false) => RolesetMode.UseLegacyEntitlements
        case (rs, _)      => RolesetMode.SpecificRoleset(rs.flatten.toSet)
      }
    }
    val realId = proto.getRealId()
    val effectiveId = if (proto.hasEffectiveId()) Some(proto.getEffectiveId()) else None
    val establishmentTime = deserializeTxTime(proto)
    val classpathHash = ImmutableArray(proto.getClasspathHash().toByteArray())
    val sessionId = proto.getSessionIdentifier()
    val pid = Option(proto.getPid())
    val slotMapOverrides = if (proto.hasSlotMapOverrides) Option(fromProto(proto.getSlotMapOverrides)) else None
    val onBehalfTokenType =
      if (proto.hasOnBehalfTokenType) OnBehalfTokenType(proto.getOnBehalfTokenType) else OnBehalfTokenType.Default
    val onBehalfSessionToken =
      if (proto.hasOnBehalfSessionToken) Some(proto.getOnBehalfSessionToken.toByteArray.toVector) else None
    val clientPath = if (proto.hasClientPath) proto.getClientPath else ""
    val fullClasspath =
      if (proto.getFullClasspathCount != 0) Some(proto.getFullClasspathList.asScala.map(fromProto(_)).toSeq) else None
    val clientMachineIdentifier: ClientMachineIdentifier = fromProto(proto.getClientMachineIdentifierProto)
    (
      sessionId,
      EstablishSession(
        appIdentifier,
        SupportedFeatures(features),
        context,
        realId,
        effectiveId,
        rolesetMode,
        establishmentTime,
        classpathHash,
        fullClasspath,
        pid,
        slotMapOverrides,
        onBehalfTokenType,
        onBehalfSessionToken,
        clientPath,
        clientMachineIdentifier
      ))
  }

  override def serialize(commandAndId: (Int, EstablishSession)): EstablishSessionProto = {
    val (sessionId, command) = commandAndId
    val builder = EstablishSessionProto.newBuilder()
    builder.setSessionIdentifier(sessionId)
    builder.setApplicationIdentifier(toProto(command.applicationIdentifier))
    builder.setContext(toProto(command.context))
    val featureIdentifiers = (command.supportedFeatures.features map { f =>
      java.lang.Integer.valueOf(f.value)
    }).asJava
    builder.addAllFeatureIdentifiers(featureIdentifiers)
    command.rolesetMode match {
      case RolesetMode.SpecificRoleset(rs)                          => builder.addRolesets(toProto(rs))
      case RolesetMode.UseLegacyEntitlements | RolesetMode.AllRoles =>
    }
    if (command.effectiveId.isDefined) builder.setEffectiveId(command.effectiveId.get)
    if (command.establishmentTime.isDefined) builder.setEstablishmentTime(toProto(command.establishmentTime.get))
    if (command.pid.isDefined) builder.setPid(command.pid.get)
    if (command.slotMapOverrides.isDefined) builder.setSlotMapOverrides(toProto(command.slotMapOverrides.get))
    if (command.onBehalfSessionToken.isDefined) {
      builder.setOnBehalfTokenType(command.onBehalfTokenType.toString())
      builder.setOnBehalfSessionToken(ByteString.copyFrom(command.onBehalfSessionToken.get.toArray))
    }
    if (command.fullClasspath.isDefined) builder.addAllFullClasspath((command.fullClasspath.get.map(toProto(_))).asJava)
    builder.setRealId(command.realId)
    builder.setClasspathHash(ByteString.copyFrom(command.classpathHash.hash.rawArray))
    builder.setClientPath(command.clientPath)
    builder.setClientMachineIdentifierProto(toProto(command.sessionInfo.clientMachineIdentifier))
    builder.setEstablishAllRoles(command.establishAllRoles)
    builder.build()
  }
}

object EstablishSessionResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[EstablishSessionResult, EstablishSessionResultProto] {

  override def deserialize(proto: EstablishSessionResultProto): EstablishSessionResult = {
    if (proto.hasFailureType) {
      val failureType = proto.getFailureType match {
        case EstablishSessionResultProto.FailureType.GENERIC => EstablishSessionFailure.Type.Generic
        case EstablishSessionResultProto.FailureType.SESSION_PARAMETERS_CHANGED =>
          EstablishSessionFailure.Type.SessionParametersChanged
        case EstablishSessionResultProto.FailureType.UNREGISTERED_CLASSPATH =>
          EstablishSessionFailure.Type.UnregisteredClasspath
        case EstablishSessionResultProto.FailureType.NON_MPR_CLASSPATH => EstablishSessionFailure.Type.NonMprClasspath
        case EstablishSessionResultProto.FailureType.BROKER_PROXY_REQUIRED =>
          EstablishSessionFailure.Type.BrokerProxyRequired
        case EstablishSessionResultProto.FailureType.ONBEHALF_SESSION_INVALID_PROID =>
          EstablishSessionFailure.Type.OnBehalfSessionInvalidProid
        case EstablishSessionResultProto.FailureType.ONBEHALF_SESSION_INVALID_TOKEN =>
          EstablishSessionFailure.Type.OnBehalfSessionInvalidToken
      }
      val message = proto.getErrorMessage
      EstablishSessionFailure(failureType, message)
    } else {
      val sessionId = proto.getSessionIdentifier()
      val rolesetMode = {
        val roleset = fromProto(proto.getRoleset())
        if (roleset.isEmpty) RolesetMode.UseLegacyEntitlements else RolesetMode.SpecificRoleset(roleset)
      }
      val serverFeatures: Iterable[Feature] = proto
        .getFeatureIdentifiersList()
        .asScala map { case x =>
        if (x == Feature.RegisteredIndexes.value) {
          Feature.RegisteredIndexes(ByteStringEncoder.decodeAndDecompress(proto.getRegisteredEntities).toSet)
        } else { Feature.fromValue(x) }
      }
      val establishmentTime = fromProto(proto.getEstablishmentTime())
      val encryptedSessionToken =
        if (proto.hasEncryptedSessionToken) Some(proto.getEncryptedSessionToken.toByteArray.toVector) else None
      val appId = fromProto(proto.getApplicationIdentifier)
      val appConfigFound = if (proto.hasApplicationConfigFound) proto.getApplicationConfigFound else false

      val entitlementsToken = if (proto.hasEntitlementsToken) {
        val entitlementsTokenProto = proto.getEntitlementsToken
        if (entitlementsTokenProto.hasFailureType) {
          entitlementsTokenProto.getFailureType match {
            case EntitlementsToken.FailureType.UNSUPPORTED =>
              EntitlementsTokenUnsupported(entitlementsTokenProto.getFailureMessage)
            case EntitlementsToken.FailureType.INTERNAL =>
              EntitlementsTokenFailure(entitlementsTokenProto.getFailureMessage)
          }
        } else {
          val etsp = entitlementsTokenProto.getToken
          EntitlementsTokenSuccess(
            etsp.getHash,
            None,
            etsp.getEffectiveId,
            etsp.getSignature.toByteArray.toVector,
            etsp.getDescription,
            etsp.getEnv,
            Instant.ofEpochMilli(etsp.getSessionTxTime)
          )
        }
      } else {
        EntitlementsTokenUnsupported("Unsupported by DAL broker")
      }

      EstablishSessionSuccess(
        ResolvedClientAppIdentifier(appId),
        appConfigFound,
        sessionId,
        SupportedFeatures(serverFeatures),
        rolesetMode,
        establishmentTime,
        encryptedSessionToken,
        entitlementsToken,
        if (proto.hasProfileKeyHash) Some(ProfileKeyHash(proto.getProfileKeyHash.toByteArray)) else None
      )
    }
  }

  override def serialize(result: EstablishSessionResult): EstablishSessionResultProto = {
    val builder = EstablishSessionResultProto.newBuilder()
    result match {
      case EstablishSessionSuccess(
            appId,
            appConfigFound,
            sessionId,
            supportedFeatures,
            rolesetMode,
            establishmentTime,
            encryptedSessionToken,
            entitlementsToken,
            keyHash) =>
        builder.setSessionIdentifier(sessionId)
        val rolesetProto = rolesetMode match {
          case RolesetMode.SpecificRoleset(rs)   => RolesetProto.newBuilder().addAllRoleId(rs.asJava).build()
          case RolesetMode.UseLegacyEntitlements => RolesetProto.newBuilder().build()
        }
        builder.setRoleset(rolesetProto)
        val featureIdentifiers = (supportedFeatures.features map { f =>
          java.lang.Integer.valueOf(f.value)
        }).asJava
        supportedFeatures.registeredIndexes foreach { case Feature.RegisteredIndexes(registeredEntitySet) =>
          builder.setRegisteredEntities(
            ByteStringEncoder.encodeAndCompress(
              registeredEntitySet,
              CompressionType.getCompressionType(RegisteredIndexConfig.registeredEntitiesListCompressionType)
            )
          )

        }
        builder.addAllFeatureIdentifiers(featureIdentifiers)
        builder.setEstablishmentTime(toProto(establishmentTime))
        builder.setApplicationIdentifier(toProto(appId.underlying))
        builder.setApplicationConfigFound(appConfigFound)
        if (encryptedSessionToken.isDefined)
          builder.setEncryptedSessionToken(ByteString.copyFrom(encryptedSessionToken.get.toArray))

        val entitlementsTokenBuilder = EntitlementsToken.newBuilder()
        entitlementsToken match {
          case EntitlementsTokenUnsupported(reason) =>
            entitlementsTokenBuilder.setFailureType(EntitlementsToken.FailureType.UNSUPPORTED)
            entitlementsTokenBuilder.setFailureMessage(reason)
          case EntitlementsTokenFailure(reason) =>
            entitlementsTokenBuilder.setFailureType(EntitlementsToken.FailureType.INTERNAL)
            entitlementsTokenBuilder.setFailureMessage(reason)
          case EntitlementsTokenSuccess(
                entitlementsHash,
                // NB we currently just ignore the non-temporal hash, it doesn't need sending back to the client. At
                // some stage though we'll need to move this over to send back the non-temporal hash instead. This should
                // be totally transparent when we do it.
                _,
                effectiveId,
                tokenSignature,
                description,
                env,
                sessionTxTime) =>
            val successBuilder = EntitlementsToken.EntitlementsTokenSuccess.newBuilder()
            successBuilder.setHash(entitlementsHash)
            successBuilder.setEffectiveId(effectiveId)
            successBuilder.setSignature(ByteString.copyFrom(tokenSignature.toArray))
            successBuilder.setDescription(description)
            successBuilder.setEnv(env)
            successBuilder.setSessionTxTime(sessionTxTime.toEpochMilli)
            entitlementsTokenBuilder.setToken(successBuilder.build())
        }
        builder.setEntitlementsToken(entitlementsTokenBuilder.build())
        keyHash foreach { h =>
          builder.setProfileKeyHash(ByteString.copyFrom(h.data.rawArray))
        }

      case EstablishSessionFailure(failureType, message) =>
        val failureTypeProto = failureType match {
          case EstablishSessionFailure.Type.Generic => EstablishSessionResultProto.FailureType.GENERIC
          case EstablishSessionFailure.Type.SessionParametersChanged =>
            EstablishSessionResultProto.FailureType.SESSION_PARAMETERS_CHANGED
          case EstablishSessionFailure.Type.UnregisteredClasspath =>
            EstablishSessionResultProto.FailureType.UNREGISTERED_CLASSPATH
          case EstablishSessionFailure.Type.NonMprClasspath => EstablishSessionResultProto.FailureType.NON_MPR_CLASSPATH
          case EstablishSessionFailure.Type.BrokerProxyRequired =>
            EstablishSessionResultProto.FailureType.BROKER_PROXY_REQUIRED
          case EstablishSessionFailure.Type.OnBehalfSessionInvalidProid =>
            EstablishSessionResultProto.FailureType.ONBEHALF_SESSION_INVALID_PROID
          case EstablishSessionFailure.Type.OnBehalfSessionInvalidToken =>
            EstablishSessionResultProto.FailureType.ONBEHALF_SESSION_INVALID_TOKEN
        }
        builder.setFailureType(failureTypeProto)
        builder.setErrorMessage(message)
    }
    builder.build()
  }
}

// converts Array of classname strings to a single compressed bytestring and back
// encodes the compression method in returned sequence of bytes
object ByteStringEncoder {
  val clsnameListSeparator: String = " "
  def encodeAndCompress(input: TraversableOnce[String], cType: CompressionType): ByteString = {
    val uncompressed: Array[Byte] =
      input.mkString("", clsnameListSeparator, "").getBytes(Charset.forName("UTF-8"))
    ByteString.copyFrom(
      cType.compress(uncompressed) ++ Array(cType.encodingByte)
    ) // encode compression type info by adding a byte
  }
  def decodeAndDecompress(input: ByteString): Array[String] = {
    val inputArr = input.toByteArray
    val cType: CompressionType = CompressionType.getCompressionType(inputArr.last)
    val decompressed = cType.decompress(inputArr.dropRight(1)) // decompress after removing encoding byte
    (new String(decompressed, StandardCharsets.UTF_8)).split(clsnameListSeparator)
  }
}

sealed trait CompressionType {
  val name: String
  val encodingByte: Byte // designated appendable/prependable identifier for the compression technique
  def compress(input: Array[Byte]): Array[Byte]
  def decompress(input: Array[Byte]): Array[Byte]
}
object CompressionType {
  case object NoCompression extends CompressionType {
    override val name: String = "NONE"
    override val encodingByte: Byte = 0.toByte // avoid changing this value, might break old clients

    override def compress(input: Array[Byte]): Array[Byte] = input
    override def decompress(input: Array[Byte]): Array[Byte] = input
  }
  case object LZ4 extends CompressionType {
    override val name: String = "LZ4"
    lazy val lz4factory = LZ4Factory.safeInstance()
    val bestPossibleCompressionRatio = 4 // is usually less than 3 even in best cases, 4 to be on the safer side.
    override val encodingByte: Byte = 1.toByte // avoid changing this value, might break old clients

    override def compress(input: Array[Byte]): Array[Byte] = lz4factory.fastCompressor().compress(input)
    override def decompress(input: Array[Byte]): Array[Byte] =
      lz4factory.safeDecompressor().decompress(input, bestPossibleCompressionRatio * input.length)
  }
  def getCompressionType(b: Byte): CompressionType = b match {
    case NoCompression.encodingByte => NoCompression
    case LZ4.encodingByte           => LZ4
    case _ => throw new IllegalArgumentException(s"Byte ${b} does not correspond to any expected compression type")
  }
  def getCompressionType(name: String): CompressionType = name.toUpperCase match {
    case NoCompression.name => NoCompression
    case LZ4.name           => LZ4
    case _ => throw new IllegalArgumentException(s"${name} does not correspond to any expected compression type")
  }
}

object SessionTokenRequestSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[SessionTokenRequest, SessionTokenRequestProto] {

  override def deserialize(proto: SessionTokenRequestProto): SessionTokenRequest = {
    val effectiveID = if (proto.hasEffectiveId) Some(proto.getEffectiveId) else None
    val appID = if (proto.hasAppId) Some(proto.getAppId) else None
    val realID = if (proto.hasRealId) Some(proto.getRealId) else None
    // TODO (OPTIMUS-13040): once the multiple roles change has propagated to all environments
    // we can remove the optional role from dsi.proto and just use the repeated roles instead.
    val roles = Set(proto.getRole) ++ proto.getRolesList.asScala
    val token = if (proto.hasEncryptedSessionToken) Some(proto.getEncryptedSessionToken.toByteArray) else None
    val fi = if (proto.hasFeatureInfo) fromProto(proto.getFeatureInfo()) else ProtoFileProperties.NO_FEATURE_INFO
    SessionTokenRequest(roles, realID, effectiveID, appID, token, fi)
  }

  override def serialize(command: SessionTokenRequest): SessionTokenRequestProto = {
    val builder = SessionTokenRequestProto.newBuilder()
    // TODO (OPTIMUS-13040): once the multiple roles change has propagated to all environments
    // we can remove the optional role from dsi.proto and just use the repeated roles instead.
    builder.setRole(command.roles.head)
    builder.addAllRoles(command.roles.asJava)
    if (command.effectiveID != None) builder.setEffectiveId(command.effectiveID.get)
    if (command.appID != None) builder.setAppId(command.appID.get)
    if (command.realID != None) builder.setRealId(command.realID.get)
    if (command.token != None) builder.setEncryptedSessionToken(ByteString.copyFrom(command.token.get))
    builder.setFeatureInfo(toProto(command.featureInfo))
    builder.build()
  }
}

object CreateNewSessionSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[CreateNewSession, CreateNewSessionProto] {
  override def deserialize(proto: CreateNewSessionProto): CreateNewSession = {
    val es = fromProto(proto.getEstablishSession) match { case (_, es) => es }
    CreateNewSession(es)
  }
  override def serialize(command: CreateNewSession): CreateNewSessionProto = {
    // This session is not supposed to be used by client so setting -1 as session id
    val esProto = toProto((-1, command.es))
    CreateNewSessionProto.newBuilder().setEstablishSession(esProto).build()
  }
}

object CreateNewSessionResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[CreateNewSessionResult, CreateNewSessionResultProto] {
  override def deserialize(proto: CreateNewSessionResultProto): CreateNewSessionResult = {
    val esr = fromProto(proto.getEstablishSessionResult)
    CreateNewSessionResult(esr)
  }
  override def serialize(result: CreateNewSessionResult): CreateNewSessionResultProto = {
    val esrProto = toProto(result.esr)
    CreateNewSessionResultProto.newBuilder().setEstablishSessionResult(esrProto).build()
  }
}

object DiscoverServicesSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[DiscoverServices, DiscoverServicesProto] {
  override def serialize(ds: DiscoverServices): DiscoverServicesProto = {
    val builder = DiscoverServicesProto.newBuilder()
    builder.setEstablishSession(toProto(-1 -> ds.establishSession))
    builder.setDalEnv(ds.env.underlying)
    ds.sysLoc.foreach(builder.setSysLoc)
    builder.build()
  }

  override def deserialize(proto: DiscoverServicesProto): DiscoverServices = {
    val (_, es) = fromProto(proto.getEstablishSession)
    val env = DalEnv(proto.getDalEnv)
    val sysLocOpt = if (proto.hasSysLoc) Some(proto.getSysLoc) else None
    DiscoverServices(es, env, sysLocOpt)
  }
}

object DiscoverServicesResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[DiscoverServicesResult, DiscoverServicesResultProto] {
  override def serialize(dsr: DiscoverServicesResult): DiscoverServicesResultProto = {
    val builder = DiscoverServicesResultProto.newBuilder
    dsr match {
      case dss: DiscoverServicesSuccess =>
        val serviceProtos = dss.services.map(ServiceDiscoveryElementSerializer.serialize)
        builder.addAllServices(serviceProtos.asJava)
        builder.setEstablishSessionResult(toProto(dss.establishResult))
      case ces: DiscoverServicesFailure.CannotEstablishSession =>
        builder.setFailureType(DiscoverServicesResultProto.FailureType.CANNOT_ESTABLISH_SESSION)
        builder.setEstablishSessionResult(toProto(ces.establishResult))
      case cds: DiscoverServicesFailure.CannotDiscoverServices =>
        builder.setFailureType(DiscoverServicesResultProto.FailureType.CANNOT_DISCOVER_SERVICES)
        builder.setFailureMessage(cds.msg)
      case gf: DiscoverServicesFailure.GenericFailure =>
        builder.setFailureType(DiscoverServicesResultProto.FailureType.GENERIC)
        builder.setFailureMessage(gf.msg)
    }
    builder.build()
  }

  override def deserialize(proto: DiscoverServicesResultProto): DiscoverServicesResult = {
    if (proto.hasFailureType) {
      proto.getFailureType match {
        case DiscoverServicesResultProto.FailureType.GENERIC =>
          val msg =
            if (proto.hasFailureMessage) proto.getFailureMessage
            else "Unknown error while trying to discover services"
          DiscoverServicesFailure.GenericFailure(msg)
        case DiscoverServicesResultProto.FailureType.CANNOT_ESTABLISH_SESSION =>
          val esf = fromProto(proto.getEstablishSessionResult).asInstanceOf[EstablishSessionFailure]
          DiscoverServicesFailure.CannotEstablishSession(esf)
        case DiscoverServicesResultProto.FailureType.CANNOT_DISCOVER_SERVICES =>
          val msg = proto.getFailureMessage
          DiscoverServicesFailure.CannotDiscoverServices(msg)
      }
    } else {
      val services = proto.getServicesList.asScala.map(ServiceDiscoveryElementSerializer.deserialize)
      val ess = fromProto(proto.getEstablishSessionResult).asInstanceOf[EstablishSessionSuccess]
      DiscoverServicesSuccess(services, ess)
    }
  }
}

object ServiceDiscoveryElementSerializer
    extends ProtoSerializer[ServiceDiscoveryElement, ServiceDiscoveryElementProto] {
  override def serialize(elem: ServiceDiscoveryElement): ServiceDiscoveryElementProto = {
    val builder = ServiceDiscoveryElementProto.newBuilder
    val serviceTypeProto = elem.serviceType match {
      case DalServiceType.Read              => ServiceDiscoveryElementProto.ServiceType.READ
      case DalServiceType.Write             => ServiceDiscoveryElementProto.ServiceType.WRITE
      case DalServiceType.PubSub            => ServiceDiscoveryElementProto.ServiceType.PUB_SUB
      case DalServiceType.Accelerated       => ServiceDiscoveryElementProto.ServiceType.ACC
      case DalServiceType.ServiceDiscovery  => ServiceDiscoveryElementProto.ServiceType.SERVICE_DISCOVERY
      case DalServiceType.Deriv1Transformer => ServiceDiscoveryElementProto.ServiceType.DERIV1
      case DalServiceType.Baikal            => ServiceDiscoveryElementProto.ServiceType.BAIKAL
      case DalServiceType.Messages          => ServiceDiscoveryElementProto.ServiceType.MESSAGES
    }
    val serviceProviderProto = elem.serviceProviderType match {
      case DalServiceProviderType.Broker => ServiceDiscoveryElementProto.ServiceProviderType.BROKER
      case DalServiceProviderType.Prc    => ServiceDiscoveryElementProto.ServiceProviderType.PRC
      case DalServiceProviderType.Deriv1TransformationProcess =>
        ServiceDiscoveryElementProto.ServiceProviderType.DERIV1_BROKER
      case DalServiceProviderType.Proxy =>
        ServiceDiscoveryElementProto.ServiceProviderType.PROXY
    }
    val serviceLookupTypeProto = elem.lookupType match {
      case DalServiceLookupType.ZooKeeper => ServiceDiscoveryElementProto.ServiceLookupType.ZOO_KEEPER
    }
    builder.setServiceType(serviceTypeProto)
    builder.setServiceProvider(serviceProviderProto)
    builder.setServiceLookupType(serviceLookupTypeProto)
    elem.partition.foreach(ptn => builder.setPartitionName(ptn.name))
    builder.setLookup(elem.lookupStr)
    builder.build()
  }

  override def deserialize(proto: ServiceDiscoveryElementProto): ServiceDiscoveryElement = {
    def unknownValue(tpe: String): Nothing =
      throw new IllegalArgumentException(s"Cannot deserialize service discovery element due to unknown $tpe")

    val serviceType = proto.getServiceType match {
      case ServiceDiscoveryElementProto.ServiceType.READ              => DalServiceType.Read
      case ServiceDiscoveryElementProto.ServiceType.WRITE             => DalServiceType.Write
      case ServiceDiscoveryElementProto.ServiceType.PUB_SUB           => DalServiceType.PubSub
      case ServiceDiscoveryElementProto.ServiceType.ACC               => DalServiceType.Accelerated
      case ServiceDiscoveryElementProto.ServiceType.SERVICE_DISCOVERY => DalServiceType.ServiceDiscovery
      case ServiceDiscoveryElementProto.ServiceType.DERIV1            => DalServiceType.Deriv1Transformer
      case ServiceDiscoveryElementProto.ServiceType.BAIKAL            => DalServiceType.Baikal
      case ServiceDiscoveryElementProto.ServiceType.MESSAGES          => DalServiceType.Messages
      case ServiceDiscoveryElementProto.ServiceType.UNKNOWN_SERVICE   => unknownValue("ServiceType")
    }
    val serviceProvider = proto.getServiceProvider match {
      case ServiceDiscoveryElementProto.ServiceProviderType.BROKER => DalServiceProviderType.Broker
      case ServiceDiscoveryElementProto.ServiceProviderType.PRC    => DalServiceProviderType.Prc
      case ServiceDiscoveryElementProto.ServiceProviderType.DERIV1_BROKER =>
        DalServiceProviderType.Deriv1TransformationProcess
      case ServiceDiscoveryElementProto.ServiceProviderType.PROXY =>
        DalServiceProviderType.Proxy
      case ServiceDiscoveryElementProto.ServiceProviderType.UNKNOWN_PROVIDER => unknownValue("ServiceProvider")
    }
    val lookupType = proto.getServiceLookupType match {
      case ServiceDiscoveryElementProto.ServiceLookupType.ZOO_KEEPER     => DalServiceLookupType.ZooKeeper
      case ServiceDiscoveryElementProto.ServiceLookupType.UNKNOWN_LOOKUP => unknownValue("ServiceLookup")
    }
    val partition =
      if (proto.hasPartitionName) Some(PartitionHelper.getPartitionForString(proto.getPartitionName)) else None
    val lookup = proto.getLookup
    ServiceDiscoveryElement(serviceType, serviceProvider, lookupType, partition, lookup)
  }
}

object SessionTokenResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[SessionTokenResult, SessionTokenResultProto] {

  override def deserialize(proto: SessionTokenResultProto): SessionTokenResult = {
    val realID = if (proto.hasRealId) Some(proto.getRealId) else None
    val roles = proto.getRolesList.asScala
    SessionTokenResult(
      realID,
      roles.toSet,
      proto.getEncryptedSessionToken().toByteArray(),
      (if (proto.hasFeatureInfo) fromProto(proto.getFeatureInfo) else ProtoFileProperties.NO_FEATURE_INFO)
    )
  }

  override def serialize(command: SessionTokenResult): SessionTokenResultProto = {
    val builder =
      SessionTokenResultProto.newBuilder().setEncryptedSessionToken(ByteString.copyFrom(command.encryptedToken))
    builder.addAllRoles(command.roles.asJava)
    if (command.realID.isDefined) builder.setRealId(command.realID.get)
    builder.setFeatureInfo(toProto(command.featureInfo))
    builder.build()
  }
}

object HeartbeatSerializer extends GetCommandProtoSerialization with ProtoSerializer[Heartbeat, HeartbeatProto] {

  override def deserialize(proto: HeartbeatProto): Heartbeat = {
    Heartbeat(proto.getUuid(), fromProto(proto.getSentTime()))
  }

  override def serialize(command: Heartbeat): HeartbeatProto = {
    HeartbeatProto.newBuilder().setUuid(command.uuid).setSentTime(toProto(command.sentTime)).build()
  }
}

object HeartbeatResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[HeartbeatResult, HeartbeatResultProto] {

  override def deserialize(proto: HeartbeatResultProto): HeartbeatResult = {
    HeartbeatResult(fromProto(proto.getReceiveTime()))
  }

  override def serialize(command: HeartbeatResult): HeartbeatResultProto = {
    HeartbeatResultProto.newBuilder().setReceiveTime(toProto(command.receiveTime)).build()
  }
}

object GetProtoFilePropertiesSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[GetProtoFileProperties, GetProtoFilePropertiesProto] {
  val allowSendingGetProtoFileProperties =
    java.lang.Boolean.getBoolean("optimus.platform.dsi.allowSendingGetProtoFileProperties")

  override def deserialize(proto: GetProtoFilePropertiesProto): GetProtoFileProperties = {
    GetProtoFileProperties(
      proto.getLocalProtoFileVersion,
      (if (proto.hasClientFeature) fromProto(proto.getClientFeature) else ProtoFileProperties.NO_FEATURE_INFO))
  }

  override def serialize(command: GetProtoFileProperties): GetProtoFilePropertiesProto = {
    if (allowSendingGetProtoFileProperties) {
      GetProtoFilePropertiesProto
        .newBuilder()
        .setLocalProtoFileVersion(command.localProtoFileVersionNum)
        .setClientFeature(toProto(command.clientFeature))
        .build
    } else
      throw new UnsupportedOperationException(
        "Sending a GetProtoFileProperties is no longer supported. This should not have happened.")
  }
}

object GetProtoFilePropertiesResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[GetProtoFilePropertiesResult, GetProtoFilePropertiesResultProto] {

  override def deserialize(proto: GetProtoFilePropertiesResultProto): GetProtoFilePropertiesResult = {
    GetProtoFilePropertiesResult(
      proto.getRemoteProtoFileVersion(),
      (if (proto.hasRemoteFeature) fromProto(proto.getRemoteFeature) else ProtoFileProperties.NO_FEATURE_INFO))
  }

  override def serialize(command: GetProtoFilePropertiesResult): GetProtoFilePropertiesResultProto = {
    GetProtoFilePropertiesResultProto
      .newBuilder()
      .setRemoteProtoFileVersion(command.remoteProtoFileVersionNum)
      .setRemoteFeature(toProto(command.featureInfo))
      .build
  }
}

object SelectResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[SelectResult, SelectResultProto] {

  override def deserialize(proto: SelectResultProto): SelectResult = {
    val serializedResults = proto.getPersistentEntityList.asScala.toSeq
    SelectResult(serializedResults map { fromProto(_) })
  }

  override def serialize(result: SelectResult): SelectResultProto = {
    SelectResultProto.newBuilder
      .addAllPersistentEntity((result.value map { toProto(_) } toSeq).asJava)
      .build
  }
}

object ResultStatsSerializer extends GetCommandProtoSerialization with ProtoSerializer[ResultStats, ResultStatsProto] {

  override def deserialize(proto: ResultStatsProto): ResultStats = {
    val resultStats = if (proto.hasType) {
      proto.getType match {
        case ResultStatsProto.Type.StringStats =>
          val typeIdToCnt: Map[String, Long] = proto.getDescToCntList.asScala.map(p => p.getDesc -> p.getCnt).toMap
          ResultStatsByString(typeIdToCnt)
        case ResultStatsProto.Type.IntStats =>
          val typeIdToCnt: Map[Int, Long] = proto.getTidToCntList.asScala.map(p => p.getCnid -> p.getCnt).toMap
          ResultStatsByInt(typeIdToCnt)
      }
    } else {
      val typeIdToCnt: Map[Int, Long] = proto.getTidToCntList.asScala.map(p => p.getCnid -> p.getCnt).toMap
      ResultStatsByInt(typeIdToCnt)
    }
    if (proto.getPartitionNamesCount > 0)
      resultStats.attachPartitions(
        proto.getPartitionNamesList.asScala.iterator.map(PartitionHelper.getPartitionForString).toSet)
    resultStats
  }

  override def serialize(stats: ResultStats): ResultStatsProto = {

    val builder = ResultStatsProto.newBuilder
    stats match {
      case s: ResultStatsByString =>
        builder.setType(ResultStatsProto.Type.StringStats)
        val st = s.cnIdtoCount.map { case (desc, cnt) =>
          DescriptionToCountProto.newBuilder
            .setDesc(desc)
            .setCnt(cnt)
            .build
        }
        builder.addAllDescToCnt(st.asJava)
      case s: ResultStatsByInt =>
        builder.setType(ResultStatsProto.Type.IntStats)
        val st = s.cnIdtoCount.map { case (cnid, cnt) =>
          TypeIdToCountProto.newBuilder
            .setCnid(cnid)
            .setCnt(cnt)
            .build
        }
        builder.addAllTidToCnt(st.asJava)
    }
    stats.getPartitions.foreach(p => builder.addPartitionNames(p.name))
    builder.build()
  }
}

object GetAuditInfoResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[GetAuditInfoResult, GetAuditInfoResultProto] {

  override def deserialize(proto: GetAuditInfoResultProto): GetAuditInfoResult = {
    val serializedAuditInfo = proto.getAuditInfoList.asScala.toSeq
    GetAuditInfoResult(serializedAuditInfo map { fromProto(_) })
  }

  override def serialize(result: GetAuditInfoResult): GetAuditInfoResultProto = {
    GetAuditInfoResultProto.newBuilder
      .addAllAuditInfo((result.value map { toProto(_) } toSeq).asJava)
      .build
  }
}

object PartialGetAuditInfoResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[PartialGetAuditInfoResult, PartialGetAuditInfoResultProto] {

  override def deserialize(proto: PartialGetAuditInfoResultProto): PartialGetAuditInfoResult = {
    val serializedAuditInfo = proto.getAuditInfoList.asScala.toSeq
    PartialGetAuditInfoResult(serializedAuditInfo map { fromProto(_) }, proto.getIsLast)
  }

  override def serialize(result: PartialGetAuditInfoResult): PartialGetAuditInfoResultProto = {
    PartialGetAuditInfoResultProto.newBuilder
      .addAllAuditInfo((result.value map { toProto(_) } toSeq).asJava)
      .setIsLast(result.isLast)
      .build
  }
}

object GetBusinessEventResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[GetBusinessEventResult, SelectBusinessEventResultProto] {

  override def deserialize(proto: SelectBusinessEventResultProto): GetBusinessEventResult = {
    val serializedEvents = proto.getBusinessEventList().asScala.toSeq
    GetBusinessEventResult(serializedEvents map { fromProto(_) })
  }

  override def serialize(result: GetBusinessEventResult): SelectBusinessEventResultProto = {
    SelectBusinessEventResultProto.newBuilder
      .addAllBusinessEvent((result.events map { toProto(_) } toSeq).asJava)
      .build
  }
}

object PartialGetBusinessEventResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[PartialGetBusinessEventResult, PartialSelectBusinessEventResultProto] {

  override def deserialize(proto: PartialSelectBusinessEventResultProto): PartialGetBusinessEventResult = {
    val serializedEvents = proto.getBusinessEventList().asScala.toSeq
    PartialGetBusinessEventResult(serializedEvents map { fromProto(_) }, proto.getIsLast)
  }

  override def serialize(result: PartialGetBusinessEventResult): PartialSelectBusinessEventResultProto = {
    PartialSelectBusinessEventResultProto.newBuilder
      .addAllBusinessEvent((result.events map { toProto(_) } toSeq).asJava)
      .setIsLast(result.isLast)
      .build
  }
}

object GetBusinessEventWithTTToResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[GetBusinessEventWithTTToResult, SelectBusinessEventWithTTToResultProto] {

  override def deserialize(proto: SelectBusinessEventWithTTToResultProto): GetBusinessEventWithTTToResult = {
    val serializedEvents = proto.getBusinessEventList().asScala.toSeq
    GetBusinessEventWithTTToResult(serializedEvents map { fromProto(_) })
  }

  override def serialize(result: GetBusinessEventWithTTToResult): SelectBusinessEventWithTTToResultProto = {
    SelectBusinessEventWithTTToResultProto.newBuilder
      .addAllBusinessEvent((result.events map { toProto(_) } toSeq).asJava)
      .build
  }
}

object GetInitiatingEventResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[GetInitiatingEventResult, GetInitiatingEventResultProto] {

  override def deserialize(proto: GetInitiatingEventResultProto): GetInitiatingEventResult = {
    val eventOpt = if (proto.hasBusinessEvent) Some(fromProto(proto.getBusinessEvent)) else None
    val entityOpt = if (proto.hasPersistentEntity) Some(fromProto(proto.getPersistentEntity)) else None
    GetInitiatingEventResult(eventOpt, entityOpt)
  }

  override def serialize(result: GetInitiatingEventResult): GetInitiatingEventResultProto = {
    val GetInitiatingEventResult(eventOpt, entityOpt) = result
    val builder = GetInitiatingEventResultProto.newBuilder
    eventOpt.foreach(event => builder.setBusinessEvent(toProto(event)))
    entityOpt.foreach(entity => builder.setPersistentEntity(toProto(entity)))
    builder.build
  }
}

object PartialSelectResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[PartialSelectResult, PartialSelectResultProto] {

  override def deserialize(proto: PartialSelectResultProto): PartialSelectResult = {
    val serializedResults: mutable.Seq[PersistentEntityProto] =
      proto.getPersistentEntityList.asScala // PersistentEntityProto - contain entityRef strings
    // -> ProtoSerializationFrom#fromProto -> BasicProtoSer#deserialize -> ... BPS#des:247 : SerializedEntity
    PartialSelectResult(serializedResults map { fromProto(_) }, proto.getLast())
  }

  override def serialize(result: PartialSelectResult): PartialSelectResultProto = {
    PartialSelectResultProto.newBuilder
      .addAllPersistentEntity((result.value map { toProto(_) }).asJava)
      .setLast(result.isLast)
      .build
  }
}

object PrcPartialSelectResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[PrcPartialSelectResult, PrcPartialSelectResultProto] {

  override def deserialize(proto: PrcPartialSelectResultProto): PrcPartialSelectResult = {
    require(proto.hasUnderlying, "PrcPartialSelectResultProto should have the underlying PartialSelectResult")
    val pss = fromProto(proto.getUnderlying)
    val txInterval = fromProto(proto.getTxInterval)
    val vtInterval = fromProto(proto.getVtInterval)
    val lsqtMap: Map[Partition, Instant] = proto.getLsqtMapKeysList.asScala
      .zip(proto.getLsqtMapValuesList.asScala)
      .iterator
      .map { case (partitionName: String, lsqtProto: InstantProto) =>
        val p: Partition =
          if (partitionName == DefaultPartition.name) DefaultPartition else NamedPartition(partitionName)
        val lsqt: Instant = fromProto(lsqtProto)
        p -> lsqt
      }
      .toMap
    PrcPartialSelectResult(pss, vtInterval, txInterval, lsqtMap)
  }

  override def serialize(result: PrcPartialSelectResult): PrcPartialSelectResultProto = {
    val builder = PrcPartialSelectResultProto.newBuilder
    builder.setUnderlying(toProto(result.underlying))
    builder.setTxInterval(toProto(result.txInterval))
    builder.setVtInterval(toProto(result.vtInterval))
    result.remoteLsqtMap.foreach { case (partition: Partition, lsqt: Instant) =>
      builder.addLsqtMapKeys(partition.name)
      builder.addLsqtMapValues(toProto(lsqt))
    }
    builder.build()
  }
}

object CountResultSerializer extends GetCommandProtoSerialization with ProtoSerializer[CountResult, CountResultProto] {

  override def deserialize(proto: CountResultProto): CountResult = {
    new CountResult(proto.getNumber())
  }

  override def serialize(res: CountResult): CountResultProto = {
    CountResultProto.newBuilder
      .setNumber(res.number)
      .build
  }
}

object FeatureInfoSerializer extends GetCommandProtoSerialization with ProtoSerializer[FeatureInfo, FeatureInfoProto] {

  override def deserialize(proto: FeatureInfoProto): FeatureInfo = {
    FeatureInfo(proto.getFeatureBuild, proto.getFeatureInfoTag)
  }

  override def serialize(fInfo: FeatureInfo): FeatureInfoProto = {
    FeatureInfoProto.newBuilder
      .setFeatureBuild(fInfo.build)
      .setFeatureInfoTag(fInfo.infoTag)
      .build
  }
}

object GetInfoResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[GetInfoResult, GetInfoResultProto] {

  override def deserialize(proto: GetInfoResultProto): GetInfoResult = {
    import optimus.dsi.partitioning.PartitionHelper._

    val fi = if (proto.hasServerFeature) fromProto(proto.getServerFeature) else ProtoFileProperties.NO_FEATURE_INFO
    val partitionLsqtMap = proto.getPartitionLsqtsList.asScala.map { l =>
      (getPartitionForString(l.getPartitionName), fromProto(l.getLsqt))
    }.toMap
    val timeLordClocks = proto.getTimelordClocksList.asScala.map { tlc =>
      (tlc.getSystemName, fromProto(tlc.getTime))
    }.toMap
    GetInfoResult(fromProto(proto.getServerTime), fi, timeLordClocks, partitionLsqtMap)
  }

  override def serialize(res: GetInfoResult): GetInfoResultProto = {
    GetInfoResultProto.newBuilder
      .setServerTime(toProto(res.serverTime))
      .setServerFeature(toProto(res.featureInfo))
      .addAllTimelordClocks(serializeTimeLordClocks(res.timeLordClocks))
      .addAllPartitionLsqts(serializeLsqtMap(res.partitionLsqtMap))
      .build
  }

  private def serializeTimeLordClocks(timeLordClocks: Map[String, Instant]): java.lang.Iterable[TimelordClockProto] = {
    val tlcProtos = timeLordClocks.map { case (systemName, time) =>
      TimelordClockProto.newBuilder
        .setSystemName(systemName)
        .setTime(toProto(time))
        .build
    }
    tlcProtos.asJava
  }

  private[optimus] def serializeLsqtMap(
      partitionLsqtMap: Map[Partition, Instant]): java.lang.Iterable[PartitionLsqtProto] = {
    val partitionLsqtProto = partitionLsqtMap map { case (partition, lsqt) =>
      PartitionLsqtProto.newBuilder
        .setPartitionName(partition.name)
        .setLsqt(toProto(lsqt))
        .build
    }

    partitionLsqtProto.asJava
  }
}

object RoleMembershipQuerySerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[RoleMembershipQuery, RoleMembershipQueryProto] {

  override def deserialize(proto: RoleMembershipQueryProto): RoleMembershipQuery = {
    new RoleMembershipQuery(proto.getRealId)
  }
  override def serialize(cmd: RoleMembershipQuery): RoleMembershipQueryProto = {
    RoleMembershipQueryProto.newBuilder
      .setRealId(cmd.realId)
      .build
  }
}

object CanPerformActionSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[CanPerformAction, CanPerformActionProto] {

  val protoActionMappings = Map(
    CanPerformActionProto.ActionType.READ -> EntityAction.Read,
    CanPerformActionProto.ActionType.CREATE -> EntityAction.Create,
    CanPerformActionProto.ActionType.UPDATE -> EntityAction.Update,
    CanPerformActionProto.ActionType.INVALIDATE -> EntityAction.Invalidate
  )

  val actionProtoMappings = protoActionMappings
    .map { case (proto, action) => action -> proto }
    .toMap[EntityAction, CanPerformActionProto.ActionType]

  override def deserialize(proto: CanPerformActionProto): CanPerformAction = {
    new CanPerformAction(proto.getTypeRef, protoActionMappings(proto.getAction))
  }
  override def serialize(cmd: CanPerformAction): CanPerformActionProto = {
    CanPerformActionProto.newBuilder
      .setTypeRef(cmd.typeRef)
      .setAction(actionProtoMappings(cmd.action))
      .build
  }
}

object BooleanResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[BooleanResult, BooleanResultProto] {

  override def deserialize(proto: BooleanResultProto): BooleanResult = {
    new BooleanResult(proto.getValue)
  }
  override def serialize(cmd: BooleanResult): BooleanResultProto = {
    BooleanResultProto.newBuilder
      .setValue(cmd.value)
      .build
  }
}

object RoleMembershipResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[RoleMembershipResult, RoleMembershipResultProto] {

  override def deserialize(proto: RoleMembershipResultProto): RoleMembershipResult = {
    new RoleMembershipResult(proto.getRoleIdList.asScala.toSet)
  }

  override def serialize(res: RoleMembershipResult): RoleMembershipResultProto = {
    RoleMembershipResultProto.newBuilder
      .addAllRoleId(res.roleIds.asJava)
      .build
  }
}

object TemporalityQueryCommandSerializer
    extends GetCommandProtoSerialization
    with DSIQueryTemporalitySerialization
    with ProtoSerializer[TemporalityQueryCommand, TemporalityQueryCommandProto] {
  protected override val log = getLogger(this)

  override def deserialize(proto: TemporalityQueryCommandProto): TemporalityQueryCommand = {
    val temporality = getTemporality(proto)

    val queryDataType = fromProto(proto.getDataType())
    val queryResultType =
      if (proto.getQueryResultType() == TemporalityQueryCommandProto.ResultType.DATA_REFERENCE)
        DALQueryResultType.StorableReference
      else DALQueryResultType.FullData

    new TemporalityQueryCommand(
      ImmutableByteArray(proto.getQueryTree().toByteArray),
      proto.getClassNamesList().asScala.toSeq,
      temporality,
      queryDataType,
      queryResultType,
      proto.getTreeVersion()
    )()
  }

  override def serialize(req: TemporalityQueryCommand): TemporalityQueryCommandProto = {
    // TODO (OPTIMUS-13040): duplicate from toProto(req: EnumerateKeys)
    val queryDataType = toProto(req.dataType)

    val queryResultType =
      if (DALQueryResultType.StorableReference == req.queryResultType)
        TemporalityQueryCommandProto.ResultType.DATA_REFERENCE
      else TemporalityQueryCommandProto.ResultType.FULL_DATA

    val bld = TemporalityQueryCommandProto.newBuilder
      .setQueryTree(ByteString.copyFrom(req.query.data))
      .addAllClassNames(req.classNames.asJava)
      .setDataType(queryDataType)
      .setQueryResultType(queryResultType)
      .setTreeVersion(req.treeVersion)

    val builder = req.temporality match {
      case All(readTT)           => bld.setReadTxTime(toProto(readTT))
      case TxTime(tx)            => bld.setTxTime(toProto(tx))
      case ValidTime(vt, readTT) => bld.setValidTime(toProto(vt)).setReadTxTime(toProto(readTT))
      case At(vt, tx)            => bld.setTxTime(toProto(tx)).setValidTime(toProto(vt))
      case TxRange(_) =>
        throw new UnsupportedOperationException("TxRange not supported in TemporalityQueryCommand serialization.")
      case BitempRange(_, _, _) =>
        throw new UnsupportedOperationException(
          "BitempRange is not supported for TemporalityQueryCommand serialization.")
      case OpenVtTxRange(_) =>
        throw new UnsupportedOperationException(
          "OpenVtTxRange is not supported for TemporalityQueryCommand serialization.")
    }

    builder.build
  }
}
trait QueryReferenceResultSerialization {

  def getRefWithTxTimesAndVref(
      refs: Iterable[StorableReference with TypedReference],
      txTimesFromProto: Iterable[Option[Instant]],
      vrefsFromProto: Iterable[Option[VersionedReference]],
      isPartial: Boolean = false) = {
    val txTimes = if (txTimesFromProto.isEmpty) List.fill(refs.size)(None) else txTimesFromProto
    val vrefs = if (vrefsFromProto.isEmpty) List.fill(refs.size)(None) else vrefsFromProto

    val resultType = if (isPartial) PartialQueryReferenceResult.getClass else QueryReferenceResult.getClass
    require(
      refs.size == txTimes.size && refs.size == vrefs.size,
      s"unexpected number of refs (${refs.size})/ txTimes (${txTimes.size})/ vrefs ${vrefs.size} in ${resultType.getSimpleName}"
    )
    (refs, txTimes, vrefs).zipped.toIterable
  }
}

object QueryReferenceResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[QueryReferenceResult, QueryReferenceResultProto]
    with QueryReferenceResultSerialization {

  override def deserialize(proto: QueryReferenceResultProto): QueryReferenceResult = {
    val dataType = fromProto(proto.getDataType)
    val typeIds = proto.getTypeIdList().asScala
    val refList = proto.getReferenceList().asScala
    val refWithTypeIds = refList.zip(typeIds)
    val refs: Iterable[StorableReference with TypedReference] = refWithTypeIds.map { case (r, typeId) =>
      if (dataType == DALDataType.Entity)
        EntityReference.finalTypedRef(r.getData.toByteArray, typeId)
      else
        new TypedBusinessEventReference(r.getData.toByteArray, typeId)
    }
    val txTimesFromProto = proto.getTxTimeList().asScala.map(t => Some(fromProto(t)))
    val vrefsFromProto = proto.getVersionedRefList.asScala.map(v => Some(fromProto(v)))
    val refWithTxTimesAndVref = getRefWithTxTimesAndVref(refs, txTimesFromProto, vrefsFromProto)
    QueryReferenceResult(dataType, refWithTxTimesAndVref)
  }

  override def serialize(result: QueryReferenceResult): QueryReferenceResultProto = {
    val (refs, optionTxTimes, vrefs) = result.refs.unzip3
    require(optionTxTimes.forall(_.isDefined))
    val txTimes = optionTxTimes.map(_.get)
    val refList =
      (refs.toSeq.map(ref => { StorableReferenceProto.newBuilder.setData(ByteString.copyFrom(ref.data)).build })).asJava
    val txTimeList = (txTimes.toSeq.map(tx => toProto(tx))).asJava
    val vrefsList = vrefs.map(v => toProto(v.get)).asJava

    QueryReferenceResultProto.newBuilder
      .setDataType(toProto(result.dataType))
      .addAllReference(refList)
      .addAllTxTime(txTimeList)
      .addAllVersionedRef(vrefsList)
      .build
  }
}

object PartialQueryReferenceResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[PartialQueryReferenceResult, PartialQueryReferenceResultProto]
    with QueryReferenceResultSerialization {

  override def deserialize(proto: PartialQueryReferenceResultProto): PartialQueryReferenceResult = {
    val dataType = fromProto(proto.getDataType)
    val typeIds = proto.getTypeIdList().asScala
    val refList = proto.getReferenceList().asScala
    val refWithTypeIds = refList.zip(typeIds)
    val refs: Iterable[StorableReference with TypedReference] = refWithTypeIds.map { case (r, typeId) =>
      if (dataType == DALDataType.Entity)
        EntityReference.finalTypedRef(r.getData.toByteArray, typeId)
      else
        new TypedBusinessEventReference(r.getData.toByteArray, typeId)
    }
    val txTimesFromProto = proto.getTxTimeList().asScala.map(t => Some(fromProto(t)))
    val vrefsFromProto = proto.getVersionedRefList.asScala.map(v => Some(fromProto(v)))
    val refWithTxTimesAndVref = getRefWithTxTimesAndVref(refs, txTimesFromProto, vrefsFromProto)
    PartialQueryReferenceResult(dataType, refWithTxTimesAndVref, proto.getIsLast)
  }

  override def serialize(result: PartialQueryReferenceResult): PartialQueryReferenceResultProto = {
    val (refs, optionTxTimes, vrefs) = result.refs.unzip3
    require(optionTxTimes.forall(_.isDefined))
    val txTimes = optionTxTimes.map(_.get)
    val refList =
      (refs.toSeq.map(ref => { StorableReferenceProto.newBuilder.setData(ByteString.copyFrom(ref.data)).build })).asJava
    val typeIdsList =
      (refs.toSeq.map(i => Integer.valueOf(i.typeId))).asJava
    val txTimeList = (txTimes.toSeq.map(tx => toProto(tx))).asJava
    val vrefsList = vrefs.map(v => toProto(v.get)).asJava

    PartialQueryReferenceResultProto.newBuilder
      .setDataType(toProto(result.dataType))
      .addAllReference(refList)
      .addAllTxTime(txTimeList)
      .addAllVersionedRef(vrefsList)
      .addAllTypeId(typeIdsList)
      .setIsLast(result.isLast)
      .build
  }
}

object GetRefsNotAtSlotSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[GetRefsNotAtSlot, GetRefsNotAtSlotProto]
    with DSIQueryTemporalitySerialization {
  protected val log = getLogger(this)

  override def deserialize(proto: GetRefsNotAtSlotProto): GetRefsNotAtSlot = {
    val temporality = getTemporality(proto)
    val query =
      if (proto.hasReferenceQuery) fromProto(proto.getReferenceQuery)
      else if (proto.hasSerializedKeyQuery) fromProto(proto.getSerializedKeyQuery)
      else if (proto.hasEntityClassQuery) fromProto(proto.getEntityClassQuery)
      else if (proto.hasLinkageQuery) fromProto(proto.getLinkageQuery)
      else throw new IllegalArgumentException(s"Query type not supported for GetRefsAtSlot")
    val slot = proto.getSlot
    val fqcn = proto.getFqcn
    GetRefsNotAtSlot(query, fqcn, slot, temporality)
  }

  override def serialize(cmd: GetRefsNotAtSlot): GetRefsNotAtSlotProto = {
    val builder = GetRefsNotAtSlotProto.newBuilder
    // TODO (OPTIMUS-13040): this needs refactoring out (copied and pasted)
    cmd.temporality match {
      case DSIQueryTemporality.All(readTT)           => builder.setReadTxTime(toProto(readTT))
      case DSIQueryTemporality.TxTime(tx)            => builder.setTxTime(toProto(tx))
      case DSIQueryTemporality.ValidTime(vt, readTT) => builder.setValidTime(toProto(vt)).setReadTxTime(toProto(readTT))
      case DSIQueryTemporality.At(vt, tx)            => builder.setTxTime(toProto(tx)).setValidTime(toProto(vt))
      case DSIQueryTemporality.TxRange(range)        => builder.setTxRange(toProto(range))
      case BitempRange(_, _, _) =>
        throw new UnsupportedOperationException("BitempRange is not supported for GetRefsNotAtSlot serialization.")
      case OpenVtTxRange(_) =>
        throw new UnsupportedOperationException("OpenVtTxRange is not supported for GetRefsNotAtSlot serialization.")
    }
    cmd.query match {
      case q: SerializedKeyQuery => builder.setSerializedKeyQuery(toProto(q))
      case q: EntityClassQuery   => builder.setEntityClassQuery(toProto(q))
      case q: LinkageQuery       => builder.setLinkageQuery(toProto(q))
      case q: ReferenceQuery     => builder.setReferenceQuery(toProto(q))
      case o =>
        throw new IllegalArgumentException(s"Query type ${o.getClass.getName} not supported for GetRefsNotAtSlot")
    }
    builder.setSlot(cmd.slot)
    builder.setFqcn(cmd.fqcn)
    builder.build
  }
}

object GetRefsNotAtSlotResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[GetRefsNotAtSlotResult, GetRefsNotAtSlotResultProto] {
  override def deserialize(proto: GetRefsNotAtSlotResultProto): GetRefsNotAtSlotResult = {
    val refs = proto.getEntityReferencesList.asScala.map(r => EntityReference(r.getData.toByteArray))
    GetRefsNotAtSlotResult(refs)
  }

  override def serialize(result: GetRefsNotAtSlotResult): GetRefsNotAtSlotResultProto = {
    val entityRefs = Iterable.newBuilder[EntityReferenceProto]
    result.refs.foreach {
      case er: EntityReference => entityRefs += toProto(er)
      case o =>
        throw new IllegalArgumentException(
          s"Unsupported reference ${o.toString} of type ${o.getClass.getName} in GetRefsNotAtSlot result set")
    }
    GetRefsNotAtSlotResultProto.newBuilder.addAllEntityReferences(entityRefs.result().asJava).build
  }
}

object GetSlotsSerializer extends GetCommandProtoSerialization with ProtoSerializer[GetSlots, GetSlotsProto] {
  override def serialize(cmd: GetSlots): GetSlotsProto = {
    val builder = GetSlotsProto.newBuilder
    builder.setClassName(cmd.fqcn)
    builder.build
  }

  override def deserialize(proto: GetSlotsProto): GetSlots = {
    val className = proto.getClassName
    GetSlots(className)
  }
}
object GetSlotsResultSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[GetSlotsResult, GetSlotsResultProto] {
  override def deserialize(proto: GetSlotsResultProto): GetSlotsResult = {
    val slots = proto.getSlotsList.asScala.map(_.toInt).toSet
    new GetSlotsResult(slots)
  }

  override def serialize(res: GetSlotsResult): GetSlotsResultProto = {
    val builder = GetSlotsResultProto.newBuilder
    if (res.slots.nonEmpty)
      builder.addAllSlots((res.slots.map(Integer.valueOf(_))).asJava)
    builder.build()
  }
}

object ExpressionQuerySerializer
    extends GetCommandProtoSerialization
    with ExpressionSerialization
    with ProtoSerializer[ExpressionQueryCommand, ExpressionQueryCommandProto] {
  import ExpressionQueryCommandProto.{QueryPlan => QueryPlanProto}

  override def serialize(command: ExpressionQueryCommand): ExpressionQueryCommandProto = {
    val plan = command.plan match {
      case QueryPlan.Accelerated    => QueryPlanProto.ACCELERATED
      case QueryPlan.FullTextSearch => QueryPlanProto.FULLTEXTSEARCH
      case QueryPlan.Default        => QueryPlanProto.DEFAULT
      case QueryPlan.Sampling       => QueryPlanProto.SAMPLING
    }
    ExpressionQueryCommandProto.newBuilder
      .setQuery(toProto(command.query))
      .setPlan(plan)
      .setEntitledOnly(command.entitledOnly)
      .build
  }

  override def deserialize(proto: ExpressionQueryCommandProto): ExpressionQueryCommand = {
    val plan = proto.getPlan match {
      case QueryPlanProto.ACCELERATED    => QueryPlan.Accelerated
      case QueryPlanProto.FULLTEXTSEARCH => QueryPlan.FullTextSearch
      case QueryPlanProto.DEFAULT        => QueryPlan.Default
      case QueryPlanProto.SAMPLING       => QueryPlan.Sampling
    }
    ExpressionQueryCommand(fromProto(proto.getQuery), plan, proto.getEntitledOnly)
  }
}
