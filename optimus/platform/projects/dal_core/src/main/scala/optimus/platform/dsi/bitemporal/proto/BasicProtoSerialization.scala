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

import com.google.protobuf.ByteString
import msjava.slf4jutils.scalalog.getLogger
import net.iharder.Base64
import optimus.dsi.session.SlotMap
import optimus.entity.EntityLinkageProperty
import optimus.platform._
import optimus.platform.dal.config.DalAppId
import optimus.platform.dal.config.DalZoneId
import optimus.platform.dal.config.HostPort
import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform.storable._

import scala.jdk.CollectionConverters._

trait BasicProtoSerialization extends ProtoSerialization with ChainedIdSerialization {
  implicit def entityAuditInfoSerializer: EntityAuditInfoSerializer.type = EntityAuditInfoSerializer
  implicit def serializedAppEventSerializer: SerializedAppEventSerializer.type = SerializedAppEventSerializer
  implicit def serializedKeySerializer: SerializedKeySerializer.type = SerializedKeySerializer
  implicit def instantSerializer: InstantSerializer.type = InstantSerializer
  implicit def contextSerializer: ContextSerializer.type = ContextSerializer
  implicit def childToParentLinkageSerializer: ChildToParentLinkageSerializer.type = ChildToParentLinkageSerializer
  implicit def serializedEntitySerializer: SerializedEntitySerializer.type = SerializedEntitySerializer
  implicit def serializedBusinessEventSerializer: SerializedBusinessEventSerializer.type =
    SerializedBusinessEventSerializer
  implicit def serializedBusinessEventWithTTToSerializer: SerializedBusinessEventWithTTToSerializer.type =
    SerializedBusinessEventWithTTToSerializer
  implicit def persistentEntitySerializer: PersistentEntitySerializer.type = PersistentEntitySerializer
  implicit def validTimeIntervalSerializer: ValidTimeIntervalSerializer.type = ValidTimeIntervalSerializer
  implicit def versionedReferenceSerializer: VersionedReferenceSerializer.type = VersionedReferenceSerializer
  implicit def entityReferenceSerializer: EntityReferenceSerializer.type = EntityReferenceSerializer
  implicit def storableReferenceSerializer: StorableReferenceSerializer.type = StorableReferenceSerializer
  implicit def entityTimeSliceReferenceSerializer: EntityTimeSliceReferenceSerializer.type =
    EntityTimeSliceReferenceSerializer
  implicit def businessEventReferenceSerializer: BusinessEventReferenceSerializer.type =
    BusinessEventReferenceSerializer
  implicit def appEventReferenceSerializer: AppEventReferenceSerializer.type = AppEventReferenceSerializer
  implicit def cmReferenceSerializer: CmReferenceSerializer.type = CmReferenceSerializer
  implicit def timeIntervalSerializer: TimeIntervalSerializer.type = TimeIntervalSerializer
  implicit def persistentEntitiesWithTemporalContextImplSerializer
      : PersistentEntitiesWithTemporalContextImplSerializer.type = PersistentEntitiesWithTemporalContextImplSerializer
  implicit def clientAppIdentifierSerializer: ClientAppIdentifierSerializer.type = ClientAppIdentifierSerializer

  final def toProto(ctxt: Context): ContextProto = ContextSerializer.serialize(ctxt)
  final def fromProto(proto: ContextProto): Context = ContextSerializer.deserialize(proto)
}

object SerializedAppEventSerializer
    extends BasicProtoSerialization
    with ProtoSerializer[SerializedAppEvent, SerializedAppEventProto] {

  override def serialize(in: SerializedAppEvent): SerializedAppEventProto = ??? /* {
    val bld = SerializedAppEventProto.newBuilder
      .setEventRef(toProto(in.id))
      .setTxTime(toProto(in.tt))
    if (in.user != null) bld.setUser(in.user)
    if (in.reqHostPort != null) bld.setReqHostPort(in.reqHostPort.hostport)
    in.elevatedForUser.foreach(onBehalfUser => bld.setElevatedForUser(onBehalfUser))
    in.receivedAt.foreach(rcvdAt => bld.setReceivedAt(toProto(rcvdAt)))
    in.effectiveUser.foreach(bld.setEffectiveUser)
    bld
      .setApplication(in.application)
      .setContentOwner(in.contentOwner)
      .setReqId(in.reqId)
      .setAppId(in.appId.underlying)
      .setZoneId(in.zoneId.underlying)
      .build
  } */

  override def deserialize(proto: SerializedAppEventProto): SerializedAppEvent = ??? /* {
    val user = if (proto.hasUser) proto.getUser else null
    val hp = if (proto.hasReqHostPort) HostPort(proto.getReqHostPort) else HostPort.NoHostPort
    val appId = if (proto.hasAppId) DalAppId(proto.getAppId) else DalAppId.unknown
    val zoneId = if (proto.hasZoneId) DalZoneId(proto.getZoneId) else DalZoneId.unknown
    val elevatedForUser = if (proto.hasElevatedForUser) Some(proto.getElevatedForUser) else None
    val receivedAt = if (proto.hasReceivedAt) Some(fromProto(proto.getReceivedAt)) else None
    val effectiveUser = if (proto.hasEffectiveUser) Some(proto.getEffectiveUser) else None
    SerializedAppEvent(
      fromProto(proto.getEventRef()),
      fromProto(proto.getTxTime()),
      user,
      effectiveUser,
      proto.getApplication(),
      proto.getContentOwner(),
      proto.getReqId(),
      hp,
      None,
      None,
      appId,
      zoneId,
      elevatedForUser,
      receivedAt
    )
  } */
}

object SerializedKeySerializer extends BasicProtoSerialization with ProtoSerializer[SerializedKey, SerializedKeyProto] {

  override def serialize(key: SerializedKey): SerializedKeyProto = ??? /* {
    val propertiesProto = ProtoPickleSerializer.propertiesToProto(key.properties.toSeq)
    SerializedKeyProto.newBuilder
      .setTypeName(key.typeName)
      .setUnique(key.unique)
      .setIndexed(key.indexed)
      .setRefFilter(key.refFilter)
      .setProperties(propertiesProto)
      .build
  } */

  override def deserialize(proto: SerializedKeyProto): SerializedKey = ??? /* {
    val properties = ProtoPickleSerializer.protoToProperties(proto.getProperties).asInstanceOf[Seq[(String, Any)]]
    val serializedSizeOpt = Option(proto.getSerializedSize)
    SerializedKey(
      proto.getTypeName.intern(),
      properties,
      proto.getUnique,
      proto.getIndexed,
      proto.getRefFilter,
      serializedSizeOpt)
  } */
}

object InstantSerializer extends BasicProtoSerialization with ProtoSerializer[Instant, InstantProto] {

  // Small cache to minimize allocations
  val instantCache = new Array[Instant](32)
  // Mask is 1F
  final def ofEpochSecond(second: Long, nano: Int) = {
    val index = (second.toInt ^ nano) & 0x1f
    val cur = instantCache(index)
    if ((cur ne null) && cur.getEpochSecond() == second && cur.getNano() == nano)
      cur
    else {
      val newInstant = Instant.ofEpochSecond(second, nano)
      instantCache(index) = newInstant
      newInstant
    }
  }

  override def deserialize(proto: InstantProto): Instant = ??? /* {
    ofEpochSecond(proto.getSeconds, proto.getNano)
  } */

  override def serialize(instant: Instant): InstantProto = ??? /* {
    InstantProto.newBuilder
      .setNano(instant.getNano)
      .setSeconds(instant.getEpochSecond)
      .build
  } */
}

object ContextSerializer extends BasicProtoSerialization with ProtoSerializer[Context, ContextProto] {

  val log = getLogger(this)

  override def serialize(context: Context): ContextProto = ??? /* {
    val builder = ContextProto.newBuilder
    (context match {
      case NamedContext(name)  => builder.setName(name).setType(ContextProto.Type.NAMED)
      case SharedContext(name) => builder.setSharedName(name).setType(ContextProto.Type.SHARED)
      case UniqueContext(uuid) => builder.setUuid(uuid.toString).setType(ContextProto.Type.UNIQUE)
      case DefaultContext      => builder.setType(ContextProto.Type.DEFAULT)
      case _                   => throw new IllegalArgumentException("Invalid context")
    }).build
  } */

  override def deserialize(proto: ContextProto): Context = ??? /* {
    if (proto.hasSharedName()) {
      SharedContext(proto.getSharedName())
    } else if (proto.hasName()) {
      NamedContext(proto.getName())
    } else if (proto.hasUuid()) {
      UniqueContext(java.util.UUID.fromString(proto.getUuid))
    } else {
      DefaultContext
    }
  } */
}

object ChildToParentLinkageSerializer
    extends BasicProtoSerialization
    with ProtoSerializer[SerializedEntity.EntityLinkage, SinglePropertyLinkageProto] {

  override def serialize(linkage: SerializedEntity.EntityLinkage): SinglePropertyLinkageProto = ??? /* {
    SinglePropertyLinkageProto.newBuilder.setEntityReference((toProto(linkage.permRef))).build()
  } */

  override def deserialize(proto: SinglePropertyLinkageProto): SerializedEntity.EntityLinkage = ??? /* {
    SerializedEntity.EntityLinkage(fromProto(proto.getEntityReference))
  } */
}

object SerializedEntitySerializer
    extends BasicProtoSerialization
    with ProtoSerializer[SerializedEntity, SerializedEntityProto] {

  override def serialize(entity: SerializedEntity): SerializedEntityProto = ??? /* {
    val propertiesProto = ProtoPickleSerializer.propertiesToProto(entity.properties)
    val builder = SerializedEntityProto.newBuilder
      .setProperties(propertiesProto)
      .setClassName(entity.className)
      .addAllKeys(entity.keys map { toProto(_) } asJava)
      .addAllTypes(entity.types.asJava)
      .addAllInlinedEntitites(entity.inlinedEntities map { serialize _ } asJava)
      .setSlot(entity.slot)

    if (entity.entityRef != null) {
      builder.setEntityRef(toProto(entity.entityRef))
      builder.setEntityRefString(Base64.encodeBytes(entity.entityRef.data))
    }
    if (entity.linkages.isDefined) {
      val linkageProtos = entity.linkages.get map { case (linkageField, links) =>
        val bld = PropertyLinkagesProto.newBuilder
          .setFieldName(linkageField.propertyName)
          .addAllLinkages(links map { case l: SerializedEntity.EntityLinkage => toProto(l) } asJava)
        if (linkageField.typeName != null) bld.setTypeName(linkageField.typeName)
        bld.build
      }
      builder.addAllLinkages(linkageProtos.toSeq.asJava)
      builder.setHasLinkages(true)
    } else builder.setHasLinkages(false)
    entity.cmid.foreach(ref => builder.setCmRef(toProto(ref)))
    builder.build
  } */

  override def deserialize(proto: SerializedEntityProto): SerializedEntity = ??? /* {
    val properties = ProtoPickleSerializer.protoToProperties(proto.getProperties).asInstanceOf[Map[String, Any]]
    val keyIterator = proto.getKeysList.asScala.iterator map { fromProto(_) }
    val keys = ProtoSerialization
      .distinctBy(keyIterator, (s: SerializedKey) => (s.unique, s.indexed, s.refFilter, s))
      .toIndexedSeq
    val ref = if (proto.hasEntityRef) fromProto(proto.getEntityRef) else null
    val inlinedEntites = proto.getInlinedEntititesList.asScala map { deserialize }
    val linkages: Option[SerializedEntity.LinkageMap] =
      if (proto.getHasLinkages)
        Some(proto.getLinkagesList.asScala.iterator.map { l =>
          val propName = l.getFieldName
          val typeName = if (l.hasTypeName) l.getTypeName else null
          val links = l.getLinkagesList.asScala map { fromProto(_) }
          (EntityLinkageProperty(propName, typeName), links.toSet)
        }.toMap)
      else None
    val cmid = if (proto.hasCmRef) Some(fromProto(proto.getCmRef)) else None
    val slot = if (proto.hasSlot) proto.getSlot else 0
    val types = proto.getTypesList.asScala.map(_.intern())
    SerializedEntity(
      ref,
      cmid,
      proto.getClassName.intern(),
      properties,
      keys,
      types,
      inlinedEntites.toSeq,
      linkages = linkages,
      slot = slot)
  } */
}

object SerializedBusinessEventWithTTToSerializer
    extends BasicProtoSerialization
    with ProtoSerializer[SerializedBusinessEventWithTTTo, SerializedBusinessEventWithTTToProto] {

  override def deserialize(proto: SerializedBusinessEventWithTTToProto): SerializedBusinessEventWithTTTo = ??? /* {
    SerializedBusinessEventWithTTTo(fromProto(proto.getBusinessEvent()), fromProto(proto.getTxTime()))
  } */

  override def serialize(event: SerializedBusinessEventWithTTTo): SerializedBusinessEventWithTTToProto = ??? /* {
    SerializedBusinessEventWithTTToProto.newBuilder
      .setBusinessEvent(toProto(event.sbe))
      .setTxTime(toProto(event.ttTo))
      .build

  } */
}

object PersistentEntitySerializer
    extends BasicProtoSerialization
    with ProtoSerializer[PersistentEntity, PersistentEntityProto] {

  override def serialize(entity: PersistentEntity): PersistentEntityProto = ??? /* {
    PersistentEntityProto.newBuilder
      .setSerializedEntity(toProto(entity.serialized))
      .setVersionedReference(toProto(entity.versionedRef))
      .setValidTimeInternal(toProto(entity.vtInterval))
      .setTimeInterval(toProto(entity.txInterval))
      .setLockToken(entity.lockToken)
      .build()
  } */

  override def deserialize(proto: PersistentEntityProto): PersistentEntity = ??? /* {
    fromProto(proto.getSerializedEntity).toPersistentEntity(
      fromProto(proto.getVersionedReference),
      proto.getLockToken,
      fromProto(proto.getValidTimeInternal),
      fromProto(proto.getTimeInterval),
      None)
  } */
}

object ValidTimeIntervalSerializer
    extends BasicProtoSerialization
    with ProtoSerializer[ValidTimeInterval, ValidTimeIntervalProto] {

  override def serialize(vti: ValidTimeInterval): ValidTimeIntervalProto = ??? /* {
    ValidTimeIntervalProto.newBuilder
      .setFrom(toProto(vti.from))
      .setTo(toProto(vti.to))
      .setFixed(false)
      .build
  } */

  override def deserialize(proto: ValidTimeIntervalProto): ValidTimeInterval = ??? /* {
    new ValidTimeInterval(fromProto(proto.getFrom), fromProto(proto.getTo))
  } */
}

object VersionedReferenceSerializer
    extends BasicProtoSerialization
    with ProtoSerializer[VersionedReference, VersionedReferenceProto] {

  override def serialize(ref: VersionedReference): VersionedReferenceProto = ??? /* {
    VersionedReferenceProto.newBuilder
      .setData(ByteString.copyFrom(ref.data))
      .build
  } */

  override def deserialize(proto: VersionedReferenceProto): VersionedReference = ??? /* {
    new VersionedReference(proto.getData.toByteArray)
  } */
}

object StorableReferenceSerializer
    extends BasicProtoSerialization
    with ProtoSerializer[StorableReference, StorableReferenceProto] {

  override def serialize(ref: StorableReference): StorableReferenceProto = ??? /* {
    StorableReferenceProto.newBuilder
      .setData(ByteString.copyFrom(ref.data))
      .build
  } */

  override def deserialize(proto: StorableReferenceProto): StorableReference = ??? /* {
    val bytes = proto.getData.toByteArray
    new StorableReference(bytes)
  } */
}

object EntityTimeSliceReferenceSerializer
    extends BasicProtoSerialization
    with ProtoSerializer[EntityTimeSliceReference, EntityTimeSliceReferenceProto] {

  override def serialize(ts: EntityTimeSliceReference): EntityTimeSliceReferenceProto = ??? /* {
    EntityTimeSliceReferenceProto.newBuilder
      .setEntityRef(toProto(ts.entityRef))
      .setTimeSliceNumber(ts.timeSliceNumber)
      .setIsInfiniteTxtoAtCreation(ts.isInfiniteTxtoAtCreation)
      .build
  } */

  override def deserialize(proto: EntityTimeSliceReferenceProto): EntityTimeSliceReference = ??? /* {
    new EntityTimeSliceReference(
      EntityReferenceSerializer.deserialize(proto.getEntityRef),
      proto.getTimeSliceNumber,
      proto.getIsInfiniteTxtoAtCreation)
  } */
}

object EntityReferenceSerializer
    extends BasicProtoSerialization
    with ProtoSerializer[EntityReference, EntityReferenceProto] {

  override def serialize(ref: EntityReference): EntityReferenceProto = ??? /* {
    val builder = EntityReferenceProto.newBuilder
      .setData(ByteString.copyFrom(ref.data))
      .setTemporary(ref.isTemporary)

    ref.getTypeId foreach { typeId =>
      builder.setTypeId(typeId)
    }

    builder.build
  } */

  override def deserialize(proto: EntityReferenceProto): EntityReference = ??? /* {
    val bytes = proto.getData.toByteArray
    if (proto.hasTemporary && proto.getTemporary) EntityReference.temporary(bytes)
    else if (proto.hasTypeId) EntityReference.finalTypedRef(bytes, proto.getTypeId)
    else EntityReference.finalRef(bytes)
  } */
}

object BusinessEventReferenceSerializer
    extends BasicProtoSerialization
    with ProtoSerializer[BusinessEventReference, BusinessEventReferenceProto] {

  override def serialize(ref: BusinessEventReference): BusinessEventReferenceProto = ??? /* {
    val builder = BusinessEventReferenceProto.newBuilder
      .setData(ByteString.copyFrom(ref.data))

    ref.getTypeId foreach { typeId =>
      builder.setTypeId(typeId)
    }

    builder.build
  } */

  override def deserialize(proto: BusinessEventReferenceProto): BusinessEventReference = ??? /* {
    val bytes = proto.getData.toByteArray
    if (proto.hasTypeId) BusinessEventReference.typedRef(bytes, proto.getTypeId)
    else new BusinessEventReference(bytes)
  } */
}

object AppEventReferenceSerializer
    extends BasicProtoSerialization
    with ProtoSerializer[AppEventReference, AppEventReferenceProto] {

  override def serialize(ref: AppEventReference): AppEventReferenceProto = ??? /* {
    AppEventReferenceProto.newBuilder
      .setData(ByteString.copyFrom(ref.data))
      .build
  } */

  override def deserialize(proto: AppEventReferenceProto): AppEventReference = ??? /* {
    new AppEventReference(proto.getData.toByteArray)
  } */
}

object CmReferenceSerializer extends BasicProtoSerialization with ProtoSerializer[CmReference, CmReferenceProto] {

  override def serialize(ref: CmReference): CmReferenceProto = ??? /* {
    CmReferenceProto.newBuilder
      .setData(ByteString.copyFrom(ref.data))
      .build
  } */

  override def deserialize(proto: CmReferenceProto): CmReference = ??? /* {
    val bytes = proto.getData.toByteArray
    CmReference(bytes)
  } */
}

object TimeIntervalSerializer extends BasicProtoSerialization with ProtoSerializer[TimeInterval, TimeIntervalProto] {

  override def serialize(ti: TimeInterval): TimeIntervalProto = ??? /* {
    TimeIntervalProto.newBuilder
      .setFrom(toProto(ti.from))
      .setTo(toProto(ti.to))
      .build()
  } */

  override def deserialize(proto: TimeIntervalProto): TimeInterval = ??? /* {
    new TimeInterval(fromProto(proto.getFrom), fromProto(proto.getTo))
  } */
}

object PersistentEntitiesWithTemporalContextImplSerializer
    extends BasicProtoSerialization
    with ProtoSerializer[PersistentEntitiesWithTemporalContextImpl, PersistentEntityWithTemporalContextProto] {

  // PersistentEntityWithTemporalContextProto protocol buffers are only used as a work-around for specific performance
  // issues and to send entities via GPB from one client side application to another; they are *not* used in the optimus
  // code for client-server communication!
  override def deserialize(
      proto: PersistentEntityWithTemporalContextProto): PersistentEntitiesWithTemporalContextImpl = ??? /* {
    val entities = proto.getPersistentEntitiesList.asScala.iterator.map { fromProto(_) }.toIndexedSeq
    val cascaded = proto.getCascadedEntitiesList.asScala.iterator.map { fromProto(_) }.toIndexedSeq
    val validTime = fromProto(proto.getValidTime)
    val txTime = fromProto(proto.getTxTime)
    val clsName = proto.getEntityClassName
    new PersistentEntitiesWithTemporalContextImpl(entities, validTime, txTime, cascaded, Some(clsName))
  } */

  override def serialize(data: PersistentEntitiesWithTemporalContextImpl): PersistentEntityWithTemporalContextProto = ??? /* {
    val entitiesProto = data.entities.iterator.map { toProto(_) }.toIndexedSeq
    val cascadedProto = data.cascadedEntities.iterator.map { toProto(_) }.toIndexedSeq
    PersistentEntityWithTemporalContextProto.newBuilder
      .addAllPersistentEntities(entitiesProto.asJava)
      .setValidTime(toProto(data.validTime))
      .setTxTime(toProto(data.txTime))
      .addAllCascadedEntities(cascadedProto.asJava)
      .setEntityClassName(data.className)
      .build
  } */
}

object SerializedBusinessEventSerializer
    extends BasicProtoSerialization
    with ProtoSerializer[SerializedBusinessEvent, SerializedBusinessEventProto] {

  override def serialize(event: SerializedBusinessEvent): SerializedBusinessEventProto = ??? /* {
    val propertiesProto = ProtoPickleSerializer.propertiesToProto(event.properties)
    val builder = SerializedBusinessEventProto.newBuilder
      .setEventRef(toProto(event.id))
      .setClassName(event.className)
      .setProperties(propertiesProto)
      .addAllKeys(event.keys map { toProto(_) } asJava)
      .addAllTypes(event.types.asJava)
      .setValidTime(toProto(event.validTime))
      .setVersionId(event.versionId)
      .setLockToken(event.lockToken)
      .setIsCancelled(event.isCancel)
      .setSlot(event.slot)
    event.vrefOpt.foreach(vref => builder.setVersionedReference(toProto(vref)))
    event.cmid.foreach(ref => builder.setCmRef(toProto(ref)))

    if (event.appEventId != null) builder.setAppeventRef(toProto(event.appEventId))
    if (event.tt != null) builder.setTxTime(toProto(event.tt))

    builder.build
  } */

  override def deserialize(proto: SerializedBusinessEventProto): SerializedBusinessEvent = ??? /* {
    val properties = ProtoPickleSerializer.protoToProperties(proto.getProperties).asInstanceOf[Map[String, Any]]
    val keys = proto.getKeysList.asScala map { fromProto(_) }
    val types = proto.getTypesList.asScala
    val appEventRef = if (proto.hasAppeventRef()) fromProto(proto.getAppeventRef()) else null
    val txTime = if (proto.hasTxTime()) fromProto(proto.getTxTime()) else null
    val cancelled = if (proto.hasIsCancelled()) proto.getIsCancelled() else false
    val slot = if (proto.hasSlot) proto.getSlot else 0
    val cmid = if (proto.hasCmRef) Some(fromProto(proto.getCmRef)) else None
    val vref =
      if (proto.hasVersionedReference)
        Some(VersionedReferenceSerializer.deserialize(proto.getVersionedReference))
      else None
    SerializedBusinessEvent(
      id = fromProto(proto.getEventRef()),
      cmid = cmid,
      className = proto.getClassName(),
      properties = properties,
      keys = keys.toSeq,
      types = types.toSeq,
      validTime = fromProto(proto.getValidTime()),
      versionId = proto.getVersionId().intValue,
      appEventId = appEventRef,
      tt = txTime,
      lockToken = proto.getLockToken,
      isCancel = cancelled,
      slot = slot,
      vrefOpt = vref
    )
  } */
}

object SlotMapSerializer extends BasicProtoSerialization with ProtoSerializer[SlotMap, SlotMapProto] {
  override def serialize(sm: SlotMap): SlotMapProto = ??? /* {
    val builder = SlotMapProto.newBuilder
    val (classNames, slots) = sm.map.unzip
    builder.addAllFqcns(classNames.asJava)
    builder.addAllSlots(slots.map(Integer.valueOf(_)).asJava)
    builder.build
  } */

  override def deserialize(proto: SlotMapProto): SlotMap = ??? /* {
    val classNames: Iterable[SerializedEntity.TypeRef] = proto.getFqcnsList.asScala
    val slots: Iterable[Int] = proto.getSlotsList.asScala.map(_.toInt)
    val map: Map[SerializedEntity.TypeRef, Int] = (classNames.iterator zip slots.iterator).toMap
    SlotMap(map)
  } */
}

object ClientAppIdentifierSerializer
    extends GetCommandProtoSerialization
    with ProtoSerializer[ClientAppIdentifier, ClientAppIdentifierProto] {

  override def deserialize(proto: ClientAppIdentifierProto): ClientAppIdentifier = ??? /* {
    ClientAppIdentifier(DalZoneId(proto.getZoneId()), DalAppId(proto.getAppId()))
  } */

  override def serialize(appIdentifier: ClientAppIdentifier): ClientAppIdentifierProto = ??? /* {
    ClientAppIdentifierProto.newBuilder
      .setZoneId(appIdentifier.zoneId.underlying)
      .setAppId(appIdentifier.appId.underlying)
      .build()
  } */
}
