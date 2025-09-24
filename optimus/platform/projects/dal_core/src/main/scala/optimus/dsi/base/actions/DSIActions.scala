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
package optimus.dsi.base.actions

import msjava.slf4jutils.scalalog.getLogger
import optimus.dal.storable.DBRawEffect
import optimus.dal.storable.ObliterateEffect
import optimus.dsi.base.ObliterateAction
import optimus.dsi.base.RefHolder
import optimus.dsi.base.SlottedVersionedReference
import optimus.graph.DiagnosticSettings
import optimus.platform._
import optimus.platform.bitemporal.BitemporalSpace
import optimus.platform.bitemporal.Rectangle
import optimus.platform.bitemporal.Segment
import optimus.platform.bitemporal.ValidSegment
import optimus.platform.dal.config.DalAppId
import optimus.platform.dal.config.DalZoneId
import optimus.platform.dal.config.HostPort
import optimus.platform.dsi.bitemporal
import optimus.platform.dsi.bitemporal._
import optimus.platform.storable._

import java.time.Instant
import java.util.Arrays
import java.util.Objects
import scala.collection.immutable

sealed trait DSIAction

// TODO (OPTIMUS-11412): do RCA for this JIRA, should be removed once resolved
object EmptyKeySpaceActionsDebugging {
  private lazy val emptyKeySpaceActionsDebugging =
    DiagnosticSettings.getBoolProperty("optimus.dsi.emptyKeySpaceActionsDebugging", false)
  private lazy val log = getLogger[TxnAction]
  def validatePutTemporalKey(key: PutTemporalKey, action: String): Unit = {
    if (key.s.all.isEmpty) {
      val msg = s"Space is empty for action:${action} PutTemporalKey:${key}"
      log.warn(msg)
      if (emptyKeySpaceActionsDebugging) throw new DSISpecificError(msg)
    }
  }
}

sealed trait TxnAction extends DSIAction

//These actions should only be written to worklog and replicate through b2b replication
//they should not have any impact on the data stored in Mongo
sealed trait WorklogOnlyTxnAction extends TxnAction {
  val className: String
}

//We generate this action when business event is loaded from DAl and reused to write
//a new transaction. This is used by Uow matching logic to match against business event types
final case class ReuseBusinessEvent(eventRef: TypedBusinessEventReference, className: String, types: Seq[String])
    extends WorklogOnlyTxnAction

//We generate this action when business event is loaded from DAl and reused to write
//a new transaction. This is used by Partitioned Keyed UowKafka matching logic to
//extract partition keys from business event payload if configured as such.
final case class ReuseBusinessEventBlobAction(
    eventRef: TypedBusinessEventReference,
    className: String,
    serializedBusinessEvent: Option[SerializedBusinessEvent])
    extends WorklogOnlyTxnAction {
  override def equals(obj: Any): Boolean = {
    obj match {
      case rbeba: ReuseBusinessEventBlobAction =>
        rbeba.eventRef == eventRef && rbeba.className == className
      case _ => false
    }
  }
  override def hashCode(): Int = Objects.hash(eventRef, className)
  override def toString: String = s"ReuseBusinessEventBlobAction($eventRef, $className)"
}
//We generate this action when an entity is reverted to an existing version using DAL.revert.
//This is used by Partitioned Keyed UowKafka matching logic to extract partition keys from
//entity payload. This is also used to perform matching in UowKafka matcher if a priql expression
//is defined as matching rule.
final case class RevertEntityBlobAction(
    entityRef: FinalTypedReference,
    vref: VersionedReference,
    className: String,
    serializedEntity: Option[SerializedEntity])
    extends WorklogOnlyTxnAction {
  override def equals(obj: Any): Boolean = {
    obj match {
      case reba: RevertEntityBlobAction =>
        reba.entityRef == entityRef && reba.vref == vref && reba.className == className
    }
  }
  override def hashCode(): Int = Objects.hash(entityRef, vref, className)
  override def toString: String = s"RevertEntityBlobAction($entityRef, $vref, $className)"
}

// accInfoOpt is filled by Acc replica when AccAwareWriter collectActions if it is Create or Refresh action
final case class AccAction(
    clsName: String,
    slot: Int,
    action: AccMetadataOp,
    accTtOpt: Option[Instant],
    accInfoOpt: Option[SerializedEntity],
    forceCreateIndex: Boolean)
    extends TxnAction

final case class PutTemporalKey(
    key: SerializedKey,
    id: RefHolder,
    s: BitemporalSpace[EntityReference],
    classIdOpt: Option[Int])
    extends TxnAction {
  EmptyKeySpaceActionsDebugging.validatePutTemporalKey(this, "PutTemporalKey")
  def createKeyGrouping(tt: Instant): KeyGrouping = {
    createKeyGrouping(DateTimeSerialization.fromInstant(tt))
  }
  def createKeyGrouping(token: Long): KeyGrouping = {
    KeyGrouping(key, id, s, token, classIdOpt)
  }
}

/*
 * Grouping Actions
 */
sealed trait GroupingTxnAction[T <: StorableReference] extends TxnAction {
  def id: T
  def className: String
  def types: Seq[String]
  def maxTimeSliceCount: Int
}

final case class LinkedTypes(
    keyDefinerType: Option[String],
    indexDefinerTypes: Set[String],
    uniqueIndexDefinerTypes: Set[String],
    linkageDefinerTypes: Set[String]) {
  def merge(other: LinkedTypes): LinkedTypes = {
    LinkedTypes(
      keyDefinerType.orElse(other.keyDefinerType),
      indexDefinerTypes ++ other.indexDefinerTypes,
      uniqueIndexDefinerTypes ++ other.uniqueIndexDefinerTypes,
      linkageDefinerTypes ++ other.linkageDefinerTypes
    )
  }
}

/*
 * Entity Groupings
 */
sealed trait EntityGroupingTxnAction extends GroupingTxnAction[FinalTypedReference] {
  def entityRef: FinalTypedReference
  def maxVersionCount: Int
  def id: FinalTypedReference = entityRef
  def className: String
  def types: Seq[String]
  def cmid: Option[CmReference]
  def linkedTypes: Option[LinkedTypes]
}

final case class PutEntityGrouping(
    entityRef: FinalTypedReference,
    cmid: Option[CmReference],
    maxTimeSliceCount: Int,
    maxVersionCount: Int,
    className: String,
    types: Seq[SerializedEntity.TypeRef],
    linkedTypes: Option[LinkedTypes])
    extends EntityGroupingTxnAction {

  def createEntityGrouping(tt: Instant): EntityGrouping =
    EntityGrouping(
      entityRef,
      cmid,
      className,
      types,
      maxTimeSliceCount,
      maxVersionCount,
      DateTimeSerialization.fromInstant(tt),
      linkedTypes)
}

final case class UpdateEntityGrouping(
    entityRef: FinalTypedReference,
    existingLockToken: Long,
    maxTimeSliceCount: Int,
    maxVersionCount: Int,
    className: String,
    types: Seq[SerializedEntity.TypeRef],
    typesUpdated: Boolean,
    cmid: Option[CmReference],
    linkedTypes: Option[LinkedTypes],
    linkedTypesUpdated: Boolean)
    extends EntityGroupingTxnAction {}

/*
 * ClassInfo
 */
sealed trait ClassInfoEntryTxnAction extends TxnAction {
  def classInfoEntry: ClassInfoEntry
  def className: String = classInfoEntry.className
  def types: Seq[SerializedEntity.TypeRef] = classInfoEntry.types
  def linkedTypes: Option[LinkedTypes] = classInfoEntry.linkedTypes
  def monoTemporal: Boolean = classInfoEntry.monoTemporal
}

final case class PutClassInfoEntry(classInfoEntry: ClassInfoEntry) extends ClassInfoEntryTxnAction

final case class UpdateClassInfoEntry(
    classInfoEntry: ClassInfoEntry,
    linkedTypesUpdated: Boolean,
    typesUpdated: Boolean)
    extends ClassInfoEntryTxnAction

/*
 * Entity Unique Index Groupings
 */

sealed trait UniqueIndexGroupingTxnAction extends GroupingTxnAction[RefHolder]

final case class PutUniqueIndexGrouping(id: RefHolder, maxTimeSliceCount: Int, key: SerializedKey)
    extends UniqueIndexGroupingTxnAction {

  def className: String = key.typeName
  def types: Seq[String] = className :: Nil

  def createUniqueIndexGrouping(tt: Instant): UniqueIndexGrouping =
    UniqueIndexGrouping(key, id, maxTimeSliceCount, DateTimeSerialization.fromInstant(tt))

}

final case class UpdateUniqueIndexGrouping(
    id: RefHolder,
    existingLockToken: Long,
    maxTimeSliceCount: Int,
    typeName: String,
    typeUpdated: Boolean)
    extends UniqueIndexGroupingTxnAction {

  def className: String = typeName
  def types: Seq[String] = className :: Nil

}

/*
 * Event Groupings
 */

sealed trait BusinessEventGroupingTxnAction extends GroupingTxnAction[TypedBusinessEventReference] {
  def eventRef: TypedBusinessEventReference
  def className: String
  def types: Seq[String]

  def id: TypedBusinessEventReference = eventRef
}

final case class PutBusinessEventGrouping(
    eventRef: TypedBusinessEventReference,
    cmid: Option[CmReference],
    maxTimeSliceCount: Int,
    className: String,
    types: Seq[String])
    extends BusinessEventGroupingTxnAction {

  /*
   * Changed back the locktoken value from DateTimeSerialization.fromInstant(tt) back to 1, as the old business event collection cannot
   * move to tt based locktoken.
   */
  def createBusinessEventGrouping(lockToken: Long)(tt: Instant): BusinessEventGrouping =
    BusinessEventGrouping(eventRef, cmid, className, types, maxTimeSliceCount, lockToken)

}

final case class UpdateBusinessEventGrouping(
    eventRef: TypedBusinessEventReference,
    existingLockToken: Long,
    maxTimeSliceCount: Int,
    className: String,
    types: Seq[String],
    typesUpdated: Boolean,
    cmid: Option[CmReference])
    extends BusinessEventGroupingTxnAction

object PutCmReference {
  private final case class PutCmReferenceImpl(cmid: CmReference, stableRef: StorableReference, isEntity: Boolean)
      extends PutCmReference {
    lazy val toCmReferenceEntry = CmReferenceEntry(cmid, stableRef)
  }
  def apply(cmid: CmReference, ref: FinalTypedReference, isEntity: Boolean): PutCmReference =
    PutCmReferenceImpl(cmid, ref, isEntity)
  def apply(cmid: CmReference, bref: TypedBusinessEventReference, isEntity: Boolean): PutCmReference =
    PutCmReferenceImpl(cmid, bref, isEntity)
}
trait PutCmReference extends TxnAction {
  def cmid: CmReference
  def stableRef: StorableReference
  def isEntity: Boolean
  def toCmReferenceEntry: CmReferenceEntry
}

/*
 * TimeSliceActions
 */
sealed trait TimeSliceTxnAction[T <: StorableReference, R <: RawReference] extends TxnAction {
  def groupingRef: T
  def timeSliceNumber: Int
  def targetRef: R

  def id: (T, Int) = (groupingRef, timeSliceNumber)
}
sealed trait HasVersionedRef {
  def versionedRef: VersionedReference
}

/*
 * Entity TimeSlices
 */
sealed trait EntityTimeSliceTxnAction
    extends TimeSliceTxnAction[FinalTypedReference, VersionedReference]
    with HasVersionedRef {
  def entityRef: FinalTypedReference
  // TODO (OPTIMUS-13289): Remove. This is needed by DSITxnWorkManager.getReplicatePutApplicationEvent to correlate effects to actions.
  // Once getTheGap is just reading out the saved translog this can be removed.
  def vtInterval: ValidTimeInterval
  def versionedRef: VersionedReference
  def updatedAppEvent: AppEventReference

  def groupingRef: FinalTypedReference = entityRef
  def targetRef: VersionedReference = versionedRef
}

/**
 * PutEntityTimeSlice is used for putting a new rectangle in the Bitemporal space for an entityRef This can happen in 2
 * cases:
 *   1. Bitemporal space for that entityRef is empty and this will be the first rectangle 2. Bitemporal space for that
 *      entityRef is not empty, then the PutEntityTimeSlice will create a new rectangle In this case, the existing
 *      rectangle(s) may be closed with close CloseEntityTimeSlice
 *
 * @param entityRef
 * @param timeSliceNumber
 * @param updatedAppEvent
 * @param seg
 * @param ttInterval
 *   // TODO (OPTIMUS-14903): remove ttInterval
 */
final case class PutEntityTimeSlice(
    entityRef: FinalTypedReference,
    timeSliceNumber: Int,
    updatedAppEvent: AppEventReference,
    seg: ValidSegment[TimeSliceData],
    ttInterval: Option[TimeInterval] = None)
    extends EntityTimeSliceTxnAction {

  def vtInterval: ValidTimeInterval = seg.vtInterval
  def versionedRef: VersionedReference = seg.data.vref

  def createTimeSlice(tt: Instant): EntityTimeSlice =
    EntityTimeSlice(
      entityRef,
      updatedAppEvent,
      new Rectangle(seg.data, ttInterval.getOrElse(TimeInterval(tt)), seg.vtInterval, timeSliceNumber))
}

object CloseEntityTimeSlice {
  // New code should use the next method instead. This method can be removed when all work log entries has 'txFrom'
  // field. After this method gets removed, we may assume 'txFromOption.isDefined' always being true.
  def apply(
      entityRef: FinalTypedReference,
      timeSliceNumber: Int,
      updatedAppEvent: AppEventReference,
      versionedRef: VersionedReference,
      vtInterval: ValidTimeInterval): CloseEntityTimeSlice =
    new CloseEntityTimeSlice(entityRef, timeSliceNumber, updatedAppEvent, versionedRef, vtInterval, None)

  def apply(
      entityRef: FinalTypedReference,
      timeSliceNumber: Int,
      updatedAppEvent: AppEventReference,
      versionedRef: VersionedReference,
      vtInterval: ValidTimeInterval,
      txFrom: Instant): CloseEntityTimeSlice =
    new CloseEntityTimeSlice(entityRef, timeSliceNumber, updatedAppEvent, versionedRef, vtInterval, Some(txFrom))
}

/**
 * CloseEntityTimeSlice is used when you're putting a new rectangle in the Bitemporal space in which another rectangle
 * already exists. The tt or vt of the existing rectangle will be closed by setting the existing rectangle's vtTo or
 * ttTo to the new rectangle's vtFrom or ttFrom
 *
 * @param entityRef
 * @param timeSliceNumber
 * @param updatedAppEvent
 * @param versionedRef
 * @param vtInterval
 */
final case class CloseEntityTimeSlice private (
    override val entityRef: FinalTypedReference,
    override val timeSliceNumber: Int,
    override val updatedAppEvent: AppEventReference,
    override val versionedRef: VersionedReference,
    override val vtInterval: ValidTimeInterval,
    txFromOption: Option[Instant])
    extends EntityTimeSliceTxnAction

/*
 * Unique Index TimeSlices
 */
sealed trait UniqueIndexTimeSliceTxnAction extends TimeSliceTxnAction[RefHolder, FinalTypedReference] {
  def hash: RefHolder
  def entityRef: FinalTypedReference

  def groupingRef: RefHolder = hash
  def targetRef: FinalTypedReference = entityRef

  // TODO (OPTIMUS-36520): Change the txn actions to use concrete type rather than Option for fields added after initial implementation.
  def typeNameOpt: Option[String]
  require(typeNameOpt.forall(_.nonEmpty))
}

final case class PutUniqueIndexTimeSlice(
    hash: RefHolder,
    timeSliceNumber: Int,
    entityRef: FinalTypedReference,
    vtInterval: ValidTimeInterval,
    properties: Seq[(String, Any)],
    typeNameOpt: Option[String])
    extends UniqueIndexTimeSliceTxnAction {

  def createUniqueIndexTimeSlice(tt: Instant): UniqueIndexTimeSlice =
    UniqueIndexTimeSlice(
      hash,
      new Rectangle(entityRef, TimeInterval(tt), vtInterval, timeSliceNumber),
      Some(entityRef.typeId))
}

// The properties seq will hold the (id, value) tuple for the unique index which is being closed by the txnAction.
// This sequence is used to create a minimal SE in the NotificationEntry transform method.
final case class CloseUniqueIndexTimeSlice(
    hash: RefHolder,
    timeSliceNumber: Int,
    entityRef: FinalTypedReference,
    properties: Seq[(String, Any)],
    typeNameOpt: Option[String])
    extends UniqueIndexTimeSliceTxnAction

/*
 * Business Event TimeSlices
 */

sealed trait BusinessEventTimeSliceTxnAction
    extends TimeSliceTxnAction[TypedBusinessEventReference, VersionedReference]
    with HasVersionedRef {
  def eventRef: TypedBusinessEventReference
  def versionedRef: VersionedReference

  def groupingRef: TypedBusinessEventReference = eventRef
  def targetRef: VersionedReference = versionedRef
}

final case class PutBusinessEventTimeSlice(
    eventRef: TypedBusinessEventReference,
    timeSliceNumber: Int,
    appEventId: AppEventReference,
    validTime: Instant,
    versionedRef: VersionedReference,
    isCancel: Boolean)
    extends BusinessEventTimeSliceTxnAction {

  def createBusinessEventTimeSlice(tt: Instant): BusinessEventTimeSlice =
    BusinessEventTimeSlice(
      eventRef,
      appEventId,
      timeSliceNumber,
      validTime,
      Segment(versionedRef, TimeInterval(tt)),
      isCancel)
}

final case class CloseBusinessEventTimeSlice(
    eventRef: TypedBusinessEventReference,
    timeSliceNumber: Int,
    updatedAppEvent: AppEventReference,
    versionedRef: VersionedReference)
    extends BusinessEventTimeSliceTxnAction

// Following are new put/close txn actions for new index format, where we have collection/table per type in the
// backend and there would be a single doc/row per entity version.

final case class PutRegisteredIndexEntry(
    id: AnyRef,
    versionedRef: VersionedReference,
    entityRef: FinalTypedReference,
    typeName: String,
    properties: Seq[(String, Any)],
    vtInterval: ValidTimeInterval)
    extends TxnAction {
  def createRegisteredIndexEntry(tt: Instant): RegisteredIndexEntry =
    RegisteredIndexEntry(id, versionedRef, entityRef, typeName, properties, vtInterval, TimeInterval(tt))
}

final case class CloseRegisteredIndexEntry(
    id: AnyRef,
    properties: Seq[(String, Any)],
    vref: VersionedReference,
    typeName: String,
    erefOpt: Option[FinalTypedReference])
    extends TxnAction {
  require(id ne null)
  require(!id.isInstanceOf[RegisteredIndexEntry])
}

final case class PutIndexEntry(
    id: AnyRef,
    versionedRef: VersionedReference,
    entityRef: FinalTypedReference,
    typeName: String,
    properties: Seq[(String, Any)],
    vtInterval: ValidTimeInterval,
    hash: Array[Byte],
    refFilter: Boolean)
    extends TxnAction {

  override def equals(o: Any): Boolean = o match {
    case entry: PutIndexEntry =>
      entry.id == id && entry.versionedRef == versionedRef && entry.typeName == typeName && entry.vtInterval == vtInterval && Arrays
        .equals(entry.hash, hash) && entry.refFilter == refFilter && entry.properties == properties
    case _ => false
  }

  override def hashCode: Int = if (id != null) id.hashCode() else vtInterval.from.getNano

  def createIndexEntry(tt: Instant): IndexEntry =
    IndexEntry(id, versionedRef, entityRef, typeName, properties, vtInterval, TimeInterval(tt), hash, refFilter)
}

// The properties seq will hold the (id, value) tuple for the index which is being closed by the txnAction.
// This sequence is used to create a minimal SE in the NotificationEntry transform method.
// The vrefOpt is required to associate the index to a particular entity version. It will be defined if properties is nonEmpty.
// TODO (OPTIMUS-36520): Remove Option from typeNameOpt as
// the field is always populated..
final case class CloseIndexEntry(
    id: AnyRef,
    refFilter: Boolean,
    properties: Seq[(String, Any)],
    vrefOpt: Option[VersionedReference],
    typeNameOpt: Option[String],
    erefOpt: Option[FinalTypedReference])
    extends TxnAction {
  require(id ne null)
  require(!id.isInstanceOf[IndexEntry])
}

/**
 * PutLinkageEntry action occurs when we form a link from an entity to the referenced parent entity. Linkages tells us
 * about references to other entities - the annotation {@code @stored(childToParent=true) val foo} means forming a link
 * from this entity to the referenced parent entity in val foo
 *
 * @param id
 * @param parentRef
 * @param childRef
 * @param parentPropertyName
 * @param parentType
 *   \- the type to which the linkage field belongs to, which could some base class
 * @param parentRefClassId
 *   \- the type id of concrete class of parent eref
 * @param childType
 *   // TODO (OPTIMUS-36520): Remove Option from childType as the field is always populated
 * @param vtInterval
 */
final case class PutLinkageEntry(
    id: LinkageReference,
    parentRef: EntityReference,
    childRef: EntityReference,
    parentPropertyName: String,
    parentType: String,
    parentRefClassId: Option[Int],
    childType: Option[String],
    vtInterval: ValidTimeInterval)
    extends TxnAction {

  def createLinkageEntry(tt: Instant): LinkageEntry = createLinkageEntry(TimeInterval(tt))
  def createLinkageEntry(txInterval: TimeInterval): LinkageEntry =
    LinkageEntry(
      id,
      parentRef,
      childRef,
      parentPropertyName,
      parentType,
      parentRefClassId,
      childType,
      vtInterval,
      txInterval)
}
final case class CloseLinkageEntry(
    id: LinkageReference,
    parentRef: FinalTypedReference,
    childRef: EntityReference,
    parentPropertyName: String)
    extends TxnAction

object PutAppEvent {
  def from(appEvent: SerializedAppEvent): PutAppEvent =
    PutAppEvent(
      appEvent.id,
      appEvent.user,
      appEvent.effectiveUser,
      appEvent.application,
      appEvent.contentOwner,
      appEvent.reqId,
      appEvent.reqHostPort,
      appEvent.auditDetails,
      appEvent.appId,
      appEvent.zoneId,
      appEvent.elevatedForUser,
      appEvent.writerHostPort,
      appEvent.receivedAt,
      appEvent.teaIds
    )
}

final case class PutAppEvent(
    id: AppEventReference,
    user: String,
    effectiveUser: Option[String],
    application: Int,
    contentOwner: Int,
    reqId: String,
    reqHostPort: HostPort,
    auditDetails: Option[String],
    appId: DalAppId,
    zoneId: DalZoneId,
    elevatedForUser: Option[String],
    writerHostPort: Option[HostPort],
    receivedAt: Option[Instant],
    teaIds: Set[String])
    extends TxnAction {

  def createAppEvent(tt: Instant, dalTt: Option[Instant]): SerializedAppEvent = {
    // This is to keep backward compatibility, the dalTt field is to be present only when it is different from tt.
    val finalDalTt = dalTt.filterNot(_.equals(tt))
    SerializedAppEvent(
      id,
      tt,
      user,
      effectiveUser,
      application,
      contentOwner,
      reqId,
      reqHostPort,
      auditDetails,
      finalDalTt,
      appId,
      zoneId,
      elevatedForUser,
      writerHostPort,
      receivedAt,
      teaIds
    )
  }
}

final case class PutBusinessEventKey(key: BusinessEventKey) extends TxnAction
final case class PutBusinessEventIndexEntry(
    id: AnyRef,
    eventRef: BusinessEventReference,
    typeName: String,
    properties: Seq[(String, Any)],
    hash: Array[Byte])
    extends TxnAction {

  override def equals(o: Any): Boolean = o match {
    case entry: PutBusinessEventIndexEntry =>
      entry.id == id && entry.typeName == typeName &&
      entry.properties == properties && Arrays.equals(entry.hash, hash)
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hash(id, typeName, properties, hash)
  }

  def createIndexEntry(tt: Instant): EventIndexEntry =
    EventIndexEntry(id, eventRef, typeName, properties, tt, hash)
}

// TODO (OPTIMUS-24842): remove PutObliterateEffect
final case class PutObliterateEffect(effect: ObliterateEffect) extends TxnAction
final case class ExecuteObliterate(action: ObliterateAction) extends TxnAction

final case class DBRawAction(ref: StorableReference, keys: Seq[SerializedKey], operation: DBRawOperation)
    extends TxnAction
final case class PutDBRawEffect(rawEffect: DBRawEffect) extends TxnAction

// TODO (OPTIMUS-15391): remove AddWriteSlots
final case class AddWriteSlots(className: SerializedEntity.TypeRef, writeSlots: Set[Int]) extends TxnAction

final case class FillWriteSlot(vref: VersionedReference, writeSlot: Int) extends TxnAction

final case class PutClassIdMapping(cid: Int, cname: String) extends TxnAction

/*
 * BlobActions
 */
sealed trait BlobTypeEnum { val tpe: Int }
case object EntityType extends BlobTypeEnum { val tpe = 1 }
case object EventType extends BlobTypeEnum { val tpe = 2 }

object BlobTypeEnum {
  def apply(i: Int): BlobTypeEnum = i match {
    case EntityType.tpe => EntityType
    case EventType.tpe  => EventType
  }
  def apply[V <: SerializedStorable](blob: V): BlobTypeEnum = blob match {
    case _: SerializedEntity        => EntityType
    case _: SerializedBusinessEvent => EventType
  }
}

sealed trait BlobAction extends DSIAction {
  type T
  def blobType(): BlobTypeEnum
  def blobReference(): T
  def replicate: Boolean
}

sealed trait AddBlobAction extends BlobAction {
  type T = VersionedReference
  type V <: SerializedStorable
  def blob: V
  def versionHandle(muser: String): VersionHandle[V]
  def archivalTimestamp: Option[Instant]

  val blobType = BlobTypeEnum(blob)
  val slottedRef = SlottedVersionedReference(blobReference(), blob.slot)
}

final case class AddEntityBlob(
    blobReference: VersionedReference,
    blob: SerializedEntity,
    replicate: Boolean,
    archivalTimestamp: Option[Instant])
    extends AddBlobAction {
  override type V = SerializedEntity

  def versionHandle(muser: String): VersionHandle[SerializedEntity] =
    VersionHandle(blobReference, blob, muser, archivalTimestamp)
}

final case class AddEventBlob(
    blobReference: VersionedReference,
    blob: SerializedBusinessEvent,
    replicate: Boolean,
    archivalTimestamp: Option[Instant] = None)
    extends AddBlobAction {
  override type V = SerializedBusinessEvent

  def versionHandle(muser: String): VersionHandle[SerializedBusinessEvent] =
    VersionHandle(blobReference, blob, muser, archivalTimestamp)
}

sealed trait BlobActionWithTypeId extends BlobAction {
  def typeId: Option[Int]
}

// TODO (OPTIMUS-14677): Remove this blob action when it cannot be generated from worklog and tombstonedBlob collection.
final case class RemoveBlobAction(
    blobReference: VersionedReference,
    slots: Seq[Int],
    blobType: BlobTypeEnum,
    replicate: Boolean,
    typeId: Option[Int])
    extends BlobActionWithTypeId {
  type T = VersionedReference
  val slottedRefs = slots.map(SlottedVersionedReference(blobReference, _))
}

final case class RemoveBlobsByRef(
    blobReference: StorableReference,
    blobType: BlobTypeEnum,
    replicate: Boolean,
    typeId: Option[Int])
    extends BlobActionWithTypeId {
  type T = StorableReference
}

/*
 * High-level actions
 */

/*
 * High-level command actions
 */

sealed trait HighLevelAction extends DSIAction
object HighLevelAction {
  private final case class SplittedActions[T](private val actions: Iterable[T]) {
    private val splits = if (actions.size > 1) actions.splitAt(actions.size / 2) else (actions, Iterable.empty[T])
    val first: Seq[T] = splits._1.toSeq
    val second: Seq[T] = splits._2.toSeq
  }
  val log = getLogger(HighLevelAction)
  def apply(
      cmd: WriteCommand,
      vrefMap: Map[SerializedEntity, VersionedReference],
      bevrefMap: Map[SerializedBusinessEvent, VersionedReference]): HighLevelAction = {
    cmd match {
      case p: bitemporal.Put                        => Put(vrefMap(p.value), p.lockToken, p.validTime, p.monoTemporal)
      case i: bitemporal.InvalidateAfter            => InvalidateAfter.make(i)
      case a: bitemporal.AssertValid                => AssertValid.make(a)
      case g: bitemporal.GeneratedAppEvent          => GeneratedAppEvent.make(g, vrefMap)
      case p: bitemporal.PutApplicationEvent        => PutApplicationEvent.make(p, vrefMap, bevrefMap)
      case w: bitemporal.WriteBusinessEvent         => WriteBusinessEvent.make(w, vrefMap, bevrefMap)
      case c: bitemporal.CreateSlots                => Placeholder()
      case f: bitemporal.FillSlot                   => Placeholder()
      case a: bitemporal.AbstractRawCommand         => Placeholder()
      case i: bitemporal.InvalidateAllCurrent       => Placeholder()
      case i: bitemporal.InvalidateAllCurrentByRefs => Placeholder()
      case m: bitemporal.PrepareMonoTemporal        => Placeholder()
      case o: bitemporal.Obliterate                 => Placeholder()
      case a: bitemporal.AccMetadataCommand         => AccAction.make(a)
    }
  }
  def split(a: HighLevelAction): (HighLevelAction, HighLevelAction) = {
    a match {
      case s: AssertValid => {
        val sz = s.refs.size
        if (sz > 1) {
          val splitRefs = s.refs.splitAt(sz / 2)
          AssertValid(splitRefs._1, s.validTime) -> AssertValid(splitRefs._2, s.validTime)
        } else {
          log.warn("Couldn't split AssertValid: only one ref")
          throw new RuntimeException("Couldn't split AssertValid")
        }
      }
      case g: GeneratedAppEvent => {
        val sz = Array(g.asserts.size, g.puts.size, g.invalidates.size)
        val splittedAsserts = SplittedActions(g.asserts)
        val splittedPuts = SplittedActions(g.puts)
        val splittedInvs = SplittedActions(g.invalidates)
        val sz2 = Array(splittedAsserts.first.size, splittedPuts.first.size, splittedInvs.first.size)
        if (sz == sz2) {
          val msg =
            s"Couldn't split GeneratedAppEvent: ${g.asserts.size} asserts, ${g.puts.size} puts, ${g.invalidates.size} invalidates"
          log.warn(msg)
          throw new RuntimeException(msg)
        } else
          (
            GeneratedAppEvent(splittedAsserts.first, splittedPuts.first, splittedInvs.first),
            GeneratedAppEvent(splittedAsserts.second, splittedPuts.second, splittedInvs.second))
      }
      case p: PutApplicationEvent => {
        val sz = p.bes.size
        if (sz > 1) {
          val bs = p.bes.splitAt(sz / 2)
          (
            PutApplicationEvent(bs._1, p.application, p.contentOwner, p.clientTxTime, p.elevatedForUser, p.appEvtId),
            PutApplicationEvent(bs._2, p.application, p.contentOwner, p.clientTxTime, p.elevatedForUser, p.appEvtId))
        } else {
          log.warn("Couldn't split PutApplicationEvent: only one event")
          throw new RuntimeException("Couldn't split PutApplicationEvent")
        }
      }
      case w: WriteBusinessEvent => {
        val sz = Array(w.asserts.size, w.puts.size, w.putSlots.size, w.invalidates.size, w.reverts.size)
        val splittedAsserts = SplittedActions(w.asserts)
        val splittedPuts = SplittedActions(w.puts)
        val splittedPutSlots = SplittedActions(w.putSlots)
        val splittedInvs = SplittedActions(w.invalidates)
        val splittedReverts = SplittedActions(w.reverts)
        val sz2 = Array(
          splittedAsserts.first.size,
          splittedPuts.first.size,
          splittedPutSlots.first.size,
          splittedInvs.first.size,
          splittedReverts.first.size)
        if (sz == sz2) {
          val msg =
            s"""Couldn't split WriteBusinessEvent: ${w.asserts.size} asserts, ${w.puts.size} puts,
               |${w.putSlots.size} putSlots, ${w.invalidates.size} invalidates, ${w.reverts.size} reverts""".stripMargin
          log.warn(msg)
          throw new RuntimeException(msg)
        } else
          (
            WriteBusinessEvent(
              w.evt,
              w.state,
              splittedAsserts.first,
              splittedPuts.first,
              splittedPutSlots.first,
              splittedInvs.first,
              splittedReverts.first),
            WriteBusinessEvent(
              w.evt,
              w.state,
              splittedAsserts.second,
              splittedPuts.second,
              splittedPutSlots.second,
              splittedInvs.second,
              splittedReverts.second))
      }
      case _: Put => val msg = "Couldn't split HighLevelAction.Put"; log.warn(msg); throw new RuntimeException(msg)
      case _: InvalidateAfter =>
        val msg = "Couldn't split HighLevelAction.InvalidateAfter"; log.warn(msg); throw new RuntimeException(msg)
      case _: Placeholder =>
        val msg = "Couldn't split HighLevelAction.Placeholder"; log.warn(msg); throw new RuntimeException(msg)
      case _: AccAction =>
        val msg = "Couldn't split HighLevelAction.AccAction"; log.warn(msg); throw new RuntimeException(msg)
    }
  }
  final case class Placeholder() extends HighLevelAction
  final case class Put(
      value: VersionedReference,
      lockToken: Option[Long],
      validTime: Instant,
      monoTemporal: Boolean = false)
      extends HighLevelAction
  object Put {
    def make(p: bitemporal.Put, vref: Map[SerializedEntity, VersionedReference]) =
      Put(vref(p.value), p.lockToken, p.validTime, p.monoTemporal)
  }
  final case class InvalidateAfter(
      entityRef: EntityReference,
      versionedRef: VersionedReference,
      lockToken: Long,
      validTime: Instant,
      monoTemporal: Boolean = false)
      extends HighLevelAction
  object InvalidateAfter {
    def make(i: bitemporal.InvalidateAfter) =
      InvalidateAfter(i.entityRef, i.versionedRef, i.lockToken, i.validTime, i.monoTemporal)
  }
  final case class AssertValid(refs: Iterable[EntityReference], validTime: Instant) extends HighLevelAction
  object AssertValid {
    def make(a: bitemporal.AssertValid) = AssertValid(a.refs, a.validTime)
  }
  final case class GeneratedAppEvent(asserts: Seq[AssertValid], puts: Seq[Put], invalidates: Seq[InvalidateAfter])
      extends HighLevelAction
  object GeneratedAppEvent {
    def make(g: bitemporal.GeneratedAppEvent, vrefMap: Map[SerializedEntity, VersionedReference]) = {
      val asserts = g.asserts.map(a => AssertValid.make(a))
      val puts = g.puts.map(p => Put.make(p, vrefMap))
      val invalidates = g.invalidates.map(i => InvalidateAfter.make(i))
      GeneratedAppEvent(asserts, puts, invalidates)
    }
  }
  final case class PutApplicationEvent(
      bes: Seq[WriteBusinessEvent],
      application: Int,
      contentOwner: Int,
      clientTxTime: Option[Instant],
      elevatedForUser: Option[String],
      appEvtId: Option[AppEventReference])
      extends HighLevelAction
  object PutApplicationEvent {
    def make(
        p: bitemporal.PutApplicationEvent,
        vrefMap: Map[SerializedEntity, VersionedReference],
        bevrefMap: Map[SerializedBusinessEvent, VersionedReference]) = {
      val bs = p.bes.map(b => WriteBusinessEvent.make(b, vrefMap, bevrefMap))
      PutApplicationEvent(bs, p.application, p.contentOwner, p.clientTxTime, p.elevatedForUser, p.appEvtId)
    }
  }
  final case class WriteBusinessEvent(
      evt: VersionedReference,
      state: EventStateFlag,
      asserts: Iterable[EntityReference],
      puts: Seq[WriteBusinessEvent.Put],
      putSlots: Seq[WriteBusinessEvent.PutSlots],
      invalidates: Seq[(EntityReference, Long, Boolean)],
      reverts: Seq[(EntityReference, Long)])
      extends HighLevelAction
  object WriteBusinessEvent {
    final case class Put(ent: VersionedReference, lockToken: Option[Long], monoTemporal: Boolean = false)
    final case class PutSlots(ents: immutable.SortedSet[VersionedReference], lockToken: Option[Long])
    object PutSlots {
      def make(bev: bitemporal.WriteBusinessEvent.PutSlots, vrefMap: Map[SerializedEntity, VersionedReference]) = {
        PutSlots(bev.ents.entities.map(vrefMap(_)), bev.lockToken)
      }
    }
    def make(
        b: bitemporal.WriteBusinessEvent,
        vrefMap: Map[SerializedEntity, VersionedReference],
        bevrefMap: Map[SerializedBusinessEvent, VersionedReference]) = {
      val puts = b.puts.map(p => Put(vrefMap(p.ent), p.lockToken, p.monoTemporal))
      val putSlots = b.putSlots.map(p => PutSlots.make(p, vrefMap))
      // For WriteBusinessEvents with EventStateFlag.RESTATE, we won't have a versioned reference in the map, so just put in a blank vref
      WriteBusinessEvent(
        bevrefMap.getOrElse(b.evt, VersionedReference.Nil),
        b.state,
        b.asserts,
        puts,
        putSlots,
        b.invalidates.map(i => (i.er, i.lt, i.monoTemporal)),
        b.reverts.map(i => i.er -> i.lt)
      )
    }
  }
  final case class AccAction(
      val clsName: String,
      val slot: Int,
      action: AccMetadataOp,
      accTtOpt: Option[Instant],
      accInfoOpt: Option[SerializedEntity],
      forceCreateIndex: Boolean)
      extends HighLevelAction
  object AccAction {
    def make(accCommand: bitemporal.AccMetadataCommand) =
      AccAction(
        accCommand.clsName,
        accCommand.slot,
        accCommand.action,
        accCommand.accTtOpt,
        accCommand.accInfoOpt,
        accCommand.forceCreateIndex)
  }
}
