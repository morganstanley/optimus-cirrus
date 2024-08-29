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
package optimus.platform.dsi.bitemporal

import optimus.dsi.base.EntityVersionHandle
import optimus.dsi.base.EventVersionHandle
import optimus.dsi.base.RefHolder
import optimus.dsi.base.actions.LinkedTypes
import optimus.entity.EntityAuditInfo
import optimus.platform.TimeInterval
import optimus.platform.bitemporal._
import optimus.platform.storable._

import java.time.Instant
import java.util.Objects
import scala.collection.immutable.SortedSet

trait OptimisticallyVersioned {
  val lockToken: Long
}

trait HasDSIId[A] {
  def id: A
}

trait TemporalMatching extends Temporal {
  def matches(temp: DSIQueryTemporality): Boolean = {
    temp match {
      case DSIQueryTemporality.At(validTime, txTime) => vtInterval.contains(validTime) && txInterval.contains(txTime)
      case DSIQueryTemporality.ValidTime(validTime, readTxTime) =>
        vtInterval.contains(validTime) && !(txInterval.from isAfter readTxTime)
      case DSIQueryTemporality.TxTime(txTime)  => txInterval.contains(txTime)
      case DSIQueryTemporality.All(readTxTime) => !(txInterval.from isAfter readTxTime)
      case DSIQueryTemporality.TxRange(range)  => txInterval overlaps range
      case DSIQueryTemporality.BitempRange(vtRange, ttRange, inRange) =>
        inRange match {
          case true =>
            !(vtInterval.to isBefore vtRange.from) &&
            !(vtInterval.from isAfter vtRange.to) &&
            !(txInterval.to isBefore ttRange.from) &&
            !(txInterval.from isAfter ttRange.to)
          case _ =>
            (vtInterval.to isAfter vtRange.from) &&
            !(vtInterval.from isAfter vtRange.to) &&
            (txInterval.to isAfter ttRange.from) &&
            !(txInterval.from isAfter ttRange.to)
        }
      case DSIQueryTemporality.OpenVtTxRange(range) =>
        vtInterval.to.equals(TimeInterval.Infinity) &&
        ((!(txInterval.from isBefore (range.from)) &&
          (txInterval.from isBefore (range.to))) ||
          (!(txInterval.to isBefore (range.from)) &&
            (txInterval.to isBefore (range.to))))

    }
  }
}

trait VersionHandle[V <: SerializedStorable] extends HasDSIId[VersionedReference] {
  def id: VersionedReference
  def payload: V
  def muser: String
  def archivalTimestamp: Option[Instant]
}

object VersionHandle {
  def apply(
      id: VersionedReference,
      entity: SerializedEntity,
      muser: String,
      archivalTimestamp: Option[Instant]): EntityVersionHandle =
    VersionHandleImpl(id, entity, muser, archivalTimestamp)
  def apply(id: VersionedReference, event: SerializedBusinessEvent, muser: String): EventVersionHandle =
    EventVersionHandleImpl(id, event, muser)
  def apply(
      id: VersionedReference,
      event: SerializedBusinessEvent,
      muser: String,
      archivalTimestamp: Option[Instant]): EventVersionHandle =
    EventVersionHandleImpl(id, event, muser, archivalTimestamp)

  def createWithLazyEntity(
      id: VersionedReference,
      entityFactory: () => SerializedEntity,
      muser: String,
      archivalTimestamp: Option[Instant]): EntityVersionHandle =
    new LazyVersionHandleImpl(id, entityFactory, muser, archivalTimestamp)

  def createWithLazyEvent(
      id: VersionedReference,
      entityFactory: () => SerializedBusinessEvent,
      muser: String): EventVersionHandle =
    new LazyEventVersionHandleImpl(id, entityFactory, muser)

  private final case class VersionHandleImpl(
      id: VersionedReference,
      payload: SerializedEntity,
      muser: String,
      archivalTimestamp: Option[Instant])
      extends EntityVersionHandle

  private final case class EventVersionHandleImpl(
      id: VersionedReference,
      payload: SerializedBusinessEvent,
      muser: String,
      archivalTimestamp: Option[Instant] = None)
      extends EventVersionHandle

  private class LazyVersionHandleImpl(
      val id: VersionedReference,
      private var entityFactory: () => SerializedEntity,
      val muser: String,
      val archivalTimestamp: Option[Instant])
      extends EntityVersionHandle {
    lazy val payload = {
      val e = entityFactory()
      entityFactory = null // free the memory (e.g. the factory might be holding on to a large BSON document)
      e
    }
    override def equals(other: Any): Boolean = {
      other match {
        case otherHandle: LazyVersionHandleImpl =>
          id == otherHandle.id && muser == otherHandle.muser && payload == otherHandle.payload
        case _ => false
      }
    }
    override def hashCode(): Int = Objects.hash(id, muser, payload)
  }

  private class LazyEventVersionHandleImpl(
      val id: VersionedReference,
      private var entityFactory: () => SerializedBusinessEvent,
      val muser: String,
      val archivalTimestamp: Option[Instant] = None)
      extends EventVersionHandle {
    lazy val payload = {
      val e = entityFactory()
      entityFactory = null // free the memory (e.g. the factory might be holding on to a large BSON document)
      e
    }
  }

}

final case class AuditInfoHandle(id: VersionedReference, auditInfo: EntityAuditInfo)
    extends HasDSIId[VersionedReference]

sealed trait AdvanceOp {
  val info: String
  override def toString: String = info
}
object AdvanceOp {
  def apply(info: String): AdvanceOp = {
    info match {
      case RevertAdvanceOp.info => RevertAdvanceOp
      case _                    => throw new IllegalArgumentException(s"unexpected AdvanceOp: $info encountered")
    }
  }
}
case object RevertAdvanceOp extends AdvanceOp {
  override val info: String = "revert"
}

object TimeSliceData {
  implicit def toVref(tsd: TimeSliceData): VersionedReference = tsd.vref
}
sealed trait TimeSliceData {
  val vref: VersionedReference
  val vnum: Int
  val creationAppEvt: AppEventReference
}
final case class EvtTimeSliceData(
    vref: VersionedReference,
    vnum: Int,
    creationAppEvt: AppEventReference,
    evtId: BusinessEventReference,
    evtVid: Int,
    evtType: Option[String],
    advanceOp: Option[AdvanceOp])
    extends TimeSliceData
final case class NoEvtTimeSliceData(vref: VersionedReference, vnum: Int, creationAppEvt: AppEventReference)
    extends TimeSliceData

final case class EntityTimeSlice(
    entityRef: EntityReference,
    updatedAppEvent: AppEventReference,
    rect: Rectangle[TimeSliceData])
    extends TemporalMatching {
  def vtInterval = rect.vtInterval
  def txInterval = rect.ttInterval

  def timeSliceNumber = rect.index
  def versionedRef = rect.data.vref
  def vnum = rect.data.vnum
  def id: (EntityReference, Int) = (entityRef, rect.index)

  override def toString() = s"TS(ref=${entityRef},rect=${rect},uae=${updatedAppEvent})"
}

abstract class Grouping[TargetType](val id: RefHolder, val space: BitemporalSpace[TargetType])
    extends HasDSIId[RefHolder]
    with OptimisticallyVersioned

object EntityGrouping {
  type LockTokenType = Long
  implicit def ordering[T <: EntityGrouping]: Ordering[T] = new Ordering[T] {
    override def compare(x: T, y: T): Int = RawReference.ordering.compare(x.id, y.id)
  }
}

/**
 * Encapsulates bitemporal metadata for all versions of an entity. This is the canonical source for when each entity
 * version is valid, and which keys are used by some version of entity at a given transaction time. This is also used to
 * enforce optimistic locking for changes to an entity and the keys used by an entity.
 *
 * @param permanentRef
 *   the permanent reference of the entity
 * @param keys
 *   the keys used by the entity at various transaction times. For each transaction time range in this sequence, a set
 *   of keys used by any entity version valid during that transaction time range.
 * @param lockToken
 *   the optimistic lock token
 */
final case class EntityGrouping(
    val id: FinalTypedReference,
    val cmid: Option[CmReference],
    val className: String,
    val types: Seq[String],
    val maxTimeSliceCount: Int,
    val maxVersionCount: Int,
    val lockToken: Long,
    val linkedTypes: Option[LinkedTypes])
    extends HasDSIId[EntityReference]
    with OptimisticallyVersioned {

  def linkageDefinerTypesOpt: Option[Set[String]] = linkedTypes.map(_.linkageDefinerTypes)

  /**
   * @return
   *   a new version of the grouping
   */
  def next(maxTimeSliceCount: Int, maxVersionCount: Int, tt: Instant) =
    new EntityGrouping(
      permanentRef,
      cmid,
      className,
      types,
      maxTimeSliceCount,
      maxVersionCount,
      DateTimeSerialization.fromInstant(tt),
      linkedTypes)

  private def mergeLinkedTypes(newLinkedTypes: Option[LinkedTypes]): Option[LinkedTypes] = {
    (linkedTypes, newLinkedTypes) match {
      // when original linkedTypes is None, we will still put None
      case (None, _)            => None
      case (Some(l1), Some(l2)) => Some(l1.merge(l2))
      case (Some(_), None)      => linkedTypes
    }
  }

  // transitional code to promote old grouping schema to new (including types). remove after schema migration complete.
  def next(
      maxTimeSliceCount: Int,
      maxVersionCount: Int,
      newTypes: Seq[String],
      newLinkedTypes: Option[LinkedTypes],
      tt: Instant) =
    new EntityGrouping(
      permanentRef,
      cmid,
      className,
      newTypes,
      maxTimeSliceCount,
      maxVersionCount,
      DateTimeSerialization.fromInstant(tt),
      mergeLinkedTypes(newLinkedTypes)
    )

  override def equals(o: Any): Boolean = o match {
    case et: EntityGrouping => lockToken == et.lockToken && permanentRef == et.permanentRef
    case _                  => false
  }

  override def hashCode: Int = id.hashCode * 31 + lockToken.hashCode

  final def permanentRef = id

  override def toString() =
    s"Grp(ref=${id}, cmid=${cmid}, cn=${className}, lt=${lockToken}, tsCnt=${maxTimeSliceCount}, vnCnt=${maxVersionCount})"
}

final case class KeyGrouping(
    key: SerializedKey,
    i: RefHolder,
    s: BitemporalSpace[EntityReference],
    lockToken: Long,
    classIdOpt: Option[Int])
    extends Grouping(i, s) {
  def next(newSpace: BitemporalSpace[EntityReference], tt: Instant) =
    KeyGrouping(key, id, newSpace, DateTimeSerialization.fromInstant(tt), classIdOpt)
  def keyRef = id
}

/**
 * Encapsulates metadata about a single concrete entity class
 */
final case class StoredClassMetadata(
    className: SerializedEntity.TypeRef,
    writeSlots: Set[Int],
    readSlots: Set[Int],
    registrationWriteMode: RegistrationWriteMode = RegistrationWriteMode.Default)
object StoredClassMetadata {
  def empty(cn: SerializedEntity.TypeRef): StoredClassMetadata = apply(cn, Set.empty, Set.empty)
}

final case class DalSecureConfig(entityList: SortedSet[String], packageList: SortedSet[String]) {
  def isEmpty = { entityList.isEmpty && packageList.isEmpty }
}
object DalSecureConfig {
  def empty(): DalSecureConfig = apply(SortedSet.empty, SortedSet.empty)
}

sealed trait RegistrationWriteMode {
  def id: String
}

object RegistrationWriteMode {
  case object UnregisteredAllowed extends RegistrationWriteMode { val id = "unregisteredAllowed" }
  case object RegisteredWritesOnly extends RegistrationWriteMode { val id = "registeredWritesOnly" }
  case object AlertUnregisteredWrites extends RegistrationWriteMode { val id = "alertUnregisteredWrites" }
  case object ShapeMatchWritesAllowed extends RegistrationWriteMode { val id = "shapeMatchWritesAllowed" }

  // used when super types (e.g. trait) have ShapeMatchWritesAllowed mode set.
  // the check will ensure the data shape is a super set.
  final case class ShapeIncludeWritesAllowed(types: Seq[String]) extends RegistrationWriteMode {
    val id = "shapeIncludeWritesAllowed"
  }

  def Default: RegistrationWriteMode = UnregisteredAllowed

  def apply(mode: String): RegistrationWriteMode = mode match {
    case UnregisteredAllowed.id     => UnregisteredAllowed
    case RegisteredWritesOnly.id    => RegisteredWritesOnly
    case AlertUnregisteredWrites.id => AlertUnregisteredWrites
    case ShapeMatchWritesAllowed.id => ShapeMatchWritesAllowed
    case _                          => throw new IllegalArgumentException(s"Unknown registration write mode: $mode")
  }
}

final case class UniqueIndexGrouping(
    val key: SerializedKey,
    val id: RefHolder,
    val maxTimeSliceCount: Int,
    val lockToken: Long)
    extends HasDSIId[RefHolder]
    with OptimisticallyVersioned {

  /**
   * @return
   *   a new version of the unique index
   */
  def next(newMaxTimeSliceCount: Int, tt: Instant) =
    UniqueIndexGrouping(key, id, newMaxTimeSliceCount, DateTimeSerialization.fromInstant(tt))
}

final case class UniqueIndexTimeSlice(val hash: RefHolder, rect: Rectangle[EntityReference], val typeIdOpt: Option[Int])
    extends TemporalMatching {
  def vtInterval = rect.vtInterval
  def txInterval = rect.ttInterval

  def timeSliceNumber = rect.index
  def entityRef = rect.data
  def id: (RefHolder, Int) = (hash, timeSliceNumber)

  override def toString() = s"TS(id=${id},ref=${entityRef},${rect})"
}
