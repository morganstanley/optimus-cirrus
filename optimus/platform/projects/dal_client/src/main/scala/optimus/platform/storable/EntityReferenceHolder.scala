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
package optimus.platform.storable

import java.lang.ref.WeakReference
import java.util.concurrent.atomic.AtomicInteger
import java.time.Instant
import msjava.slf4jutils.scalalog.getLogger
import optimus.platform._
import optimus.platform.annotations.deprecating
import optimus.platform.dal.DALImpl
import optimus.platform.dal.DSIStorageInfo
import optimus.platform.dal.EntitySerializer
import optimus.platform._
import optimus.platform.bitemporal.EntityBitemporalSpace
import optimus.platform.bitemporal.EntityRectangle
import optimus.platform.temporalSurface.impl.FlatTemporalContext
import optimus.platform.temporalSurface.operations.EntityReferenceQueryReason

final case class EntityReferenceHolder[T <: Entity] private (
    val ref: EntityReference,
    val tc: TemporalContext,
    val storageTxTime: Option[Instant] = None)
    extends ReferenceHolder[T] {

  // volatile cache is sufficient because in the worst race we would just end up doing findByReference more than once
  // on different threads, which is ok because reads are fully repeatable.
  @volatile @transient private[this] var cache: WeakReference[T] = _

  /*
  versioned ref here is a private mutable var as opposed to a case class parameter as it is an internal
  detail used mainly to support the isSameVersionAs api
   */
  private var _versionedReference: Option[VersionedReference] = None
  private[optimus] def versionedReference: Option[VersionedReference] = _versionedReference
  private[optimus] def withVersionedReference(vref: VersionedReference) = {
    this._versionedReference = Some(vref)
    this
  }

  private def setCache(t: T): Unit = {
    withTypeInfo(t.getClass(), true)
    cache = new WeakReference(t)
  }

  private[optimus] def withTypeInfo(clazz: Class[_ <: T], concrete: Boolean): this.type = {
    if (concrete || (knownPayloadType eq null) || knownPayloadType.isAssignableFrom(clazz)) {
      this.knownPayloadType = clazz
      this.concrete = concrete
    } else {
      EntityReferenceHolder.log.warn(s"cant refine type. was $knownPayloadType, proposed $clazz")
    }
    this
  }
  private var knownPayloadType: Class[_ <: T] = _
  private var concrete: Boolean = false

  def payloadType = {
    val result = if (cache ne null) cache.get else null.asInstanceOf[T]
    if (result ne null) Some(ConcreteHolderType(result.getClass))
    else if (knownPayloadType ne null) Some(HolderType(concrete, knownPayloadType))
    else None
  }

  @scenarioIndependent @node def payload: T = {
    var result: T = if (cache != null) cache.get else null.asInstanceOf[T]
    if (result eq null) {
      val reason =
        if (cache eq null) EntityReferenceQueryReason.Holder
        else EntityReferenceQueryReason.HolderExpired

      result =
        if (knownPayloadType eq null)
          DALImpl.resolver.findByReference(ref, tc).asInstanceOf[T]
        else DALImpl.resolver.findByReferenceWithType(ref, tc, knownPayloadType, concrete, reason)
      setCache(result)
    }
    result
  }

  def isSameVersionAs(other: EntityReferenceHolder[T]) = {
    require(
      versionedReference.isDefined && other.versionedReference.isDefined,
      s"Versioned reference should be defined on both eref holders found ${versionedReference} and ${other.versionedReference}"
    )
    versionedReference == other.versionedReference
  }
}

object EntityReferenceHolder {
  @deprecating(suggestion = "only for specific tests - discuss with the graph team if you need to use this")
  def forTest[T <: Entity](bytes: Array[Byte], tc: TemporalContext, clazz: Class[T]): EntityReferenceHolder[T] = {
    new EntityReferenceHolder[T](EntityReference(bytes), tc, None).withTypeInfo(clazz, false)
  }

  def withoutStorageInfo[T <: Entity](t: T): EntityReferenceHolder[T] = {
    require(!t.dal$isTemporary, s"Cannot create a EntityReferenceHolder for heap entity: $t")
    val efh = new EntityReferenceHolder[T](t.dal$entityRef, t.dal$temporalContext, None)
    efh.setCache(t)
    efh
  }

  private[optimus] def apply[T <: Entity](
      permRef: EntityReference,
      tc: TemporalContext,
      storageTxTime: Option[Instant],
      versionedReference: Option[VersionedReference]): EntityReferenceHolder[T] = {
    val holder = new EntityReferenceHolder[T](permRef, tc, storageTxTime)
    versionedReference.map(holder.withVersionedReference).getOrElse(holder)
  }

  def apply[T <: Entity](t: T): EntityReferenceHolder[T] = {
    require(
      !t.dal$isTemporary && t.dal$storageInfo.isInstanceOf[DSIStorageInfo],
      s"Cannot create a EntityReferenceHolder for heap entity: $t")
    val versionedRef = Option(t.dal$storageInfo) collect { case DSIStorageInfo(_, _, _, vref) => vref }
    val efh =
      EntityReferenceHolder[T](t.dal$entityRef, t.dal$temporalContext, Some(t.dal$storageInfo.txTime), versionedRef)
    efh.setCache(t)
    efh
  }
  private[optimus] def withKnownAbstractType[T <: Entity](
      permRef: EntityReference,
      tc: TemporalContext,
      storageTxTime: Option[Instant],
      versionedReference: Option[VersionedReference],
      abstractType: Class[T]): EntityReferenceHolder[T] = {
    EntityReferenceHolder[T](permRef, tc, storageTxTime, versionedReference).withTypeInfo(abstractType, false)
  }
  private[optimus] def fromJustStoredEntity[T <: Entity](
      t: T,
      permRef: EntityReference,
      tc: TemporalContext,
      versionedRef: VersionedReference): EntityReferenceHolder[T] = {
    new EntityReferenceHolder[T](permRef, tc)
      .withTypeInfo(t.getClass(), true)
      .withVersionedReference(versionedRef)
  }
  private val log = getLogger(getClass)

  private def clearedReference(): Unit = {
    val clearedReferences = clearedReferenceCounter.incrementAndGet()
    if (Integer.bitCount(clearedReferences) == 1)
      log.info(s"total entity references requested due EntityReferenceHolder cache clear = $clearedReferences")
  }
  private val clearedReferenceCounter = new AtomicInteger()
}

final case class InMemoryReferenceHolder[T <: Storable](ref: T) extends ReferenceHolder[T] {
  @node def payload: T = ref
  def payloadType = Some(ConcreteHolderType(ref.getClass))
}

object ReferenceHolderFactory {
  def apply[T <: Entity](t: T): ReferenceHolder[T] =
    if (t.dal$isTemporary) InMemoryReferenceHolder(t) else EntityReferenceHolder(t)
}

final case class EntityPointHolder(eref: EntityReference, creationContext: TemporalContext) {
  @node @scenarioIndependent private def persistedEntity: Option[PersistentEntity] =
    DALImpl.resolver.getPersistentEntityByRef(eref, creationContext).filterNot(_ == PersistentEntity.Null)
  @node @scenarioIndependent def entityAtCreation: Option[Entity] =
    persistedEntity.map(EntitySerializer.hydrate[Entity](_, creationContext))
}

final case class EntityVersionHolder[T <: Entity](val bitempSpace: EntityBitemporalSpace[T]) extends VersionHolder[T] {

  lazy val firstRectangleOfAVersion: EntityRectangle[T] = bitempSpace.all.minBy(r => (r.ttFrom, r.vtFrom))

  lazy val creationContext: TemporalContext =
    FlatTemporalContext(firstRectangleOfAVersion.vtFrom, firstRectangleOfAVersion.ttFrom, None)

  @node @scenarioIndependent def persistentEntityAt(tc: TemporalContext): PersistentEntity = {
    tc match {
      case t: FlatTemporalContext => bitempSpace.at(t.vt, t.tt).pe
      case _ => throw new IllegalStateException("persistentEntityAt operation only supported FlatTemporalContext")
    }
  }

  @node @scenarioIndependent def persistentEntityOptionAt(tc: TemporalContext): Option[PersistentEntity] = {
    tc match {
      case t: FlatTemporalContext =>
        bitempSpace.atOption(t.vt, t.tt) flatMap { rect =>
          rect.peOption
        }
      case _ => throw new IllegalStateException("persistentEntityAt operation only supported FlatTemporalContext")
    }
  }

  @node @scenarioIndependent def payloadAt(tc: TemporalContext): T = {
    tc match {
      case t: FlatTemporalContext => bitempSpace.at(t.vt, t.tt).entityAt(t.vt, t.tt)
      case _ => throw new IllegalStateException("payloadAt operation only supported when FlatTemporalContext is passed")
    }
  }

  @node @scenarioIndependent def payloadOptionAt(tc: TemporalContext, entitledOnly: Boolean): Option[T] = {
    tc match {
      case t: FlatTemporalContext =>
        bitempSpace.atOption(t.vt, t.tt) flatMap { rect =>
          rect.entityOptionAt(t.vt, t.tt, entitledOnly)
        }
      case _ => throw new IllegalStateException("payloadAt operation only supported when FlatTemporalContext is passed")
    }
  }
}

final case class EntityChange[A <: Entity](
    private[optimus] val eref: EntityReference,
    val from: Option[VersionHolder[A]],
    val to: Option[VersionHolder[A]],
    private[optimus] val readTemporalContext: Option[TemporalContext])
