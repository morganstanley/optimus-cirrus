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

import java.util.UUID

import msjava.base.util.uuid.MSUuid
import optimus.platform.TemporalContext
import optimus.platform.pickling.PickledInputStream

private[optimus] sealed trait EntityUniverse
private[optimus] object AppliedUniverse extends EntityUniverse
private[optimus] object UniqueUniverse extends EntityUniverse
private[optimus] object DALUniverse extends EntityUniverse

// make the EntityFlavor as abstract class not sealed trait to optimize method access
// while a class can extend only one superclass, a class can implement any number of interfaces,
// so method calls through an interface are more complicated.
// For interface calls, it has to look up the class's list of interfaces first
// to find the interface that's wanted, before it can look up the method implementation in that interface's table.
//
// Keep in sync with DoNotUseIJCompileTimeEntityInjectedImplementation
private[optimus] sealed abstract class EntityFlavor extends Serializable {

  // DAL entity methods - these all return null unless this EntityFlavor is backed by a DALEntityFlavor
  def dalEntityFlavor: DALEntityFlavor
  def dal$entityRef: EntityReference
  def dal$cmid: Option[MSUuid]
  def dal$storageInfo: StorageInfo
  def dal$loadContext: TemporalContext
  def dal$inlineEntities: InlineEntityHolder
  final def dal$temporalContext: TemporalContext = dal$loadContext

  // General purpose methods - these work on all EntityFlavors
  def entityHashCode(e: Entity): Int
  def entityEquals(e1: Entity, e2: Entity): Boolean
  def universe: EntityUniverse
  def dal$isTemporary: Boolean
}

private[optimus] object EntityFlavor {
  def apply(is: PickledInputStream, info: StorageInfo, eref: EntityReference): EntityFlavor = {
    info match {
      case null | AppliedStorageInfo if eref eq null => DefaultAppliedEF
      case UniqueStorageInfo                         => DefaultUniqueEF
      case _ => // currently, we only have DAL entity flavor (may need to add ticking one later)
        val flavor = new DALEntityFlavor
        flavor.dal$entityRef = eref
        flavor.dal$storageInfo = info
        flavor.dal$loadContext = is.temporalContext
        flavor.dal$inlineEntities = is.inlineEntitiesByRef
        flavor
    }
  }
}

/** The base type for all EntityFlavors which are not backed by a DALEntityFlavor */
private[optimus] sealed trait NonDALEntityFlavor extends EntityFlavor {
  override final def dalEntityFlavor: DALEntityFlavor = null

  final def dal$entityRef: EntityReference = null
  final def dal$entityRef_=(entityRef: EntityReference): Unit =
    throw new IllegalStateException("cannot mutate entity reference of default entity flavor")

  final def dal$cmid: Option[MSUuid] =
    throw new IllegalStateException(
      "CMID should be assigned automatically on hydration, and it shouldn't be possible to call this getter before hydration.")
  final def dal$cmid_=(cmid: Option[MSUuid]): Unit =
    throw new IllegalStateException("cannot mutate cmid of default entity flavor")

  final def dal$storageInfo_=(info: StorageInfo): Unit =
    throw new IllegalStateException("cannot mutate entity storage info of default entity flavor")

  final def dal$loadContext: TemporalContext = null
  final def dal$loadContext_=(in: TemporalContext): Unit =
    throw new IllegalStateException("cannot mutate load context of default entity flavor")

  override final def dal$inlineEntities: InlineEntityHolder = InlineEntityHolder.empty

}

private[optimus] sealed trait AppliedFlavorBase {
  final def entityHashCode(e: Entity): Int = 41 * EntityImpl.argsHash(e) + e.$info.entityClassHash
  final def entityEquals(e1: Entity, e2: Entity): Boolean = EntityImpl.argsEquals(e1, e2)
  final def universe: EntityUniverse = AppliedUniverse
  final def dal$isTemporary: Boolean = true
}

private[optimus] sealed trait UniqueFlavorBase {
  final def universe: EntityUniverse = UniqueUniverse
  final def dal$isTemporary: Boolean = true
}

// these two default value is used for normal heap entities (without mutation)
// they don't have any field (won't have extra memory cost)
private[optimus] object DefaultAppliedEF extends NonDALEntityFlavor with AppliedFlavorBase {
  final def dal$storageInfo: StorageInfo = AppliedStorageInfo
}

private[optimus] object DefaultUniqueEF extends NonDALEntityFlavor with UniqueFlavorBase {
  final def entityEquals(e1: Entity, e2: Entity): Boolean = e1 eq e2
  final def dal$storageInfo: StorageInfo = UniqueStorageInfo
  final def entityHashCode(e: Entity): Int = System.identityHashCode(e)
}

/**
 * When a non-DAL entity is persisted in a legacy persist { } block, it gets some DAL-entity information (store context,
 * entity ref etc.) stamped into it. To make that possible, the previous NonDALEntityFlavor gets swapped out for a
 * subclass of HybridEntityFlavor.
 *
 * TODO (OPTIMUS-7177): Remove this hybrid after decoing the persist block mutation
 */
private[optimus] sealed abstract class HybridEntityFlavor extends EntityFlavor {
  override def dalEntityFlavor: DALEntityFlavor

  def dal$entityRef: EntityReference = dalEntityFlavor.dal$entityRef

  def dal$cmid: Option[MSUuid] = dalEntityFlavor.dal$cmid

  def defaultStorageInfo: StorageInfo

  final def dal$storageInfo: StorageInfo =
    if (dalEntityFlavor.dal$storageInfo eq null) defaultStorageInfo else dalEntityFlavor.dal$storageInfo

  final def dal$loadContext: TemporalContext = dalEntityFlavor.dal$loadContext

  final var dal$inlineEntities: InlineEntityHolder = dalEntityFlavor.dal$inlineEntities
}

/**
 * An applied entity flavor with an added mutable DALEntityFlavor (see docs on that class)
 */
private[optimus] final class HybridAppliedEntityFlavor(override val dalEntityFlavor: DALEntityFlavor)
    extends HybridEntityFlavor
    with AppliedFlavorBase {
  def defaultStorageInfo: StorageInfo = AppliedStorageInfo
}

private[optimus] final class DALEntityFlavor extends EntityFlavor {
  override def dalEntityFlavor: DALEntityFlavor = this

  def dal$isTemporary: Boolean = false

  def entityHashCode(e: Entity): Int = {
    require(
      e.dal$entityRef ne null,
      "Null entity reference on a stored entity - is construction in progress? - You cant calculate a hashcode before construction is complete"
    )
    41 * e.dal$temporalContext.hashCode + e.dal$entityRef.hashCode
  }

  def entityEquals(e1: Entity, e2: Entity): Boolean =
    (e1.dal$temporalContext == e2.dal$temporalContext) && (e1.dal$entityRef == e2.dal$entityRef)

  def universe: EntityUniverse = DALUniverse

  // NB: These values should be DAL entity only, which is used to check DAL entity equality
  // var usage: both these fields might get mutated when an entity is saved; they represent internal state
  private[this] var _dal$entityRef: EntityReference = _
  private[this] var _dal$cmid: Option[MSUuid] = _

  private def requireState(state: Boolean, errorMessage: String): Unit = {
    if (!state) throw new IllegalStateException(errorMessage)
  }

  // NB: These are public because they are accessed via code injected into user packages.
  def dal$entityRef: EntityReference = _dal$entityRef
  def dal$entityRef_=(entityRef: EntityReference): Unit = {
    requireState(_dal$entityRef == null, "Entity reference can be set only once.")
    _dal$entityRef = entityRef
  }

  def dal$cmid_=(cmid: Option[MSUuid]): Unit = {
    requireState(
      _dal$cmid == null,
      "This setter should not be called more than once (that once should be during hydration).")
    require(cmid != null, "CMID should have a value during hydration. Setting to null would be unreasonable.")
    _dal$cmid = cmid
  }

  def dal$cmid: Option[MSUuid] = {
    requireState(
      _dal$cmid != null,
      "CMID should be assigned automatically on hydration, and it shouldn't be possible to call this getter before hydration.")
    _dal$cmid
  }

  // _dal$storageInfo should be explicitly set after the constructor as completed
  // it is used as a flag that the constructor has completed by temporal contexts witness version.
  // generally the hashcode is not valid until the storage info is set
  private[this] var _dal$storageInfo: StorageInfo = _
  def dal$storageInfo: StorageInfo = _dal$storageInfo
  def dal$storageInfo_=(info: StorageInfo): Unit = _dal$storageInfo = info

  // NB: This must never be set for temporary entities which get persisted.
  // They must remain in "temporary" state to avoid violating equals, hashcode contracts.
  // Is set to actual context in generated unpickling ctor
  private[this] var _dal$loadContext: TemporalContext = _
  def dal$loadContext: TemporalContext = _dal$loadContext
  def dal$loadContext_=(in: TemporalContext): Unit = {
    // the hash code depends on the load context, so it is an error if this has been calculated already
    if (_dal$loadContext ne null)
      require(in == _dal$loadContext, s"Cannot change temporal context $in != ${_dal$loadContext}")

    _dal$loadContext = in
  }

  var dal$inlineEntities: InlineEntityHolder = InlineEntityHolder.empty
}

// In Scala 2.13, collections are writeReplaced with scala.collection.generic.DefaultSerializationProxy which then
// readResolves back to the collection. However, readResolve DOES NOT WORK in the presence of cycles such as
// a --> collection --> b --> (same) collection. After deserialization, b will point to the non-readResolved proxy (or
// you'll get a ClassCastException if the field in b cannot point to the proxy).
// We can work around this by putting a wrapper around the collection, but it MUST always be the SAME wrapper for a
// given collection instance. Then we have a --> c --> collection --> b --> (same) c and the problem doesn't occur.
// It matters here because @inlined entities point to the refToEntity map and it points back to them (in a cycle).
final case class InlineEntityHolder(refToEntity: collection.Map[EntityReference, Entity])
object InlineEntityHolder {
  val empty = InlineEntityHolder(Map.empty)
}

/**
 * A UniqueEntityFlavor with frozen identity in the form of a UUID for equality and a fixed hashCode. This is used when
 * serializing unique entity instances so that their identity remains unique but same on the deserializing side
 * (especially important if a unique entity is sent to the grid and then received back on the client)
 */
private[optimus] sealed trait FrozenUniqueEntityFlavorBase extends EntityFlavor with UniqueFlavorBase {
  val uuid: UUID
  val frozenHashCode: Int
  final def entityEquals(e1: Entity, e2: Entity): Boolean = (e1.entityFlavorInternal, e2.entityFlavorInternal) match {
    case (ef1: FrozenUniqueEntityFlavorBase, ef2: FrozenUniqueEntityFlavorBase) => ef1.uuid == ef2.uuid
    case (_, _)                                                                 => false
  }
  final def entityHashCode(e: Entity): Int = frozenHashCode
}

/**
 * A FrozenUniqueEntityFlavor without any DAL fields
 */
private[optimus] final class FrozenUniqueEntityFlavor(val uuid: UUID, val frozenHashCode: Int)
    extends NonDALEntityFlavor
    with FrozenUniqueEntityFlavorBase {
  override def dal$storageInfo: StorageInfo = UniqueStorageInfo
}

/**
 * An unique entity flavor with an added mutable DALEntityFlavor. Also contains frozen uniqueness fields (not needed in
 * all cases, but the hybrid case is rare enough that carrying around sometimes unneeded frozen fields is no big deal)
 */
private[optimus] final class HybridUniqueEntityFlavor(
    override val dalEntityFlavor: DALEntityFlavor,
    val uuid: UUID,
    val frozenHashCode: Int)
    extends HybridEntityFlavor
    with FrozenUniqueEntityFlavorBase {
  override def defaultStorageInfo: StorageInfo = UniqueStorageInfo
}
