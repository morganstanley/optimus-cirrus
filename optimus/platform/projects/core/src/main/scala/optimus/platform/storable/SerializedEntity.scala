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

import optimus.entity.EntityLinkageProperty
import optimus.platform.{TimeInterval, ValidTimeInterval}
import optimus.platform.annotations.parallelizable

import scala.collection.immutable

object SerializedEntity {
  type TypeRef = String
  type LinkageMap = Map[EntityLinkageProperty, Set[SerializedEntity.EntityLinkage]]

  trait LinkageEntry[T] {
    val link: T
  }
  final case class EntityLinkage(permRef: EntityReference) extends LinkageEntry[EntityReference] {
    val link = permRef
  }

  // We need a placeholder for PersistentEntity.Null.
  private[storable] val Null = SerializedEntity(EntityReference(Array.empty[Byte]), None, "", Map.empty[String, Any])

  private[optimus] def adjustLockTokenWithReadTxTime(lockToken: Long, readTxTimeLockToken: Option[Long]): Long = {
    // [OPTIMUS-5736] In order to stop updates based on Outdated versions of an entity, the entity lock token
    // is changed to NegInfinity if the outdated version is read back by the client. The readTxTimeLockToken is sent when reading the entities.
    val ltOpt = readTxTimeLockToken.map { t =>
      if (t >= lockToken)
        lockToken
      else
        Long.MinValue
    }
    ltOpt getOrElse lockToken
  }
}

private[optimus] abstract class SerializedStorable extends Serializable {
  def className: SerializedEntity.TypeRef
  def properties: Map[String, Any]
  def keys: Seq[SerializedKey]
  def types: Seq[SerializedEntity.TypeRef]
  def slot: Int
  def storableRef: StorableReference
}

@parallelizable
final case class SerializedEntity(
    entityRef: EntityReference,
    cmid: Option[CmReference],
    className: SerializedEntity.TypeRef,
    properties: Map[String, Any],
    keys: Seq[SerializedKey] = Seq.empty,
    types: Seq[SerializedEntity.TypeRef] = Seq.empty,
    inlinedEntities: Seq[SerializedEntity] = Nil,
    /*
     * NB: we use a Option[Map] here to differentiate 3 different cases:
     * (1) SerializedEntity has no childToParent linkages (=None)
     * (2) SerializedEntity has linkages but they haven't been loaded yet (= Some(Map.empty))
     * (3) SerializedEntity has linkages and there is data (=Some(linkages))
     *
     * See also optimus.dsi.base.EntityLinkageHelper.populateLinkage().
     */
    linkages: Option[SerializedEntity.LinkageMap] = None,
    slot: Int = 0)
    extends SerializedStorable {

  override def storableRef: StorableReference = entityRef

  private[optimus] def toPrettyString(indent: String = "\n"): String = {
    val nextIndent = indent + "    "
    s"""${indent}entityRef: $entityRef
       |${indent}cmid: $cmid
       |${indent}className: $className
       |${indent}properties:
       |${nextIndent}${properties.mkString(nextIndent)}
       |${indent}keys:
       |${nextIndent}${keys.mkString(nextIndent)}
       |${indent}types:
       |${nextIndent}${types.mkString(nextIndent)}
       |${indent}inlinedEntities:
       |${nextIndent}${inlinedEntities.map(_.toPrettyString(nextIndent)).mkString(nextIndent)}
       |${indent}linkages:
       |${nextIndent}$linkages
       |""".stripMargin
  }
  def copySerialized(
      entityRef: EntityReference = entityRef,
      cmid: Option[CmReference] = cmid,
      className: SerializedEntity.TypeRef = className,
      properties: Map[String, Any] = properties,
      keys: Seq[SerializedKey] = keys,
      types: Seq[SerializedEntity.TypeRef] = types,
      inlinedEntities: Seq[SerializedEntity] = inlinedEntities,
      linkages: Option[Map[EntityLinkageProperty, Set[SerializedEntity.EntityLinkage]]] = linkages,
      slot: Int = slot) =
    new SerializedEntity(entityRef, cmid, className, properties, keys, types, inlinedEntities, linkages, slot)

  def toPersistentEntity(
      versionedRef: VersionedReference,
      lockToken: Long,
      vtInterval: ValidTimeInterval,
      txInterval: TimeInterval,
      readTxTimeLockToken: Option[Long] = None): PersistentEntity = {

    val lt = SerializedEntity.adjustLockTokenWithReadTxTime(lockToken, readTxTimeLockToken)
    PersistentEntity(this, versionedRef, lt, vtInterval, txInterval)
  }

  def getKey: Option[SerializedKey] = keys.find(_.isKey)
}

final case class MultiSlotSerializedEntity private (entities: immutable.SortedSet[SerializedEntity]) {
  // INVARIANT: entities is non-empty
  require(entities.nonEmpty, "MultiSlotSerializedEntities must contain at least one SerializedEntity")

  val someSlot = entities.head
  private val atOtherSlots = entities.tail

  lazy val notAtSlot0 = if (someSlot.slot == 0) entities.tail else entities

  // INVARIANTS:
  // ents is non-empty (this is enforced by MultiSlotSerializedEntity)
  // all ents have the same entityRef, className, types, keys, linkages
  // all ents have different slot
  require(
    atOtherSlots.forall { se =>
      se.className == someSlot.className &&
      se.types == someSlot.types &&
      se.keys == someSlot.keys &&
      se.linkages == someSlot.linkages &&
      se.cmid == someSlot.cmid
    },
    "Multi-schema writes only supported where all entities have the same className, types, keys, linkages, and CMID."
  )
  require(
    entities.map(_.slot).toSet.size == entities.size,
    "Schema numbers should be different for all entities in a multi-schema write.")
  lazy val entityRef = someSlot.entityRef
  lazy val className = someSlot.className
  lazy val linkages = someSlot.linkages
  lazy val types = someSlot.types
  lazy val keys = someSlot.keys
  lazy val cmid = someSlot.cmid
  lazy val getKey = someSlot.getKey
}

object MultiSlotSerializedEntity {
  private implicit val entityOrdering: Ordering[SerializedEntity] = Ordering.by[SerializedEntity, Int](se => se.slot)
  def apply(entities: Seq[SerializedEntity]): MultiSlotSerializedEntity = {
    val sorted = immutable.SortedSet(entities.toSeq: _*)
    new MultiSlotSerializedEntity(sorted)
  }
}

final case class EntityMetadata(entityRef: EntityReference, className: String, types: Seq[SerializedEntity.TypeRef])
