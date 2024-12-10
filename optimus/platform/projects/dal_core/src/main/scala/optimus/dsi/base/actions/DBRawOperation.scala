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

import java.util.Arrays

import net.iharder.Base64
import optimus.platform.dsi.bitemporal._
import optimus.platform.storable.{EntityTimeSliceReference, SerializedAppEvent, SerializedKey, StorableReference}
import DBRawObjectTypename._
import optimus.dsi.base.ContainerName
import optimus.dsi.base.RefHolder
import optimus.dsi.base.UnknownContainerName
import optimus.dsi.shard.DefaultShardKey
import optimus.dsi.shard.ShardKey
import optimus.dsi.shard.ShardKeySupportedByAdminScript

final case class ShardKeyAndValue[T](key: ShardKey[T] with ShardKeySupportedByAdminScript, value: T)
    extends Serializable

sealed trait DBRawOperation {
  val container: ContainerName
  def shardKeyAndValue: ShardKeyAndValue[_]
}
object DBRawOperation {
  // TODO (OPTIMUS-34635): pass correct shard key and remove the default
  val defaultShardKeyAndValue = ShardKeyAndValue(DefaultShardKey(), None)
  sealed trait RawOpName { def name: String }
  object Insert extends RawOpName {
    def apply(
        container: String,
        dataToInsert: DBRawObject,
        shardKeyAndValue: ShardKeyAndValue[_] = defaultShardKeyAndValue): Insert =
      new Insert(UnknownContainerName(container), dataToInsert, shardKeyAndValue)
    override val name: String = "Insert"
  }
  final case class Insert(container: ContainerName, dataToInsert: DBRawObject, shardKeyAndValue: ShardKeyAndValue[_])
      extends DBRawOperation
  object Update extends RawOpName {
    def apply(
        container: String,
        query: DBRawQuery,
        dataToUpdate: Map[String, DBRawUpdateOps],
        shardKeyAndValue: ShardKeyAndValue[_] = defaultShardKeyAndValue): Update =
      new Update(UnknownContainerName(container), query, dataToUpdate, shardKeyAndValue)
    override val name: String = "Update"
  }
  final case class Update(
      container: ContainerName,
      query: DBRawQuery,
      dataToUpdate: Map[String, DBRawUpdateOps],
      shardKeyAndValue: ShardKeyAndValue[_])
      extends DBRawOperation
  object Upsert extends RawOpName {
    def apply(
        container: String,
        query: DBRawQuery,
        dataToUpdate: Map[String, SetField],
        shardKeyAndValue: ShardKeyAndValue[_] = defaultShardKeyAndValue): Upsert =
      new Upsert(UnknownContainerName(container), query, dataToUpdate, shardKeyAndValue)
    override val name: String = "Upsert"
  }
  final case class Upsert(
      container: ContainerName,
      query: DBRawQuery,
      dataToUpdate: Map[String, SetField],
      shardKeyAndValue: ShardKeyAndValue[_])
      extends DBRawOperation
  object Delete extends RawOpName {
    def apply(
        container: String,
        query: DBRawQuery,
        shardKeyAndValue: ShardKeyAndValue[_] = defaultShardKeyAndValue): Delete =
      new Delete(UnknownContainerName(container), query, shardKeyAndValue)
    override val name: String = "Delete"
  }
  final case class Delete(container: ContainerName, query: DBRawQuery, shardKeyAndValue: ShardKeyAndValue[_])
      extends DBRawOperation
}

// DBRawQuery
sealed trait DBRawQuery
final case class DBRawIdentity(ref: StorableReference) extends DBRawQuery
object DBRawTimesliceIdentity {
  def apply(id: (StorableReference, Int)): DBRawTimesliceIdentity = {
    val (ref, tsnum) = id
    apply(ref, tsnum)
  }
}
final case class DBRawTimesliceIdentity(ref: StorableReference, tsnum: Int) extends DBRawQuery
object DBRawUidxTimesliceIdentity {
  def apply(id: (RefHolder, Int)): DBRawUidxTimesliceIdentity = {
    val (hashHolder, tsnum) = id
    apply(hashHolder, tsnum)
  }
}
final case class DBRawUidxTimesliceIdentity(hash: RefHolder, tsnum: Int) extends DBRawQuery
final case class DBRawStringId(id: String) extends DBRawQuery
final case class DBRawOpaqueId(bytes: Array[Byte]) extends DBRawQuery {
  override def hashCode: Int = Arrays.hashCode(bytes)

  override def equals(rhs: Any): Boolean = rhs match {
    case r: DBRawOpaqueId => Arrays.equals(bytes, r.bytes)
    case _                => false
  }

  override def toString = Base64.encodeBytes(bytes)
}

// DBRawValueType
sealed trait DBRawValue
final case class DBRawBoolean(b: Boolean) extends DBRawValue
final case class DBRawInt(i: Int) extends DBRawValue
final case class DBRawString(s: String) extends DBRawValue
final case class DBRawLong(l: Long) extends DBRawValue
final case class DBRawReference(r: StorableReference) extends DBRawValue
/*
 * this is specifically used for unique index migration which only
 * needs key property name and their value is either ignored or
 * assigned placeholder value, please check before using this anywhere else
 */
final case class DBRawSerializedKey(sk: SerializedKey) extends DBRawValue
final case class DBRawLinkedTypes(lt: LinkedTypes) extends DBRawValue
final case class DBRawSequence(seq: Seq[DBRawValue]) extends DBRawValue

sealed trait DBRawUpdateOps
case object ClearField extends DBRawUpdateOps
final case class SetField(value: DBRawValue) extends DBRawUpdateOps
final case class AddToSet(value: DBRawValue) extends DBRawUpdateOps

// DBRawObjectType
sealed trait DBRawObject {
  def typename: Int
}
object DBRawObjectTypename {
  val DBRawUniqueIndexGroupingT = 1
  val DBRawUniqueIndexTimeSliceT = 2
  val DBRawIndexEntryT = 3
  val DBRawBusinessEventGroupingT = 4
  val DBRawLinkageEntryT = 6
  val DBRawAppEventT = 7
  val DBRawEntityGroupingT = 8
  val DBRawEntityTimeSliceT = 9
  val DBRawKeyGroupingT = 10
  val DBRawReferenceHolderT = 11
  val DBRawBackfilledTombstoneT = 12
}
final case class DBRawUniqueIndexGrouping(g: UniqueIndexGrouping) extends DBRawObject {
  def typename = DBRawUniqueIndexGroupingT
}
final case class DBRawUniqueIndexTimeSlice(ts: UniqueIndexTimeSlice, classNameOpt: Option[String]) extends DBRawObject {
  def typename = DBRawUniqueIndexTimeSliceT
}
final case class DBRawIndexEntry(ie: IndexEntry) extends DBRawObject {
  def typename = DBRawIndexEntryT
}
// TODO (OPTIMUS-65890): Added DBRawRegisteredIndexEntry and related functionality similar to DBRawIndexEntry
final case class DBRawBusinessEventGrouping(g: BusinessEventGrouping) extends DBRawObject {
  def typename = DBRawBusinessEventGroupingT
}
final case class DBRawLinkageEntry(le: LinkageEntry) extends DBRawObject {
  def typename = DBRawLinkageEntryT
}
final case class DBRawAppEvent(ae: SerializedAppEvent) extends DBRawObject {
  def typename = DBRawAppEventT
}
final case class DBRawEntityGrouping(et: EntityGrouping) extends DBRawObject {
  def typename = DBRawEntityGroupingT
}
final case class DBRawEntityTimeSlice(et: EntityTimeSlice, classNameOpt: Option[String]) extends DBRawObject {
  def typename = DBRawEntityTimeSliceT
}
final case class DBRawKeyGrouping(key: KeyGrouping) extends DBRawObject {
  def typename = DBRawKeyGroupingT
}
final case class DBRawReferenceHolder(eref: StorableReference) extends DBRawObject {
  def typename = DBRawReferenceHolderT
}

final case class DBRawBackfilledTombstone(etsr: EntityTimeSliceReference) extends DBRawObject {
  def typename = DBRawBackfilledTombstoneT
}
