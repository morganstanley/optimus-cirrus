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
package optimus.dsi.shard

import java.time.Instant

import optimus.dsi.base.ObliterateAction.RemoveKey
import optimus.dsi.base.ObliterateAction.RemoveUniqueIndexGrouping
import optimus.platform.TimeInterval
import optimus.platform.dsi.bitemporal._
import optimus.platform.storable.BusinessEventReference
import optimus.platform.storable.EntityReference
import optimus.platform.storable.SerializedAppEvent
import optimus.platform.storable.SerializedEntity
import optimus.platform.storable.SerializedKey
import optimus.platform.storable.VersionedReference

object ShardKeySupportedByAdminScript {
  def apply(id: Int): ShardKey[_] with ShardKeySupportedByAdminScript = {
    id match {
      case 0                          => new DefaultShardKey[Any]()
      case EntityReferenceShardKey.id => EntityReferenceShardKey
      case TypeNameShardKey.id        => TypeNameShardKey
      case _ => throw new IllegalArgumentException(s"Shard key is not supported by admin script $id")
    }
  }
}

/**
 * ShardKey classes extending this trait are supported by admin script
 */
sealed trait ShardKeySupportedByAdminScript
sealed trait ShardKey[T] extends Serializable {
  // id is used during serialization and deserialization of shard key
  def id: Int
}
final case class DefaultShardKey[T]() extends ShardKey[T] with ShardKeySupportedByAdminScript { val id: Int = 0 }
case object EntityReferenceShardKey extends ShardKey[EntityReference] with ShardKeySupportedByAdminScript {
  val id: Int = 1
}
case object TypeNameShardKey extends ShardKey[SerializedEntity.TypeRef] with ShardKeySupportedByAdminScript {
  val id: Int = 2
}
case object IndexEntryShardKey extends ShardKey[IndexEntry] { val id: Int = 3 }
case object KeyGroupingShardKey extends ShardKey[KeyGrouping] { val id: Int = 4 }
case object EntityTimesliceShardKey extends ShardKey[EntityTimeSlice] { val id: Int = 5 }
case object AppEventShardKey extends ShardKey[SerializedAppEvent] { val id: Int = 6 }
case object EventGroupingShardKey extends ShardKey[BusinessEventGrouping] { val id: Int = 7 }
case object EventTimesliceShardKey extends ShardKey[BusinessEventTimeSlice] { val id: Int = 8 }
case object LinkageEntryShardKey extends ShardKey[LinkageEntry] { val id: Int = 9 }
case object EventKeyShardKey extends ShardKey[BusinessEventKey] { val id: Int = 10 }
case object UniqueIndexGroupingShardKey extends ShardKey[UniqueIndexGrouping] { val id: Int = 11 }
case object UniqueIndexTimesliceShardKey extends ShardKey[UniqueIndexTimeSlice] { val id: Int = 12 }
case object BlobTombstoneShardKey extends ShardKey[TombstoneBlobEntry] { val id: Int = 13 }
case object EntityKeyShardKey extends ShardKey[SerializedKey] { val id: Int = 14 }
case object OblRemoveKeyShardKey extends ShardKey[RemoveKey] { val id: Int = 15 }
case object OblRemoveUnqIdxGroupingShardKey extends ShardKey[RemoveUniqueIndexGrouping] { val id: Int = 16 }
case object EntityGroupingShardKey extends ShardKey[EntityGrouping] { val id: Int = 17 }
case object BusinessEventReferenceShardKey extends ShardKey[BusinessEventReference] { val id: Int = 18 }
case object EventIndexEntryMapShardKey extends ShardKey[(AnyRef, EventIndexEntry)] { val id: Int = 19 }
case object EventIndexEntryShardKey extends ShardKey[EventIndexEntry] { val id: Int = 20 }
case object ErefVrefShardKey extends ShardKey[(VersionedReference, EntityReference)] { val id: Int = 21 }
case object TxTimeShardKey extends ShardKey[Instant] with ShardKeySupportedByAdminScript { val id: Int = 22 }
case object TxTimeIntervalShardKey extends ShardKey[TimeInterval] with ShardKeySupportedByAdminScript {
  val id: Int = 23
}
