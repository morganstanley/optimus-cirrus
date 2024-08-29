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
package optimus.platform.dal

import java.time.Instant

import msjava.base.util.uuid.MSUuid
import optimus.platform.dsi.bitemporal._
import optimus.platform.storable._

import scala.collection.mutable

class RequestGenerator(dsi: DSI) {
  private def hasUnpersistedChanges(entity: Entity, validTime: Instant): Boolean = {
    true
  }

  private def isPersisted(entity: Entity): Boolean = entity.dal$storageInfo match {
    case si if si == AppliedStorageInfo || si == UniqueStorageInfo => false
    case _: DSIStorageInfo                                         => true
    case u => throw new IllegalArgumentException(s"Unknown storage info type ${u.getClass.getName} saving to ${dsi}")
  }

  def nonRecursiveGenerateRequestsForPut(
      entity: Entity,
      entityRefs: mutable.Map[Entity, EntityReference],
      lockTokens: collection.Map[Entity, Long],
      validTime: Instant,
      upsert: Boolean,
      cmid: Option[MSUuid],
      minAssignableTtOpt: Option[Instant]): Put = {

    def getLockToken(entity: Entity): Option[Long] = entity.dal$storageInfo.lockToken match {
      case lt: Some[Long] => lt
      case None           => lockTokens.get(entity) orElse Some(0)
    }

    val blob = EntitySerializer.serialize(entity, entityRefs, cmid, true)
    require(
      blob.entities.size == 1,
      "Multi-schema writes are only supported in transaction/event blocks, not in persist blocks.")
    val lockToken = if (upsert) {
      None
    } else {
      getLockToken(entity)
    }
    Put(blob.someSlot, lockToken, validTime, minAssignableTtOpt)
  }

  def generateInvalidateAfterRequest(entity: Entity, validTime: Instant) = {
    if (!isPersisted(entity))
      throw new UnsavedEntityUpdateException(entity)

    val dsiInfo = entity.dal$storageInfo match {
      case d: DSIStorageInfo => d
      case _                 => throw new IllegalStateException(s"Cannot invalidate heap entity $entity")
    }

    InvalidateAfter(entity.dal$entityRef, dsiInfo.versionedRef, dsiInfo.lt, validTime, entity.getClass.getName)
  }

  def generateInvalidateAfterRequest(ref: EntityReference, validTime: Instant, clazzName: String): InvalidateAfter = {
    InvalidateAfter(ref, VersionedReference.Nil, -1, validTime, clazzName)
  }
}
