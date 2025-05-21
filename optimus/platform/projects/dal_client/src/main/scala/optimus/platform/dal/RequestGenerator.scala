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
import msjava.slf4jutils.scalalog.getLogger
import optimus.entity.EntityInfoRegistry
import optimus.platform.TimeInterval
import optimus.platform.dsi.Feature
import optimus.platform.dsi.SupportedFeatures
import optimus.platform.dsi.bitemporal._
import optimus.platform.storable._

import scala.collection.mutable

object RequestGenerator {
  private val log = getLogger[RequestGenerator.type]

  def checkMonoTemporalSupported(serverFeatures: SupportedFeatures, className: String): Unit = {
    require(
      serverFeatures.supports(Feature.SupportMonoTemporal),
      s"DAL broker doesn't support MonoTemporal entities such as ${className}")
  }
}
class RequestGenerator(dsi: DSI) {
  import RequestGenerator._

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

    val monoTemporal = entity.$info.monoTemporal
    if (monoTemporal) checkMonoTemporalSupported(dsi.serverFeatures(), entity.$info.runtimeClass.getName)

    val vt = if (monoTemporal && validTime != TimeInterval.NegInfinity) {
      log.warn(s"Entity is monotemporal: changing valid time from $validTime to negative infinity")
      TimeInterval.NegInfinity
    } else validTime
    val lockToken = if (upsert) {
      None
    } else {
      getLockToken(entity)
    }
    Put(blob.someSlot, lockToken, vt, minAssignableTtOpt, monoTemporal)
  }

  def generateInvalidateAfterRequest(entity: Entity, validTime: Instant) = {
    if (!isPersisted(entity))
      throw new UnsavedEntityUpdateException(entity)

    val dsiInfo = entity.dal$storageInfo match {
      case d: DSIStorageInfo => d
      case _                 => throw new IllegalStateException(s"Cannot invalidate heap entity $entity")
    }

    val monoTemporal = entity.$info.monoTemporal
    if (monoTemporal) checkMonoTemporalSupported(dsi.serverFeatures(), entity.$info.runtimeClass.getName)
    val vt = if (monoTemporal && validTime != TimeInterval.NegInfinity) {
      log.warn(s"Entity is monotemporal: changing valid time from $validTime to negative infinity")
      TimeInterval.NegInfinity
    } else validTime
    InvalidateAfter(
      entity.dal$entityRef,
      dsiInfo.versionedRef,
      dsiInfo.lt,
      vt,
      entity.getClass.getName,
      monoTemporal = monoTemporal)
  }

  def generateInvalidateAfterRequest(ref: EntityReference, validTime: Instant, clazzName: String): InvalidateAfter = {
    val info = EntityInfoRegistry.getClassInfo(clazzName)
    val monoTemporal = info.monoTemporal
    if (monoTemporal) checkMonoTemporalSupported(dsi.serverFeatures(), clazzName)
    val vt = if (monoTemporal && validTime != TimeInterval.NegInfinity) {
      log.warn(s"Entity is monotemporal: changing valid time from $validTime to negative infinity")
      TimeInterval.NegInfinity
    } else validTime
    InvalidateAfter(ref, VersionedReference.Nil, -1, vt, clazzName, monoTemporal)
  }
}
