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
package optimus.platform.temporalSurface.cache

import optimus.graph.tracking.TrackingLeafTemporalSurface
import optimus.platform.dal.QueryTemporality
import optimus.platform.storable.EntityReference
import optimus.platform.storable.PersistentEntity
import optimus.platform.temporalSurface.TemporalSurfaceCache
import java.time.Instant
import optimus.platform.reactive.handlers.NotificationUpdate

trait TickingCache { this: TemporalSurfaceCache =>

  /**
   * advance the cache to this time. Any prepareToAdvance with a prior TT can be disregarded
   */
  def timeAdvanced(
      trackingLeaf: TrackingLeafTemporalSurface,
      tt: Instant,
      filter: (EntityReference) => Boolean): Set[EntityReference]

  def hasPotentialUpdate(tt: Instant): Boolean

  def processNotification(update: NotificationUpdate): Unit

  def getPersistentEntity(eRef: EntityReference, time: QueryTemporality.At): Option[PersistentEntity]
}
