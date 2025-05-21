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
package optimus.platform.temporalSurface.impl

import optimus.platform.annotations.nodeSync
import optimus.graph.NodeFuture
import optimus.platform.storable.EntityReference
import optimus.platform.internal.ClassInfo
import optimus.platform.temporalSurface.LeafTemporalSurface
import optimus.platform.temporalSurface.operations.TemporalSurfaceQuery
import optimus.platform.temporalSurface.TemporalSurface
import optimus.platform.temporalSurface.operations.EntityReferenceQueryReason

// TODO (OPTIMUS-50103): remove second parameter and reassess if it needs to be a trait
trait TemporalSurfaceDataAccess {
  @nodeSync def getItemKeys(
      operation: TemporalSurfaceQuery,
      sourceTemporalitySurface: LeafTemporalSurface): Seq[operation.ItemKey]
  def getItemKeys$queued(
      operation: TemporalSurfaceQuery,
      sourceTemporalitySurface: LeafTemporalSurface): NodeFuture[Seq[operation.ItemKey]]

  @nodeSync def getItemData(
      operation: TemporalSurfaceQuery,
      sourceTemporalitySurface: LeafTemporalSurface,
      itemTemporalitySurface: LeafTemporalSurface): Map[operation.ItemKey, operation.ItemData]
  def getItemData$queued(
      operation: TemporalSurfaceQuery,
      sourceTemporalitySurface: LeafTemporalSurface,
      itemTemporalitySurface: LeafTemporalSurface): NodeFuture[Map[operation.ItemKey, operation.ItemData]]

  @nodeSync def getSingleItemData(operation: TemporalSurfaceQuery, sourceTemporalitySurface: LeafTemporalSurface)(
      itemKey: operation.ItemKey): operation.ItemData
  def getSingleItemData$queued(operation: TemporalSurfaceQuery, sourceTemporalitySurface: LeafTemporalSurface)(
      itemKey: operation.ItemKey): NodeFuture[operation.ItemData]

  @nodeSync def getClassInfo(
      operation: TemporalSurfaceQuery,
      surface: TemporalSurface,
      entityRef: EntityReference,
      requireConcrete: Boolean,
      reason: EntityReferenceQueryReason): ClassInfo
  def getClassInfo$queued(
      operation: TemporalSurfaceQuery,
      surface: TemporalSurface,
      entityRef: EntityReference,
      requireConcrete: Boolean,
      reason: EntityReferenceQueryReason): NodeFuture[ClassInfo]

}
