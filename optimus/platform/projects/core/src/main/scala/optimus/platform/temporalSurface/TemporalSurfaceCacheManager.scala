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
package optimus.platform.temporalSurface

import optimus.platform.temporalSurface.operations.TemporalSurfaceQuery

trait TemporalSurfaceCache {
  def getItemKeys(operation: TemporalSurfaceQuery)(
      sourceTemporality: operation.TemporalityType): Option[Seq[operation.ItemKey]]
  def getItemData(operation: TemporalSurfaceQuery)(
      sourceTemporality: operation.TemporalityType,
      itemTemporality: operation.TemporalityType): Option[Map[operation.ItemKey, operation.ItemData]]
  def getSingleItemData(operation: TemporalSurfaceQuery)(
      temporality: operation.TemporalityType,
      key: operation.ItemKey): Option[operation.ItemData]
}
