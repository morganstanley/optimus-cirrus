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
package optimus.platform.temporalSurface.operations

object OperationHelper {
  def coerceItemKey(operation1: TemporalSurfaceQuery, operation2: TemporalSurfaceQuery)(
      key: operation1.ItemKey): operation2.ItemKey = {
    require(operation1 eq operation2)
    key.asInstanceOf[operation2.ItemKey]
  }
  def coerceItemData(operation1: TemporalSurfaceQuery, operation2: TemporalSurfaceQuery)(
      data: operation1.ItemData): operation2.ItemData = {
    require(operation1 eq operation2)
    data.asInstanceOf[operation2.ItemData]
  }
  def coerceResult(operation1: TemporalSurfaceQuery, operation2: TemporalSurfaceQuery)(
      data: operation1.ResultType): operation2.ResultType = {
    require(operation1 eq operation2)
    data.asInstanceOf[operation2.ResultType]
  }

}
