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
package optimus.platform.runtime

import optimus.platform.dal.config.DalZoneId

// These runtime properties are shared between the DAL client and broker
private[optimus /*platform*/ ] object SharedRuntimeProperties {
  val DsiIDProperty = "optimus.runtime.uuid"
  val DsiZoneProperty = "optimus.dsi.zone"
  val DsiAppProperty = "optimus.dsi.appid"
  val DsiZonePropertyDefaultValue: DalZoneId = DalZoneId.default
  val DsiCityContinentMappingProperty = "optimus.dsi.mapping"
}
