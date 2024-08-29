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
package optimus.platform.dsi.bitemporal

import optimus.platform.dal.config.DalAppId
import optimus.platform.dal.config.DalZoneId

/*
 * In future if we want to add more information for client identification, it can go in here.
 */
final case class ClientAppIdentifier(zoneId: DalZoneId, appId: DalAppId)
object ClientAppIdentifier {
  val Unknown = ClientAppIdentifier(DalZoneId.unknown, DalAppId.unknown)
}

// Wrapping type to indicate that the client application identifier has been resolved from the Application entity if
// possible
final case class ResolvedClientAppIdentifier(underlying: ClientAppIdentifier) extends AnyVal {
  def appId = underlying.appId
  def zoneId = underlying.zoneId
}

object ResolvedClientAppIdentifier {
  val Unknown = ResolvedClientAppIdentifier(ClientAppIdentifier.Unknown)
}
