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
package optimus.platform.dsi.prc.session

import java.time.Instant

import optimus.dsi.session.EstablishedClientSession
import optimus.platform.dal.session.RolesetMode
import optimus.platform.dal.session.RolesetMode.SpecificRoleset
import optimus.platform.dsi.bitemporal.Context
import optimus.platform.dsi.bitemporal.DefaultContext

final case class PrcClientSessionInfo(
    realId: String,
    effectiveId: Option[String],
    roles: Set[String],
    establishmentTime: Instant,
    context: Context)

object PrcClientSessionInfo {
  val Unknown =
    new PrcClientSessionInfo("", None, Set.empty, patch.MilliInstant.now, DefaultContext)

  def apply(establishedSession: EstablishedClientSession, context: Context): PrcClientSessionInfo = {
    require(
      establishedSession.rolesetMode.isInstanceOf[SpecificRoleset],
      "Prc session can only be established for specified role sets")
    apply(
      establishedSession.establishingCommand.realId,
      establishedSession.establishingCommand.effectiveId,
      establishedSession.rolesetMode.asInstanceOf[RolesetMode.SpecificRoleset],
      establishedSession.establishmentTime,
      context
    )
  }

  def apply(
      realId: String,
      effectiveId: Option[String],
      rolesetMode: RolesetMode.SpecificRoleset,
      establishmentTime: Instant,
      context: Context): PrcClientSessionInfo = {
    apply(realId, effectiveId, rolesetMode.underlying, establishmentTime, context)
  }
}
