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
package optimus.platform.dal.client.prc

import optimus.dsi.session.EstablishedClientSession
import optimus.dsi.session.ProfileKeyHash
import optimus.platform.dal.client.PrcKeyProvider
import optimus.platform.dal.session.RolesetMode
import optimus.platform.dsi.bitemporal.Command
import optimus.platform.dsi.bitemporal.ReadOnlyCommand

object PrcEligibility {

  def isEligible(ecs: EstablishedClientSession, keyProvider: PrcKeyProvider, cmd: ReadOnlyCommand): Boolean = {
    isSessionEligible(ecs.profileKeyHash, ecs.rolesetMode) && isCmdEligible(keyProvider, cmd)
  }

  private[optimus] def isCmdEligible(keyProvider: PrcKeyProvider, cmd: Command): Boolean = {
    keyProvider.mkKey(cmd).nonEmpty
  }

  private[optimus] def isSessionEligible(profileKeyHash: Option[ProfileKeyHash], rolesetMode: RolesetMode): Boolean = {
    profileKeyHash.nonEmpty && rolesetMode
      .isInstanceOf[RolesetMode.SpecificRoleset]
  }
}
