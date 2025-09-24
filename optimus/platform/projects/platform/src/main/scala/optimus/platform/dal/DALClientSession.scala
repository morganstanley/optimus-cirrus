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

import optimus.platform._
import optimus.platform.dal.session.RolesetMode

import java.time.Instant

trait DALClientSession {

  def resolver: DALEntityResolver

  object session {

    @async private def getClientSession = resolver.getSession()

    @async def effectiveUserId: String = getClientSession.effectiveId

    @async private[optimus] def encryptedSessionToken: Option[Vector[Byte]] = getClientSession.encryptedSessionToken

    @async private[optimus] def establishmentTime: Instant = getClientSession.establishmentTime

    @async private[optimus] def rolesetMode: RolesetMode.Established = getClientSession.rolesetMode

  }

}
