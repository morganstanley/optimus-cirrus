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

import optimus.dsi.session.ClientSessionInfo
import optimus.platform._
import optimus.platform.dsi.Feature.RoleMembershipQueryWithClientSessionInfo
import optimus.platform.dsi.bitemporal._
import optimus.platform.storable.Entity

import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait DALEntitlements {
  def resolver: ResolverImpl

  object entitlements {

    /**
     * Get all roles the user has membership at tx time determined by DAL broker.
     *
     * The user issuing this query must meet one of the following conditions:
     *    - same as the `user` parameter
     *    - is allow listed by one of the following allow lists:
     *       - AllowListedEstablishersHolder
     *       - MatrixOnBehalfAllowList
     *       - SoaOnBehalfAllowList
     *       - TrustedInternalOnBehalfAllowList
     *    - or must have read entitlements for Basic Role or Role
     *
     * @param user The user id to query role membership
     * @return all role names user has permission to, including default role and TEA roles.
     */
    @async def getAvailableRoles(user: String): Try[Set[String]] = {
      val cmd = RoleMembershipQuery(user)
      queryRoleMembership(cmd)
    }

    @async private def queryRoleMembership(cmd: RoleMembershipQuery) = {
      if (cmd.realId == null) Failure(new IllegalArgumentException("user cannot be null"))
      else {
        val results = resolver.executor.executeQuery(resolver.dsi, cmd)
        results match {
          case RoleMembershipResult(ids) => Success(ids)
          case ErrorResult(ex, _)        => Failure(ex)
          case o                         => Failure(new GeneralDALException(s"Unexpected result type: $o"))
        }
      }
    }

    /**
     * Query Role Membership for given user after validate the client session info.
     *
     * The following validation should be performed by DAL:
     *   - `user` is same as:
     *      - `clientSessionInfo.effectiveId` if it is not empty
     *      - `clientSessionInfo.realId` if `effectiveId` is empty
     *   -  `clientSessionInfo`:
     *      - `onBehalfTokenType` must be `Default`
     *      - `establishmentTime` must present
     *      - `onBehalfSessionToken` must present and valid:
     *         - its effective id equals to `user` parameter
     *         - its establishment time equals to  `clientSessionInfo.establishmentTime`
     *   - the user issuing this query must have the permission to query RoleMembership for `user`,
     *     same as {{{def getAvailableRoles(user: String)}}}
     *
     * @param user the user id to query role membership
     * @param clientSessionInfo client session information.
     * @return All role names the `user` is a member of at tx time of `clientSessionInfo.establishmentTIme`, including
     *         default role and TEA roles.
     */
    @async def getAvailableRoles(user: String, clientSessionInfo: ClientSessionInfo): Try[Set[String]] = {
      if (!resolver.serverFeatures.supports(RoleMembershipQueryWithClientSessionInfo)) {
        throw new DSISpecificError(s"RoleMembershipQuery with ClientSessionInfo is not supported by DAL server.")
      }
      val cmd = RoleMembershipQuery(user, Some(clientSessionInfo))
      queryRoleMembership(cmd)
    }

    @async def availableRoles: Set[String] = {
      val roles: Try[Set[String]] = getAvailableRoles(System.getProperty("user.name"))
      roles.get
    }

    @async def canReadAny[E <: Entity](implicit m: Manifest[E]): Boolean = resolver.canPerformAction(EntityAction.Read)
    @async def canCreateAny[E <: Entity](implicit m: Manifest[E]): Boolean =
      resolver.canPerformAction(EntityAction.Create)
    @async def canUpdateAny[E <: Entity](implicit m: Manifest[E]): Boolean =
      resolver.canPerformAction(EntityAction.Update)
    @async def canInvalidateAny[E <: Entity](implicit m: Manifest[E]): Boolean =
      resolver.canPerformAction(EntityAction.Invalidate)

    @async def canPerformActionOnEntity(entityName: String, action: EntityAction): Boolean = {
      resolver.canPerformActionOnEntity(entityName, action)
    }
  }
}
