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
import optimus.platform.dsi.bitemporal._
import optimus.platform.storable.Entity

import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait DALEntitlements {
  def resolver: ResolverImpl

  object entitlements {
    @async def getAvailableRoles(user: String): Try[Set[String]] = {
      if (user == null) Failure(new IllegalArgumentException("user cannot be null"))
      else {
        val cmd = RoleMembershipQuery(user)
        val results = resolver.executor.executeQuery(resolver.dsi, cmd)
        results match {
          case RoleMembershipResult(ids) => Success(ids)
          case ErrorResult(ex, _)        => Failure(ex)
          case o                         => Failure(new GeneralDALException(s"Unexpected result type: $o"))
        }
      }
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
