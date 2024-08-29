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

import optimus.entity.EntityAuditInfo
import optimus.platform._
import optimus.platform.storable.Entity

trait DALAudit {
  def resolver: ResolverImpl

  object audit {

    /**
     * Get audit information by entity.
     *
     * @param entity
     *   The entity to be queried for audit information. <p><i> This entity must be loaded from DAL. The temporality
     *   information on this entity is not used. This entity serves as a reference only. </i></p>
     * @param qt
     *   The query temporality. <p><i> The version at the specified time will be queried.The current load context
     *   doesn't interfere with the query. The transaction time of this argument can exceed read transaction time of the
     *   current load context. </i></p>
     * @tparam E
     *   The type of the entity. <p><i> The type is not used in query. The query is based on the actual class of the
     *   entity. </i></p>
     * @return
     *   Optional audit information. <p><i> If there {@code qt} is equivalent to {@code
     *   DAL.getTemporalCoordinates(entity)}, this value must not be absent.</i></p>
     */
    @node @scenarioIndependent def get[E <: Entity](entity: E, qt: QueryTemporality.At): Option[EntityAuditInfo] = {
      val eref = entity.dal$entityRef
      if (eref == null)
        throw new IllegalArgumentException(
          "The entity has no entity reference. Please check whether it's a heap entity.")
      val clazz = entity.getClass
      resolver.findEntitiesAuditInfo(eref, clazz.getName, qt).get
    }
  }
}
