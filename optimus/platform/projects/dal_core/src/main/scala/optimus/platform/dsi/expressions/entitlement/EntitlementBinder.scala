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
package optimus.platform.dsi.expressions.entitlement

import optimus.platform.dsi.expressions._

/**
 * To check entitlement for an expression, we need collect the following info for each entity.
 *   1. Entity filter conditions. All filter conditions will be collected. But at this time, the only operators
 *      supported for the definition of DALResource are ==, != , contains and &&. 2. Entity OneToOne linkage. 3. Entity
 *      OneToMany(Child2Parent) linkage.
 */
object EntitlementBinder {
  def bind(expr: Expression): ResourceGraph = {
    val entities: Map[Id, Entity] = QuerySourceCollector.collectEntities(expr).iterator.map(e => e.id -> e).toMap
    val entityFilters = EntityFilterCollector.collect(entities, expr)
    val collectedLinkages = OneToOneLinkageCollector.collect(expr) ++ OneToManyLinkageCollector.collect(expr)

    val equalEntityGroup = EqualEntityGroupCollector.collect(expr)
    val linkages =
      if (equalEntityGroup.isEmpty()) collectedLinkages.toList
      else replaceEqualLinkages(collectedLinkages, equalEntityGroup.idtoTarget)

    val entityResources = entities.collect {
      case (id, e) if !equalEntityGroup.idtoTarget.contains(id) =>
        equalEntityGroup.targetToIds.get(id).map { idSet =>
          // Merge filters in the same equalEntityGroup and replace entityId with targetId.
          val originalFilters = (idSet + id).flatMap(i => entityFilters.getOrElse(i, Nil))
          val filters: List[Expression] =
            originalFilters.iterator.map(e => PropertyOwnerIdReplacer.replace(e, idSet, id)).toList

          (id, EntityResource(e, filters, linkages.filter(_.destResource == id)))
        } getOrElse ((id, EntityResource(e, entityFilters.getOrElse(id, Nil), linkages.filter(_.destResource == id))))
    }

    new ResourceGraph(entityResources)
  }

  // Replace entity id in the equal entity group with key entity id in the ResourceLinkage.
  private def replaceEqualLinkages(linkages: Set[ResourceLinkage], entityMap: Map[Id, Id]) = {
    val replacedLinkages = linkages.flatMap {
      case linkage @ ResourceLinkage(destResource, srcResource, fieldName, linkageType) =>
        val dest = entityMap.getOrElse(destResource, destResource)
        val src = entityMap.getOrElse(srcResource, srcResource)
        if (dest == src) None
        else if (dest == destResource && src == srcResource) Some(linkage)
        else Some(ResourceLinkage(dest, src, fieldName, linkageType))
    }
    replacedLinkages.toList
  }
}
