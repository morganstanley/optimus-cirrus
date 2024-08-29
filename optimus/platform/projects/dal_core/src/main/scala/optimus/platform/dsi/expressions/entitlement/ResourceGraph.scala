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

import optimus.platform.dsi.expressions.Entity
import optimus.platform.dsi.expressions.Expression
import optimus.platform.dsi.expressions.Id

sealed trait ResourceLinkageType
object ResourceLinkageType {
  case object ParentToChild extends ResourceLinkageType
  case object ChildToParent extends ResourceLinkageType
}

final case class ResourceLinkage(destResource: Id, srcResource: Id, fieldName: String, linkageType: ResourceLinkageType)
final case class EntityResource(e: Entity, filters: List[Expression], linkages: List[ResourceLinkage]) {
  def id: Id = e.id
}

class ResourceGraph(val entityResources: Map[Id, EntityResource]) {
  def getEntityResourceByName(className: String): Seq[EntityResource] = {
    entityResources.collect {
      case (id, resource) if resource.e.name == className => resource
    } toSeq
  }

  def getEntityTypeNames(): Set[(String, Seq[String])] =
    entityResources.values.iterator.map(r => (r.e.name, r.e.superClasses)).toSet

  def getEntityById(id: Id): Entity = {
    entityResources
      .get(id)
      .map(_.e)
      .getOrElse(throw new IllegalArgumentException(s"Can't find Entity in ResourceGraph with $id"))
  }
}
