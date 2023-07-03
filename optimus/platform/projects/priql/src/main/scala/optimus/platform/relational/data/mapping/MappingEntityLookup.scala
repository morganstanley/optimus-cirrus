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
package optimus.platform.relational.data.mapping

import optimus.platform.Query
import optimus.platform.relational.tree.TypeInfo

import scala.collection.mutable

trait MappingEntityLookup {
  private val cache = new mutable.HashMap[(Class[_], Option[(String, Class[_])]), MappingEntity]

  protected def getEntityImpl(shape: TypeInfo[_]): MappingEntity
  final def getEntity(t: TypeInfo[_]): MappingEntity = {
    val u = TypeInfo.underlying(t)
    cache.getOrElseUpdate(u.clazz -> None, getEntityImpl(u))
  }
  protected def getRelatedEntityImpl(entity: MappingEntity, member: MemberInfo): MappingEntity
  final def getRelatedEntity(entity: MappingEntity, member: MemberInfo): MappingEntity = {
    val relatedType = Query.findShapeType(member.memberType)
    cache.getOrElseUpdate(
      entity.projectedType.clazz -> Some(member.name -> relatedType.clazz),
      getRelatedEntityImpl(entity, member))
  }
}
