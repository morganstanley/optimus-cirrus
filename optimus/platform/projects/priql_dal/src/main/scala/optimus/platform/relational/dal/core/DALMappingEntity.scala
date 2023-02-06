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
package optimus.platform.relational.dal.core

import optimus.platform.dsi.expressions.Expression
import optimus.platform.dsi.expressions.Id
import optimus.platform.relational.data.mapping.MappingEntity
import optimus.platform.relational.data.mapping.MemberInfo
import optimus.platform.relational.data.tree.ColumnInfo
import optimus.platform.relational.tree.TypeInfo

trait DALMappingEntity extends MappingEntity {
  def mappedMembers: List[MemberInfo]

  def getMappedMember(member: String): MemberInfo

  def isMapped(member: String): Boolean

  def getColumnInfo(member: MemberInfo): ColumnInfo

  def isColumn(member: MemberInfo): Boolean

  def getCompoundMembers(member: String): Option[List[MemberInfo]]

  def getDefaultValue(member: String): Option[Any]

  def getOriginalType(member: String): TypeInfo[_]

  def format(id: Id): Expression
}
