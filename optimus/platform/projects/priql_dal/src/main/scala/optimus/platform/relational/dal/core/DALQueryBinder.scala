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

import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.data.mapping.MemberInfo
import optimus.platform.relational.data.mapping.QueryBinder
import optimus.platform.relational.data.tree.ColumnElement
import optimus.platform.relational.data.tree.DALHeapEntityElement
import optimus.platform.relational.tree.RelationElement
import optimus.platform.storable.EntityReference

trait DALQueryBinder extends QueryBinder {
  override def bindMember(source: RelationElement, member: MemberInfo): RelationElement = {
    source match {
      case e: DALHeapEntityElement =>
        e.memberNames
          .zip(e.members)
          .collectFirst {
            case (name, e) if name == member.name => e
          }
          .getOrElse(makeMemberAccess(source, member))
      case c: ColumnElement if member.name == DALProvider.EntityRef && member.memberType <:< classOf[EntityReference] =>
        c.columnInfo match {
          case i: IndexColumnInfo =>
            new ColumnElement(member.memberType, c.alias, c.name, IndexColumnInfo.asEntityRefColumnInfo(i))
          case _ =>
            super.bindMember(source, member)
        }
      case _ => super.bindMember(source, member)
    }
  }
}
