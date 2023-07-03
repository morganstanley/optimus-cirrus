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
package optimus.platform.relational.data.translation

import optimus.platform.relational.data.mapping.MemberInfo
import optimus.platform.relational.data.mapping.QueryMapper
import optimus.platform.relational.data.tree.DbEntityElement
import optimus.platform.relational.data.tree.DbQueryTreeVisitor
import optimus.platform.relational.data.tree.JoinElement
import optimus.platform.relational.data.tree.JoinType
import optimus.platform.relational.data.tree.ProjectionElement
import optimus.platform.relational.data.tree.SelectElement
import optimus.platform.relational.tree.MemberElement
import optimus.platform.relational.tree.RelationElement

class RelationshipBinder private (mapper: QueryMapper) extends DbQueryTreeVisitor {
  private val mapping = mapper.mapping
  private val language = mapper.translator.language
  private var currentFrom: RelationElement = _

  protected override def handleSelect(s: SelectElement): RelationElement = {
    val savedCurrentFrom = currentFrom
    currentFrom = visitSource(s.from)
    try {
      val where = visitElement(s.where)
      val orderBy = visitOrderBy(s.orderBy)
      val groupBy = visitElementList(s.groupBy)
      val skip = visitElement(s.skip)
      val take = visitElement(s.take)
      val columns = visitColumnDeclarations(s.columns)
      updateSelect(s, currentFrom, where, orderBy, groupBy, skip, take, columns)
    } finally {
      currentFrom = savedCurrentFrom
    }
  }

  protected override def handleMemberRef(m: MemberElement): RelationElement = {
    val src = visitElement(m.instanceProvider)
    val memberInfo = MemberInfo(m.member.declaringType, m.member.name, m.rowTypeInfo)
    src match {
      case e: DbEntityElement if mapping.isRelationship(e.entity, memberInfo) =>
        val proj = visitElement(mapper.getMemberElement(src, e.entity, memberInfo)).asInstanceOf[ProjectionElement]
        if ((currentFrom ne null) && mapping.isSingletonRelationship(e.entity, memberInfo)) {
          val newFrom = new JoinElement(JoinType.OuterApply, currentFrom, proj.select, null)
          currentFrom = newFrom
          proj.projector
        } else proj

      case _ =>
        mapper.binder.bindMember(src, memberInfo) match {
          case me: MemberElement if (me.instanceProvider eq m.instanceProvider) && me.member.name == m.member.name => m
          case x                                                                                                   => x
        }
    }
  }
}

object RelationshipBinder {
  def bind(mapper: QueryMapper, e: RelationElement): RelationElement = {
    val elem = new RelationshipBinder(mapper).visitElement(e)
    SingletonProjectionRewriter.rewrite(mapper, elem)
  }
}
