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

import optimus.platform.NoKey
import optimus.platform.relational.data.mapping.MemberInfo
import optimus.platform.relational.data.mapping.QueryMapper
import optimus.platform.relational.data.tree._
import optimus.platform.relational.tree.MemberElement
import optimus.platform.relational.tree.RelationElement

class SingletonProjectionRewriter private (mapper: QueryMapper) extends DbQueryTreeVisitor {
  private val mapping = mapper.mapping
  private val language = mapper.translator.language
  import SingletonProjectionRewriter._

  private var isTopLevel = true
  private var currentSelect: SelectElement = null

  protected override def handleProjection(proj: ProjectionElement): RelationElement = {
    if (isTopLevel) {
      isTopLevel = false
      currentSelect = proj.select
      val projector = visitElement(proj.projector)
      updateProjection(proj, currentSelect, projector, proj.aggregator)
    } else {
      if (proj.isSingleton && canJoinOnServer(currentSelect)) {
        val newAlias = new TableAlias
        currentSelect = addRedundantSelect(currentSelect, newAlias)
        val source = ColumnMapper.map(proj.select, newAlias, currentSelect.alias).asInstanceOf[SelectElement]
        val pe =
          new ProjectionElement(source, proj.projector, NoKey, proj.keyPolicy, null, entitledOnly = proj.entitledOnly)
        val pc = ColumnProjector.projectColumns(
          language,
          pe.projector,
          currentSelect.columns,
          currentSelect.alias,
          newAlias,
          proj.select.alias)
        val join = new JoinElement(JoinType.OuterApply, currentSelect.from, pe.select, null)
        currentSelect =
          new SelectElement(currentSelect.alias, pc.columns, join, null, null, null, null, null, false, false)
        visitElement(pc.projector)
      } else {
        val saveTop = isTopLevel
        val saveSelect = currentSelect
        isTopLevel = true
        currentSelect = null
        val result = handleProjection(proj)
        isTopLevel = saveTop
        currentSelect = saveSelect
        result
      }
    }
  }

  protected override def handleMemberRef(m: MemberElement): RelationElement = {
    val src = visitElement(m.instanceProvider)
    val memberInfo = MemberInfo(m.member.declaringType, m.member.name, m.rowTypeInfo)
    src match {
      case e: DbEntityElement if mapping.isRelationship(e.entity, memberInfo) =>
        visitElement(mapper.getMemberElement(src, e.entity, memberInfo))

      case _ =>
        mapper.binder.bindMember(src, memberInfo) match {
          case me: MemberElement if (me.instanceProvider eq m.instanceProvider) && me.member.name == m.member.name => m
          case x                                                                                                   => x
        }
    }
  }

  protected override def handleSubquery(subquery: SubqueryElement): RelationElement = {
    subquery
  }

  private def canJoinOnServer(sel: SelectElement): Boolean = {
    !sel.isDistinct && (sel.groupBy == null || sel.groupBy.isEmpty) && !AggregateChecker.hasAggregates(sel)
  }

  private def addRedundantSelect(sel: SelectElement, newAlias: TableAlias): SelectElement = {
    val newColumns = sel.columns.map { cd =>
      new ColumnDeclaration(
        cd.name,
        new ColumnElement(cd.element.rowTypeInfo, newAlias, cd.name, ColumnInfo.from(cd.element)))
    }
    val newFrom = new SelectElement(
      newAlias,
      sel.columns,
      sel.from,
      sel.where,
      sel.orderBy,
      sel.groupBy,
      sel.skip,
      sel.take,
      sel.isDistinct,
      sel.reverse)
    new SelectElement(sel.alias, newColumns, newFrom, null, null, null, null, null, false, false)
  }
}

object SingletonProjectionRewriter {
  def rewrite(mapper: QueryMapper, e: RelationElement): RelationElement = {
    new SingletonProjectionRewriter(mapper).visitElement(e)
  }

  class ColumnMapper private (oldAliases: Set[TableAlias], newAlias: TableAlias) extends DbQueryTreeVisitor {
    protected override def handleColumn(column: ColumnElement): RelationElement = {
      if (oldAliases.contains(column.alias))
        new ColumnElement(column.rowTypeInfo, newAlias, column.name, column.columnInfo)
      else column
    }
  }

  object ColumnMapper {
    def map(e: RelationElement, newAlias: TableAlias, oldAliases: TableAlias*): RelationElement = {
      new ColumnMapper(oldAliases.toSet, newAlias).visitElement(e)
    }
  }
}
