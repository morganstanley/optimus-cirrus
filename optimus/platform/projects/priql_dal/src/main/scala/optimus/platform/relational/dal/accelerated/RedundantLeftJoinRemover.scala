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
package optimus.platform.relational.dal.accelerated

import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.data.translation.ElementReplacer
import optimus.platform.relational.data.translation.ReferencedAliasGatherer
import optimus.platform.relational.data.tree.ColumnElement
import optimus.platform.relational.data.tree.DbQueryTreeVisitor
import optimus.platform.relational.data.tree.JoinElement
import optimus.platform.relational.data.tree.JoinType
import optimus.platform.relational.data.tree.SelectElement
import optimus.platform.relational.data.tree.TableAlias
import optimus.platform.relational.data.tree.TableElement
import optimus.platform.relational.tree.BinaryExpressionElement
import optimus.platform.relational.tree.BinaryExpressionType
import optimus.platform.relational.tree.ExpressionListElement
import optimus.platform.relational.tree.RelationElement

import scala.collection.mutable

/**
 * remove the join with Table on dal$entityRef that selects no column from this Table we intentionally reserve the Table
 * in some cases to make it easier for expression-based entitlement check
 *
 * there is a server side remover that will handle more cases after entitlement check
 */
class RedundantLeftJoinRemover private () extends DbQueryTreeVisitor {
  private[this] var currentSelect: SelectElement = _
  private val aliasToColumns = new mutable.HashMap[TableAlias, mutable.HashSet[ColumnElement]]
  private val columnMappings = new mutable.HashMap[ColumnElement, ColumnElement]

  def remove(e: RelationElement): RelationElement = {
    val result = visitElement(e)
    if (columnMappings.isEmpty) result
    else {
      val (searchFor, replaceWith) = columnMappings.toList.unzip
      ElementReplacer.replace(result, searchFor, replaceWith)
    }
  }

  protected override def handleSelect(select: SelectElement): RelationElement = {
    val savedSelect = select
    currentSelect = select
    val where = visitElement(select.where)
    val orderBy = visitOrderBy(select.orderBy)
    val groupBy = visitElementList(select.groupBy)
    val skip = visitElement(select.skip)
    val take = visitElement(select.take)
    val columns = visitColumnDeclarations(select.columns)
    val from = visitSource(select.from)

    val s = updateSelect(select, from, where, orderBy, groupBy, skip, take, columns)
    currentSelect = savedSelect
    s
  }

  protected override def handleJoin(join: JoinElement): RelationElement = {
    (visitElement(join.condition), visitSource(join.right)) match {
      case (BinaryExpressionElement(BinaryExpressionType.EQ, c1: ColumnElement, c2: ColumnElement, _), t: TableElement)
          if join.joinType == JoinType.LeftOuter &&
            aliasToColumns(t.alias).size == 1 &&
            aliasToColumns(t.alias).head.name == DALProvider.EntityRef &&
            (c1.alias == t.alias || c2.alias == t.alias) &&
            !ReferencedAliasGatherer
              .gather(new ExpressionListElement(currentSelect.columns.map(_.element)))
              .contains(t.alias) =>
        if (c1.alias == t.alias) columnMappings.put(c1, c2) else columnMappings.put(c2, c1)
        visitSource(join.left)
      case (condition, right) =>
        val left = visitSource(join.left)
        updateJoin(join, join.joinType, left, right, condition)
    }
  }

  protected override def handleBinaryExpression(element: BinaryExpressionElement): RelationElement = {
    super.handleBinaryExpression(element) match {
      case b @ BinaryExpressionElement(BinaryExpressionType.EQ, c1: ColumnElement, c2: ColumnElement, _) =>
        if (c1.name == DALProvider.ParentRef && c2.name == DALProvider.EntityRef)
          aliasToColumns(c2.alias)
            .add(new ColumnElement(c2.rowTypeInfo, c2.alias, c2.name + "__reserved__", c2.columnInfo))
        b
      case b => b
    }
  }

  protected override def handleColumn(column: ColumnElement): RelationElement = {
    if (currentSelect ne null) {
      val columnSet = aliasToColumns.getOrElseUpdate(column.alias, new mutable.HashSet[ColumnElement]())
      columnSet.add(column)
    }
    column
  }
}

object RedundantLeftJoinRemover {
  def remove(e: RelationElement): RelationElement = {
    new RedundantLeftJoinRemover().remove(e)
  }
}
