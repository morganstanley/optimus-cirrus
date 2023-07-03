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

import optimus.platform.relational.data.tree._
import optimus.platform.relational.tree.RelationElement
import java.util.HashMap
import java.util.HashSet

import optimus.platform.relational.data.mapping.QueryMapping

import scala.collection.mutable.ListBuffer

/*
 * Removes column declarations in SelectElement's that are not referenced
 */
class UnusedColumnRemover private (val mapping: QueryMapping) extends DbQueryTreeVisitor {
  private val allColumnsUsed = new HashMap[TableAlias, HashSet[String]]()
  private[this] var reserveColumnsForCount = false

  private def markColumnAsUsed(alias: TableAlias, name: String): Unit = {
    var columns = allColumnsUsed.get(alias)
    if (columns eq null) {
      columns = new HashSet[String]()
      allColumnsUsed.put(alias, columns)
    }
    columns.add(name)
  }

  private def isColumnUsed(alias: TableAlias, name: String): Boolean = {
    val columns = allColumnsUsed.get(alias)
    (columns ne null) && columns.contains(name)
  }

  private def clearColumnsUsed(alias: TableAlias): Unit = {
    allColumnsUsed.put(alias, new HashSet[String]())
  }

  override protected def handleColumn(column: ColumnElement): RelationElement = {
    markColumnAsUsed(column.alias, column.name)
    column
  }

  override protected def handleSubquery(subquery: SubqueryElement): RelationElement = {
    markColumnAsUsed(subquery.select.alias, subquery.select.columns(0).name)
    super.handleSubquery(subquery)
  }

  override protected def handleSelect(select: SelectElement): RelationElement = {
    val wasReserved = reserveColumnsForCount
    reserveColumnsForCount = false
    if (select.isDistinct)
      mapping.inferKeyColumnDeclarations(select).foreach(decl => markColumnAsUsed(select.alias, decl.name))
    else if (wasReserved && (allColumnsUsed.get(select.alias) eq null))
      mapping.inferKeyColumnDeclarations(select).foreach(decl => markColumnAsUsed(select.alias, decl.name))

    var alternate: ListBuffer[ColumnDeclaration] = null
    var index = 0
    for (decl <- select.columns) {
      val colDecl = if (isColumnUsed(select.alias, decl.name)) {
        val e = visitElement(decl.element)
        if (e ne decl.element) ColumnDeclaration(decl.name, e) else decl
      } else null
      if ((alternate eq null) && (colDecl ne decl)) {
        alternate = new ListBuffer[ColumnDeclaration]()
        alternate ++= select.columns.take(index)
      }
      if ((alternate ne null) && (colDecl ne null))
        alternate += colDecl
      index += 1
    }
    val columns = if (alternate ne null) alternate.toList else select.columns
    val take = visitElement(select.take)
    val skip = visitElement(select.skip)
    val groupBy = visitElementList(select.groupBy)
    val orderBys = visitOrderBy(select.orderBy)
    val where = visitElement(select.where)
    val from = visitElement(select.from)
    clearColumnsUsed(select.alias)
    reserveColumnsForCount = wasReserved
    updateSelect(select, from, where, orderBys, groupBy, skip, take, columns)
  }

  override protected def handleAggregate(aggregate: AggregateElement): RelationElement = {
    if ((aggregate.aggregateName == "count" || aggregate.aggregateName == "countOption") && aggregate.arguments.isEmpty)
      reserveColumnsForCount = true
    super.handleAggregate(aggregate)
  }

  override protected def handleProjection(proj: ProjectionElement): RelationElement = {
    // visit mapping in reverse order
    val projector = visitElement(proj.projector)
    val select = visitElement(proj.select).asInstanceOf[SelectElement]
    updateProjection(proj, select, projector, proj.aggregator)
  }

  override protected def handleJoin(join: JoinElement): RelationElement = {
    // visit join in reverse order
    val condition = visitElement(join.condition)
    val right = visitSource(join.right)
    val left = visitSource(join.left)
    updateJoin(join, join.joinType, left, right, condition)
  }
}

object UnusedColumnRemover {
  def remove(mapping: QueryMapping, element: RelationElement): RelationElement = {
    new UnusedColumnRemover(mapping).visitElement(element)
  }
}
