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

import optimus.platform.relational.data.language.QueryLanguage
import optimus.platform.relational.data.tree._
import optimus.platform.relational.tree._

import scala.collection.mutable

/*
 * Moves order-bys to the outermost select if possible
 */
class OrderByRewriter private (val language: QueryLanguage) extends DbQueryTreeVisitor {
  private[this] var isOuterMostSelect = true
  private[this] var gatheredOrderings = new mutable.ArrayBuffer[OrderDeclaration]

  protected override def handleSubquery(subquery: SubqueryElement): RelationElement = {
    val saveOrderings = gatheredOrderings
    gatheredOrderings = new mutable.ArrayBuffer[OrderDeclaration]
    val result = super.handleSubquery(subquery)
    gatheredOrderings = saveOrderings
    result
  }

  protected override def handleJoin(join: JoinElement): RelationElement = {
    val left = visitSource(join.left)
    val leftOrderings = gatheredOrderings.toList
    gatheredOrderings.clear()
    val right = visitSource(join.right)
    prependOrderings(leftOrderings)
    val condition = visitElement(join.condition)
    updateJoin(join, join.joinType, left, right, condition)
  }

  protected override def handleSelect(select: SelectElement): RelationElement = {
    val saveIsOuterMostSelect = isOuterMostSelect
    try {
      isOuterMostSelect = false
      val sel = super.handleSelect(select).asInstanceOf[SelectElement]

      val hasOrderBy = (sel.orderBy ne null) && sel.orderBy.nonEmpty
      val hasGroupBy = (sel.groupBy ne null) && sel.groupBy.nonEmpty
      val canHaveOrderBy = saveIsOuterMostSelect || (sel.take ne null) || (sel.skip ne null)
      val canReceiveOrderings = canHaveOrderBy && !hasGroupBy && !sel.isDistinct && !AggregateChecker.hasAggregates(sel)

      if (hasOrderBy)
        prependOrderings(sel.orderBy)

      if (sel.reverse)
        reverseOrderings()

      val orderings = if (canReceiveOrderings) gatheredOrderings.toList else if (canHaveOrderBy) sel.orderBy else null
      val canPassOnOrderings = !saveIsOuterMostSelect && !hasGroupBy && !sel.isDistinct
      var columns = sel.columns
      if (gatheredOrderings.nonEmpty) {
        if (canPassOnOrderings) {
          val producedAliases = DeclaredAliasGatherer.gather(sel.from).toSet
          val (newColumns, newOrderings) = rebindOrderings(gatheredOrderings, sel.alias, producedAliases, sel.columns)
          gatheredOrderings.clear()
          prependOrderings(newOrderings)
          columns = newColumns
        } else gatheredOrderings.clear()
      }
      if ((orderings ne sel.orderBy) || (columns ne sel.columns) || sel.reverse)
        new SelectElement(
          sel.alias,
          columns,
          sel.from,
          sel.where,
          orderings,
          sel.groupBy,
          sel.skip,
          sel.take,
          sel.isDistinct,
          false)
      else
        sel
    } finally {
      isOuterMostSelect = saveIsOuterMostSelect
    }
  }

  protected def prependOrderings(newOrderings: List[OrderDeclaration]): Unit = {
    if (newOrderings ne null) {
      gatheredOrderings.insertAll(0, newOrderings)
      // trim off obvious duplicates
      val unique = new mutable.HashSet[String]
      var index = 0
      while (index < gatheredOrderings.size) {
        gatheredOrderings(index).element match {
          case e: ColumnElement =>
            val hash = s"${e.alias}:${e.name}"
            if (!unique.add(hash))
              gatheredOrderings.remove(index)
            else index += 1

          case _ => index += 1
        }
      }
    }
  }

  protected def rebindOrderings(
      orderings: Iterable[OrderDeclaration],
      alias: TableAlias,
      existingAliases: Set[TableAlias],
      existingColumns: List[ColumnDeclaration]): (List[ColumnDeclaration], List[OrderDeclaration]) = {
    val newColumns = new mutable.ListBuffer[ColumnDeclaration]
    newColumns ++= existingColumns
    val newOrderings = new mutable.ListBuffer[OrderDeclaration]
    for (ordering <- orderings) {
      val (doCheck, column) = ordering.element match {
        case c: ColumnElement => (existingAliases.contains(c.alias), c)
        case _                => (true, null)
      }
      if (doCheck) {
        newColumns.zipWithIndex.find { case (decl @ ColumnDeclaration(_, elem), idx) =>
          val declColumn = elem match {
            case e: ColumnElement => e
            case _                => null
          }
          (decl.element eq ordering.element) || ((column ne null) && (declColumn ne null) && column.alias == declColumn.alias && column.name == declColumn.name)
        } match {
          case Some((decl, idx)) =>
            newOrderings += OrderDeclaration(
              ordering.direction,
              new ColumnElement(column.projectedType(), alias, decl.name, ColumnInfo.from(ordering.element)))

          case None =>
            val idx = newColumns.size
            var colName = if (column ne null) column.name else s"c$idx"
            colName = ColumnDeclaration.getAvailableColumnName(newColumns, colName)
            newColumns += new ColumnDeclaration(colName, ordering.element)
            newOrderings += OrderDeclaration(
              ordering.direction,
              new ColumnElement(ordering.element.projectedType(), alias, colName, ColumnInfo.from(ordering.element)))
        }
      }
    }
    (newColumns.result(), newOrderings.result())
  }

  protected def reverseOrderings(): Unit = {
    if (gatheredOrderings ne null)
      gatheredOrderings = gatheredOrderings.map { ord =>
        val direction =
          if (ord.direction == SortDirection.Ascending) SortDirection.Descending else SortDirection.Ascending
        OrderDeclaration(direction, ord.element)
      }
  }
}

object OrderByRewriter {
  def rewrite(language: QueryLanguage, element: RelationElement): RelationElement = {
    new OrderByRewriter(language).visitElement(element)
  }
}

class DeclaredAliasGatherer private () extends DbQueryTreeVisitor {
  val aliases = new mutable.LinkedHashSet[TableAlias]

  protected override def handleSelect(select: SelectElement): RelationElement = {
    aliases.add(select.alias)
    select
  }

  protected override def handleTable(table: TableElement): RelationElement = {
    aliases.add(table.alias)
    table
  }
}

object DeclaredAliasGatherer {
  def gather(element: RelationElement): List[TableAlias] = {
    val gatherer = new DeclaredAliasGatherer()
    gatherer.visitElement(element)
    gatherer.aliases.toList
  }
}
