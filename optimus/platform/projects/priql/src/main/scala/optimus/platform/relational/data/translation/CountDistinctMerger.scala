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
import optimus.platform.relational.tree._

// turn following query:
//   select count(*) from ( select distinct column1 from table )
// into
//   select count(distinct column1) from table
// when column1 is not Option[_]
class CountDistinctMerger protected () extends DbQueryTreeVisitor {
  override protected def handleSelect(select: SelectElement): RelationElement = {
    if (select.columns.size == 1) {
      val (from, columns) = visitSource(select.from) match {
        case s: SelectElement if s.isDistinct && s.columns.size == 1 =>
          (s.columns.head.element, select.columns.head.element) match {
            case (c: ColumnElement, AggregateElement(aggTypeInfo, "count", Nil, false)) =>
              val newSel = new SelectElement(
                s.alias,
                s.columns,
                s.from,
                s.where,
                s.orderBy,
                s.groupBy,
                s.skip,
                s.take,
                isDistinct = false,
                s.reverse)
              val cde = new AggregateElement(aggTypeInfo, "count", List(c), true)
              val newCols = List(select.columns.head.copy(element = cde))
              (newSel, newCols)
            case (c: ColumnElement, AggregateElement(aggTypeInfo, "countOption", Nil, false)) =>
              val newSel = new SelectElement(
                s.alias,
                s.columns,
                s.from,
                s.where,
                s.orderBy,
                s.groupBy,
                s.skip,
                s.take,
                isDistinct = false,
                s.reverse)
              val countNotNone = new AggregateElement(aggTypeInfo, "count", List(c), true)
              val test = ElementFactory.equal(c, ElementFactory.constant(null, c.rowTypeInfo))
              val ifTrue = ElementFactory.constant(1, TypeInfo.INT)
              val ifFalse = ElementFactory.constant(null, TypeInfo.INT)
              val cond = ElementFactory.condition(test, ifTrue, ifFalse, TypeInfo.INT)
              val countNone = new AggregateElement(aggTypeInfo, "count", List(cond), true)
              val cde = ElementFactory.add(countNotNone, countNone)
              val newCols = List(select.columns.head.copy(element = cde))
              (newSel, newCols)
            case _ => (s, visitColumnDeclarations(select.columns))
          }
        case f => (f, visitColumnDeclarations(select.columns))
      }
      val where = visitElement(select.where)
      val orderBy = visitOrderBy(select.orderBy)
      val groupBy = visitElementList(select.groupBy)
      val skip = visitElement(select.skip)
      val take = visitElement(select.take)
      updateSelect(select, from, where, orderBy, groupBy, skip, take, columns)
    } else {
      super.handleSelect(select)
    }
  }
}

object CountDistinctMerger {
  def merge(e: RelationElement): RelationElement = {
    val elem = new CountDistinctMerger().visitElement(e)
    if (elem ne e)
      RedundantSubqueryRemover.remove(elem)
    else elem
  }
}
