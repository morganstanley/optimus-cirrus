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

import optimus.platform.relational.data.tree.AggregateElement
import optimus.platform.relational.data.tree.DbQueryTreeVisitor
import optimus.platform.relational.data.tree.SelectElement
import optimus.platform.relational.data.tree.SubqueryElement
import optimus.platform.relational.tree.RelationElement

class AggregateChecker private () extends DbQueryTreeVisitor {
  private var hasAggregate = false

  override protected def handleAggregate(aggregate: AggregateElement): RelationElement = {
    hasAggregate = true
    aggregate
  }

  override protected def handleSelect(select: SelectElement): RelationElement = {
    // only consider aggregates in these locations
    visitElement(select.where)
    visitOrderBy(select.orderBy)
    visitColumnDeclarations(select.columns)
    select
  }

  override protected def handleSubquery(subquery: SubqueryElement): RelationElement = {
    // don't count aggregates in subqueries
    subquery
  }
}

object AggregateChecker {
  def hasAggregates(s: SelectElement): Boolean = {
    val checker = new AggregateChecker()
    checker.visitElement(s)
    checker.hasAggregate
  }
}
