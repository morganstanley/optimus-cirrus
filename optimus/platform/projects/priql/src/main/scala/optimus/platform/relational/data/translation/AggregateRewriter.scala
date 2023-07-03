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
import optimus.platform.relational.tree.RelationElement

import scala.collection.mutable

class AggregateRewriter private (language: QueryLanguage, element: RelationElement) extends DbQueryTreeVisitor {
  import AggregateRewriter._

  private val map = new mutable.HashMap[AggregateSubqueryElement, RelationElement]
  private val lookup = AggregateGatherer.gather(element).groupBy(_.groupAlias)

  override protected def handleSelect(select: SelectElement): RelationElement = {
    val s = super.handleSelect(select).asInstanceOf[SelectElement]
    lookup.get(s.alias).map { elems =>
      val aggColumns = s.columns ::: elems.zipWithIndex.map { case (ae, idx) =>
        val cd = ColumnDeclaration(s"agg$idx", ae.aggregateInGroupSelect)
        map.put(ae, new ColumnElement(cd.element.rowTypeInfo, ae.groupAlias, cd.name, ColumnInfo.from(cd.element)))
        cd
      }
      new SelectElement(
        s.alias,
        aggColumns,
        s.from,
        s.where,
        s.orderBy,
        s.groupBy,
        s.skip,
        s.take,
        s.isDistinct,
        s.reverse)
    } getOrElse (s)
  }

  override protected def handleAggregateSubquery(aggregate: AggregateSubqueryElement): RelationElement = {
    map.getOrElse(aggregate, visitElement(aggregate.aggregateAsSubquery))
  }
}

object AggregateRewriter {
  def rewrite(language: QueryLanguage, element: RelationElement): RelationElement = {
    new AggregateRewriter(language, element).visitElement(element)
  }

  class AggregateGatherer private () extends DbQueryTreeVisitor {
    private var aggregates: List[AggregateSubqueryElement] = Nil

    override protected def handleAggregateSubquery(aggregate: AggregateSubqueryElement): RelationElement = {
      aggregates = aggregate :: aggregates
      super.handleAggregateSubquery(aggregate)
    }
  }

  object AggregateGatherer {
    def gather(e: RelationElement): List[AggregateSubqueryElement] = {
      val g = new AggregateGatherer()
      g.visitElement(e)
      g.aggregates.reverse
    }
  }
}
