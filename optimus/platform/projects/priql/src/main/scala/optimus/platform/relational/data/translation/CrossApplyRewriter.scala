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

class CrossApplyRewriter private (lan: QueryLanguage) extends DbQueryTreeVisitor {

  override protected def handleJoin(join: JoinElement): RelationElement = {
    val j = super.handleJoin(join).asInstanceOf[JoinElement]
    if (j.joinType != JoinType.CrossApply && j.joinType != JoinType.OuterApply) j
    else
      j.right match {
        case t: TableElement =>
          new JoinElement(JoinType.CrossJoin, j.left, j.right, null)

        // Only consider rewriting cross apply if
        //   1) right side is a select
        //   2) project s.where to columns in the right-side select, no left-side declared aliases are referenced
        //   3) and has no behavior that would change semantics if the where clause is removed (like groups, aggregates, take, skip, etc).
        // Note: it is best to attempt this after redundant subqueries have been removed.
        case s: SelectElement
            if (s.take eq null) && (s.skip eq null) && !AggregateChecker.hasAggregates(s) &&
              ((s.groupBy eq null) || s.groupBy.isEmpty) && !s.isDistinct =>
          val pc =
            ColumnProjector.projectColumns(lan, s.where, s.columns, s.alias, DeclaredAliasGatherer.gather(s.from))
          val select = new SelectElement(
            s.alias,
            pc.columns,
            s.from,
            null,
            s.orderBy,
            s.groupBy,
            s.skip,
            s.take,
            s.isDistinct,
            s.reverse)
          val referencedAliases = ReferencedAliasGatherer.gather(select)
          val declaredAliases = DeclaredAliasGatherer.gather(j.left).toSet
          val intersectedAliases = referencedAliases.intersect(declaredAliases)
          if (intersectedAliases.nonEmpty) j
          else {
            val where = pc.projector
            val jt =
              if (where eq null) JoinType.CrossJoin
              else if (j.joinType == JoinType.CrossApply) JoinType.InnerJoin
              else JoinType.LeftOuter
            new JoinElement(jt, j.left, select, where)
          }

        case _ => j
      }
  }
}

object CrossApplyRewriter {
  def rewrite(lan: QueryLanguage, e: RelationElement): RelationElement = {
    new CrossApplyRewriter(lan).visitElement(e)
  }
}
