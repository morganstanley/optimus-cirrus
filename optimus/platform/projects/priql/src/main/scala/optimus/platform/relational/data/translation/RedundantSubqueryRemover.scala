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

import optimus.platform.relational.tree.ElementType
import optimus.platform.relational.tree.ElementFactory
import optimus.platform.relational.tree.RelationElement
import optimus.platform.relational.data.tree._
import scala.collection.mutable.ListBuffer

/*
 * Removes select elements that don't add any additional semantic value
 */
class RedundantSubqueryRemover extends DbQueryTreeVisitor {
  protected override def handleSelect(select: SelectElement): RelationElement = {
    super.handleSelect(select) match {
      case s: SelectElement =>
        // remove all purely redundant subqueries
        val redundant = RedundantSubqueryGatherer.gather(s.from)
        if (redundant.isEmpty) s else SubqueryRemover.remove(s, redundant: _*)
    }
  }

  protected override def handleProjection(proj: ProjectionElement): RelationElement = {
    super.handleProjection(proj) match {
      case p @ ProjectionElement(select, _, _) =>
        select.from match {
          case _: SelectElement =>
            val redundant = RedundantSubqueryGatherer.gather(select)
            if (redundant.isEmpty) p else SubqueryRemover.remove(p, redundant: _*)
          case _ =>
            p
        }
    }
  }
}

object RedundantSubqueryRemover {
  def remove(element: RelationElement): RelationElement = {
    var e = new RedundantSubqueryRemover().visitElement(element)
    e = SubqueryMerger.merge(e)
    e
  }

  def isSimpleProjection(select: SelectElement): Boolean = {
    select.columns.forall {
      case ColumnDeclaration(name, col: ColumnElement) if name == col.name => true
      case _                                                               => false
    }
  }

  def isNameMapProjection(select: SelectElement): Boolean = {
    select.from match {
      case fromSelect: SelectElement if fromSelect.columns.size == select.columns.size =>
        // test that all columns in 'select' are referring to columns in the same position
        // in from.
        fromSelect.columns.zip(select.columns).forall {
          case (ColumnDeclaration(fromName, _), ColumnDeclaration(_, c: ColumnElement)) if fromName == c.name => true
          case _                                                                                              => false
        }
      case _ => false
    }
  }
}

private object SubqueryMerger {
  import RedundantSubqueryRemover._

  def merge(element: RelationElement): RelationElement = {
    new SubqueryMerger().visitElement(element)
  }

  def getLeftMostSelect(source: RelationElement): SelectElement = {
    source match {
      case s: SelectElement => s
      case j: JoinElement   => getLeftMostSelect(j.left)
      case _                => null
    }
  }

  def isRightSideOuterJoin(leftMostSel: SelectElement, source: RelationElement): Boolean = {
    source match {
      case j: JoinElement if j.joinType == JoinType.RightOuter || j.joinType == JoinType.FullOuter => true
      case j: JoinElement                       => isRightSideOuterJoin(leftMostSel, j.left)
      case s: SelectElement if s != leftMostSel => isRightSideOuterJoin(leftMostSel, s.from)
      case _                                    => false
    }
  }

  def canMergeWithFrom(select: SelectElement): Boolean = {
    val fromSelect = getLeftMostSelect(select.from)
    if (fromSelect eq null)
      false
    else if (!isColumnProjection(fromSelect))
      false
    else {
      val selHasNameMapProjection = isNameMapProjection(select)
      val selHasJoin = select.from.elementType == DbElementType.Join
      val selHasOrderBy = (select.orderBy ne null) && select.orderBy.nonEmpty
      val selHasGroupBy = (select.groupBy ne null) && select.groupBy.nonEmpty
      val selHasAggregates = AggregateChecker.hasAggregates(select)
      val frmHasOrderBy = (fromSelect.orderBy ne null) && fromSelect.orderBy.nonEmpty
      val frmHasGroupBy = (fromSelect.groupBy ne null) && fromSelect.groupBy.nonEmpty
      val frmHasAggregates = AggregateChecker.hasAggregates(fromSelect)
      val frmHasWhere = (fromSelect.where ne null)
      // both cannot have orderby
      if (selHasOrderBy && frmHasOrderBy)
        false
      // both cannot have groupby
      else if (selHasGroupBy && frmHasGroupBy)
        false
      else if (select.reverse || fromSelect.reverse)
        false
      // cannot move forward order-by if outer has group-by
      else if (frmHasOrderBy && (selHasGroupBy || selHasAggregates || select.isDistinct))
        false
      // cannot move forward group-by if outer has where clause
      else if (frmHasGroupBy) // need to assert projection is the same in order to move group-by forward
        false
      // cannot move where if outer join is right-side-outer
      else if (frmHasWhere && selHasJoin && isRightSideOuterJoin(fromSelect, select.from))
        false
      // cannot move forward a take if outer has take or skip or distinct
      else if (
        (fromSelect.take ne null) && ((select.take ne null) || (select.skip ne null) || select.isDistinct ||
          selHasAggregates || selHasGroupBy || selHasJoin)
      )
        false
      // cannot move forward a skip if outer has skip or distinct
      else if (
        (fromSelect.skip ne null) && ((select.skip ne null) || select.isDistinct || selHasAggregates ||
          selHasGroupBy || selHasJoin)
      )
        false
      // cannot move forward a distinct if outer has take, skip, groupby or a different projection
      else if (
        fromSelect.isDistinct && ((select.take ne null) || (select.skip ne null) || !selHasNameMapProjection ||
          selHasGroupBy || selHasAggregates || selHasOrderBy || selHasJoin)
      )
        false
      else if (
        frmHasAggregates && ((select.take ne null) || (select.skip ne null) || select.isDistinct ||
          selHasAggregates || selHasGroupBy || selHasJoin)
      )
        false
      else
        true
    }
  }

  def isColumnProjection(select: SelectElement): Boolean = {
    select.columns.forall { cd =>
      cd.element.elementType match {
        case DbElementType.Column                                       => true
        case ElementType.ConstValue if !cd.name.startsWith("null$test") => true
        case _                                                          => false
      }
    }
  }
}

private class SubqueryMerger extends DbQueryTreeVisitor {
  private var isTop = true

  protected override def handleSelect(select: SelectElement): RelationElement = {
    import SubqueryMerger._

    val wasTopLevel = isTop
    isTop = false
    super.handleSelect(select) match {
      case s: SelectElement =>
        var sel = s

        // next attempt to merge subqueries that would have been removed by the above
        // logic except for the existence of a where clause
        while (canMergeWithFrom(sel)) {
          val fromSelect = getLeftMostSelect(sel.from)

          // remove the redundant subquery
          sel = SubqueryRemover.remove(sel, fromSelect)

          // merge where expressions
          val where = (sel.where, fromSelect.where) match {
            case (null, y) => y
            case (x, null) => x
            case (x, y)    => ElementFactory.andAlso(y, x)
          }
          val orderBy = if ((sel.orderBy ne null) && sel.orderBy.nonEmpty) sel.orderBy else fromSelect.orderBy
          val groupBy = if ((sel.groupBy ne null) && sel.groupBy.nonEmpty) sel.groupBy else fromSelect.groupBy
          val skip = if (sel.skip ne null) sel.skip else fromSelect.skip
          val take = if (sel.take ne null) sel.take else fromSelect.take
          val isDistinct = sel.isDistinct | fromSelect.isDistinct

          if (
            (where ne sel.where) ||
            (orderBy ne sel.orderBy) ||
            (groupBy ne sel.groupBy) ||
            isDistinct != sel.isDistinct ||
            (skip ne sel.skip) ||
            (take ne sel.take)
          )
            sel = new SelectElement(
              sel.alias,
              sel.columns,
              sel.from,
              where,
              orderBy,
              groupBy,
              skip,
              take,
              isDistinct,
              sel.reverse)
        }
        sel

      case other => other
    }
  }

}

private class RedundantSubqueryGatherer extends DbQueryTreeVisitor {
  val redundant = new ListBuffer[SelectElement]

  protected override def handleSelect(select: SelectElement): RelationElement = {
    if (isRedudantSubquery(select))
      redundant += select
    select
  }

  protected override def handleSubquery(subquery: SubqueryElement): RelationElement = {
    // don't gather inside subquery
    subquery
  }

  private def isRedudantSubquery(select: SelectElement): Boolean = {
    import RedundantSubqueryRemover._

    (isSimpleProjection(select) || isNameMapProjection(select)) &&
    select.from.elementType != DbElementType.Join &&
    !select.isDistinct &&
    !select.reverse &&
    (select.where eq null) &&
    (select.take eq null) &&
    (select.skip eq null) &&
    ((select.orderBy eq null) || select.orderBy.isEmpty) &&
    ((select.groupBy eq null) || select.groupBy.isEmpty)
  }
}

private object RedundantSubqueryGatherer {
  def gather(element: RelationElement): List[SelectElement] = {
    val gatherer = new RedundantSubqueryGatherer()
    gatherer.visitElement(element)
    gatherer.redundant.result()
  }
}
