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
import optimus.platform.relational.tree.ScopedHashMap

import scala.collection.mutable

/**
 * Removes joins elements that are identical to joins that already exist
 */
class DuplicateJoinRemover private () extends DbQueryTreeVisitor {
  private[this] val map = new mutable.HashMap[TableAlias, TableAlias]

  override protected def handleColumn(column: ColumnElement): RelationElement = {
    map
      .get(column.alias)
      .map { ta =>
        new ColumnElement(column.projectedType(), ta, column.name, column.columnInfo)
      }
      .getOrElse(column)
  }

  override protected def handleJoin(join: JoinElement): RelationElement = {
    super.handleJoin(join) match {
      case j: JoinElement =>
        (j.left, j.right) match {
          case (lj: JoinElement, a: AliasedElement) =>
            val similarRight = findSimilarRight(lj, j).asInstanceOf[AliasedElement]
            if (similarRight eq null) j
            else {
              map.put(a.alias, similarRight.alias)
              lj
            }
          case _ => j
        }
      case x => x
    }
  }

  private def findInto(join: JoinElement, compareTo: JoinElement): RelationElement = {
    (join.left, join.right) match {
      case (lj: JoinElement, rj: JoinElement) =>
        val result = findSimilarRight(lj, compareTo)
        if (result ne null) result else findSimilarRight(rj, compareTo)
      case (lj: JoinElement, _) => findSimilarRight(lj, compareTo)
      case (_, rj: JoinElement) => findSimilarRight(rj, compareTo)
      case _                    => null
    }
  }

  private def findSimilarRight(join: JoinElement, compareTo: JoinElement): RelationElement = {
    if (
      join.joinType == compareTo.joinType && join.right.elementType == compareTo.right.elementType
      && DbQueryTreeComparer.areEqual(join.right, compareTo.right)
    ) {

      if (join.condition eq compareTo.condition) join.right
      else {
        val scope = new ScopedHashMap[TableAlias, TableAlias](null)
        scope.put(join.right.asInstanceOf[AliasedElement].alias, compareTo.right.asInstanceOf[AliasedElement].alias)
        if (DbQueryTreeComparer.areEqual(null, scope, join.condition, compareTo.condition)) join.right
        else findInto(join, compareTo)
      }
    } else findInto(join, compareTo)
  }
}

object DuplicateJoinRemover {
  def remove(e: RelationElement): RelationElement = {
    new DuplicateJoinRemover().visitElement(e)
  }
}
