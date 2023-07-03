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

import optimus.platform.Query
import optimus.platform.relational.data.tree._
import optimus.platform.relational.tree._

import scala.collection.mutable.HashSet
import scala.collection.compat._

/**
 * Attempt to rewrite cross joins as inner joins
 */
class CrossJoinRewriter private () extends DbQueryTreeVisitor {
  private[this] var currentWhere: RelationElement = _

  override protected def handleSelect(select: SelectElement): RelationElement = {
    val savedWhere = currentWhere
    try {
      currentWhere = select.where
      val s = super.handleSelect(select).asInstanceOf[SelectElement]
      if (currentWhere eq s.where) s
      else {
        new SelectElement(
          s.alias,
          s.columns,
          s.from,
          currentWhere,
          s.orderBy,
          s.groupBy,
          s.skip,
          s.take,
          s.isDistinct,
          s.reverse)
      }
    } finally {
      currentWhere = savedWhere
    }
  }

  override protected def handleJoin(join: JoinElement): RelationElement = {
    val j = super.handleJoin(join).asInstanceOf[JoinElement]
    if (j.joinType == JoinType.CrossJoin && (currentWhere ne null)) {
      // try to figure out which parts of the current where element can be used for a join condition
      val declaredLeft = DeclaredAliasGatherer.gather(j.left).to(HashSet)
      val declaredRight = DeclaredAliasGatherer.gather(j.right).to(HashSet)
      val declared = declaredLeft.clone().union(declaredRight)
      val elems = Query.flattenBOOLANDConditions(currentWhere)
      val (good, bad) = elems.partition(e => canBeJoinCondition(e, declaredLeft, declaredRight, declared))
      if (good.isEmpty) j
      else {
        val condition = good.tail.foldLeft(good.head)((e1, e2) => ElementFactory.andAlso(e1, e2))
        val newJ = updateJoin(j, JoinType.InnerJoin, j.left, j.right, condition)
        currentWhere =
          if (bad.isEmpty) null else bad.tail.foldLeft(bad.head)((e1, e2) => ElementFactory.andAlso(e1, e2))
        newJ
      }
    } else j
  }

  private def canBeJoinCondition(
      e: RelationElement,
      left: HashSet[TableAlias],
      right: HashSet[TableAlias],
      all: HashSet[TableAlias]): Boolean = {
    // an expression could be a join condition if it has at least one reference to an alias from both left & right
    // sets and does not have any additional references that are not in both left & right sets
    val refs = ReferencedAliasGatherer.gather(e)
    val refLeft = refs.exists(left.contains)
    val refRight = refs.exists(right.contains)
    val isSubset = refs.subsetOf(all)
    refLeft && refRight && isSubset
  }
}

object CrossJoinRewriter {
  def rewrite(e: RelationElement): RelationElement = {
    new CrossJoinRewriter().visitElement(e)
  }
}

class ReferencedAliasGatherer private () extends DbQueryTreeVisitor {
  private val alias = new HashSet[TableAlias]()

  override protected def handleColumn(column: ColumnElement): RelationElement = {
    alias.add(column.alias)
    column
  }
}

object ReferencedAliasGatherer {
  def gather(e: RelationElement): HashSet[TableAlias] = {
    val g = new ReferencedAliasGatherer()
    g.visitElement(e)
    g.alias
  }
}
