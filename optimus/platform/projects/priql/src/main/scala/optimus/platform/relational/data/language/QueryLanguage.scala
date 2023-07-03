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
package optimus.platform.relational.data.language

import optimus.platform.Query
import optimus.platform.relational.data.QueryTranslator
import optimus.platform.relational.data.mapping.MappingEntityLookup
import optimus.platform.relational.data.translation._
import optimus.platform.relational.data.tree._
import optimus.platform.relational.tree._

import scala.collection.mutable

trait QueryLanguage {

  def lookup: MappingEntityLookup

  def isAggregate(member: MemberDescriptor): Boolean = {
    if (member eq null) false
    else if (member.declaringType <:< classOf[Query[_]]) {
      member.name match {
        case "sum" | "max" | "min" | "average" | "count" | "variance" | "stddev" | "corr" => true
        case _                                                                            => false
      }
    } else false
  }

  def isHeadLast(member: MemberDescriptor): Boolean = {
    if (member eq null) false
    else if (member.declaringType <:< classOf[Query[_]]) {
      member.name match {
        case "head" | "headOption" | "last" | "lastOption" => true
        case _                                             => false
      }
    } else false
  }

  def aggregateArgumentIsPredicate(aggregateName: String): Boolean = {
    aggregateName == "count"
  }

  def canBeColumn(e: RelationElement): Boolean = {
    e match {
      case contains: ContainsElement if contains.values.isLeft => true
      case _                                                   => mustBeColumn(e)
    }
  }

  def mustBeColumn(e: RelationElement): Boolean = {
    import DbElementType._

    e.elementType match {
      case Column | Scalar | Aggregate | AggregateSubquery | Exists => true
      case _                                                        => false
    }
  }

  def quote(name: String): String = {
    name
  }

  def canBeProjector(e: RelationElement): Boolean = {
    ProjectorChecker.canBeProjector(e)
  }

  def canBeWhere(e: RelationElement): Boolean = {
    import DbElementType._
    import ElementType._

    QueryTreeChecker.forall(
      e,
      _.elementType match {
        case BinaryExpression | ConstValue | Column | Contains => true
        case _                                                 => false
      })
  }

  def canBeGroupBy(e: RelationElement): Boolean = {
    canBeProjector(e)
  }

  def canBeAggregate(e: RelationElement): Boolean = {
    canBeProjector(e)
  }

  def canBeOn(e: RelationElement, methodCode: QueryMethod): Boolean = {
    canBeWhere(e)
  }

  def canBeSubqueryProjector(p: RelationElement): Boolean = {
    ColumnCollector.collect(p).size == 1
  }

  def shouldBeDistinct(clz: Class[_]): Boolean = {
    clz == classOf[Set[_]]
  }

  def canBeOrderBy(e: RelationElement): Boolean = {
    def doCheck(elem: RelationElement): Boolean = {
      QueryTreeChecker.forall(
        elem,
        _ match {
          case BinaryExpressionElement(op, _, _, _) => true
          case e: ConstValueElement                 => true
          case e: ColumnElement                     => true
          case FuncElement(callee: MethodCallee, _, null) if callee.name == "apply" =>
            val className = callee.resType.clazz.getName
            className == "optimus.platform.relational.desc" || className == "optimus.platform.relational.asc"
          case _ => false
        }
      )
    }

    e match {
      case TupleElement(elements) => elements.forall(doCheck)
      case _                      => doCheck(e)
    }
  }

  def addOuterJoinTest(
      select: SelectElement,
      originValue: RelationElement,
      defaultValue: RelationElement,
      isLeft: Boolean): (SelectElement, OuterJoinedElement) = {
    var sel = select
    val (newFrom, test) = getOuterJoinTest(select, isLeft)

    // look to see if test expression exists in columns already
    val col = sel.columns.find(c => test == c.element)
    val testCol =
      col.map(c => new ColumnElement(test.rowTypeInfo, sel.alias, c.name, ColumnInfo.from(test))).getOrElse {
        // add expression to projection
        val colName = ColumnDeclaration.getAvailableColumnName(sel.columns, test.name)
        val newColumns = sel.columns :+ ColumnDeclaration(colName, test)
        sel = new SelectElement(
          sel.alias,
          newColumns,
          newFrom,
          sel.where,
          sel.orderBy,
          sel.groupBy,
          sel.skip,
          sel.take,
          sel.isDistinct,
          sel.reverse)
        new ColumnElement(test.rowTypeInfo, sel.alias, colName, ColumnInfo.from(test))
      }

    val outerJoin = new OuterJoinedElement(testCol, originValue, defaultValue)
    (sel, outerJoin)
  }

  def getOuterJoinTest(s: SelectElement, isLeft: Boolean): (JoinElement, ColumnElement) = {
    // if the column is used in the join condition (equality test)
    // if it is null in the database then the join test won't match (null != null) so the row won't appear
    // we can safely use this existing column as our test to determine if the outer join produced a row

    // find a column that is used in equality test
    val join = s.from.asInstanceOf[JoinElement]
    val aliases =
      if (isLeft) DeclaredAliasGatherer.gather(join.left).toSet else DeclaredAliasGatherer.gather(join.right).toSet

    val joinColumns = JoinColumnGatherer.gather(aliases, join.condition).toList
    if (joinColumns.nonEmpty) {
      val testCol = joinColumns
        .find { jc =>
          s.columns.exists(col => jc == col.element)
        }
        .getOrElse(joinColumns.head)
      (join, testCol)
    } else {
      if (isLeft) {
        val (left, test) = createConstTestColumn(join.left)
        val newJoin = new JoinElement(join.joinType, left, join.right, join.condition)
        (newJoin, test)
      } else {
        val (right, test) = createConstTestColumn(join.right)
        val newJoin = new JoinElement(join.joinType, join.left, right, join.condition)
        (newJoin, test)
      }
    }
  }

  // Create a const column named "null$test" to judge if left/ right is null
  def createConstTestColumn(source: RelationElement): (SelectElement, ColumnElement) = {
    source match {
      case select: SelectElement =>
        val testColumn = ElementFactory.constant(1, TypeInfo(classOf[Option[_]], TypeInfo.INT))
        val colName = ColumnDeclaration.getAvailableColumnName(select.columns, "null$test")
        val columns = select.columns :+ ColumnDeclaration(colName, testColumn)
        val sel = new SelectElement(
          select.alias,
          columns,
          select.from,
          select.where,
          select.orderBy,
          select.groupBy,
          select.skip,
          select.take,
          select.isDistinct,
          select.reverse)

        val test = new ColumnElement(testColumn.rowTypeInfo, select.alias, colName, ColumnInfo.from(testColumn))
        (sel, test)
    }
  }

  def createDialect(translator: QueryTranslator): QueryDialect
}

abstract class QueryDialect(val language: QueryLanguage, val translator: QueryTranslator) {

  def translate(e: RelationElement): RelationElement = {
    // remove redundant layers again before cross apply rewrite
    var elem = OrderByRewriter.rewrite(language, e)
    elem = UnusedColumnRemover.remove(translator.mapping, elem)
    elem = RedundantColumnRemover.remove(elem)
    elem = RedundantSubqueryRemover.remove(elem)

    // convert cross-apply joins into cross & inner joins if possible
    var rewritten = CrossApplyRewriter.rewrite(language, elem)
    // convert cross joins into inner joins
    rewritten = CrossJoinRewriter.rewrite(rewritten)

    if (rewritten ne elem) {
      elem = UnusedColumnRemover.remove(translator.mapping, rewritten)
      elem = RedundantSubqueryRemover.remove(elem)
      elem = DuplicateJoinRemover.remove(elem)
      elem = RedundantColumnRemover.remove(elem)
    }

    elem = CountDistinctMerger.merge(elem)
    rewritten = ScalarSubqueryRewriter.rewrite(language, elem)
    while (rewritten ne elem) {
      elem = UnusedColumnRemover.remove(translator.mapping, rewritten)
      elem = RedundantSubqueryRemover.remove(elem)
      rewritten = ScalarSubqueryRewriter.rewrite(language, elem)
    }

    elem
  }

  def format(e: RelationElement): FormattedQuery
}

class ProjectorChecker private () extends DbQueryTreeVisitor {
  private[this] var map = new ScopedHashMap[ParameterElement, ParameterElement](null)
  private[this] var hasExternalParameter = false
  private[this] var hasMethodElement = false

  def canBeProjector(e: RelationElement): Boolean = {
    visitElement(e)
    !hasExternalParameter && !hasMethodElement
  }

  override def visitElement(e: RelationElement): RelationElement = {
    if (hasExternalParameter || hasMethodElement) e
    else super.visitElement(e)
  }

  override protected def handleLambda(lambda: LambdaElement): RelationElement = {
    val save = map
    map = new ScopedHashMap(save)
    for (p <- lambda.parameters)
      map.put(p, p)
    visitElement(lambda.body)
    map = save
    lambda
  }

  override protected def handleParameter(element: ParameterElement): RelationElement = {
    if (!map.contains(element))
      hasExternalParameter = true
    element
  }

  override protected def handleMethod(method: MethodElement): RelationElement = {
    if (method.methodCode != QueryMethod.TAKE_DISTINCT_BYKEY)
      hasMethodElement = true
    method
  }
}

object ProjectorChecker {
  def canBeProjector(e: RelationElement): Boolean = {
    new ProjectorChecker().canBeProjector(e)
  }
}

class JoinColumnGatherer private (aliases: Set[TableAlias]) {

  val columns = new mutable.HashSet[ColumnElement]

  def gather(e: RelationElement): Unit = {
    import BinaryExpressionType._
    e match {
      case BinaryExpressionElement(EQ | NE, left, right, _) =>
        val lCol = getColumn(left)
        val rCol = getColumn(right)
        if (isExternalColumn(lCol) && (rCol ne null))
          columns.add(rCol)
        else if (isExternalColumn(rCol) && (lCol ne null))
          columns.add(lCol)

      case BinaryExpressionElement(BOOLAND, left, right, _) =>
        gather(left)
        gather(right)

      case _ =>
    }
  }

  private def getColumn(e: RelationElement): ColumnElement = {
    import UnaryExpressionType._

    e match {
      case UnaryExpressionElement(CONVERT, operand, _) => getColumn(operand)
      case c: ColumnElement                            => c
      case _                                           => null
    }
  }

  private def isExternalColumn(c: ColumnElement): Boolean = {
    (c ne null) && !aliases.contains(c.alias)
  }
}

object JoinColumnGatherer {
  def gather(aliases: Set[TableAlias], e: RelationElement): Set[ColumnElement] = {
    val g = new JoinColumnGatherer(aliases)
    g.gather(e)
    g.columns.toSet
  }
}
