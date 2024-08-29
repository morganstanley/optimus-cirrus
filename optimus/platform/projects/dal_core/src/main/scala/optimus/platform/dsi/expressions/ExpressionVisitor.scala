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
package optimus.platform.dsi.expressions

import scala.collection.mutable.ListBuffer

class ExpressionVisitor {

  def visit(ex: Expression): Expression = {
    if (ex eq null) null
    else
      ex match {
        case e: Entity                => visitEntity(e)
        case e: Event                 => visitEvent(e)
        case e: Linkage               => visitLinkage(e)
        case e: Embeddable            => visitEmbeddable(e)
        case e: Property              => visitProperty(e)
        case e: Constant              => visitConstant(e)
        case e: Binary                => visitBinary(e)
        case e: In                    => visitIn(e)
        case e: Select                => visitSelect(e)
        case e: Join                  => visitJoin(e)
        case e: Member                => visitMember(e)
        case e: Function              => visitFunction(e)
        case e: Unary                 => visitUnary(e)
        case e: Condition             => visitCondition(e)
        case e: Aggregate             => visitAggregate(e)
        case e: Scalar                => visitScalar(e)
        case e: Exists                => visitExists(e)
        case c: EntityBitemporalSpace => visitEntityBitemporalSpace(c)
        case _                        => throw new IllegalArgumentException(s"Invalid expression: $ex")
      }
  }

  protected def visitEntityBitemporalSpace(e: EntityBitemporalSpace): Expression = {
    e
  }

  protected def visitEntity(e: Entity): Expression = {
    e
  }

  protected def visitEvent(e: Event): Expression = {
    e
  }

  protected def visitLinkage(e: Linkage): Expression = {
    e
  }

  protected def visitEmbeddable(e: Embeddable): Expression = {
    e
  }

  protected def visitProperty(p: Property): Expression = {
    p
  }

  protected def visitConstant(c: Constant): Expression = {
    c
  }

  protected def visitBinary(b: Binary): Expression = {
    val l = visit(b.left)
    val r = visit(b.right)
    updateBinary(b, l, r)
  }

  final protected def updateBinary(origin: Binary, left: Expression, right: Expression): Binary = {
    if ((left eq origin.left) && (right eq origin.right))
      origin
    else
      Binary(origin.op, left, right)
  }

  protected def visitIn(i: In): Expression = {
    val e = visit(i.e)
    val values = i.values match {
      case l @ Left(s) =>
        val select = visit(s).asInstanceOf[Select]
        if (select eq s) l else Left(select)
      case r @ Right(l) =>
        val list = visitExpressionList(l)
        if (list eq l) r else Right(list)
    }
    updateIn(i, e, values)
  }

  final protected def updateIn(origin: In, e: Expression, values: Either[Select, List[Expression]]): In = {
    if ((origin.e eq e) && (origin.values eq values))
      origin
    else
      In(e, values)
  }

  protected def visitSelect(s: Select): Expression = {
    val from = visit(s.from).asInstanceOf[QuerySource]
    val props = visitList(s.properties, visitPropertyDef)
    val where = visitOption(s.where)
    val sortBy = visitList(s.sortBy, visitSortByDef)
    val groupBy = visitExpressionList(s.groupBy)
    val skip = visitOption(s.skip)
    val take = visitOption(s.take)
    updateSelect(s, from, props, where, sortBy, groupBy, skip, take)
  }

  final protected def updateSelect(
      origin: Select,
      from: QuerySource,
      props: List[PropertyDef],
      where: Option[Expression],
      sortBy: List[SortByDef],
      groupBy: List[Expression],
      skip: Option[Expression],
      take: Option[Expression]): Select = {
    if (
      (origin.from eq from) && (origin.properties eq props) && (origin.where eq where) && (origin.sortBy eq sortBy)
      && (origin.groupBy eq groupBy) && (origin.skip eq skip) && (origin.take eq take)
    ) {
      origin
    } else {
      Select(from, props, where, sortBy, groupBy, take, skip, origin.isDistinct, origin.reverse, origin.id)
    }
  }

  protected def visitJoin(j: Join): Expression = {
    val l = visit(j.left).asInstanceOf[QuerySource]
    val r = visit(j.right).asInstanceOf[QuerySource]
    val on = j.on.map(visit)
    updateJoin(j, l, r, on)
  }

  final protected def updateJoin(origin: Join, left: QuerySource, right: QuerySource, on: Option[Expression]): Join = {
    if ((origin.left eq left) && (origin.right eq right)) {
      val identicalOn = (origin.on, on) match {
        case (Some(a), Some(b)) => a eq b
        case (None, None)       => true
        case _                  => false
      }
      if (identicalOn) origin
      else Join(origin.joinType, left, right, on)
    } else {
      Join(origin.joinType, left, right, on)
    }
  }

  protected def visitMember(m: Member): Expression = {
    val o = visit(m.owner)
    updateMember(m, o)
  }

  final protected def updateMember(origin: Member, owner: Expression): Member = {
    if (origin.owner eq owner)
      origin
    else
      Member(origin.name, owner)
  }

  protected def visitFunction(f: Function): Expression = {
    val args = visitExpressionList(f.arguments)
    updateFunction(f, args)
  }

  final protected def updateFunction(origin: Function, arguments: List[Expression]): Function = {
    if (origin.arguments eq arguments)
      origin
    else
      Function(origin.method, arguments)
  }

  protected def visitUnary(u: Unary): Expression = {
    val e = visit(u.e)
    updateUnary(u, e)
  }

  final protected def updateUnary(origin: Unary, e: Expression): Unary = {
    if (origin.e eq e)
      origin
    else
      Unary(origin.op, e)
  }

  protected def visitCondition(c: Condition): Expression = {
    val check = visit(c.check)
    val ifTrue = visit(c.ifTrue)
    val ifFalse = visit(c.ifFalse)
    updateCondition(c, check, ifTrue, ifFalse)
  }

  final protected def updateCondition(
      origin: Condition,
      check: Expression,
      ifTrue: Expression,
      ifFalse: Expression): Condition = {
    if ((origin.check eq check) && (origin.ifTrue eq ifTrue) && (origin.ifFalse eq ifFalse))
      origin
    else
      Condition(check, ifTrue, ifFalse)
  }

  protected def visitAggregate(a: Aggregate): Expression = {
    val arguments = visitExpressionList(a.arguments)
    updateAggregate(a, arguments)
  }

  final protected def updateAggregate(origin: Aggregate, arguments: List[Expression]): Aggregate = {
    if (origin.arguments eq arguments)
      origin
    else
      Aggregate(origin.aggregateType, arguments, origin.isDistinct)
  }

  protected def visitScalar(s: Scalar): Expression = {
    val q = visit(s.query).asInstanceOf[Select]
    updateScalar(s, q)
  }

  final protected def updateScalar(origin: Scalar, query: Select): Scalar = {
    if (origin.query eq query)
      origin
    else
      Scalar(query)
  }

  protected def visitExists(e: Exists): Expression = {
    val q = visit(e.query).asInstanceOf[Select]
    updateExists(e, q)
  }

  final protected def updateExists(origin: Exists, query: Select): Exists = {
    if (origin.query eq query)
      origin
    else
      Exists(query)
  }

  final protected def visitList[T <: AnyRef](list: List[T], f: T => T): List[T] = {
    var count = 0
    var listBuffer: ListBuffer[T] = null
    for (e <- list) {
      val elem = f(e)
      if (listBuffer != null)
        listBuffer += elem
      else if (elem ne e) {
        listBuffer = new ListBuffer[T]()
        listBuffer ++= list.take(count)
        listBuffer += elem
      }
      count += 1
    }
    if (listBuffer != null) listBuffer.result() else list
  }

  final protected def visitExpressionList(list: List[Expression]): List[Expression] = {
    visitList(list, visit)
  }

  final protected def visitOption(o: Option[Expression]): Option[Expression] = {
    o match {
      case Some(ex) =>
        val e = visit(ex)
        if (e eq ex) o else Some(e)
      case _ => o
    }
  }

  protected def visitPropertyDef(prop: PropertyDef): PropertyDef = {
    val e = visit(prop.e)
    if (e eq prop.e) prop else PropertyDef(prop.name, e)
  }

  protected def visitSortByDef(sortBy: SortByDef): SortByDef = {
    val e = visit(sortBy.e)
    if (e eq sortBy.e) sortBy else SortByDef(sortBy.sortType, e)
  }
}
