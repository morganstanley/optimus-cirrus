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

import optimus.platform.relational.tree.ScopedHashMap

import scala.collection.mutable

class ExpressionComparer protected (protected var idsScope: ScopedHashMap[Id, Id]) {

  def compare(a: Expression, b: Expression): Boolean = {
    if (a eq b) true
    else if (a == null || b == null) false
    else
      (a, b) match {
        case (ea: Entity, eb: Entity)                               => compareEntity(ea, eb)
        case (ea: EntityBitemporalSpace, eb: EntityBitemporalSpace) => compareEntityBitempSpace(ea, eb)
        case (ea: Event, eb: Event)                                 => compareEvent(ea, eb)
        case (la: Linkage, lb: Linkage)                             => compareLinkage(la, lb)
        case (ea: Embeddable, eb: Embeddable)                       => compareEmbeddable(ea, eb)
        case (pa: Property, pb: Property)                           => compareProperty(pa, pb)
        case (ca: Constant, cb: Constant)                           => compareConstant(ca, cb)
        case (ba: Binary, bb: Binary)                               => compareBinary(ba, bb)
        case (ia: In, ib: In)                                       => compareIn(ia, ib)
        case (sa: Select, sb: Select)                               => compareSelect(sa, sb)
        case (ja: Join, jb: Join)                                   => compareJoin(ja, jb)
        case (ma: Member, mb: Member)                               => compareMember(ma, mb)
        case (fa: Function, fb: Function)                           => compareFunction(fa, fb)
        case (ua: Unary, ub: Unary)                                 => compareUnary(ua, ub)
        case (ca: Condition, cb: Condition)                         => compareCondition(ca, cb)
        case (aa: Aggregate, ab: Aggregate)                         => compareAggregate(aa, ab)
        case (sa: Scalar, sb: Scalar)                               => compareScalar(sa, sb)
        case (ea: Exists, eb: Exists)                               => compareExists(ea, eb)
        case _                                                      => false
      }
  }

  protected def compareOption(aOption: Option[Expression], bOption: Option[Expression]): Boolean = {
    (aOption, bOption) match {
      case (Some(a), Some(b)) => compare(a, b)
      case (None, None)       => true
      case _                  => false
    }
  }

  protected def compareEntityBitempSpace(a: EntityBitemporalSpace, b: EntityBitemporalSpace): Boolean = {
    val sortedA = a.superClasses.sorted
    val sortedB = b.superClasses.sorted
    a.name == b.name && a.kind == b.kind && a.when == b.when && sortedA == sortedB
  }

  protected def compareEntity(a: Entity, b: Entity): Boolean = {
    val sortedA = a.superClasses.sorted
    val sortedB = b.superClasses.sorted
    a.name == b.name && a.when == b.when && sortedA == sortedB
  }

  protected def compareEvent(a: Event, b: Event): Boolean = {
    a.name == b.name && a.when == b.when
  }

  protected def compareLinkage(a: Linkage, b: Linkage): Boolean = {
    a.name == b.name && a.when == b.when
  }

  protected def compareEmbeddable(a: Embeddable, b: Embeddable): Boolean = {
    a.entity == b.entity && a.property == b.property && a.tag == b.tag
  }

  protected def compareProperty(a: Property, b: Property): Boolean = {
    a.propType == b.propType && a.names == b.names && compareId(a.owner, b.owner)
  }

  protected def compareId(a: Id, b: Id): Boolean = {
    if (idsScope ne null) {
      val mapped = idsScope.getOrElse(a, null)
      if (mapped ne null) mapped == b else a == b
    } else a == b
  }

  protected def compareConstant(a: Constant, b: Constant): Boolean = {
    a.value == b.value && a.typeCode == b.typeCode
  }

  protected def compareBinary(a: Binary, b: Binary): Boolean = {
    a.op == b.op && compare(a.left, b.left) && compare(a.right, b.right)
  }

  protected def compareIn(a: In, b: In): Boolean = {
    compare(a.e, b.e) && {
      if (a.values.isLeft && b.values.isLeft) compare(a.values.left.get, b.values.left.get)
      else if (a.values.isRight && b.values.isRight) compareExpressionList(a.values.right.get, b.values.right.get)
      else false
    }
  }

  protected def compareSelect(a: Select, b: Select): Boolean = {
    val save = idsScope
    try {
      compare(a.from, b.from) && {
        idsScope = new ScopedHashMap(save)
        mapIds(a.from, b.from)
        comparePropertyDefList(a.properties, b.properties) && compareExpressionOption(a.where, b.where) &&
        compareSortByDefList(a.sortBy, b.sortBy) && compareExpressionList(
          a.groupBy,
          b.groupBy) && compareExpressionOption(a.take, b.take) &&
        compareExpressionOption(a.skip, b.skip) && a.isDistinct == b.isDistinct && a.reverse == b.reverse
      }
    } finally {
      idsScope = save
    }
  }

  private def mapIds(a: QuerySource, b: QuerySource): Unit = {
    val prodA = DeclaredIdGatherer.gather(a)
    val prodB = DeclaredIdGatherer.gather(b)
    for ((a1, a2) <- prodA.zip(prodB)) {
      idsScope.put(a1, a2)
    }
  }

  protected def compareJoin(a: Join, b: Join): Boolean = {
    val save = idsScope
    try {
      a.joinType == b.joinType && compare(a.left, b.left) && compare(a.right, b.right) && {
        idsScope = new ScopedHashMap(save)
        mapIds(a.left, b.left)
        mapIds(a.right, b.right)
        compareOption(a.on, b.on)
      }
    } finally {
      idsScope = save
    }
  }

  protected def compareMember(a: Member, b: Member): Boolean = {
    a.name == b.name && compare(a.owner, b.owner)
  }

  protected def compareFunction(a: Function, b: Function): Boolean = {
    a.method == b.method && compareExpressionList(a.arguments, b.arguments)
  }

  protected def compareUnary(a: Unary, b: Unary): Boolean = {
    a.op == b.op && compare(a.e, b.e)
  }

  protected def compareCondition(a: Condition, b: Condition): Boolean = {
    compare(a.check, b.check) && compare(a.ifTrue, b.ifTrue) && compare(a.ifFalse, b.ifFalse)
  }

  protected def compareAggregate(a: Aggregate, b: Aggregate): Boolean = {
    a.aggregateType == b.aggregateType &&
    compareExpressionList(a.arguments, b.arguments) &&
    a.isDistinct == b.isDistinct
  }

  protected def compareScalar(a: Scalar, b: Scalar): Boolean = {
    compare(a.query, b.query)
  }

  protected def compareExists(a: Exists, b: Exists): Boolean = {
    compare(a.query, b.query)
  }

  protected def compareExpressionList(a: List[Expression], b: List[Expression]): Boolean = {
    if (a eq b) true
    else if (a == null || b == null) false
    else if (a.size != b.size) false
    else {
      a.zip(b).forall(t => compare(t._1, t._2))
    }
  }

  protected def compareExpressionOption(a: Option[Expression], b: Option[Expression]): Boolean = {
    (a, b) match {
      case (Some(e1), Some(e2)) => compare(e1, e2)
      case (None, None)         => true
      case _                    => false
    }
  }

  protected def comparePropertyDefList(a: List[PropertyDef], b: List[PropertyDef]): Boolean = {
    if (a eq b) true
    else if (a == null || b == null) false
    else if (a.size != b.size) false
    else {
      a.zip(b).forall { case (p1, p2) =>
        p1.name == p2.name && compare(p1.e, p2.e)
      }
    }
  }

  protected def compareSortByDefList(a: List[SortByDef], b: List[SortByDef]): Boolean = {
    if (a eq b) true
    else if (a == null || b == null) false
    else if (a.size != b.size) false
    else {
      a.zip(b).forall { case (p1, p2) =>
        p1.sortType == p2.sortType && compare(p1.e, p2.e)
      }
    }
  }
}

object ExpressionComparer {
  def areEqual(a: Expression, b: Expression): Boolean = {
    areEqual(null, a, b)
  }

  def areEqual(idsScope: ScopedHashMap[Id, Id], a: Expression, b: Expression): Boolean = {
    new ExpressionComparer(idsScope).compare(a, b)
  }
}

private class DeclaredIdGatherer private () extends ExpressionVisitor {
  val ids = new mutable.HashSet[Id]

  protected override def visitSelect(select: Select): Expression = {
    ids.add(select.id)
    select
  }

  protected override def visitEntity(entity: Entity): Expression = {
    ids.add(entity.id)
    entity
  }

  protected override def visitEntityBitemporalSpace(e: EntityBitemporalSpace): Expression = {
    ids.add(e.id)
    e
  }

  protected override def visitEvent(event: Event): Expression = {
    ids.add(event.id)
    event
  }

  protected override def visitLinkage(linkage: Linkage): Expression = {
    ids.add(linkage.id)
    linkage
  }
}

private object DeclaredIdGatherer {
  def gather(element: QuerySource): mutable.HashSet[Id] = {
    val gatherer = new DeclaredIdGatherer()
    gatherer.visit(element)
    gatherer.ids
  }
}
