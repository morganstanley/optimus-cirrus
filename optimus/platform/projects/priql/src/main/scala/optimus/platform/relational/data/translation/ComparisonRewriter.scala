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

import optimus.platform.cm.Knowable
import optimus.platform.cm.Known
import optimus.platform.cm.NotApplicable
import optimus.platform.cm.Unknown
import optimus.platform.relational.AsyncValueHolder
import optimus.platform.relational.RelationalException
import optimus.platform.relational.data.tree.AggregateElement
import optimus.platform.relational.data.tree.ContainsElement
import optimus.platform.relational.data.tree.DbQueryTreeVisitor
import optimus.platform.relational.data.tree.KnowableValueElement
import optimus.platform.relational.data.tree.NamedValueElement
import optimus.platform.relational.data.tree.OptionElement
import optimus.platform.relational.data.tree.TupleElement
import optimus.platform.relational.tree.BinaryExpressionElement
import optimus.platform.relational.tree.BinaryExpressionType._
import optimus.platform.relational.tree.ConditionalElement
import optimus.platform.relational.tree.ConstValueElement
import optimus.platform.relational.tree.ElementFactory
import optimus.platform.relational.tree.RelationElement
import optimus.platform.relational.tree.RuntimeMethodDescriptor
import optimus.platform.relational.tree.TypeInfo

import scala.annotation.tailrec

class ComparisonRewriter extends DbQueryTreeVisitor {
  import ComparisonRewriter._

  protected override def handleBinaryExpression(binary: BinaryExpressionElement): RelationElement = {
    val op = binary.op
    if (op == EQ || op == NE) {
      (binary.left, binary.right) match {
        // tuple compare with tuple
        case (TupleElement(elems), ConstValueElement(value: Product, _)) =>
          visitElement(rewriteTupleCompare(elems, value, op))
        case (ConstValueElement(value: Product, _), TupleElement(elems)) =>
          visitElement(rewriteTupleCompare(elems, value, op))
        case (TupleElement(elems1), TupleElement(elems2)) => visitElement(rewriteTupleCompare(elems1, elems2, op))
        // option compare with option
        case (OptionElement(e), ConstValueElement(value, _)) =>
          visitElement(rewriteOptionCompare(e, value, op, binary.left.rowTypeInfo))
        case (ConstValueElement(value, _), OptionElement(e)) =>
          visitElement(rewriteOptionCompare(e, value, op, binary.right.rowTypeInfo))
        case (OptionElement(e1), OptionElement(e2)) => visitElement(rewriteOptionCompare(e1, e2, op))
        case (OptionElement(e), nv @ NamedValueElement(_, OptionElement(_))) =>
          visitElement(ElementFactory.makeBinary(op, e, OptionValueElement(nv)))
        case (nv @ NamedValueElement(_, OptionElement(_)), OptionElement(e)) =>
          visitElement(ElementFactory.makeBinary(op, e, OptionValueElement(nv)))
        case (e1, e2) if e1.rowTypeInfo <:< classOf[Knowable[_]] && e2.rowTypeInfo <:< classOf[Knowable[_]] =>
          (e1, e2) match {
            case (ConstValueElement(kn: Knowable[_], _), e) => visitElement(rewriteKnowableCompare(e, kn, op))
            case (e, ConstValueElement(kn: Knowable[_], _)) => visitElement(rewriteKnowableCompare(e, kn, op))
            case _ =>
              val t = KnowableElement.typeEquals(e1, e2)
              val l = if (op == EQ) t else ElementFactory.makeBinary(EQ, t, ElementFactory.constant(false))
              val kvOpt1 = OptionElement(KnowableValueElement(e1))
              val kvOpt2 = OptionElement(KnowableValueElement(e2))
              val r = ElementFactory.makeBinary(op, kvOpt1, kvOpt2)
              visitElement(ElementFactory.makeBinary(if (op == EQ) BOOLAND else BOOLOR, l, r))
          }
        // double.IsNaN
        case (ConstValueElement(value: Double, _), _) if java.lang.Double.isNaN(value) =>
          ElementFactory.constant(op == NE, TypeInfo.BOOLEAN)
        case (_, ConstValueElement(value: Double, _)) if java.lang.Double.isNaN(value) =>
          ElementFactory.constant(op == NE, TypeInfo.BOOLEAN)
        case (l, ConstValueElement(_: Option[_], _)) if !TypeInfo.isOption(l.rowTypeInfo) =>
          ElementFactory.constant(op == NE, TypeInfo.BOOLEAN)
        case (ConstValueElement(_: Option[_], _), r) if !TypeInfo.isOption(r.rowTypeInfo) =>
          ElementFactory.constant(op == NE, TypeInfo.BOOLEAN)
        case _ => super.handleBinaryExpression(binary)
      }
    } else {
      super.handleBinaryExpression(binary)
    }
  }

  private def rewriteKnowableCompare(e: RelationElement, kn: Knowable[_], op: BinaryExpressionType): RelationElement = {
    val kv = KnowableValueElement(e)
    kn match {
      case Known(x) =>
        ElementFactory.makeBinary(op, kv, ElementFactory.constant(x, kv.rowTypeInfo))
      case Unknown() if op == EQ =>
        val l = ElementFactory.makeBinary(EQ, kv, ElementFactory.constant(null, kv.rowTypeInfo))
        val r = KnowableElement.isUnknown(e)
        ElementFactory.makeBinary(BOOLAND, l, r)
      case Unknown() if op == NE =>
        ElementFactory.makeBinary(EQ, KnowableElement.isUnknown(e), ElementFactory.constant(false))
      case NotApplicable if op == EQ =>
        val l = ElementFactory.makeBinary(EQ, kv, ElementFactory.constant(null, kv.rowTypeInfo))
        val r = KnowableElement.isNotApplicable(e)
        ElementFactory.makeBinary(BOOLAND, l, r)
      case NotApplicable if op == NE =>
        ElementFactory.makeBinary(EQ, KnowableElement.isNotApplicable(e), ElementFactory.constant(false))
      case v => throw new MatchError(v)
    }
  }

  private def rewriteOptionCompare(
      e: RelationElement,
      value: Any,
      op: BinaryExpressionType,
      optionType: TypeInfo[_]): RelationElement = {
    value match {
      case None | _: AsyncValueHolder[_] =>
        val constant = ElementFactory.constant(value, optionType)
        ElementFactory.makeBinary(op, e, wrapConstantAsOptionValue(constant))
      case Some(_: Option[_]) if !TypeInfo.isOption(e.rowTypeInfo) =>
        ElementFactory.constant(op == NE, TypeInfo.BOOLEAN)
      case Some(_) =>
        val constant = ElementFactory.constant(value, optionType)
        ElementFactory.makeBinary(op, e, wrapConstantAsOptionValue(constant))
      case _ => ElementFactory.constant(op == NE, TypeInfo.BOOLEAN)
    }
  }

  private def rewriteOptionCompare(
      e1: RelationElement,
      e2: RelationElement,
      op: BinaryExpressionType): RelationElement = {
    val compare = ElementFactory.makeBinary(op, e1, e2)
    if (TypeInfo.underlying(e1.rowTypeInfo) eq e1.rowTypeInfo) {
      val e1check = ElementFactory.makeBinary(op, e1, ElementFactory.constant(null, e1.rowTypeInfo))
      val e2check = ElementFactory.makeBinary(op, e2, ElementFactory.constant(null, e2.rowTypeInfo))
      val nullCheck = ElementFactory.makeBinary(if (op == EQ) BOOLAND else BOOLOR, e1check, e2check)
      ElementFactory.makeBinary(if (op == EQ) BOOLOR else BOOLAND, compare, nullCheck)
    } else compare
  }

  private def rewriteTupleCompare(
      elements: List[RelationElement],
      value: Product,
      op: BinaryExpressionType): RelationElement = {
    val valueList = value.productIterator.toList
    if (elements.length == valueList.length) {
      val boolOp = if (op == EQ) BOOLAND else BOOLOR
      elements zip valueList map { case (e, v) =>
        ElementFactory.makeBinary(op, e, ElementFactory.constant(v, e.rowTypeInfo))
      } reduce ((l, r) => ElementFactory.makeBinary(boolOp, l, r))
    } else {
      ElementFactory.constant(op == NE, TypeInfo.BOOLEAN)
    }
  }

  private def rewriteTupleCompare(
      elements1: List[RelationElement],
      elements2: List[RelationElement],
      op: BinaryExpressionType): RelationElement = {
    if (elements1.length == elements2.length) {
      val boolOp = if (op == EQ) BOOLAND else BOOLOR
      elements1 zip elements2 map { case (e1, e2) =>
        ElementFactory.makeBinary(op, e1, e2)
      } reduce ((l, r) => ElementFactory.makeBinary(boolOp, l, r))
    } else {
      ElementFactory.constant(op == NE, TypeInfo.BOOLEAN)
    }
  }

  override def handleOptionElement(option: OptionElement): RelationElement = {
    visitElement(option.element)
  }

  override protected def handleContains(contains: ContainsElement): RelationElement = {
    contains.element match {
      case TupleElement(elems) =>
        // 'contains.values' should be Right(List[ConstValueElement]) since there is only one column in subquery.
        contains.values match {
          case Left(v) =>
            throw new RelationalException("Expect ConstValueElement list in ContainsElement but got ScalarElement")
          case Right(v) =>
            BinaryExpressionElement.balancedOr(v.map { case ConstValueElement(value: Product, _) =>
              visitElement(rewriteTupleCompare(elems, value, EQ))
            })
        }

      case OptionElement(element) =>
        contains.values match {
          case Left(_)  => super.handleContains(contains)
          case Right(v) => new ContainsElement(element, Right(v.map(v => wrapConstantAsOptionValue(v))))
        }

      case _ => super.handleContains(contains)
    }
  }

  override def handleAggregate(aggregate: AggregateElement): RelationElement = {
    super.handleAggregate(aggregate) match {
      case AggregateElement(aggType, "countOption", args, isDistinct) =>
        new AggregateElement(aggType, "count", args, isDistinct)
      case x => x
    }
  }

  protected def wrapConstantAsOptionValue(e: RelationElement): RelationElement = OptionValueElement(e)
}

object ComparisonRewriter {
  def rewrite(e: RelationElement): RelationElement = {
    val elem = new ComparisonRewriter().visitElement(e)
    elem
  }

  def rewriteGroupKey(e: RelationElement): RelationElement = {
    val rewriter = new ComparisonRewriter() {
      override def wrapConstantAsOptionValue(e: RelationElement): RelationElement = e match {
        case ConstValueElement(v: Option[_], typeInfo) =>
          ElementFactory.constant(v.getOrElse(null), typeInfo.typeParams.head)
        case _ => OptionValueElement(e)
      }
      override def handleConditional(c: ConditionalElement): RelationElement = {
        super.handleConditional(c) match {
          case ConditionalElement(test, OptionElement(o1), OptionElement(o2), _) =>
            OptionElement(ElementFactory.condition(test, o1, o2, o1.rowTypeInfo))
          case ConditionalElement(test, OptionElement(o), ConstValueElement(v: Option[_], _), _) =>
            val value = ElementFactory.constant(v.getOrElse(null), o.rowTypeInfo)
            OptionElement(ElementFactory.condition(test, o, value, o.rowTypeInfo))
          case ConditionalElement(test, ConstValueElement(v: Option[_], _), OptionElement(o), _) =>
            val value = ElementFactory.constant(v.getOrElse(null), o.rowTypeInfo)
            OptionElement(ElementFactory.condition(test, value, o, o.rowTypeInfo))
          case ConditionalElement(test, ConstValueElement(v1: Option[_], _), ConstValueElement(v2: Option[_], _), ti) =>
            val typeInfo = ti.typeParams.head
            val value1 = ElementFactory.constant(v1.getOrElse(null), typeInfo)
            val value2 = ElementFactory.constant(v2.getOrElse(null), typeInfo)
            OptionElement(ElementFactory.condition(test, value1, value2, typeInfo))
          case x => x
        }
      }
      override def handleOptionElement(option: OptionElement): RelationElement = {
        val e = visitElement(option.element)
        updateOption(option, e)
      }
    }
    rewriter.visitElement(e)
  }

  object KnowableElement {
    def isUnknown(kn: RelationElement): RelationElement = {
      val method = new RuntimeMethodDescriptor(kn.rowTypeInfo, "isUnknown", TypeInfo.BOOLEAN)
      ElementFactory.call(kn, method, Nil)
    }

    def isNotApplicable(kn: RelationElement): RelationElement = {
      val method = new RuntimeMethodDescriptor(kn.rowTypeInfo, "isNotApplicable", TypeInfo.BOOLEAN)
      ElementFactory.call(kn, method, Nil)
    }

    def typeEquals(kn1: RelationElement, kn2: RelationElement): RelationElement = {
      val method = new RuntimeMethodDescriptor(kn1.rowTypeInfo, "typeEquals", TypeInfo.BOOLEAN)
      ElementFactory.call(kn1, method, List(kn2))
    }
  }

  object OptionValueElement {
    // internal way to express the value (nullable) of an option
    // only used to wrap a NamedValueElement, will be handled before sending to server
    def apply(o: RelationElement): RelationElement = {
      val returnType = o.rowTypeInfo.typeParams.head
      val method = new RuntimeMethodDescriptor(o.rowTypeInfo, "value", returnType)
      ElementFactory.call(o, method, Nil)
    }
  }
}
