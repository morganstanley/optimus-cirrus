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
package optimus.platform.relational.tree

import BinaryExpressionType._
import optimus.platform.relational.RelationalUnsupportedException

import scala.annotation.tailrec

/**
 * BinaryExpressionElement represents the binary expression, e.g. ( a == "xx" && b > 10)
 */
class BinaryExpressionElement(
    val op: BinaryExpressionType,
    val left: RelationElement,
    val right: RelationElement,
    typeDescriptor: TypeInfo[_])
    extends ExpressionElement(ElementType.BinaryExpression, typeDescriptor) {

  val method: MethodDescriptor = null

  override def prettyPrint(indent: Int, out: StringBuilder): Unit = {
    indentAndStar(indent, out)
    out ++= serial + " Binary Expr: '" + op + "' of\n"
    left.prettyPrint(indent + 1, out)
    right.prettyPrint(indent + 1, out)
  }

  override def toString: String = " BinaryExpressionElement[ " + left + " " + op + " " + right + " ] "
}

object BinaryExpressionElement {
  def unapply(b: BinaryExpressionElement) = Some(b.op, b.left, b.right, b.rowTypeInfo)

  def flip(expType: BinaryExpressionType.Value): BinaryExpressionType = expType match {
    case GT => LT
    case GE => LE
    case LE => GE
    case LT => GT
    // Assume everything else is symmetrical
    case x => x
  }

  def invert(op: BinaryExpressionType.Value): BinaryExpressionType = op match {
    case BOOLAND => BOOLOR
    case BOOLOR  => BOOLAND
    case NE      => EQ
    case EQ      => NE
    case LT      => GE
    case GT      => LE
    case GE      => LT
    case LE      => GT
    case _       => throw new RelationalUnsupportedException(s"Cannot invert binary op: $op")
  }

  def canonicalize(exp: BinaryExpressionElement): BinaryExpressionElement = exp match {
    case BinaryExpressionElement(expType, const @ ConstValueElement(c, _), param, typeInfo) =>
      new BinaryExpressionElement(flip(expType), param, const, typeInfo)
    case exp => exp
  }

  def balancedAnd(conds: Seq[RelationElement]): RelationElement = {
    balancedBinaryWithOp(BOOLAND, conds)
  }

  def balancedOr(conds: Seq[RelationElement]): RelationElement = {
    balancedBinaryWithOp(BOOLOR, conds)
  }

  @tailrec
  private def balancedBinaryWithOp(op: BinaryExpressionType, conds: Seq[RelationElement]): RelationElement = {
    if (conds.length == 1) {
      conds.head
    } else {
      balancedBinaryWithOp(op, conds.grouped(2).map(_.reduce(ElementFactory.makeBinary(op, _, _))).toVector)
    }
  }
}

private[tree] class MethodBinaryExpressionElement(
    op: BinaryExpressionType,
    left: RelationElement,
    right: RelationElement,
    override val method: MethodDescriptor)
    extends BinaryExpressionElement(op, left, right, method.returnType) {}

private[tree] class LogicalBinaryExpressionElement(
    op: BinaryExpressionType,
    left: RelationElement,
    right: RelationElement)
    extends BinaryExpressionElement(op, left, right, TypeInfo.BOOLEAN)
