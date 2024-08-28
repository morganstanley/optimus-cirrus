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

import AggregateExpressionType._

/**
 * BinaryExpressionElement represents the aggregation operation, Seq[A] => B
 */
class AggregateExpressionElement(
    val op: AggregateExpressionType,
    val fromType: TypeInfo[_],
    val aggregatedType: TypeInfo[_])
    extends ExpressionElement(ElementType.AggregateExpression, aggregatedType) {

  val method: MethodDescriptor = null

  override def prettyPrint(indent: Int, out: StringBuilder): Unit = {
    indentAndStar(indent, out)
    out ++= serial + this.toString
  }

  override def toString: String = s" AggregateExpressionElement[ $fromType $op + $aggregatedType ] "
}

object AggregateExpressionElement {

  def apply(opString: String, fromType: TypeInfo[_], aggregatedType: TypeInfo[_]): AggregateExpressionElement =
    opString match {
      case "sum" => new AggregateExpressionElement(AggregateExpressionType.SUM, fromType, aggregatedType)
      case _     => AggregateExpressionElement(fromType, aggregatedType)
    }

  def apply(fromType: TypeInfo[_], aggregatedType: TypeInfo[_]): AggregateExpressionElement =
    new AggregateExpressionElement(AggregateExpressionType.UNKNOWN, fromType, aggregatedType)

  def unapply(a: AggregateExpressionElement) = Some(a.op, a.fromType, a.aggregatedType)
}
