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

import UnaryExpressionType._

class UnaryExpressionElement(val op: UnaryExpressionType, val element: RelationElement, typeDescriptor: TypeInfo[_])
    extends ExpressionElement(ElementType.UnaryExpression, typeDescriptor) {
  override def prettyPrint(indent: Int, out: StringBuilder): Unit = {
    indentAndStar(indent, out)
    out ++= serial + " Unary Expr: '" + op + "' of\n"
    element.prettyPrint(indent + 1, out)
  }

  override def toString: String = " UnaryExpressionElement[ " + op + " " + element + " ] "

  override def equals(that: Any): Boolean = that match {
    case that: UnaryExpressionElement => super.equals(that) && op == that.op && element == that.element
    case _                            => false
  }

  override def hashCode: Int = (super.hashCode * 29 + op.hashCode) * 31 + element.hashCode
}

object UnaryExpressionElement {
  def unapply(element: UnaryExpressionElement) = Some(element.op, element.element, element.rowTypeInfo)
}
