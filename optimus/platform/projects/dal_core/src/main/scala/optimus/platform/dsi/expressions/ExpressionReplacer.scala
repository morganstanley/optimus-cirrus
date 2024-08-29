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

class ExpressionReplacer private (val searchFor: List[Expression], val replaceWith: List[Expression])
    extends ExpressionVisitor {
  if (searchFor.size != replaceWith.size)
    throw new IllegalArgumentException("Invalid argument, searchFor and replaceWith should have a same size.")

  private val lookup: Map[Expression, Expression] = searchFor.iterator.zip(replaceWith.iterator).toMap

  override def visit(expr: Expression): Expression = {
    lookup.getOrElse(expr, super.visit(expr))
  }
}

object ExpressionReplacer {
  def replace(e: Expression, searchFor: List[Expression], replaceWith: List[Expression]): Expression = {
    new ExpressionReplacer(searchFor, replaceWith).visit(e)
  }
}
