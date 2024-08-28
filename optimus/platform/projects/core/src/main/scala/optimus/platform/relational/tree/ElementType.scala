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

/**
 * sub case object of this trait stands for node type of RelationElement tree which is build from user query
 */
trait ElementType

object ElementType {
  case object Method extends ElementType
  case object BinaryExpression extends ElementType
  case object MemberRef extends ElementType
  case object Parameter extends ElementType
  case object ConstValue extends ElementType
  case object UnaryExpression extends ElementType
  case object Provider extends ElementType
  case object ExpressionList extends ElementType
  case object ForteFuncCall extends ElementType
  case object TernaryExpression extends ElementType
  case object New extends ElementType
  case object Lambda extends ElementType
  case object TypeIs extends ElementType

  case object AggregateExpression extends ElementType
}
