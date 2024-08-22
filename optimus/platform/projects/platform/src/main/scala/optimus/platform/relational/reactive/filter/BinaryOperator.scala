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
package optimus.platform.relational.reactive.filter

object BinaryOperator extends Enumeration {
  type BinaryOperator = Value
  val EQ, NE, LT, GT, LTE, GTE, AND, OR, IS, ISNT, IN = Value

  class BinaryOperatorOps(val op: Value) {
    def repr: String = op match {
      case EQ   => "="
      case NE   => "!="
      case LT   => "<"
      case GT   => ">"
      case LTE  => "<="
      case GTE  => ">="
      case IS   => "IS"
      case ISNT => "IS NOT"
      case IN   => "IN"
    }

    def reverse: Value = op match {
      case EQ   => EQ
      case NE   => NE
      case LT   => GT
      case GT   => LT
      case LTE  => GTE
      case GTE  => LTE
      case IS   => IS
      case ISNT => ISNT
    }

    override def toString = op.toString
  }

  implicit def value2Ops(op: Value): BinaryOperatorOps = new BinaryOperatorOps(op)
  implicit def ops2Value(ops: BinaryOperatorOps): Value = ops.op
}
