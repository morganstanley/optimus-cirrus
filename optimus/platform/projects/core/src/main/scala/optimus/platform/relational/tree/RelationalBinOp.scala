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

object RelationalBinOp extends Enumeration {
  type RelationalBinOp = Value

  val EQ = Value(0)

  /** "==" */
  val NE = Value(1)

  /** "!=" */
  val LT = Value(2)
  val LE = Value(3)
  val GT = Value(4)
  val GE = Value(5)
  val BOOLAND = Value(6)
  val BOOLOR = Value(7)
  val PLUS = Value(8)
  val MINUS = Value(9)
  val MUL = Value(10)
  val DIV = Value(11)
  val MODULO = Value(12)
  val ASSIGN = Value(13)

  /** for use in projections only */
  val ITEM_IS_IN = Value(14)

  /** "a in ['a','b']" */
  val COLLECTION_ELEMENT = Value(15)
  val NOT_SUPPORTED = Value(16)
  val ITEM_IS_NOT_IN = Value(17)

}
