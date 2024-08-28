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

object BinaryExpressionType extends Enumeration {

  type BinaryExpressionType = Value

  val EQ = Value(RelationalBinOp.EQ.id)
  val NE = Value(RelationalBinOp.NE.id)
  val LT = Value(RelationalBinOp.LT.id)
  val LE = Value(RelationalBinOp.LE.id)
  val GT = Value(RelationalBinOp.GT.id)
  val GE = Value(RelationalBinOp.GE.id)
  val BOOLAND = Value(RelationalBinOp.BOOLAND.id)
  val BOOLOR = Value(RelationalBinOp.BOOLOR.id)
  val PLUS = Value(RelationalBinOp.PLUS.id)
  val MINUS = Value(RelationalBinOp.MINUS.id)
  val MUL = Value(RelationalBinOp.MUL.id)
  val DIV = Value(RelationalBinOp.DIV.id)
  val MODULO = Value(RelationalBinOp.MODULO.id)
  val ASSIGN = Value(RelationalBinOp.ASSIGN.id)
  val ITEM_IS_IN = Value(RelationalBinOp.ITEM_IS_IN.id)
  val ITEM_IS_NOT_IN = Value(RelationalBinOp.ITEM_IS_NOT_IN.id)
  val COLLECTION_ELEMENT = Value(RelationalBinOp.COLLECTION_ELEMENT.id)
  val NOT_SUPPORTED = Value(RelationalBinOp.NOT_SUPPORTED.id)
}
