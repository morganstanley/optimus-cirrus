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
package optimus.platform.relational.reactive

import optimus.entity.IndexInfo
import optimus.platform.pickling.Pickler
import optimus.platform.pickling.PropertyMapOutputStream
import optimus.platform.relational.tree._

trait ExpressionNormalizer {
  def normalize(
      left: RelationElement,
      op: BinaryExpressionType.Value,
      right: RelationElement): (MemberElement, BinaryExpressionType.Value, RelationElement) = {
    val ret = (left, right) match {
      case (_: MemberElement, _: MemberElement) => None
      case (l: MemberElement, _)                => Some((l, right), false)
      case (_, r: MemberElement)                => Some((r, left), true)
      case _                                    => None
    }
    val ((lhs, rhs), reversed) =
      ret.getOrElse(throw new UnsupportedFilterCondition("Filter must be on a member reference and a concrete value"))
    if (reversed) {
      (lhs, reverseOperation(op), rhs)
    } else {
      (lhs, op, rhs)
    }
  }

  private def reverseOperation(op: BinaryExpressionType.Value): BinaryExpressionType.Value = {
    import BinaryExpressionType._
    op match {
      case EQ => EQ
      case NE => NE
      case GT => LT
      case LT => GT
      case GE => LE
      case LE => GE
      case _  => throw new UnsupportedOperation("operator " + op)
    }
  }

  def pickled(b: RelationElement, pickler: Pickler[_]): Any = b match {
    case ConstValueElement(value, _) =>
      PropertyMapOutputStream.pickledValue(value, pickler.asInstanceOf[Pickler[Any]], stableOrdering = true)
    case _ => throw new IllegalStateException("Unexpected element: " + b)
  }

  def getConstOrFreevar(b: RelationElement): Any = b match {
    case ConstValueElement(value, _) => value
    case _                           => throw new IllegalStateException("Unexpected element: " + b)
  }

  def getIndexConstValue(indexInfo: IndexInfo[_, _], b: RelationElement, propName: String): Any = b match {
    case ConstValueElement(value, _) =>
      indexInfo.asInstanceOf[IndexInfo[_, Any]].makeKey(value).toSerializedKey.properties(propName)
    case _ => throw new IllegalStateException("Unexpected element: " + b)
  }
}
