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
package optimus.platform.relational.internal

import optimus.platform.internal.ValueFetch
import optimus.platform.relational.{ComparableOptions, RelationalException}
import optimus.platform.relational.tree.MemberElement
import optimus.platform.storable.Entity

class RowOrdering[T](val members: Array[MemberElement], val isAscs: Array[Boolean]) extends Ordering[T] {

  lazy val comparableOptions = new ComparableOptions

  if (isAscs.size != members.size) {
    throw new RelationalException(
      "the size of MemberElement is not equal to isAscs number when constructing RowOrdering")
  }

  override def compare(left: T, right: T): Int = {
    var re = 0 // re is var because we need assign it when traversing the whole list.
    var i = 0
    while (re == 0 && i < isAscs.length) {
      val member = members(i)
      val lValue = RelationalUtils.box(ValueFetch.getValue(left, member))
      val rValue = RelationalUtils.box(ValueFetch.getValue(right, member))
      re = (lValue, rValue) match {
        case (c1: Comparable[_], c2: Comparable[_]) => c1.asInstanceOf[Comparable[AnyRef]].compareTo(rValue)
        case (op1: Option[_], op2: Option[_]) =>
          if (comparableOptions.<(op1, op2)) -1 else if (comparableOptions.>(op1, op2)) 1 else 0
        case (l, r) => throw new IllegalArgumentException(s"can't compare types ${l.getClass} and ${r.getClass}")
      }
      if (re != 0 && !isAscs(i)) re = -re
      i += 1
    }
    re
  }
}

object TxTimeOrdering {
  private def checkAndCompare(left: Entity, right: Entity)(f: (Entity, Entity) => Int) = {
    if (left.dal$storageInfo.txTime != null && right.dal$storageInfo.txTime != null)
      f(left, right)
    else throw new RelationalException("Tx time compare only apply to stored entity")
  }
  case object TxTimeAscOrdering extends TxTimeOrdering {
    override def compare(left: Entity, right: Entity): Int = checkAndCompare(left, right) {
      (left: Entity, right: Entity) =>
        left.dal$storageInfo.txTime.compareTo(right.dal$storageInfo.txTime)
    }
  }

  case object TxTimeDescOrdering extends TxTimeOrdering {
    override def compare(left: Entity, right: Entity): Int = checkAndCompare(left, right) {
      (left: Entity, right: Entity) =>
        right.dal$storageInfo.txTime.compareTo(left.dal$storageInfo.txTime)
    }
  }
}
sealed trait TxTimeOrdering extends Ordering[Entity]
