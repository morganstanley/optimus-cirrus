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
package optimus.platform.relational.dynamic

// import javax.jms.IllegalStateException
import java.time.LocalDate

import optimus.platform._
import optimus.platform.relational.MapBasedDynamicObject

import scala.collection.immutable.ListMap
import scala.math.BigDecimal

final case class DynamicObjSortRule(orderByFieldsAndIsAscending: List[(String, Boolean)]) {

  /**
   * Returns x where:
   *   - x < 0 when this < that
   *   - x == 0 when this == that
   *   - x > 0 when this > that
   */
  def compare(o1: DynamicObjSortOrderable, o2: DynamicObjSortOrderable): Int = {
    orderByFieldsAndIsAscending
      .map {
        case (fieldName, isAscending) =>
          require(
            o1.obj.contains(fieldName),
            "Left does not have field:" + fieldName + ", available fields:" + o1.obj.getAll.keys)
          require(
            o2.obj.contains(fieldName),
            "Right does not have field:" + fieldName + ", available fields:" + o1.obj.getAll.keys)
          val leftO = o1.obj.get(fieldName)
          val rightO = o2.obj.get(fieldName)
          // this is a pain, I tried Numeric and Ordered, but they need explicit tyep params, so I cannot use the compare methods:(
          val ret = (leftO, rightO) match {
            case (Some(left: Any), Some(right: Any)) => compareLeftRight(left, right)
            case _                                   => compareLeftRight(leftO, rightO)
          }
          if (isAscending) ret else -1 * ret

        case _ => throw new IllegalStateException("Unexpected failure in comparing DynamicObjects")
      }
      .find(_ != 0)
      .getOrElse(0)
  }

  private def compareLeftRight(leftO: Any, rightO: Any): Int = {
    (leftO, rightO) match {
      case (left: String, right: String) => left.compareTo(right)
      // we are checking with if to avoid overflow
      case (left: Int, right: Int)               => if (left < right) -1 else if (left == right) 0 else 1
      case (left: Long, right: Long)             => if (left < right) -1 else if (left == right) 0 else 1
      case (left: Byte, right: Byte)             => if (left < right) -1 else if (left == right) 0 else 1
      case (left: Char, right: Char)             => if (left < right) -1 else if (left == right) 0 else 1
      case (left: Float, right: Float)           => if (left < right) -1 else if (left == right) 0 else 1
      case (left: Double, right: Double)         => if (left < right) -1 else if (left == right) 0 else 1
      case (left: BigDecimal, right: BigDecimal) => left.compare(right)
      case (left: BigInt, right: BigInt)         => left.compare(right)
      case (left: LocalDate, right: LocalDate)   => left.compareTo(right)
      case _ => throw new IllegalArgumentException("Cannot compare:" + leftO + " and " + rightO)
    }
  }
}

private[platform] final case class DynamicObjSortOrderable(obj: DynamicObject, rule: DynamicObjSortRule)
    extends Ordered[DynamicObjSortOrderable] {
  override def compare(that: DynamicObjSortOrderable): Int = rule.compare(this, that)
}

private[platform] object DynamicObjectHelper {
  def extendDynamicContainerObj(obj: DynamicObject, m: (String, Any)*): DynamicObject = {
    val valueMap = obj.getAll ++ ListMap[String, Any](m: _*)
    new MapBasedDynamicObject(valueMap)
  }

  def createDynamicContainerObj(m: Tuple2[String, Any]*): DynamicObject = {
    val valueMap = ListMap[String, Any](m: _*)
    new MapBasedDynamicObject(valueMap)
  }

  def createDynamicContainerObj(obj: DynamicObject, fields: Seq[String]): DynamicObject = {
    val valueMap =
      fields.foldLeft(ListMap[String, Any]())((acc, fieldName: String) => acc + (fieldName -> obj.get(fieldName)))
    new MapBasedDynamicObject(valueMap)
  }

  def createDynamicContainerObj(obj: DynamicObject, renameFields: Map[String, String]): DynamicObject = {
    val valueMap = (obj.getAll.map { case (k, v) =>
      renameFields.get(k) match {
        case Some(newFieldName) => (newFieldName -> v)
        case None               => (k, v)
      }
    }).toMap
    new MapBasedDynamicObject(valueMap)
  }
}
