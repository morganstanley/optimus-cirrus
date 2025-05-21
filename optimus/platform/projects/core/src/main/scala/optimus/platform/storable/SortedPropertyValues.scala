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
package optimus.platform.storable

import optimus.scalacompat.collection.ArrayToVarArgsOps

import java.util.{Arrays => JArrays}
import scala.collection.AbstractIterator
import scala.collection.immutable.SortedSet

object SortedPropertyValues {
  val empty = new SortedPropertyValues(new Array[String](0), new Array[Any](0))

  def apply(properties: Map[String, Any]): SortedPropertyValues = apply(properties.toSeq)
  def apply(properties: Seq[(String, Any)]): SortedPropertyValues = {
    val sortedPV = properties.sortBy(_._1)
    val size = sortedPV.size
    val names = new Array[String](size)
    val values = new Array[Any](size)
    var i = 0
    val it = sortedPV.iterator
    while (it.hasNext) {
      val nv = it.next()
      names(i) = nv._1.intern
      values(i) = nv._2
      i += 1
    }
    new SortedPropertyValues(names, values)
  }

  def apply(keys: Seq[String], orgValues: Map[String, Any]): SortedPropertyValues = {
    val sortedKeys = asSortedKeys(keys)
    val values = new Array[Any](sortedKeys.length)
    var i = 0
    while (i < sortedKeys.length) {
      sortedKeys(i) = sortedKeys(i).intern
      values(i) = orgValues(sortedKeys(i))
      i += 1
    }
    new SortedPropertyValues(sortedKeys, values)
  }

  def apply(nv: (String, Any)): SortedPropertyValues = new SortedPropertyValues(Array(nv._1.intern), Array(nv._2))

  private def asSortedKeys(keys: Seq[String]): Array[String] = {
    val sortedKeys = keys.toArray
    JArrays.sort(sortedKeys.asInstanceOf[Array[AnyRef]])
    sortedKeys
  }
}

final class SortedPropertyValues(private val names: Array[String], private val vals: Array[Any])
    extends SortedPropertyValuesBase
    with Serializable {

  override def iterator: Iterator[(String, Any)] = new AbstractIterator[(String, Any)] {
    private var index = 0
    override def hasNext: Boolean = names.length > index
    override def next(): (String, Any) = {
      val i = index
      index += 1
      (names(i), vals(i))
    }
  }

  override def keySet: SortedSet[String] = SortedSet(names.toVarArgsSeq: _*)

  override def apply(name: String): Any = {
    val index = JArrays.binarySearch(names.asInstanceOf[Array[AnyRef]], name)
    vals(index)
  }

  def get(name: String): Option[Any] = {
    val index = JArrays.binarySearch(names.asInstanceOf[Array[AnyRef]], name)
    if (index >= 0)
      Some(vals(index))
    else
      None
  }

  def forAll(predicate: (String, Any) => Boolean): Boolean = {
    var i = 0
    while (i < names.length) {
      if (!predicate(names(i), vals(i)))
        return false
      i += 1
    }
    true
  }

  override def contains(name: String): Boolean = {
    val index = JArrays.binarySearch(names.asInstanceOf[Array[AnyRef]], name)
    index >= 0
  }

  // sorted by construction
  private[optimus] def sortedPropertyValues: Array[Any] = vals

  override def size: Int = names.length
  override def head: (String, Any) = (names(0), vals(0))

  override def toString: String = mkString("Map(", ", ", ")")

  def compareNames(other: SortedPropertyValues): Int = {
    if (names.length != other.names.length)
      return names.length - other.names.length
    JArrays.compare(names, other.names)
  }

  def equalNames(that: Seq[String]): Boolean = {
    val thatKeys = SortedPropertyValues.asSortedKeys(that)
    JArrays.equals(names.asInstanceOf[Array[AnyRef]], thatKeys.asInstanceOf[Array[AnyRef]])
  }
}
