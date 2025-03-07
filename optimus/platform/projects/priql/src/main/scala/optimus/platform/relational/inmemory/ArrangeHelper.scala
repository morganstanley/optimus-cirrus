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
package optimus.platform.relational.inmemory

import optimus.platform._
import optimus.platform.AsyncImplicits._
import optimus.platform.storable.RawReference

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import optimus.scalacompat.collection._

object ArrangeHelper {

  @node def arrange[T: ClassTag](src: Iterable[T], key: RelationKey[T]): Iterable[T] = {
    val size = src.size
    if (size <= 1) src
    else {
      val data = src.toArray
      val keyHashCodes = getKeyHashCodes(src, key)
      sortByKey(data, keyHashCodes, key)
    }
  }

  @node private def sortByKey[T](data: Array[T], keyHashCodes: Array[Int], key: RelationKey[T]): Iterable[T] = {

    // step one: sort on keys so that all rows with the same hashcode of key stay together
    quickSort(keyHashCodes, data)

    val dupIndexList = new ListBuffer[(Int, Int)]
    var from = 0
    while (from < data.length) {
      var to = from
      val hashCode = keyHashCodes(from)
      while (to < data.length - 1 && keyHashCodes(to + 1) == hashCode) to += 1
      if (to > from) {
        // Currently aync plugin can't handle @node call in this "if {}" properly, especially when data is large.
        // So we just store (from, to) in dupIndexList and sort key later.
        dupIndexList += ((from, to))
      }
      from = to + 1
    }

    dupIndexList.apar.map { case (from: Int, to: Int) =>
      val keys = getRangeKeys(data, from, to + 1, key)
      // step two: sort on real key since they have same key hashcode
      rangedQuickSortOnKey(data, from, to, keys, key)
    }
    data.toVector
  }

  def quickSort[T](values: Array[Int], adjoint: Array[T]): Unit = {
    def exchange(i: Int, j: Int): Unit = {
      val temp1 = values(i)
      values(i) = values(j)
      values(j) = temp1

      val temp2 = adjoint(i)
      adjoint(i) = adjoint(j)
      adjoint(j) = temp2
    }

    def quicksort(low: Int, high: Int): Unit = {
      var i = low
      var j = high
      val pivot = values(low + (high - low) / 2)

      while (i <= j) {
        while (values(i) < pivot) i += 1
        while (values(j) > pivot) j -= 1
        if (i <= j) {
          exchange(i, j)
          i += 1
          j -= 1
        }
      }
      if (low < j)
        quicksort(low, j)
      if (i < high)
        quicksort(i, high)
    }

    if (adjoint.length != 0)
      quicksort(0, adjoint.length - 1)
  }

  @node def getKeyHashCodes[T](src: Iterable[T], key: RelationKey[T]): Array[Int] = {
    def hashFor(t: Any): Int = t match {
      case ref: RawReference => java.util.Arrays.hashCode(ref.data);
      case other             => other.hashCode()
    }
    if (key.isSyncSafe) {
      val hashes = new Array[Int](src.size)
      var pos = 0
      val it = src.iterator
      while (it.hasNext) {
        hashes(pos) = hashFor(key.ofSync(it.next()))
        pos += 1
      }
      hashes
    } else
      src.apar.map(r => hashFor(key.of(r)))(Array.breakOut)
  }

  @node private def getRangeKeys[T](src: Array[T], from: Int, until: Int, key: RelationKey[T]): Array[Any] = {
    val length = until - from
    val keys = new Array[Any](length)
    var pos = 0
    while (pos < length) {
      keys(pos) = if (key.isSyncSafe) key.ofSync(src(pos + from)) else key.of(src(pos + from))
      pos += 1
    }
    keys
  }

  private def rangedQuickSortOnKey[T](
      values: Array[T],
      low: Int,
      high: Int,
      keys: Array[Any],
      key: RelationKey[T]): Unit = {
    def exchange(i: Int, j: Int): Unit = {
      val temp1 = keys(i)
      keys(i) = keys(j)
      keys(j) = temp1

      val temp2 = values(i + low)
      values(i + low) = values(j + low)
      values(j + low) = temp2
    }

    def quicksort(low: Int, high: Int): Unit = {
      var i = low
      var j = high
      val pivot = low + (high - low) / 2

      while (i <= j) {
        while (key.compareKeys(keys(i), keys(pivot)) < 0) i += 1
        while (key.compareKeys(keys(j), keys(pivot)) > 0) j -= 1
        if (i <= j) {
          exchange(i, j)
          i += 1
          j -= 1
        }
      }
      if (low < j)
        quicksort(low, j)
      if (i < high)
        quicksort(i, high)
    }

    if (values.length > 1 && keys.length > 1 && (high - low) > 0)
      quicksort(0, high - low)
  }
}
