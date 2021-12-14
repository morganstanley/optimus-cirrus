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
package optimus.collection

private[collection] object OptimusSeqSupport {
  def foreach[T, U](array: Array[AnyRef], f: T => U): Unit = {
    var i = 0
    while (i < array.length) {
      f(array(i).asInstanceOf[T])
      i += 1
    }
  }

  def forall[T](array: Array[AnyRef], p: T => Boolean): Boolean = {
    var i = 0
    while (i < array.length) {
      if (!p(array(i).asInstanceOf[T]))
        return false
      i += 1
    }
    true
  }

  def indexOf[T](array: Array[AnyRef], offset: Int, startIndex: Int, elem: T): Int = {
    var i = startIndex
    while (i < array.length) {
      if (elem == array(i))
        return i + offset
      i += 1
    }
    -1
  }

  def lastIndexOf[T](array: Array[AnyRef], offset: Int, endIndex: Int, elem: T): Int = {
    var i = Math.min(array.length - 1, endIndex)
    while (i >= 0) {
      if (elem == array(i))
        return i + offset
      i -= 1
    }
    -1
  }

  def indexWhere[T](array: Array[AnyRef], offset: Int, startIndex: Int, f: T => Boolean): Int = {
    var i = startIndex
    while (i < array.length) {
      if (f(array(i).asInstanceOf[T]))
        return i + offset
      i += 1
    }
    -1
  }

  def lastIndexWhere[T](array: Array[AnyRef], offset: Int, endIndex: Int, f: T => Boolean): Int = {
    var i = Math.min(array.length - 1, endIndex)
    while (i >= 0) {
      if (f(array(i).asInstanceOf[T]))
        return i + offset
      i -= 1
    }
    -1
  }

  def foldLeft[T, B](array: Array[AnyRef], initial: B, op: (B, T) => B): B = {
    var i = 0
    var res = initial
    while (i < array.length) {
      res = op(res, array(i).asInstanceOf[T])
      i += 1
    }
    res
  }
}
