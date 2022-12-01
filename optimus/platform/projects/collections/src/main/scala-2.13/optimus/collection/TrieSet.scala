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

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

/** A [[scala.collection.concurrent.TrieMap]] backed concurrent [[Set]]. */
class TrieSet[Elem] extends mutable.AbstractSet[Elem] {
  private[this] val map: TrieMap[Elem, Unit] = new TrieMap[Elem, Unit]()

  override def addOne(elem: Elem): TrieSet.this.type = {
    map.addOne((elem, ()))
    this
  }

  override def subtractOne(elem: Elem): TrieSet.this.type = {
    map subtractOne elem
    this
  }

  override def contains(elem: Elem): Boolean = map.contains(elem)

  override def iterator: Iterator[Elem] = map.keysIterator

  override def clear(): Unit = map.clear()
}
