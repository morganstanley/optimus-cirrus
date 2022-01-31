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
package optimus.breadcrumbs.util

import java.util

/**
 * <p>Map implementation with maximum number of keys enforced. If adding a
 * key / value pair would push the size of the map above the maximum size, then
 * the pair having the key that was least recently used would be evicted.</p>
 *
 * @param maxSize the maximum number of key / value pairs permitted in the map
 * @tparam K the type of keys maintained by this map
 * @tparam V the type of mapped values
 */
class LimitedSizeConcurrentHashMap[K, V](maxSize: Int) {
  require(maxSize > 0, s"maxSize must be > 0, but got $maxSize")

  protected object Lock

  protected val m: util.HashMap[K, V] = new util.LinkedHashMap[K, V]()
  protected val touchedKeys: util.LinkedList[K] = new util.LinkedList[K]

  // Move key to front such that least recently used is the last element.
  private[this] def moveKeyToFront(key: K): Unit = {
    if (touchedKeys.isEmpty) {
      touchedKeys.add(key)
    } else if (touchedKeys.getFirst() != key) {
      touchedKeys.removeFirstOccurrence(key)
      touchedKeys.addFirst(key)
    }
  }

  def put(key: K, value: V): V = {
    Lock.synchronized {
      if (!m.keySet().contains(key) && m.size() >= maxSize) {
        m.remove(touchedKeys.getLast())
        touchedKeys.removeLast()
      }
      moveKeyToFront(key)
      m.put(key, value)
    }
  }

  def get(key: K): Option[V] = {
    Lock.synchronized {
      if (m.containsKey(key)) {
        moveKeyToFront(key)
        Some(m.get(key))
      } else {
        None
      }
    }
  }

  def getOrElse(key: K, defaultValue: V): V = {
    get(key).getOrElse(defaultValue)
  }
}
