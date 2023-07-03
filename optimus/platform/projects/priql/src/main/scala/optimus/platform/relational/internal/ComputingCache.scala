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

import java.util.concurrent.ConcurrentHashMap

/**
 * A threadsafe concurrent cache which computes missing values on demand. The cache will only block if you try to
 * retrieve a value which is already being computed on another thread. Access to other keys in the cache during a
 * computation does not block, i.e. blocking is on a per-key basis.
 *
 * There is no eviction, i.e. values are retained forever.
 */
class ComputingCache[A, B](func: A => B) {
  private val cache = new ConcurrentHashMap[A, B]

  /**
   * gets the entry, computing it on demand if it was missing
   */
  def apply(key: A): B = {
    // atomically get the existing entry, or add one if it's missing
    cache.computeIfAbsent(key, x => func(x))
  }

  /**
   * puts the specified (user computed) value in the cache, overwriting existing value if any
   */
  def put(key: A, value: B): Unit = {
    cache.put(key, value)
  }

  /**
   * clears the whole cache. this is probably more useful for testing than in production.
   */
  def clear(): Unit = { cache.clear() }
}
