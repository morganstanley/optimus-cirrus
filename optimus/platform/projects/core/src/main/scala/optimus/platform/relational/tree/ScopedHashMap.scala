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

import scala.collection.mutable.HashMap

/**
 * need to keep the previous scanned params result as well as this round scanned result, since all results will be used
 * in further scala tree building
 */
private[optimus /*should be platform*/ ] class ScopedHashMap[TKey, TValue](val previous: ScopedHashMap[TKey, TValue]) {
  private val map = new HashMap[TKey, TValue]

  def put(key: TKey, value: TValue): Unit =
    map.put(key, value)

  def getOrElse(key: TKey, default: => TValue): TValue =
    if (map.contains(key)) map(key)
    else if (previous != null) previous.getOrElse(key, default)
    else default

  def contains(key: TKey): Boolean =
    map.contains(key) || (previous != null && previous.contains(key))

  def exists(f: ((TKey, TValue)) => Boolean): Boolean = {
    if (previous eq null)
      map.exists(f)
    else
      map.exists(f) || previous.exists(f)
  }
}
