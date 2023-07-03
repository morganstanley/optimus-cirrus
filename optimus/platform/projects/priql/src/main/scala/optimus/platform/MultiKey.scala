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
package optimus.platform

class MultiKey(val keys: Array[Any]) {
  if (keys == null || keys.isEmpty)
    throw new IllegalArgumentException("key list is empty")

  private[this] val hashCodeVal = keys.foldLeft(1)((hash, key) => hash * 17 + key.##)

  override def hashCode(): Int = hashCodeVal

  override def equals(that: Any): Boolean = {
    that match {
      case other: MultiKey if (keys.length == other.keys.length) =>
        var i = 0
        var result = true // result is var because we need assign it when comparing the keys.
        while (result && i < keys.length) {
          if (keys(i) != other.keys(i))
            result = false
          i += 1
        }
        result
      case _ => false
    }
  }
}
