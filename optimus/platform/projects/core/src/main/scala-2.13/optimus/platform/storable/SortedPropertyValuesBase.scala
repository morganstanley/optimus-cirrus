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
import scala.collection.immutable

abstract class SortedPropertyValuesBase extends Map[String, Any] {
  def removed(key: String): scala.collection.immutable.Map[String, Any] = toSeq.toMap.removed(key)
  def updated[V1 >: Any](key: String, value: V1): immutable.Map[String, V1] = toSeq.toMap.updated(key, value)
}
