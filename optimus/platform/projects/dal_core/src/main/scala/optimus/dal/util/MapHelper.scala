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
package optimus.dal.util

object MapHelper {
  def pureJoin[K, V1, V2](container: Map[K, V1], elements: Map[K, V2], default: V1, add: (V1, V2) => V1): Map[K, V1] = {
    val updated = elements.map { case (k, v2) =>
      val v1 = container.getOrElse(k, default)
      k -> add(v1, v2)
    }
    container ++ updated
  }
}
