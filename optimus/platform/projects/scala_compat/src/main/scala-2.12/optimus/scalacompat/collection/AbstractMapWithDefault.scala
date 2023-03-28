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
package optimus.scalacompat.collection

abstract class AbstractMapWithDefault[A, +B](underlying: Map[A, B], defaultValue: A => B)
    extends Map.WithDefault[A, B](underlying, defaultValue) {
  override def +[B1 >: B](kv: (A, B1)): Map.WithDefault[A, B1] = updated(kv._1, kv._2)
  override def -(key: A): Map.WithDefault[A, B] = removed(key)
  def removed(key: A): Map.WithDefault[A, B]
}
