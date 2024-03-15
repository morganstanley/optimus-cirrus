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

import scala.collection.compat.IterableOnce
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable

abstract class AbstractBuildFrom[-From, -Elem, +To] extends CanBuildFrom[From, Elem, To] {
  override def newBuilder(from: From): mutable.Builder[Elem, To] = apply(from)
  def newBuilder: mutable.Builder[Elem, To]
  def fromSpecific(from: From)(it: IterableOnce[Elem]): To = newBuilder(from).addAll(it).result()
}
