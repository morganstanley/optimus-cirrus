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

import scala.collection.immutable
import scala.collection.immutable.Map

/**
 * scala WithDefault.get won't guarantee to return Some(). define a map that has this behavior.
 *
 * @param underlying
 * @param d
 * @tparam A
 * @tparam B
 */
class ConsistentDefaultMap[A, +B](
  underlying: Map[A, B],
  d: A => B
) extends Map.WithDefault[A, B](underlying, d) {

  override def get(key: A) = super.get(key) orElse Some(d(key))

  override def contains(key: A) = true
  override def size = Int.MaxValue // i will always return Some so i contain the universe.

  override def empty = new ConsistentDefaultMap(underlying.empty, d)
  override def updated[B1 >: B](key: A, value: B1) =
    new ConsistentDefaultMap[A, B1](underlying.updated[B1](key, value), d)
  override def +[B1 >: B](kv: (A, B1)) = updated(kv._1, kv._2)
  override def -(key: A) = new ConsistentDefaultMap(underlying - key, d)
  override def withDefault[B1 >: B](d: A => B1): immutable.Map[A, B1] = new ConsistentDefaultMap[A, B1](underlying, d)
  override def withDefaultValue[B1 >: B](d: B1): immutable.Map[A, B1] =
    new ConsistentDefaultMap[A, B1](underlying, x => d)
}