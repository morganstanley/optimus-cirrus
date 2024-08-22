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
package optimus.platform.temporalSurface.cache

import java.util.concurrent.ConcurrentHashMap
import scala.annotation.tailrec

/**
 * an associative map from a key to a single, value or set of values
 */
class ConcurrentMultiSet[K <: AnyRef, V <: AnyRef: Manifest] {

  private val underlying = new ConcurrentHashMap[K, AnyRef]

  /**
   * adds a key -> value mapping
   * @return
   *   true if any modification took place
   */
  @tailrec final def add(key: K, value: V): Boolean = {
    underlying.get(key) match {
      case null => if (underlying.putIfAbsent(key, value) == null) true else /* try again */ add(key, value)
      case v: V =>
        if (v == value) false
        else if (underlying.replace(key, v, Set(v, value))) true
        else /* try again */ add(key, value)
      case set: Set[V @unchecked] =>
        val added = set + value
        if (added == set) false else if (underlying.replace(key, set, added)) true else /* try again */ add(key, value)
    }
  }

  def applyTo(key: K, fn: (V) => Unit): Unit = {
    underlying.get(key) match {
      case null                   =>
      case v: V                   => fn(v)
      case set: Set[V @unchecked] => set foreach fn
    }
  }

  def get(key: K): Iterable[V] = {
    underlying.get(key) match {
      case null                   => Nil
      case v: V                   => v :: Nil
      case set: Set[V @unchecked] => set
    }
  }

  def getFirstDefined[B](key: K, fn: (V) => Option[B]): Option[B] = {
    @tailrec def first(values: Set[V]): Option[B] = {
      if (values.isEmpty) None
      else {
        val result = fn(values.head)
        if (result.isDefined) result else first(values.tail)
      }
    }
    underlying.get(key) match {
      case null                   => None
      case v: V                   => fn(v)
      case set: Set[V @unchecked] => first(set)
    }
  }

  /**
   * adds a key -> value mapping
   * @return
   *   true if any modification took place
   */
  @tailrec final def remove(key: K, value: V): Boolean = {
    underlying.get(key) match {
      case null => false
      case v: V =>
        if (v != value) false else if (underlying.remove(key, value)) true else /* try again */ remove(key, value)
      case set: Set[V @unchecked] =>
        val removed = set - value
        if (removed.isEmpty) {
          if (underlying.remove(key, value)) true else /* try again */ remove(key, value)
        } else {
          if (removed == set) false
          else if (underlying.replace(key, set, removed)) true
          else /* try again */ remove(key, value)
        }
    }
  }

}
