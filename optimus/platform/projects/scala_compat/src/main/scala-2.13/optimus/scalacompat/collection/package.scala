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
package optimus.scalacompat

import scala.{collection => sc}

package object collection {
  def isView(c: Iterable[_]): Boolean = c match {
    case _: sc.View[_] => true
    case _             => false
  }
  def empty[From, Elem, Repr](bf: BuildFrom[From, Elem, Repr], from: From): Repr = bf.newBuilder(from).result()

  type IterableLike[+A, +Repr] = sc.IterableOps[A, Any, Repr]
  type Iterable2[A, B] = sc.Iterable[(A, B)]
  type MapLike[K, +V, +Repr] = sc.MapOps[K, V, Iterable2, Repr]
  type SeqLike[A, +Repr] = sc.SeqOps[A, Any, Repr]
  type SetLike[A, +Repr <: sc.SetOps[A, Any, Repr]] = sc.SetOps[A, Any, Repr]
  type TraversableLike[+A, +Repr] = IterableLike[A, Repr]
  type BuildFrom[-From, -Elem, +Repr] = sc.BuildFrom[From, Elem, Repr]
  type IterableFactory[+CC[X]] = sc.IterableFactory[CC]
  type IterableView[+A] = sc.View[A]
  type SeqView[+A] = sc.SeqView[A]

  def newBuilderFor[A, CC[A]](t: sc.IterableOps[A, CC, CC[A]]): sc.mutable.Builder[A, CC[A]] =
    t.iterableFactory.newBuilder[A]
  implicit class GenTraversableOnceSeqOp[C <: sc.IterableOnce[_]](val coll: C) extends AnyVal {
    def seq: coll.type = coll
  }
  def knownSize(t: sc.Iterable[_]): Int = t.knownSize
  implicit class BreakOutTo[CC[_]](private val companion: sc.IterableFactory[CC]) extends AnyVal {
    def breakOut[A]: sc.BuildFrom[Any, A, CC[A]] = new sc.BuildFrom[Any, A, CC[A]] {
      override def fromSpecific(from: Any)(it: sc.IterableOnce[A]): CC[A] = companion.newBuilder[A].addAll(it).result()
      override def newBuilder(from: Any): sc.mutable.Builder[A, CC[A]] = companion.newBuilder
    }
  }
  implicit class BreakOutToArray(private val companion: Array.type) extends AnyVal {
    def breakOut[A: scala.reflect.ClassTag]: sc.BuildFrom[Any, A, Array[A]] = new sc.BuildFrom[Any, A, Array[A]] {
      override def fromSpecific(from: Any)(it: sc.IterableOnce[A]): Array[A] =
        companion.newBuilder[A].addAll(it).result()
      override def newBuilder(from: Any): sc.mutable.Builder[A, Array[A]] = companion.newBuilder
    }
  }
  implicit class BreakOutToMap[CC[_, _]](private val companion: sc.MapFactory[CC]) extends AnyVal {
    def breakOut[A, B]: sc.BuildFrom[Any, (A, B), CC[A, B]] = new sc.BuildFrom[Any, (A, B), CC[A, B]] {
      override def fromSpecific(from: Any)(it: sc.IterableOnce[(A, B)]): CC[A, B] =
        companion.newBuilder[A, B].addAll(it).result()
      override def newBuilder(from: Any): sc.mutable.Builder[(A, B), CC[A, B]] = companion.newBuilder[A, B]
    }
  }

  // TODO: make polymorphic like 2.12 version
  implicit class MapValuesFilterKeysNow[K, +V](private val self: sc.Map[K, V]) extends AnyVal {
    def mapValuesNow[W](f: V => W): Map[K, W] = self.view.mapValues(f).toMap
    def filterKeysNow(p: K => Boolean): Map[K, V] = self.view.filterKeys(p).toMap
  }

  implicit class IterableOnceConvertTo[A](private val coll: IterableOnce[A]) {
    /**
     * Convert a collection into a different collection type.
     * This is an alias for `collection.to(Target)` used while cross-building with 2.12.
     */
    def convertTo[C](factory: Factory[A, C]): C = coll.to(factory)
  }
}
