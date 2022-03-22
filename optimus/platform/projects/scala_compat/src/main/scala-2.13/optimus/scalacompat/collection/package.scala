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

import scala.{ collection => sc }

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

  def newBuilderFor[A, CC[A]](t: sc.IterableOps[A, CC, CC[A]]): sc.mutable.Builder[A, CC[A]] = {
    BuilderProvider.exposedBuilder(t).asInstanceOf[sc.mutable.Builder[A, CC[A]]]
  }
  def buildFromFor[A, CC[A]](t: sc.IterableOps[Any, CC, CC[_]], implicitArgs: Any*): sc.BuildFrom[Any, A, CC[A]] = {
    val bf = t match {
      case t: sc.BitSet =>
        sc.BuildFrom.buildFromBitSet
      case t: sc.SortedSet[A] =>
        sc.BuildFrom.buildFromSortedSetOps(implicitArgs.head.asInstanceOf[Ordering[A]])
      case t: sc.SortedMap[k, v] =>
        sc.BuildFrom.buildFromSortedMapOps(implicitArgs.head.asInstanceOf[Ordering[A]])
      case t: Map[k, v] =>
        sc.BuildFrom.buildFromMapOps
//      case t: sc.ArrayOps[a] =>
//        sc.BuildFrom.buildFromArray(implicitArgs.head.asInstanceOf[scala.reflect.ClassTag[A]])
      case t =>
        sc.BuildFrom.buildFromIterableOps
    }
    bf.asInstanceOf[sc.BuildFrom[Any, A, CC[A]]]
  }
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
  implicit class BreakOutToFactory[E, O](private val factory: sc.compat.Factory[E, O]) extends AnyVal {
    def breakOut: sc.BuildFrom[Any, E, O] = new sc.BuildFrom[Any, E, O] {
      override def fromSpecific(from: Any)(it: sc.IterableOnce[E]): O = factory.newBuilder.addAll(it).result()
      override def newBuilder(from: Any): sc.mutable.Builder[E, O] = factory.newBuilder
    }
  }
  def FloatOrdering: Ordering[Float] = Ordering.Float.IeeeOrdering
  def DoubleOrdering: Ordering[Double] = Ordering.Double.IeeeOrdering

  implicit class MapValuesFilterKeysNow[K, +V, Repr <: sc.MapOps[K, V, Iterable2, Repr]](private val self: Repr with sc.Map[K, V]) extends AnyVal {
    def mapValuesNow[W, That](f: V => W)(implicit bf: sc.BuildFrom[Repr, (K, W), That]): That = {
      self match {
        case im: sc.immutable.Map[K, V] =>
          im.transform((k, v) => f(v)).asInstanceOf[That]
        case _ =>
          val b = bf.newBuilder(self)
          self.foreachEntry((k, v) => b.addOne((k, f(v))))
          b.result()
      }
    }
    def filterKeysNow(p: K => Boolean): Repr = self.filter{ case (k, _) => p(k) }
  }

  implicit class IterableOnceConvertTo[A](private val coll: IterableOnce[A]) {
    /**
     * Convert a collection into a different collection type.
     * This is an alias for `collection.to(Target)` used while cross-building with 2.12.
     */
    def convertTo[C](factory: sc.compat.Factory[A, C]): C = coll.to(factory)
  }

  lazy val ParCollectionConverters = scala.collection.parallel.CollectionConverters
}
