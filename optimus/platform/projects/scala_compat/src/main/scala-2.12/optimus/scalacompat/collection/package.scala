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

import scala.annotation.nowarn
import scala.collection.compat.Factory
import scala.collection.generic.CanBuildFrom
import scala.reflect.ClassTag
import scala.{collection => sc}

package object collection {
  def isView(c: Iterable[_]): Boolean = c match {
    case _: sc.IterableView[_, _] => true
    case _                        => false
  }
  def empty[From, Elem, Repr](cbf: BuildFrom[From, Elem, Repr], from: From): Repr = {
    if ((cbf.asInstanceOf[AnyRef] eq sc.Seq.canBuildFrom) || (cbf.asInstanceOf[AnyRef] eq List.canBuildFrom)) {
      // optimize here by skipping the builder and just returning Nil
      // this is safe because the caller asked for a Seq or a List, and Nil is a valid value for either
      // the same optimization is in scala's List, too
      Nil.asInstanceOf[Repr]
    } else {
      cbf.newBuilder(from).result()
    }
  }
  def wrappedArrayFactory[T: ClassTag]: Factory[T, Seq[T]] = sc.mutable.WrappedArray.canBuildFrom

  type IterableLike[+A, +Repr] = sc.IterableLike[A, Repr]
  type IterableOnceOps[+A, +CC[_], +C] = sc.TraversableLike[A, C]
  type MapLike[K, +V, +Repr <: MapLike[K, V, Repr] with sc.Map[K, V]] = sc.MapLike[K, V, Repr]
  type MapOps[K, +V, +CC[_, _] <: IterableLike[_, _], +C <: MapLike[K, V, C] with sc.Map[K, V]] = sc.MapLike[K, V, C]
  type SeqLike[+A, +Repr] = sc.SeqLike[A, Repr]
  type SetLike[A, +Repr <: SetLike[A, Repr] with sc.Set[A]] = sc.SetLike[A, Repr]
  type TraversableLike[+A, +Repr] = sc.TraversableLike[A, Repr]
  type BuildFrom[-From, -Elem, +Repr] = sc.generic.CanBuildFrom[From, Elem, Repr]
  type IterableView[+A] = sc.IterableView[A, Iterable[_]]
  type SeqView[+A] = sc.SeqView[A, Seq[_]]
  implicit class BuildFromOps[-From, -Elem, +Repr](val bf: BuildFrom[From, Elem, Repr]) extends AnyVal {
    def newBuilder(from: From): sc.mutable.Builder[Elem, Repr] = bf.apply(from)
  }
  type IterableFactory[+CC[X] <: sc.GenTraversable[X]] = sc.generic.GenericCompanion[CC]
  def newBuilderFor[A, CC](t: TraversableLike[A, CC]): sc.mutable.Builder[A, CC] = t match {
    case _: sc.generic.HasNewBuilder[A, CC] =>
      BuilderProvider.exposedBuilder(t)
    case _ =>
      throw new UnsupportedOperationException(s"Collection ${t.getClass} must implement HasNewBuilder")
  }
  def buildFromFor[A, CC[A]](t: sc.Iterable[Any], implicitArgs: Any*): sc.generic.CanBuildFrom[Any, A, CC[A]] =
    throw new UnsupportedOperationException("not supported on 2.12.x")

  def knownSize(t: sc.GenTraversableOnce[_]): Int = {
    CanEqual.knownSize(t)
  }
  def simpleFactory[A, B](f: => sc.mutable.Builder[A, B]): sc.compat.Factory[A, B] = new sc.compat.Factory[A, B] {
    override def apply(from: Nothing): sc.mutable.Builder[A, B] = apply()
    override def apply(): sc.mutable.Builder[A, B] = f
  }
  implicit class BreakOutTo[CC[A] <: sc.GenTraversable[A]](private val companion: sc.generic.GenericCompanion[CC])
      extends AnyVal {
    def breakOut[A]: sc.generic.CanBuildFrom[Any, A, CC[A]] = new sc.generic.CanBuildFrom[Any, A, CC[A]] {
      override def apply(from: Any): sc.mutable.Builder[A, CC[A]] = companion.newBuilder
      override def apply(): sc.mutable.Builder[A, CC[A]] = companion.newBuilder
    }
  }
  implicit class BreakOutToArray(private val companion: Array.type) extends AnyVal {
    def breakOut[A: scala.reflect.ClassTag]: sc.generic.CanBuildFrom[Any, A, Array[A]] =
      new sc.generic.CanBuildFrom[Any, A, Array[A]] {
        override def apply(from: Any): sc.mutable.Builder[A, Array[A]] = companion.newBuilder
        override def apply(): sc.mutable.Builder[A, Array[A]] = companion.newBuilder
      }
  }
  implicit class BreakOutToMap[CC[A, B] <: sc.GenMap[A, B] with sc.GenMapLike[A, B, CC[A, B]]](
      private val companion: sc.generic.GenMapFactory[CC])
      extends AnyVal {
    def breakOut[A, B]: sc.generic.CanBuildFrom[Any, (A, B), CC[A, B]] =
      new sc.generic.CanBuildFrom[Any, (A, B), CC[A, B]] {
        override def apply(from: Any): sc.mutable.Builder[(A, B), CC[A, B]] = companion.newBuilder
        override def apply(): sc.mutable.Builder[(A, B), CC[A, B]] = companion.newBuilder
      }
  }
  implicit class BreakOutToSortedMap[CC[A, B] <: sc.SortedMap[A, B] with sc.SortedMapLike[A, B, CC[A, B]]](
      private val companion: sc.generic.SortedMapFactory[CC])
      extends AnyVal {
    def breakOut[A: Ordering, B]: sc.generic.CanBuildFrom[Any, (A, B), CC[A, B]] =
      new sc.generic.CanBuildFrom[Any, (A, B), CC[A, B]] {
        override def apply(from: Any): sc.mutable.Builder[(A, B), CC[A, B]] = companion.newBuilder
        override def apply(): sc.mutable.Builder[(A, B), CC[A, B]] = companion.newBuilder
      }
  }
  implicit class BreakOutToSortedSet[CC[A] <: sc.SortedSet[A] with sc.SortedSetLike[A, CC[A]]](
      private val companion: sc.generic.SortedSetFactory[CC])
      extends AnyVal {
    def breakOut[A: Ordering]: sc.generic.CanBuildFrom[Any, A, CC[A]] =
      new sc.generic.CanBuildFrom[Any, A, CC[A]] {
        override def apply(from: Any): sc.mutable.Builder[A, CC[A]] = companion.newBuilder
        override def apply(): sc.mutable.Builder[A, CC[A]] = companion.newBuilder
      }
  }

  implicit class MapValuesFilterKeysNow[K, V, Repr <: sc.MapLike[K, V, Repr] with sc.Map[K, V]](
      private val self: sc.MapLike[K, V, Repr])
      extends AnyVal {

    /**
     * A strict version of `mapValues` that is polymorphic in the resulting map type.
     *
     * The built-in `mapValues` method returns a "view" on the original map where the transformation function is
     * recomputed on each traversal. `mapValuesNow` eagerly builds a new map.
     *
     * The built-in `mapValues` method has a static return type `collection.Map`, an override in `SortedMap` refines the
     * result type. `mapValuesNow` is polymorphic in the resulting map type, like the built-in `map` method.
     */
    def mapValuesNow[W, That](f: V => W)(implicit bf: CanBuildFrom[Repr, (K, W), That]): That = self match {
      case im: sc.immutable.MapLike[K, V, Repr] => im.transform { case (_, v) => f(v) }
      case _                                    => self.map { case (k, v) => (k, f(v)) }
    }

    /**
     * A strict version of `filterKeys` that retains the map's static type.
     *
     * The built-in `filterKeys` method has a static return type `collection.Map`, an override * in `SortedMap` refines
     * the result type. `filterKeysNow` returns a map of the same type as the source, like the built-in `filter` method.
     */
    def filterKeysNow(p: K => Boolean): Repr = self.filter(kv => p(kv._1))
  }

  implicit class TraversableOnceConvertTo[A](private val coll: TraversableOnce[A]) {

    /**
     * Convert a collection into a different collection type.
     *
     * When used with scala-collection-compat, this method is equivalent to using `.to(Target)` in Scala 2.13. With
     * scala-collection-compat, the collection is only copied if necessary: if the source collection is immutable and
     * matches the target type, it is returned unchanged.
     *
     * Compared to `.to[Target]` in Scala 2.12 (or `to(Target)` with scala-collection-compat), this method has a more
     * precise static type; the `to[Target]` method returns a `CC[T]`, so the static type cannot be a `Map[K, V]` or a
     * `BitSet`.
     */
    def convertTo[C](cbf: CanBuildFrom[Nothing, A, C]): C = {
      val b = cbf()
      b ++= coll.seq
      b.result()
    }
  }

  /**
   * Implicit conversion from `mutable.Map` companion to `CanBuildFrom` building a mutable map.
   *
   * This is a workaround for a bug in scala-collection-compat, where `mapFactoryToCBF` by mistake only converts the
   * `immutable.Map` companion to a `CanBuildFrom`, instead of `collection.Map`. This cannot be changed in a binary
   * compatible manner.
   *
   * The implicit here converts `mutable.Map` only, not `collection.Map`, because the latter would cause ambiguities
   * between itself and the definition in scala-collection-compat.
   */
  implicit def mutableMapFactoryToCBF[K, V, CC[A, B] <: sc.mutable.Map[A, B] with sc.mutable.MapLike[A, B, CC[A, B]]](
      fact: sc.generic.MapFactory[CC]): CanBuildFrom[Any, (K, V), CC[K, V]] = new CanBuildFrom[Any, (K, V), CC[K, V]] {
    override def apply(from: Any): sc.mutable.Builder[(K, V), CC[K, V]] = apply()
    override def apply(): sc.mutable.Builder[(K, V), CC[K, V]] = fact.newBuilder[K, V]
  }

  implicit class BreakOutToFactory[E, O](private val factory: sc.compat.Factory[E, O]) extends AnyVal {
    def breakOut: sc.generic.CanBuildFrom[Any, E, O] = new sc.generic.CanBuildFrom[Any, E, O] {
      override def apply(from: Any): sc.mutable.Builder[E, O] = factory()
      override def apply(): sc.mutable.Builder[E, O] = factory()
    }
  }

  def FloatOrdering: Ordering[Float] = Ordering.Float
  def DoubleOrdering: Ordering[Double] = Ordering.Double
  type DoubleOrdering = Ordering.DoubleOrdering

  object ParCollectionConverters

  implicit class immutableSortedSetHasUnsorted[A](private val self: sc.immutable.SortedSet[A]) {
    def unsorted: sc.immutable.Set[A] = self
  }

  implicit class MutableIndexedSeqViewSlice[A](private val self: sc.mutable.IndexedSeq[A]) {
    def viewSlice(from: Int, until: Int): sc.mutable.IndexedSeqView[A, sc.mutable.IndexedSeq[A]] =
      self.view.slice(from, until)
  }
  implicit def ArrayViewSlice[A](self: Array[A]): MutableIndexedSeqViewSlice[A] = MutableIndexedSeqViewSlice(self)
  @nowarn("cat=deprecation")
  type ForkJoin = scala.concurrent.forkjoin.ForkJoinPool

  implicit class ArrayToVarArgsOps[A](val toVarArgsSeq: Array[A]) extends AnyVal
}
