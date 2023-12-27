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

import scala.collection.mutable
import scala.{collection => sc}
import scala.reflect.ClassTag

package object collection extends MapBuildFromImplicits {
  def isView(c: Iterable[_]): Boolean = c match {
    case _: sc.View[_] => true
    case _             => false
  }
  def empty[From, Elem, Repr](bf: BuildFrom[From, Elem, Repr], from: From): Repr = bf.newBuilder(from).result()
  def wrappedArrayFactory[T: ClassTag]: sc.Factory[T, Seq[T]] = sc.immutable.ArraySeq

  type IterableLike[+A, +Repr] = sc.IterableOps[A, Any, Repr]
  type IterableOnceOps[+A, +CC[_], +C] = sc.IterableOnceOps[A, CC, C]
  type Iterable2[A, B] = sc.Iterable[(A, B)]
  type MapLike[K, +V, +Repr] = sc.MapOps[K, V, Iterable2, Repr]
  type MapOps[K, +V, +CC[_, _] <: IterableLike[_, _], +C <: MapLike[K, V, C]] = sc.MapOps[K, V, CC, C]
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

  implicit class GenTraversableOnceSeqOp[C <: sc.IterableOnce[_]](val coll: C) extends AnyVal {
    def seq: coll.type = coll
  }
  def knownSize(t: sc.IterableOnce[_]): Int = t.knownSize
  def simpleFactory[A, B](f: => sc.mutable.Builder[A, B]): sc.compat.Factory[A, B] = new sc.compat.Factory[A, B] {
    override def fromSpecific(it: IterableOnce[A]): B = newBuilder.addAll(it).result()
    override def newBuilder: mutable.Builder[A, B] = f
  }
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
  implicit class BreakOutToSortedMap[CC[_, _]](private val companion: sc.SortedMapFactory[CC]) extends AnyVal {
    def breakOut[A: Ordering, B]: sc.BuildFrom[Any, (A, B), CC[A, B]] = new sc.BuildFrom[Any, (A, B), CC[A, B]] {
      override def fromSpecific(from: Any)(it: sc.IterableOnce[(A, B)]): CC[A, B] =
        companion.newBuilder[A, B].addAll(it).result()
      override def newBuilder(from: Any): sc.mutable.Builder[(A, B), CC[A, B]] = companion.newBuilder[A, B]
    }
  }
  implicit class BreakOutToSortedIterable[CC[_]](private val companion: sc.SortedIterableFactory[CC]) extends AnyVal {
    def breakOut[A: Ordering]: sc.BuildFrom[Any, A, CC[A]] = new sc.BuildFrom[Any, A, CC[A]] {
      override def fromSpecific(from: Any)(it: sc.IterableOnce[A]): CC[A] =
        companion.newBuilder[A].addAll(it).result()
      override def newBuilder(from: Any): sc.mutable.Builder[A, CC[A]] = companion.newBuilder[A]
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
  type DoubleOrdering = Ordering.Double.IeeeOrdering

  implicit class MapValuesFilterKeysNow[K, +V, Repr <: sc.MapOps[K, V, Iterable2, Repr]](
      private val self: Repr with sc.Map[K, V])
      extends AnyVal {
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
    def filterKeysNow(p: K => Boolean): Repr = self.filter { case (k, _) => p(k) }
  }

  implicit class IterableOnceConvertTo[A](private val coll: IterableOnce[A]) {

    /**
     * Convert a collection into a different collection type. This is an alias for `collection.to(Target)` used while
     * cross-building with 2.12.
     */
    def convertTo[C](factory: sc.Factory[A, C]): C = coll.iterator.to(factory)
  }

  lazy val ParCollectionConverters = scala.collection.parallel.CollectionConverters

  implicit class ToStringPrefix(c: Iterable[_]) {
    def stringPrefix: String = BuilderProvider.className(c)
  }

  implicit class MutableIndexedSeqViewSlice[A](private val self: MutableIndexedSeqView.SomeIndexedSeqOps[A]) {
    def viewSlice(from: Int, until: Int): MutableIndexedSeqView[A] = new MutableIndexedSeqView[A](self, from, until)
  }
  implicit def ArrayViewSlice[A](self: Array[A]): MutableIndexedSeqViewSlice[A] = MutableIndexedSeqViewSlice(self)
  // used widely, let's keep it for now.
  def asScalaBuffer[A](as: java.util.List[A]): sc.mutable.Buffer[A] = {
    import scala.jdk.javaapi.{CollectionConverters => CC}
    CC.asScala(as)
  }

  final private class DeepArray(as: Array[_]) extends sc.IndexedSeq[Any] {
    override def apply(i: Int): Any = {
      val a = as(i)
      if (a != null && a.getClass.isArray) new DeepArray(a.asInstanceOf[Array[_]])
      else a
    }
    override def length: Int = as.length
  }
  implicit class ArrayDeepOps(private val self: Array[_]) {
    def deep: sc.IndexedSeq[Any] = new DeepArray(self)
  }
  type ForkJoin = java.util.concurrent.ForkJoinPool

  implicit class ArrayToVarArgsOps[A](private val self: Array[A]) extends AnyVal {
    def toVarArgsSeq: scala.collection.immutable.Seq[A] = scala.collection.immutable.ArraySeq.unsafeWrapArray(self)
  }
}
