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
import scala.{ collection => sc }

package object collection {
  def isView(c: Iterable[_]): Boolean = c match {
    case  _: sc.IterableView[_, _] => true
    case _ => false
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
  type IterableLike[+A, +Repr] = sc.IterableLike[A, Repr]
  type MapLike[K, +V, +Repr <: MapLike[K, V, Repr] with sc.Map[K, V]] = sc.MapLike[K, V, Repr]
  type SeqLike[+A, +Repr] = sc.SeqLike[A, Repr]
  type SetLike[A, +Repr <: SetLike[A, Repr] with sc.Set[A]] = sc.SetLike[A, Repr]
  type TraversableLike[+A, +Repr] = sc.TraversableLike[A, Repr]
  type BuildFrom[-From, -Elem, +Repr] = sc.generic.CanBuildFrom[From, Elem, Repr]
  type IterableView[+A] = sc.IterableView[A, Iterable[_]]
  type SeqView[+A] = sc.SeqView[A, Seq[_]]
  implicit class BuildFromOps[-From, -Elem, +Repr](val bf: BuildFrom[From, Elem, Repr]) extends AnyVal {
    def newBuilder(from: From): sc.mutable.Builder[Elem, Repr] = bf.apply(from)
  }
  implicit class FactoryOps[-Elem, +Repr](val bf: sc.compat.Factory[Elem, Repr]) extends AnyVal {
    def newBuilder: sc.mutable.Builder[Elem, Repr] = bf.apply()
  }
  type IterableFactory[+CC[X] <: sc.GenTraversable[X]] = sc.generic.GenericCompanion[CC]
  def newBuilderFor[A, CC](t: TraversableLike[A, CC]): sc.mutable.Builder[A, CC] = t match {
    case _: sc.generic.HasNewBuilder[A, CC] =>
      BuilderProvider.exposedBuilder(t)
    case _ =>
      throw new UnsupportedOperationException(s"Collection ${t.getClass} must implement HasNewBuilder")
  }
  def knownSize(t: sc.GenTraversableOnce[_]): Int = {
    CanEqual.knownSize(t)
  }
  implicit class BreakOutTo[CC[A] <: sc.GenTraversable[A]](private val companion: sc.generic.GenericCompanion[CC]) extends AnyVal {
    def breakOut[A]: sc.generic.CanBuildFrom[Any, A, CC[A]] = new sc.generic.CanBuildFrom[Any, A, CC[A]] {
      override def apply(from: Any): sc.mutable.Builder[A, CC[A]] = companion.newBuilder
      override def apply(): sc.mutable.Builder[A, CC[A]] = companion.newBuilder
    }
  }
  implicit class BreakOutToArray(private val companion: Array.type) extends AnyVal {
    def breakOut[A: scala.reflect.ClassTag]: sc.generic.CanBuildFrom[Any, A, Array[A]] = new sc.generic.CanBuildFrom[Any, A, Array[A]] {
      override def apply(from: Any): sc.mutable.Builder[A, Array[A]] = companion.newBuilder
      override def apply(): sc.mutable.Builder[A, Array[A]] = companion.newBuilder
    }
  }
  implicit class BreakOutToMap[CC[A, B] <: sc.GenMap[A, B] with sc.GenMapLike[A, B, CC[A, B]]](private val companion: sc.generic.GenMapFactory[CC]) extends AnyVal {
    def breakOut[A, B]: sc.generic.CanBuildFrom[Any, (A, B), CC[A, B]] = new sc.generic.CanBuildFrom[Any, (A, B), CC[A, B]] {
      override def apply(from: Any): sc.mutable.Builder[(A, B), CC[A, B]] = companion.newBuilder
      override def apply(): sc.mutable.Builder[(A, B), CC[A, B]] = companion.newBuilder
    }
  }
}
