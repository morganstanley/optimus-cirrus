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
package optimus.core

import scala.annotation.tailrec
import scala.collection.GenTraversable
import scala.collection.compat._
import optimus.scalacompat.collection._
import optimus.scalacompat.collection.BuildFrom
import scala.collection.generic.Growable
import scala.collection.mutable

object Collections {
  private[this] def map2Impl[A, B, R, Z](as: Iterable[A], bs: Iterable[B])(f: (A, B) => R)(
      bldr: mutable.Builder[R, Z]): Z = {
    val a = as.iterator
    val b = bs.iterator
    while (a.hasNext && b.hasNext) {
      bldr += f(a.next(), b.next())
    }
    bldr.result()
  }

  def map2[A, B, R, CC[X] <: Iterable[X]](as: CC[A], bs: CC[B])(f: (A, B) => R)(implicit
      cbf: BuildFrom[CC[A], R, CC[R]]): CC[R] = {
    map2Impl(as, bs)(f)(cbf.newBuilder(as))
  }

  def map2Map[A, B, C, D](as: Iterable[A], bs: Iterable[B])(f: (A, B) => (C, D)): Map[C, D] = {
    map2Impl(as, bs)(f)(Map.newBuilder[C, D])
  }

  def flatMap2[A, B, R, CC[X] <: Iterable[X]](as: CC[A], bs: CC[B])(f: (A, B) => TraversableOnce[R])(implicit
      cbf: BuildFrom[CC[A], R, CC[R]]) = {
    val bldr = cbf.newBuilder(as)
    val a = as.iterator
    val b = bs.iterator
    while (a.hasNext && b.hasNext) {
      bldr ++= f(a.next(), b.next())
    }
    bldr.result()
  }

  def foreach2[A, B](as: Iterable[A], bs: Iterable[B])(f: (A, B) => Unit): Unit = {
    val a = as.iterator
    val b = bs.iterator
    while (a.hasNext && b.hasNext) {
      f(a.next(), b.next())
    }
  }

  def inline_++=[A](bldr: Growable[A], rhs: Iterable[A]): Growable[A] = {
    val it = rhs.iterator
    while (it.hasNext) bldr += it.next()
    bldr
  }

  def unzip4[A, B, C, D, CC[X] <: GenTraversable[X]](coll: Seq[Product4[A, B, C, D]])(factory: IterableFactory[CC]) = {
    val b1 = factory.newBuilder[A]
    val b2 = factory.newBuilder[B]
    val b3 = factory.newBuilder[C]
    val b4 = factory.newBuilder[D]

    for (elem <- coll) {
      b1 += elem._1
      b2 += elem._2
      b3 += elem._3
      b4 += elem._4
    }
    (b1.result(), b2.result(), b3.result(), b4.result())
  }

  def unzip3[A, B, C, CC[X] <: GenTraversable[X]](coll: collection.Seq[Product3[A, B, C]])(
      factory: IterableFactory[CC]): (CC[A], CC[B], CC[C]) = {
    val b1 = factory.newBuilder[A]
    val b2 = factory.newBuilder[B]
    val b3 = factory.newBuilder[C]

    for (elem <- coll) {
      b1 += elem._1
      b2 += elem._2
      b3 += elem._3
    }
    (b1.result(), b2.result(), b3.result())
  }

  type SpanPredicateAndApply[T, R] = (T => Boolean, Iterable[T] => R)
  def applyRepeatedSpan[T, R](iter: Iterable[T])(predicates: SpanPredicateAndApply[T, R]*): collection.Seq[R] = {

    def applySpan(iter: Iterable[T], predicate: SpanPredicateAndApply[T, R]): (Iterable[T], Option[R]) = {
      val (items, others) = iter.span(predicate._1)
      val res =
        if (items.nonEmpty) Some(predicate._2.apply(items))
        else None
      others -> res
    }

    @tailrec
    def doRepeatedSpan(iter: Iterable[T], result: collection.Seq[R] = collection.Seq.empty): collection.Seq[R] = {
      if (iter.isEmpty) result
      else {
        val initialSize = iter.size
        val (resBuilder, remainingItems) = predicates.foldLeft((Vector.newBuilder[R], iter)) {
          case ((builder, accIter), predicate) =>
            val (remainingItems, optRes) = applySpan(accIter, predicate)
            optRes foreach { res =>
              builder += res
            }
            builder -> remainingItems
        }
        if (remainingItems.size == initialSize)
          throw new IllegalArgumentException(s"Invalid predicates or some combinations are missing??!!")
        else {
          if (remainingItems.nonEmpty) doRepeatedSpan(remainingItems, result ++ resBuilder.result())
          else result ++ resBuilder.result()
        }
      }
    }

    require(predicates.nonEmpty, s"Predicates cannot be empty!")
    doRepeatedSpan(iter)
  }
}
