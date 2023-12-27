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
package optimus.utils

import java.{util => ju}
import optimus.utils.CollectionUtils.ExtraTraversableOps2
import optimus.utils.CollectionUtils.TraversableOps
import optimus.scalacompat.collection._
import optimus.scalacompat.collection.BuildFrom

import scala.collection.SeqLike
import scala.collection.immutable.Seq
import scala.collection.mutable.ListBuffer
import scala.util._

object CollectionUtils extends CollectionUtils {
  final class TraversableOps[A](val as: Traversable[A]) extends AnyVal {

    /**
     * return 0 if empty, 1 if size 1, or 2 if size > 1
     */
    private[this] def zeroOneOrMany: Int = as match {
      case x: SeqLike[_, _] @unchecked =>
        val lc = x.lengthCompare(1)
        if (lc < 0) 0
        else if (lc == 0) 1
        else 2
      case x =>
        val it = x.toIterator // mostly `as` is Iterable for which this is just .iterator, thus cheap
        if (!it.hasNext) 0
        else {
          it.next()
          if (it.hasNext) 2 else 1
        }
    }

    /**
     * Like zeroOneOrMany, but as if it was run on the result of .distinct.
     */
    private[this] def distinctZeroOneOrMany: Int = {
      // note that this doesn't build an intermediate set, which is expensive
      val iter = as.toIterator
      if (iter.hasNext) {
        val first = iter.next()
        while (iter.hasNext) {
          val next = iter.next()
          if (next != first) return 2 // break out of the loop, we have at least 2 distinct elements
        }
        1
      } else 0
    }

    /**
     * Resolution logic
     *   - This returns an object of type A if this is the only element of the Traversable.
     *   - Otherwise an exception is thrown.
     */
    def single: A = zeroOneOrMany match {
      case 0 => throw new NoSuchElementException("single on empty Traversable")
      case 1 => as.head
      case _ => throw new IllegalArgumentException(s"single on multi-element Traversable: $as")
    }

    /**
     * Resolution logic
     *   - Returns Some(x) if there is a single element or None if empty.
     *   - If more than 1 elements are found, this method throws.
     */
    def singleOption: Option[A] = zeroOneOrMany match {
      case 0 | 1 => as.headOption
      case _     => throw new IllegalArgumentException(s"singleOption on multi-element Traversable: $as")
    }

    /**
     * Resolution logic
     *   - Returns Some(x) if there is a single element.
     *   - If there are 0 or more than 1 elements, this method returns None.
     */
    def singleOrNone: Option[A] = zeroOneOrMany match {
      case 1 => Some(as.head)
      case _ => None
    }

    /**
     * An element of type A is returned if the Traversable is a singleton. Otherwise an exception flies.
     *
     * @param f
     *   a function that composes an exception message according to type A.
     * @return
     */
    def singleOrThrow(f: Traversable[A] => String): A =
      if (zeroOneOrMany == 1) as.head
      else throw new IllegalArgumentException(f(as))

    /**
     * Returns Some(x) if there is a single element or None if empty. If more than 1 elements are found, use fallback
     */
    def singleOptionOr(fallback: => Option[A]): Option[A] =
      if (zeroOneOrMany < 2) singleOption else fallback

    /**
     * Returns true if the Traversable contains one distinct element (possibly multiple times).
     */
    def isSingleDistinct: Boolean = distinctZeroOneOrMany == 1

    /**
     * Resolution logic
     *   - Returns an element of type A if this is the only member of the Traversable.
     *   - Otherwise an exception is thrown.
     */
    def singleDistinct: A = distinctZeroOneOrMany match {
      case 0 => throw new IllegalArgumentException("Expected single element, but was empty!")
      case 1 => as.head
      case 2 =>
        throw new IllegalArgumentException(
          s"Expected single distinct element, but found multiple: [${as.mkString(",")}]")
    }

    /**
     * Resolution logic
     *   - Returns Some(a) if a is the only element of type A contained by the Traversable
     *   - Returns None if the Traversable is empty
     *   - Otherwise exception flies.
     */
    def singleDistinctOption: Option[A] =
      if (distinctZeroOneOrMany < 2) as.headOption
      else
        throw new IllegalArgumentException(
          s"Expected zero or one distinct element, but found multiple: [${as.mkString(",")}]")

    /**
     * Resolution logic
     *   - Returns Some(a) if a is the only element of type A contained by the Traversable
     *   - Returns None if the Traversable is empty
     *   - Otherwise returns single distinct value or None if no distinct value.
     */
    def singleDistinctOrNone: Option[A] =
      if (distinctZeroOneOrMany < 2) as.headOption
      else None

    /** Not async friendly, for that see singleOrElseAsync. */
    def singleOptionOrElse(nonSingleHandler: (Int, Traversable[A]) => Option[A]): Option[A] = as.size match {
      case 1 => as.headOption
      case n => nonSingleHandler(n, as)
    }

    /** Not async friendly, for that see singleOrElseAsync. */
    def singleOr(nonSingleHandler: => A): A = as.size match {
      case 1 => as.head
      case n => nonSingleHandler
    }

    /** Not async friendly, for that see singleOrElseAsync. */
    def singleOrElse(nonSingleHandler: (Int, Traversable[A]) => A): A = as.size match {
      case 1 => as.head
      case n => nonSingleHandler(n, as)
    }

    def groupReduceStable[K, V](key: A => K, value: A => V)(reduce: (V, V) => V, build: (K, V) => A): Seq[A] = {
      val result = new java.util.LinkedHashMap[K, V]()
      for (a <- as) {
        val k = key(a)
        val v = value(a)
        result.merge(k, v, (v1: V, v2: V) => reduce(v1, v2))
      }
      import scala.jdk.CollectionConverters._
      result.entrySet.iterator.asScala.map(entry => build(entry.getKey, entry.getValue)).toVector
    }

    /**
     * Groups the collection based on the key function. Results are deterministically ordered in the same order as the
     * input collection.
     */
    def groupByStable[K](key: A => K): Seq[(K, Seq[A])] = {
      val result = new java.util.LinkedHashMap[K, ListBuffer[A]]()
      for (a <- as) {
        val k = key(a)
        val buffer = result.computeIfAbsent(k, (k: K) => ListBuffer[A]())
        buffer += a
      }
      val vectorBuilder = Vector.newBuilder[(K, Seq[A])]
      vectorBuilder.sizeHint(result.size())
      val it = result.entrySet().iterator()
      while (it.hasNext) {
        val elem = it.next()
        vectorBuilder += (elem.getKey -> elem.getValue.result())
      }
      vectorBuilder.result()
    }
  }

  class ExtraTraversableOps2[T, Repr[T] <: TraversableLike[T, Repr[T]]](underlying: Repr[T]) {
    def onEmpty(action: => Unit): Repr[T] = { if (underlying.isEmpty) action else (); underlying }

    /**
     * returns only those elements of the collection which are instances of the specified type
     *
     * (this is named 'collect' rather than 'filter' because the type of the returned collection is changed to be a
     * collection of T, and this follows the convention of collect in the scala api)
     */
    def collectInstancesOf[X](implicit xManifest: Manifest[X], cbf: BuildFrom[Repr[T], X, Repr[X]]): Repr[X] = {
      val cls = xManifest.runtimeClass
      val builder = cbf.newBuilder(underlying)
      underlying.foreach {
        case c if cls.isInstance(c) => builder += c.asInstanceOf[X]
        case _                      =>
      }
      builder.result()
    }

    def collectInstancesNotOf[X](implicit xManifest: Manifest[X]): Repr[T] = {
      val cls = xManifest.runtimeClass
      underlying.filterNot(c => cls.isInstance(c))
    }

    def collectFirstInstanceOf[X](implicit xManifest: Manifest[X]): Option[X] = {
      val cls = xManifest.runtimeClass
      underlying.collectFirst {
        case c if cls.isInstance(c) => c.asInstanceOf[X]
      }
    }

    def collectAllInstancesOf[X](implicit
        xManifest: Manifest[X],
        cbf: BuildFrom[Repr[T], X, Repr[X]]): Option[Repr[X]] = {
      val collected = collectInstancesOf[X]
      if (collected.size == underlying.size) Some(collected) else None
    }
  }
}

// note that this is inherited by optimus.platform package object, so you only need to import it if you can't see platform
trait CollectionUtils {
  implicit def traversable2Ops[A](as: Traversable[A]): TraversableOps[A] = new TraversableOps(as)

  implicit def traversable2ExtraTraversableOps2[T, Repr[T] <: TraversableLike[T, Repr[T]]](
      t: Repr[T]
  ): ExtraTraversableOps2[T, Repr] = new ExtraTraversableOps2[T, Repr](t)

  implicit class TraversableTuple2Ops[A, B](iterable: Traversable[(A, B)]) {
    def toSingleMap: Map[A, B] = iterable.groupBy(_._1).map { case (k, kvs) =>
      k -> kvs.map { case (_, v) => v }.single
    }

    def toDistinctMap: Map[A, B] = iterable.groupBy(_._1).map { case (k, kvs) =>
      k -> kvs.map { case (_, v) => v }.toSeq.distinct.single
    }

    def toGroupedMap: Map[A, Seq[B]] = {
      iterable.groupBy(_._1).map { case (k, kvs) =>
        k -> kvs.map { case (_, v) => v }.toVector
      }
    }

    def toGroupedMap[C](f: Traversable[B] => C): Map[A, C] =
      iterable.groupBy { case (a, b) => a }.transform((_, abs) => f(abs.map { case (a, b) => b }))
  }

  implicit class juPropertiesOps(props: ju.Properties) {
    def addAll[K <: AnyRef, V <: AnyRef](all: ju.Map[K, V]): Unit = { // hack against scala/bug#10418 (fixed in 2.13 only)
      (props: ju.Hashtable[AnyRef, AnyRef]).putAll(all)
    }
  }
  implicit class OptimusOptionOps[A](underlying: Option[A]) {
    def getOrThrow(msg: => String): A = underlying.getOrElse(throw new IllegalArgumentException(msg))
    def onEmpty(action: => Unit): Option[A] = {
      if (underlying.isEmpty) action else (); underlying
    }
    // TODO(OPTIMUS-48501) Just use Option.zip once 2.12 support is not needed
    def zipp[B](ob: Option[B]): Option[(A, B)] = underlying match {
      case Some(x) =>
        ob match {
          case Some(y) => Some((x, y))
          case _       => None
        }
      case _ => None
    }
  }

  implicit class OptimusTryOps[A](underlying: Try[A]) {
    def getOrThrow(msg: => String): A = underlying match {
      case Success(value)     => value
      case Failure(exception) => throw new IllegalArgumentException(s"$msg. See details: $exception.")
    }
    def onEmpty(action: Throwable => Unit): Try[A] = onFailure(action)
    def onFailure(action: Throwable => Unit): Try[A] = {
      if (underlying.isFailure) action(underlying.failed.get) else ();
      underlying
    }
  }
  implicit class MapFromMaps(private val underlying: Map.type) {

    /**
     * Build a map with the Tuple elements in `maps`.
     *
     * @see
     *   the related lint rule in optimus.tools.scalacplugins.entity.PostTyperCodingStandardsComponent that advises when
     *   this method should be used to avoid different compilation under different Scala versions.
     *
     * @see
     *   MapFromMap.fromAll for an alternative API to concatenate such maps
     */
    def fromAll[K, V](maps: Iterable[(K, V)]*): Map[K, V] = {
      val builder = Map.newBuilder[K, V]
      maps.foreach(builder ++= _)
      builder.result()
    }
  }
  implicit class OptimusMapOps[M[A, B] <: Map[A, B], K, V](private val self: M[K, V]) {

    /**
     * Concatenate the `self` Map with the Tuple elements in `those`, potentially resulting in a Map with wider
     * key/value types than `self`
     *
     * In Scala 2.12 this could be expressed directly with `self ++ those`, but in Scala 2.13 this no longer returns a
     * Map unless the key type of `those` is a subtype of `K`
     *
     * @see
     *   the related lint rule in optimus.tools.scalacplugins.entity.PostTyperCodingStandardsComponent that advises when
     *   this method should be used to avoid different compilation under different Scala versions.
     *
     * @see
     *   MapFromMap.fromAll for an alternative API to concatenate such maps
     */
    def +~+[A >: (K, V), That <: Map[_, _]](
        those: Traversable[A]
    )(implicit buildFrom: BuildFrom[M[K, V], A, That]): That = {
      val builder = buildFrom.apply(self)
      builder ++= self
      builder ++= those
      builder.result()
    }
  }

  implicit class RichSeqOps[A, C[X] <: Seq[X]](private val xs: C[A]) {
    def combinations2: Iterator[(A, A)] =
      xs.combinations(2).map(x => (x: @unchecked) match { case Seq(a, b) => (a, b) })
    def sliding2: Iterator[(A, A)] = xs.sliding(2).map(x => (x: @unchecked) match { case Seq(a, b) => (a, b) })
  }
}
