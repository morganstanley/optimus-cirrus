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

import optimus.utils.CollectionUtils.TraversableOps

import scala.collection.SeqLike
import scala.collection.immutable.Seq
import scala.util._

object CollectionUtils extends CollectionUtils {
  final class TraversableOps[A](val as: Traversable[A]) extends AnyVal {
    //return 0 if empty, 1 if size 1, or 2 if size > 1
    private[this] def zeroOneOrMany: Int = as match {
      case x: SeqLike[_, _] @unchecked =>
        val lc = x.lengthCompare(1)
        if (lc < 0) 0
        else if (lc == 0) 1
        else 2
      case x =>
        val it = x.toIterator
        if (!it.hasNext) 0
        else {
          it.next()
          if (it.hasNext) 2 else 1
        }
    }

    def single: A = zeroOneOrMany match {
      case 0 => throw new NoSuchElementException("single on empty Traversable")
      case 1 => as.head
      case _ => throw new IllegalArgumentException(s"single on multi-element Traversable: $as")
    }

    /** Returns Some(x) if there is a single element or None if empty. If more than 1 elements are found, this method throws. */
    def singleOption: Option[A] = zeroOneOrMany match {
      case 0 | 1 => as.headOption
      case _     => throw new IllegalArgumentException(s"singleOption on multi-element Traversable: $as")
    }

    /** Returns Some(x) if there is a single element. If there are 0 or more than 1 elements, this method returns None. */
    def singleOrNone: Option[A] = zeroOneOrMany match {
      case 1 => Some(as.head)
      case _ => None
    }
  }
}

// note that this is inherited by optimus.platform package object, so you only need to import it if you can't see platform
trait CollectionUtils {
  implicit def traversable2Ops[A](as: Traversable[A]): TraversableOps[A] = new TraversableOps(as)

  implicit class TraversableTuple2Ops[A, B](iterable: Traversable[(A, B)]) {
    def toSingleMap: Map[A, B] = iterable.groupBy(_._1).map {
      case (k, kvs) => k -> kvs.map { case (_, v) => v }.single
    }

    def toDistinctMap: Map[A, B] = iterable.groupBy(_._1).map {
      case (k, kvs) => k -> kvs.map { case (_, v) => v }.toSeq.distinct.single
    }

    def toGroupedMap: Map[A, Seq[B]] = iterable.groupBy(_._1).map {
      case (k, kvs) => k -> kvs.map { case (_, v) => v }.to[Seq]
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
    def onEmpty(action: => Unit): Option[A] = { if (underlying.isEmpty) action else (); underlying }
  }

  implicit class OptimusTryOps[A](underlying: Try[A]) {
    def getOrThrow(msg: => String): A = underlying match {
      case Success(value)     => value
      case Failure(exception) => throw new IllegalArgumentException(s"$msg. See details: $exception.")
    }
    def onEmpty(action: Throwable => Unit): Try[A] = onFailure(action)
    def onFailure(action: Throwable => Unit): Try[A] = {
      if (underlying.isFailure) action(underlying.failed.get) else (); underlying
    }
  }
}
