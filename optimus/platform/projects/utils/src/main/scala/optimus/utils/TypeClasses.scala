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

import optimus.exceptions.RTExceptionTrait
import optimus.scalacompat.collection._
import optimus.scalacompat.collection.BuildFrom

import scala.collection.compat._
import scala.collection.immutable.SortedMap
import scala.collection.{generic, immutable, mutable}
import scala.reflect.ClassTag

/** @see [[MiscUtils]] */
trait TypeClasses {
  implicit class StringOps(s: String) {
    def nonEmptyOption: Option[String] = if (s.nonEmpty) Some(s) else None
    def splitBy(sep: Char): Seq[String] = if (s.nonEmpty) s.split(sep).toSeq else Seq.empty
  }

  implicit class TraversableOps[A, CC <: TraversableLike[A, CC]](t: CC with TraversableLike[A, CC]) {
    private type Rebuild[B, Z] = BuildFrom[CC, B, Z]
    // This sort of thing really should be in scala-library.
    def distinctBy[B](f: A => B): CC = {
      val isNew = mutable.Set.empty[B].add _
      t.filter(a => isNew(f(a)))
    }
    def fproductL[B, That](f: A => B)(implicit cbf: Rebuild[(B, A), That]): That = {
      val builder = cbf.newBuilder(t)
      t.foreach(a => builder.+=((f(a), a)))
      builder.result()
    }
    def fproductR[B, That](f: A => B)(implicit cbf: Rebuild[(A, B), That]): That = {
      val builder = cbf.newBuilder(t)
      t.foreach(a => builder.+=((a, f(a))))
      builder.result()
    }
  }

  implicit class TraversableCollectAllOps[F[a] <: TraversableLike[a, F[a]], A](t: F[A] with Traversable[A]) {
    def collectAll[B <: A](implicit ct: ClassTag[B], factory: Factory[B, F[B]]): F[B] = {
      val builder = factory.newBuilder
      t.foreach {
        case b: B => builder += b
        case _ =>
      }
      builder.result()
    }
  }

  implicit class BooleanOps(val _value: Boolean) {
    // a fyne thyngge from scala-z (but there it's called ??; this is clearer)
    def opt[T](t: => T): Option[T] = macro TypeClasses.macros.BooleanOps_opt[T]
  }

  implicit class OptionOps[A](oa: Option[A]) {
    def orTap(action: => Unit): Option[A] = { if (oa.isEmpty) action; oa }
  }

  implicit class OptionTupleOps[A, B](o: Option[(A, B)]) {
    def unzipOption: (Option[A], Option[B]) = o match {
      case Some((a, b)) => (Some(a), Some(b))
      case None         => (None, None)
    }
  }

  implicit class SeqSortedMapOps[A: Ordering, B](seq: Seq[SortedMap[A, B]]) {
    def merge[A1 >: A: Ordering]: SortedMap[A1, B] = {
      val allContents = seq.flatten
      val merged = SortedMap[A1, B](allContents.toSeq: _*)
      if (allContents.size != merged.size) {
        val keys = merged.keys
        val missing = allContents.map(_._1) diff keys.toSeq
        val ordering = implicitly[Ordering[A1]]
        val dupes = keys.filter(k => missing.exists(m => ordering.compare(k, m) == 0))
        throw new DuplicateKeyException((missing ++ dupes).sorted)
      }
      merged
    }
  }

  implicit class WhateverOps[A](a: A) {
    def when(cond: Boolean)(endo: A => A): A = if (cond) endo(a) else a
    def tap(fn: A => Unit): A = { fn(a); a }
    def returning[B](r: => B): B = r
  }

  implicit class FieldFilter[F[a] <: TraversableLike[a, F[a]], T](private val scopes: F[T]) {
    def fieldFilter(field: T => String, matchStr: Option[String]): F[T] = matchStr match {
      case Some(str) =>
        val perfectMatches = scopes.filter(s => field(s) == str)
        if (perfectMatches.nonEmpty) perfectMatches
        else scopes.filter(s => field(s).startsWith(str))
      case None => scopes
    }
  }

  implicit class Elze(val i: Int) {
    def elze(j: => Int): Int = if (i == 0) j else i
  }

  implicit class OptOrd[X: Ordering](val ceci: Option[X]) extends Ordered[Option[X]] {
    override def compare(cela: Option[X]): Int = (ceci, cela) match {
      case (Some(x), Some(y)) => implicitly[Ordering[X]].compare(x, y)
      case (Some(_), None)    => -1
      case (None, Some(_))    => 1
      case _                  => 0
    }
  }

  class Extractor[A, R](f: A => Option[R]) {
    final def unapply(a: A): Option[R] = f(a)
  }
  class Extractor2[A, R](p: A => Boolean, f: A => R) extends Extractor[A, R](a => if (p(a)) Some(f(a)) else None)
  class StartingWith(pre: String) extends Extractor2[String, String](_ startsWith pre, _ stripPrefix pre)

  final class ?[T](private[TypeClasses] val ot: Option[T]) {
    def asOption: Option[T] = ot
  }
  def ?[T]: ?[T] = new ?(None)
  implicit def asOption_?[T](t_? : ?[T]): Option[T] = t_?.asOption
  implicit def fromValue_?[T](t: T): ?[T] = new ?(Some(t))
}

object TypeClasses extends TypeClasses {
  object macros {
    import scala.reflect.macros.blackbox
    // this one is macrotastic for the same reason that Boolean#map and so on is inlined by the plugin
    type BooleanOps_opt_context = blackbox.Context { type PrefixType = TypeClasses#BooleanOps }
    def BooleanOps_opt[T](c: BooleanOps_opt_context)(t: c.Expr[T]): c.Expr[Option[T]] =
      c.universe.reify(if (c.prefix.splice._value) Some(t.splice) else None)
  }
}

class DuplicateKeyException(val keys: Seq[Any])
    extends IllegalArgumentException(keys.mkString(", "))
    with RTExceptionTrait
