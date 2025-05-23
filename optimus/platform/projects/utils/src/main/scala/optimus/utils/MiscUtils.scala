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

import java.util.concurrent.atomic.AtomicLong
import org.slf4j
import msjava.slf4jutils.scalalog

import java.time.Instant
import scala.collection.LinearSeq
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable
import scala.reflect.macros.whitebox
import scala.util.Failure
import scala.util.Random
import scala.util.Success
import scala.util.Try

/** @see [[TypeClasses]] */
object MiscUtils {

  private val useTestingTimestampProp = "optimus.miscUtils.useTestingTimestamp"

  private def useTestingTimestamp: Boolean = System.getProperty(useTestingTimestampProp) match {
    case "true" | "1" => true
    case _            => false
  }

  val timestampGen: AtomicLong = new AtomicLong(Instant.now.toEpochMilli)

  def nextTimestamp: Long = if (useTestingTimestamp) timestampGen.incrementAndGet() else Instant.now.toEpochMilli

  def nextTimestampAsInstant: Instant = if (useTestingTimestamp) Instant.ofEpochMilli(nextTimestamp)
  else
    throw new IllegalArgumentException(
      "you must set -D" + useTestingTimestampProp + "=true to use MiscUtils.nextTimestampAsInstant")

  //  The idea is that you can call niceBreak from code you're actively developing,
  // and the breakpoints you set here will not move around as you edit.
  def niceBreak(s: String, index: Int = 0, log: Option[slf4j.Logger] = None): Unit = {
    def breakHere() = log.foreach(_.debug(s))
    def orHere() = breakHere() // absolutely necessary
    index match {
      case 1 =>
        breakHere()
      case 2 =>
        orHere()
      case 3 =>
        orHere()
      case 4 =>
        orHere()
      case 5 =>
        orHere()
      case _ =>
        orHere()
    }
  }

  object NullCandy {
    // Instead of
    // if (Objects.nonNull(foo) && Objects.nonNull(foo.bar) && Objects.nonNull(foo.bar.baz)) ...
    // you can write
    // val bazOpt = foo.nonNull(_.bar).nonNull(_.baz)

    implicit class NonEmptyOption[A <: AnyRef](val o: Option[A]) extends AnyVal {
      def nonNull[B](f: A => B): Option[B] = o.flatMap(a => Option(f(a)))
      def `&?`[B](f: A => B): Option[B] = nonNull(f)
    }

    implicit class NullableToOption[A <: AnyRef](val a: A) extends AnyVal {
      def nonNull: Option[A] = Option(a)
      def `&?` : Option[A] = nonNull
      def nonNull[B](f: A => B): Option[B] = Option(a).flatMap(a => Option(f(a)))
      def `&?`[B](f: A => B): Option[B] = nonNull(f)
    }
  }

  implicit class OptionalToOption[A](j: java.util.Optional[A]) {
    def asScala: Option[A] = if (j.isPresent) Some(j.get()) else None
  }

  def retry[A](
      attempts: Int = 3,
      delayMs: Int = 500,
      exceptionClasses: List[Class[_ <: Throwable]],
      expectedResult: Option[A] = None)(f: () => A): A = {
    val r = new Random()
    var result = Try(f())
    var exceptionMessage = s""
    var attemptCount = 1
    var updatedDelayMs = delayMs
    def retryAttempt(): Unit = {
      if (attemptCount < attempts) {
        Thread.sleep(updatedDelayMs + r.nextInt(updatedDelayMs))
        updatedDelayMs *= 2
        attemptCount += 1
        checkResult(Try(f()))
      } else {
        throw new RuntimeException(exceptionMessage)
      }
    }
    def checkResult(midResult: Try[A]): Unit = {
      midResult match {
        case Success(value) =>
          if (expectedResult.forall(_ == value)) result = midResult
          else {
            exceptionMessage = s"Attempt $attemptCount failed; expected ${expectedResult.get} but got $value"
            // log.warn(exceptionMessage)
            retryAttempt()
          }
        case Failure(ex) =>
          if (!exceptionClasses.contains(ex.getClass)) throw ex
          else {
            exceptionMessage = s"Attempt $attemptCount failed with: ${ex.getMessage}"
            // log.warn(exceptionMessage, ex)
            retryAttempt()
          }
      }
    }
    checkResult(result)
    result.get
  }

  /*
  Sequester code in a Function0 in _1, and return its source as _2
   */
  implicit def codeStringCandy[X](t: (() => X, String)): String = t._2
  def codeString[X](x: X): (() => X, String) = macro codeString$impl[X]
  def codeString$impl[X: c.WeakTypeTag](c: whitebox.Context)(x: c.Expr[X]): c.Expr[(() => X, String)] = {
    import c.universe._
    val xt = x.tree
    val pos = xt.pos
    val source =
      if (pos.isRange) {
        val lines = pos.source.content
          .slice(pos.start, pos.end)
          .mkString
          .replaceFirst("""^[\s\n]*\{[\s\n]*\n""", "")
          .replaceFirst("""[\s\n]*\}$""", "")
          .split("\n")
        val indent = lines.map(_.replaceFirst("\\S.*", "").length).min
        lines.map(_.substring(indent)).mkString("\n")
      } else
        xt.toString
    val s = Literal(Constant(source))
    val xtu = c.untypecheck(xt)
    val xtc = c.typecheck(q"""(() => {$xtu}, $s)""")
    c.Expr[(() => X, String)](xtc)
  }

  def codeBetweenComments(commentBefore: String, commentAfter: String): String = macro codeBetweenComments$impl
  def codeBetweenComments$impl(
      c: whitebox.Context)(commentBefore: c.Expr[String], commentAfter: c.Expr[String]): c.Expr[String] = {
    import c.universe._
    val q"${c1: String}" = commentBefore.tree
    val q"${c2: String}" = commentAfter.tree
    val source = c.macroApplication.pos.source.content.mkString.split('\n').toIndexedSeq.zipWithIndex
    val i1 = source.find(_._1.contains("// " + c1)).getOrElse(throw new Exception(s"Can find $c1 in $source"))._2
    val i2 = source.find(_._1.contains("// " + c2)).getOrElse(throw new Exception(s"Can find $c2 in $source"))._2
    assert(i2 > i1, s"No lines between $c1 and $c2")
    val snippet = (i1 + 1 until i2).map(source(_)._1.replaceFirst("^\\s+", "")).mkString("\n")
    c.Expr[String](Literal(Constant(snippet)))
  }

  object Endoish {

    implicit final class Optish(val b: Boolean) extends AnyVal {

      /**
       * boolExpr -?> something = if(boolExpr) Some(something) else None
       */
      def so[T](t: T): Option[T] = macro EndoishMacros.condOption[T]
    }

    /**
     * Utility implicit macros to treat functions like monoids: appended left to right rather than composed right to
     * left The typical use case is when you need to apply sequential transformations and have considered using a var
     * var x: X = x0 x = foo(x) + 5 if(hmm(x)) { x = blah(x/3) } x = postProcess(x) but hate introducing mutation to
     * your code, some monad, Some(x). map(foo(_) + 5). map { x => if(hmm(x)) blah(x/3) else x }. map(postProcess). get
     * which seems both arbitrary (in that _any_ monad or functor would do) and a wasteful proliferation of closures,
     * which may introduce sync stacks.
     *
     * Instead, you can write
     *
     * x0 |> { x => foo(x) +5 }.applyIf(hmm(_)) { x => blah(x/3) } |> postProcess
     *
     * All the functions applications get inlined, so the result is efficient and sync-stack-free.
     */
    implicit final class Endoish[T](val t: T) extends AnyVal {

      /**
       * A.k.a. pipe forward in F#. x |> f |> g |> h == h(g(f(x))
       */
      def |>[U](f: T => U): U = macro EndoishMacros.pipeImpl[T, U]
      def pipe[U](f: T => U): U = macro EndoishMacros.pipeImpl[T, U]

      // Transform if a condition is met:
      //   myList.applyIf(doScaleUp)(_.map(x => 100*x))
      def applyIf[U >: T](cond: Boolean)(f: T => U): U = macro EndoishMacros.applyIfCondImpl[T, U]
      // above one is nifty, this one is not
      def boringApplyIf[U >: T](cond: Boolean)(f: T => U): U = if (cond) f(t) else t

      // Transform in one of two ways, based on condition:
      //   myList.applyOr(scaleUpOrDrop)(_.map(x => 100*x))(_.filter(myPred))
      def applyOr[U >: T](cond: Boolean)(f: T => U)(g: T => U): U = macro EndoishMacros.applyOrCondImpl[T, U]

      // Transform if a predicate is met:
      //   myList.applyIf(pred(_))(_.map(x => 100*x))
      def applyIf[U >: T](cond: T => Boolean)(f: T => U): U = macro EndoishMacros.applyIfFuncImpl[T, U]

    }

    implicit final class EndoList[A](val l: List[A]) extends AnyVal {
      // note: operators must begin and end with : to have both the right associativity and precedence

      // TODO (https://issues.scala-lang.org/browse/SI-1980): Enable after laziness properly implemented for
      // right-associative operators.  Expected fix in 2.13: https://github.com/scala/scala/pull/5969
      // def :?:[B >: A](tup: (Boolean, B)): List[B] = macro EndoishMacros.prependIf[B]

      /**
       * The candidatePrependeeExpr will be evaluated and prepended only if booleanExpr is true. otherPrependee ::
       * booleanExpression.so(candidatePrependeeExpr) :?: someList
       */
      def :?:[B >: A](b: Option[B]): List[B] = macro EndoishMacros.prependIfNotNone[B]

      /**
       * The candidatePrependeeExpr will be evaluated, and prepended if it is not null. otherPrependee ::
       * possiblyNullPrependee :?: someListbbb
       */
      def :?:[B >: A](b: B): List[B] = macro EndoishMacros.prependIfNotNull[B]
    }

    object EndoishMacros {
      import scala.reflect.macros.blackbox.Context

      def condOption[T: c.WeakTypeTag](c: Context)(t: c.Expr[T]): c.Expr[Option[T]] = {
        import c.universe._
        val thiz = q"${c.prefix}.b"
        val thisTmp = internal.reificationSupport.freshTermName("thiz")
        val ret = q"{val $thisTmp = $thiz; if ($thisTmp.b) Some($t) else None}"
        c.Expr[Option[T]](MacroUtils.typecheckAndValidate(c)(ret))
      }

      def prependIfNotNull[B: c.WeakTypeTag](c: Context)(b: c.Expr[B]): c.Expr[List[B]] = {
        import c.universe._
        val thiz = q"${c.prefix}.l"
        val thisTmp = internal.reificationSupport.freshTermName("thiz")
        val candidateTmp = internal.reificationSupport.freshTermName("cand")
        val ret =
          q"{val $thisTmp = $thiz; val $candidateTmp = $b; if (($candidateTmp) ne null) { $candidateTmp :: $thisTmp } else { $thisTmp }}"
        c.Expr[List[B]](MacroUtils.typecheckAndValidate(c)(ret))
      }

      def prependIfNotNone[B: c.WeakTypeTag](c: Context)(b: c.Expr[Option[B]]): c.Expr[List[B]] = {
        import c.universe._
        val thiz = q"${c.prefix}.l"
        val thisTmp = internal.reificationSupport.freshTermName("thiz")
        val candidateTmp = internal.reificationSupport.freshTermName("cand")
        val ret =
          q"{val $thisTmp = $thiz; val $candidateTmp = $b; if ($candidateTmp.isDefined) { $candidateTmp.get :: $thisTmp } else { $thisTmp }}"
        c.Expr[List[B]](MacroUtils.typecheckAndValidate(c)(ret))
      }

      /*
    TODO (https://issues.scala-lang.org/browse/SI-1980): Enable after laziness properly implemented for
    right-associative operators.  Expected fix in 2.13: https://github.com/scala/scala/pull/5969

    def prependIf[B: c.WeakTypeTag](c: Context)(tup: c.Expr[(Boolean, B)]): c.Expr[List[B]] = {
      import c.universe._
      val thiz = q"${c.prefix}.l"
      val thisTmp = internal.reificationSupport.freshTermName("thiz")
      val ret = try {
        // Try to extract (cond, candidate) directly from the tuple.  This won't work if the user (or the compiler) has
        // stuck the tuple into a temporary variable.
        val q"(..$exprs)" = tup
        val Seq(cond, x) = exprs
        q"""{ val $thisTmp = $thiz;
              if ($cond) {
                $x :: $thisTmp
               } else { $thisTmp }}"""
      } catch {
        case e: MatchError =>
          val tupTmp = internal.reificationSupport.freshTermName("tup")
          q"""{ val $thisTmp = $thiz;
                val $tupTmp = $tup;
                if (($tupTmp)._1) {
                   ($tupTmp)._2 :: $thisTmp
                } else { $thisTmp }}"""
      }
      c.info(c.enclosingPosition, ret.toString, true)
      c.Expr[List[B]](ret)
    }
       */

      def applyIfCondImpl[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(cond: c.Expr[Boolean])(f: c.Expr[T => U]) = {
        import c.universe._
        val eo = f.tree.symbol.owner
        val (tmpVd, tmpSymbol) = MacroUtils.temporaryValDef(c)("thiz", eo, q"${c.prefix}.t")
        val inlinedApply = MacroUtils.applyFunctionToSym(c)(f.tree, tmpSymbol)
        val ret = q"{$tmpVd; if ($cond) { $inlinedApply } else { $tmpSymbol}}"
        MacroUtils.typecheckAndValidate(c)(ret)
      }

      def applyOrCondImpl[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(cond: c.Expr[Boolean])(f: c.Expr[T => T])(
          g: c.Expr[T => U]) = {
        import c.universe._
        val (tmpVd, tmpSymbol) = MacroUtils.temporaryValDef(c)("thiz", f.tree.symbol.owner, q"${c.prefix}.t")
        val inlinedApply1 = MacroUtils.applyFunctionToSym(c)(f.tree, tmpSymbol)
        val inlinedApply2 = MacroUtils.applyFunctionToSym(c)(g.tree, tmpSymbol)
        val ret = q"{$tmpVd; if ($cond) { $inlinedApply1 } else { $inlinedApply2 }}"
        MacroUtils.typecheckAndValidate(c)(ret)
      }
      def applyIfFuncImpl[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(cond: c.Expr[T => Boolean])(
          f: c.Expr[T => U]) = {
        import c.universe._
        val (tmpVd, tmpSymbol) = MacroUtils.temporaryValDef(c)("thiz", f.tree.symbol.owner, q"${c.prefix}.t")
        val inlinedApply1 = MacroUtils.applyFunctionToSym(c)(cond.tree, tmpSymbol)
        val inlinedApply2 = MacroUtils.applyFunctionToSym(c)(f.tree, tmpSymbol)
        val ret = q"{$tmpVd; if ($inlinedApply1) { $inlinedApply2 } else { $tmpSymbol }}"
        MacroUtils.typecheckAndValidate(c)(ret)
      }

      def pipeImpl[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(f: c.Expr[T => U]) = {
        import c.universe._
        val argVal = q"${c.prefix.tree}.t"
        val tree = MacroUtils.applyFunctionInline(c)(f.tree, argVal)
        c.Expr[U](tree) // Already validated
      }
    }
  }

  implicit final class Traversablish[CC[X] <: Traversable[X], A](private val self: CC[A]) extends AnyVal {
    // alas, we have no Applicative, and must do this manually
    /**
     * Map over `self` using `f`, returning a result only if `f` is defined for every element of `self`. Otherwise,
     * return the first error found.
     */
    def traverseEither[E, B](f: A => Either[E, B])(implicit cbf: CanBuildFrom[CC[A], B, CC[B]]): Either[E, CC[B]] = {
      val res = cbf(self); res.sizeHint(self) // result will be the same size as `self`, if we return a result
      self match { // this could just be the "exotic" case, but I feel ashamed to traverse the whole collection
        case linear: LinearSeq[A] =>
          var curr: LinearSeq[A] = linear
          while (curr.nonEmpty) {
            f(curr.head) match {
              case Right(b) => res += b
              case Left(e)  => return Left(e) // do NOT move into a closure
            }
            curr = curr.tail
          }
        case indexed: IndexedSeq[A] =>
          var ix = 0
          while (ix < indexed.size) {
            f(indexed(ix)) match {
              case Right(b) => res += b
              case Left(e)  => return Left(e) // do NOT move into a closure
            }
            ix += 1
          }
        case exotic => // no good way to bail early
          var err: Left[E, CC[B]] = null
          exotic.foreach { a =>
            if (err eq null) f(a) match {
              case Right(b) => res += b
              case Left(e)  => err = Left(e)
            }
          }
          if (err ne null) return err
      }
      Right(res.result())
    }
    // Again avoiding CT idioms by using the `Option <~> Either[Unit, ?]` natural iso
    def traverseOption[B](f: A => Option[B])(implicit cbf: CanBuildFrom[CC[A], B, CC[B]]): Option[CC[B]] =
      traverseEither(f(_).fold[Either[Unit, B]](Left(()))(Right(_))).toOption
  }

  implicit def toImmutableSet[T](cs: collection.Set[T]): immutable.Set[T] = cs match {
    case is: collection.immutable.Set[T] => is
    case x                               => x.toSet
  }

  implicit class OrderingChain[T](private val self: Ordering[T]) extends AnyVal {

    /**
     * Construct a derived ordering on [[T]] which breaks ties based on a second [[Ordering]].
     */
    // this should be called `then` but They insist on keeping `if ... then ... else ... fi` syntax a possibility.
    def orElse(other: Ordering[T]): Ordering[T] = (x: T, y: T) => {
      val bySelf = self.compare(x, y)
      if (bySelf == 0) other.compare(x, y) else bySelf
    }
    def orElseBy[U: Ordering](view: T => U): Ordering[T] = orElse(Ordering.by(view))
  }

  implicit class NumericFoldable[A](private val self: Iterable[A]) extends AnyVal {
    def sumOf[B](f: A => B)(implicit B: Numeric[B]): B = self.foldLeft(B.zero)((b, a) => B.plus(b, f(a)))
  }

  // blah.optionally(pred) = Some(blah) if pred(blah)
  implicit class Optionable[T](private val t: T) extends AnyVal {
    def optionally(pred: T => Boolean): Option[T] = if (pred(t)) Some(t) else None

    def orIfNull(u: T): T = if (t.asInstanceOf[AnyRef] ne null) t else u
  }
  // bool.thenSome(blah) = Some(blah) if bool
  implicit class ThenSome(private val pred: Boolean) extends AnyVal {
    def thenSome[T](t: => T): Option[T] = if (pred) Some(t) else None
  }

  def isPowerOfTwo(n: Long): Boolean = n > 0 && (n & (n - 1)) == 0
  implicit class LongCandy(val n: Long) extends AnyVal {
    def isPowerOfTwo: Boolean = MiscUtils.isPowerOfTwo(n)
  }
}

/*
Run something at a maximum per-minute rate over the lifetime of the squelcher
 */
class Squelch(maxPerMinute: Int) {
  private val t0 = System.currentTimeMillis()
  private val n = new AtomicLong(0)
  def apply[T](f: => T): Option[T] = {
    val dt = (System.currentTimeMillis() - t0) / (60 * 1000) + 1
    val rate = (n.get() + 1) / dt
    if (rate > maxPerMinute)
      None
    else {
      n.incrementAndGet()
      Some(f)
    }
  }
}

/**
 * Utility class for periodically logging while we perform some large number of tasks.
 */
class CountLogger(label: => String, intervalMs: Long, log: slf4j.Logger) {

  def this(label: => String, intervalMs: Long, log: scalalog.Logger) =
    this(label, intervalMs, log.javaLogger)

  def this(label: => String, intervalMs: Long) =
    this(label, intervalMs, scalalog.getLogger(classOf[CountLogger]).javaLogger)

  log.info(s"$label...")
  private val t0 = System.currentTimeMillis()
  private var t = 0L
  private var tPrint = t0 + intervalMs
  private var n = 0
  private def elapsed: Long = (t - t0) / 1000L
  private def rate: Long = if (t <= t0) 0L else 1000L * n / (t - t0)
  def apply(dn: Int): Unit = synchronized {
    n += dn
    t = System.currentTimeMillis()
    if (t > tPrint) {
      tPrint = t + intervalMs
      log.info(s"${label} n=$n elapsed=${elapsed}s rate=${rate}/s)")
    }
  }
  def apply(): Unit = apply(1)
  def done(): Int = synchronized {
    t = System.currentTimeMillis()
    log.info(s"${label} complete, n=$n, elapsed=${elapsed}s, rate=${rate}/s")
    n
  }
}
