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
package optimus.buildtool.format

import com.typesafe.config._

import scala.collection.compat._
import scala.collection.immutable.Seq

sealed trait Result[+A] {
  val problems: Seq[Message]

  // For for comprehension
  def withFilter(op: A => Boolean): Result[A] = this match {
    case Success(r, ps) => if (op(r)) this else Failure(ps)
    case f: Failure     => f
  }

  def flatMap[B](op: A => Result[B]): Result[B] = this match {
    case Success(r, ps) => op(r).withProblems(ps)
    case f: Failure     => f
  }

  def map[B](op: A => B): Result[B] = this match {
    case Success(r, ps) => Success(op(r), ps)
    case f: Failure     => f
  }

  def withProblems(newProblems: Seq[Message]): Result[A] = this match {
    case Success(r, ps) => Success(r, ps ++ newProblems)
    case Failure(ps)    => Failure(ps ++ newProblems)
  }

  def withProblems(newProblems: A => Seq[Message]): Result[A] = this match {
    case Success(r, ps) => Success(r, ps ++ newProblems(r))
    case f: Failure     => f
  }

  def getOrElse[B >: A](default: => B): B = this match {
    case Success(r, _) => r
    case _: Failure    => default
  }

  def get: A = getOrElse(throw new NoSuchElementException(s"No result for $this"))

  def isFailure: Boolean

  def warnings: Seq[Warning] = problems.collect { case w: Warning => w }
  def errors: Seq[Error] = problems.collect { case e: Error => e }
  def hasErrors: Boolean = errors.nonEmpty
}

final case class Success[+A](result: A, problems: Seq[Message] = Nil) extends Result[A] {
  override def isFailure: Boolean = false
}

final case class Failure(problems: Seq[Message] = Nil) extends Result[Nothing] {
  override def isFailure: Boolean = true
}
object Failure {
  def apply(msg: String, file: ObtFile): Failure = Failure(Seq(Error(msg, file, 0)))
}

object Result {
  def unwrap[A](rs: ResultSeq[A]): Result[Seq[A]] = rs.value

  def optional[A](predicate: Boolean)(v: => Result[A]): Result[Option[A]] =
    if (predicate) v.map(Some(_)) else Success(None)

  def sequence[A](results: Seq[Result[A]]): Result[Seq[A]] = traverse(results)(identity)

  // Note, this always returns SuccessSeq in order that we can capture as many problems as possible
  def traverse[A, B](as: Seq[A])(op: A => Result[B]): Success[Seq[B]] = {
    as.foldLeft[Success[Seq[B]]](Success(Nil)) { case (accum, a) =>
      (accum, op(a)) match {
        case (Success(bs, ps), Success(b, newPs)) => Success(bs ++ Seq(b), ps ++ newPs)
        case (Success(bs, ps), b)                 => Success(bs, ps ++ b.problems)
      }
    }
  }

  def traverse[A, B](as: Result[Seq[A]])(op: A => Result[B]): Result[Seq[B]] = as.flatMap(traverse(_)(op))

  def traverseWithFilter[A, B](as: Result[Seq[A]])(op: A => Result[B])(f: A => Boolean): Result[Seq[B]] =
    as.flatMap(s => traverse(s.filter(f))(op))

  def tryWith[A](file: ObtFile, line: Int)(op: => Result[A]): Result[A] =
    try op
    catch {
      case e: ConfigException =>
        val actualLine = if (e.origin() != null) e.origin().lineNumber() else line
        Error(e.getMessage, file, actualLine).failure
      case e: AssertionError =>
        Error(e.getMessage, file, line).failure
    }

  def tryWith[A](file: ObtFile, configValue: ConfigValue)(op: => Result[A]): Result[A] =
    tryWith(file, configValue.origin().lineNumber())(op)

  @inline
  def tryWith[A](file: ObtFile, config: Config)(op: => Result[A]): Result[A] =
    tryWith(file, config.origin().lineNumber())(op)

  def withProblemsFrom[A](v: A)(messagesSources: Result[_]*): Result[A] =
    Success(v, messagesSources.flatMap(_.problems).to(Seq))
}

trait ResultMapper {
  def map[A, B, C](ra: Result[A], rb: Result[B])(op: (A, B) => C): Result[C] = (ra, rb) match {
    case (Success(a, aProbs), Success(b, bProbs)) => Success(op(a, b), aProbs ++ bProbs)
    case _                                        => Failure(ra.problems ++ rb.problems)
  }
}

// Primarily intended for use to make for comprehensions easier
object ResultSeq {
  def apply[A](result: Result[Seq[A]]): ResultSeq[A] = result match {
    case Success(rs, ps) => SuccessSeq(rs, ps)
    case Failure(ps)     => FailureSeq(ps)
  }

  def single[A](result: Result[A]): ResultSeq[A] = result match {
    case Success(r, ps) => SuccessSeq(Seq(r), ps)
    case Failure(ps)    => FailureSeq(ps)
  }

  def sequence[A](as: Seq[ResultSeq[A]]): ResultSeq[A] = traverse(as)(identity)

  // Note, this always returns SuccessSeq in order that we can capture as many problems as possible
  def traverse[A, B](as: Seq[A])(op: A => ResultSeq[B]): SuccessSeq[B] = {
    as.foldLeft[SuccessSeq[B]](SuccessSeq(Nil, Nil)) { case (accum, a) =>
      (accum, op(a)) match {
        case (SuccessSeq(bs, ps), SuccessSeq(newBs, newPs)) => SuccessSeq(bs ++ newBs, ps ++ newPs)
        case (SuccessSeq(bs, ps), b)                        => SuccessSeq(bs, ps ++ b.problems)
      }
    }
  }
}
sealed trait ResultSeq[+A] {
  def problems: Seq[Message]

  def withFilter(op: A => Boolean): ResultSeq[A] = this match {
    case SuccessSeq(rs, ps) => SuccessSeq(rs.filter(op), ps)
    case f: FailureSeq      => f
  }

  def flatMap[B](op: A => ResultSeq[B]): ResultSeq[B] = this match {
    case SuccessSeq(rs, ps) => ResultSeq.traverse(rs)(op).withProblems(ps)
    case f: FailureSeq      => f
  }

  def map[B](op: A => B): ResultSeq[B] = this match {
    case SuccessSeq(rs, ps) => SuccessSeq(rs.map(op), ps)
    case f: FailureSeq      => f
  }

  def value: Result[Seq[A]] = this match {
    case SuccessSeq(rs, ps) => Success(rs, ps)
    case FailureSeq(ps)     => Failure(ps)
  }

  protected def withProblems(newProblems: Seq[Message]): ResultSeq[A] = this match {
    case SuccessSeq(r, ps) => SuccessSeq(r, ps ++ newProblems)
    case FailureSeq(ps)    => FailureSeq(ps ++ newProblems)
  }
}
final case class SuccessSeq[+A](results: Seq[A], problems: Seq[Message] = Nil) extends ResultSeq[A]
final case class FailureSeq(problems: Seq[Message] = Nil) extends ResultSeq[Nothing]
