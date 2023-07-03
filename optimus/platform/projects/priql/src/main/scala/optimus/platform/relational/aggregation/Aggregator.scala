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
package optimus.platform.relational.aggregation

import optimus.platform.relational.{RelationalUnsupportedException, RelationalException}
import optimus.platform.relational.tree.TypeInfo

/**
 * Base class for aggregators used by the Query[T] API
 */
trait Aggregator[-Value] extends Serializable {

  /**
   * type of the seed
   */
  type Seed

  /**
   * type of result
   */
  type Result

  /**
   * The seed used to do the aggregation
   */
  def seed: Seed

  /**
   * Accumulate the value to a seed
   */
  def accumulate(v: Value, s: Seed, state: LoopState): Seed

  /**
   * Combine the seeds, used for parallel aggregation
   */
  def combine(s1: Seed, s2: Seed, state: LoopState): Seed

  /**
   * Fetch the result from the given seed
   */
  def result(s: Seed): Option[Result]

  /**
   * default value is the result is None
   */
  def default: Result

  /**
   * TypeInfo of result
   */
  val resultType: TypeInfo[Result]

  /**
   * in addition to incremental API above, we also provide this (so that we could optimize when needed).
   */
  def evaluateAll(values: Iterable[Value]): Option[Result] = {
    var s = this.seed
    val state = new LoopState
    val srcIterator = values.iterator
    while (state.shouldContinue && srcIterator.hasNext) s = accumulate(srcIterator.next(), s, state)
    result(s)
  }
}

class LoopState private[aggregation] () {
  // since LoopState is only allowed to be initialized inside aggregation package
  // and now we only initialize it in Aggregator.aggregate, so no shared state among
  // different threads
  def done(): Unit = isDone = true
  private[aggregation] def shouldContinue: Boolean = !isDone
  private[this] var isDone = false
}

object LoopState {
  object Ignore extends LoopState {
    override def done(): Unit = {}
  }
}

object Aggregator {

  def aggregate[T](src: Iterable[T], aggr: Aggregator[T]): Any = {
    aggr.evaluateAll(src).getOrElse(aggr.default)
  }

  def reduce[T: TypeInfo](op: (T, T) => T): Aggregator[T] { type Result = T } = {
    new ReduceAggregator[T](op)
  }

  def reduceOption[T: TypeInfo](op: (T, T) => T): Aggregator[T] { type Result = Option[T] } = {
    new ReduceOptionAggregator[T](new ReduceAggregator[T](op))
  }

  def foldLeft[S, T](z: S)(op: (S, T) => S): Aggregator[T] { type Result = S } = {
    new FoldLeftAggregator[S, T](z, op)
  }

  def mkString: Aggregator[Any] { type Result = String } = mkString("")

  def mkString(sep: String): Aggregator[Any] { type Result = String } = mkString("", sep, "")

  def mkString(start: String, sep: String, end: String): Aggregator[Any] { type Result = String } = {
    new MakeStringAggregator(start, sep, end)
  }

  implicit object StringAggregator extends DefaultStringAggregator

  private[aggregation] class ObjectRef[T](var ref: T)

  private class ReduceAggregator[T: TypeInfo](op: (T, T) => T) extends Aggregator[T] {
    type Seed = (ObjectRef[T], Boolean)
    type Result = T

    def seed: Seed = (null, false)
    def accumulate(v: T, s: Seed, state: LoopState): Seed = {
      if (s._2) {
        s._1.ref = op(s._1.ref, v)
        s
      } else {
        (new ObjectRef(v), true)
      }
    }
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      if (s1._2 == false) s2
      else if (s2._2 == false) s1
      else {
        s1._1.ref = op(s1._1.ref, s2._1.ref)
        s1
      }
    }
    def result(s: Seed): Option[Result] = if (s._2) Some(s._1.ref) else None
    def default: Result = throw new RelationalException("Sequence contains no elements")
    val resultType: TypeInfo[Result] = implicitly[TypeInfo[Result]]
  }

  private class ReduceOptionAggregator[T: TypeInfo](val a: ReduceAggregator[T]) extends Aggregator[T] {
    type Seed = a.Seed
    type Result = Option[T]

    def seed: Seed = a.seed
    def accumulate(v: T, s: Seed, state: LoopState): Seed = {
      a.accumulate(v, s, state)
    }
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      a.combine(s1, s2, state)
    }
    def result(s: Seed): Option[Result] = a.result(s).map(t => Some(t))
    def default: Result = None
    val resultType: TypeInfo[Result] = implicitly[TypeInfo[Result]]
  }

  private class FoldLeftAggregator[S, T](z: S, op: (S, T) => S) extends Aggregator[T] {
    type Seed = S
    type Result = S
    def seed: Seed = z
    def accumulate(v: T, s: Seed, stat: LoopState): Seed = {
      op(s, v)
    }
    def combine(s1: Seed, s2: Seed, stat: LoopState): Seed = {
      throw new RelationalUnsupportedException("Combine is not defined for foldLeft")
    }
    def result(s: Seed): Option[Result] = Some(s)
    def default: Result = z
    val resultType: TypeInfo[Result] = TypeInfo.generic(z).asInstanceOf[TypeInfo[Result]]
  }

  private class MakeStringAggregator(start: String, sep: String, end: String) extends Aggregator[Any] {
    type Seed = (StringBuilder, Boolean)
    type Result = String
    def seed: Seed = (StringBuilder.newBuilder, false)
    def accumulate(v: Any, s: Seed, state: LoopState): Seed = {
      if (s._2 == false) {
        s._1.append(v)
        (s._1, true)
      } else {
        s._1.append(sep)
        s._1.append(v)
        s
      }
    }
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      if (s1._2 == false) s2
      else if (s2._2 == false) s1
      else {
        s1._1.append(sep)
        s1._1.append(s2._1)
        s1
      }
    }
    def result(s: Seed): Option[Result] = if (s._2) Some(s"$start${s._1.result()}$end") else None
    def default: String = s"$start$end"
    val resultType: TypeInfo[Result] = TypeInfo.STRING
  }

  trait DefaultStringAggregator extends Aggregator[String] {
    private val empty = new Seed("", true)
    type Seed = (String, Boolean)
    type Result = String
    def seed: Seed = ("", false)
    def accumulate(v: String, s: Seed, state: LoopState): Seed = {
      if (s._2 == false) new Seed(v, true)
      else if (v == s._1) s
      else {
        state.done()
        empty
      }
    }
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      if (s1._2 == false) s2
      else if (s2._2 == false) s1
      else {
        if (s1._1 == s2._1) s1
        else {
          state.done()
          empty
        }
      }
    }
    def result(s: Seed): Option[Result] = if (s._2) Some(s._1) else Some("")
    def default: String = ""
    val resultType: TypeInfo[Result] = TypeInfo.STRING
  }

  type Aux[T, R] = Aggregator[T] { type Result = R }

  implicit def Option[T](implicit aggregator: Aggregator[T]): Aux[Option[T], Option[aggregator.Result]] = {
    new Aggregator[Option[T]] {
      type Seed = aggregator.Seed
      type Result = Option[aggregator.Result]
      def seed: Seed = aggregator.seed
      def accumulate(v: Option[T], s: Seed, state: LoopState): Seed = {
        v.map(x => aggregator.accumulate(x, s, state)).getOrElse(s)
      }
      def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
        aggregator.combine(s1, s2, state)
      }
      def result(s: Seed): Option[Result] = aggregator.result(s).map(t => Some(t))
      override def default: Result = None
      val resultType: TypeInfo[Result] = implicitly[TypeInfo[Result]]
    }
  }
}
