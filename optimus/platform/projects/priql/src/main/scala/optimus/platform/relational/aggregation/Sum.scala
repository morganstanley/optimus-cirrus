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

import optimus.platform.relational.tree.TypeInfo
import optimus.platform.relational.tree.typeInfo
import optimus.platform.relational.RelationalException

/**
 * Sum implementation
 */
trait Sum[T] extends Aggregator[T] {
  type Result = T
  def default: Result = throw new RelationalException("Sequence contains no elements")
}

object Sum {
  trait IntSum extends Sum[Int] {
    type Seed = (Array[Int], Array[Boolean])
    val resultType = TypeInfo.INT
    def seed: Seed = (Array(0), Array(false))
    def accumulate(v: Int, s: Seed, state: LoopState): Seed = {
      s._1(0) += v
      s._2(0) = true
      s
    }
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      s1._1(0) += s2._1(0)
      s1._2(0) |= s2._2(0)
      s1
    }
    def result(s: Seed): Option[Result] = if (s._2(0)) Some(s._1(0)) else None
    override def evaluateAll(values: Iterable[Int]): Option[Int] = {
      evaluateAll(values.toArray)
    }
    def evaluateAll(values: Array[Int]): Option[Int] = {
      val len = values.length
      if (len == 0) None
      else {
        var idx = 0
        var sum = 0
        while (idx < len) {
          sum += values(idx)
          idx += 1
        }
        Some(sum)
      }
    }
  }
  implicit object Int extends IntSum

  trait LongSum extends Sum[Long] {
    type Seed = (Array[Long], Array[Boolean])
    val resultType = TypeInfo.LONG
    def seed: Seed = (Array(0L), Array(false))
    def accumulate(v: Long, s: Seed, state: LoopState): Seed = {
      s._1(0) += v
      s._2(0) = true
      s
    }
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      s1._1(0) += s2._1(0)
      s1._2(0) |= s2._2(0)
      s1
    }
    def result(s: Seed): Option[Result] = if (s._2(0)) Some(s._1(0)) else None
    override def evaluateAll(values: Iterable[Long]): Option[Long] = {
      evaluateAll(values.toArray)
    }
    def evaluateAll(values: Array[Long]): Option[Long] = {
      val len = values.length
      if (len == 0) None
      else {
        var idx = 0
        var sum = 0L
        while (idx < len) {
          sum += values(idx)
          idx += 1
        }
        Some(sum)
      }
    }
  }
  implicit object Long extends LongSum

  trait DoubleSum extends Sum[Double] {
    type Seed = (Array[Double], Array[Boolean])
    val resultType = TypeInfo.DOUBLE
    def seed: Seed = (Array(0.0), Array(false))
    def accumulate(v: Double, s: Seed, state: LoopState): Seed = {
      s._1(0) += v
      s._2(0) = true
      s
    }
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      s1._1(0) += s2._1(0)
      s1._2(0) |= s2._2(0)
      s1
    }
    def result(s: Seed): Option[Result] = if (s._2(0)) Some(s._1(0)) else None
    override def evaluateAll(values: Iterable[Double]): Option[Double] = {
      evaluateAll(values.toArray)
    }
    def evaluateAll(values: Array[Double]): Option[Double] = {
      val len = values.length
      if (len == 0) None
      else {
        var idx = 0
        var sum = 0.0
        while (idx < len) {
          sum += values(idx)
          idx += 1
        }
        Some(sum)
      }
    }
  }
  implicit object Double extends DoubleSum

  trait FloatSum extends Sum[Float] {
    type Seed = (Array[Float], Array[Boolean])
    val resultType = TypeInfo.FLOAT
    def seed: Seed = (Array(0f), Array(false))
    def accumulate(v: Float, s: Seed, state: LoopState): Seed = {
      s._1(0) += v
      s._2(0) = true
      s
    }
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      s1._1(0) += s2._1(0)
      s1._2(0) |= s2._2(0)
      s1
    }
    def result(s: Seed): Option[Result] = if (s._2(0)) Some(s._1(0)) else None
    override def evaluateAll(values: Iterable[Float]): Option[Float] = {
      evaluateAll(values.toArray)
    }
    def evaluateAll(values: Array[Float]): Option[Float] = {
      val len = values.length
      if (len == 0) None
      else {
        var idx = 0
        var sum = 0.0
        while (idx < len) {
          sum += values(idx)
          idx += 1
        }
        Some(sum.toFloat)
      }
    }
  }
  implicit object Float extends FloatSum

  class NumericSum[T: TypeInfo](val num: Numeric[T]) extends Sum[T] {
    import Aggregator.ObjectRef

    type Seed = (ObjectRef[T], Array[Boolean])
    val resultType = typeInfo[T]
    def seed: Seed = (new ObjectRef[T](null.asInstanceOf[T]), Array(false))

    def accumulate(v: T, s: Seed, state: LoopState): Seed = {
      if (v == null) s
      else {
        if (s._2(0) == false)
          s._1.ref = v
        else
          s._1.ref = num.plus(s._1.ref, v)
        s._2(0) = true
        s
      }
    }

    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      if (s1._2(0) && s2._2(0)) {
        s1._1.ref = num.plus(s1._1.ref, s2._1.ref)
      } else if (s2._2(0)) {
        s1._1.ref = s2._1.ref
      }
      s1._2(0) |= s2._2(0)
      s1
    }
    def result(s: Seed): Option[Result] = if (s._2(0)) Some(s._1.ref) else None
    override def default: Result = {
      if (!resultType.clazz.isPrimitive()) null.asInstanceOf[Result]
      else super.default
    }
  }
  implicit def Numeric[T: TypeInfo](implicit num: Numeric[T]): Sum[T] = {
    new NumericSum(num)
  }

  class OptionSum[T](val sum: Sum[T]) extends Sum[Option[T]] {
    type Seed = sum.Seed
    implicit val tType: TypeInfo[T] = sum.resultType
    val resultType = typeInfo[Option[T]]
    def seed: Seed = sum.seed
    def accumulate(v: Option[T], s: Seed, state: LoopState): Seed = {
      if (v eq null) s else v.map(x => sum.accumulate(x, s, state)).getOrElse(s)
    }
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      sum.combine(s1, s2, state)
    }
    def result(s: Seed): Option[Result] = sum.result(s).map(t => Some(t))
    override def default: Result = None
    override def evaluateAll(values: Iterable[Option[T]]): Option[Option[T]] = {
      sum.evaluateAll(values.withFilter(_ ne null).flatMap(t => t)).map(t => Some(t))
    }
  }
  implicit def Option[T](implicit sum: Sum[T]): Sum[Option[T]] = {
    new OptionSum[T](sum)
  }

  class SeqDoubleSum extends Sum[Seq[Double]] {
    type Seed = (Array[Seq[Double]], Array[Boolean])
    val resultType = new TypeInfo[Seq[Double]](Seq(classOf[Seq[Double]]), Nil, Nil, Nil)
    def seed: Seed = (Array(Seq.empty[Double]), Array(false))
    def accumulate(v: Seq[Double], s: Seed, state: LoopState): Seed = {
      if (s._1.length == 0) {
        s._1(0) = v
      } else {
        require(s._1.length == v.length, s"accumulate: seqs of different length ${s._1.length} != ${v.length}")
        s._1(0) = (s._1(0) zip v) map { case (a, b) => a + b }
      }
      s._2(0) = true
      s
    }
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      if (s1._1.length != 0 && s2._1.length != 0) {
        require(s1._1.length == s2._1.length, s"combine: seqs of different length ${s1._1.length} != ${s2._1.length}")
        s1._1(0) = (s1._1(0) zip s2._1(0)) map { case (a, b) => a + b }
      } else if (s1._1.length == 0) {
        s1._1(0) = s2._1(0)
      }
      s1._2(0) |= s2._2(0)
      s1
    }
    def result(s: Seed): Option[Result] = if (s._2(0)) Some(s._1(0)) else None
    override def evaluateAll(values: Iterable[Seq[Double]]): Option[Seq[Double]] = {
      evaluateAll(values.toArray)
    }
    def evaluateAll(values: Array[Seq[Double]]): Option[Seq[Double]] = {
      val len = values.length
      if (len == 0) None
      else {
        var idx = 0
        var sum = List.fill(values(0).length)(0.0)
        while (idx < len) {
          require(
            sum.length == values(idx).length,
            s"evaluateAll: row $idx of length ${values(idx).length} different from base seq with length ${sum.length}")
          sum = (sum zip values(idx)) map { case (a, b) => a + b }
          idx += 1
        }
        Some(sum)
      }
    }
  }
}
