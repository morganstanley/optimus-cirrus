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

import optimus.platform.relational.tree.typeInfo
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.relational.RelationalException

/**
 * Average implementation
 */
trait Avg[T] extends Aggregator[T] {
  val valueType: TypeInfo[T]
  def default: Result = throw new RelationalException("Sequence contains no elements")
}

object Avg {
  trait DoubleLongIntAvg[T] extends Avg[T] {
    type Result = Double
    type Seed = (Array[Double], Array[Long])
    val resultType = TypeInfo.DOUBLE
    def seed: Seed = (Array(0.0), Array(0L))
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      s1._1(0) += s2._1(0)
      s1._2(0) += s2._2(0)
      s1
    }
    def result(s: Seed): Option[Result] = {
      if (s._2(0) == 0L) None else Some(s._1(0) / s._2(0).toDouble)
    }
  }

  trait IntAvg extends DoubleLongIntAvg[Int] {
    val valueType = TypeInfo.INT
    def accumulate(v: Int, s: Seed, state: LoopState): Seed = {
      s._1(0) += v
      s._2(0) += 1L
      s
    }
    override def evaluateAll(values: Iterable[Int]): Option[Double] = {
      evaluateAll(values.toArray)
    }
    def evaluateAll(values: Array[Int]): Option[Double] = {
      val len = values.length
      if (len == 0) None
      else {
        var idx = 0
        var sum = 0.0
        while (idx < len) {
          sum += values(idx)
          idx += 1
        }
        Some(sum / len)
      }
    }
  }
  implicit object Int extends IntAvg

  trait LongAvg extends DoubleLongIntAvg[Long] {
    val valueType = TypeInfo.LONG
    def accumulate(v: Long, s: Seed, state: LoopState): Seed = {
      s._1(0) += v
      s._2(0) += 1L
      s
    }
    override def evaluateAll(values: Iterable[Long]): Option[Double] = {
      evaluateAll(values.toArray)
    }
    def evaluateAll(values: Array[Long]): Option[Double] = {
      Sum.Long.evaluateAll(values).map { sum =>
        sum.toDouble / values.length
      }
    }
  }
  implicit object Long extends LongAvg

  trait DoubleAvg extends DoubleLongIntAvg[Double] {
    val valueType = TypeInfo.DOUBLE
    def accumulate(v: Double, s: Seed, state: LoopState): Seed = {
      s._1(0) += v
      s._2(0) += 1L
      s
    }
    override def evaluateAll(values: Iterable[Double]): Option[Double] = {
      evaluateAll(values.toArray)
    }
    def evaluateAll(values: Array[Double]): Option[Double] = {
      Sum.Double.evaluateAll(values).map { sum =>
        val len = values.length
        val xbar = sum / len
        var correction = 0.0
        var idx = 0
        while (idx < len) {
          correction += values(idx) - xbar
          idx += 1
        }
        xbar + (correction / len)
      }
    }
  }
  implicit object Double extends DoubleAvg

  trait FloatShortByteAvg[T] extends Avg[T] {
    type Result = Float
    type Seed = (Array[Double], Array[Long])
    val resultType = TypeInfo.FLOAT
    def seed: Seed = (Array(0.0), Array(0L))
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      s1._1(0) += s2._1(0)
      s1._2(0) += s2._2(0)
      s1
    }
    def result(s: Seed): Option[Result] = {
      if (s._2(0) == 0L) None else Some((s._1(0) / s._2(0)).toFloat)
    }
  }

  trait FloatAvg extends FloatShortByteAvg[Float] {
    val valueType = TypeInfo.FLOAT
    def accumulate(v: Float, s: Seed, state: LoopState): Seed = {
      s._1(0) += v
      s._2(0) += 1L
      s
    }
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
        val xbar = sum / len
        var correction = 0.0
        idx = 0
        while (idx < len) {
          correction += values(idx) - xbar
          idx += 1
        }
        Some((xbar + (correction / len)).toFloat)
      }
    }
  }
  implicit object Float extends FloatAvg

  trait ShortAvg extends FloatShortByteAvg[Short] {
    val valueType = TypeInfo.SHORT
    def accumulate(v: Short, s: Seed, state: LoopState): Seed = {
      s._1(0) += v
      s._2(0) += 1L
      s
    }
    override def evaluateAll(values: Iterable[Short]): Option[Float] = {
      evaluateAll(values.toArray)
    }
    def evaluateAll(values: Array[Short]): Option[Float] = {
      val len = values.length
      if (len == 0) None
      else {
        var idx = 0
        var sum = 0.0
        while (idx < len) {
          sum += values(idx)
          idx += 1
        }
        Some((sum / len).toFloat)
      }
    }
  }
  implicit object Short extends ShortAvg

  trait ByteAvg extends FloatShortByteAvg[Byte] {
    val valueType = TypeInfo.BYTE
    def accumulate(v: Byte, s: Seed, state: LoopState): Seed = {
      s._1(0) += v
      s._2(0) += 1L
      s
    }
  }
  implicit object Byte extends ByteAvg

  trait BigDecimalAvg extends Avg[BigDecimal] {
    type Result = BigDecimal
    type Seed = (Array[BigDecimal], Array[Long])

    val valueType = typeInfo[BigDecimal]
    val resultType = typeInfo[BigDecimal]
    private val zero: BigDecimal = 0
    def seed: Seed = (Array(zero), Array(0L))
    def accumulate(v: BigDecimal, s: Seed, state: LoopState): Seed = {
      if (v ne null) {
        s._1(0) += v
        s._2(0) += 1L
      }
      s
    }
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      s1._1(0) += s2._1(0)
      s1._2(0) += s2._2(0)
      s1
    }
    def result(s: Seed): Option[Result] = if (s._2(0) == 0L) None else Some(s._1(0) / s._2(0))
    override def default: Result = null
  }
  implicit object BigDecimal extends BigDecimalAvg

  type Aux[T, R] = Avg[T] { type Result = R }

  implicit def Option[T](implicit avg: Avg[T]): Aux[Option[T], Option[avg.Result]] = {
    implicit val tType = avg.valueType
    implicit val rType = avg.resultType
    new Avg[Option[T]] {
      type Result = Option[avg.Result]
      type Seed = avg.Seed

      val valueType = typeInfo[Option[T]]
      val resultType = TypeInfo(classOf[Option[_]], rType).cast[Result]

      def seed: Seed = avg.seed
      def accumulate(v: Option[T], s: Seed, state: LoopState): Seed = {
        if (v eq null) s else v.map(x => avg.accumulate(x, s, state)).getOrElse(s)
      }
      def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
        avg.combine(s1, s2, state)
      }
      def result(s: Seed): Option[Result] = avg.result(s).map(t => Some(t))
      override def default: Result = None
      override def evaluateAll(values: Iterable[Option[T]]): Option[Result] = {
        avg.evaluateAll(values.withFilter(_ ne null).flatMap(t => t)).map(t => Some(t))
      }
    }
  }
}
