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

trait StandardDeviation[T] extends Aggregator[T] {
  def isBiasCorrected: Boolean
  def valueType: TypeInfo[T]
}

trait PopulationStandardDeviation[T] extends StandardDeviation[T] {
  def isBiasCorrected = false
}

trait SampleStandardDeviation[T] extends StandardDeviation[T] {
  def isBiasCorrected = true
}

object StandardDeviation {
  type Aux[T, R] = StandardDeviation[T] { type Result = R }
  implicit def doubleStddev: Aux[Double, Double] = SampleStandardDeviation.Double
  implicit def intStddev: Aux[Int, Double] = SampleStandardDeviation.Int
  implicit def floatStddev: Aux[Float, Double] = SampleStandardDeviation.Float
  implicit def bigDecimalStddev: Aux[BigDecimal, BigDecimal] = SampleStandardDeviation.BigDecimal
  implicit def optionStddev[T](implicit stdDev: StandardDeviation[T]): Aux[Option[T], Option[stdDev.Result]] = {
    implicit val tType = stdDev.valueType
    implicit val rType = stdDev.resultType
    new SampleStandardDeviation[Option[T]] {
      type Result = Option[stdDev.Result]
      type Seed = stdDev.Seed

      val valueType = typeInfo[Option[T]]
      val resultType = TypeInfo(classOf[Option[_]], rType).cast[Result]

      def seed: Seed = stdDev.seed
      def accumulate(v: Option[T], s: Seed, state: LoopState): Seed = {
        if (v eq null) s else v.map(x => stdDev.accumulate(x, s, state)).getOrElse(s)
      }
      def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
        stdDev.combine(s1, s2, state)
      }
      def result(s: Seed): Option[Result] = stdDev.result(s).map(t => Some(t))
      override def default: Result = None
      override def evaluateAll(values: Iterable[Option[T]]): Option[Result] = {
        stdDev.evaluateAll(values.withFilter(_ ne null).flatMap(t => t)).map(t => Some(t))
      }
    }
  }
}

object SampleStandardDeviation {
  implicit object Double extends SampleStandardDeviation[Double] with Variance.DoubleVariance {
    override def result(s: (Array[Double], Array[Int])): Option[Double] = {
      super.result(s).map(v => math.sqrt(v))
    }
    override private[aggregation] def evaluateAll(values: Array[Double], mean: Double, len: Int): Option[Double] = {
      super.evaluateAll(values, mean, len).map(v => math.sqrt(v))
    }
  }
  implicit object Int extends SampleStandardDeviation[Int] with Variance.IntVariance {
    override def result(s: (Array[Double], Array[Int])): Option[Double] = {
      super.result(s).map(v => math.sqrt(v))
    }
    override def evaluateAll(values: Array[Int]): Option[Double] = {
      super.evaluateAll(values).map(v => math.sqrt(v))
    }
  }
  implicit object Float extends SampleStandardDeviation[Float] with Variance.FloatVariance {
    override def result(s: (Array[Double], Array[Int])): Option[Double] = {
      super.result(s).map(v => math.sqrt(v))
    }
    override def evaluateAll(values: Array[Float]): Option[Double] = {
      super.evaluateAll(values).map(v => math.sqrt(v))
    }
  }
  implicit object BigDecimal extends SampleStandardDeviation[BigDecimal] with Variance.BigDecimalVariance {
    override def result(s: (Array[BigDecimal], Array[Int])): Option[BigDecimal] = {
      super.result(s).map(v => math.sqrt(v.doubleValue))
    }
    override def evaluateAll(values: Array[BigDecimal]): Option[BigDecimal] = {
      super.evaluateAll(values).map(v => math.sqrt(v.doubleValue))
    }
  }

  type Aux[T, R] = SampleStandardDeviation[T] { type Result = R }

  implicit def Option[T](implicit stdDev: SampleStandardDeviation[T]): Aux[Option[T], Option[stdDev.Result]] = {
    implicit val tType = stdDev.valueType
    implicit val rType = stdDev.resultType
    new SampleStandardDeviation[Option[T]] {
      type Result = Option[stdDev.Result]
      type Seed = stdDev.Seed

      val valueType = typeInfo[Option[T]]
      val resultType = TypeInfo(classOf[Option[_]], rType).cast[Result]

      def seed: Seed = stdDev.seed
      def accumulate(v: Option[T], s: Seed, state: LoopState): Seed = {
        if (v eq null) s else v.map(x => stdDev.accumulate(x, s, state)).getOrElse(s)
      }
      def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
        stdDev.combine(s1, s2, state)
      }
      def result(s: Seed): Option[Result] = stdDev.result(s).map(t => Some(t))
      override def default: Result = None
      override def evaluateAll(values: Iterable[Option[T]]): Option[Result] = {
        stdDev.evaluateAll(values.withFilter(_ ne null).flatMap(t => t)).map(t => Some(t))
      }
    }
  }
}

object PopulationStandardDeviation {
  implicit object Double extends PopulationStandardDeviation[Double] with Variance.DoubleVariance {
    override def result(s: (Array[Double], Array[Int])): Option[Double] = {
      super.result(s).map(v => math.sqrt(v))
    }

    override private[aggregation] def evaluateAll(values: Array[Double], mean: Double, len: Int): Option[Double] = {
      super.evaluateAll(values, mean, len).map(v => math.sqrt(v))
    }
  }
  implicit object Int extends PopulationStandardDeviation[Int] with Variance.IntVariance {
    override def result(s: (Array[Double], Array[Int])): Option[Double] = {
      super.result(s).map(v => math.sqrt(v))
    }
    override def evaluateAll(values: Array[Int]): Option[Double] = {
      super.evaluateAll(values).map(v => math.sqrt(v))
    }
  }
  implicit object Float extends PopulationStandardDeviation[Float] with Variance.FloatVariance {
    override def result(s: (Array[Double], Array[Int])): Option[Double] = {
      super.result(s).map(v => math.sqrt(v))
    }
    override def evaluateAll(values: Array[Float]): Option[Double] = {
      super.evaluateAll(values).map(v => math.sqrt(v))
    }
  }
  implicit object BigDecimal extends PopulationStandardDeviation[BigDecimal] with Variance.BigDecimalVariance {
    override def result(s: (Array[BigDecimal], Array[Int])): Option[BigDecimal] = {
      super.result(s).map(v => math.sqrt(v.doubleValue))
    }
    override def evaluateAll(values: Array[BigDecimal]): Option[BigDecimal] = {
      super.evaluateAll(values).map(v => math.sqrt(v.doubleValue))
    }
  }

  type Aux[T, R] = PopulationStandardDeviation[T] { type Result = R }

  implicit def Option[T](implicit stdDev: PopulationStandardDeviation[T]): Aux[Option[T], Option[stdDev.Result]] = {
    implicit val tType = stdDev.valueType
    implicit val rType = stdDev.resultType
    new PopulationStandardDeviation[Option[T]] {
      type Result = Option[stdDev.Result]
      type Seed = stdDev.Seed

      val valueType = typeInfo[Option[T]]
      val resultType = TypeInfo(classOf[Option[_]], rType).cast[Result]

      def seed: Seed = stdDev.seed
      def accumulate(v: Option[T], s: Seed, state: LoopState): Seed = {
        if (v eq null) s else v.map(x => stdDev.accumulate(x, s, state)).getOrElse(s)
      }
      def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
        stdDev.combine(s1, s2, state)
      }
      def result(s: Seed): Option[Result] = stdDev.result(s).map(t => Some(t))
      override def default: Result = None
      override def evaluateAll(values: Iterable[Option[T]]): Option[Result] = {
        stdDev.evaluateAll(values.withFilter(_ ne null).flatMap(t => t)).map(t => Some(t))
      }
    }
  }
}
