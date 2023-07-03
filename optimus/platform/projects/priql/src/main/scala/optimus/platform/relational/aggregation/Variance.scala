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

import optimus.platform.relational.RelationalException
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.relational.tree.typeInfo

trait Variance[T] extends Aggregator[T] {
  def isBiasCorrected: Boolean
  def valueType: TypeInfo[T]
  def default: Result = throw new RelationalException("Sequence contains no elements")
}

trait PopulationVariance[T] extends Variance[T] {
  def isBiasCorrected = false
}

trait SampleVariance[T] extends Variance[T] {
  def isBiasCorrected = true
}

object Variance {
  trait DoubleFloatIntVariance[T] extends Variance[T] {
    type Result = Double
    type Seed = (Array[Double], Array[Int])
    val resultType = TypeInfo.DOUBLE
    def seed: Seed = (Array(0.0, 0.0), Array(0))
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      val sum1 = s1._1(1)
      val sum2 = s2._1(1)
      val n1 = s1._2(0)
      val n2 = s2._2(0)

      val x2sum1: BigDecimal = if (n1 == 0) 0 else s1._1(0) + BigDecimal(sum1) * sum1 / n1
      val x2sum2: BigDecimal = if (n2 == 0) 0 else s2._1(0) + BigDecimal(sum2) * sum2 / n2
      val n = n1 + n2
      val sum = sum1 + sum2

      s1._1(0) = if (n <= 1) 0 else (x2sum1 + x2sum2 - BigDecimal(sum) * sum / n).toDouble
      s1._1(1) = sum
      s1._2(0) = n
      s1
    }
    def result(s: Seed): Option[Result] = {
      val n = s._2(0)
      if (n == 0) None
      else if (n == 1) Some(0.0)
      else {
        Some(s._1(0) / (if (isBiasCorrected) (n - 1.0) else n))
      }
    }
    final def accumulateDouble(
        v: Double,
        s: (Array[Double], Array[Int]),
        state: LoopState): (Array[Double], Array[Int]) = {
      val sum0 = s._1(1)
      val n0 = s._2(0)
      val sum1 = sum0 + v
      val n1 = n0 + 1
      s._1(1) = sum1
      s._2(0) = n1
      if (n0 > 0)
        s._1(0) += (v - sum0 / n0) * (v - sum1 / n1)
      s
    }
  }

  trait DoubleVariance extends DoubleFloatIntVariance[Double] {
    val valueType = TypeInfo.DOUBLE
    def accumulate(v: Double, s: (Array[Double], Array[Int]), state: LoopState): (Array[Double], Array[Int]) = {
      accumulateDouble(v, s, state)
    }
    override def evaluateAll(values: Iterable[Double]): Option[Double] = {
      evaluateAll(values.toArray)
    }
    def evaluateAll(values: Array[Double]): Option[Double] = {
      val len = values.length
      if (len == 0) None
      else if (len == 1) Some(0.0)
      else {
        val mean = Avg.Double.evaluateAll(values).get
        evaluateAll(values, mean, len)
      }
    }

    private[aggregation] def evaluateAll(values: Array[Double], mean: Double, len: Int): Option[Double] = {
      var accum = 0.0
      var dev = 0.0
      var accum2 = 0.0
      var idx = 0
      while (idx < len) {
        dev = values(idx) - mean
        accum += dev * dev
        accum2 += dev
        idx += 1
      }
      Some((accum - (accum2 * accum2 / len)) / (if (isBiasCorrected) (len - 1.0) else len))
    }
  }

  trait IntVariance extends DoubleFloatIntVariance[Int] {
    val valueType = TypeInfo.INT
    def accumulate(v: Int, s: (Array[Double], Array[Int]), state: LoopState): (Array[Double], Array[Int]) = {
      accumulateDouble(v, s, state)
    }
    override def evaluateAll(values: Iterable[Int]): Option[Double] = {
      evaluateAll(values.toArray)
    }
    def evaluateAll(values: Array[Int]): Option[Double] = {
      val len = values.length
      if (len == 0) None
      else if (len == 1) Some(0.0)
      else {
        val mean = Avg.Int.evaluateAll(values).get
        var accum = 0.0
        var dev = 0.0
        var accum2 = 0.0
        var idx = 0
        while (idx < len) {
          dev = values(idx) - mean
          accum += dev * dev
          accum2 += dev
          idx += 1
        }
        Some((accum - (accum2 * accum2 / len)) / (if (isBiasCorrected) (len - 1.0) else len))
      }
    }
  }

  trait FloatVariance extends DoubleFloatIntVariance[Float] {
    val valueType = TypeInfo.FLOAT
    def accumulate(v: Float, s: (Array[Double], Array[Int]), state: LoopState): (Array[Double], Array[Int]) = {
      accumulateDouble(v, s, state)
    }
    override def evaluateAll(values: Iterable[Float]): Option[Double] = {
      evaluateAll(values.toArray)
    }
    def evaluateAll(values: Array[Float]): Option[Double] = {
      val len = values.length
      if (len == 0) None
      else if (len == 1) Some(0.0)
      else {
        val mean = Avg.Float.evaluateAll(values).get
        var accum = 0.0
        var dev = 0.0
        var accum2 = 0.0
        var idx = 0
        while (idx < len) {
          dev = values(idx) - mean
          accum += dev * dev
          accum2 += dev
          idx += 1
        }
        Some((accum - (accum2 * accum2 / len)) / (if (isBiasCorrected) (len - 1.0) else len))
      }
    }
  }

  trait BigDecimalVariance extends Variance[BigDecimal] {
    type Result = BigDecimal
    type Seed = (Array[BigDecimal], Array[Int])
    val valueType = typeInfo[BigDecimal]
    val resultType = valueType
    def seed: Seed = (Array[BigDecimal](0, 0), Array(0))
    def accumulate(
        v: BigDecimal,
        s: (Array[BigDecimal], Array[Int]),
        state: LoopState): (Array[BigDecimal], Array[Int]) = {
      val sum0 = s._1(1)
      val n0 = s._2(0)
      val sum1 = sum0 + v
      val n1 = n0 + 1
      s._1(1) = sum1
      s._2(0) = n1
      if (n0 > 0)
        s._1(0) += (v - sum0 / n0) * (v - sum1 / n1)
      s
    }
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      val sum1 = s1._1(1)
      val sum2 = s2._1(1)
      val n1 = s1._2(0)
      val n2 = s2._2(0)

      val x2sum1: BigDecimal = if (n1 == 0) 0 else s1._1(0) + sum1 * sum1 / n1
      val x2sum2: BigDecimal = if (n2 == 0) 0 else s2._1(0) + sum2 * sum2 / n2
      val n = n1 + n2
      val sum = sum1 + sum2

      s1._1(0) = if (n <= 1) 0 else (x2sum1 + x2sum2 - sum * sum / n)
      s1._1(1) = sum
      s1._2(0) = n
      s1
    }
    def result(s: Seed): Option[Result] = {
      val n = s._2(0)
      if (n == 0) None
      else if (n == 1) Some(0)
      else {
        Some(s._1(0) / (n - 1))
      }
    }
    override def evaluateAll(values: Iterable[BigDecimal]): Option[BigDecimal] = {
      evaluateAll(values.toArray)
    }
    def evaluateAll(values: Array[BigDecimal]): Option[BigDecimal] = {
      val len = values.length
      if (len == 0) None
      else if (len == 1) Some(0)
      else {
        val mean = Avg.BigDecimal.evaluateAll(values).get
        var accum: BigDecimal = 0
        var dev: BigDecimal = 0
        var accum2: BigDecimal = 0
        var idx = 0
        while (idx < len) {
          dev = values(idx) - mean
          accum += dev * dev
          accum2 += dev
          idx += 1
        }
        Some((accum - (accum2 * accum2 / len)) / (len - 1.0))
      }
    }
  }

  type Aux[T, R] = Variance[T] { type Result = R }
  implicit def doubleVariance: Aux[Double, Double] = SampleVariance.Double
  implicit def intVariance: Aux[Int, Double] = SampleVariance.Int
  implicit def floatVariance: Aux[Float, Double] = SampleVariance.Float
  implicit def bigDecimalVariance: Aux[BigDecimal, BigDecimal] = SampleVariance.BigDecimal
  implicit def optionVariance[T](implicit vr: Variance[T]): Aux[Option[T], Option[vr.Result]] = {
    implicit val tType = vr.valueType
    implicit val rType = vr.resultType
    new SampleVariance[Option[T]] {
      type Result = Option[vr.Result]
      type Seed = vr.Seed

      val valueType = typeInfo[Option[T]]
      val resultType = TypeInfo(classOf[Option[_]], rType).cast[Result]

      def seed: Seed = vr.seed
      def accumulate(v: Option[T], s: Seed, state: LoopState): Seed = {
        if (v eq null) s else v.map(x => vr.accumulate(x, s, state)).getOrElse(s)
      }
      def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
        vr.combine(s1, s2, state)
      }
      def result(s: Seed): Option[Result] = vr.result(s).map(t => Some(t))
      override def default: Result = None
      override def evaluateAll(values: Iterable[Option[T]]): Option[Result] = {
        vr.evaluateAll(values.withFilter(_ ne null).flatMap(t => t)).map(t => Some(t))
      }
    }
  }
}

object SampleVariance {
  implicit object Double extends SampleVariance[Double] with Variance.DoubleVariance
  implicit object Int extends SampleVariance[Int] with Variance.IntVariance
  implicit object Float extends SampleVariance[Float] with Variance.FloatVariance
  implicit object BigDecimal extends SampleVariance[BigDecimal] with Variance.BigDecimalVariance

  type Aux[T, R] = SampleVariance[T] { type Result = R }

  implicit def Option[T](implicit sVar: SampleVariance[T]): Aux[Option[T], Option[sVar.Result]] = {
    implicit val tType = sVar.valueType
    implicit val rType = sVar.resultType
    new SampleVariance[Option[T]] {
      type Result = Option[sVar.Result]
      type Seed = sVar.Seed

      val valueType = typeInfo[Option[T]]
      val resultType = TypeInfo(classOf[Option[_]], rType).cast[Result]

      def seed: Seed = sVar.seed
      def accumulate(v: Option[T], s: Seed, state: LoopState): Seed = {
        if (v eq null) s else v.map(x => sVar.accumulate(x, s, state)).getOrElse(s)
      }
      def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
        sVar.combine(s1, s2, state)
      }
      def result(s: Seed): Option[Result] = sVar.result(s).map(t => Some(t))
      override def default: Result = None
      override def evaluateAll(values: Iterable[Option[T]]): Option[Result] = {
        sVar.evaluateAll(values.withFilter(_ ne null).flatMap(t => t)).map(t => Some(t))
      }
    }
  }
}

object PopulationVariance {
  implicit object Double extends PopulationVariance[Double] with Variance.DoubleVariance
  implicit object Int extends PopulationVariance[Int] with Variance.IntVariance
  implicit object Float extends PopulationVariance[Float] with Variance.FloatVariance
  implicit object BigDecimal extends PopulationVariance[BigDecimal] with Variance.BigDecimalVariance

  type Aux[T, R] = PopulationVariance[T] { type Result = R }

  implicit def Option[T](implicit pVar: PopulationVariance[T]): Aux[Option[T], Option[pVar.Result]] = {
    implicit val tType = pVar.valueType
    implicit val rType = pVar.resultType
    new PopulationVariance[Option[T]] {
      type Result = Option[pVar.Result]
      type Seed = pVar.Seed

      val valueType = typeInfo[Option[T]]
      val resultType = TypeInfo(classOf[Option[_]], rType).cast[Result]

      def seed: Seed = pVar.seed
      def accumulate(v: Option[T], s: Seed, state: LoopState): Seed = {
        if (v eq null) s else v.map(x => pVar.accumulate(x, s, state)).getOrElse(s)
      }
      def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
        pVar.combine(s1, s2, state)
      }
      def result(s: Seed): Option[Result] = pVar.result(s).map(t => Some(t))
      override def default: Result = None
      override def evaluateAll(values: Iterable[Option[T]]): Option[Result] = {
        pVar.evaluateAll(values.withFilter(_ ne null).flatMap(t => t)).map(t => Some(t))
      }
    }
  }
}
