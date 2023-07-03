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

trait Correlation[T] extends Aggregator[(T, T)] {
  type Result = Double
  val resultType = TypeInfo.DOUBLE
  def default: Result = throw new RelationalException("Sequence contains less than 2 elements")
}

object Correlation {
  type Aux[T, R] = Correlation[T] { type Result = R }
  implicit def doubleCorrelation: Aux[Double, Double] = PearsonsCorrelation

  trait PearsonsCorrelation extends Correlation[Double] {
    type StddevSeed = (Array[Double], Array[Int])
    type SumSeed = (Array[Double], Array[Boolean])
    type Seed = (StddevSeed, StddevSeed, SumSeed)

    def seed: Seed = {
      (SampleStandardDeviation.Double.seed, SampleStandardDeviation.Double.seed, Sum.Double.seed)
    }

    def accumulate(v: (Double, Double), s: Seed, state: LoopState): Seed = {
      val s1 = SampleStandardDeviation.Double.accumulate(v._1, s._1, state)
      val s2 = SampleStandardDeviation.Double.accumulate(v._2, s._2, state)
      val s3 = Sum.Double.accumulate(v._1 * v._2, s._3, state)
      if ((s1 eq s._1) && (s2 eq s._2) && (s3 eq s._3)) s else (s1, s2, s3)
    }

    def combine(seed1: Seed, seed2: Seed, state: LoopState): Seed = {
      val s1 = SampleStandardDeviation.Double.combine(seed1._1, seed2._1, state)
      val s2 = SampleStandardDeviation.Double.combine(seed1._2, seed2._2, state)
      val s3 = Sum.Double.combine(seed1._3, seed2._3, state)
      (s1, s2, s3)
    }

    override def result(s: Seed): Option[Double] = {
      val n = s._1._2(0)
      if (n < 2) None
      else {
        val sumx = s._1._1(1)
        val sumy = s._2._1(1)
        val sumxy = Sum.Double.result(s._3).get
        val stddevx = SampleStandardDeviation.Double.result(s._1).get
        val stddevy = SampleStandardDeviation.Double.result(s._2).get

        val meanx = sumx / n
        val meany = sumy / n
        Some((sumxy - n * meanx * meany) / (n - 1) / stddevx / stddevy)
      }
    }

    override def evaluateAll(values: Iterable[(Double, Double)]): Option[Double] = {
      val (x, y) = values.unzip
      evaluateAll(x.toArray, y.toArray)
    }

    def evaluateAll(x: Array[Double], y: Array[Double]): Option[Double] = {
      require(x.length == y.length)
      val len = x.length
      if (len < 2) None
      else {
        val mean1 = Avg.Double.evaluateAll(x).get
        val mean2 = Avg.Double.evaluateAll(y).get
        val stddev1 = SampleStandardDeviation.Double.evaluateAll(x, mean1, len).get
        val stddev2 = SampleStandardDeviation.Double.evaluateAll(y, mean2, len).get

        var idx = 0
        var sum = 0.0
        while (idx < len) {
          sum += (x(idx) - mean1) * (y(idx) - mean2)
          idx += 1
        }
        Some(sum / (len - 1) / stddev1 / stddev2)
      }
    }
  }
  object PearsonsCorrelation extends PearsonsCorrelation
}
