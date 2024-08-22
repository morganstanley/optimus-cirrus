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
package optimus.platform.relational.dynamic

import optimus.platform.DynamicObject
import optimus.platform.relational.RelationalException
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.relational.aggregation._

import scala.collection.mutable.ListBuffer
import scala.math.BigDecimal

object AggregationType {
  object Sum {
    def apply(source: String) = AnySumAggregation(source)
  }

  object Min {
    def apply(source: String) = AnyMinAggregation(source)
  }

  object Max {
    def apply(source: String) = AnyMaxAggregation(source)
  }

  object Avg {
    def apply(source: String) = AnyAvgAggregation(source)
  }

  object Dev {
    def apply(source: String) = AnyDevAggregation(source)
  }

  def apply(s: String, source: String): AnyAggregation = {
    s.toUpperCase() match {
      case "SUM" => Sum(source)
      case "MIN" => Min(source)
      case "MAX" => Max(source)
      case "AVG" => Avg(source)
      case "DEV" => Dev(source)
      case _ =>
        throw new IllegalArgumentException(
          "Cannot parse string into aggregation type: " + s + ", available options are: Min,Max,Sum,Avg,Dev")
    }
  }
}

private[dynamic] class AsDoubleAggregator(val aggregator: Aggregator[Double]) extends Aggregator[Any] {
  type Result = aggregator.Result
  type Seed = aggregator.Seed
  def seed = aggregator.seed
  val resultType = aggregator.resultType

  def default: Result = aggregator.default

  def toDouble(x: Any): Double = {
    x match {
      case x: Int        => x.toDouble
      case x: Byte       => x.toDouble
      case x: Short      => x.toDouble
      case x: Long       => x.toDouble
      case x: Float      => x.toDouble
      case x: Double     => x
      case x: BigDecimal => x.toDouble
      case _             => throw new IllegalArgumentException("Not a numeric type: " + x)
    }
  }

  override def accumulate(v: Any, s: Seed, state: LoopState) = {
    val d = toDouble(v)
    aggregator.accumulate(d, s, state)
  }

  override def combine(s1: Seed, s2: Seed, state: LoopState): Seed = aggregator.combine(s1, s2, state)
  def result(s: Seed): Option[Result] = aggregator.result(s)
}

sealed trait AnyAggregation extends SimpleUntypedAggregation {
  type ValueType = Any
}

private[dynamic] final case class AnySumAggregation(sourceColumn: String) extends AnyAggregation {
  val aggregateColumn = "Sum:" + sourceColumn
  val aggregator = new AsDoubleAggregator(Sum.Double)
}

private[dynamic] final case class AnyMinAggregation(sourceColumn: String) extends AnyAggregation {
  val aggregateColumn = "Min:" + sourceColumn
  val aggregator = new AsDoubleAggregator(Min.Double)
}

private[dynamic] final case class AnyMaxAggregation(sourceColumn: String) extends AnyAggregation {
  val aggregateColumn = "Max:" + sourceColumn
  val aggregator = new AsDoubleAggregator(Max.Double)
}

private[dynamic] final case class AnyAvgAggregation(sourceColumn: String) extends AnyAggregation {
  val aggregateColumn = "Avg:" + sourceColumn
  val aggregator = new AsDoubleAggregator(Avg.Double)
}

private[dynamic] final case class AnyDevAggregation(sourceColumn: String) extends AnyAggregation {
  val aggregateColumn = "Dev:" + sourceColumn
  val aggregator = new AsDoubleAggregator(DoubleDevAggregator)
}

private[dynamic] object DoubleDevAggregator extends Aggregator[Double] {
  type Result = Double
  val resultType = TypeInfo.DOUBLE

  def default: Result = throw new RelationalException("Sequence contains no elements")

  type Seed = (Array[Double], Array[Long], ListBuffer[Double])
  def seed: Seed = (Array(0.0), Array(0), ListBuffer.empty)

  def accumulate(v: Double, s: Seed, state: LoopState): Seed = {
    s._1(0) += v
    s._2(0) += 1L
    s._3.+=(v)
    s
  }

  def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
    s1._1(0) += s2._1(0)
    s1._2(0) += s2._2(0)
    s1._3.++=(s2._3)
    s1
  }

  def result(s: Seed): Option[Result] = {
    if (s._2(0) == 0) None
    else {
      val avg = s._1(0) / s._2(0)
      val sum = s._3.foldLeft(0.0)((sum: Double, ele: Double) => sum + Math.pow(ele - avg, 2))
      Some(Math.sqrt(sum / s._2(0)))
    }
  }
}
