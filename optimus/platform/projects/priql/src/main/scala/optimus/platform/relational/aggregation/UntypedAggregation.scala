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

import optimus.platform._
import optimus.platform.relational.aggregation.Sum.SeqDoubleSum
import optimus.platform.relational.tree._

trait UntypedAggregation {
  type ValueType

  val aggregateColumn: String
  val aggregator: Aggregator[ValueType]

  def read(d: DynamicObject): Option[ValueType]
}

trait SimpleUntypedAggregation extends UntypedAggregation {
  val sourceColumn: String

  override def read(d: DynamicObject): Option[ValueType] = {
    if (d.contains(sourceColumn))
      Some(d.get(sourceColumn).asInstanceOf[ValueType])
    else
      None
  }
}

trait Tuple2UntypedAggregation extends UntypedAggregation {
  val sourceColumns: (String, String)

  override def read(d: DynamicObject): Option[ValueType] = {
    if (d.contains(sourceColumns._1) && d.contains(sourceColumns._2))
      Some((d.get(sourceColumns._1), d.get(sourceColumns._2)).asInstanceOf[ValueType])
    else
      None
  }
}

final case class UntypedSum[T: Sum](sourceColumn: String, aggregateColumn: String) extends SimpleUntypedAggregation {
  type ValueType = T
  val aggregator: Sum[T] = implicitly[Sum[ValueType]]
}

final case class UntypedMax[T: Max](sourceColumn: String, aggregateColumn: String) extends SimpleUntypedAggregation {
  type ValueType = T
  val aggregator: Max[T] = implicitly[Max[ValueType]]
}

final case class UntypedAvg[T: Avg](sourceColumn: String, aggregateColumn: String) extends SimpleUntypedAggregation {
  type ValueType = T
  val aggregator: Avg[T] = implicitly[Avg[ValueType]]
}

final case class UntypedStringConcatenation(sourceColumn: String, aggregateColumn: String, sep: String)
    extends SimpleUntypedAggregation {
  type ValueType = String
  val aggregator: Aggregator[String] = Aggregator.mkString(sep)
}

final case class UntypedSumSeqDouble(sourceColumn: String, aggregateColumn: String) extends SimpleUntypedAggregation {
  type ValueType = Seq[Double]
  val aggregator: Sum[Seq[Double]] = new SeqDoubleSum
}

final case class SumUnarySeqDouble(sourceColumn: String, aggregateColumn: String) extends SimpleUntypedAggregation {
  type ValueType = Seq[Double]
  val aggregator: Sum[Seq[Double]] = new Sum[Seq[Double]] {
    type Seed = (Array[Double], Array[Boolean])
    def seed: Seed = (Array(0d), Array(false))
    def accumulate(v: Seq[Double], s: Seed, state: LoopState): Seed = {
      v match {
        case Seq(v1, v2) =>
          s._1(0) += v1 + v2
          s._2(0) = true
        case Seq(v) =>
          s._1(0) += v
          s._2(0) = true
        case Nil =>
        case _   => throw new IllegalArgumentException(s"Expect sequence of length 2, 1, or 0 but found $v")
      }
      s
    }
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      s1._1(0) += s2._1(0)
      s1._2(0) |= s2._2(0)
      s1
    }
    def result(s: Seed): Option[Seq[Double]] = if (s._2(0)) Some(Seq(s._1(0))) else None
    override def default: Seq[Double] = Seq()
    val resultType: TypeInfo[Seq[Double]] = new TypeInfo[Seq[Double]](Seq(classOf[Seq[Double]]), Nil, Nil, Nil)
  }
}
