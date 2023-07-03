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
package optimus.platform.relational.accumulators.untyped

import optimus.platform.relational.aggregation._

final case class SumUnarySeqDoubleAccumulatorBuilder(
    sourceColumnName: String,
    aggregateColumnName: String,
    aggregator: Sum[Seq[Double]]
) extends UntypedAccumulatorBuilder {
  def accumulator = SumUnarySeqDoubleAccumulator(sourceColumnName, aggregateColumnName, aggregator)
}

final case class SumUnarySeqDoubleAccumulator(
    sourceColumnName: String,
    aggregateColumnName: String,
    aggregator: Sum[Seq[Double]]
) extends StatefulAccumulator {
  type SourceColumnType = Seq[Double]
  type AggregateColumnType = Seq[Double]

  private val sum = aggregator.seed
  def accumulate(x: Seq[Double]): Unit = aggregator.accumulate(x, sum, LoopState.Ignore)
  def result: Seq[Double] = aggregator.result(sum).getOrElse(Seq.empty)
}

final case class UntypedSumColumnAccumulatorBuilder[ItemType: Numeric: Manifest](
    sourceColumnName: String,
    aggregateColumnName: String)
    extends UntypedAccumulatorBuilder {
  def accumulator = SumColumnAccumulator(sourceColumnName, aggregateColumnName)
}

final case class UntypedAvgColumnAccumulatorBuilder[ItemType: Numeric: Manifest](
    sourceColumnName: String,
    aggregateColumnName: String)
    extends UntypedAccumulatorBuilder {
  def accumulator = AvgColumnAccumulator(sourceColumnName, aggregateColumnName)
}

final case class SumColumnAccumulator[ItemType](sourceColumnName: String, aggregateColumnName: String)(implicit
    num: Numeric[ItemType],
    cm: Manifest[ItemType])
    extends StatefulAccumulator {
  private var sum = num.zero

  type SourceColumnType = ItemType
  type AggregateColumnType = ItemType

  override def accumulate(a: ItemType): Unit = {
    sum = num.plus(sum, a)
  }

  override def result: ItemType = sum
}

final case class AvgColumnAccumulator[ItemType](sourceColumnName: String, aggregateColumnName: String)(implicit
    num: Numeric[ItemType],
    cm: Manifest[ItemType])
    extends StatefulAccumulator {
  private var sum = num.zero
  private var count = 0L

  type SourceColumnType = ItemType
  type AggregateColumnType = Double

  override def accumulate(a: ItemType): Unit = {
    sum = num.plus(sum, a)
    count += 1
  }

  override def result: Double = {
    if (count > 0) num.toDouble(sum) / count
    else 0
  }
}
