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

/**
 * Count implementation
 */
trait Count[-T] extends Aggregator[T] {
  type Result = Long
  val resultType = TypeInfo.LONG
  def default: Result = 0L
}

object Count {
  trait AnyCount extends Count[Any] {
    type Seed = Array[Long]
    def seed: Seed = Array(0L)
    def accumulate(v: Any, s: Seed, state: LoopState): Seed = {
      s(0) += 1
      s
    }
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      s1(0) += s2(0)
      s1
    }
    def result(s: Seed): Option[Result] = Some(s(0))
    override def evaluateAll(values: Iterable[Any]): Option[Long] = {
      Some(values.size.toLong)
    }
  }
  object Any extends AnyCount

  trait BooleanCount extends Count[Boolean] {
    type Seed = Array[Long]
    def seed: Seed = Array(0L)
    def accumulate(v: Boolean, s: Seed, state: LoopState): Seed = {
      if (v) s(0) += 1
      s
    }
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      s1(0) += s2(0)
      s1
    }
    def result(s: Seed): Option[Result] = Some(s(0))
    override def evaluateAll(values: Iterable[Boolean]): Option[Long] = {
      Some(values.count(identity).toLong)
    }
  }
  object Boolean extends BooleanCount
}
