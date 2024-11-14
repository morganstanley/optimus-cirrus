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
package optimus.examples.platform03.relational

import optimus.platform.LegacyOptimusApp
import optimus.platform._
import optimus.platform.relational._

trait ValueCoordinates {
  def value: Int
}

object ValueCoordinates {
  def apply(sumValue: Int): ValueCoordinates = new ValueCoordinates {
    def value: Int = sumValue
  }
}

trait ObservableCoordinates {
  def observable: String
}

trait TenorCoordinates {
  def tenor: String
}

abstract class SymbolTrade(val symbol: String)

class SummedValueCoordinates(val value: Int)

class UnitCoordinates(val unit: String, val value: Int, val observable: String, val tenor: String)
    extends SymbolTrade(observable)
    with ValueCoordinates
    with ObservableCoordinates
    with TenorCoordinates

object AggregatedBy extends LegacyOptimusApp {

  val original = from(
    List(
      new UnitCoordinates("ap", 12, "ms", "usid"),
      new UnitCoordinates("ap", 10, "ms", "usid"),
      new UnitCoordinates("ap", 12, "fb", "usid"),
      new UnitCoordinates("ap", 8, "fb", "usid")
    ))
  val q1 = original.aggregateBy[ObservableCoordinates, ValueCoordinates, SummedValueCoordinates](f =>
    new SummedValueCoordinates(f.sum(_.value)))
  val res1 = Query.execute(q1)
  res1.foreach(f => println(f.observable + " -> " + f.value))

  val q2 = original.aggregateBy[ObservableCoordinates, ValueCoordinates, ValueCoordinates](f => {
    val sumValue = f.sum(_.value)
    ValueCoordinates(sumValue)
  })
  val res2 = Query.execute(q2)
  res2.foreach(f => println(f.observable + " -> " + f.value))
}
