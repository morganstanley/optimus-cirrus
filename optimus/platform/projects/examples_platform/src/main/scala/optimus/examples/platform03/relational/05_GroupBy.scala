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

import optimus.platform._
import optimus.platform.relational._

object GroupBy extends LegacyOptimusApp {

  val byPositionsQ = from(Data.getPositions()).groupBy(pv => pv.portfolio)

  // Similar to Scala Collection's groupBy API, Relational groupBy will return a Map.
  println("Groupby:")
  //    Query.display(Query.execute(byPositionsQ))
  val byPositionsMap: Map[String, Iterable[Position]] = Query.execute(byPositionsQ)
  // println(byPositionsMap)
  printResult(byPositionsMap)

  // Groupby Aggregation.
  val groupQ =
    for ((key, group) <- from(Data.getPositions()).groupBy(pv => pv.portfolio))
      yield new {
        val portfolio = key
        val posTotal = group.map(p => p.quantity).sum
      }
  println("Groupby Aggregation: ")
  //    Query.display(Query.execute(groupQ))
  Query.execute(groupQ).foreach(p => println("portfolio:" + p.portfolio + ", posTotal:" + p.posTotal))

  // groupby multiple fields
  val byPositionsMultiQ = from(Data.getPositions()).groupBy(pv => (pv.portfolio, pv.symbol))
  println("Groupby multiple keys: ")
  //    Query.display(Query.execute(byPositionsMultiQ))
  // println(Query.execute(byPositionsMultiQ))
  printResult(Query.execute(byPositionsMultiQ))

  def printResult[RowType](result: Map[RowType, Iterable[Position]]): Unit = {
    result.foreach { case (key, value) =>
      println(s"  $key -> $value")
    }
  }
}
