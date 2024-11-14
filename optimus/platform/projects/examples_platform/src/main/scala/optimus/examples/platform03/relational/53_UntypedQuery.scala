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

object UntypedQuery extends LegacyOptimusApp {

  // This example demonstrates how we execute untyped query.

  val untypedPos = from(Data.getPositions()).untype

  filterQuery
  sortAndTake
  groupbyAggregation
  joins

  def filterQuery = {
    val simpleFilter = for (p <- from(untypedPos) if p("portfolio") == "James" || p("portfolio") == "Kostya") yield p
    val result = Query.execute(simpleFilter)
    println("\n Simple Filtering\n")
    result.foreach(p => println("" + p("portfolio") + ", " + p("symbol") + "," + p("quantity")))
  }

  def sortAndTake = {
    val sortby = untypedPos.sortBy(p => p("portfolio").asInstanceOf[String]).takeFirst(2)
    val result = Query.execute(sortby)
    println("\n Sort and Take\n")
    result.foreach(p => println("" + p("portfolio") + ", " + p("symbol") + "," + p("quantity")))
  }

  def groupbyAggregation = {
    val groupQ =
      for ((key, group) <- from(untypedPos).groupBy(pv => pv("portfolio")))
        yield {
          val quantitySum = group.sum(p => p("quantity").asInstanceOf[Int])
          new {
            val portfolio = key
            val posTotal = quantitySum
          }
        }
    println("\n Groupby Aggregation: \n")
    Query.execute(groupQ).foreach(p => println("portfolio:" + p.portfolio + ", posTotal:" + p.posTotal))
  }

  def joins = {
    val untypedPrices = from(Data.getPrices()).untype
    val query =
      for ((pos, price) <- from(untypedPos).innerJoin(untypedPrices).on((pos, p) => pos("symbol") == p("symbol")))
        yield PositionValue(
          pos("portfolio").asInstanceOf[String],
          pos("symbol").asInstanceOf[String],
          pos("quantity").asInstanceOf[Int],
          price("price").asInstanceOf[Float],
          price("price").asInstanceOf[Float] * pos("quantity").asInstanceOf[Int]
        )

    println("\n Inner Join:\n ")
    Query.execute(query).foreach(pv => println(pv))

  }
}
