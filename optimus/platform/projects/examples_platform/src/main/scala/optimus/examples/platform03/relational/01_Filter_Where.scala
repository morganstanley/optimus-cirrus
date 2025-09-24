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

import optimus.examples.testinfra.PrintlnInterceptor
import optimus.platform._
import optimus.platform.relational._

object FilterWhere extends LegacyOptimusApp with PrintlnInterceptor {

  // This example demonstrates simple filter.
  val simpleFilter = for (p <- from(Data.getPositions()) if p.portfolio == "James" || p.portfolio == "Kostya") yield p
  // this is the actual execution of the query
  val result = Query.execute(simpleFilter)
  println("Simple Filtering 1\n")
  result.foreach(p => println(p))

  // creates a new query based on query q (no execution takes place).
  val s = bigPositions(simpleFilter)
  // shows the result of the combined query.
  println("\nCombined Filtering 1\n")
  Query.execute(s).foreach(p => println(p))

  /**
   * Creates new Position query by filtering supplied query for Positions with quantity greater than 1000
   */
  def bigPositions(query: Query[Position]) = query.filter(pos => pos.quantity > 1000)

}
