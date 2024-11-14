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

object SubQuery extends LegacyOptimusApp {

  /**
   * Example demonstrates use of sub queries which can either be executed immediately or stored along with result data
   * for later execution (when needed)
   */
  val posQ =
    for ((key, positions) <- from(Data.getPositions()).groupBy(p => p.portfolio))
      yield new {
        val portfolio = key
        val numPos = positions.count
        // the following two queries use immediate sub query execution
        val minPos = positions.min(p => (p.quantity, p.symbol))
        val maxPos = positions.max(p => (p.quantity, p.symbol))
        // the following query is not executed, but stored as a result set member for later execution
        val members = positions.sortBy(p => (p.symbol, desc(p.quantity)))
      }

  println("Portfolio  NumPos  minPos maxPos ")
  for (pos <- Query.execute(posQ)) {
    println(pos.portfolio + " | " + pos.numPos + " | " + pos.minPos + " | " + pos.maxPos)
    println("  Members: " + Query.execute(pos.members).toList)
  }

  println("Sub query in filter")
  val portfolios = Set("David", "James")
  val portfolioQ = for (p <- from(portfolios)) yield p
  val subQ = for (p <- from(Data.getPositions()) if Query.execute(portfolioQ).exists(s => s == p.portfolio)) yield p

  for (pos <- Query.execute(subQ)) {
    println(pos)
  }
}
