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

object SortBy extends LegacyOptimusApp {

  // This example demonstrates ordering of rows
  val sortedPos = from(Data.getPositions()).sortBy(pos =>
    (
      pos.portfolio, // default direction is "ascending"
      asc(pos.symbol), // specify direction explicitly
      desc(pos.quantity)))

  val result = Query.execute(sortedPos)
  println("Row ordering")
  result.foreach(f => println(f))

  // Sort data on expression
  val sortedPos2 = from(Data.getPositions()).sortBy(pos => (pos.portfolio, pos.symbol.length(), pos.quantity))

  val result2 = Query.execute(sortedPos2)
  println("Row ordering on expression")
  result2.foreach(f => println(f))
}
