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

object StructuralTypeQuery extends LegacyOptimusApp {

  // This example demonstrates writing a query function with structural type
  val posQuery = for (p <- from(Data.getPositions())) yield p
  val pos = findPortfolio(posQuery)
  println("Find all Positions " + Query.execute(pos))

  val bondQuery = for (b <- from(Data.getBonds())) yield b
  val bonds = findPortfolio(bondQuery)
  println("Find all Bonds " + Query.execute(bonds))

  def findPortfolio[T <: { def portfolio: String }](q: Query[T]) = {
    q.filter(t => t.portfolio == "David")
  }

}
