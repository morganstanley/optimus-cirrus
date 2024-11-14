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

object IterateResult extends LegacyOptimusApp {

  //// This example use of query result.
  val portfolios = List("James", "Kostya", "David")
  val positions =
    for (
      pos <- from(Data.getPositions())
      if pos.portfolio == "James" || pos.portfolio == "Kostya" || pos.portfolio == "David"
    ) yield pos

  for (row <- Query.execute(positions)) {
    println(row)
  }

}
