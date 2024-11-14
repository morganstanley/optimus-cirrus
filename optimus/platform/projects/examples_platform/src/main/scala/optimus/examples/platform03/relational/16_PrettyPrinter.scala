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
import java.io.ByteArrayOutputStream
import optimus.platform.relational.formatter._

object PrettyPrinter extends LegacyOptimusApp {

  final case class Holding(val symbol: String, val quantity: Int) {}

  val holds = for (pos <- from(Data.getPositions())) yield Holding(pos.symbol, pos.quantity)

  // display a common result to standard output console
  Query.display(holds)

  val bout = new ByteArrayOutputStream()

  // display to a output stream
  Query.displayTo(holds, bout)

  // display a group result
  val byPositionsQ = from(Data.getPositions()).groupBy(pv => pv.portfolio)
  Query.display(byPositionsQ)
}
