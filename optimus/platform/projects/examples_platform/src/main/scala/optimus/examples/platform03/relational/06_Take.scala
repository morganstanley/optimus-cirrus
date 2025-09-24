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

object Take extends LegacyOptimusApp with PrintlnInterceptor {

  // This example demonstrates 'take', 'takeFirst', 'takeRight'
  // and 'takeDistinct' methods

  val posQ = from(Data.getPositions()).sortBy(p => (desc(p.quantity), p.symbol))

  val highPosQ = posQ.takeFirst(1)
  println("Highest position" + Query.execute(highPosQ).toList)

  val lowPosQ = posQ.takeRight(1)
  println("Lowest position" + Query.execute(lowPosQ).toList)

  val top5PosQ = posQ.take(0, 5) // note that the 1st row has zero index
  println("Top 5 position" + Query.execute(top5PosQ).toList)

  val portQ = for (p <- from(Data.getPositions())) yield p.portfolio
  val uniquePort = portQ.distinct.sortBy(x => x)

  println("Unique portfolios" + Query.execute(uniquePort).toList)

}
