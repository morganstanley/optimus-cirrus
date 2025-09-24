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

object InnerJoin extends LegacyOptimusApp with PrintlnInterceptor {

  final case class Comparison(
      val symbol: String,
      val now: Float,
      val close: Float,
      val delta: Float,
      val deltaPercentage: Float) {}

  // This example demonstrates creating an inner join.
  val moves =
    for (
      (price, close) <- from(Data.getPrices())
        .innerJoin(from(Data.getYesterdaysClose()))
        .on((p, clo) => p.symbol == clo.symbol)
    )
      yield Comparison(
        price.symbol,
        price.price,
        close.price,
        price.price - close.price,
        100 * ((price.price - close.price) / close.price))

  println("Movement")
  Query.execute(moves).foreach(f => println(f))

  // create queries for all the symbols
  val up = for (movement <- moves if movement.delta > 0) yield movement
  val unChanged = for (movement <- moves if movement.delta == 0) yield movement
  val down = for (movement <- moves if movement.delta < 0) yield movement

  println("\nup:")
  Query.execute(up).foreach(f => println(f))

  println("\nPositions and values:")
  Query.execute(getPositionValues(Data.getPositions(), Data.getPrices())).foreach(f => println(f))

  def getPositionValues(positions: Set[Position], prices: Set[Price]): Query[PositionValue] = {
    val priceSrc = from(prices)
    val query =
      for ((pos, price) <- from(positions).innerJoin(priceSrc).on((pos, p) => pos.symbol == p.symbol))
        yield PositionValue(pos.portfolio, pos.symbol, pos.quantity, price.price, price.price * pos.quantity)
    query
  }

  // The below example shows Join with automatic keys: because both relations have DimensionKeys, innerJoin will use the intersection of the fields in those keys
  val left = from(Data.getPositions())
  val right = from(Data.getPrices())
  val joins =
    for ((pos, price) <- left.innerJoin(right))
      yield PositionValue(pos.portfolio, pos.symbol, pos.quantity, price.price, price.price * pos.quantity)

  println("\nJoin with key: " + joins.element.key)
  Query.execute(joins).foreach(f => println(f))

}
