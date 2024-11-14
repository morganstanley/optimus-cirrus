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

object OuterJoin extends LegacyOptimusApp {

  // This example demonstrates left, right and full outer  joins
  leftJoin()
  rightJoin()
  fullOuterJoin()
  leftJoinWithDefaultValue()

  def leftJoin() = {
    val portfolioSrc = from(Data.getPortfolio())
    val positionSrc = from(Data.getPositions())

    val portfolioPositionsL =
      for ((port, pos) <- portfolioSrc.leftOuterJoin(positionSrc).on((port, pos) => port.id == pos.portfolio))
        yield new {
          // In leftOuterJoin, the right side might be null, so we use 'if' to check if the right value is null, and give a default value.
          private val position = if (pos != null) pos else Position("", "-", 0)

          val portfolio = port.id
          val client = port.clientName
          val symbol = position.symbol
          val quantity = position.quantity
        }
    // this is the actual execution of the query
    val resultL = Query.execute(portfolioPositionsL)
    println("\nPortfolio positions (Left Join)")
    resultL.foreach(p =>
      println(
        "portfolio=" + p.portfolio + " ,client=" + p.client + " ,symbol=" + p.symbol + " ,quantity=" + p.quantity))

  }

  def leftJoinWithDefaultValue() = {
    val portfolioSrc = from(Data.getPortfolio())
    val positionSrc = from(Data.getPositions())
    val portfolioPositionsL =
      for (
        (port, pos) <- portfolioSrc
          .leftOuterJoin(positionSrc)
          .on((port, pos) => port.id == pos.portfolio)
          .withRightDefault((t: Portfolio) => Position(t.id, t.clientName, 0))
      )
        yield new {

          val portfolio = port.id
          val client = port.clientName
          val symbol = pos.symbol
          val quantity = pos.quantity
        }
    val resultL = Query.execute(portfolioPositionsL)
    println("\nPortfolio positions (Left Join with default value)")
    resultL.foreach(p =>
      println(
        "portfolio=" + p.portfolio + " ,client=" + p.client + " ,symbol=" + p.symbol + " ,quantity=" + p.quantity))
  }

  def rightJoin() = {
    val portfolioSrc = from(Data.getPortfolio());
    val positionSrc = from(Data.getPositions())

    val portfolioPositionsR =
      for ((pos, port) <- positionSrc.rightOuterJoin(portfolioSrc).on((pos, port) => pos.portfolio == port.id))
        yield new {
          // In rightOuterJoin, the left side might be null, so we use 'if' to check null, and give a default value.
          private val position = if (pos != null) pos else Position("", "-", 0);

          val portfolio = port.id;
          val client = port.clientName;
          val symbol = position.symbol;
          val quantity = position.quantity
        }

    // this is the actual execution of the query
    val resultR = Query.execute(portfolioPositionsR);
    println("\nPortfolio positions (Right Join)")
    resultR.foreach(p =>
      println(
        "portfolio=" + p.portfolio + " ,client=" + p.client + " ,symbol=" + p.symbol + " ,quantity=" + p.quantity))

  }

  def fullOuterJoin() = {
    val yesterdayPosSrc = from(Data.getYesterdayPositions())
    val positionSrc = from(Data.getPositions())

    val posMoveQ =
      for (
        (yPos, tPos) <- yesterdayPosSrc
          .fullOuterJoin(positionSrc)
          .on((y, t) => y.portfolio == t.portfolio && y.symbol == t.symbol)
      )
        yield new {
          // In fullOuterJoin, both left side and right side could be null, so we use 'if' to check null, and give a default value.
          private val tmpTPos = if (tPos != null) tPos else Position(yPos.portfolio, yPos.symbol, 0);
          private val yPosQuantity = if (yPos != null) yPos.quantity else 0;

          val portfolio = tmpTPos.portfolio;
          val symbol = tmpTPos.symbol;
          val prevQuantity = yPosQuantity;
          val quantityMove = tmpTPos.quantity - yPosQuantity
        }

    val posDiffQ = for (posMove <- posMoveQ if posMove.quantityMove != 0) yield posMove

    val posDiffSortQ = posDiffQ.sortBy(pos => desc(pos.quantityMove))

    Query
      .execute(posDiffSortQ)
      .foreach(p =>
        println(
          "posDiff [portfolio=" + p.portfolio + ", symbol=" + p.symbol +
            ", prevQuantity=" + p.prevQuantity + ", quantityMove=" + p.quantityMove + "]"))

  }
}
