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
package optimus.examples.platform01.data

import optimus.platform._
import optimus.platform.relational._
import optimus.examples.platform.entities.SimpleEvent

@stored @entity
class Trade(@key val id: String, val symbol: String, val amount: Int, val cusip: Int) {}

@stored @entity
class TradeOperation(@key val trade_id: Int, val query: Query[Trade]) {}

object DALSerialization extends LegacyOptimusApp {

  val tr1 = Trade.getOption("x123")
  val tr11 = tr1 getOrElse { Trade("x123", "David", 500, 3) }

  val tr2 = Trade.getOption("y456")
  val tr22 = tr2 getOrElse { Trade("y456", "Kostya", 400, 4) }

  newEvent(SimpleEvent.uniqueInstance("SaveEmployee")) {
    // save Employee entity instances
    if (tr1.isEmpty) {
      DAL.put(tr22)
    }
    if (tr2.isEmpty) {
      DAL.put(tr22)
    }
  }

  saveToDAL()
  loadFromDAL()

  def saveToDAL() = {
    val query = from(Trade) filter (t => t.amount > 400)
    val tradeOperation = TradeOperation(1, query)
    newEvent(SimpleEvent.uniqueInstance("SaveTradeOps")) {
      DAL.put(tradeOperation)
    }
    println("Success save to DAL")
  }

  def loadFromDAL() = {
    val tradeOperation = TradeOperation.get(1)
    println("id: " + tradeOperation.trade_id + " query: " + tradeOperation.query)
  }

}
