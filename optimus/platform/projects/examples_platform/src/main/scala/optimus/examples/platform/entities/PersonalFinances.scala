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
package optimus.examples.platform.entities

import optimus.platform._
import scala.collection.immutable._

@event class UpdateFinancial()

@stored @entity class HolidayCurrency(@key val name: String, @node val rate: Double)

@stored @entity class Holiday(@node val localCurrency: String, @node val localCost: Double) {
  @node def cost = {
    val c = HolidayCurrency.get(localCurrency)
    localCost * c.rate
  }
}

@stored @entity class FinancialYear(
    @node val payRise: Double,
    @node val interestRate: Double,
    @node val inflation: Double,
    @node val holiday: Holiday)

class OhNoException(message: String) extends Exception(message)

@stored @entity class MyFinances(
    @key val name: String,
    @node val salary: Double = 0,
    @node val mortgage: Double = 0,
    @node val costOfLiving: Double = 0,
    @node val marketData: List[FinancialYear] = List.empty) {
  @entersGraph def salary$init(): Double = 0
  @entersGraph def mortgage$init(): Double = 0
  @entersGraph def costOfLiving$init(): Double = 0
  @entersGraph def marketData$init(): List[FinancialYear] = List.empty
  @node private def recYearsUntilMortgagePaidOff(
      salary: Double,
      mortgage: Double,
      costOfLiving: Double,
      marketData: List[FinancialYear],
      counter: Int = 1): Int = {
    if (marketData.isEmpty) throw new OhNoException("No market data about future financial years")
    val newSalary = salary * (1 + marketData.head.payRise)
    val newCostOfLiving = costOfLiving * (1 + marketData.head.inflation)
    val newMortgage = (mortgage * (1 + marketData.head.interestRate)) - newSalary +
      newCostOfLiving + marketData.head.holiday.cost
    if (newMortgage < 0) counter
    else recYearsUntilMortgagePaidOff(newSalary, newMortgage, newCostOfLiving, marketData.tail, counter + 1)
  }

  @node def yearsUntilMortgagePaidOff: Int = {
    recYearsUntilMortgagePaidOff(salary, mortgage, costOfLiving, marketData)
  }
}

object MyFinanceUtils extends LegacyOptimusApp {
  write()

  @entersGraph def write(): Unit = {
    val dollar = HolidayCurrency.getOption("USD") getOrElse HolidayCurrency("USD", 1.6)

    val euro = HolidayCurrency.getOption("EUR") getOrElse HolidayCurrency("EUR", 1.2)

    val bob = MyFinances.getOption("bob") getOrElse MyFinances("bob")

    val usa = FinancialYear(0.03, 0.02, 0.03, Holiday("USD", 2000))
    val italy = FinancialYear(0.03, 0.02, 0.03, Holiday("EUR", 1000))

    // bob.salary := 20000
    // bob.mortgage := 200000
    // bob.costOfLiving := 4000

    newEvent(UpdateFinancial.uniqueInstance()) {
      DAL.put(dollar)
      DAL.put(euro)
      DAL.put(usa.holiday)
      DAL.put(italy.holiday)
      DAL.put(usa)
      DAL.put(italy)
      DAL.modify(bob)(
        salary = 20000,
        mortgage = 200000,
        costOfLiving = 4000,
        marketData = List(
          usa,
          italy,
          usa,
          italy,
          usa,
          italy,
          usa,
          italy,
          usa,
          italy,
          usa,
          italy,
          usa,
          italy,
          usa,
          italy,
          usa,
          italy,
          usa,
          italy)
      )
    }
  }
}
