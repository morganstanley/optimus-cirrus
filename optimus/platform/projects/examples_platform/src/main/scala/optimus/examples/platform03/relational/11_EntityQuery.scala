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

import optimus.examples.platform.entities.SimpleEvent
import optimus.platform._
import optimus.platform.relational._

object EntityQuery extends LegacyOptimusApp {

  val eop1 = Employee.getOption("x123")
  val emp1 = eop1 getOrElse { Employee("x123", "David", "Boston") }

  val eop2 = Employee.getOption("y456")
  val emp2 = eop2 getOrElse { Employee("y456", "Kostya", "Dallas") }

  newEvent(SimpleEvent.uniqueInstance("Save")) {
    // save Employee entity instances
    if (eop1.isEmpty) {
      DAL.put(emp1)
    }
    if (eop2.isEmpty) {
      DAL.put(emp2)
    }
  }

  // query Employee entities from Entity Depot
  val employee = for (t <- from(Employee) if t.firstName == "David" && t.lastName == "Boston") yield t

  val result = employee.permitTableScan.execute
  println("Employee entities:")
  result.foreach(p => println("Employee[" + p.employeeID + "," + p.firstName + "," + p.lastName + "]"))

}
