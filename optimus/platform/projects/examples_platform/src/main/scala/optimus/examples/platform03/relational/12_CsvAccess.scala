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
import optimus.platform.relational.formatter._
import java.io.File
import optimus.platform.relational.csv.CsvSource

object CsvAccess extends LegacyOptimusApp {

  // CSV string, it could be a file
  val csv = """ employeeID,firstName,lastName 
    "x123", "David", "Boston"
    "y456", "Kostya", "Dallas"
    "z789", "James", "Miami"
    """

  // Define CSV source which loads rows from CSV and maps to Employee entity.
  val csvSource = new CsvSource(Employee, csv, true)
  val q = for (c <- from(csvSource) if c.firstName == "David" || c.firstName == "Kostya") yield c
  val result = Query.execute(q)

  println("CSV Query:")
  result.foreach(p => println("Employee[" + p.employeeID + "," + p.firstName + "," + p.lastName + "]"))

  val file = java.io.File.createTempFile("query", ".csv")
  file.deleteOnExit()
  // Write Query's result to csv file. The query should return an Iterable of Entity
  val out = new java.io.FileOutputStream(file)
  Csv.format(q, out)
  out.close()
  file.deleteOnExit()
}
