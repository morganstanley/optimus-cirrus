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

import optimus.examples.platform.entities._

// This example demonstrates the use of the DAL debug interface

object DALDebugTools extends AdvancedOptimusApp {
  val user = System.getProperty("user.name")

  override def setup(): Unit = {
    if (cmdLine.uri == null)
      cmdLine.uri = "broker://dev?context=named&context.name=" + user
  }

  @stored @entity class SimpleIndexedEntity(name: String, @indexed val city: String, @node(tweak = true) val x: Int)
      extends SimpleEntity(name)

  @entersGraph override def run(): Unit = {
    DAL.purgePrivateContext()

    val name = user + "#" + "MyEntity"
    val e = SimpleIndexedEntity(name, "London", 0)
    newEvent(SimpleEvent.uniqueInstance("Put")) {
      DAL.put(e)
    }

    val tc1 = DAL.getTemporalCoordinates(e)
    println(tc1)

    given(validTimeAndTransactionTimeNow) {
      newEvent(SimpleEvent.uniqueInstance("Modify")) {
        DAL.modify(e)(x = 1)
      }
    }

    given(validTimeAndTransactionTimeNow) {
      // Get all versions
      val cnt = DAL.debug.getAllByRef(e.dal$entityRef)
      cnt foreach { println(_) }

      // get a specific version
      val pe = given(transactionTime := tc1.txTime, validTime := tc1.validTime.from) {
        DAL.debug.getByRef(e.dal$entityRef)
      }

      println(pe)
    }

    println("Done.")

    DAL.purgePrivateContext()

  }
}
