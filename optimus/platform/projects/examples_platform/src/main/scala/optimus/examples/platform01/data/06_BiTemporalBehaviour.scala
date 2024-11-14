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

import optimus.examples.platform.entities.SimpleEntity
import optimus.platform._
import optimus.examples.platform.entities.SimpleEvent

// It is advisable to consult the optimus user manual for an explanation of bi-temporal
// if you are unfamiliar with bi-temporal a picture model tends to be best to begin with
// http://optimusdoc/
object BiTemporalBehaviour extends LegacyOptimusApp[SupplyEntityNameArgs] {
  override def setup(): Unit = {
    if (cmdLine.uri == null)
      cmdLine.uri = "broker://dev?context=named&context.name=" + System.getProperty("user.name")
  }

  // Purge the user database
  DAL.purgePrivateContext()

  // applications start with a valid time and a read transaction time
  println("Valid time: " + validTime + " read transaction time: " + transactionTime)

  val simpleEntity = SimpleEntity(cmdLine.name)

  // the persist block will set the transaction time of the write to
  // be equal to the clock time of the data store.
  newEvent(SimpleEvent.uniqueInstance("Create")) {
    DAL.put(simpleEntity)
  }

  // because the read transaction time is unchanged and before the
  // write transaction time - this call returns empty
  val fetchEntity = SimpleEntity.getOption(cmdLine.name)
  println("The read transaction time is before: " + fetchEntity)

  val snapshotTime = At.now

  // In order to 'see' the entity the read transaction time must be after the write time
  val correctFetch = given(transactionTime := snapshotTime) { SimpleEntity.get(cmdLine.name) }
  println("Correctly fetched entity: " + correctFetch)

  // optimus provides a utility to move valid and transaction times on
  given(validTimeAndTransactionTimeNow) {
    val anotherFetch = SimpleEntity.get(cmdLine.name)
    println("Valid time: " + validTime + " read transaction time: " + transactionTime)
    println("Entity: " + anotherFetch)

    // clean up
    newEvent(SimpleEvent.uniqueInstance("Invalidate")) {
      DAL.invalidate(anotherFetch)
    }

    // however the delete happens after the advanceTimeToNow - as a result this operation
    // successfully retrieves the entity again
    println("Entity: " + SimpleEntity.get(cmdLine.name))
  }

  given(validTimeAndTransactionTimeNow) {
    // now removal is visible
    val removedEntity = SimpleEntity.getOption(cmdLine.name)
    println("Now the removal is visible: " + removedEntity)
  }

  // its still possible to 'go back in time' and see the old entity:
  val gobackAndFetch = given(transactionTime := snapshotTime) { SimpleEntity.get(cmdLine.name) }
  println("Past entity: " + gobackAndFetch)
}
