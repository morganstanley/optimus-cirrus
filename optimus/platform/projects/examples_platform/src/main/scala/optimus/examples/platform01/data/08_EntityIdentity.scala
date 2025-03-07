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
import optimus.examples.platform.entities.SimpleEntity
import optimus.examples.platform.entities.SimpleEvent

object EntityIdentity extends OptimusApp {
  val user = System.getProperty("user.name")

  override def setup(): Unit = {
    if (cmdLine.uri == null)
      cmdLine.uri = "broker://dev?context=named&context.name=" + user
  }

  // helper function for printing
  def compare(a: AnyRef, b: AnyRef) = (a == b).toString

  @entersGraph override def run(): Unit = {
    val name = user + "#" + "MyEntity"

    val heap1 = SimpleEntity(name)
    val heap2 = SimpleEntity(name)
    given(validTimeAndTransactionTimeNow) { // makes no difference to heap creation
      val heap3 = SimpleEntity(name)

      // heap entities
      println(
        "Heap entities are equal only to themselves: " +
          compare(heap1, heap1) + " " +
          compare(heap2, heap2) + " " +
          compare(heap3, heap3))
      println(
        "Optimus never considers different heap entities equal: " +
          compare(heap1, heap2) + " " +
          compare(heap2, heap3) + " " +
          compare(heap3, heap1))
    }

    // stored entities
    DAL.purgePrivateContext()
    newEvent(SimpleEvent.uniqueInstance("Create")) {
      DAL.put(SimpleEntity(name))
    }

    given(validTimeAndTransactionTimeNow) {
      val storedEntity1 = SimpleEntity.get(name)
      val storedEntity2 = SimpleEntity.get(name)
      given(validTimeAndTransactionTimeNow) {
        val storedEntity3 = SimpleEntity.get(name)
        println(
          "Stored entities with the same key and temporal context are equal: " +
            compare(storedEntity1, storedEntity2))
        println(
          "Different temporal contexts break identity: " +
            compare(storedEntity2, storedEntity3))
        println(
          "Heap entities are never equal to stored entities: " +
            compare(storedEntity1, heap1))
      }
    }
    DAL.purgePrivateContext()
  }
}
