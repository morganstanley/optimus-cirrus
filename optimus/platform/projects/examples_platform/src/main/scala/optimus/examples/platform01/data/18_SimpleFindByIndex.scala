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

import optimus.examples.platform.entities.SimpleEvent
import optimus.examples.platform.entities.SimpleIndexedEntity
import optimus.platform._

object SimpleFindByIndex extends AdvancedOptimusApp {
  val user = System.getProperty("user.name")

  override def setup(): Unit = {
    if (cmdLine.uri == null)
      cmdLine.uri = "broker://dev?context=named&context.name=" + user
  }

  @entersGraph override def run(): Unit = {
    DAL.purgePrivateContext()

    val name = user + "#" + "MyEntity"

    println("Persisting 3 enitities...")
    // SimpleIndexedEintity definition looks like:
    // @entity class SimpleIndexedEntity(@key val name: String, @indexed val city: String)
    newEvent(SimpleEvent.uniqueInstance("Put")) {
      val e1 = SimpleIndexedEntity(name + "#1", "London")
      println("Persisting " + e1 + " with city = " + e1.city)
      DAL.put(e1)
      val e2 = SimpleIndexedEntity(name + "#2", "London")
      println("Persisting " + e2 + " with city = " + e2.city)
      DAL.put(e2)
      val e3 = SimpleIndexedEntity(name + "#3", "New York")
      println("Persisting " + e3 + " with city = " + e3.city)
      DAL.put(e3)
    }

    given(validTimeAndTransactionTimeNow) {
      println("Find all entities with city = London using an getByIndex query")
      for (e <- SimpleIndexedEntity.city.find("London"))
        println(e)
    }

    println("Deleting database")
    DAL.purgePrivateContext()
  }
}
