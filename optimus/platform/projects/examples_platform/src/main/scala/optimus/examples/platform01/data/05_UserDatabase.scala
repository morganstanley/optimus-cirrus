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

// This example shows how the user can create their own sandboxed bit of datastore
// this is useful when testing and experimenting.
object UserDatabase extends AdvancedOptimusApp {
  val user = System.getProperty("user.name")

  override def setup(): Unit = {
    if (cmdLine.uri == null)
      cmdLine.uri = "broker://dev?context=named&context.name=" + user
  }

  @entersGraph override def run(): Unit = {
    DAL.purgePrivateContext()

    val name = user + "#" + "MyEntity"
    newEvent(SimpleEvent.uniqueInstance("UserDatabase")) {
      DAL.put(SimpleEntity(name))
    }

    given(validTimeAndTransactionTimeNow) {
      val eExists = SimpleEntity.get(name)
      println("got entity: " + eExists)
    }
    DAL.purgePrivateContext()

    // Sometimes we get the same vt and tt in the 2 times validTimeAndTransactionTimeNow call. So although we have cleaned up the database using DAL.purgeDatabase(),
    // when calling val eGone = SimpleEntity.getOption(name) we directly get the cached entity since vt, tt is the same.
    // This will fail the test. Adding some waiting time here to let the test pass.
    Thread sleep 100

    given(validTimeAndTransactionTimeNow) {
      val eGone = SimpleEntity.getOption(name)
      assert(eGone == None)
    }
    println("got none, after database purge")
  }
}
