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
package optimus.examples.platform02.graph

import optimus.platform._
import optimus.examples.platform.entities._
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.DalLocation
import optimus.platform.dsi.bitemporal.Context
import optimus.platform.dsi.bitemporal.NamedContext

object TargetIdentity extends OptimusApp {
  protected override def dalLocation: DalLocation = DalEnv.dev
  protected override def dalContext: Context = NamedContext(user)

  private val user = System.getProperty("user.name")
  private val name = user + "#" + "MyEntity"

  @entersGraph override def run(): Unit = {
    // ensure there is a simple entity
    val writeResult = newTransaction {
      newEvent(SimpleEvent.uniqueInstance(name)) {
        DAL.upsert(SimpleEntity(name))
      }
    }

    // entities are targeted by their key + temporal context
    val ttTime = writeResult.tt

    // get some entities in the same temporal context
    val (a, b) = given(transactionTime := ttTime) {
      (SimpleEntity.get(name), SimpleEntity.get(name))
    }

    // get some entities in a different temporal context
    val (c, d) = given(transactionTime := ttTime.plusMillis(1)) {
      (SimpleEntity.get(name), SimpleEntity.get(name))
    }

    // some in memory entities
    val e = SimpleEntity(name)
    val f = SimpleEntity(name)

    val (a1, b1, c1, d1, e1, f1) = given(a.value := 23.0) {
      (a.value, b.value, c.value, d.value, e.value, f.value)
    }
    println(s"Entities with the same id are tweaked: $a1 $b1")
    println(s"Entities with different ids are not: $c1 $d1 $e1 $f1")

    val (a2, b2, c2, d2, e2, f2) = given(d.value := 18.0) {
      (a.value, b.value, c.value, d.value, e.value, f.value)
    }
    println(s"Entities with the same id are tweaked: $c2 $d2")
    println(s"Entities with different ids are not: $a2 $b2 $e2 $f2")

    val (e3, f3) = given(e.value := 12.0) {
      (e.value, f.value)
    }
    println(s"Heap entities all share the same null temporal context: $e3 $f3")
  }
}
