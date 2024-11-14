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

import optimus.examples.platform.entities.SimpleEntity
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.DalLocation
import optimus.platform.util.Log
import optimus.platform._

object BasicTweaks extends OptimusApp with Log {
  override def dalLocation: DalLocation = DalEnv("none")

  @entersGraph override def run(): Unit = {
    // entity properties can be tweaked, this example shows tweaking of in memory
    // entities for simplicity.
    val s = SimpleEntity("My simple entity")
    println(s"The entity s has a field value initialised to be: ${s.value}")

    println(s"Within a given block the entity's values are changed, s.value is: ${given(s.value := 10) {
        s.value
      }}")

    println(s"Entity tweaks only survive the scope of the given block, s.value is: ${s.value}")
  }

}
