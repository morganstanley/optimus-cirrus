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
import optimus.platform._
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.DalLocation

object FunctionOverrideTweak extends OptimusApp {
  protected override def dalLocation: DalLocation = DalEnv("none")

  @entersGraph override def run(): Unit = {
    // entity properties can be tweaked, this example shows tweaking of in memory entities for simplicity.
    val s = SimpleEntity("My simple entity")
    println("f function just multiplies the values" + s.f(2, 3))

    given(SimpleEntity.f := { (se, i, j) => se.f(i, j) * 2 }) {
      println("Within a given block f values are doubled:" + s.f(2, 3))
    }
  }
}
