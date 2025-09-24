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

import optimus.examples.platform.entities.SimpleCalcs
import optimus.examples.testinfra.PrintlnInterceptor
import optimus.platform._
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.DalLocation

object AdvancedTweaks extends OptimusApp with PrintlnInterceptor {
  override def dalLocation: DalLocation = DalEnv.none

  @entersGraph override def run(): Unit = {
    val s = SimpleCalcs("id", 3, 4)

    println(s"Initial: ${s.b(10)}")

    // tweaking of an instance method
    println(s"Tweaked by instance: ${given(s.a := 12) { s.b(10) }}")

    // tweaking all nodes of a type + relative tweaks
    // here all a fields are multiplied by 2
    println(s"Tweaked by class: ${given(SimpleCalcs.a :*= 2) { s.b(10) }}")

    // tweaking properties at specific values
    println(s"Tweaked by parameters: ${given(s.b(10) := 3) { s.b(10) }}")

    // tweaking under conditions
    println(s"Tweaked via when: ${given(SimpleCalcs.x when { s => s.y < 10 } := 5) { s.b(10) }}")

    // we can tweak all values when there are inputs
    println(s"Function tweak: ${given(SimpleCalcs.b :*= 0.95) { s.b(10) }}")

    val d = DerivedCalcs()
    println(s"A derived class: ${d.a}")

    // inheritance hierarchies are honoured by the tweak targets.
    println(s"Overriding base class dependents: ${given(SimpleCalcs.x :*= 2) { d.a }}")
    println(s"Overriding method via baseclass: ${given(SimpleCalcs.a := 2) { d.a }}")

  }
}

@entity
class DerivedCalcs(id: String = "id", xInit: Int = 3, yInit: Int = 4) extends SimpleCalcs(id, xInit, yInit) {
  @node(tweak = true) override def a: Int = y * 2 + x
}
