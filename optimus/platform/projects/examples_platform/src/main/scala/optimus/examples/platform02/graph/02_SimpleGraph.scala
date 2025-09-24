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
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.DalLocation
import optimus.platform._

object SimpleGraph extends OptimusApp with PrintlnInterceptor {
  override def dalLocation: DalLocation = DalEnv.none

  @entersGraph override def run(): Unit = {
    val s = SimpleCalcs("id", 3, 4)
    println(s"s initial values: ${s.x} ${s.y}")
    println("calc some values for s")
    for (x <- 0 to 2) println(s"b($x) is: ${s.b(x)}")

    // at this point the graph has a cache of values
    // calling s.b(1) for example will result in no further calculation.

    println("apply a tweak, the values change: ")
    given(s.a := 12) { for (x <- 0 to 2) println(s"given(s.a := 12) { b($x) } is: ${s.b(x)}") }
    // this shows the dependents of a are recalculated just like in a graph.

    println("outside the given block they are the same again")
    // in this case the cached values will be used, no recalc takes place.
    for (x <- 0 to 2) println(s"Outside b($x) is: ${s.b(x)}")
  }
}
