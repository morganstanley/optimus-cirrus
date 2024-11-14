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

@entity object AsyncPartialFunctionNodes {

  @node def default = 42
  @node def big = default * 10

  @node def asyncPartial(des: String)(pf: OptimusPartialFunction[Int, Int]) = {
    val res = (99 to 101).apar map { x =>
      if (pf.isDefinedAt(x)) pf(x) else big /*pf.lift(x).getOrElse(big)*/
    } // the lift retruns OptAsync which have async operators

    println(des)
    res foreach println
    println()
  }

  val pf1 = asNodePF[Int, Int] { case i: Int if i < 100 => default }
  val pf2 = asNodePF[Int, Int] { case i: Int if i > 100 => big }

  //  val pfAndThen = pf1 andThen asNode { x => println(s"inside pfAndThen ${x}"); x }
  //  val pfOrElse = pf1 orElse pf2
  //  val pfRunWith = pf1 runWith asNode { x => println(s"inside pfRunWith ${x}") }

  val cantAsyncPF = asNodePF[Int, Int] { case i: Int if i > default => big } // we should have compile warning here

  big_info.cacheable = false
}

object AsyncPartialFunction extends LegacyOptimusApp {
  import AsyncPartialFunctionNodes._

  asyncPartial("asyncPartial pf1: ")(pf1)
  asyncPartial("asyncPartial pf2: ")(pf2)
  //  asyncPartial("asyncPartial pfAndThen: ")(pfAndThen)
  //  asyncPartial("asyncPartial pfOrElse: ")(pfOrElse)
  //  (99 to 101).apar map { x => pfRunWith(x) }
}
