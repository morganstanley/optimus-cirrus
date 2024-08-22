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
package optimus.examples

import optimus.graph.cache.NCPolicy
import optimus.platform._

@entity class AnalyzeEntity(x: Int) {
  @node(tweak = true) val tweakableA: Int = 0
  @node(tweak = true) val tweakableB: Int = 0
  @node(tweak = true) val tweakableC: Int = 0
  @node(tweak = true) def tweakableD: Int = 0

  /** all inputs: calc depends on input args and entity constructor parameters and tweakableA (but not tweakableB) */
  @node def calc(i: Int): Int = tweakableA + i + x + tweakableC

  /** depends on entity constructor parameters and tweakableA (but not tweakableB or input param) */
  @node def calcNoDependencyOnParam(i: Int): Int = tweakableA + x

  /** depends on tweakableA and input param and could've been moved to an object in presence of property tweaks */
  @node def calcNoDependencyOnEntity(i: Int): Int = tweakableA + i

  /** depends only on entity identity and input param */
  @node def calcNoDependencyOnScenario(i: Int): Int = i + x

  /** exactly the same as calc but marked xs */
  @node def calcXs(i: Int): Int = tweakableA + i + x + tweakableC

  /** exactly the same as calc but marked xsft */
  @node def calcXSFT(i: Int): Int = tweakableA + i + x + tweakableC

  /** exactly the same as calc but calling a non-cacheable child so that we can test profile time attribution */
  @node def calcWithAnc(i: Int): Int = tweakableA + i + x + tweakableC + anc
  @node def anc: Int = identity(1)
}

object AnalyzeEntity {
  calc.setCachePolicy(NCPolicy.Basic)
  calcXSFT.setCachePolicy(NCPolicy.XSFT)
  calcXs.setFavorReuse(true)
  calcWithAnc.setCachePolicy(NCPolicy.XSFT)
  anc.setCacheable(false)
}
