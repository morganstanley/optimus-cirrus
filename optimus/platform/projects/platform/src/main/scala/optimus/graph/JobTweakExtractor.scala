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
package optimus.graph

import optimus.platform.Scenario
import optimus.platform.ScenarioStack
import optimus.platform.Tweak

private[optimus] object JobTweakExtractor {

  private val redirectToGuide =
    "Please check out http://optimusguide/AdvancedAnnotations/AnnotationJob.html#troubleshooting for further info"

  /**
   * Extracts [[Tweak]] from [[ScenarioStack]]. Supported [[Tweak]] must be [[InstancePropertyTarget]] on a
   * parameter-less @node defined on an entity object.
   * @param ss
   *   the [[ScenarioStack]] whose [[Tweak]] need extracting
   * @throws GraphException
   *   if the tweaks present in the scenario do not conform to requirements
   * @return
   *   the flattened sequence of [[Tweak]] present in the [[ScenarioStack]]
   */
  def fromScenarioStack(ss: ScenarioStack): Seq[Tweak] = {
    // first build the array of scenarios, from outermost to innermost
    val scenarios = ScenarioStackMoniker.buildSSCacheIDArray(ss).reverse
    var scenario = if (scenarios.nonEmpty) scenarios.head else Scenario.empty
    // now flatten the scenarios, converting tweaks to by value where possible
    if (scenarios.tail.nonEmpty) {
      scenarios.tail.foreach(s => scenario = scenario.nestWithFlatten(s))
    }
    // ensure there are no nested scenarios, which would imply evaluation is needed
    CacheIDs.validateInputScenario(scenario)
    scenario.allTweaks.foreach(validateTweak)
    scenario.allTweaks
  }

  private[this] def validateTweak(tweak: Tweak): Unit = {
    val nodeKey = tweak.target.hashKey
    if (nodeKey eq null) {
      throw new GraphException(s"Tweak target is not instance property target. $redirectToGuide")
    }
    if (!nodeKey.entity.$isModule) {
      throw new GraphException(
        s"Tweak is not defined on @entity object (could it be defined on @entity class?). $redirectToGuide")
    }
    if (nodeKey.args ne NodeTask.argsEmpty) {
      throw new GraphException(s"Tweak must be defined on @node without arguments. $redirectToGuide")
    }
  }

}
