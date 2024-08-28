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
package optimus.config

import java.time.Instant
import optimus.platform.RuntimeEnvironment
import optimus.platform.dal.EntityResolver
import optimus.platform.Scenario
import optimus.platform.ScenarioStack
import optimus.platform.UntweakedScenarioState
import optimus.platform.ScenarioState
import optimus.platform.UntweakedScenarioState
import optimus.platform.inputs.loaders.FrozenNodeInputMap

abstract class RuntimeComponents(final val runtimeConfig: RuntimeConfiguration) {
  def createEntityResolver(): EntityResolver
  def createRuntime(): RuntimeEnvironment

  /** Returns env as scenario state */
  private[optimus] final def createUntweakedScenarioState(localInputs: FrozenNodeInputMap): UntweakedScenarioState =
    new UntweakedScenarioState(createRuntime(), localInputs)

  /** Returns env as scenario state */
  final def createUntweakedScenarioState(): UntweakedScenarioState = ScenarioState(createRuntime())

  final def envName: String = if (runtimeConfig ne null) runtimeConfig.env else null

  private[optimus] def newTopScenario(resolver: EntityResolver): Scenario
  private[optimus] def initialRuntimeScenarioStack(ss: ScenarioStack, initialTime: Instant): ScenarioStack

  private[optimus] def contextClassloader: ClassLoader = classOf[RuntimeComponents].getClassLoader

  private[optimus] def isThin: Boolean = false
}
