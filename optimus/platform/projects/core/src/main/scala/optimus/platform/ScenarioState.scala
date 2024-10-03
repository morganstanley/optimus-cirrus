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
package optimus.platform

import optimus.config.RuntimeComponents
import optimus.config.scoped.ScopedSchedulerPlugin
import optimus.graph.NodeTask
import optimus.graph.NodeTaskInfo
import optimus.platform.dal.EntityResolver
import optimus.platform.inputs.loaders.FrozenNodeInputMap
import optimus.platform.inputs.loaders.OptimusNodeInputStorage

sealed abstract class ScenarioState extends Serializable {
  def environment: RuntimeEnvironment

  final def config: RuntimeComponents = environment.config

  /** Should be used only by a low level code, e.g. dist */
  private[optimus] def scenarioStack: ScenarioStack

  /** Should be used only by a low level code, e.g. dist */
  final def attachTo(n: NodeTask): Unit = n.attach(scenarioStack)

  /** Get a descriptive representation of the scenario state. */
  final def prettyString: String = s"${getClass.getSimpleName}:${scenarioStack.prettyString}"
}

/** Represent fully specified Scenario Stacks and therefore currently executing evaluation environment */
class FullySpecifiedScenarioState private[optimus /*platform*/ ] (
    private[optimus /*platform*/ ] val scenarioStack: ScenarioStack)
    extends ScenarioState {
  override def hashCode(): Int = scenarioStack._cacheID.hashCode()
  override def equals(obj: scala.Any): Boolean = obj match {
    case otherState: FullySpecifiedScenarioState => otherState.scenarioStack._cacheID eq this.scenarioStack._cacheID
    case _                                       => false
  }

  def environment: RuntimeEnvironment = scenarioStack.env
}

/** UntweakedScenarioState */
class UntweakedScenarioState private[optimus] (
    override val environment: RuntimeEnvironment,
    val scopedConfiguration: Map[NodeTaskInfo, ScopedSchedulerPlugin],
    val inputs: FrozenNodeInputMap)
    extends ScenarioState {

  override private[optimus] lazy val scenarioStack: ScenarioStack =
    ScenarioStack.newRoot(environment, inputs, scopedConfiguration, uniqueID = false)
  private[optimus] def initialRuntimeScenarioStack(requeryTime: Boolean) =
    scenarioStack.initialRuntimeScenarioStack(requeryTime)
}

object ScenarioState {
  val minimal: UntweakedScenarioState = UntweakedScenarioState(RuntimeEnvironment.minimal)
  private[optimus] def apply(ss: ScenarioStack) = new FullySpecifiedScenarioState(ss)

  def apply(env: RuntimeEnvironment): UntweakedScenarioState = UntweakedScenarioState(env)
}

object UntweakedScenarioState {
  def apply(env: RuntimeEnvironment) =
    new UntweakedScenarioState(env, null, OptimusNodeInputStorage.loadScopedState)
  def apply(config: RuntimeComponents, entityResolver: EntityResolver): UntweakedScenarioState =
    apply(new RuntimeEnvironment(config, entityResolver))

}
