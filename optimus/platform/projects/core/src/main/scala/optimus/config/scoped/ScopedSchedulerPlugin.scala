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
package optimus.config.scoped

import optimus.graph.SchedulerPlugin
import optimus.platform.PluginTagKey

import java.io.{Serializable => JSerializable}

/**
 * Instances extending this class can be put on the [[ optimus.platform.ScenarioStack]] targeting a [[optimus.graph.NodeTaskInfo]].
 * At runtime, the [[SchedulerPlugin]] will be instantiated and used to handle the desired @node.
 */
abstract class ScopedSchedulerPlugin extends JSerializable {

  /** If the [[ScopedSchedulerPlugin]] should only take effect when a certain [[PluginTagKey]] is on the scenario stack */
  private[optimus] def enablePluginTagKey: PluginTagKey[_] = null

  /** @see [[createPlugin()]] */
  @transient lazy final val schedulerPlugin: SchedulerPlugin = createPlugin()

  /** @return the [[SchedulerPlugin]] used when adapting @nodes */
  protected def createPlugin(): SchedulerPlugin
}
