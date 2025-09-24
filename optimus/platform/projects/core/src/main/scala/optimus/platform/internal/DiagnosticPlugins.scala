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
package optimus.platform.internal

import optimus.graph.NodeTask
import optimus.graph.NodeTaskInfo
import optimus.graph.OGSchedulerContext
import optimus.graph.SchedulerPlugin
import optimus.graph.Settings

object IgnoreSyncStacksPlugin extends SchedulerPlugin {
  def installIfNeeded(nodes: NodeTaskInfo*): Unit = {
    if (Settings.syncStacksDetectionEnabled) nodes foreach { taskInfo =>
      require(!taskInfo.shouldLookupPlugin(), s"There should not already be a plugin set on ${taskInfo.name}")
      taskInfo.setPlugin(IgnoreSyncStacksPlugin)
    }
  }
  def adapt(n: NodeTask, ec: OGSchedulerContext): Boolean = {
    val newStack = n.scenarioStack().ensureIgnoreSyncStack
    n.replace(newStack)
    false
  }
}
