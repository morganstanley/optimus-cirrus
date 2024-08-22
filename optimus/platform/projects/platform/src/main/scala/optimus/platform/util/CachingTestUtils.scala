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
package optimus.platform.util

import optimus.graph.NodeTaskInfo
import optimus.graph.PropertyInfo
import optimus.graph.SchedulerPlugin

object CachingTestUtils {
  // Watch out! If multiple tests are running the same task and the counter is only being set once in test setup,
  // this can result in flakiness if the tests are asserting counts relevant only to them without resetting in between
  def runCount(info: PropertyInfo[_]): Int = getCount(info.getPlugin)
  def runCount(info: NodeTaskInfo): Int = getCount(info.getPlugin)

  def resetCount(info: PropertyInfo[_]): Unit = info.getPlugin match {
    case c: NodeCounterPlugin => c.reset()
    case _                    => throw new IllegalArgumentException("Expected NodeCounterPlugin")
  }

  private def getCount(plugin: SchedulerPlugin) = plugin match {
    case c: NodeCounterPlugin => c.count
    case _                    => throw new IllegalArgumentException("Expected NodeCounterPlugin")
  }
}
