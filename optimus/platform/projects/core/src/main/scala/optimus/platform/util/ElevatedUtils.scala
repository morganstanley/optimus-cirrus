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

import optimus.graph.NodeTask
import optimus.platform.EvaluationContext
import optimus.platform.ForwardingPluginTagKey

object ElevatedPluginTag extends ForwardingPluginTagKey[OnBehalfUserName] {}

final case class OnBehalfUserName(userName: String)

object ElevatedUtils {

  def throwOnElevated(): Unit = {
    val currentNode = EvaluationContext.currentNode
    if (ElevatedUtils.hasElevatedPluginTag(currentNode))
      throw new UnsupportedOperationException(s"Cannot call legacy DAL API from @elevated context $currentNode")
  }

  def hasElevatedPluginTag(node: NodeTask): Boolean = node.scenarioStack().findPluginTag(ElevatedPluginTag).isDefined

  def elevatedForUser(node: NodeTask): Option[String] =
    node.scenarioStack().findPluginTag(ElevatedPluginTag).map(_.userName)

  def currentElevatedForUser: Option[String] = elevatedForUser(EvaluationContext.currentNode)
}
