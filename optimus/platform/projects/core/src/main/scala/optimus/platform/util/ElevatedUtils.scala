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

import optimus.graph.JobForwardingPluginTagKey
import optimus.graph.NodeTask
import optimus.platform.EvaluationContext

import java.time.Instant

object ElevatedPluginTag extends JobForwardingPluginTagKey[OnBehalfUserName] {}

final case class OnBehalfUserName(
    userName: String,
    roleSets: Set[String],
    // when we create the DAL session on the engine side, we connect as the proid running the engine
    // so we keep these three original values because they are required for @job execution (see ElevatedJobCoreAPI)
    ogEstablishTime: Instant,
    ogDalSessionToken: Vector[Byte],
    ogRoleSets: Set[String]
)

object ElevatedUtils {

  def throwOnElevated(): Unit = {
    val currentNode = EvaluationContext.currentNode
    if (ElevatedUtils.hasElevatedPluginTag(currentNode))
      throw new UnsupportedOperationException(s"Cannot call legacy DAL API from @elevated context $currentNode")
  }

  def hasElevatedPluginTag(node: NodeTask): Boolean = elevatedFor(node).isDefined

  def elevatedForUser(node: NodeTask): Option[String] =
    elevatedFor(node).map(_.userName)

  def elevatedFor(node: NodeTask): Option[OnBehalfUserName] =
    node.scenarioStack().findPluginTag(ElevatedPluginTag)

  def currentElevatedForUser: Option[String] = elevatedForUser(EvaluationContext.currentNode)

  def requireCurrentElevatedForUser: String = requireElevated(_.userName)
  def requireCurrentElevatedEstablishTime: Instant = requireElevated(_.ogEstablishTime)
  def requireCurrentElevatedDalSessionToken: Vector[Byte] = requireElevated(_.ogDalSessionToken)
  def requireCurrentElevatedRolesets: Set[String] = requireElevated(_.ogRoleSets)
  private def requireElevated[T](f: OnBehalfUserName => T): T = elevatedFor(EvaluationContext.currentNode) match {
    case None =>
      throw new IllegalStateException("No @elevated metadata set - please report this, it should never happen!")
    case Some(u) => f(u)
  }
}
