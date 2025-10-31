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
package optimus.core

import optimus.platform.EvaluationContext
import optimus.breadcrumbs.ChainedID
import optimus.graph.NodeTask

import java.util.Objects

object ChainedNodeID {

  private val overrides = new ThreadLocal[Option[ChainedID]] {
    override def initialValue(): Option[ChainedID] = None
  }

  private[optimus] def withNodeIDOverride(id: ChainedID)(f: => Unit): Unit = {
    val prev = overrides.get
    overrides.set(Some(id))
    f
    overrides.set(prev)
  }

  def nodeID: ChainedID = overrides.get.getOrElse {
    if (EvaluationContext.isInitialised) {
      // n.b. currentNode can be null if we've exited the graph on this context, and scenarioStack can be null
      // on a not fully initialized node. we can be called from almost anywhere so need to be defensive.
      val n = EvaluationContext.currentNode
      if (n ne null) {
        val ss = n.scenarioStack
        if (ss ne null) ss.getTrackingNodeID else ChainedID.root
      } else ChainedID.root
    } else ChainedID.root
  }

  def nodeID(node: NodeTask): ChainedID = {
    val id = node.scenarioStack().getTrackingNodeID
    if (Objects.isNull(id)) ChainedID.root else id
  }

  def apply(node: NodeTask): ChainedID = nodeID(node)
  def apply(): ChainedID = nodeID

  def rootID: ChainedID = EvaluationContext.scenarioStack.rootScenarioStack.getTrackingNodeID

  def nodeTag = s"$nodeID:${EvaluationContext.currentNode}"
}
