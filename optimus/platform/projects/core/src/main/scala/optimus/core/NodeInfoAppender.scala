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

import com.github.benmanes.caffeine.cache.Caffeine
import optimus.graph.GraphStallInfo
import optimus.graph.NodeTask
import optimus.platform.EvaluationContext
import optimus.platform.EvaluationQueue

private[core] class NodeInfoAppender[T <: AnyRef] {
  private val nodeInfoMap = Caffeine.newBuilder().weakKeys().build[NodeTask, T]().asMap()

  def attachExtraData(node: NodeTask, associatedData: T): Unit = {
    // first time we add to map attach the continuation to remove it from map
    if (nodeInfoMap.put(node, associatedData) eq null) {
      node.continueWithIfEverRuns((_: EvaluationQueue, n: NodeTask) => nodeInfoMap.remove(n), EvaluationContext.current)
    }
  }

  def getExtraData(node: NodeTask): T = nodeInfoMap.get(node)
  def removeExtraData(node: NodeTask): Unit = nodeInfoMap.remove(node)
}

object StallInfoAppender extends NodeInfoAppender[() => GraphStallInfo] {
  override def attachExtraData(node: NodeTask, associatedData: () => GraphStallInfo): Unit = {
    super.attachExtraData(node, associatedData)
  }
  override def getExtraData(node: NodeTask): () => GraphStallInfo = super.getExtraData(node)
}

object RunCalcDistributedTaskLabelAppender extends NodeInfoAppender[String]
