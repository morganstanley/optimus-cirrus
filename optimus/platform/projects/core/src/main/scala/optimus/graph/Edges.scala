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

import com.google.common.cache.CacheBuilder
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.CrumbNodeType
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.breadcrumbs.crumbs.EdgeType
import optimus.breadcrumbs.graph.RawEdges
import optimus.platform._

object Edges {

  private val dummy = new Object
  private val edgeAlreadyTracked = CacheBuilder.newBuilder.maximumSize(Long.MaxValue).weakKeys.build[ChainedID, AnyRef]

  /**
   * Create a new ChainedID for the a new node and attach it. This should only be called on new nodes or nodes being
   * adapted. Ensure that the new node is tracked by breadcrumbs if enabled.
   */
  private class CrumbCompleter(source: Crumb.Source = Crumb.OptimusSource) extends NodeAwaiter {
    override def onChildCompleted(q: EvaluationQueue, n: NodeTask): Unit = {
      if (n.isDoneWithException)
        Breadcrumbs.info(n.ID, PropertiesCrumb(_, source, Properties.exception -> n.exception()))
    }
  }
  private val crumbCompleter = new CrumbCompleter
  private val observableCrumbCompleter = new CrumbCompleter(Crumb.ObservableSource)

  def ensureTracked(node: NodeTask, label: String, tpe: CrumbNodeType.CrumbNodeType): ChainedID = {
    val ss = if (node.scenarioStack ne null) node.scenarioStack else EvaluationContext.scenarioStack
    val parentId = ss.getTrackingNodeID // Current tracking node
    val ssChild = ss.withTrackingNode // Child scenario stack
    node.replace(ssChild)
    val id = ssChild.getTrackingNodeID
    edgeAlreadyTracked.put(id, dummy)
    if (RawEdges.edgeCrumbsByDefault && Breadcrumbs.collecting) {
      node.tryAddToWaiterList(crumbCompleter)
      ensureTracked(id, parentId, label, tpe, EdgeType.InvokedBy)
    }
    id
  }

  /**
   * Ensure that a tracking ID is _available_ at node (note that the ID returned will be that of the current tracking
   * node, which may not be the current node) and that it is tracked by breadcrumbs if enabled. (Does NOT create a new
   * nodeID.)
   */
  def ensureTracked(node: NodeTask, ss: ScenarioStack): ChainedID = {
    val id = ss.getTrackingNodeID
    // Note: can't use get(,Callable), because we could end up being called recursively via logging;
    // an erroneous cache miss will just result in extra breadcrumbs being sent.
    if (edgeAlreadyTracked.getIfPresent(id) eq null) {
      edgeAlreadyTracked.put(id, dummy)
      val parentId = ss.getParentTrackingNode
      ensureTracked(id, parentId, node.toPrettyName(true, false), CrumbNodeType.GenericNode, EdgeType.InvokedBy)
    }

    id
  }

  /**
   * Ensure that a breadcrumb has been sent.
   */
  def ensureTracked(
      child: ChainedID,
      parent: => ChainedID,
      name: => String,
      tpe: CrumbNodeType.CrumbNodeType,
      edge: EdgeType.EdgeType,
      source: Crumb.Source = Crumb.OptimusSource): ChainedID =
    RawEdges.ensureTracked(child, parent, name, tpe, edge, source)

  // [JOB_EXPERIMENTAL]
  private[graph] def ensurePropertyJobNodeTracked[R](key: PropertyNode[R], ec: OGSchedulerContext): Node[R] = {
    // `ss.getNode(key, ec)` can return a node with a scenario stack different than `ss`, for example when a cache hit occurs.
    // therefore, keep an explicit reference to `ec.scenarioStack()` to correctly link edges.
    val ss = ec.scenarioStack()
    val node = ss.withTrackingNode.getNode(key, ec)
    val name = node.toPrettyName(true, true)
    if (Breadcrumbs.collecting) {
      node.tryAddToWaiterList(observableCrumbCompleter)
      ensureTracked(
        node.ID,
        ss.getTrackingNodeID,
        name,
        CrumbNodeType.JobNode,
        EdgeType.InvokedBy,
        source = Crumb.ObservableSource)
    }
    node
  }

  // [JOB_EXPERIMENTAL]
  private[graph] def ensureJobNodeTracked[R](node: Node[R]): Unit = {
    val ss = node.scenarioStack
    node.replace(ss.withTrackingNode)
    val name = node.toPrettyName(true, true)
    if (Breadcrumbs.collecting) {
      node.tryAddToWaiterList(observableCrumbCompleter)
      ensureTracked(
        node.ID,
        ss.getTrackingNodeID,
        name,
        CrumbNodeType.JobNode,
        EdgeType.InvokedBy,
        source = Crumb.ObservableSource)
    }
  }
}
