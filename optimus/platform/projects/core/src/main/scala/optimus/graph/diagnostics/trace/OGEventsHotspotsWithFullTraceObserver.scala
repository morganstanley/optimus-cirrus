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
package optimus.graph.diagnostics.trace

import java.util.function.Consumer

import optimus.core.EdgeList
import optimus.graph.NodeTask
import optimus.graph.NodeTrace
import optimus.platform.EvaluationQueue

object OGEventsHotspotsWithFullTraceObserver {
  var OnNodeCompleted: Consumer[NodeTask] = _
}

class OGEventsHotspotsWithFullTraceObserver private[trace]
    extends OGEventsHotspotsObserver
    with OGEventsTimeLineObserverImpl {
  this.traceNodes = true

  override def name: String = "traceNodes"
  override def description: String =
    "<html>Record every executed node and its timings<br>" + "Huge overhead in performance and memory</html>"
  override def title: String = "Trace Nodes"

  override def includeInUI = true
  override def traceTweaks = true
  override def traceWaits = true

  override final def dependency(fromTask: NodeTask, toTask: NodeTask, eq: EvaluationQueue): Unit = {
    super.dependency(fromTask, toTask, eq)
    dependency(fromTask, toTask)
  }

  protected def dependency(fromTask: NodeTask, toTask: NodeTask): Unit = {
    val caller = NodeTrace.accessProfile(fromTask)
    val callee = NodeTrace.accessProfile(toTask)
    if (callee.traceSelfAndParents) {
      NodeTrace.markNodeAndParentsToBeTraced(caller)
      caller.addLiveCallee(callee)
      callee.addCaller(caller)
    }
  }

  override def enqueue(fromTask: NodeTask, toTask: NodeTask): Unit = {
    val caller = NodeTrace.accessProfile(fromTask)
    val callee = NodeTrace.accessProfile(toTask)
    caller.addLiveEnqueued(callee)
  }

  override def enqueueFollowsSequenceLogic(task: NodeTask, maxConcurrency: Int): Unit = {
    val profile = NodeTrace.accessProfile(task)
    if (profile.callees eq null)
      profile.callees = EdgeList.newSeqOps(maxConcurrency)
  }

  override def completed(eq: EvaluationQueue, task: NodeTask): Unit = {
    super.completed(eq, task)
    if (OGEventsHotspotsWithFullTraceObserver.OnNodeCompleted != null)
      OGEventsHotspotsWithFullTraceObserver.OnNodeCompleted.accept(task)
  }
}
