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
import optimus.graph.diagnostics.sampling.ChildProcessSampling
import optimus.platform.EvaluationContext

import java.util.Objects

object AsyncProfilerGraphIntegration {

  final case class GraphCustomEvent private (eventType: AsyncProfilerIntegration.CustomEventType) {
    // Has to be lazy to avoid recursive class init trouble with GridProfiler
    import eventType._
    private lazy val eventExecutionInfo = NodeTaskInfo.internal(s"profiler.CustomEvent.${name}")
    def eventPtr(info: String) = AsyncProfilerIntegration.saveString(s"${name}.${valueType}.$info")

    private final class Recording(fromTask: Awaitable, value: Long, infoPtr: Long) extends NodeTask {
      // We still publish if we don't but we won't have a stack.
      if (Objects.nonNull(fromTask)) AwaitStackManagement.setLaunchData(fromTask, this, false, 0)

      override def executionInfo(): NodeTaskInfo = eventExecutionInfo
      override def run(ec: OGSchedulerContext): Unit = {
        val ptr = if (infoPtr != 0) infoPtr else eventNamePtr
        AsyncProfilerIntegration.recordCustomEvent(eventType, value.toDouble, value, ptr)
      }
    }

    /**
     * Record a custom event, ensuring async stack capture
     */
    def record(ctx: OGSchedulerContext, ntsk: NodeTask, value: Long, infoPtr: Long): Unit = {
      if (AsyncProfilerIntegration.ensureLoadedIfEnabled()) {
        val awaitMgr = if (ctx eq null) null else ctx.awaitMgt
        val recordingNtsk = new Recording(ntsk, value, infoPtr)
        awaitMgr.runChain(recordingNtsk)
      }
    }

    // Call overload above if OGSC is known already
    def record(value: Long, infoPtr: Long): Unit = {
      val ctx = OGSchedulerContext.current
      val ntsk = if (ctx eq null) null else ctx.getCurrentNodeTask
      record(ctx, ntsk, value, infoPtr)
    }
  }

  // These must be ordered and added before any profiling begins.
  val testingEvent = GraphCustomEvent(AsyncProfilerIntegration.TestingEvent)
  val fullWaitEvent = GraphCustomEvent(AsyncProfilerIntegration.FullWaitEvent)

  // Set child context to the stack of the currently executing node
  def setChildContext(childId: Int): Unit = {
    ChildProcessSampling.setChildContext(childId, EvaluationContext.currentNode)
  }

}
