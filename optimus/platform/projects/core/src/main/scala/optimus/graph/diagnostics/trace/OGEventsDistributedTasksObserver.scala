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

import optimus.graph.NodeTask
import optimus.graph.OGTrace
import optimus.graph.OGLocalTables
import optimus.graph.OGTrace.trace

class OGEventsDistributedTasksObserver private[trace]
    extends OGEventsNewHotspotsObserver
    with OGEventsTimeLineObserverImpl {
  override def name: String = "distributedTasks"
  override def title: String = "distributedTasks"
  override def description: String =
    "Treat distributed tasks treated as individual nodes and collect timeline and hotspots summaries"

  // requires some system properties set at startup and does not support switching (since it is for distributed processes)
  override def includeInUI = false

  override def ignoreReset: Boolean = true // We don't want to allow drop of information

  override def start(lCtx: OGLocalTables, task: NodeTask, isNew: Boolean): Unit = {
    super.start(lCtx, task, isNew)
    if (isNew && task.isRemoted) {
      trace.ensureProfileRecorded(task.getProfileId, task)
      lCtx.eventsTrace.publishNodeStarted(task.getId, task.ID.toString, OGTrace.nanoTime())
    }
  }

  override def resetBetweenTasks(): Boolean = false

  override def publishNodeExecuted(task: NodeTask): Unit = {
    super.publishNodeExecuted(task)
    val hotspots = OGTrace.liveReader.getHotspots
    val lt = OGLocalTables.getOrAcquire()
    OGTrace.trace.writeProfileData(hotspots, lt.eventsTrace) // Weird API passes writer back
    lt.release()

  }
}
