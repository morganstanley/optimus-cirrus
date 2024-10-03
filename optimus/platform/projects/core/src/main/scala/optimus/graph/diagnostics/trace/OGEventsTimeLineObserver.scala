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

import optimus.core.RunCalcDistributedTaskLabelAppender
import optimus.graph.NodeTask
import optimus.graph.OGLocalTables
import optimus.graph.OGTrace
import optimus.graph.OGTraceCounter
import optimus.graph.PluginType
import optimus.graph.diagnostics.messages.OGEvent
import optimus.platform.EvaluationQueue

import java.lang.{Long => JLong}

object OGEventsTimeLineObserver {
  private val NANO_OFFSET_PREFIX = "_dt="
  def isNanoOffset(s: String): Boolean = s.startsWith(NANO_OFFSET_PREFIX)
  def packNanoOffsetInString: String = {
    val nanoOffset = System.currentTimeMillis - System.nanoTime / 1000000
    NANO_OFFSET_PREFIX + nanoOffset
  }
  def millisFromNanos(nanos: Long, packedOffset: String): Long = {
    val offset = nanoOffset(packedOffset)
    nanos / 1000000 + offset
  }

  def nanoOffset(packedOffset: String): Long = {
    JLong.parseLong(packedOffset.substring(NANO_OFFSET_PREFIX.length()))
  }
}

trait OGEventsTimeLineObserverImpl extends OGEventsGlobalGraphObserver {

  override def collectsTimeLine: Boolean = true

  override protected def graphEnter(lt: OGLocalTables, time: Long): Unit = lt.eventsTrace.enterGraph(time)

  override def graphExit(lt: OGLocalTables, time: Long): Unit = lt.eventsTrace.exitGraph(time)

  override protected def graphEnterWait(lt: OGLocalTables, causalityID: Int, time: Long): Unit = {
    lt.eventsTrace.enterWait(causalityID, time)
  }

  override protected def graphExitWait(lt: OGLocalTables, time: Long): Unit = lt.eventsTrace.exitWait(time)

  override protected def graphSpinEnter(lt: OGLocalTables, time: Long): Unit = lt.eventsTrace.enterSpin(time)

  override protected def graphSpinExit(lt: OGLocalTables, time: Long): Unit = lt.eventsTrace.exitSpin(time)

  override def completed(eq: EvaluationQueue, task: NodeTask): Unit = {
    super.completed(eq, task)
    var pluginType: PluginType = PluginType.None
    if (task.getPlugin ne null) pluginType = task.getPlugin.pluginType
    if (task.isAdapted) OGTraceCounter.adaptedChangeBy(pluginType, -1)
  }

  override def adapted(eq: EvaluationQueue, pluginType: PluginType): Unit = {
    OGTraceCounter.adaptedChangeBy(pluginType, 1)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////

  override def publishNodeSending(task: NodeTask, timestamp: Long): Unit = {
    val lt = OGLocalTables.getOrAcquire()
    val data = RunCalcDistributedTaskLabelAppender.getExtraData(task)
    val label = if (data eq null) "no-label-rcvd" else data
    lt.eventsTrace.publishNodeSending(
      task.getId,
      task.getProfileId,
      task.profileBlockID,
      task.ID.toString,
      label,
      timestamp)
    lt.release()
  }

  override def publishNodeReceived(task: NodeTask): Unit = {
    val lt = OGLocalTables.getOrAcquire()
    val timestamp = OGTrace.nanoTime()
    lt.eventsTrace.publishNodeReceived(task.getId, task.getProfileId, task.profileBlockID, task.ID.toString, timestamp)
    lt.release()
  }

  override def publishNodeStolenForLocalExecution(task: NodeTask): Unit = {
    val lt = OGLocalTables.getOrAcquire()
    val timestamp = OGTrace.nanoTime()
    lt.eventsTrace.publishNodeReceivedLocally(
      task.getId,
      task.getProfileId,
      task.profileBlockID,
      task.ID.toString,
      timestamp)
    lt.release()
  }

  override def publishNodeExecuted(task: NodeTask): Unit = {
    val lt = OGLocalTables.getOrAcquire()
    lt.eventsTrace.publishTaskCompleted(task.getId, task.ID.toString, OGTrace.nanoTime())
    lt.release()
  }

  override def publishNodeResultReceived(task: NodeTask): Unit = {
    val lt = OGLocalTables.getOrAcquire()
    lt.eventsTrace.publishNodeResultReceived(task.getId, task.ID.toString, OGTrace.nanoTime())
    lt.release()
  }

  override def publishSerializedNodeResultArrived(task: NodeTask): Unit = {
    val lt = OGLocalTables.getOrAcquire()
    lt.eventsTrace.publishSerializedNodeResultArrived(task.getId, task.ID.toString, OGTrace.nanoTime())
    lt.release()
  }

  //////////////////////////////////////////////////////////////////////////////////////////////

  override def writeEvent(event: OGEvent): Unit = {
    val lt = OGLocalTables.getOrAcquire()
    if (event.time == 0)
      event.time = OGTrace.nanoTime() // start off with timestamp, processor will correct offset [SEE_OFFSET]
    lt.eventsTrace.write(event)
    lt.release()
  }

  override def writeEventComplete(counterID: Int, id: Int): Unit = {
    val lt = OGLocalTables.getOrAcquire()
    lt.eventsTrace.counterEventComplete(counterID, id, OGTrace.nanoTime())
    lt.release()
  }
}

class OGEventsTimeLineObserver private[trace] extends OGEventsTimeLineObserverImpl {
  override def name: String = "timelineLight"
  override def description: String =
    "<html>Record scheduler transitions and custom counters<br>Very low overhead</html>"
}
