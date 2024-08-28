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

import optimus.core.EdgeIDList
import optimus.graph.NodeTask
import optimus.graph.NodeTrace
import optimus.graph.OGTrace.trace
import optimus.graph.{OGLocalTables => LT}
import optimus.platform.EvaluationQueue

// TODO (OPTIMUS-44050): enable tracing only for specified nodes
abstract class OGEventsRecordingObserverBase protected (config: RecordingConfig)
    extends OGEventsHotspotsObserver
    with OGEventsTimeLineObserverImpl {
  override def title: String = "Recording"
  override def description: String = "Record to a trace file, no processing"

  override def includeInUI: Boolean = true
  override def supportsReadingFromFile: Boolean = true
  override def supportsProfileBlocks: Boolean = false
  override def recordLostConcurrency: Boolean = false
  override def liveProcess: Boolean = false // Just record

  protected def this() = this(RecordingConfig.default)

  override def dependency(fromTask: NodeTask, toTask: NodeTask, eq: EvaluationQueue): Unit = {

    /** always need both calls to accessProfile because they call ensureProfileRecorded to write the PNodeTaskInfo */
    val callee = NodeTrace.accessProfile(toTask)
    val caller = NodeTrace.accessProfile(fromTask)

    if (callee.traceSelfAndParents) {
      caller.traceSelfAndParents = true
      caller.addLiveCalleeID(callee.id)
    }
    super.dependency(fromTask, toTask, eq)
  }

  override def enqueue(fromTask: NodeTask, toTask: NodeTask): Unit = {
    val caller = NodeTrace.accessProfile(fromTask)
    caller.addLiveIDEnqueued(toTask.getId)
  }

  override def enqueueFollowsSequenceLogic(task: NodeTask, maxConcurrency: Int): Unit = {
    val profile = NodeTrace.accessProfile(task)
    if (profile.calleeIDs eq null)
      profile.calleeIDs = EdgeIDList.newSeqOps(maxConcurrency)
  }

  override def initializeAsCompleted(task: NodeTask): Unit = {
    super.initializeAsCompleted(task)
    if (config.storeHashes) {
      val pnt = NodeTrace.accessProfile(task)
      val lt = LT.getOrAcquire()
      OGEventObserverUtils.writeHashes(lt.eventsTrace, pnt, task)
      lt.release()
    }
  }

  override def completed(eq: EvaluationQueue, task: NodeTask): Unit = {
    if (task.executionInfo.isTraceSelfAndParent) {
      val lt = LT.getOrAcquire(eq)
      val pnt = NodeTrace.accessProfile(task)

      if (pnt.calleeIDs ne null)
        trace.writeEdges(lt, task.getId, pnt.calleeIDs)

      if (config.storeHashes)
        OGEventObserverUtils.writeHashes(lt.eventsTrace, pnt, task)

      lt.release()
      super.completed(eq, task)
    }
  }
}

/**
 * TODO (OPTIMUS-43938): Currently a few useful configurations but ultimately we want a whole config file that specifies
 * recording types e.g. serialization options/hashes etc..
 */
private[trace] final case class RecordingConfig(storeHashes: Boolean)
private[trace] object RecordingConfig {
  val default: RecordingConfig = RecordingConfig(storeHashes = false)
  val hashedValues: RecordingConfig = RecordingConfig(storeHashes = true)
}

class OGEventsRecordingObserver private[trace] extends OGEventsRecordingObserverBase {
  override def name: String = "recording"
}

class OGEventsRecordingObserverWithHashes private[trace]
    extends OGEventsRecordingObserverBase(RecordingConfig.hashedValues) {
  override def name: String = "recordingWithHashes"
}
