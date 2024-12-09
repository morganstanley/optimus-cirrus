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

import optimus.platform.EvaluationQueue

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.{ArrayList => JArrayList}
import msjava.slf4jutils.scalalog.getLogger

/**
 * Allows scoping of batched calls with respect to other outstanding calls. Tracks outstanding DSI calls, and triggers
 * scheduler.reportDsiCountCleared() when there are no longer DSI calls in flight. Could be extended in future in
 * required to other types of external call.
 *
 * The idea is that batchers want to defer batching for as long as possible (unless they already hit their desired batch
 * size), but if they wait forever we'll deadlock. So the scheduler triggers all of the OutOfWorkListeners whenever the
 * graph runs out of work, but intelligent batchers can choose to ignore this while there are outstanding DSI calls
 * (which could lead to more nodes for batching) and then get retriggered once there are no oustanding calls.
 */
final class BatchScope extends Serializable {
  private val callsInFlight = new ConcurrentHashMap[BatchScopeKey, AtomicInteger]
  private val tasksInFlight = new ConcurrentHashMap[IdentityKey, NodeTask]

  private class IdentityKey(val o: AnyRef) {
    override def hashCode(): Int = o.hashCode()
    override def equals(that: Any): Boolean = this.o eq that.asInstanceOf[IdentityKey].o
  }

  def enter(key: BatchScopeKey, task: NodeTask): Unit = {
    if (DiagnosticSettings.batchScopeVerboseLogging)
      BatchScope.log.info(s"$this entering $key $task ${System.identityHashCode(task)} ")

    callsInFlight.putIfAbsent(key, new AtomicInteger(0))
    callsInFlight.get(key).incrementAndGet()
    if (key.track) {
      val prevTask = tasksInFlight.putIfAbsent(new IdentityKey(task), task)
      if (prevTask != null) {
        val msg =
          s"$this task entered twice $key $task ${System.identityHashCode(task)} ${System.identityHashCode(prevTask)}"
        BatchScope.log.error(msg, new Exception(msg))
      }
    }
  }

  def exit(eq: EvaluationQueue, key: BatchScopeKey, task: NodeTask): Unit = {
    if (DiagnosticSettings.batchScopeVerboseLogging)
      BatchScope.log.info(s"$this exiting $key $task ${System.identityHashCode(task)}")

    val callsAfter = {
      if (!callsInFlight.containsKey(key)) {
        throw new GraphInInvalidState(s"Something is really wrong: No calls in flight with key $key")
      }

      callsInFlight.get(key).decrementAndGet()
    }

    if (callsAfter < 0) {
      throw new GraphInInvalidState(s"Something is really wrong: $callsAfter calls in flight, keyOpt = $key")
    }

    // since batchers are allowed to ignore out of work callbacks while hasCallsInFlight == true, we need
    // to poke them all when hasCallsInFlight transitions to false, i.e. right now.
    if (callsAfter == 0) {
      eq.scheduler.reportCallsInFlightCountCleared()
    }

    if (key.track && (tasksInFlight.remove(new IdentityKey(task)) eq null)) {
      val msg = s"$this remove nonexistent task $key $task ${System.identityHashCode(task)}"
      BatchScope.log.error(msg, new Exception(msg))
    }
  }

  def enterDSI(task: NodeTask): Unit = {
    enter(DSI, task)
  }

  def exitDSI(eq: EvaluationQueue, task: NodeTask): Unit = {
    exit(eq, DSI, task)
  }

  def enterDMC(task: NodeTask): Unit = {
    enter(DMC, task)
  }

  def exitDMC(eq: EvaluationQueue, task: NodeTask): Unit = {
    exit(eq, DMC, task)
  }

  def hasCallsInFlight(keys: Set[BatchScopeKey]): Boolean = hasCallsInFlight(keys, null)

  def hasCallsInFlight(keys: Set[BatchScopeKey], outstandingTasks: JArrayList[NodeTask]): Boolean = {
    if (keys.exists(hasCallsInFlightWithKey)) {
      if (outstandingTasks ne null)
        outstandingTasks.addAll(tasksInFlight.values)
      true
    } else false
  }

  private def hasCallsInFlightWithKey(key: BatchScopeKey): Boolean = {
    callsInFlight.containsKey(key) && callsInFlight.get(key).get() > 0
  }

  def logBatchScopeStatus: Unit = {
    val callsMessage = new StringBuilder(s"$this tasksInFlight(${tasksInFlight.size}) callsInFlight: ")
    callsInFlight.forEach { (key, value) =>
      {
        callsMessage.append(s"$key=${value.get} ")
      }
    }
    BatchScope.log.info(callsMessage.toString)
  }
}

object BatchScope {
  val DefaultBatchScopeKeys: Set[BatchScopeKey] = Set(DMC, DSI)
  private val log = getLogger(this)
}

trait BatchScopeKey {
  // For XSFT cycle recovery
  def track: Boolean = true
}

private object DMC extends BatchScopeKey {
  override def track: Boolean = DiagnosticSettings.batchScopeTrackNodes
}

private object DSI extends BatchScopeKey {
  override def track: Boolean = DiagnosticSettings.batchScopeTrackNodes
}
