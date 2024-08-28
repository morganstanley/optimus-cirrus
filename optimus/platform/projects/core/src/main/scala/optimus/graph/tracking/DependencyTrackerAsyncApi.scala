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
package optimus.graph.tracking

import optimus.graph.Node
import optimus.graph.NodeKey
import optimus.graph.PropertyNode
import optimus.graph.Scheduler
import optimus.graph.Settings
import optimus.graph.tracking.CleanupScheduler.DependencyTrackerRootCleanupState
import optimus.graph.tracking.CleanupScheduler.InterruptionFlag
import optimus.graph.tracking.DependencyTracker.log
import optimus.platform.Scenario
import optimus.platform.ScenarioStack
import optimus.platform.Tweak
import optimus.ui.ScenarioReference

import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * The asynchronous public API of DependencyTracker
 */
trait DependencyTrackerAsyncApi {
  self: DependencyTracker =>
  def addTweaksAsync(tweaks: Iterable[Tweak], throwOnDuplicate: Boolean, callback: Try[Unit] => Unit): Unit =
    queue.executeAsync(new TSA_AddTweaks(tweaks, throwOnDuplicate), callback)

  def executeEvaluateAsync[T](expr: () => T, callback: Try[T] => Unit, onLowPriorityQueue: Boolean = false): Unit =
    queue.executeAsync(
      new TSA_Evaluate(expr) {
        override def toString = s"executeEvaluateAsync@${Integer.toHexString(System.identityHashCode(this))}"
        override def logSummary: String = toString
        override def isLowPriority: Boolean = onLowPriorityQueue
      },
      callback
    )

  /** Evaluate asynchronously given node by key */
  def evaluateNodeKeyAsync[T](key: NodeKey[T], callback: Try[T] => Unit, cause: EventCause): Unit =
    queue.executeAsync(new EvaluateNodeKeyAction(key, cause), callback)

  def evaluateNodeKeysAsync(keys: collection.Seq[NodeKey[_]], callback: NodeKeysCallback, cause: EventCause): Unit = {
    val action = new EvaluateNodeKeysAction(keys, cause)
    queue.executeAsync(action, adaptedNodeKeysCallback(callback, action))
  }

  def evaluateNodeKeysLowPriorityAsync(
      keys: collection.Seq[NodeKey[_]],
      callback: NodeKeysCallback,
      cause: EventCause): Unit = {
    val action = new EvaluateNodeKeysLowPriorityAction(keys, cause)
    queue.executeAsync(action, callback = adaptedNodeKeysCallback(callback, action))
  }

  type NodeKeysCallback = Try[(collection.Seq[Node[_]], collection.Seq[NodeKey[_]])] => Unit
  private[this] def adaptedNodeKeysCallback(cb: NodeKeysCallback, action: EvaluateNodeKeysAction)(
      res: Try[Unit]): Unit = {
    if (cb ne null) {
      res.failed.toOption match {
        case Some(ex) => cb(Failure(ex))
        case None     => cb(Success(action.nodes -> action.nodeKeys))
      }
    }
  }

  def removeTweaksAsync(nks: Iterable[NodeKey[_]], callback: Try[Unit] => Unit): Unit =
    queue.executeAsync(new TSA_RemoveTweak(nks), callback)

  def setUnderlayAsync(scenario: Scenario, callback: Try[Unit] => Unit): Unit =
    queue.executeAsync(new TSA_SetUnderlay(scenario), callback)

  def removeAllTweaksAsync(callback: Try[Unit] => Unit): Unit =
    queue.executeAsync(new TSA_RemoveAllTweaks(), callback)

  def snapshotScenarioAsync(callback: Try[ScenarioStack] => Unit): Unit = {
    log.debug("Snapshotting scenario stack")
    queue.executeAsync(new TSA_SnapshotScenario, callback)
  }

  def executeBatchUpdateAsync[R](
      cause: EventCause,
      update: DependencyTrackerBatchUpdater => R,
      callback: Try[R] => Unit,
      highPriority: Boolean = false,
      scheduler: Scheduler = Scheduler.currentOrDefault): Unit = {
    if (Settings.trackingScenarioConcurrencyDebug && scenarioReference == ScenarioReference.Root) {
      log.warn(
        "Running update action against the Root DependencyTracker. This could negatively affect concurrency. " +
          s"Cause($cause), Update($update), Callback($callback), HighPriority($highPriority)")
    }
    if (isDisposed) {
      callback(Failure(new DependencyTrackerDisposedException(name)))
    } else {
      queue.executeAsync(new TSA_BatchUpdate(cause, update, scheduler), callback, highPriority)
    }
  }

  def disposeAsync(clearCacheOfDisposedNodes: Boolean = true): Unit = {
    // Note - we execute the dispose on root queue as Dispose can modify the parent and root view of child scenarios
    // keep this in step with dispose
    root.queue.executeAsync(new TSA_Dispose(clearCacheOfDisposedNodes), null)
  }
  private[tracking] def reevaluateTrackedNodesAction(cause: EventCause, nodes: collection.Seq[PropertyNode[_]]) =
    new EvaluateNodeKeysAction(nodes, cause)

  private[tracking] def cleanupAsync(
      state: Option[DependencyTrackerRootCleanupState],
      interruptMode: InterruptionFlag,
      triggeredBy: TrackingGraphCleanupTrigger,
      callback: Try[Unit] => Unit): Unit = {
    val action = root.createTrackingGraphCleanupAction(state, interruptMode, triggeredBy)
    queue.executeAsync(action, callback)
  }
}
