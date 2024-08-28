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

import optimus.graph.NodeKey
import optimus.graph.Scheduler
import optimus.graph.tracking.DependencyTracker.log
import optimus.graph.Settings
import optimus.platform.Scenario
import optimus.platform.Tweak
import optimus.platform.annotations.closuresEnterGraph
import optimus.platform.storable.Entity
import optimus.ui.ScenarioReference

/**
 * The synchronous public API of DependencyTracker
 */
trait DependencyTrackerSyncApi {
  self: DependencyTracker =>

  def track[T, M >: Null <: TrackingMemo](key: NodeKey[T], scope: TrackingScope[M]): TrackedNode[T] =
    track(key, scope, scope.defaultMemo(key, this))

  def track[T, M >: Null <: TrackingMemo](key: NodeKey[T], scope: TrackingScope[M], memo: M): TrackedNode[T] =
    userNodeTracker.userTrackUpdate(key, scope, (_: M) => memo)

  def trackUpdate[T, M >: Null <: TrackingMemo](
      key: NodeKey[T],
      scope: TrackingScope[M],
      memoUpdater: M => M): TrackedNode[T] = {
    userNodeTracker.userTrackUpdate(key, scope, memoUpdater)
  }

  def executeBatchUpdate[R](cause: EventCause, update: DependencyTrackerBatchUpdater => R): R = {
    if (Settings.trackingScenarioConcurrencyDebug && scenarioReference == ScenarioReference.Root) {
      log.warn(
        "Running update action against the Root DependencyTracker. This could negatively affect concurrency. " +
          s"Cause($cause), Update($update)")
    }
    queue.execute(new TSA_BatchUpdate(cause, update, Scheduler.currentOrDefault))
  }

  /** evaluate node key in sync */
  def evaluateNodeKey[T](key: NodeKey[T], cause: EventCause): T = queue.execute(new EvaluateNodeKeyAction(key, cause))

  /** evaluate node keys in sync */
  def evaluateNodeKeys(keys: collection.Seq[NodeKey[_]], cause: EventCause): Unit =
    queue.execute(new EvaluateNodeKeysAction(keys, cause))

  /** Generally pass through, but maintain order */
  @closuresEnterGraph
  def executeEvaluate[T](expr: () => T, highPriority: Boolean = false): T =
    queue.execute(new TSA_Evaluate(expr), highPriority = highPriority)

  /**
   * Add tweaks to the underlying ScenarioStack. This cannot be called within a TSA_Evaluate action (such as those
   * created by executeEvaluate) or else it will deadlock.
   *
   * @param tweaks
   *   Tweaks to add.
   * @param throwOnDuplicate
   *   Whether to fail if there are any duplicates.
   */
  def addTweaks(tweaks: Iterable[Tweak], throwOnDuplicate: Boolean = false): Unit = {
    if (tweaks.nonEmpty) queue.execute(new TSA_AddTweaks(tweaks, throwOnDuplicate))
  }

  /** test helpers only */
  private[tracking] def removeTweak(nk: NodeKey[_]): Unit = queue.execute(new TSA_RemoveTweak(nk :: Nil))

  private[tracking] def removeTweakByEntity(entity: Entity, nk: Option[NodeKey[_]]): Unit = {
    val nks = nk.map(Seq(_)).getOrElse(Seq.empty[NodeKey[_]])
    queue.execute(new TSA_RemoveTweakByEntity(entity, nks))
  }

  private[tracking] def removeAllTweaks(): Unit = queue.execute(new TSA_RemoveAllTweaks())

  /**
   * Dispose this tracking scenario, its underlying scenario, and all of its tracking information
   */
  def dispose(clearCacheOfDisposedNodes: Boolean = true, immediate: Boolean = false): Unit = {
    // Note - we execute the dispose on root queue as Dispose can modify the parent and root view of child scenarios
    // keep this in step with disposeAsync
    root.queue.execute(new TSA_Dispose(clearCacheOfDisposedNodes, immediate))
  }

  /**
   * Set the underlay scenario. See documentation on SetUnderlayGesture.
   */
  private[optimus] def setUnderlay(scenario: Scenario): Unit = queue.execute(new TSA_SetUnderlay(scenario))
}
