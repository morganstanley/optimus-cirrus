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

import java.util.concurrent.ConcurrentHashMap
import optimus.graph.OGSchedulerContext
import optimus.graph._
import optimus.graph.tracking.DependencyTrackerRoot.TweakLambda
import optimus.graph.tracking.handler.OutOfScopeScenarioReference
import optimus.platform._
import optimus.platform.annotations.closuresEnterGraph
import optimus.platform.storable.Entity
import optimus.ui.ScenarioReference

import java.util.concurrent.LinkedBlockingQueue
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * A [[DependencyTrackerBatchUpdater]] is the mechanism to allow a sequence of operations to be applied to a
 * [[DependencyTracker]]. A [[DependencyTrackerBatchUpdater]] is obtained from `DependencyTracker.executeBatchUpdate`
 * (or async version) in a specific [[DependencyTracker]], and that executeBatchUpdate should only access the
 * [[DependencyTracker]] via the APIs in [[DependencyTrackerBatchUpdater]] and [[DependencyMultiTrackerUpdater]].
 *
 * This API does not allow re-entrancy, or any parallelism, by design. This is to make the steps of the update lambda
 * simple and safe. All the internal parallelism of the graph is hidden by this API
 *
 * [[DependencyTrackerBatchUpdater]] is invoked on a specific [[DependencyTracker]] (let's call this the parent), and
 * may access any children of the parent via `updater`. A [[DependencyTrackerBatchUpdater]] obtained via updater may
 * still access any [[DependencyTrackerBatchUpdater]] that is a child of the original parent.
 *
 * All of the APIs in [[DependencyTrackerBatchUpdater]] target a specific [[DependencyTracker]], except `multiUpdater`
 * which provides APIs that can perform operations against several [[DependencyTracker]]s
 *
 * A [[DependencyTrackerBatchUpdater]] cannot be used after the command that created it has completed, and cannot target
 * a disposed scenario (with the normal issues that a ScenarioReference may re-create a [[DependencyTracker]]) The API
 * is not reentrant or parallel. This is to reduce to cost of tracking inter-operation of the command
 *
 * Note - all action APIs are suffixed with Immediate to make it obvious that we are using the API within this API not
 * enqueueing via [[DependencyTracker]] APIs
 */
sealed trait DependencyTrackerBatchUpdater {
  def target: DependencyTracker

  /**
   * provide a [[DependencyTrackerBatchUpdater]] for the specified [[DependencyTracker]] Multiple calls to `updaterFor`
   * for the same batch command referring to the same underlying [[DependencyTracker]] return the same instance
   *
   * @param ts
   *   the target for commands sent subsequently to the returned [[DependencyTrackerBatchUpdater]]. The supplied
   *   [[DependencyTracker]] must be a child of the original [[DependencyTrackerBatchUpdater]] that represents the
   *   command target for the batch of commands
   */
  def updaterFor(ts: DependencyTracker): DependencyTrackerBatchUpdater
  def updaterFor(ts: ScenarioReference): DependencyTrackerBatchUpdater

  /**
   * enqueue an evaluation of a NodeKey in the target scenario
   *
   * @param key
   *   the key of the node to calculate. Any current scenario is ignored
   * @return
   *   a PropertyNode that is enqueued, or may already be completed
   */
  def evaluateNodeKeyImmediate[T](
      key: NodeKey[T],
      removePendingInvalidate: Boolean = false,
      eventCause: Option[EventCause] = None,
      trackingScope: TrackingScope[_]): Try[T]

  def evaluateNodeKeysImmediate[T](
      keys: Iterable[NodeKey[T]],
      removePendingInvalidate: Boolean = false,
      eventCause: Option[EventCause] = None,
      trackingScope: TrackingScope[_]): List[Try[T]]

  def evaluateNodeKeysMapImmediate[T](
      keys: Iterable[NodeKey[T]],
      removePendingInvalidate: Boolean = false,
      eventCause: Option[EventCause] = None,
      trackingScope: TrackingScope[_]): Map[NodeKey[T], Try[T]]

  def addTweaksImmediate(tweaks: Seq[Tweak], throwOnDuplicate: Boolean = Settings.throwOnDuplicateInstanceTweaks): Unit

  def removeTweaksImmediate(keys: Iterable[NodeKey[_]]): Unit
  def removeAllTweaksImmediate(): Unit

  def removeTweaksForEntityInstanceWithKeysImmediate(
      entityInstance: Entity,
      keys: Seq[NodeKey[_]],
      excludes: Seq[NodeKey[_]]): Unit

  def setUnderlayImmediate(scenario: Scenario, cause: EventCause): Unit

  def setPluginTag[P](key: PluginTagKey[P], value: P): Unit

  def clearPluginTag[P](key: PluginTagKey[P]): Unit

  @closuresEnterGraph
  def evaluateImmediate[T](f: => T, modifySS: ScenarioStack => ScenarioStack = ss => ss): T

  def disposeImmediate(cacheClearMode: CacheClearMode): Unit

  def snapshotImmediate(): ScenarioStack

  def clearTracking(): Unit

  def multiUpdater: DependencyMultiTrackerUpdater

  // Returns the nearest DependencyTrackerUpdaterRoot - NB. this is not analogous to DependencyTracker.rootOfConsistentSubtree
  private[optimus] def consistentRoot: DependencyTrackerBatchUpdaterRoot
  private[optimus] def cause: EventCause = consistentRoot.cause
}

/**
 * To improve evaluation concurrency, we want to submit evaluations from a node completion callback if they need to run
 * on graph. The APIs in [[DependencyMultiTrackerEvaluator]] can be called from a different thread from which the
 * [[DependencyTrackerBatchUpdater]] is running in, so it needs to be concurrently safe.
 *
 * This is only extended by [[DependencyTrackerBatchUpdaterRoot]], and it's an abstraction of the evaluation APIs - as
 * we only want to expose the evaluation API to a node completion callback thread to avoid race condition.
 */
sealed trait DependencyMultiTrackerEvaluator {
  def evaluateNodeKeysAsync[T](
      keys: Map[ScenarioReference, Set[NodeKey[T]]],
      callback: EventCause => Unit = _ => {},
      removePendingInvalidate: Boolean = false,
      eventCause: EventCause = NoEventCause,
      trackingScope: TrackingScope[_]): Unit

  def evaluateAsync[T](
      scenarioReference: ScenarioReference,
      fn: => T,
      callback: (Try[T], EventCause) => Unit,
      eventCause: EventCause): Unit

  def evaluateNodeKeysLowPriorityAsync[T](
      keys: Map[ScenarioReference, Set[NodeKey[T]]],
      eventCause: Option[EventCause],
      trackingScope: TrackingScope[_]): Unit

  def evaluateNodeKeysImmediateInMultiScenario[T](
      keys: Map[ScenarioReference, Set[NodeKey[T]]],
      removePendingInvalidate: Boolean = false,
      eventCause: Option[EventCause] = None,
      trackingScope: TrackingScope[_]): Map[ScenarioReference, Map[NodeKey[T], Try[T]]]

  /**
   * Call this to make sure it's ok to run evaluation on this batch updater. This will increase the inflight counter to
   * make sure the batch updater wait until the evaluation finishes, and should always be called in pair with
   * [[DependencyMultiTrackerEvaluator.finishEvaluating()]] to decrease the counter
   * @return
   *   true if the batch updater not in updating stage or closed.
   */
  private[optimus] def safeToEvaluate: Boolean

  /**
   * Decrease the inflight counter - should always be called in pair with
   * [[DependencyMultiTrackerEvaluator.safeToEvaluate]]
   */
  private[optimus] def finishEvaluating(): Unit
}

sealed trait DependencyMultiTrackerUpdater extends DependencyMultiTrackerEvaluator {

  def addTweakMapImmediate(
      tweaks: Map[ScenarioReference, Seq[Tweak]],
      tweakLambdas: Map[ScenarioReference, Seq[TweakLambda]],
      throwOnDuplicate: Boolean = Settings.throwOnDuplicateInstanceTweaks): Unit

  /**
   * retrieves the nodes that are invalidated currently, and may not have been reported to an attached application. The
   * whole of the update operation is atomic WRT to the external application, but for instance we may want to add some
   * tweaks, invalidate a UI button and then run a calc, then re-enable the button. This API allows the UI to flush and
   * re-calculate the button state and inform the user as to the progress of the atomic action
   *
   * @param scope
   *   the scope that nodes were tracked with the request internally
   */
  def allInvalidatedNodes[M >: Null <: TrackingMemo](
      scope: TrackingScope[M]): Map[ScenarioReference, Map[NodeKey[_], M]]
}

private[tracking] sealed abstract class DependencyTrackerBatchUpdaterBase extends DependencyTrackerBatchUpdater {
  protected def logTimedCommand[T](name: String, details: => String)(fn: => T): T = {
    if (Settings.timeTrackingScenarioUpdateCommands) {
      val info = details
      val seq = consistentRoot.nextSeq()
      val start = System.nanoTime()
      DependencyTracker.log.info(s"batch command #$seq $name $info started")
      try fn
      finally {
        val end = System.nanoTime()
        consistentRoot.recordGraphTime(end - start)
        DependencyTracker.log.info(
          s"batch command #$seq $name $info completed and took ${(end - start) / 1000 / 1000.0} ms")
      }
    } else {
      fn
    }
  }

  final protected def beforeAccessTarget(forUpdate: Boolean): Unit = {
    consistentRoot.checkInvariants(allowAccessFromOtherGraphThreads = false)
    target.assertNotDisposed()
    if (forUpdate) consistentRoot.evaluationBarrier(forClose = false)
    if (consistentRoot.target.queue.currentCancellationScope.isCancelled)
      throw new GraphInInvalidState(s"${consistentRoot.target.queue}'s scope was already cancelled")
  }

  final protected def runUpdate[T](name: String, details: => String)(fn: => T): T = {
    try {
      beforeAccessTarget(forUpdate = true)
      logTimedCommand(name, details)(fn)
    } finally consistentRoot.finishUpdating()
  }

  override final def addTweaksImmediate(tweaks: Seq[Tweak], throwOnDuplicate: Boolean): Unit =
    multiUpdater.addTweakMapImmediate(Map(target.scenarioReference -> tweaks), Map.empty, throwOnDuplicate)

  override final def removeTweaksForEntityInstanceWithKeysImmediate(
      entityInstance: Entity,
      keys: Seq[NodeKey[_]] = Seq(),
      excludes: Seq[NodeKey[_]] = Seq()): Unit =
    runUpdate("removeTweaksForEntityInstanceWithKeys", s"entity: $entityInstance, keys: $keys, excludes: $excludes") {
      target.tweakContainer
        .doRemoveTweaksFromGivenEntity(entityInstance, keys, excludes, cause, consistentRoot.invalidationBatcher)
    }

  override final def removeTweaksImmediate(keys: Iterable[NodeKey[_]]): Unit =
    runUpdate("removeTweaksImmediate", keys.toString) {
      target.tweakContainer.doRemoveTweaks(keys, cause, consistentRoot.invalidationBatcher)
    }

  override final def removeAllTweaksImmediate(): Unit =
    runUpdate("removeAllTweaksImmediate", "") {
      target.tweakContainer.doRemoveAllTweaks(cause, consistentRoot.invalidationBatcher)
    }

  override final def setPluginTag[P](key: PluginTagKey[P], value: P): Unit =
    logTimedCommand("setPluginTag", s"key = $key, value = $value") { target.setPluginTag(key, value) }

  override final def clearPluginTag[P](key: PluginTagKey[P]): Unit = {
    logTimedCommand("clearPluginTag", key.toString) { target.clearPluginTag(key) }
  }

  override final def setUnderlayImmediate(scenario: Scenario, cause: EventCause): Unit =
    runUpdate("setUnderlayImmediate", scenario.toString) {
      target.doSetUnderlay(scenario, cause, consistentRoot.invalidationBatcher)
    }

  override final def evaluateNodeKeyImmediate[T](
      nk: NodeKey[T],
      removePendingInvalidate: Boolean,
      eventCause: Option[EventCause],
      trackingScope: TrackingScope[_]): Try[T] =
    evaluateNodeKeysImmediate(nk :: Nil, removePendingInvalidate, eventCause, trackingScope: TrackingScope[_]).head

  override def evaluateNodeKeysImmediate[T](
      keys: Iterable[NodeKey[T]],
      removePendingInvalidate: Boolean,
      eventCause: Option[EventCause],
      trackingScope: TrackingScope[_]): List[Try[T]] = {
    val res = evaluateNodeKeysMapImmediate(keys, removePendingInvalidate, eventCause, trackingScope)
    keys.iterator.map(res).toList
  }

  override def evaluateNodeKeysMapImmediate[T](
      keys: Iterable[NodeKey[T]],
      removePendingInvalidate: Boolean,
      eventCause: Option[EventCause],
      trackingScope: TrackingScope[_]): Map[NodeKey[T], Try[T]] = {
    beforeAccessTarget(forUpdate = false)
    val results = multiUpdater
      .evaluateNodeKeysImmediateInMultiScenario[T](
        Map(target.scenarioReference -> keys.toSet),
        removePendingInvalidate,
        eventCause,
        trackingScope)(target.scenarioReference)
    keys.iterator.map(key => key -> results(key)).toMap
  }

  @closuresEnterGraph
  override final def evaluateImmediate[T](f: => T, modifySS: ScenarioStack => ScenarioStack): T = {
    beforeAccessTarget(forUpdate = false)

    // grab the dependency tracker's scenario stack
    val ss = target.nc_scenarioStack
    val newSS = modifySS(ss.withCancellationScope(consistentRoot.target.queue.currentCancellationScope))

    logTimedCommand("evaluateImmediate", "") {
      // to replace target.given
      val node =
        new DependencyTrackerActionTask(DependencyTrackerActionTask.evaluateImm, cause, () => f)
      EvaluationContext.enqueueAndWait(
        node,
        newSS
      )
      node.result
    }
  }

  override final def disposeImmediate(cacheClearMode: CacheClearMode): Unit = try {
    consistentRoot.checkInvariants(allowAccessFromOtherGraphThreads = false)
    consistentRoot.evaluationBarrier(forClose = false)
    logTimedCommand("disposeImmediate", target.name) {
      target.doDispose(cacheClearMode, cause, consistentRoot.invalidationBatcher)
    }
  } finally consistentRoot.finishUpdating()

  override def snapshotImmediate(): ScenarioStack = {
    beforeAccessTarget(forUpdate = false)
    logTimedCommand("snapshotImmediate", target.name) { target.snapshotScenarioStack(consistentRoot.target) }
  }

  override def clearTracking(): Unit = runUpdate("clearTracking", target.name) {
    target.doClearTracking(consistentRoot.invalidationBatcher)
  }

  override def multiUpdater: DependencyMultiTrackerUpdater = consistentRoot
}

private[tracking] final class DependencyTrackerBatchUpdaterRoot(
    override val target: DependencyTracker,
    val command: DependencyTrackerActionUpdate[_],
    override val cause: EventCause)
    extends DependencyTrackerBatchUpdaterBase
    with DependencyMultiTrackerUpdater {

  target.setCurrentEvaluator(this)

  private var gen: Int = 0
  def nextSeq(): Int = {
    gen += 1
    gen
  }
  private val startTime: Long = System.nanoTime()
  private var graphTime: Long = 0L
  def recordGraphTime(ns: Long): Unit = graphTime += ns
  private def toMillis(ns: Long): Double = ns / 1000 / 1000.0

  def logTimingSummary(): Unit = {
    if (Settings.timeTrackingScenarioUpdateCommands)
      DependencyTracker.log.info(s"Batch update took ${toMillis(
          System.nanoTime() - startTime)} ms to execute $gen commands. Graph time was ${toMillis(graphTime)} ms")
  }

  private val initialNode = EvaluationContext.currentNode

  def consistentRoot: DependencyTrackerBatchUpdaterRoot = this

  private val updaters = new ConcurrentHashMap[DependencyTracker, DependencyTrackerBatchUpdater]
  override def updaterFor(ts: DependencyTracker): DependencyTrackerBatchUpdater = if (ts eq target) this
  else {
    assert(
      consistentRoot.target hasChild ts,
      s"root.target: (${consistentRoot.target.name}) doesn't have child: (${ts.name})")
    updaters.computeIfAbsent(ts, (ts: DependencyTracker) => new DependencyTrackerBatchUpdaterView(ts, consistentRoot))
  }
  override def updaterFor(ref: ScenarioReference): DependencyTrackerBatchUpdater =
    if (ref == target.scenarioReference) this else updaterFor(target.root.getOrCreateScenario(ref))

  private[tracking] val invalidationBatcher = new InvalidationCallbackBatcher(target.root, cause)

  private object StateManager {
    private var evaluating = 0
    private var updating = false
    private val pendingNodes = new LinkedBlockingQueue[NodeTask]()
    private[DependencyTrackerBatchUpdaterRoot] var closed = false

    /**
     * If the batch updater is already closed or in updating status, it cannot start evaluation. In practice, tryStart
     * should never be called if batch updater is in updating status, checkout
     * DependencyTrackerTester.testBatchUpdateWaitingForAllAsyncEvaluationsBeforeAddTweaks, but to avoid unexpected race
     * conditions we still check updating status here, since it's better to delay an evaluation than messing up
     * everything with tweaks and ttracks.
     *
     * This is really just a temporary thing - the [[DependencyTrackerBatchUpdater]] needs to be refactored.
     *
     * TODO (OPTIMUS-55550): Redesign DependencyTrackerQueue etc.
     *
     * @return
     *   true if we still hold evaluation lock and can start evaluation
     */
    private[DependencyTrackerBatchUpdaterRoot] def tryIncreaseEvaluating: Boolean = StateManager.synchronized {
      if (closed || updating) false
      else {
        evaluating += 1
        true
      }
    }

    private[DependencyTrackerBatchUpdaterRoot] def finishEvaluating(): Unit = StateManager.synchronized {
      evaluating -= 1
      if (evaluating < 0) {
        throw new GraphInInvalidState()
      }
    }

    def keepTrackOfNode(node: NodeTask): Unit =
      if (!node.isDone) pendingNodes.put(node)

    def keepTrackOfNodes(nodes: Seq[NodeTask]): Unit =
      nodes.foreach(n => if (!n.isDone) pendingNodes.put(n))

    private[graph] def finishUpdating(): Unit = StateManager.synchronized { updating = false }

    /**
     * Wait for incomplete evaluations and change stage to:
     *   - closed if forClose = true
     *   - updating if forClose = false
     *
     * Do the status transition in the StageManager lock to avoid race conditions
     *
     * @param forClose
     *   - true if this evaluationBarrier is before closing
     *   - false if this evaluationBarrier is before updating
     */
    private[graph] def waitForEvaluations(forClose: Boolean): Unit = {
      val context = OGSchedulerContext.current()
      var node: NodeTask = pendingNodes.poll()

      def ready: Boolean = StateManager.synchronized {
        // note that evaluating can be non-zero even when there are no pendingNodes because of calls to
        // tryIncreaseEvaluating via DependencyMultiTrackerEvaluator.safeToEvaluate
        // Also don't need to lock pendingNodes as evaluating is guaranteed to be > 0 during pendingNodes evaluations.
        val evalsDone = evaluating == 0
        if (evalsDone) {
          if (forClose) {
            target.clearCurrentEvaluator()
            closed = true
          } else {
            updating = true
          }
        }
        evalsDone
      }

      while ((node ne null) || !ready) {
        // No pending nodes, but spin in case more are added. Note that we can't wait here - we must check for newly
        // added pending nodes because we might need this thread to run them in order for evaluating count to reduce.
        if (node eq null) Thread.`yield`()
        // lend this thread to help progress pending node
        else if (!node.isDone) {
          context.runAndWait(node)
        }
        node = pendingNodes.poll()
      }

    }
  }

  override private[optimus] def safeToEvaluate: Boolean = StateManager.tryIncreaseEvaluating
  override private[optimus] def finishEvaluating(): Unit = StateManager.finishEvaluating()
  private[tracking] def finishUpdating(): Unit = StateManager.finishUpdating()

  /**
   * Waits for any async evaluations running in the background to finish.
   *
   * The rule is that a DependencyTrackerBatchUpdater may only be used from a single thread and almost all methods are
   * fully synchronous so we don't have to worry about any interleaving of updates and evaluates. The only exception is
   * asynchronous evaluations which remain running after the methods return to the caller. We need to make sure that an
   * update method doesn't start running (and that close() doesn't complete) while there are are still async evaluations
   * in the background.
   */
  private[tracking] def evaluationBarrier(forClose: Boolean): Unit = {
    val startTime = System.nanoTime()
    // need to wait for the callback (which runs after any user callbacks on those nodes, which is important
    // because all callbacks need to be complete before we start doing anything else, or else ordering can be incorrect)
    StateManager.waitForEvaluations(forClose)

    // and now that they are complete, we need to cancel any orphaned nodes (e.g. a + b, a throws an exception but b is
    // still running) so that they aren't still running when updates start
    target.queue.evaluationBarrier(cause)
    val duration = System.nanoTime() - startTime
    if (Settings.timeTrackingScenarioUpdateCommands && duration > 1000000)
      DependencyTracker.log.info(
        s"batch command #$gen waited ${toMillis(duration)} ms for previously scheduled " +
          "evaluateNodeKeysAsync batch(es) to complete")
  }

  /**
   * checks that the targeted [[DependencyTracker]] is not disposed, and the API is not being called in a parallel or
   * reentrant manner if allowAccessFromOtherThreads = false
   */
  private[tracking] def checkInvariants(allowAccessFromOtherGraphThreads: Boolean): Unit = {
    assert(!StateManager.closed)
    assert(
      (allowAccessFromOtherGraphThreads && (EvaluationContext.current ne null)) || (initialNode eq EvaluationContext.currentNode))
  }

  private[tracking] def close(): Unit = {
    evaluationBarrier(forClose = true)
    invalidationBatcher.complete()
    logTimingSummary()
  }

  override def allInvalidatedNodes[M >: Null <: TrackingMemo](
      scope: TrackingScope[M]): Map[ScenarioReference, Map[NodeKey[_], M]] = {
    logTimedCommand("allInvalidatedNodes", "") {
      consistentRoot.invalidationBatcher
        .currentBatches(scope)
        .collectFirst {
          case cb if cb.scope == scope =>
            cb.copyNodes().map { case (k, v) =>
              k.scenarioReference -> v.map { case (tracked, memo) =>
                tracked.key -> memo.asInstanceOf[M]
              }
            }
        }
        .getOrElse(Map.empty)
    }
  }

  override def addTweakMapImmediate(
      tweaks: Map[ScenarioReference, Seq[Tweak]],
      tweakLambdas: Map[ScenarioReference, Seq[TweakLambda]],
      throwOnDuplicate: Boolean): Unit =
    runUpdate("addTweakMapImmediate", tweaks.keys.toString()) {
      assertSafeToUpdate(tweaks.keys ++ tweakLambdas.keys)
      val tweaksByScenario = toScenario(tweaks)
      val tweakLambdasByScenario = toScenario(tweakLambdas)
      UpdateMultipleScenarios
        .evaluateAndApplyTweakLambdas(
          tweaksByScenario,
          tweakLambdasByScenario,
          consistentRoot.target,
          cause,
          throwOnDuplicate,
          invalidationBatcher)
    }

  private def causePrefix: String = s"[Graph][DependencyTrackerBatchUpdater-${target.scenarioReference}]"

  /**
   * Checks that all of the supplied scenario references are safe to update in the current updater
   * @param scenarioReferences
   *   the scenarioReferences
   */
  private def assertSafeToUpdate(scenarioReferences: Iterable[ScenarioReference]): Unit = {
    val safeParent = target.scenarioReference.rootOfConsistentSubtree
    scenarioReferences foreach { ref =>
      if (!ref.isSelfOrChildOf(safeParent)) throw new OutOfScopeScenarioReference(safeParent, ref)
    }
  }

  /**
   * Checks that all of the supplied scenario references are safe to read in the current updater
   * @param scenarioReferences
   *   the scenarioReferences
   */
  private def assertSafeToRead(scenarioReferences: Iterable[ScenarioReference]): Unit = {
    val safeParent = target.scenarioReference.rootOfConsistentSubtree
    scenarioReferences foreach { ref =>
      assert(
        ref.isSelfOrChildOf(safeParent) || ref.isParentOf(safeParent),
        s"not safe to read $ref when locked ${target.scenarioReference}"
      )
    }
  }
  private def toScenario[T](byRef: Map[ScenarioReference, T]): Map[DependencyTracker, T] = {
    byRef map { case (k, v) =>
      toScenario(k) -> v
    }
  }
  private def toScenario(byRef: ScenarioReference): DependencyTracker = target.root.getOrCreateScenario(byRef)

  private def toTry[T](completedNode: Node[T]): Try[T] = {
    if (completedNode.isDoneWithException) Failure(completedNode.exception())
    else Success(completedNode.result)
  }

  private val parentTrackers = new ConcurrentHashMap[ScenarioReference, DependencyTracker]
  private def safeEvaluationDependencyTracker(scenarioRef: ScenarioReference): DependencyTracker = {
    parentTrackers.computeIfAbsent(
      scenarioRef,
      { ref =>
        if (ref.isParentOf(target.scenarioReference)) target.root.getOrCreateScenario(ref)
        else if (ref.isSelfOrChildOf(target.scenarioReference)) updaterFor(ref).target
        else throw new Exception(s"scenarioRef $ref is neither a parent nor a child of ${target.scenarioReference}")
      }
    )
  }

  private def attachNodes[T](
      nodes: Map[ScenarioReference, Set[NodeKey[T]]],
      ec: OGSchedulerContext,
      eventCause: Option[EventCause],
      trackingScope: TrackingScope[_]): Map[ScenarioReference, Map[NodeKey[T], Node[T]]] = {
    // note that we use the cancel scope of the tracker that the batch update is running in, not the
    // (potentially stale) cancel scope of whatever child we're evaluating in
    val cs = consistentRoot.target.queue.currentCancellationScope
    val triggerCause = eventCause.getOrElse(cause)
    nodes.map { case (scenarioRef, keys) =>
      val tracker = safeEvaluationDependencyTracker(scenarioRef)
      scenarioRef -> keys.iterator.map { key =>
        key -> tracker.nodeOf(key.tidyKey, ec, triggerCause, trackingScope, cs).asInstanceOf[Node[T]]
      }.toMap
    }
  }

  private def removePendingInvalidations[T](
      nodes: Map[ScenarioReference, Set[NodeKey[T]]],
      trackingScope: TrackingScope[_]): Unit = {
    val invalidated = invalidationBatcher.currentBatches(trackingScope)
    invalidated
      .filter { _.scope == trackingScope }
      .foreach { batch =>
        batch.removeMatching { pendingInvalidate =>
          nodes
            .getOrElse(pendingInvalidate.scenarioReference, Set.empty)
            .contains(pendingInvalidate.key.asInstanceOf[NodeKey[T]])
        }
      }
  }

  override def evaluateNodeKeysImmediateInMultiScenario[T](
      keys: Map[ScenarioReference, Set[NodeKey[T]]],
      removePendingInvalidate: Boolean,
      eventCause: Option[EventCause],
      trackingScope: TrackingScope[_]): Map[ScenarioReference, Map[NodeKey[T], Try[T]]] = {
    assertSafeToRead(keys.keys)

    logTimedCommand("evaluateNodeKeysImmediate", s"nodes = $keys removeFrom = $removePendingInvalidate") {
      val ec = OGSchedulerContext.current()
      val nodesAttached = attachNodes(keys, ec, eventCause, trackingScope)
      EvaluationContext.enqueueAndWaitForAll(nodesAttached.iterator.flatMap(_._2.values).toIndexedSeq)
      if (removePendingInvalidate) removePendingInvalidations(keys, trackingScope)
      nodesAttached.map { case (scenarioRef, nodes) =>
        val res: Map[NodeKey[T], Try[T]] = nodes.iterator.map { node =>
          node._1.tidyKey.asInstanceOf[NodeKey[T]] -> toTry(node._2)
        }.toMap
        scenarioRef -> res
      }
    }
  }

  override def evaluateNodeKeysAsync[T](
      keys: Map[ScenarioReference, Set[NodeKey[T]]],
      callback: EventCause => Unit = _ => {},
      removePendingInvalidate: Boolean = false,
      eventCause: EventCause,
      trackingScope: TrackingScope[_]): Unit = {
    checkInvariants(allowAccessFromOtherGraphThreads = true)
    assertSafeToRead(keys.keys)
    logTimedCommand("evaluateNodeKeysAsync", s"nodes = $keys removeFrom = $removePendingInvalidate") {
      if (StateManager.tryIncreaseEvaluating) {
        val evalCause = eventCause.createChild(s"$causePrefix evaluateNodeKeysAsync")
        val token = evalCause.createAndTrackToken()
        val ec = EvaluationContext.current
        if (removePendingInvalidate) removePendingInvalidations(keys, trackingScope)
        val nodesAttached = attachNodes(keys, ec, Some(evalCause), trackingScope)
        val nodesToRun = nodesAttached.valuesIterator.flatMap(_.valuesIterator).toSeq
        StateManager.keepTrackOfNodes(nodesToRun)
        // important: we run the callback inline since we may (briefly) block the current scheduler thread while waiting
        // in StateManager.await(), which could cause deadlock if no other graph threads were available
        ec.scheduler.evaluateNodesAsync(
          nodesToRun,
          _ => {
            try { callback(evalCause) }
            finally {
              StateManager.finishEvaluating()
              token.release()
            }
          },
          callbackInline = true
        )
      } else {
        val msg = "evaluateNodeKeysAsync failure: BatchUpdater already closed or is updating!"
        DependencyTracker.log.error(msg, new GraphInInvalidState(msg))
      }
    }
  }

  override def evaluateAsync[T](
      scenarioReference: ScenarioReference,
      fn: => T,
      callback: (Try[T], EventCause) => Unit,
      eventCause: EventCause): Unit = {
    checkInvariants(allowAccessFromOtherGraphThreads = true)
    logTimedCommand("evaluateAsync", s"fn = $fn") {
      if (StateManager.tryIncreaseEvaluating) {
        val evalCause = eventCause.createChild(s"$causePrefix evaluateAsync")
        val token = evalCause.createAndTrackToken()
        val ec = EvaluationContext.current
        val tracker = safeEvaluationDependencyTracker(scenarioReference)
        // use the cancellation scope from the updater root, child dependency tracker doesn't have their own cancellation scope
        val cs = target.queue.currentCancellationScope
        // Need to use the non cached scenarioStack as fn can call non-RT functions.
        val ss = tracker.nonCachedScenarioStackWithCancelScope(cs)
        val cb = (result: Try[T]) => {
          try { callback(result, evalCause) }
          finally {
            StateManager.finishEvaluating()
            token.release()
          }
        }
        val node: Node[T] = new PropertyNodeSync[T] {
          override def entity: Entity = null
          override def run(ec: OGSchedulerContext): Unit = completeWithResult(fn, ec)
          override def propertyInfo: NodeTaskInfo = NodeTaskInfo.UITrackDebug
          override def invalidateCache(observer: TrackedNodeInvalidationObserver): Unit = {
            super.invalidateCache(observer)
            OverInvalidationDetection.checkOverInvalidationInDebugTrack(evalCause, getId, tracker.root)
          }
        }
        node.attach(ss)
        evalCause.root.keepAliveWhileEvaluating(node)
        StateManager.keepTrackOfNode(node)
        // need to run callback inline, otherwise there will deadlock
        ec.scheduler.evaluateNodeAsync(node, maySAS = true, trackCompletion = false, cb, callbackInline = true)
      } else {
        val msg = "evaluateAsync failure: BatchUpdater already closed or is updating!"
        DependencyTracker.log.error(msg, new GraphInInvalidState(msg))
      }
    }
  }

  override def evaluateNodeKeysLowPriorityAsync[T](
      keys: Map[ScenarioReference, Set[NodeKey[T]]],
      cause: Option[EventCause],
      trackingScope: TrackingScope[_]): Unit = {
    assertSafeToRead(keys.keys)
    logTimedCommand("evaluateNodeKeysLowPriorityAsync", s"nodes = $keys") {
      keys.foreach { case (sr, keys) =>
        safeEvaluationDependencyTracker(sr)
          .evaluateNodeKeysLowPriorityAsync(keys.toList, _ => (), cause getOrElse this.cause, trackingScope)
      }
    }
  }
}

private[tracking] final class DependencyTrackerBatchUpdaterView(
    override val target: DependencyTracker,
    val consistentRoot: DependencyTrackerBatchUpdaterRoot)
    extends DependencyTrackerBatchUpdaterBase {
  override def updaterFor(ts: DependencyTracker): DependencyTrackerBatchUpdater = consistentRoot.updaterFor(ts)

  override def updaterFor(ts: ScenarioReference): DependencyTrackerBatchUpdater = {
    if (!ts.isSelfOrChildOf(consistentRoot.target.scenarioReference))
      throw new OutOfScopeScenarioReference(consistentRoot.target.scenarioReference, ts)
    updaterFor(consistentRoot.target.root.getOrCreateScenario(ts))
  }

  override private[optimus] def cause = consistentRoot.cause

  override def toString: String =
    s"DependencyTrackerBatchUpdaterView(target = ${target.scenarioReference}, root = ${consistentRoot.target.scenarioReference})"
}
