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
import optimus.graph._
import optimus.graph.diagnostics.pgo.Profiler
import optimus.graph.tracking.ttracks.TTrackStatsActions
import optimus.graph.tracking.ttracks.TweakableTracker
import optimus.platform.util.PrettyStringBuilder
import optimus.platform._
import optimus.ui.ScenarioReference

import java.util.concurrent.ConcurrentHashMap
import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.Success
import java.util.function

/**
 * Metadata object associated with a Scenario, which is capable of tracking dependencies amongst Nodes and propagating
 * invalidations in response to Tweaks which are added to the underlying Scenario. This is also capable of tracking
 * arbitrary user information (UTracks).
 *
 * DependencyTrackers share many of the same operations as Scenarios, but they are not Scenarios. They cannot be used as
 * arguments to given, for example.
 *
 * Historical note: DependencyTracker used to be called TrackingScenario and was in fact a subclass of Scenario.
 *
 * @param rootParam
 *   The root of a sequence of nested DependencyTrackers including this one.
 * @param scenarioReference
 *   A reference to this tracker from UI
 * @param parentScenarioStack
 *   The Scenario stack onto which to add the underlying Scenario for this DependencyTracker.
 */
private[optimus] class DependencyTracker private[tracking] (
    rootParam: DependencyTrackerRoot,
    private[optimus] val scenarioReference: ScenarioReference,
    protected[optimus] val parentScenarioStack: ScenarioStack,
    appletName: String)
    extends DependencyTrackerChildren
    with TTrackStatsActions
    with DependencyTrackerSyncApi
    with DependencyTrackerAsyncApi
    with SnapshotSupport
    with UnderlayScenarioSupport
    with DisposalSupport
    with PluginTagSupport {

  import DependencyTracker.log

  private[optimus] def root: DependencyTrackerRoot = rootParam
  assert(root ne null)

  private[optimus] val parentDependencyTracker: Option[DependencyTracker] =
    parentScenarioStack.tweakableListener match {
      case t: TweakableTracker => Some(t.owner)
      case _                   => None
    }

  def name: String = scenarioReference.name

  private[optimus] val parent: DependencyTracker = parentDependencyTracker.orNull

  private[tracking] val nodeInputs: MutableNodeInputMap =
    if (parent eq null) new MutableNodeInputMap(Map.empty, parentScenarioStack.siParams.nodeInputs)
    else new MutableNodeInputMap(Map.empty, parent.nodeInputs)

  protected[tracking] val queue: DependencyTrackerQueue =
    if (scenarioReference.shareParentQueue) parent.queue // we know shareParentQueue is false if isRoot
    else new DependencyTrackerQueue(this)

  private[optimus] def queueOwner = queue.tracker

  /** tracks tweakable usage in the underlay scenario stack(s) */
  private[optimus] final val underlayTweakableTracker: TweakableTracker =
    new TweakableTracker(
      this,
      s"$name[underlay]",
      parentScenarioStack.trackingDepth + 1,
      queue,
      root,
      root.timedScheduler,
      { nodeKey: TweakableKey =>
        // we don't propagate invalidations if the key is already instance tweaked (but we do if it's property tweaked
        // since it's difficult to work out whether a property tweak applies to a key or not, and such tweaks are rare)
        if (
          (nodeKey eq null) || !scenarioStack._cacheID
            .containsInstanceTweak(nodeKey)
        ) { tweakableTracker :: Nil }
        else Nil
      },
      parentScenarioStack.tweakableListener,
      // clear all tracking within our consistent subtree (because of overlays it's not sufficient to only clear us
      // and our own children)
      onInvalidateAll = queueOwner.doClearTracking _
    )

  /** tracks tweakable usage in the mutable scenario stack */
  private[optimus] final val tweakableTracker: TweakableTracker =
    new TweakableTracker(
      this,
      name,
      parentScenarioStack.trackingDepth + 2,
      queue,
      root,
      root.timedScheduler,
      { _: TweakableKey =>
        children.map { _.underlayTweakableTracker }
      },
      underlayTweakableTracker,
      onInvalidateAll = queueOwner.doClearTracking _
    )

  final def isRoot: Boolean = root eq this

  private[optimus] final val tweakContainer: TweakContainer = {
    if (scenarioReference.rejectTweaks) new ImmutableTweakContainer(root, scenarioReference)
    else
      new MutableTweakContainer(
        root,
        scenarioReference,
        cs => scenarioStackWithCancelScope(cs.getOrElse(queue.currentCancellationScope)),
        tweakableTracker,
        this)
  }

  private def newScenarioStack(cached: Boolean, nodeInputs: MutableNodeInputMap): ScenarioStack =
    parentScenarioStack
      .createChildForTrackingScenario(name, tweakableTracker, tweakContainer.cacheId, cached, nodeInputs)
      .withScopePath(appletName)

  private[optimus] final val scenarioStack: ScenarioStack = newScenarioStack(cached = true, nodeInputs)
  final val scenarioState: ScenarioState = scenarioStack.asScenarioState

  private[tracking] def snapshotConsistentSubtree: SnapshotScenarioStack = {
    val childSnapshots = mutable.ArrayBuffer[SnapshotScenarioStack]()
    val childIt = children.iterator
    while (childIt.hasNext) {
      val child = childIt.next()
      if (!child.scenarioReference.introduceConcurrentSubtree) // snapshot consistent children as they can be overlayed
        childSnapshots += child.snapshotConsistentSubtree
    }
    val scenario = scenarioStack.nestScenariosUpTo(parentScenarioStack)
    val tags = scenarioStack.siParams.nodeInputs.freeze
    SnapshotScenarioStack(scenarioReference, scenario, childSnapshots.toArray, tags)
  }

  /**
   * A clone of scenarioStack with transitively cachable set to false.
   */
  final val nc_scenarioStack: ScenarioStack = newScenarioStack(cached = false, nodeInputs)

  private val userNodeTrackers = new ConcurrentHashMap[TrackingScope[TrackingMemo], UserNodeTracker[TrackingMemo]]()
  private def createUserNodeTracker: function.Function[TrackingScope[TrackingMemo], UserNodeTracker[TrackingMemo]] =
    (scope: TrackingScope[TrackingMemo]) => new UserNodeTracker(this, scope)
  final def userNodeTracker[M >: Null <: TrackingMemo](scope: TrackingScope[M]): UserNodeTracker[M] =
    userNodeTrackers
      .computeIfAbsent(scope.asInstanceOf[TrackingScope[TrackingMemo]], createUserNodeTracker)
      .asInstanceOf[UserNodeTracker[M]]
  final def allUserNodeTrackers: Iterable[UserNodeTracker[_]] = userNodeTrackers.values.asScala

  if (NodeTrace.profileSSUsage.getValue) Profiler.t_sstack_usage(scenarioStack, this, hit = false)

  /**
   * Record the current [[DependencyMultiTrackerEvaluator]] running from this [[DependencyTracker]], so we can access it
   * from the node completion callbacks.
   *
   * We need a better way to propagate the evaluation API to node completion callbacks.
   *
   * TODO (OPTIMUS-55550): Redesign DependencyTrackerQueue etc.
   */
  @volatile private var currentEvaluator: DependencyMultiTrackerEvaluator = _

  private[tracking] def setCurrentEvaluator(evaluator: DependencyMultiTrackerEvaluator): Unit =
    currentEvaluator = evaluator
  private[tracking] def clearCurrentEvaluator(): Unit = currentEvaluator = null

  /**
   * @return
   */
  private[optimus] def getCurrentEvaluator: Option[DependencyMultiTrackerEvaluator] = {
    Option(currentEvaluator).filter(_.safeToEvaluate).orElse(if (parent ne null) parent.getCurrentEvaluator else None)
  }

  // always need to install our underlay so that it starts tracking (nobody is yet tracking nodes here, so noop observer is ok)
  doSetUnderlay(
    Scenario.empty,
    TrackingActionEventCause("DependencyTracker.<init>"),
    NoOpTrackedNodeInvalidationObserver)

  // there is no cancellation scope installed at initial time
  @volatile private[this] var scenarioStackWithCancelScopeCache = scenarioStack
  private[tracking] def scenarioStackWithCancelScope(scope: CancellationScope): ScenarioStack = {
    val cached = scenarioStackWithCancelScopeCache
    if (cached.cancelScope eq scope) cached
    else {
      val fresh = scenarioStack.withCancellationScope(scope)
      // volatile write (needed since the initialization of SS in withCancellationScope doesn't have a write barrier)
      scenarioStackWithCancelScopeCache = fresh
      fresh
    }
  }

  // Need another for non cached scenario stack used for evaluating non-RT functions.
  // This is duplicative; we should revisit the scenarioStack with combination of cached / cancellation scope
  @volatile private[this] var nc_scenarioStackWithCancelScopeCache = nc_scenarioStack
  private[tracking] def nonCachedScenarioStackWithCancelScope(scope: CancellationScope): ScenarioStack = {
    val cached = nc_scenarioStackWithCancelScopeCache
    if (cached.cancelScope eq scope) cached
    else {
      val fresh = nc_scenarioStack.withCancellationScope(scope)
      // volatile write (needed since the initialization of SS in withCancellationScope doesn't have a write barrier)
      nc_scenarioStackWithCancelScopeCache = fresh
      fresh
    }
  }

  final private[tracking] def nodeOf[A](
      key: NodeKey[A],
      ec: OGSchedulerContext,
      cause: EventCause,
      trackingScope: TrackingScope[_],
      cancelScope: CancellationScope = queue.currentCancellationScope): PropertyNode[A] = {
    val utrk = userNodeTracker(trackingScope.asInstanceOf[TrackingScope[TrackingMemo]]).getUTrack(key)
    val ss = scenarioStackWithCancelScope(cancelScope)
    val r = ss.getNode(key.prepareForExecutionIn(ss), ec).asInstanceOf[PropertyNode[A]]
    if (utrk ne null) {
      if (Settings.trackingScenarioLoggingEnabled) DependencyTrackerLogging.requestUtrack(name, key)
      val tn = utrk.registerRunningNode(r, cause)
      OGTrace.dependency(ec.getCurrentNodeTask, tn, ec)
    }
    r
  }

  override def equals(other: Any): Boolean = this eq other.asInstanceOf[AnyRef]
  override def hashCode: Int = System.identityHashCode(this)

  /**
   * An action to evaluate the Nodes referenced by a set of NodeKeys. This is rather inefficient, as it has to look up
   * each NodeKey.
   *
   * @param nodeKeys
   *   The NodeKeys referencing the Nodes to evaluate.
   */
  private[tracking] sealed class EvaluateNodeKeysAction(
      val nodeKeys: Seq[NodeKey[_]],
      eventCause: EventCause,
      trackingScope: TrackingScope[_])
      extends DependencyTrackerActionEvaluateBase[Unit]
      with DependencyTrackerActionManyNodesBase
      with InScenarioAction {
    if (nodeKeys.isEmpty) log.warn("inefficient - no keys")

    // only a lazy val can override a lazy val or we wouldn't need this at all
    override lazy val cause: EventCause = eventCause.createChild("EvaluateNodeKeysAction")
    assert(cause ne null)

    lazy val nodes: Seq[Node[_]] = {
      val ec = EvaluationContext.current
      if (disposed) Nil
      else nodeKeys.map { nodeOf(_, ec, cause, trackingScope, cancelScope) }
    }
    override protected def asNodes: Seq[NodeTask] = nodes
    protected[this] def cancelScope: CancellationScope = queue.currentCancellationScope

    override def writeWorkInfo(sb: PrettyStringBuilder): Unit = {
      // use eventCause other than cause here since it gives more information about what triggered this action
      sb.append(s"${getClass.getSimpleName} - Cause: $eventCause; Keys:")
      sb.appendln("")
      sb.indent()
      nodeKeys.foreach(nk => sb.appendln(nk.toDebugString))
      sb.unIndent()
    }
    override def toString = s"${getClass.getSimpleName} [${nodeKeys mkString ", "}]"
  }

  /**
   * Evaluates the provided nodes, but does so:
   *   - on the low-priority queue, so anything but cleanup actions runs in preference to this
   *   - with preemption semantics, so any update action causes this action to be cancelled and re-enqueued
   */
  private[tracking] final class EvaluateNodeKeysLowPriorityAction private (
      nodeKeys: Seq[NodeKey[_]],
      cause: EventCause,
      trackingScope: TrackingScope[_],
      attempt: Int
  ) extends EvaluateNodeKeysAction(nodeKeys, cause, trackingScope)
      with DependencyTrackerActionCancellable {
    def this(nodeKeys: Seq[NodeKey[_]], cause: EventCause, trackingScope: TrackingScope[_]) =
      this(nodeKeys, cause, trackingScope, attempt = 0)

    // cancel() and afterAction() can race so we synchronize so the token is not released (by super.afterAction)
    // while cancel() is potentially using the underlying eventcause. Note that if afterAction() starts before cancel(),
    // this must have been because all nodes were already complete, in which case cancel() doesn't use the eventcause.
    override def afterAction(): Unit = synchronized { super.afterAction() }

    // lazy! because the currentCancellationScope is the one that is cancelled as part of the evaluationBarrier
    // so we need to wait until this one is re-enqueued after the update action
    override lazy val cancelScope: CancellationScope = queue.currentCancellationScope.childScope()

    // cancel() needs to run at most once (so that we don't submit multiple retries unnecessarily), and we can't rely on
    // just checking if cancelScope was cancelled, because it might have been cancelled by its parent rather than by us
    private var actionCancelled: Boolean = false

    // synchronized so that it doesn't run concurrently with afterAction()
    override def cancel(): Option[DependencyTrackerActionCancellable] = synchronized {
      if (!actionCancelled) {
        actionCancelled = true
        // Now issue the cancellation. It's possible that, at some point before this takes place, all the
        // nodes finish before being cancelled (for instance, if the only running nodes are the ones we passed in,
        // and they make no more node calls, so the scheduler never gets a chance to cancel.)
        cancelScope.cancelInternal(new CancelPreemptedInFlightLowPriorityNodes)
        scheduler.waitForCancelledNodesToStop(cancelScope)
        // All nodes here should be done (result/exception), cancelled (done with CPIFLPN above), or not yet running
        // not-yet-running nodes will start and immediately get cancelled
        // isDoneWithUsableResult excludes cancelled nodes (and those with non-RT exceptions, but that's fine)
        val incompleteNodes = nodes.filterNot(_.isDoneWithUsableResult)
        val reschedule =
          if (incompleteNodes.isEmpty) { // lucky us! the nodes actually completed before we got the cancel request
            log.info(s"All nodes were completed before cancellation of $this")
            None
          } else {
            // This is the expected case: we've got to try again.
            log.info(s"Rescheduling ${incompleteNodes.size} preempted nodes")
            log.debug(s"Cancelled $incompleteNodes and will reschedule")
            val newNodeKeys = incompleteNodes.map(_.asInstanceOf[PropertyNode[_]].tidyKey) // make sure they're "fresh"
            Some(new EvaluateNodeKeysLowPriorityAction(newNodeKeys, cause, trackingScope, attempt = attempt + 1))
          }
        reschedule
      } else None
    }
    override def toString = s"${super.toString()} (attempt $attempt)"
  }

  /**
   * Action to evaluate the Node corresponding to a given NodeKey.
   *
   * @param nodeKey
   *   The NodeKey whose Node should be evaluated.
   * @tparam T
   *   The result type.
   */
  private[tracking] class EvaluateNodeKeyAction[T](
      val nodeKey: NodeKey[T],
      val eventCause: EventCause,
      trackingScope: TrackingScope[_])
      extends DependencyTrackerActionEvaluateOneNodeBase[T]
      with InScenarioAction {
    override lazy val cause: EventCause = eventCause.createChild("EvaluateNodeKeyAction")
    assert(cause ne null)

    /**
     * Get the Node for nodeKey.
     *
     * @return
     *   The node to be evaluated.
     */
    protected override def asNode: PropertyNode[T] = nodeOf(nodeKey, EvaluationContext.current, cause, trackingScope)

    override def writeWorkInfo(sb: PrettyStringBuilder): Unit = {
      sb.append(s"${getClass.getSimpleName} - key:")
      sb.appendln("")
      sb.indent()
      sb.appendln(nodeKey.toDebugString)
      sb.unIndent()
    }
  }

  protected trait InScenarioAction {
    protected final def disposed: Boolean = isDisposed
    protected final def targetScenarioName: String = name
  }

  /**
   * Action to evaluate a given expression, but in the non-cacheable ScenarioStack.
   *
   * @param expr
   *   The function to nodify and evaluate as the action.
   * @tparam T
   *   The result type.
   */
  private[tracking] class TSA_Evaluate[T](expr: () => T)
      extends DependencyTrackerActionEvaluate(expr)
      with InScenarioAction {
    override def applyInScenarioStack: ScenarioStack = {
      val cs = queue.currentCancellationScope
      assert(!cs.isCancelled)
      nc_scenarioStack.withCancellationScope(cs)
    }
  }

  private[tracking] class TSA_BatchUpdate[R](
      eventCause: EventCause,
      update: DependencyTrackerBatchUpdater => R,
      protected override val scheduler: Scheduler)
      extends DependencyTrackerActionUpdate[R]
      with InScenarioAction {
    override lazy val cause: EventCause = eventCause.createChild(s"[Graph][DependencyTracker-$name] TSA_BatchUpdate")

    override protected def doUpdate(): R = {
      val tracker = root.getOrCreateScenario(scenarioReference.rootOfConsistentSubtree)
      val updater = new DependencyTrackerBatchUpdaterRoot(tracker, this, cause)
      try update(updater.updaterFor(scenarioReference))
      finally {
        updater.close()
      }
    }

    override def writeWorkInfo(sb: PrettyStringBuilder): Unit = {
      // use eventCause other than cause here since it gives more information about what triggered this action
      sb.appendln(s"${getClass.getSimpleName} - Cause: $eventCause")
    }
  }

  private[tracking] class TSA_BatchUpdateIgnoreDisposed(
      eventCause_ : EventCause,
      update_ : DependencyTrackerBatchUpdater => Unit,
      scheduler_ : Scheduler)
      extends TSA_BatchUpdate[Unit](eventCause_, update_, scheduler_) {

    override def alreadyDisposedResult: Try[Unit] = Success(())
  }

  override def toString: String = {
    val sb = new PrettyStringBuilder
    sb.append("DependencyTracker ")
    sb.append(name)
    sb.startBlock()
    if (isDisposed) sb.append(" *** disposed ***")
    else {
      sb.appendln(s"utracks.size = ${allUserNodeTrackers.map(_.utracks.size).sum}")
      sb.appendln(s"ttracks.size = ${tweakableTracker.ttracks.size()}")
      /*for (trk <- utracks.values()) {
      sb.appendln(trk.toString)
    }
    sb.endBlock
    sb.append("Dependencies")
    sb.startBlock
    for (trk <- ttracks.entrySet()) {
      sb.append(trk.getKey())
      sb.append(" -> ")
      sb.append(trk.getValue())
    }*/
    }
    sb.endBlock
    sb.toString
  }
  private[tracking] val level: Int = if (parent eq null) 0 else parent.level + 1
  private[tracking] def parentOrSelfAtLevel(aLevel: Int): DependencyTracker = {
    if (aLevel > level)
      throw new IllegalStateException(s"cant get a parent at level $aLevel when we are at level $level")
    var current = this
    while (current.level > aLevel) current = current.parent
    current
  }

  /**
   * Clears all dependency tracking and invalidates all user tracked nodes (typically causing user to request
   * recomputation)
   */
  final private[tracking] def doClearTracking(observer: TrackedNodeInvalidationObserver): Unit = {
    tweakableTracker.clearAllTracking()
    underlayTweakableTracker.clearAllTracking()
    userNodeTrackers.values().forEach(_.invalidateAll(observer))
    children.foreach(_.doClearTracking(observer))
  }

  protected def resetAllTraversalIds(newId: Int): Unit = {
    tweakableTracker.resetTraversalIds(newId)
    underlayTweakableTracker.resetTraversalIds(newId)
    children.foreach(_.resetAllTraversalIds(newId))
  }

  private[tracking] def notifyQueueIdle(lastAction: DependencyTrackerAction[_]): Unit = ()

  /**
   * Get work count for this tracker, but not its children.
   */
  def getWorkCount: Int = queue.outstandingWorkCount
}

object DependencyTracker {
  private[tracking] val log = msjava.slf4jutils.scalalog.getLogger(getClass)

  /**
   * Get the topmost DependencyTracker for a ScenarioStack, if there is one. This will look arbitrarily many levels down
   * the ScenarioStack to find a DependencyTracker.
   *
   * @param ss
   *   The ScenarioStack to inspect.
   * @return
   *   The topmost DependencyTracker, or None.
   */
  @tailrec def topDependencyTracker(ss: ScenarioStack = EvaluationContext.scenarioStack): Option[DependencyTracker] =
    ss.tweakableListener match {
      case t: TweakableTracker => Some(t.owner)
      case _ if ss.isRoot      => None
      case _                   => topDependencyTracker(ss.parent)
    }

  /**
   * Get the DependencyTrackerRoot for current scenarioStack
   *
   * @return
   *   The DependencyTrackerRoot, or throw
   */
  def getDependencyTrackerRoot: DependencyTrackerRoot =
    DependencyTrackerRootWeakReference.forStack.get

  /**
   * Get the topmost DependencyTracker for a ScenarioStack, which must have one. This will look arbitrarily many levels
   * down the ScenarioStack to find a DependencyTracker, but will throw an exception if it doesn't find one.
   *
   * @param ss
   *   The ScenarioStack to inspect.
   * @return
   *   The top DependencyTracker.
   * @throws IllegalStateException
   *   If there is no DependencyTracker.
   */
  def requireTopDependencyTracker(ss: ScenarioStack = EvaluationContext.scenarioStack): DependencyTracker =
    topDependencyTracker(ss).getOrElse(
      throw new IllegalStateException("Expected tracking data for the top of the scenario stack"))

  private[optimus] def commonParent(scenarios: Iterable[DependencyTracker]): DependencyTracker =
    commonParent(scenarios toSeq)

  private[optimus] def commonParent(scenarios: Seq[DependencyTracker]): DependencyTracker = {

    /**
     * find the common parent. Ts1 and t2 must be at the same level, and have a common root
     *
     * @param ts1
     *   DependencyTracker1
     * @param ts2
     *   DependencyTracker2
     * @return
     */
    @tailrec def commonAtLevel(ts1: DependencyTracker, ts2: DependencyTracker): DependencyTracker = {
      if (ts1 eq ts2) ts1
      else commonAtLevel(ts1.parent, ts2.parent)
    }

    scenarios.headOption match {
      case None => throw new IllegalArgumentException("no scenarios")
      case Some(first) =>
        scenarios.tail.foldLeft(first) { (ts, top) =>
          if (ts.root ne top.root)
            throw new IllegalArgumentException("no common parent if tracking scenarios are in different roots")
          val commonLevel = Math.min(ts.level, top.level)
          commonAtLevel(ts.parentOrSelfAtLevel(commonLevel), top.parentOrSelfAtLevel(commonLevel))
        }
    }
  }
}

sealed trait CacheClearMode
case object NoClear extends CacheClearMode // doesn't clear cache of disposed nodes (they will get LRU'd out later)
case object SyncClear extends CacheClearMode // clears cache of disposed nodes before completing disposal action
case object AsyncClear extends CacheClearMode // clears cache of disposed nodes in background
