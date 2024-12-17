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

import optimus.graph.CancellationScope
import optimus.graph.GraphInInvalidState
import optimus.graph.OGTrace
import optimus.graph.Scheduler
import optimus.graph.Settings
import optimus.graph.diagnostics.GraphDiagnostics
import optimus.graph.tracking.CleanupScheduler.InterruptionFlag
import optimus.graph.tracking.CleanupScheduler.TrackingGraphCleanupAction
import optimus.graph.tracking.monitoring.ActionSummary
import optimus.graph.tracking.monitoring.QueueActionSummary
import optimus.graph.tracking.monitoring.QueueMonitor
import optimus.graph.tracking.monitoring.QueueStats
import optimus.platform.util.PrettyStringBuilder

import java.util
import scala.collection.compat._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.collection.mutable
import scala.util.control.ControlThrowable

object DependencyTrackerQueue {
  private[optimus] val log = msjava.slf4jutils.scalalog.getLogger(getClass)
}

/**
 * TrackingScenarioActions such as update(addTweaks|removeTweaks) and evaluate (all or some nodes) need to be scheduled.
 * This class manages that scheduling
 */
final class DependencyTrackerQueue(private[tracking] val tracker: DependencyTracker)
    extends DependencyTrackerActionEvaluateBase[Unit]
    with CallbackBatchOwner {
  import DependencyTrackerQueue._
  private val monitor = QueueMonitor.register(this)

  val interrupter: InterruptionFlag = new InterruptionFlag {
    override def isInterrupted: Boolean = hasPendingWork
  }

  override protected def trackerRoot: DependencyTrackerRoot = tracker.root
  override def toString: String = s"DependencyTrackerQueue[${tracker.scenarioReference}] [$stateManager]"

  override def syncApply(): Boolean = {
    // kick off our work
    afterActionCompleted(null)
    true
  }

  override def executeAsync(afterExecute: Long => Unit): Unit = throw new UnsupportedOperationException()
  override def executeSync(): Unit = throw new UnsupportedOperationException()

  private[tracking] def hasPendingWork =
    stateManager.isParentWaiting || queueLock.synchronized(!workQueue.isEmpty || !lowPriorityQueue.isEmpty)

  private[tracking] object stateManager {
    // volatile so we can observe parent status without holding lock
    @volatile private var stateUpdating = false
    @volatile private var parentIsWaiting = false
    @volatile private var inFlightWorkSet = Set[DependencyTrackerAction[_]]()

    def inFlightWork: Set[DependencyTrackerAction[_]] = inFlightWorkSet
    def isEvaluating: Boolean = workInProgress && !stateUpdating
    def isUpdating: Boolean = stateUpdating
    def workInProgress: Boolean = inFlightWorkSet.nonEmpty
    def isParentWaiting: Boolean = parentIsWaiting

    def parentManager = tracker.parentDependencyTracker.map(_.queue.stateManager)

    def visitInFlightWork(fn: DependencyTrackerAction[_] => Unit): Unit = {
      workQueue.assertLocked()
      queueLock.synchronized(inFlightWorkSet.foreach(fn))
    }

    def isThisOrAncestorUpdating: Boolean =
      isUpdating || parentManager.exists(_.isThisOrAncestorUpdating)

    def isThisAndAncestorsEvaluating: Boolean =
      isEvaluating && parentManager.forall(_.isThisAndAncestorsEvaluating)

    // Allows unit tests to temporarily disable invariant checking, since the checks are fairly expensive and so affect
    // actual race behavior. Tests should typically test both with and without invariant checking enabled.
    @volatile var suppressInvariantChecking: Boolean = false

    def checkInvariants(actuallyEvaluating: Boolean = false, actuallyUpdating: Boolean = false): Unit = {
      if (Settings.schedulerAsserts && !suppressInvariantChecking) {
        queueLock.synchronized {
          if (actuallyEvaluating && !isEvaluating)
            throw new GraphInInvalidState("we are running an evaluation but isEvaluating is false")

          if (actuallyUpdating && !isUpdating)
            throw new GraphInInvalidState("we are running an update but isUpdating is false")

          val wip = workInProgress
          if (wip && parentManager.exists(!_.isThisAndAncestorsEvaluating))
            throw new GraphInInvalidState("we have work in progress but an ancestor's stateEvaluating was zero")

          if (wip && parentManager.isDefined && !parentIsWaiting)
            throw new GraphInInvalidState("we have work in progress but parentIsWaiting was false")

          if (isUpdating) {
            if (!wip)
              throw new GraphInInvalidState("we are updating but have no work in progress")
            if (inFlightWorkSet.size > 1)
              throw new GraphInInvalidState("we are updating but have more than one action in flight")
          } else if (isEvaluating && !wip)
            throw new GraphInInvalidState("we are evaluating but have no work in progress")

          parentManager.foreach {
            _.checkInvariants(actuallyEvaluating = actuallyEvaluating || actuallyUpdating, actuallyUpdating = false)
          }
        }
      }
    }

    def recordStart(action: DependencyTrackerAction[_]): Unit = {
      workQueue.assertLocked()
      action.recordStart(tracker.scenarioReference)

      log.debug(s"recordStart: $action (isUpdate = ${action.isUpdate})")
      checkInvariants()

      if (action.isUpdate) {
        if (workInProgress)
          throw new GraphInInvalidState(s"tried to start an update when work was in progress")
        stateUpdating = true
      } else if (isUpdating)
        throw new GraphInInvalidState("tried to start an evaluation when an update was in progress")

      if (inFlightWorkSet.contains(action))
        throw new GraphInInvalidState(s"tried to start an action that was already in flight $action")
      else inFlightWorkSet += action

      checkInvariants()
    }

    def recordStop(action: DependencyTrackerAction[_]): Unit = {
      if (Settings.schedulerAsserts && !Thread.holdsLock(queueLock))
        throw new GraphInInvalidState("called recordStop without holding queueLock")

      log.debug(s"recordStop: $action (isUpdate = ${action.isUpdate})")
      action.recordStop()
      checkInvariants()

      if (inFlightWorkSet.contains(action)) {
        inFlightWorkSet -= action
        if (action.isUpdate)
          stateUpdating = false
      }

      // cancellable actions can be stopped multiple times due to a race between cancellation and completion, and that's
      // not a problem
      else if (!action.isInstanceOf[DependencyTrackerActionCancellable])
        throw new GraphInInvalidState(s"tried to stop an action that was not in flight $action")

      checkInvariants()
    }

    // only call this if you hold the workQueue lock
    def setParentIsWaiting(value: Boolean): Unit = {
      workQueue.assertLocked()
      if (parentIsWaiting == value)
        throw new GraphInInvalidState(s"tried to set parentIsWaiting to $value but it was already equal to $value")
      checkInvariants()
      parentIsWaiting = value
      checkInvariants()
    }

    override def toString(): String =
      s"isEvaluating:$isEvaluating isUpdating:$isUpdating parentIsWaiting:$parentIsWaiting inflightWork:${inFlightWork.size}"
  }

  // Note: the only reason workQueue is not empty if there is at least one pending update
  private val workQueue = new WorkQueue
  private val lowPriorityQueue = new WorkQueue
  private val queueLock = new Object // used to synchronized queue access

  private[optimus] def outstandingWorkCount = workQueue.size

  private def registerOrEnqueueActionParent(highPriority: Boolean = false): Boolean = {
    tracker.parentDependencyTracker.get.queue
      .registerOrEnqueueAction(this, prepareWait = false /* No blocks*/, highPriority = highPriority)
  }

  private def checkActionsStalling(): Unit = {
    val logStalling = workQueue.iterator.exists(_.isStalling) ||
      (lowPriorityQueue.iterator.exists(_.isStalling) && Settings.trackingScenarioConcurrencyDebug)
    if (logStalling) trackerRoot.logStalling(tracker.scenarioReference)
  }

  /** Check if the action can be ran immediately (return that) and if yes, also mark the queue appropriately */
  private def registerOrEnqueueAction(
      action: DependencyTrackerAction[_],
      prepareWait: Boolean,
      highPriority: Boolean = false): Boolean = {
    var immediate = false
    var nextIsUpdate = false
    queueLock.synchronized {
      stateManager.checkInvariants()
      log.debug(s"registerOrEnqueueAction start action:$action, prepareWait:$prepareWait $stateManager")
      if (Settings.trackingScenarioConcurrencyDebug) checkActionsStalling()
      val okWithParents =
        if (tracker.isRoot || stateManager.workInProgress)
          true // Root self approves or we previously had parent approve
        else if (stateManager.isParentWaiting) false // Registered with parent already
        else {
          stateManager.setParentIsWaiting(true)
          registerOrEnqueueActionParent(highPriority = highPriority)
        }

      if (action.isUpdate && stateManager.workInProgress && !action.isLowPriority) { // no low-priority updates for now but hey
        cancelAndRescheduleLowPriorityActions()
      }

      if (okWithParents) {
        val headOfQueue = workQueue.isEmpty || highPriority
        if (action.isUpdate) {
          if (!stateManager.workInProgress && headOfQueue) {
            nextIsUpdate = true
            immediate = true
          }
        } else if (!stateManager.isUpdating && headOfQueue) {
          // This needs to be called before recording start
          setNewCancellationScopeBeforeEvaluating()
          immediate = true
        }
      }

      if (immediate) stateManager.recordStart(action)
      else {
        if (prepareWait) action.prepareWait()
        action.logEnqueued()
        if (highPriority) workQueue.addFirst(action)
        else {
          if (action.isLowPriority) lowPriorityQueue.addLast(action)
          else workQueue.addLast(action)
        }
      }
      log.debug(
        s"registerOrEnqueueAction end action:$action, prepareWait:$prepareWait $stateManager immediate:$immediate")
      stateManager.checkInvariants()
    }

    if (nextIsUpdate) {
      evaluationBarrier(action.cause)
    }
    immediate
  }
  def debugQueue: String = queueLock.synchronized {
    s" queue size ${workQueue.size} head ${workQueue.peek}\nlow priority queue size ${lowPriorityQueue.size} head ${lowPriorityQueue.peek} "
  }

  /** Note: executed under queueLock */
  private def cancelAndRescheduleLowPriorityActions(): Unit = {
    workQueue.assertLocked()
    val toReschedule = mutable.ListBuffer.empty[DependencyTrackerActionCancellable]
    stateManager.visitInFlightWork {
      case cancellable: DependencyTrackerActionCancellable =>
        cancellable.cancel() foreach { rescheduled =>
          toReschedule += rescheduled
          log.info(s"Reschedule preemptable $cancellable as $rescheduled")
        }
      case _ => // do nothing
    }
    toReschedule.foreach(lowPriorityQueue.addLast)
    stateManager.checkInvariants()
  }

  // for testing cleanup heuristics
  private[graph] def currentQueue(): Seq[DependencyTrackerAction[_]] =
    queueLock.synchronized { workQueue.iterator.toList }

  private def afterActionCompleted(completedAction: DependencyTrackerAction[_]): Unit = {
    var immediate: ArrayBuffer[DependencyTrackerAction[_]] = null
    var nextIsUpdate = false

    queueLock.synchronized {
      log.debug(s"afterActionCompleted start completedAction:$completedAction, hasLatch:$hasLatch $stateManager")
      if (completedAction ne null) {
        stateManager.checkInvariants(
          actuallyUpdating = completedAction.isUpdate,
          actuallyEvaluating = !completedAction.isUpdate)
        stateManager.recordStop(completedAction)
        if (completedAction.isUpdate) {
          cancelOrphanedNodesAndResetCancellationScope(completedAction.cause)
        } else {
          // Only reset cancellation scope after the last evaluation completedAction is done
          if (!stateManager.isEvaluating) cancelOrphanedNodesAndResetCancellationScope(completedAction.cause)
        }
      }

      // there are multiple sources of work
      // the queue contains a sequence of update and evaluate calls
      // if the parent is waiting then we could take work from there
      // we could start or continue a cleanup, or take actions off the low priority queue
      // the ordering of these should be able to be determined via a policy
      // but for the moment this is hard coded
      def findWork(queue: WorkQueue): Unit = {
        // Schedule the next action if it's update or next actions if they are evaluate
        immediate = new ArrayBuffer[DependencyTrackerAction[_]]()
        var break = false
        while (!break) {
          val nextAction = queue.pop()
          immediate += nextAction
          if (nextAction.isUpdate) {
            nextIsUpdate = true
            break = true
          } else {
            // This needs to be called before marking the action as started
            setNewCancellationScopeBeforeEvaluating()
            val previewAction = queue.peek
            if ((previewAction eq null) || previewAction.isUpdate)
              break = true
          }
          stateManager.recordStart(nextAction)
        }
      }

      if (!stateManager.workInProgress) {
        if (!workQueue.isEmpty) findWork(workQueue)
        else { // no work on main queue, check low priority queue
          if (!lowPriorityQueue.isEmpty) findWork(lowPriorityQueue)
          else if (stateManager.isParentWaiting) {
            // no more work to do on either queue - release parent
            stateManager.setParentIsWaiting(false)
            tracker.parentDependencyTracker.get.queue.afterActionCompleted(this)
          }
        }
      }
      stateManager.checkInvariants()
      log.debug(
        s"afterActionCompleted end completedAction:$completedAction, hasLatch:$hasLatch $stateManager immediate(size) = ${if (immediate eq null) 0
          else immediate.size}")
    }

    if (immediate ne null) {
      if (nextIsUpdate) {
        evaluationBarrier(immediate.head.cause)
      }
      var i = 0
      while (i < immediate.size) {
        val action = immediate(i)
        log.debug(s"afterActionCompleted process $i/${immediate.size} action:$action")
        if (!action.wakeUp()) {
          log.debug(s"afterActionCompleted process $i/${immediate.size} action:$action !wakeup")
          scheduleAction(action)
        }
        i += 1
      }
    } else {
      tracker.notifyQueueIdle(completedAction)
    }
  }

  /**
   * Because TTrack modifications during evaluation are not safe to run during invalidation/cleanup, we must ensure that
   * no evaluations are happening when invalidation/cleanup runs. It's not sufficient to just wait for the
   * DependencyTrackerAction's nodes to complete, because transitive nodes may not have completed. For example in c =
   * (a,b).apar, if a fails with an exception, c will complete even though b is still running, and b may then modify
   * tracking information after the completion of c. See OrphanedNodesTest and [WAIT_FOR_CANCELLED] comments.
   *
   * The solution is to run all evaluations in a cancel scope, and when we switch to update mode we cancel that scope
   * (which will prevent any of the orphaned nodes from starting), and then wait for the schedulers to finish any
   * currently running cancelled nodes (to avoid the race condition where the node was running already when it was
   * cancelled).
   *
   * CancellationScope doesn't have the same structure as DependencyTracker concurrent trees, all nodes which are
   * evaluated from an action in this queue must evaluate in this scope, including those running in concurrent children
   * DependencyTrackers.
   *
   * CancellationScope is recreated every time when we start an evaluation phase (with multiple evaluate actions) or
   * updating phase and cleaned after
   */
  private[tracking] def currentCancellationScope = {
    if (_currentCancellationScope eq null)
      throw new GraphInInvalidState(s"Invalid cancellation scope! There is no action running on this queue.")
    _currentCancellationScope
  }
  @volatile private[this] var _currentCancellationScope: CancellationScope = _

  /**
   * Creates a new child scope from our parent queue's current scope (or a new root scope if we're root). We need it to
   * be a child scope so that if the parent wants to start an update it can cancel its own scope and know that ours is
   * cancelled too, so that any orphaned nodes from us are cancelled. We have to update currentScope before evaluating
   * whenever the parent scope could have changed
   */
  private def newChildOfParentScope: CancellationScope =
    tracker.parentDependencyTracker
      .map(_.queue.currentCancellationScope.childScope())
      .getOrElse(CancellationScope.newScope())

  /**
   * cancels any outstanding nodes, waits for the cancellation and installs a new cancellation scope - see comment on
   * currentCancellationScope for more details
   */
  private[tracking] def evaluationBarrier(eventCause: EventCause): Unit = {
    if (!stateManager.isUpdating)
      throw new GraphInInvalidState(
        s"DependencyTrackerQueue#evaluationBarrier may only be used when updating, but state was $stateManager")
    if (_currentCancellationScope ne null) {
      currentCancellationScope.cancelInternal(new CancelOrphanedDependencyTrackerNodesException(eventCause))
      Scheduler.schedulers.foreach(_.waitForCancelledNodesToStop(currentCancellationScope))
    }
    _currentCancellationScope = newChildOfParentScope
  }

  /**
   * Setup a new cancellation scope, this should be called before running evaluate actions
   */
  private def setNewCancellationScopeBeforeEvaluating(): Unit = {
    // Only create a new cancellation scope before the first evaluation task, as multiple evaluations can run in parallel
    // and they share the cancellataion scope
    if (!stateManager.isEvaluating) {
      if (_currentCancellationScope ne null)
        throw new GraphInInvalidState(s"A previous cancellation scope is not cleaned!")
      _currentCancellationScope = newChildOfParentScope
    }
  }

  /**
   * Unset the cancellation scope, this should be called after actions complete
   */
  private def cancelOrphanedNodesAndResetCancellationScope(eventCause: EventCause): Unit = {
    // Cancel orphaned nodes and set _currentCancellationScope = null so no further evaluations can be attached to
    // this cancellation scope if it's not from a new action for this queue
    currentCancellationScope.cancelInternal(new CancelOrphanedDependencyTrackerNodesException(eventCause))
    Scheduler.schedulers.foreach(_.waitForCancelledNodesToStop(currentCancellationScope))
    _currentCancellationScope = null
  }

  private def scheduleAction[T](action: DependencyTrackerAction[T]): Unit = {
    stateManager.checkInvariants(actuallyUpdating = action.isUpdate, actuallyEvaluating = !action.isUpdate)

    if (!action.syncApply()) {

      try { action.beforeAction(this) }
      catch {
        case e: Exception =>
          log.error(s"beforeAction failed for $action, resetting queue stateManager ($action will not run)", e)
          afterActionCompleted(action)
          throw e
      }

      if (Settings.trackingScenarioLoggingEnabled) DependencyTrackerLogging.beforeRunLog(action)

      val actionStartTime = OGTrace.nanoTime()
      val queueLatencyNs = actionStartTime - action.cause.profile.eventStartTimeNs
      if (queueLatencyNs >= monitor.queueLatencyThresholdNanos) {
        monitor.queueLatencyWarning(queueLatencyNs, ActionSummary(action))
      }

      action.executeAsync { accurateActionStartTime => // after execute, called on both success and failure
        action.raiseCallback()
        try { action.afterAction(this) }
        catch {
          case e: Exception =>
            log.error(s"afterAction failed for $action", e)
            throw e
        } finally {
          val endNs = OGTrace.nanoTime()
          val completionLatency =
            if (accurateActionStartTime == 0) endNs - actionStartTime else endNs - accurateActionStartTime

          if (completionLatency >= monitor.completionLatencyThresholdNanos) {
            monitor.completionLatencyWarning(completionLatency, accurateActionStartTime == 0, ActionSummary(action))
          }

          afterActionCompleted(action)
          if (Settings.trackingScenarioLoggingEnabled) DependencyTrackerLogging.afterRunLog(action, actionStartTime)
        }
      }
    }
  }

  private[graph] def execute[T](action: DependencyTrackerAction[T], highPriority: Boolean = false): T = {
    val immediate = registerOrEnqueueAction(action, prepareWait = true, highPriority = highPriority)

    if (!immediate) {
      val logWaitStats = Settings.trackingScenarioLoggingEnabled || Settings.trackingScenarioConcurrencyDebug
      val (startWait, dtStateBefore) =
        if (logWaitStats)
          (DependencyTrackerLogging.beforeRunWait(action), GraphDiagnostics.getDependencyTrackerWorkInfo(false))
        else (0L, "")
      action.await() // Block until action becomes runnable
      if (logWaitStats) DependencyTrackerLogging.afterRunWait(action, startWait, tracker, dtStateBefore)
    }

    val startNs = OGTrace.nanoTime()
    if (Settings.trackingScenarioLoggingEnabled) DependencyTrackerLogging.beforeRunLog(action)
    stateManager.checkInvariants(actuallyUpdating = action.isUpdate, actuallyEvaluating = !action.isUpdate)

    try {
      action.beforeAction(this)
      action.executeSync()
    } catch {
      case e: Exception =>
        log.error(s"action failed for $action, resetting queue stateManager", e)
        throw e
    } finally {
      try { action.afterAction(this) }
      catch {
        case e: Exception =>
          log.error(s"afterAction failed for $action, resetting queue stateManager", e)
          throw e
      } finally {
        afterActionCompleted(action)
        if (Settings.trackingScenarioLoggingEnabled) DependencyTrackerLogging.afterRunLog(action, startNs)
      }
    }
    action.result.get
  }

  private[graph] def executeAsync[T](
      action: DependencyTrackerAction[T],
      callback: Try[T] => Unit,
      highPriority: Boolean = false): Unit = {
    action.callback = callback
    val immediate = registerOrEnqueueAction(action, prepareWait = false, highPriority = highPriority)
    if (immediate) scheduleAction(action)
  }

  /**
   * Appends a representation of this queue for the cases where this queue is on another DependencyTrackerQueue This
   * happens because a DependencyTrackerQueue is also DependencyTrackerAction
   */
  override def writeWorkInfo(sb: PrettyStringBuilder): Unit = {
    sb.appendln(s"$toString Evaluate Queue")
  }

  /**
   * Append a representation of the in-flight work and queued work on this DependencyTrackerQueue
   */
  def getQueueWorkInfo(sb: PrettyStringBuilder, includeUTracks: Boolean): Unit = {
    val (wQ, lowPrioQ) = queueLock.synchronized {
      (workQueue.iterator.toList, lowPriorityQueue.iterator.toList)
    }

    sb.appendln(s"Dependency tracker work info for: $toString")
    sb.appendln("")
    sb.appendln("Work info for in-flight work:")
    sb.indent()
    getInFlightWorkInfo(sb, includeUTracks)
    sb.unIndent()
    sb.appendln("")
    sb.appendln("Work info for queued work:")
    sb.indent()
    getQueuedWorkInfo(sb, wQ)
    sb.unIndent()
    sb.appendln("")
    sb.appendln("Work info for low priority queued work:")
    sb.indent()
    getQueuedWorkInfo(sb, lowPrioQ)
    sb.unIndent()
  }

  private def getInFlightWorkInfo(sb: PrettyStringBuilder, includeUTracks: Boolean): Unit = {
    val work = stateManager.inFlightWork
    work.foreach(_.writeWorkInfo(sb))
    if (work.isEmpty) {
      sb.appendln("No in-flight work found")
    } else if (includeUTracks) {
      // print all UTrack information for the trackers this queue owns (other trackers will be printed by the other queues)
      def printAllInfo(t: DependencyTracker): Unit = {
        if (t.queue eq this) {
          t.userNodeTracker.dumpUTrackDebugInfo(sb)
          t.children.foreach(printAllInfo)
        }
      }
      printAllInfo(tracker)
    }
  }

  private def getQueuedWorkInfo(sb: PrettyStringBuilder, tempQueue: Seq[DependencyTrackerAction[_]]): Unit = {
    if (tempQueue.isEmpty) {
      sb.appendln("No queued work found")
    }
    tempQueue.foreach(qi => {
      if (qi ne null)
        qi.writeWorkInfo(sb)
    })
  }

  private[tracking] def maybeRunCleanupNow(action: TrackingGraphCleanupAction): Unit = {
    // GC triggers may fire multiple times before the cleanup has a chance to actually run - make sure we don't enqueue
    // again if the previous cleanup still hasn't run.
    //
    // We do run multiple cleanups if they differ in interruption status.
    //
    // We don't check the low priority queue, because that cleanup may never run if workQueue is always busy.
    val debounced = queueLock.synchronized {
      workQueue.peek match {
        case other: TrackingGraphCleanupAction if other.interrupt == action.interrupt => true
        case _                                                                        => false
      }
    }
    if (!debounced)
      executeAsync(action, (_: Try[_]) => log.info(s"$action completed"), highPriority = true)
  }

  // get statistics about the queue
  def snap: QueueStats = queueLock.synchronized {
    QueueStats(
      added = workQueue.added + lowPriorityQueue.added,
      removed = workQueue.removed + lowPriorityQueue.removed
    )
  }

  // get additional information about the queues
  def summary: QueueActionSummary = queueLock.synchronized {
    QueueActionSummary(
      tracker.name,
      snap,
      workQueue.iterator.map(ActionSummary(_)).to(Vector),
      lowPriorityQueue.iterator.map(ActionSummary(_)).to(Vector)
    )
  }

  // An array deque that keeps statistics. Not synchronized at all.
  private class WorkQueue {
    type A = DependencyTrackerAction[_]
    private val underlying = new util.ArrayDeque[A]()

    def assertLocked(): Unit =
      if (Settings.schedulerAsserts && !Thread.holdsLock(queueLock)) {
        throw new GraphInInvalidState("modifications AND reads from WorkQueue requires holding queueLock")
      }

    // forwarders
    def size: Int = underlying.size
    def isEmpty: Boolean = underlying.isEmpty
    def peek: A = underlying.peek() // doesnt need the lock but might wrongly return null in case of race

    /**
     * Get an iterator for this workQueue.
     *
     * Note: Consuming from the iterator requires holding the lock!
     */
    def iterator: Iterator[A] = new Iterator[A] {
      private val backed = underlying.iterator()
      override def hasNext: Boolean = backed.hasNext
      override def next(): A = {
        assertLocked()
        backed.next()
      }
    }

    // Methods that track statistics
    def addFirst(a: A): Unit = {
      assertLocked()
      underlying.addFirst(a)
      _added += 1
    }
    def addLast(a: A): Unit = {
      assertLocked()
      underlying.addLast(a)
      _added += 1
    }
    def pop(): A = {
      assertLocked()
      val out = underlying.pop() // throws NoSuchElementException and doesnt increment `removed` if its empty
      _removed += 1
      out
    }

    private var _added = 0
    private var _removed = 0
    def added: Int = _added
    def removed: Int = _removed
  }
}

private[graph] final class CancelOrphanedDependencyTrackerNodesException(private val eventCause: EventCause)
    extends RuntimeException(
      s"Cancelled by: $eventCause, not (necessarily) an error: cancelled incomplete orphaned nodes after DependencyTrackerAction completes")

// in case you need to know whether you're getting a completion callback because of low-priority preemption
private[optimus] final class CancelPreemptedInFlightLowPriorityNodes extends ControlThrowable {
  override def getMessage: String = "cancelled incomplete low-priority nodes due to incoming update"
}
