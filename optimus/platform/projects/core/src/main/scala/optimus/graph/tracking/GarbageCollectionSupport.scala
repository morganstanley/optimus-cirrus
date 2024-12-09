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

import java.util.Timer
import java.util.TimerTask

import optimus.graph.Settings
import optimus.graph.tracking.CleanupScheduler._
import optimus.graph.tracking.DependencyTrackerRoot.log
import optimus.graph.tracking.ttracks.TweakableTracker
import optimus.graph.tracking.ttracks.TweakableTracker._
import optimus.platform.annotations.deprecating
import optimus.platform.util.PrettyStringBuilder

import scala.annotation.tailrec
import scala.util.Success
import scala.util.Try

/**
 * Coordinates the periodic cleanup of all TweakableTrackers owned by a DependencyTrackerRoot.
 */
private[tracking] trait GarbageCollectionSupport {
  self: DependencyTrackerRoot =>

  // Keep some history of the last cleanup that happened (don't need to synchronize when we write to this since cleanup
  // actions are update actions and will happen one at a time, so volatile is ok here)
  @volatile private var lastCleanup: Option[DependencyTrackerRootCleanupState] = None
  private[tracking] def getLastCleanupState(): Option[DependencyTrackerRootCleanupState] = lastCleanup

  private object ScheduledTrigger extends TrackingGraphCleanupTrigger

  private[this] val cleanupScheduler = new CleanupScheduler[DependencyTrackerRootCleanupState](
    self.timedScheduler,
    this.queue.interrupter,
    (state, interruptMode) => cleanupAsync(state, interruptMode, ScheduledTrigger, _ => ())
  )

  private[tracking] def newInitialCleanupState: DependencyTrackerRootCleanupState = {
    val allTrackers = underlayTweakableTracker.selfAndAllDescendants
    DependencyTrackerRootCleanupState(allTrackers, None, 0, 0, CleanupStats.zero)
  }

  private[tracking] def doCleanup(
      cleanupRequest: CleanupRequest,
      initialState: DependencyTrackerRootCleanupState,
      interrupt: InterruptionFlag): DependencyTrackerRootCleanupState = {
    val start = System.nanoTime()

    @tailrec def cleanup(todo: DependencyTrackerRootCleanupState): DependencyTrackerRootCleanupState =
      todo.toProcess match {
        // we're done so return the completed state
        case Nil => todo
        // we've been interrupted so return the remaining work
        case _ if interrupt.isInterrupted => todo
        // else process the next tracker
        case tracker :: rest =>
          val result = tracker.cleanupTTracks(todo.inflightCleanup.getOrElse(cleanupRequest), interrupt)
          val stats = todo.stats.merge(result.stats)
          result match {
            //  that tracker returned more work to do (it's almost certainly PartiallyComplete due to interruption
            // in which case we'll return that state in the recursive call)
            case incomplete: CleanupRequest =>
              cleanup(todo.copy(inflightCleanup = Some(incomplete), stats = stats))
            // that tracker didn't need to cleanup so go to the next
            case NothingToDo =>
              cleanup(todo.copy(toProcess = rest, inflightCleanup = None, skipped = todo.skipped + 1, stats = stats))
            // that tracker is done so go to the next
            case Complete(_) =>
              cleanup(todo.copy(toProcess = rest, inflightCleanup = None, cleaned = todo.cleaned + 1, stats = stats))
          }
      }

    if (initialState.inProgress)
      log.info(s"Resuming $cleanupRequest run with ${initialState.toProcess.size} tweakable trackers still to clean")
    else log.debug(s"Starting $cleanupRequest run for ${initialState.toProcess.size} tweakable trackers")

    val result = cleanup(initialState)

    val end = System.nanoTime()

    val msg = if (result.isComplete) "complete" else "suspended due to incoming work"
    log.debug(f"Cleanup of tweakable trackers $msg after ${(end - start) / 1e6}%.2fms $result")

    result
  }

  private[tracking] def createTrackingGraphCleanupAction(
      state: Option[DependencyTrackerRootCleanupState],
      interruptMode: InterruptionFlag,
      triggeredBy: TrackingGraphCleanupTrigger) =
    new TrackingGraphCleanupActionImpl(state, interruptMode, triggeredBy)

  private[tracking] class TrackingGraphCleanupActionImpl(
      initialState: Option[DependencyTrackerRootCleanupState],
      val interrupt: InterruptionFlag,
      val triggeredBy: TrackingGraphCleanupTrigger)
      extends DependencyTrackerActionUpdate[Unit]
      with TrackingGraphCleanupAction {
    final override protected def disposed = root.isDisposed
    final override private[tracking] def alreadyDisposedResult: Try[Unit] = Success(())
    final override protected def targetScenarioName: String = root.name
    final override def isLowPriority: Boolean = true
    override def doUpdate(): Unit = {
      log.debug(s"TrackingGraphCleanupAction triggered by $triggeredBy")
      val cleanupState = initialState.getOrElse(newInitialCleanupState)
      if (CleanupScheduler.autoCleanupEnabled && !interrupt.isInterrupted) {
        try {
          val result = doCleanup(CleanupIfNecessary, cleanupState, interrupt)
          val resultState = Some(result)

          if (result.inProgress) cleanupScheduler.notifyInterruptedCleanup(resultState)
          else cleanupScheduler.notifyCompletedCleanup()
          lastCleanup = resultState
        } catch {
          // as these actions are automatically scheduled and rescheduled, they don't have a receiver for the error,
          // so if we don't log it here it will be lost
          case t: Throwable =>
            log.error("cleanup failed", t)
            throw t
        }
      } else {
        cleanupScheduler.notifyInterruptedCleanup(initialState)
        lastCleanup = Some(cleanupState)
      }
    }
    override def toString = s"TrackingGraphCleanupAction [triggered by $triggeredBy]"
    override def writeWorkInfo(sb: PrettyStringBuilder): Unit = sb.appendln(toString)
  }

  protected final def maybeScheduleCleanup(lastAction: DependencyTrackerAction[_]): Unit = {
    // don't schedule another cleanup if we just finished one else we'll end up in an infinite loop
    if (!lastAction.isInstanceOf[TrackingGraphCleanupAction]) cleanupScheduler.notifyQueueIdle()
  }

  private[tracking] final def doImmediateUninterruptableForcedCleanup(): Unit = {
    val result = doCleanup(ForceCleanup, newInitialCleanupState, NoInterruption)
    assert(result.isComplete, "forced cleanup should always complete before returning")
  }
}

/**
 * abstraction for getting the current time (exists mostly to enable unit testing)
 */
private[tracking] trait CurrentTimeSource {
  def currentTimeMillis: Long
}

/**
 * abstraction for scheduling something to happen after a specified delay (exists mostly to enable unit testing)
 */
private[tracking] trait TimedScheduler extends CurrentTimeSource {
  def schedule(delayMillis: Long)(task: => Unit): Unit
}

/**
 * Schedules cleanups when the queue is idle. The basic rules are:
 *
 *   - Try to start a cleanup minDelayFromNow after the queue goes idle (the delay is to debounce because the queue
 *     often goes briefly idle then busy again)
 *   - Unless we already started a cleanup less than minDelayFromLastStart ago, in which case wait that long (to avoid
 *     starting cleanups too frequently and wasting time - and log space!)
 *   - Since cleanup now runs on lowPriorityQueue, we know that if a cleanup has been scheduled, there is no other work
 *     on the queue (so we can be less defensive about checking for interruptions when scheduled)
 *   - Except if we've been trying for more than maxTimeSinceLastCompletion to complete a cleanup without interruption,
 *     force one through without interruption
 *
 *  There are two other main sources of cleanup actions:
 *    - Memory pressure, specifically GCMonitor
 *    - Cleared reference watermark events, when there is a lot of nodes that are cleared (and so a lot of useless
 *      tracks)
 *
 * CleanupSchedulerTest explains the expected behavior programmatically.
 */
private[tracking] final class CleanupScheduler[CleanupState](
    scheduler: TimedScheduler,
    defaultInterrupt: InterruptionFlag,
    runCleanup: (Option[CleanupState], InterruptionFlag) => Unit) {
  outer =>

  private var delayedCleanupPending = false
  private var inProgressCleanup: Option[CleanupState] = None
  private var lastAttemptToStart: Long = -minDelayFromLastStart
  private var lastInterruption: Long = -minDelayFromLastStart
  private var lastCompletion: Long = -minDelayFromLastStart

  def notifyInterruptedCleanup(state: Option[CleanupState]): Unit = synchronized {
    inProgressCleanup = state
    notifyInterruptedCleanup()
  }

  def notifyInterruptedCleanup(): Unit = synchronized {
    lastInterruption = scheduler.currentTimeMillis
    scheduleDelayedCleanup()
  }

  def notifyQueueIdle(): Unit = synchronized {
    // this gets triggered really frequently (the queue goes briefly idle a lot) but the cleanup will only run while
    // the queue stays idle
    scheduleDelayedCleanup()
  }

  def notifyCompletedCleanup(): Unit = synchronized {
    inProgressCleanup = None
    lastCompletion = scheduler.currentTimeMillis
  }

  private def scheduleDelayedCleanup(): Unit = synchronized {
    if (CleanupScheduler.autoCleanupEnabled && !delayedCleanupPending) {
      val timeUntilNextAttempt = lastAttemptToStart + minDelayFromLastStart - scheduler.currentTimeMillis
      val delay = Math.max(minDelayFromIdle, timeUntilNextAttempt)
      log.debug(s"Next cleanup scheduled for ${delay}ms from now")

      scheduler.schedule(delay) {
        outer.synchronized {
          delayedCleanupPending = false
          lastAttemptToStart = scheduler.currentTimeMillis

          // if we've been interrupted so many times since the lastCompletion that we've exceeded a time limit,
          // we'd better ignore interruptions otherwise we might never finish the clean up! this should happen
          // almost never in normal apps
          val timeSinceLastCompletion = lastAttemptToStart - lastCompletion
          val interruptMode =
            if (timeSinceLastCompletion >= maxTimeSinceLastCompletion && lastInterruption > lastCompletion) {
              log.warn(
                f"Too long since last uninterrupted clean (${timeSinceLastCompletion}%,dms) - " +
                  f"Running uninterruptable cleanup")
              NoInterruption
            } else defaultInterrupt

          // actually execute the cleanup
          runCleanup(inProgressCleanup, interruptMode)
        }
      }

      delayedCleanupPending = true
    }
  }
}

object CleanupSchedulerHelper {
  @deprecating(suggestion = "Used to disable clean-ups for tests. Not for use in any non-test code")
  def setAutoCleanupEnabled(enabled: Boolean): Unit = CleanupScheduler.autoCleanupEnabled = enabled
}

private[graph] object CleanupScheduler {
  var autoCleanupEnabled = true

  def minDelayFromIdle = Settings.ttrackCleanupMinDelayFromIdleMs
  def minDelayFromLastStart = Settings.ttrackCleanupMinDelayFromLastStartMs
  def maxTimeSinceLastCompletion = Settings.ttrackCleanupMaxDelaySinceLastCompletionMs

  /**
   * a real scheduler based on java.util.Timer
   */
  object SharedTimedScheduler extends TimedScheduler {
    private val timer = new Timer("DepTrackerCleanupTimer", true)

    override def currentTimeMillis: Long = System.currentTimeMillis()
    override def schedule(delayMillis: Long)(task: => Unit): Unit = {
      timer.schedule(
        new TimerTask {
          override def run(): Unit =
            try task
            catch {
              case ex: Exception => log.error("Exception in SharedTimedScheduler task - ignoring", ex)
            }
        },
        delayMillis)
    }
  }

  /**
   * Captures the state of a cleanup for a whole DependencyTrackerRoot (c.f. TweakableTracker.CleanupResult/Request
   * which captures the state for cleanup of an individual TweakableTracker)
   *
   * @param toProcess
   *   list of tweakable trackers that we still need to (re)visit
   * @param inflightCleanup
   *   optionally a request to resume cleanup on the first tracker in the toProcess list
   * @param cleaned
   *   count of trackers completely cleaned so far
   * @param skipped
   *   count of trackers which we visited but said that didn't need cleaning
   * @param stats
   *   cumulative statistics for all trackers (partially or fully) processed so far
   */
  final case class DependencyTrackerRootCleanupState(
      toProcess: List[TweakableTracker],
      inflightCleanup: Option[TweakableTracker.CleanupRequest],
      cleaned: Int,
      skipped: Int,
      stats: CleanupStats) {
    override def toString: String =
      s"{todo=${toProcess.size}, cleaned=${cleaned}, skipped=${skipped}, " +
        s"partial=${inflightCleanup.isDefined}, stats=$stats}"

    def isComplete: Boolean = toProcess.isEmpty
    def inProgress: Boolean = !isComplete && (inflightCleanup.isDefined || cleaned > 0 || skipped > 0)
  }

  /**
   * Represents a source of interruption status. Algorithms using the flag are expected to periodically poll the flag,
   * and stop and return control if it is true. It's expected that implementations will only transition from false to
   * true, never from true to false (though they may return constant true or false).
   */
  trait InterruptionFlag {
    def isInterrupted: Boolean
  }

  object NoInterruption extends InterruptionFlag {
    override def isInterrupted: Boolean = false
  }

  sealed trait TrackingGraphCleanupAction extends DependencyTrackerAction[Unit] {
    def interrupt: InterruptionFlag
    def triggeredBy: TrackingGraphCleanupTrigger
  }
}
