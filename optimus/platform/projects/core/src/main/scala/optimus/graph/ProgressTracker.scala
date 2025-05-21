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

import com.github.benmanes.caffeine.cache.Caffeine
import msjava.base.util.uuid.MSUuid
import optimus.graph.ProgressTracker.idToTracker
import optimus.platform.EvaluationContext

import java.util.Objects
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.annotation.tailrec

private class TrackerMoniker(val id: UUID, params: ProgressTrackerParams) extends Serializable {
  def readResolve(): AnyRef = ProgressTracker.fromID(id, params)
}

/**
 * A class to keep track of task progress and propagate progress updates to a listener.
 *
 * NOTE: All methods that reset class variables can be called from multiple threads and so are called under a lock.
 *
 * @param params
 *   Progress tracker parameters.
 * @param parent
 *   Parent progress tracker.
 */
private[optimus] class ProgressTracker private (val params: ProgressTrackerParams, @transient parent: ProgressTracker)
    extends Serializable {

  // the following vars can be read/written in multiple threads but they all called under lock
  private[this] var lastReportingTime: Long = 0 // want this value to be reset the first time it's called
  private[this] var progress: Double = 0.0
  private[this] var lastProgressReportedToParent: Double = 0.0
  private[this] var progressMessage: String = _
  @transient final var id: UUID = _

  if (Settings.progressTrackingDebugMode)
    ProgressTrackerDiagnostics.updateTrackers(this, getStallInfo(OGTrace.nanoTime()))

  private def this(id: UUID, params: ProgressTrackerParams) = { this(params = params, parent = null); this.id = id; }

  // noinspection ScalaUnusedSymbol
  private def writeReplace(): AnyRef = new TrackerMoniker(initializedID(), params)

  protected def initializedID(): UUID = this.synchronized {
    if (id eq null) id = MSUuid.generateJavaUUID()
    idToTracker.get(id, _ => this)
    id
  }

  private[optimus] def isCancellable: Boolean = this.params.cancellable

  private[optimus] def childTracker(params: ProgressTrackerParams): ProgressTracker =
    ProgressTracker(params = params, parent = this)

  private[optimus] final def progressComplete(): Unit = {
    reportProgress(ProgressTrackerState(progress = 1.0, message = null))

    if (Settings.progressTrackingDebugMode)
      ProgressTrackerDiagnostics.removeTracker(this)
  }

  private[optimus] final def progressUpdate(progress: Double): Unit =
    reportProgress(ProgressTrackerState(progress = progress))

  private[optimus] final def sendProgressMessage(message: String): Unit =
    if (message ne null)
      reportProgress(ProgressTrackerState(progress = -1.0, message = message))

  /**
   * Report progress to the tracker and propagate up the tree. If a progress tracker has a listener, propagate the
   * update to the listener.
   * @param state
   *   Progress tracker state to be used to update the tracker.
   */
  @tailrec
  private final def reportProgress(state: ProgressTrackerState): Unit = {

    // Tickles affect all exponential decays up to root
    if (state.isTickle) {
      if (params.progressType == ProgressType.ExponentialDecay) {
        val updatedState = update(state)
        reportToListener(updatedState)
      }
      if (Objects.nonNull(parent))
        parent.reportProgress(state)
    } else {
      val updatedState = update(state)
      reportToListener(updatedState)
      if (Objects.nonNull(parent) && updatedState.shouldReportToParent)
        parent.reportProgress(updatedState)
    }
  }

  /**
   * Propagate progress update to the listener.
   *
   * NOTE: this method is NOT locked. The actual propagation depends on the implementation of
   * [[params.listener.onProgress]].
   * @param state
   *   Progress tracker state.
   */
  private[this] def reportToListener(state: ProgressTrackerState): Unit = if (Objects.nonNull(params.listener)) {
    val maybeProgress = if (state.progressToListener >= 0) Some(state.progressToListener) else None
    val maybeMessage = Option(state.messageToListener)

    params.listener.onProgress(maybeProgress, maybeMessage)
  }

  /**
   * Update the tracker based on a progress state, and return a new progress state based on the updates. NOTE: all
   * transformations of vars in this class take place in this method which is why it is locked.
   * @param state
   *   Progress state.
   * @return
   *   An updated progress state.
   */
  private[this] def update(state: ProgressTrackerState): ProgressTrackerState = synchronized {
    val currTimeNanos = OGTrace.nanoTime()

    // update the current progress
    val newProgress =
      if (state.progress < 0) this.progress // passing negative progress means we don't want to update progress
      else if (state.isUpdateFromChild) calculateProgressBasedOnChildUpdate(state.progress)
      else state.progress

    // check if the data passes from the progress state is different from the data already stored in this tracker
    val progressToListener = if (newProgress != this.progress) newProgress else -1.0
    val messageToListener = if (state.message != this.progressMessage) state.message else null

    // reset progress data for the tracker
    this.progress = newProgress
    this.progressMessage = state.message

    // get the contribution that will be added to the parent
    val contributionToParent = getContribution

    // check if updates should update up the tree and if so, update the last progress reported to the parent
    val shouldReportUp = shouldReportToParent(state.shouldWaitBeforeReportingToParent, currTimeNanos)

    if (shouldReportUp)
      this.lastProgressReportedToParent = this.progress

    if (progressToListener >= 0 && Settings.progressTrackingDebugMode)
      ProgressTrackerDiagnostics.updateTrackers(this, getStallInfo(currTimeNanos))

    ProgressTrackerState(
      progress = contributionToParent,
      message = state.message,
      progressToListener = progressToListener,
      messageToListener = messageToListener,
      shouldReportToParent = shouldReportUp,
      shouldWaitBeforeReportingToParent = true,
      isUpdateFromChild = true
    )
  }

  // called under lock
  private[this] def calculateProgressBasedOnChildUpdate(progressUpdate: Double): Double = {
    params.progressType match {
      case ProgressType.Exact =>
        val newProgress = progress + progressUpdate
        if (Settings.progressTrackingDebugMode && newProgress > 1.000001) {
          ProgressTrackerDiagnostics.report(s"""Progress exceeds 1! It is $newProgress. Setting progress to 1.
            Stack trace:\n${Thread.currentThread().getStackTrace.mkString("\n")}""")
        }
        // make sure we don't exceed 100% (aka 1.0)
        Math.min(1.0, newProgress)
      case ProgressType.ExponentialDecay =>
        val multiplier = Math.pow(2, (-1) * progressUpdate)
        1 - (1 - progress) * multiplier
    }
  }

  // called under lock
  private[this] def shouldReportToParent(shouldWaitBeforeReportingToParent: Boolean, currTimeNanos: Long): Boolean = {
    if (!shouldWaitBeforeReportingToParent)
      true
    else if (params.weight <= 0) // do not report any progress to parent unless we have a strictly positive weight
      false
    else {
      val timeDiffMs = TimeUnit.NANOSECONDS.toMillis(currTimeNanos - lastReportingTime)

      if (timeDiffMs >= params.reportingIntervalMs) {
        lastReportingTime = currTimeNanos
        true
      } else
        false
    }
  }

  // called under lock
  private[this] def getContribution: Double = {
    params.weight * (progress - lastProgressReportedToParent)
  }

  private[this] def getStallInfo(currTimeNanos: Long): ProgressTrackerStallInfo =
    ProgressTrackerStallInfo(toString, progress, currTimeNanos)

  /**
   * constructs a new progress tracker params
   *
   * Note: EC.scenarioStack.progressTracker.params.cancellable = true only when inside enableCancellation { } If that is
   * set to true, it means we are inside a cancellable block, so we copy the flag (cancellable) and the user
   * cancellation scope; this way, we know it is inside an enableCancellation {} while running the handler
   */

  private[optimus] def cancellableTracker(trackerParams: ProgressTrackerParams): ProgressTrackerParams =
    if (trackerParams eq null) {
      // only create new params if cancellable flag is true; otherwise we end up with a weight of 0
      // and no progress is passed up to parent tracker
      if (params.cancellable)
        ProgressTrackerParams(cancellable = params.cancellable, userCancellationScope = params.userCancellationScope)
      else trackerParams
    } else
      trackerParams.copy(cancellable = params.cancellable, userCancellationScope = params.userCancellationScope)
}

object ProgressTracker {
  val idToTracker = Caffeine.newBuilder().weakValues().build[UUID, ProgressTracker]

  private def getReportingInterval(reportingIntervalMs: Long, parentTracker: ProgressTracker): Long = {
    if (reportingIntervalMs >= 0)
      reportingIntervalMs
    else if (parentTracker ne null)
      parentTracker.params.reportingIntervalMs
    else
      Settings.progressReportingTimeIntervalMs
  }

  private[optimus] def tickle(amt: Double): Unit = {
    val progressTracker = EvaluationContext.scenarioStack.progressTracker
    if (Objects.nonNull(progressTracker))
      progressTracker.reportProgress(ProgressTrackerState(progress = amt, isTickle = true))
  }

  private[optimus] def fromID(id: UUID, params: ProgressTrackerParams): ProgressTracker =
    new ProgressTracker(id, params)

  private[optimus] def newTracker(params: ProgressTrackerParams): ProgressTracker =
    ProgressTracker(params = params, parent = null)

  private def apply(params: ProgressTrackerParams, parent: ProgressTracker): ProgressTracker = {
    val interval = getReportingInterval(params.reportingIntervalMs, parent)
    val updatedParams = params.copy(reportingIntervalMs = interval)

    new ProgressTracker(updatedParams, parent)
  }

  private[optimus] def sendInitialMessageIfPossible(tracker: ProgressTracker): Unit = {
    if ((tracker ne null) && (tracker.params.initialMessage ne null))
      tracker.sendProgressMessage(tracker.params.initialMessage)
  }

  private[optimus] def reportCompleteIfPossible(tracker: ProgressTracker): Unit =
    if ((tracker ne null) && tracker.params.weight > 0) tracker.progressComplete()
}
