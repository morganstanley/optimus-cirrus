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
package optimus.graph.tracking.handler

import optimus.graph.CompletableNodeM
import optimus.graph.Node
import optimus.graph.NodeTaskInfo
import optimus.graph.NullCancellationScope
import optimus.graph.OGSchedulerContext
import optimus.graph.OGTrace
import optimus.graph.ProgressTracker
import optimus.graph.ProgressTrackerParams
import optimus.graph.Scheduler
import optimus.graph.diagnostics.messages.EventDescription
import optimus.graph.diagnostics.messages.HandlerStepEvent
import optimus.graph.diagnostics.messages.HandlerStepEventCounter
import optimus.graph.tracking.DependencyTracker
import optimus.graph.tracking.DependencyTrackerBatchUpdater
import optimus.graph.tracking.DependencyTrackerDisposedException
import optimus.graph.tracking.EventCause
import optimus.graph.tracking.handler.AsyncActionEvaluator.Action
import optimus.platform.EvaluationContext
import optimus.platform.NodeFunction0
import optimus.platform.util.Log
import optimus.platform.ScenarioStack
import optimus.ui.HandlerResult
import optimus.ui.HandlerStep
import optimus.ui.Immediately
import optimus.ui.InBackground
import optimus.ui.NoResult
import optimus.ui.ScenarioReference
import optimus.ui.Separately
import optimus.ui.SeparatelyMaybeWithProfilingMetadata
import optimus.ui.WaitForAllPrevious
import optimus.utils.MacroUtils.SourceLocation

import scala.annotation.tailrec
import scala.util.Failure
import scala.util.Success
import scala.util.Try

private[handler] sealed trait ActionDetails {
  val sourceLocation: SourceLocation
  val tag: String
  val uiDelegate: HandlerResultDelegate
}
private[handler] object NoActionDetails extends ActionDetails {
  val handler: () => HandlerResult = () => HandlerResult.empty
  val uiDelegate: HandlerResultDelegate = HeadlessHandlerResultDelegate
  val sourceLocation: SourceLocation = SourceLocation.Unknown
  val tag: String = ""
}
private[handler] final case class ImmediatelyActionDetails(
    handler: () => HandlerResult,
    updater: DependencyTrackerBatchUpdater,
    uiDelegate: HandlerResultDelegate,
    sourceLocation: SourceLocation,
    tag: String = "",
    progressTrackerParams: ProgressTrackerParams)
    extends ActionDetails
private[optimus] final case class InBackgroundActionDetails(
    handler: NodeFunction0[SeparatelyMaybeWithProfilingMetadata],
    updater: DependencyTrackerBatchUpdater,
    uiDelegate: HandlerResultDelegate,
    sourceLocation: SourceLocation,
    tag: String = "",
    cause: EventCause,
    progressTrackerParams: ProgressTrackerParams,
    scheduler: Scheduler
) extends ActionDetails
private[optimus] final case class SeparatelyActionDetails(
    handler: () => HandlerResult,
    tracker: DependencyTracker,
    uiDelegate: HandlerResultDelegate,
    sourceLocation: SourceLocation,
    tag: String = "",
    cause: EventCause,
    waitForAllPrevious: Boolean,
    progressTrackerParams: ProgressTrackerParams)
    extends ActionDetails

private[optimus] trait HandlerResultEvaluatorBase extends Log {
  private case class ImmediatelyAction(details: ImmediatelyActionDetails) extends Action {

    override val cause: EventCause =
      details.updater.cause.createProfiledChild("Immediately", details.sourceLocation)

    override def runAsync(runFollowing: Try[List[Action]] => Unit, progressTracker: ProgressTracker): Unit = {
      cause.counted {
        val start = OGTrace.nanoTime()
        val handlerResult = {
          val event = HandlerStepEventCounter.report(EventDescription(details.tag, details.sourceLocation))

          val scenarioModifyF = (ss: ScenarioStack) =>
            ScenarioStack.modifySS(
              progressTracker,
              details.progressTrackerParams,
              details.updater.consistentRoot.target.queue.currentCancellationScope)(ss)
          val hr = details.updater.evaluateImmediate(details.handler(), scenarioModifyF)

          details.uiDelegate.change(details.updater, hr, Some(cause))
          HandlerStepEventCounter.reportCompleted(event)
          hr
        }
        val done = OGTrace.nanoTime()
        cause.profile.update(
          incrSelfTimeNs = done - start,
          metadataToAdd = handlerResult.profilingMetadata,
          newGlobalTag = handlerResult.globalEventTag
        )
        val updater = details.updater
        runFollowing(Success(stepsToActions(handlerResult.nextSteps, updater, details)))
      }
    }
  }

  protected def buildInBackgroundAction(details: InBackgroundActionDetails): Action
  protected def buildSeparatelyAction(details: SeparatelyActionDetails): Action

  private def trackerFor(
      updater: DependencyTrackerBatchUpdater,
      scenarioReference: ScenarioReference): DependencyTracker =
    updater.consistentRoot.target.root.getOrCreateScenario(scenarioReference)

  private[optimus] def stepsToActions(
      allSteps: List[HandlerStep],
      updater: DependencyTrackerBatchUpdater,
      details: ActionDetails): List[Action] = {
    val uiDelegate = details.uiDelegate

    def buildSeparately(
        tracker: DependencyTracker,
        f: () => HandlerResult,
        sourceLoc: SourceLocation,
        tag: String,
        waitForAllPrevious: Boolean,
        progressTrackerParams: ProgressTrackerParams): Action = {
      val originCause = details match {
        // Separately that are the final step of InBackground actions are attached to the InBackground cause
        case InBackgroundActionDetails(_, _, _, _, _, bgCause, _, _) => bgCause
        case _                                                       => updater.cause
      }
      val newCause =
        originCause.createProfiledChild(if (waitForAllPrevious) "WaitForAllPrevious" else "Separately", sourceLoc)

      buildSeparatelyAction(
        SeparatelyActionDetails(
          f,
          tracker,
          uiDelegate,
          sourceLoc,
          tag,
          newCause,
          waitForAllPrevious,
          progressTrackerParams
        ))
    }

    // If no description is provided in the step but it was constructed by a parent with a description, inherit the
    // description and supply the sub-step. The source location will still refer to the nested step.
    def newDescription(eventDescription: String, stepName: String): String =
      if (eventDescription.isEmpty && details.tag.nonEmpty)
        s"${details.tag} [$stepName]"
      else eventDescription

    // This needs to be tailrec to avoid stack overflow when there are lot of steps.
    @tailrec def loop(accum: List[Action])(steps: List[HandlerStep]): List[Action] = steps match {
      case Nil                       => accum
      case NoResult.instance :: more => loop(accum)(more)
      case Immediately(target, f, sourceLoc, tag, progressTrackerParams) :: more =>
        val newDescr = newDescription(tag, Immediately.toString)
        val newDetails = ImmediatelyActionDetails(
          f,
          updater.updaterFor(target),
          uiDelegate,
          sourceLoc,
          newDescr,
          progressTrackerParams)
        loop(ImmediatelyAction(newDetails) :: accum)(more)
      case InBackground(target, f, sourceLoc, tag, progressTrackerParams, scheduler) :: more =>
        val newDescr = newDescription(tag, InBackground.toString)
        val newUpdater = updater.updaterFor(target)
        val newDetails = InBackgroundActionDetails(
          f,
          newUpdater,
          uiDelegate,
          sourceLoc,
          newDescr,
          newUpdater.cause.createBackgroundProfiledChild("InBackground", details.sourceLocation),
          progressTrackerParams,
          scheduler
        )
        loop(buildInBackgroundAction(newDetails) :: accum)(more)
      case Separately(target, createIfNotExists, f, sourceLoc, tag, progressTrackerParams) :: more =>
        val tracker =
          if (createIfNotExists) Some(trackerFor(updater, target))
          else updater.consistentRoot.target.root.getScenarioIfExists(target)
        val steps = tracker
          .map { tracker =>
            buildSeparately(
              tracker,
              f,
              sourceLoc,
              newDescription(tag, Separately.toString),
              waitForAllPrevious = false,
              progressTrackerParams) :: accum
          }
          .getOrElse {
            log.warn(
              s"Scenario ${target.name} doesn't exist and is not created as createIfNotExists = false, skip the step defined at ${sourceLoc.toString}")
            accum
          }
        loop(steps)(more)

      case WaitForAllPrevious(target, sourceLoc) :: more =>
        // WaitForAllPrevious is implemented by taking all subsequent steps and wrapping them in a Separately with the
        // waitForAllPrevious flag. We need the Separately so that we reacquire a BatchUpdate session on the tracker,
        // since that may have been released by previous InBackground tasks
        def toHandlerResult: HandlerResult = more.foldLeft(HandlerResult.empty)((h, a) => h.andThen(a))
        buildSeparately(
          trackerFor(updater, target),
          toHandlerResult _,
          sourceLoc,
          "",
          waitForAllPrevious = true,
          progressTrackerParams = null) :: accum
    }
    val out = loop(Nil)(allSteps).reverse
    out
  }

  def evaluateAndApply(
      handler: () => HandlerResult,
      updater: DependencyTrackerBatchUpdater,
      uiDelegate: HandlerResultDelegate,
      onComplete: Try[Unit] => Unit,
      eventDescription: String = "",
      progressTracker: ProgressTracker = null,
      progressTrackerParams: ProgressTrackerParams = null)(implicit sourceLocation: SourceLocation): Unit = {
    val details =
      ImmediatelyActionDetails(handler, updater, uiDelegate, sourceLocation, eventDescription, progressTrackerParams)
    AsyncActionEvaluator.start(ImmediatelyAction(details), onComplete, progressTracker)
  }
}

private[optimus] object HandlerResultEvaluator extends HandlerResultEvaluatorBase {

  private[optimus] final case class InBackgroundAction(
      evaluator: HandlerResultEvaluatorBase,
      details: InBackgroundActionDetails)
      extends Action {

    override def cause: EventCause = details.cause

    private class HandlerResultBackgroundActionTask(
        handler: NodeFunction0[SeparatelyMaybeWithProfilingMetadata],
        initialScenarioStack: ScenarioStack)
        extends CompletableNodeM[HandlerResult] {
      attach(initialScenarioStack.withoutCacheableTransitively)
      private var _startTime: Long = _
      private var event: HandlerStepEvent = _
      private var child: Node[SeparatelyMaybeWithProfilingMetadata] = _

      override def run(ec: OGSchedulerContext): Unit = {
        if (child eq null) {
          _startTime = OGTrace.nanoTime()
          val progressTracker = scenarioStack().progressTracker
          ProgressTracker.sendInitialMessageIfPossible(progressTracker)
          val description = if (details.tag.isEmpty) "InBackground" else details.tag
          event = HandlerStepEventCounter.report(EventDescription(description, details.sourceLocation))
          child = handler.apply$queued()
          ec.enqueue(this, child)
          child.continueWith(this, ec)
        } else {
          HandlerStepEventCounter.reportCompleted(event)
          ProgressTracker.reportCompleteIfPossible(scenarioStack.progressTracker)
          val durationNs = OGTrace.nanoTime() - _startTime
          val r = child.result.wrapIntoHandlerResult
          cause.profile.update(
            incrSelfTimeNs = durationNs,
            metadataToAdd = r.profilingMetadata,
            newGlobalTag = r.globalEventTag)
          combineInfo(child, ec)
          completeWithResult(r, ec)
        }
      }
      override def executionInfo(): NodeTaskInfo = NodeTaskInfo.HandlerResultBackgroundAction
    }

    override def runAsync(runFollowing: Try[List[Action]] => Unit, progressTracker: ProgressTracker): Unit = {
      // The cause is a background step, so we can create a token on it and complete it without blocking the parent.
      val token = cause.createAndTrackToken()

      val updater = details.updater
      val ss = updater.snapshotImmediate()
      val newSS = ScenarioStack.modifySS(progressTracker, details.progressTrackerParams)(ss)
      /*
       <p> In the context of cancellation, we need to run the callback under a different CS</p>
       <p>(as we cannot pass an already cancelled CS)</p>

      <p>Note: withCancellationScope*Raw* to completely overwrite the existing CS instead of merging with it</p>
      <p>(in which case if it was cancelled, we would also get cancelled, which is exactly what we don't want) </p>
       */
      val ssWithDifferentCS = EvaluationContext.scenarioStack.withCancellationScopeRaw(NullCancellationScope)

      val task = new HandlerResultBackgroundActionTask(
        details.handler,
        newSS
      )

      details.scheduler.evaluateNodeAsync(
        task,
        maySAS = true,
        trackCompletion = true,
        cb = (t: Try[HandlerResult]) => {
          runFollowing(
            for (handlerResult <- t)
              yield evaluator.stepsToActions(handlerResult.nextSteps, updater, details))

          token.release()
        },
        cbSS = ssWithDifferentCS
      )
    }
  }

  private[optimus] final case class SeparatelyAction(
      evaluator: HandlerResultEvaluatorBase,
      details: SeparatelyActionDetails)
      extends Action {

    override def cause: EventCause = details.cause

    override def waitForAllPrevious: Boolean = details.waitForAllPrevious

    override def runAsync(runFollowing: Try[List[Action]] => Unit, progressTracker: ProgressTracker): Unit = {
      val token = cause.createAndTrackToken()

      details.tracker.executeBatchUpdateAsync(
        cause,
        evaluator.evaluateAndApply(
          details.handler,
          _,
          details.uiDelegate,
          (t: Try[Unit]) => runFollowing(for (_ <- t) yield List[Action]()),
          eventDescription = "Separately",
          progressTracker,
          details.progressTrackerParams
        )(details.sourceLocation),
        (t: Try[Unit]) => {
          token.release() // always release token
          t match {
            // TODO (OPTIMUS-64532): This is a hotfix for failures caused by rejoining a now disposed tracker. The root
            //  cause is probably that we don't wait for InBackgrounds to complete enough before processing TSA_Dispose.
            case Failure(e: DependencyTrackerDisposedException) =>
              log.error("Error in Separately completion:", e)
            case Failure(exception) =>
              // This is a failure that has occurred in the completion callback - there is no way to gracefully recover
              throw exception
            case _ =>
          }
        },
        // using default rather than currentOrDefault so that if we were on the aux scheduler (due to previous action
        // being InBackground), we come back to the main scheduler (to avoid priority inversion)
        scheduler = Scheduler.defaultMT
      )
    }
  }

  override protected def buildInBackgroundAction(details: InBackgroundActionDetails): InBackgroundAction =
    InBackgroundAction(this, details)
  override protected def buildSeparatelyAction(details: SeparatelyActionDetails): SeparatelyAction =
    SeparatelyAction(this, details)
}
