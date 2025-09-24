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
package optimus.ui

import msjava.slf4jutils.scalalog._
import optimus.core.needsPluginAlwaysAutoAsyncArgs
import optimus.graph.AuxiliaryScheduler
import optimus.graph.LowerPriorityScheduler
import optimus.graph.ProgressTrackerParams
import optimus.graph.Scheduler
import optimus.graph.tracking.handler.NoResultEvaluated
import optimus.graph.tracking.handler.OutOfScopeScenarioReference
import optimus.platform._
import optimus.platform.annotations.alwaysAutoAsyncArgs
import optimus.platform.annotations.closuresEnterGraph
import optimus.platform.annotations.deprecating
import optimus.platform.annotations.poisoned
import optimus.platform.util.Log
import optimus.ui.FollowOnStepProgressUtils.maybeCancellableTrackerParams
import optimus.ui.HandlerResult.TweakLambda
import optimus.utils.MacroUtils.SourceLocation
import optimus.utils.PropertyUtils.get

/**
 * * This class represents a result of a UI handler containing multiple results. Gestures, tweaks and new gui widgets.
 * Order of execution (to prevent unnecessary evals) is gestures -> tweaks -> widgets
 * @param gestures
 *   \- gestures to be executed
 * @param tweaks
 *   \- tweaks to add
 * @param widgets
 *   \- widgets to render
 */
final case class HandlerResult(
    gestures: Seq[Gesture] = Seq.empty,
    tweaks: Map[ScenarioReference, Seq[Tweak]] = Map.empty,
    widgets: Seq[BaseWidget] = Seq.empty,
    scenarioToTweakLambdas: Map[ScenarioReference, Seq[TweakLambda]] = Map.empty,
    private val reevaluationPostponed: Boolean = false,
    private val noLazyEvaluation: Boolean = false,
    private val nextStepsBuilder: List[HandlerStep] = Nil,
    private[optimus] val profilingMetadata: Map[String, String] = Map.empty,
    private[optimus] val globalEventTag: Option[String] = None
) {
  import HandlerResult._

  lazy val nextSteps: List[HandlerStep] = nextStepsBuilder.reverse

  // This is the utility function to make sure the map of tweaks
  // can be merged properly
  private def mergeMap[T](
      tweaksA: Map[ScenarioReference, Seq[T]],
      tweaksB: Map[ScenarioReference, Seq[T]]): Map[ScenarioReference, Seq[T]] = {
    val jointKeys = tweaksA.keySet ++ tweaksB.keySet
    jointKeys.iterator.map { scenarioReference =>
      val combined = tweaksA.getOrElse(scenarioReference, Nil) ++ tweaksB.getOrElse(scenarioReference, Nil)
      scenarioReference -> combined
    }.toMap
  }

  private[optimus] def skipReevaluation: Boolean =
    (tweaks.isEmpty && scenarioToTweakLambdas.isEmpty) || reevaluationPostponed

//   Provide a way to turn off the lazyEval feature.
//   Considering the case that some Tweaks will cause a ViewerViewable to reevaluate where it contains sth like
//    Label(_.afterRenderHandler --> HandlerResult.withGui(
//      gui {
//        Dialog(_.title := "Label is renderred")
//      })
//   when lazyEval is on, the new gui block will not get evaluated when the invalidated nodes from the tweaks are in invisible applet, hence dialog won't popup.
//   but sometimes users are keen to always have immediate side effects after that Tweaks have been applied.
  private[optimus] def isLazyEvalutionOff: Boolean = noLazyEvaluation

  @deprecating("Temporary method during HandlerResultMigration")
  def mergeWithNewTweakLambdas(
      otherHr: HandlerResult,
      newTweakLambdas: Map[ScenarioReference, Seq[TweakLambda]]): HandlerResult = {
    new HandlerResult(
      gestures = gestures ++ otherHr.gestures,
      tweaks = mergeMap(tweaks, otherHr.tweaks),
      scenarioToTweakLambdas = mergeMap(scenarioToTweakLambdas, newTweakLambdas),
      widgets = widgets ++ otherHr.widgets,
      nextStepsBuilder = otherHr.nextStepsBuilder ::: nextStepsBuilder
    )
  }

  @deprecating("Temporary method during HandlerResultMigration")
  def mergeWithReplacementTweakLambdas(
      otherHr: HandlerResult,
      onlyTweakLambdas: Map[ScenarioReference, Seq[TweakLambda]]): HandlerResult = {
    new HandlerResult(
      gestures = gestures ++ otherHr.gestures,
      tweaks = mergeMap(tweaks, otherHr.tweaks),
      scenarioToTweakLambdas = onlyTweakLambdas,
      widgets = widgets ++ otherHr.widgets,
      nextStepsBuilder = otherHr.nextStepsBuilder ::: nextStepsBuilder
    )
  }

  /**
   * Creates a new HandlerResult comprising of the current one plus the given tweak lambda evaluated in Root scenario
   */
  @alwaysAutoAsyncArgs
  def withTweaksToRootScenario(newTweakLambda: () => Seq[Tweak]): HandlerResult = needsPluginAlwaysAutoAsyncArgs
  def withTweaksToRootScenario$NF(newTweakLambda: AsyncFunction0[Seq[Tweak]]): HandlerResult =
    withTweakLambda(ScenarioReference.Root, newTweakLambda)

  /**
   * Returns the tweak lambdas in this HandlerResult targeted at the given scenario
   */
  def tweakLambdasForScenarioReference(sr: ScenarioReference): Seq[TweakLambda] =
    scenarioToTweakLambdas.getOrElse(sr, Nil)

  /**
   * Return the tweaks (including evaluated tweak lambdas) in this HandlerResult targeted at the given scenario
   */
  def tweaksForScenarioReference(scen: ScenarioReference): Seq[Tweak] =
    tweakLambdasForScenarioReference(scen).flatMap(_.apply()) ++ tweaks.getOrElse(scen, Nil)

  /**
   * Returns the scenarios in this HandlerResult at which tweaks and/or tweak lambdas are targeted
   */
  def scenarioReferences: Set[ScenarioReference] = tweaks.keySet ++ scenarioToTweakLambdas.keySet

  /**
   * Creates new HandlerResult comprising of the current one plus given tweaks targeted at the current scenario
   */
  def withTweaks(newTweaks: Seq[Tweak]): HandlerResult =
    withTweaks(ScenarioReference.current, newTweaks)

  /**
   * Creates new HandlerResult comprising of the current one plus given tweaks targeted at the given scenario
   */
  def withTweaks(scenario: ScenarioReference, newTweaks: Seq[Tweak]): HandlerResult =
    withTweaks(Map(scenario -> newTweaks))

  /**
   * Creates new HandlerResult comprising of the current one plus given map of scenario references to tweaks
   */
  def withTweaks(newTweaks: Map[ScenarioReference, Seq[Tweak]]): HandlerResult =
    copy(tweaks = mergeMap(tweaks, newTweaks))

  /**
   * Creates new HandlerResult comprising of the current one plus given gesture
   */
  def withGesture(gst: Gesture): HandlerResult =
    copy(gestures = gestures :+ gst)

  /**
   * Creates new HandlerResult comprising of the current one plus given gestures
   */
  def withGestures(gsts: Seq[Gesture]): HandlerResult =
    copy(gestures = gestures ++ gsts)

  /**
   * Creates new HandlerResult comprising of the current one plus given gui widgets
   */
  def withGui(newGui: Seq[BaseWidget]): HandlerResult =
    copy(widgets = widgets ++ newGui)

  /**
   * Creates new HandlerResult comprising of the current one plus given tweak lambdas
   */
  def withTweakLambdaMap(newTweaks: Map[ScenarioReference, Seq[TweakLambda]]): HandlerResult =
    copy(scenarioToTweakLambdas = mergeMap(scenarioToTweakLambdas, newTweaks))

  /**
   * Creates new HandlerResult comprising of the current one plus given tweak lambdas
   */
  def withTweakLambda(sr: ScenarioReference, newTweakLambda: TweakLambda): HandlerResult =
    withTweakLambdaMap(Map(sr -> (newTweakLambda :: Nil)))

  /**
   * Creates new HandlerResult comprising of the current one plus given tweak lambdas
   */
  @alwaysAutoAsyncArgs
  def withTweaksEvaluatedIn(sr: ScenarioReference, newTweakLambda: () => Seq[Tweak]): HandlerResult =
    needsPluginAlwaysAutoAsyncArgs
  def withTweaksEvaluatedIn$NF(sr: ScenarioReference, newTweakLambda: AsyncFunction0[Seq[Tweak]]): HandlerResult =
    withTweakLambda(sr, newTweakLambda)

  /**
   * Creates a new HandlerResult merging the two together
   */
  def ++(hr: HandlerResult)(implicit loc: SourceLocation): HandlerResult = {
    if (this.nextStepsBuilder.nonEmpty || hr.nextStepsBuilder.nonEmpty) {
      val msg = HandlerResult.mergeWithFollowonStepsWarningMessage
      if (HandlerResult.printHandlerResultMergeWarningLocation) log.warn(s"$msg @ position: $loc")
      else log.warn(msg)
      if (HandlerResult.dumpHandlerResultMergeWarningTrace) Thread.dumpStack()
    }
    merge(hr)
  }

  private def merge(hr: HandlerResult): HandlerResult = {
    new HandlerResult(
      gestures = gestures ++ hr.gestures,
      tweaks = mergeMap(tweaks, hr.tweaks),
      scenarioToTweakLambdas = mergeMap(scenarioToTweakLambdas, hr.scenarioToTweakLambdas),
      reevaluationPostponed = skipReevaluation && hr.skipReevaluation,
      widgets = widgets ++ hr.widgets,
      nextStepsBuilder = hr.nextStepsBuilder ::: nextStepsBuilder,
      profilingMetadata = profilingMetadata ++ hr.profilingMetadata,
      globalEventTag = globalEventTag.orElse(hr.globalEventTag)
    )
  }

  /**
   * Creates a new HandlerResult with global tag Preferably used once for an entire handler chain If used in follow on
   * steps, the tag of last completed child is used (DFS style of completing follow on steps)
   */
  def withGlobalEventTag(globalTag: String): HandlerResult = {
    copy(profilingMetadata = profilingMetadata, globalEventTag = Some(globalTag))
  }

  /**
   * Creates new HandlerResult by combining profiling data
   */
  def withProfilingMetadata(data: Map[String, String], eventTag: String): HandlerResult = {
    copy(profilingMetadata = profilingMetadata ++ data ++ Map(eventTagKey -> eventTag))
  }

  /**
   * Creates a new HandlerResult with the supplied step appended to the sequence of next steps. These next steps will be
   * run in the order they were added, and each step will see the scenario effects of the previously applied step(s).
   * Note that this is different to merging HandlerResults, since in that the tweaks from the two HandlerResults do not
   * see each others effects.
   *
   * Note that if a step returns a HandlerResult which itself contains next steps, then those steps will be evaluated
   * before the next step on this HandlerResult (it's like a depth first traversal)
   */
  def andThen(step: HandlerStep): HandlerResult = copy(nextStepsBuilder = step :: nextStepsBuilder)

  /**
   * NOTE: if all previous steps (and all of their descendents) are Immediately steps (or if there are no previous
   * andThen steps at all), there is no point in using this method because the normal behavior is to wait for them anyway.
   *
   * Adds a "barrier" which will wait for all previous steps (whether Immediately, Separately or InBackground) and all
   * child steps returned by those steps (and all grandchild steps returned by those child steps and so on recursively)
   * before running any steps subsequent to the barrier.
   *
   * This is useful if you need to run some steps that must depend on the effects of all previous steps. If you know the
   * full structure of those previous steps, it might be simpler to just return the steps that must come afterwards as a
   * child of the previous steps, but if you don't know the full structure (or it might change in future) then
   * waitForAllPreviousSteps can be a simpler approach because it can be applied at (or close to) the top level of your
   * handler code, rather than at the lowest level.
   *
   * Also note that because previous steps may have been InBackground or running in different ScenarioReferences, we do
   * not know statically that we still hold the lock on the current Scenario when it is time to start subsequent steps.
   * Therefore all subsequent steps are silently wrapped inside a Separately so that we can reobtain the lock. The
   * consequence is that subsequent steps may not run immediately after previous steps complete. For example, if the
   * last previous step was InBackground, it's possible that once it completes, some unrelated handler has started
   * running already, and in that case we'll have to wait for that to complete before subsequent steps can run.
   */
  def waitForAllPreviousSteps(implicit sourceLocation: SourceLocation): HandlerResult =
    andThen(WaitForAllPrevious(ScenarioReference.current, sourceLocation))

  def postponeReevaluation: HandlerResult = copy(reevaluationPostponed = true)
  def noLazyEval: HandlerResult = copy(noLazyEvaluation = true)
}

object HandlerResult {
  private val log = getLogger(getClass)
  type TweakLambda = AsyncFunction0[Seq[Tweak]]
  val eventTagKey = "eventTag"

  /** Merging two HandlerResult with follow-on steps can lead to unexpected behaviors. */
  val mergeWithFollowonStepsWarningMessage: String =
    "Merging handler results that contain follow-on steps: this is probably not what you want. Consider using hr1.andThen(Immediately(hr2)) instead of hr1 ++ hr2, or contact the graph team."

  /**
   * Merging two HandlerResult with follow-on steps can lead to unexpected behaviors.
   *
   * With this option enabled the log warning message will print the "calling site" location to find & fix.
   */
  val printHandlerResultMergeWarningLocation: Boolean =
    get("optimus.ui.printHandlerResultMergeWarningLocation", default = false)

  /**
   * Merging two HandlerResult with follow-on steps can lead to unexpected behaviors.
   *
   * With this option enabled in addition to log warning message, will print stacktrace location to find & fix.
   */
  val dumpHandlerResultMergeWarningTrace: Boolean =
    get("optimus.ui.dumpHandlerResultMergeWarningTrace", default = false)

  /**
   * A no-op handler result
   */
  val empty: HandlerResult = HandlerResult()

  /**
   * Creates a new HandlerResult containing the given collection of tweaks targeted at the current scenario
   */
  implicit def withTweaks(tweaks: Seq[Tweak]): HandlerResult =
    withTweaks(ScenarioReference.current, tweaks)

  /**
   * Creates a new HandlerResult containing the given collection of tweaks targeted at the given scenario
   */
  def withTweaks(scenarioReference: ScenarioReference, tweaks: Seq[Tweak]): HandlerResult =
    withTweaks(Map(scenarioReference -> tweaks))

  /**
   * Creates a new HandlerResult containing the given map of scenarios to tweaks
   */
  def withTweaks(tweaks: Map[ScenarioReference, Seq[Tweak]]): HandlerResult =
    HandlerResult(tweaks = tweaks)

  /**
   * Creates new HandlerResult comprising of the current one plus given tweak lambdas
   */
  def withTweakLambdaMap(newTweaks: Map[ScenarioReference, Seq[TweakLambda]]): HandlerResult =
    HandlerResult(scenarioToTweakLambdas = newTweaks)

  /**
   * Creates new HandlerResult comprising of the current one plus given tweak lambdas
   */
  def withTweakLambda(sr: ScenarioReference, newTweakLambda: TweakLambda): HandlerResult =
    withTweakLambdaMap(Map(sr -> (newTweakLambda :: Nil)))

  /**
   * Creates new HandlerResult comprising of the current one plus given tweak lambdas
   */
  @alwaysAutoAsyncArgs
  def withTweaksEvaluatedIn(sr: ScenarioReference, newTweakLambda: () => Seq[Tweak]): HandlerResult =
    needsPluginAlwaysAutoAsyncArgs
  def withTweaksEvaluatedIn$NF(sr: ScenarioReference, newTweakLambda: AsyncFunction0[Seq[Tweak]]): HandlerResult =
    withTweakLambda(sr, newTweakLambda)

  /**
   * Creates a new HandlerResult containing the given Gesture
   */
  implicit def withGesture(gesture: Gesture): HandlerResult = withGestures(Seq(gesture))

  /**
   * Creates a new HandlerResult containing the given Gestures
   */
  implicit def withGestures(gestures: Seq[Gesture]): HandlerResult = HandlerResult(gestures = gestures)

  /**
   * Creates a new HandlerResult containing a given GUI widgets
   */
  implicit def withGui(widgets: Seq[BaseWidget]): HandlerResult = HandlerResult(widgets = widgets)

  /**
   * DANGEROUS If used with HandlerResults that depends on each other, the last HandlerResult will be applied, because
   * HandlerResults are ByValue and eagerly executed.
   */
  def sequentially(handlerResults: Seq[HandlerResult]): HandlerResult = {
    handlerResults.foldLeft(HandlerResult.empty)((l, r) => l andThen Immediately(r))
  }

  /** Creates a new [[HandlerResult]] which will execute the provided [[HandlerStep]]s. */
  def withSteps(steps: Seq[HandlerStep]): HandlerResult =
    HandlerResult(nextStepsBuilder = steps.toList.reverse)

  /**
   * Creates a new HandlerResult containing the given global handler tag
   */
  def withGlobalEventTag(globalTag: String): HandlerResult =
    HandlerResult(globalEventTag = Some(globalTag))

  /**
   * Creates a new HandlerResult containing the given profiling data
   */
  def withProfilingMetadata(data: Map[String, String], eventTag: String): HandlerResult = {
    HandlerResult(profilingMetadata = Map(eventTagKey -> eventTag) ++ data)
  }

  implicit class HandlerResultCombineUtils(val hrs: Iterable[HandlerResult]) extends AnyVal {
    def mergeHandlers: HandlerResult = hrs.foldLeft(HandlerResult.empty)(_ ++ _)
    def chainHandlers: HandlerResult = hrs.foldLeft(HandlerResult.empty)(_ andThen Immediately(_))
  }
}

private[optimus] sealed trait HandlerStep {
  private[optimus] val target: ScenarioReference
  private[optimus] val executionMode: StepType
  private[optimus] val sourceLocation: SourceLocation
}

private[optimus] sealed trait ExecutableHandlerStep extends HandlerStep {
  private[optimus] override val executionMode: ExecutableStepType
  private[optimus] val tag: String
  private[optimus] val progressTrackerParams: ProgressTrackerParams
}

private[optimus] object HandlerStep {
  implicit def toHandlerResult(handlerStep: HandlerStep): HandlerResult = HandlerResult.empty.andThen(handlerStep)
}

private[optimus] sealed trait StepType {
  override def toString: String = getClass.getSimpleName
}

private[optimus] sealed trait ExecutableStepType extends StepType {
  protected def checkInScope(target: ScenarioReference): Unit = {
    val consistentRoot = ScenarioReference.current.rootOfConsistentSubtree
    if (!target.isSelfOrChildOf(consistentRoot))
      throw new OutOfScopeScenarioReference(consistentRoot, target, "; consider using Separately(ref) { code } instead")
  }
}

private[optimus] sealed trait TransactionalHandlerStepBuilder[T <: HandlerStep] extends ExecutableStepType {
  protected def buildHandlerStep(
      target: ScenarioReference,
      createIfNotExists: Boolean,
      warnIfNotExists: Boolean,
      func: => HandlerResult,
      sourceLocation: SourceLocation,
      tag: String,
      progressTrackerParams: ProgressTrackerParams): T

  /**
   * Creates a step which will be evaluated later under the specified scenario
   */
  @closuresEnterGraph
  final def apply(target: ScenarioReference)(func: => HandlerResult)(implicit sourceLocation: SourceLocation): T = {
    checkInScope(target)
    buildHandlerStep(
      target,
      createIfNotExists = true,
      warnIfNotExists = true,
      func,
      sourceLocation,
      tag = "",
      progressTrackerParams = null)
  }

  /**
   * Creates a step which will be evaluated later under the specified scenario
   * createIfNotExists to control whether to create the scenario if it doesn't exist
   */
  @closuresEnterGraph
  final def apply(target: ScenarioReference, createIfNotExists: Boolean)(func: => HandlerResult)(implicit
      sourceLocation: SourceLocation): T = {
    checkInScope(target)
    buildHandlerStep(
      target,
      createIfNotExists,
      warnIfNotExists = true,
      func,
      sourceLocation,
      tag = "",
      progressTrackerParams = null)
  }

  /**
   * Creates a step which will be evaluated later under the specified scenario
   * createIfNotExists to control whether to create the scenario if it doesn't exist
   */
  @closuresEnterGraph
  final def apply(target: ScenarioReference, createIfNotExists: Boolean, warnIfNotExists: Boolean)(
      func: => HandlerResult)(implicit sourceLocation: SourceLocation): T = {
    checkInScope(target)
    buildHandlerStep(
      target,
      createIfNotExists,
      warnIfNotExists,
      func,
      sourceLocation,
      tag = "",
      progressTrackerParams = null)
  }

  /**
   * Creates a step which will be evaluated later under the current scenario (i.e. the scenario of the handler or step
   * in which this step was created)
   */
  @closuresEnterGraph
  final def apply(func: => HandlerResult)(implicit sourceLocation: SourceLocation): T =
    buildHandlerStep(
      ScenarioReference.current,
      createIfNotExists = true,
      warnIfNotExists = true,
      func,
      sourceLocation,
      tag = "",
      progressTrackerParams = null)

  /**
   * Supply a tag to label steps in debugger
   *
   * Note: it would be really nice just to have another apply method here with a String first parameter. However,
   * because of optimus.ui.ImplicitConversions.any2String (which is imported in lots of places where people write UI
   * code), the handler result lambda is converted to a String and the compiler gets confused about the overloaded
   * method with alternatives.
   */
  @closuresEnterGraph
  final def withTag(tag: String)(func: => HandlerResult)(implicit sourceLocation: SourceLocation): T =
    buildHandlerStep(
      ScenarioReference.current,
      createIfNotExists = true,
      warnIfNotExists = true,
      func,
      sourceLocation,
      tag,
      progressTrackerParams = null)

  @closuresEnterGraph
  final def withProgress(weight: Double, message: String = null)(func: => HandlerResult)(implicit
      sourceLocation: SourceLocation): T = {
    val params = ProgressTrackerParams(weight = weight, initialMessage = message)
    buildHandlerStep(
      ScenarioReference.current,
      createIfNotExists = true,
      warnIfNotExists = true,
      func,
      sourceLocation,
      tag = "",
      progressTrackerParams = params)
  }
}

private[optimus] object FollowOnStepProgressUtils {
  private[optimus] def maybeCancellableTrackerParams(trackerParams: ProgressTrackerParams): ProgressTrackerParams = {

    /**
     * if this is wrapped inside an enableCancellation { } block, we copy the progress tracker params (specifically the
     * 'cancellable' flag and scope) as this will determine if this follow on step can be cancelled
     */
    val currentSSTracker = EvaluationContext.scenarioStack.progressTracker
    if (currentSSTracker eq null) trackerParams
    else currentSSTracker.cancellableTracker(trackerParams)
  }
}

/**
 * Immediately steps are run immediately after the previous step has been applied, as part of the same blocking, atomic
 * transaction (just like chained handlers in a tupled UI binding).
 *
 * Consequently, when the body of the step is evaluated, all tweaks applied by previous steps will be visible, but no
 * changes from any other HandlerResult can possible have been applied. You should prefer to use Immediately steps
 * wherever possible because they are easiest to reason about.
 *
 * Note that due to the depth first traversal (see HandlerResult#andThen), andThen is associative for Immediately steps,
 * i.e.
 *
 * handlerResult1.andThen(Immediately { handlerResult2.andThen(Immediately { handlerResult3 } })
 *
 * has the same effect when applied as
 *
 * handlerResult1.andThen(Immediately { handlerResult2 }).andThen(Immediately { handlerResult3 })
 *
 * This rule is *not* in general true for non-Immediately steps.
 *
 * Note also that because these steps run in the same transaction, they cannot target ScenarioReferences outside of the
 * current consistent subtree. If you need to target such scenarios, you have to use Separately.
 *
 * The main motivation for using Immediately blocks is to create complex structures of tweaks where some tweaks need to
 * be built with other tweaks already applied.
 */
final case class Immediately(
    private[optimus] val target: ScenarioReference,
    private[optimus] val handler: () => HandlerResult,
    private[optimus] val sourceLocation: SourceLocation,
    private[optimus] val tag: String = "",
    private[optimus] val progressTrackerParams: ProgressTrackerParams
) extends ExecutableHandlerStep {
  private[optimus] val executionMode: Immediately.type = Immediately
}
object Immediately extends TransactionalHandlerStepBuilder[Immediately] {
  override protected def buildHandlerStep(
      target: ScenarioReference,
      createIfNotExists: Boolean,
      warnIfNotExists: Boolean,
      func: => HandlerResult,
      sourceLocation: SourceLocation,
      tag: String,
      progressTrackerParams: ProgressTrackerParams): Immediately = {
    val maybeCancellableParams = maybeCancellableTrackerParams(progressTrackerParams)
    new Immediately(target, () => func, sourceLocation, tag, maybeCancellableParams)
  }

  override def toString: String = "Immediately"
}

sealed trait SeparatelyMaybeWithProfilingMetadata {
  protected[optimus] def wrapIntoHandlerResult: HandlerResult
}

/**
 * Separately steps are enqueued immediately as a separate transaction on their target scenario. They will start running
 * whenever that scenario is free to run them. They are guaranteed to see all of the scenario effects from preceding
 * steps (if such effects are visible at all from the target scenario), but they may also see effects from subsequent
 * steps or completely different chains of handlers, since the separate transaction is not atomic with any other
 * transaction.
 *
 * The main motivation for using Separately is to target steps to scenarios outside of your current consistent subtree
 * (e.g. so that a concurrent child may apply steps in Root or some unrelated concurrent scenario). Avoid using
 * Separately if Immediately will do the job, because it's harder to reason about the behavior.
 */
final case class Separately(
    private[optimus] val target: ScenarioReference,
    private[optimus] val createIfNotExists: Boolean,
    private[optimus] val warnIfNotExists: Boolean,
    private[optimus] val handler: () => HandlerResult,
    private[optimus] val sourceLocation: SourceLocation,
    private[optimus] val tag: String,
    private[optimus] val progressTrackerParams: ProgressTrackerParams)
    extends ExecutableHandlerStep
    with SeparatelyMaybeWithProfilingMetadata {
  private[optimus] override val executionMode: Separately.type = Separately

  /**
   * To be used only when Separately is returned from InBackground step It doesn't typecheck if used with
   * 'andThen(Separately ...)'
   */
  def withProfilingMetadata(profilingMetadata: Map[String, String], eventTag: String): SeparatelyWithProfilingMetadata =
    SeparatelyWithProfilingMetadata(this, profilingMetadata, eventTag)
  protected[optimus] def wrapIntoHandlerResult: HandlerResult = HandlerResult.empty.andThen(this)
}

final case class SeparatelyWithProfilingMetadata(
    separately: Separately,
    profilingMetadata: Map[String, String],
    eventTag: String)
    extends SeparatelyMaybeWithProfilingMetadata {
  protected[optimus] def wrapIntoHandlerResult: HandlerResult =
    HandlerResult.empty.andThen(separately).withProfilingMetadata(profilingMetadata, eventTag)
}

/**
 * A Separately step that returns HandlerResult.empty. This can be used at the end of an InBackground step if there are
 * no tweaks to be applied, for example.
 */
object NoResult {
  private[optimus] val instance: Separately = {
    Separately(
      ScenarioReference.Dummy,
      createIfNotExists = false,
      warnIfNotExists = true,
      () => throw new NoResultEvaluated(),
      SourceLocation.makeSourceLocation,
      "",
      progressTrackerParams = null
    )
  }

  def apply(): Separately = instance
}

private[optimus] object WaitForAllPrevious extends StepType
private[optimus] final case class WaitForAllPrevious(
    override private[optimus] val target: ScenarioReference,
    override private[optimus] val sourceLocation: SourceLocation)
    extends HandlerStep {
  override private[optimus] val executionMode = WaitForAllPrevious
}

object Separately extends TransactionalHandlerStepBuilder[Separately] {
  override protected def checkInScope(target: ScenarioReference): Unit = ()
  override protected def buildHandlerStep(
      target: ScenarioReference,
      createIfNotExists: Boolean,
      warnIfNotExists: Boolean,
      func: => HandlerResult,
      sourceLocation: SourceLocation,
      tag: String,
      progressTrackerParams: ProgressTrackerParams): Separately = {
    val maybeCancellableParams = maybeCancellableTrackerParams(progressTrackerParams)
    new Separately(target, createIfNotExists, warnIfNotExists, () => func, sourceLocation, tag, maybeCancellableParams)
  }

  /**
   * Creates a step which will be evaluated later under the specified scenario
   */
  @closuresEnterGraph
  final def apply(
      target: ScenarioReference,
      tag: String,
      createIfNotExists: Boolean = true,
      warnIfNotExists: Boolean = true)(func: => HandlerResult)(implicit sourceLocation: SourceLocation): Separately = {
    checkInScope(target)
    buildHandlerStep(
      target,
      createIfNotExists,
      warnIfNotExists,
      func,
      sourceLocation,
      tag,
      progressTrackerParams = null)
  }

  override def toString: String = "Separately"
}

/**
 * InBackground steps immediately take a snapshot of the current scenario state and are then scheduled to run on the
 * auxiliary graph scheduler *outside* of any transaction. This means that they do not block other handlers or steps
 * running on any scenario.
 *
 * When the evaluation of an InBackground is complete, a transaction must be enqueued on to the target scenario in order
 * to apply the result. At the point when that transaction runs, it is entirely possible that the state of the target
 * scenario will be different to when the snapshot was taken, so InBackground steps need to take care not to clobber
 * changes which have been applied while they were running outside of any transaction.
 *
 * Usually an InBackground step will finish with a Separately step that will transactionally read-and-update the state
 * of the scenario once the background work is complete.
 *
 * Note that since InBackground steps start by transactionally snapshotting the current state of the target scenario,
 * they have the same targeting rules as Immediately steps. If you need to run the InBackground step against an out of
 * scope scenario, use Separately(target) { InBackground { code } }, which will enqueue a transaction on the target to
 * take the snapshot, and then will run in the background once the snapshot is taken.
 *
 * The main motivation for InBackground is to run slow steps without blocking other handlers from running. Since it's
 * more difficult to reason about than Immediately blocks, it's only worth using if responsiveness has proven to be an
 * actual problem.
 */
final case class InBackground(
    private[optimus] val target: ScenarioReference,
    private[optimus] val handler: NodeFunction0[SeparatelyMaybeWithProfilingMetadata],
    private[optimus] val sourceLocation: SourceLocation,
    private[optimus] val tag: String,
    private[optimus] val progressTrackerParams: ProgressTrackerParams,
    private[optimus] val scheduler: Scheduler
) extends ExecutableHandlerStep {
  private[optimus] val executionMode: InBackground.type = InBackground
}
object InBackground extends ExecutableStepType {
  private def buildHandlerStep(
      target: ScenarioReference,
      func: NodeFunction0[SeparatelyMaybeWithProfilingMetadata],
      sourceLocation: SourceLocation,
      tag: String,
      progressTrackerParams: ProgressTrackerParams,
      scheduler: Scheduler = AuxiliaryScheduler.scheduler): InBackground = {
    val maybeCancellableParams = maybeCancellableTrackerParams(progressTrackerParams)
    new InBackground(target, func, sourceLocation, tag, maybeCancellableParams, scheduler)
  }

  override def toString: String = "InBackground"

  /**
   * Creates a step which will be evaluated later under the specified scenario
   */
  @alwaysAutoAsyncArgs
  @poisoned("InBackground cannot be used in code not processed by AutoAsync")
  final def apply(target: ScenarioReference, tag: String)(func: => SeparatelyMaybeWithProfilingMetadata)(implicit
      sourceLocation: SourceLocation): InBackground = needsPluginAlwaysAutoAsyncArgs
  final def apply$NF(target: ScenarioReference, tag: String)(func: NodeFunction0[SeparatelyMaybeWithProfilingMetadata])(
      implicit sourceLocation: SourceLocation): InBackground = {
    checkInScope(target)
    buildHandlerStep(target, func, sourceLocation, tag, progressTrackerParams = null)
  }

  /**
   * Creates a step which will be evaluated later under the current scenario (i.e. the scenario of the handler or step
   * in which this step was created)
   */
  @alwaysAutoAsyncArgs
  @poisoned("InBackground cannot be used in code not processed by AutoAsync")
  final def apply(tag: String)(func: => SeparatelyMaybeWithProfilingMetadata)(implicit
      sourceLocation: SourceLocation): InBackground = needsPluginAlwaysAutoAsyncArgs
  final def apply$NF(tag: String)(func: NodeFunction0[SeparatelyMaybeWithProfilingMetadata])(implicit
      sourceLocation: SourceLocation): InBackground =
    buildHandlerStep(ScenarioReference.current, func, sourceLocation, tag, progressTrackerParams = null)

  /**
   * Creates a step which will be evaluated later under the specified scenario
   */
  @alwaysAutoAsyncArgs
  @poisoned("InBackground cannot be used in code not processed by AutoAsync")
  final def apply(target: ScenarioReference)(func: => SeparatelyMaybeWithProfilingMetadata)(implicit
      sourceLocation: SourceLocation): InBackground = needsPluginAlwaysAutoAsyncArgs
  final def apply$NF(target: ScenarioReference)(func: NodeFunction0[SeparatelyMaybeWithProfilingMetadata])(implicit
      sourceLocation: SourceLocation): InBackground = {
    checkInScope(target)
    buildHandlerStep(target, func, sourceLocation, tag = "", progressTrackerParams = null)
  }

  /**
   * Creates a step which will be evaluated later under the current scenario (i.e. the scenario of the handler or step
   * in which this step was created)
   */
  @alwaysAutoAsyncArgs
  @poisoned("InBackground cannot be used in code not processed by AutoAsync")
  final def apply(func: => SeparatelyMaybeWithProfilingMetadata)(implicit
      sourceLocation: SourceLocation): InBackground = needsPluginAlwaysAutoAsyncArgs
  final def apply$NF(func: NodeFunction0[SeparatelyMaybeWithProfilingMetadata])(implicit
      sourceLocation: SourceLocation): InBackground =
    buildHandlerStep(ScenarioReference.current, func, sourceLocation, tag = "", progressTrackerParams = null)

  /**
   * creates an InBackground step that will run with lower thread priority than normal InBackground step
   */
  @alwaysAutoAsyncArgs
  @poisoned("InBackground cannot be used in code not processed by AutoAsync")
  final def lowerPriority(func: => SeparatelyMaybeWithProfilingMetadata)(implicit
      sourceLocation: SourceLocation): InBackground = needsPluginAlwaysAutoAsyncArgs
  final def lowerPriority$NF(func: NodeFunction0[SeparatelyMaybeWithProfilingMetadata])(implicit
      sourceLocation: SourceLocation): InBackground =
    buildHandlerStep(
      ScenarioReference.current,
      func,
      sourceLocation,
      tag = "",
      progressTrackerParams = null,
      scheduler = LowerPriorityScheduler.scheduler)

  /**
   * creates an InBackground step that will run with lower thread priority than normal InBackground step
   * (later evaluated under the specified scenario)
   */
  @alwaysAutoAsyncArgs
  @poisoned("InBackground cannot be used in code not processed by AutoAsync")
  final def lowerPriority(target: ScenarioReference)(func: => SeparatelyMaybeWithProfilingMetadata)(implicit
      sourceLocation: SourceLocation): InBackground = needsPluginAlwaysAutoAsyncArgs
  final def lowerPriority$NF(target: ScenarioReference)(func: NodeFunction0[SeparatelyMaybeWithProfilingMetadata])(
      implicit sourceLocation: SourceLocation): InBackground = {
    checkInScope(target)
    buildHandlerStep(
      target,
      func,
      sourceLocation,
      tag = "",
      progressTrackerParams = null,
      scheduler = LowerPriorityScheduler.scheduler)
  }

  @alwaysAutoAsyncArgs
  @poisoned("InBackground cannot be used in code not processed by AutoAsync")
  final def withProgress(weight: Double, message: String = null)(func: => SeparatelyMaybeWithProfilingMetadata)(implicit
      sourceLocation: SourceLocation): InBackground = needsPluginAlwaysAutoAsyncArgs
  final def withProgress$NF(weight: Double, message: String = null)(
      func: NodeFunction0[SeparatelyMaybeWithProfilingMetadata])(implicit
      sourceLocation: SourceLocation): InBackground = {
    val params = ProgressTrackerParams(weight = weight, initialMessage = message)
    buildHandlerStep(ScenarioReference.current, func, sourceLocation, tag = "", progressTrackerParams = params)
  }
}

// all real implementations should derive from GuiWidget in ui_premacros, but that's not visible here so we have
// this marker trait
trait BaseWidget

//Utility class to represent handlers (independent of UI code)
private[optimus] final case class HandlerElem(handler: () => HandlerResult)

private[optimus] object HandlerElem extends Log {

  /**
   * Converts a list of handlers (representing a tuple of handlers) into a sequence of Immediately handler steps.
   *
   * @param handlers
   *   List of handlers representing a handler tuple
   * @return
   */
  def flatten(handlers: List[HandlerElem]): HandlerResult = {
    handlers match {
      case Nil      => HandlerResult.empty
      case x :: Nil => x.handler()
      case x :: xs  => x.handler().andThen(Immediately(flatten(xs)))
    }
  }
}
