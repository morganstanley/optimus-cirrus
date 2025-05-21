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

import java.io.PrintWriter
import java.io.StringWriter
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import optimus.exceptions.GenericRTException
import optimus.graph.AlreadyFailedNode
import optimus.graph.NodeTaskInfo
import optimus.graph.Settings
import optimus.graph._
import optimus.graph.diagnostics.GraphDiagnostics
import optimus.graph.diagnostics.messages.DTQEvent
import optimus.graph.diagnostics.messages.DTQEventCounter
import optimus.platform.util.PrettyStringBuilder
import optimus.platform.util.ThreadDumper
import optimus.platform._
import optimus.ui.ScenarioReference

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object DependencyTrackerActionTask {
  // we have a special execution info so that the scheduler knows we're a "safe" entry point that cannot cause
  // sync stack deadlocks
  val evaluate: NodeTaskInfo = NodeTaskInfo.internal("[UIEvaluate]", NodeTaskInfo.AT_MOST_ONE_WAITER)
  val evaluateImm: NodeTaskInfo = NodeTaskInfo.internal("[UIEvaluateNow]", NodeTaskInfo.AT_MOST_ONE_WAITER)
  val update: NodeTaskInfo = NodeTaskInfo.internal("[UIUpdate]", NodeTaskInfo.AT_MOST_ONE_WAITER)
}

/**
 * A wrapper used to convert functions into Nodes that can be used to build tracking scenario actions.
 */
final class DependencyTrackerActionTask[T](nti: NodeTaskInfo, cause: EventCause, expr: () => T)
    extends CompletableNodeM[T] {
  override def run(ec: OGSchedulerContext): Unit = {
    val progressTracker: ProgressTracker = scenarioStack.progressTracker
    ProgressTracker.sendInitialMessageIfPossible(progressTracker)

    OGTrace.observer.enqueueFollowsSequenceLogic(this, Int.MaxValue)
    completeWithResult(expr(), ec)
    ProgressTracker.reportCompleteIfPossible(progressTracker)
  }

  override def executionInfo(): NodeTaskInfo = nti
  override def subProfile(): AnyRef = cause
  override def writePrettyString(sb: PrettyStringBuilder): PrettyStringBuilder = {
    sb.append(nti.name() + " " + cause.toString)
  }
}

/**
 * A common superclass for saved actions on tracking scenarios. This consists of a core action (which can be executed
 * synchronously or asynchronously), and an interface for things like logging, callbacks, and gathering information
 * about the action. Concrete instances supply these implementations.
 *
 * @tparam T
 *   The result type.
 */
abstract class DependencyTrackerAction[T] extends NodeCauseInfo {

  /**
   * Indicate whether this action updates a tracking scenario.
   *
   * @return
   *   Whether this action updates a tracking scenario.
   */
  def isUpdate: Boolean
  def isLowPriority = false
  def syncApply() = false

  /**
   * Work to be done before performing the action.
   */
  def beforeAction(): Unit = ()

  /**
   * Work to be done after performing the action.
   */
  def afterAction(): Unit = {
    token.release()
  }

  /**
   * Execute the action synchronously.
   */
  def executeSync(): Unit

  /**
   * Execute the action asynchronously.
   *
   * @param afterExecute
   *   Action to perform after execution.
   */
  def executeAsync(afterExecute: Long => Unit): Unit

  /**
   * The scheduler that will run this action if it runs asynchronously. Note that we use default NOT currentOrDefault
   * because current at the point we start running is often just the scheduler that the previous task completed in (and
   * that could be some strange scheduler that subsequent tasks shouldn't be running in, e.g. the aux scheduler).
   */
  protected def scheduler: Scheduler = Scheduler.defaultMT

  /**
   * The result of the action.
   */
  final var result: Try[T] = _

  /**
   * A callback to which to supply the result.
   */
  final var callback: Try[T] => Unit = _
  protected def shouldTime: Boolean

  /**
   * Optionally set to indicate the complexity.
   */
  private[tracking] var size = -1
  protected final val causeInfo = if (Settings.trackingScenarioCommandCauseCapture) {
    val stack = if (EvaluationContext.isInitialised) {
      val psb: PrettyStringBuilder = new PrettyStringBuilder
      psb.indent()
      EvaluationContext.currentNode.waitersToFullMultilineNodeStack(false, psb)
      psb.toString
    } else "No optimus context"
    val ex = new Exception(s"trace for causality${System.lineSeparator()} --- $stack")
    Some(ex)
  } else None

  final def attachCauseInfoAndScenarioStack(node: NodeTask): Unit = {
    if (node.scenarioStack() eq null) node.attach(applyInScenarioStack)
    // we add this as a waiter, as this provide cause info for debug, in waitersToCallStack
    node.tryAddToWaiterList(this)
  }

  final def attachCauseInfoAndScenarioStack(nodes: Seq[NodeTask]): Unit = {
    nodes foreach { node =>
      if (node.scenarioStack() eq null) node.attach(applyInScenarioStack)
      node.tryAddToWaiterList(this)
    }
  }

  protected def desc: String = getClass.getName
  protected def stack: String = causeInfo match {
    case None => ""
    case Some(e) =>
      val stringWriter = new StringWriter
      stringWriter.append('\n')
      val pw = new PrintWriter(stringWriter)
      e.printStackTrace(pw)
      stringWriter.toString
  }
  protected def sourceLoc: String = SourceLocator.sourceOf(getClass)
  def details: String =
    if (Settings.trackingScenarioActionStackCaptureEnabled)
      s"Caused by DependencyTrackerAction [$desc] - $sourceLoc $stack"
    else ""
  protected def sizeString: String = if (size == -1) "" else s"for size $size"

  /**
   * Generate a log message indicating progress.
   *
   * @param stage
   *   The stage of operation.
   */
  protected def logProgress(stage: DependencyTrackerActionProgress): Unit
  protected def alreadyDisposed() = new AlreadyFailedNode[T](new GenericRTException("scenario disposed"))

  /**
   * Send the result to the callback. This generates the necessary logging messages and handles any exceptions.
   */
  final def raiseCallback(): Unit = {
    if ((Settings.timeTrackingScenarioUpdateCommands || Settings.timeTrackingScenarioUpdateCommands) && shouldTime)
      logProgress(DependencyTrackerActionProgress.BEFORE_CALLBACK)

    if (callback ne null) {
      try { callback(result) }
      catch {
        case e: Throwable =>
          DependencyTrackerRoot.log.error(s"Callback on TS should not leak exceptions on a callback from $this", e)
      }
    } else if (result.isFailure)
      DependencyTrackerRoot.log.error(s"no callback to capture failure on a callback from $this", result.failed.get)

    if ((Settings.timeTrackingScenarioUpdateCommands || Settings.timeTrackingScenarioUpdateCommands) && shouldTime)
      logProgress(DependencyTrackerActionProgress.AFTER_CALLBACK)
  }

  // Support for blocking
  final private var cdl: CountDownLatch = _
  private[tracking] def hasLatch = cdl ne null
  final def prepareWait(): Unit = cdl = new CountDownLatch(1)
  final def wakeUp(): Boolean = if (cdl eq null) false else { cdl.countDown(); true }
  final def await(): Unit = {
    if (!cdl.await(Settings.trackingStallPrintIntervalSecs, TimeUnit.SECONDS)) {
      val start = System.currentTimeMillis()
      DependencyTrackerRoot.log.warn(s"start waiting for tracking scenario to be ready to process $this")
      while (!cdl.await(Settings.trackingStallPrintIntervalSecs, TimeUnit.SECONDS)) {
        DependencyTrackerRoot.log.warn(s"still waiting for tracking scenario to be ready to process $this")
        GraphDiagnostics.dependencyTrackerStall()
        ThreadDumper.dumpThreadsToLog(summary = "Thread dump as waiting is too long")
        checkForDeadlock()
      }
      DependencyTrackerRoot.log.info(
        s"finished waiting for tracking scenario to be ready to process $this. Total time ${System.currentTimeMillis() - start}ms")
    }
  }
  // TODO (OPTIMUS-19245): provide full diagnosis and recovery from deadlock between commands
  private def checkForDeadlock(): Unit = {
    val current = EvaluationContext.currentNode
    var commands = Set.empty[DependencyTrackerAction[_]]
    object deadlockVisitor extends NodeTask.Visitor {
      override protected def visit(info: NodeCauseInfo, depth: Int): Unit = info match {
        case command: DependencyTrackerAction[_] => commands += command
      }
    }
    current.forAllWaiters(deadlockVisitor)
    if (commands.nonEmpty) {
      DependencyTrackerRoot.log.warn(
        s"we are waiting to start a command $this, but this action has ${commands.size} command(s) on its waiters. There may be a deadlock - $commands")
      DependencyTrackerRoot.log.warn(EvaluationContext.currentNode.waitersToFullMultilineNodeStack())
    }
  }

  // Used for graph diagnostics
  def writeWorkInfo(sb: PrettyStringBuilder): Unit = {
    sb.appendln(getClass.getSimpleName)
  }

  /**
   * Get the scenario stack to attach the node.
   */
  protected def applyInScenarioStack: ScenarioStack = ScenarioStack.constantNC

  def logSummary: String = getClass.getSimpleName

  private var maybeLogEnqueuedTime: Option[Long] = None

  /**
   * Helper method for debugging how long an action spends waiting on the queue
   */
  def logEnqueued(): Unit = maybeLogEnqueuedTime = Some(System.nanoTime())

  def isStalling: Boolean =
    maybeLogEnqueuedTime.exists(
      System.nanoTime() - _ > TimeUnit.SECONDS.toNanos(Settings.trackingScenarioActionTimeLimit))

  /**
   * Get the cause to use for starting a batch (most common use) or for EvaluateNodeKey(s) actions.
   *
   * @return
   *   The cause to use for starting a batch or other TrackingScenarioAction.
   */
  lazy val cause: EventCause = {
    val sb = new PrettyStringBuilder
    writeWorkInfo(sb)
    TrackingActionEventCause(sb.toString.stripLineEnd) // don't want a new line baked into a TrackingActionEventCause
  }

  // must be initialised here (rather than in beforeAction) because if beforeAction throws an exception
  // (which cause.getToken can do), then we end up with a null field and get an NPE in token.release when we run
  // afterAction (which still can run even if beforeAction failed - see optimus.graph.tracking.DependencyTrackerQueue.executeTry)
  protected val token: EventCause.Token = cause.createAndTrackToken()

  /**
   * For monitoring via the graph debugger
   */
  private var dtqEvent: DTQEvent = _
  private[tracking] def recordStart(scenario: ScenarioReference): Unit = {
    dtqEvent = DTQEventCounter.report(scenario, cause, isUpdate)
  }

  private[tracking] def recordStop(): Unit = {
    val event = dtqEvent
    if (event ne null) DTQEventCounter.reportCompleted(event)
    dtqEvent = null
  }
}

/**
 * A [[DependencyTrackerAction]] which can be cancelled partway through its execution.
 */
private[graph] trait DependencyTrackerActionCancellable extends DependencyTrackerAction[Unit] {

  /**
   * Cancel this action. Returns a new action to be rescheduled, if desired.
   */
  private[graph] def cancel(): Option[DependencyTrackerActionCancellable]
  final override def isLowPriority: Boolean = true
}

/**
 * Common implementations for evaluation-type actions (not updates). This supplies implementations for some of the
 * interface, but not the actual actions.
 *
 * @tparam T
 *   The result type.
 */
trait DependencyTrackerActionEvaluateBase[T] extends DependencyTrackerAction[T] {

  /**
   * This does not update.
   *
   * @return
   *   Returns false.
   */
  override def isUpdate = false

  override protected final def shouldTime: Boolean = Settings.timeTrackingScenarioEvaluate
  private var time = if (Settings.timeTrackingScenarioEvaluate) {
    DependencyTrackerRoot.commandCreated(this)
    System.nanoTime
  } else 0L
  override protected final def logProgress(progress: DependencyTrackerActionProgress): Unit = {
    val now = System.nanoTime
    val diffRaw = now - time
    val diff = diffRaw / 1000 / 1000.0
    if (Settings.timeTrackingScenarioEvaluateCommands)
      DependencyTrackerRoot.log.info(s"Update Action time to $progress ${getClass.getSimpleName} ${diff}ms $sizeString")
    if (Settings.timeTrackingScenarioCommandSummary)
      DependencyTrackerRoot.commandProgress(this, progress, diffRaw)
    time = now
  }

  /**
   * Generates a logging message for starting the action.
   *
   * @param q
   *   Ignored.
   */
  override def beforeAction(): Unit = {
    if (Settings.timeTrackingScenarioEvaluate) logProgress(DependencyTrackerActionProgress.START_DELAY)
    super.beforeAction()
  }

  /**
   * Generates a logging message for ending the action.
   *
   * @param q
   *   Ignored.
   */
  override def afterAction(): Unit = {
    if (Settings.timeTrackingScenarioEvaluate) logProgress(DependencyTrackerActionProgress.END_ACTION)
    super.afterAction()
  }
}

/**
 * Common implementation for actions that involve evaluating a single Node. This supplies implementations for
 * executeSync and executeAsync. This can be mixed with DependencyTrackerActionEvaluateBase.
 *
 * @tparam T
 *   The result type.
 */
trait DependencyTrackerActionOneNodeBase[T] extends DependencyTrackerAction[T] {
  protected def disposed: Boolean
  protected def targetScenarioName: String

  /**
   * Get the node to be evaluated.
   *
   * @return
   *   The node to be evaluated.
   */
  protected def asNode: Node[T]

  /**
   * generally will be a failure, but special case is for a dispose action on already disposed
   *
   * @return
   */
  private[tracking] def alreadyDisposedResult: Try[T] = {
    val ex = new DependencyTrackerDisposedException(targetScenarioName)
    causeInfo foreach ex.addSuppressed

    Failure[T](ex)
  }

  /**
   * Evaluate the node synchronously, then use it to set the result.
   */
  final override def executeSync(): Unit = {
    if (disposed) result = alreadyDisposedResult
    else {
      val node = asNode
      attachCauseInfoAndScenarioStack(node)
      EvaluationContext.evaluateSync(node :: Nil)
      result = Try(node.result)
    }
  }

  /**
   * Evaluate the node asynchronously, then use it to set the result.
   *
   * @param afterExecute
   *   Action to perform after execution.
   */
  final override def executeAsync(afterExecute: Long => Unit): Unit = {
    if (disposed) {
      result = alreadyDisposedResult
      // execute callback asynchronously to avoid stack overflow when there are a lot of actions on the queue
      scheduler.evaluateAsync(ScenarioStack.constantNC)(afterExecute(0L), null)
    } else {
      val node = asNode
      attachCauseInfoAndScenarioStack(node)
      if (Settings.trackingScenarioLoggingEnabled) DependencyTrackerLogging.beforeRunNodes(node :: Nil)
      scheduler.evaluateNodeAsync(
        node,
        true,
        true,
        { r: Try[T] =>
          result = r
          afterExecute(node.getFirstStartTime)
        },
        // important to override callbackSS otherwise we are called back in node's SS which may have been cancelled,
        // in which case the callback would never run
        cbSS = ScenarioStack.constantNC
      )
    }
  }
}
object DependencyTrackerActionManyNodesBase {
  private[tracking] val successUnit = Success(())
}

/**
 * A base implementation for actions that evaluate a set of nodes, but ignore their results.\ This can be mixed with
 * DependencyTrackerActionEvaluateBase.
 */
trait DependencyTrackerActionManyNodesBase extends DependencyTrackerAction[Unit] {

  /**
   * Get the set of nodes to evaluate.
   *
   * @return
   *   The set of nodes to evaluate.
   */
  protected def asNodes: Seq[NodeTask]
  protected def disposed: Boolean
  protected def targetScenarioName: String
  private final def alreadyDisposedResult: Try[Unit] = {
    val ex = new DependencyTrackerDisposedException(targetScenarioName)
    causeInfo foreach ex.addSuppressed

    Failure[Unit](ex)
  }

  /**
   * Evaluate the nodes synchronously, ignoring their results.
   */
  final override def executeSync(): Unit = {
    if (disposed) result = alreadyDisposedResult
    else {
      val nodes = asNodes
      if (Settings.timeTrackingScenarioCommandSummary) DependencyTrackerRoot.recordSize(this, nodes.size)
      attachCauseInfoAndScenarioStack(nodes)
      EvaluationContext.evaluateSync(nodes)
      result = DependencyTrackerActionManyNodesBase.successUnit
    }
  }

  /**
   * Evaluate the nodes asynchronously, ignoring their results.
   *
   * @param afterExecute
   *   Action to perform after execution.
   */
  final override def executeAsync(afterExecute: Long => Unit): Unit = {
    if (disposed) {
      result = alreadyDisposedResult
      // execute callback asynchronously to avoid stack overflow when there are a lot of actions on the queue
      scheduler.evaluateAsync(ScenarioStack.constantNC)(afterExecute(0L), null)
    } else {
      val nodes = if (disposed) Nil else asNodes
      OGTrace.observer.enqueueFollowsSequenceLogic(OGSchedulerContext.current.getCurrentNodeTask, Int.MaxValue)
      if (Settings.timeTrackingScenarioCommandSummary) DependencyTrackerRoot.recordSize(this, nodes.size)
      if (Settings.trackingScenarioLoggingEnabled) DependencyTrackerLogging.beforeRunNodes(nodes)
      attachCauseInfoAndScenarioStack(nodes)
      scheduler.evaluateNodesAsync(
        nodes,
        doneCallback = { startTime =>
          result = DependencyTrackerActionManyNodesBase.successUnit
          afterExecute(startTime)
        })
    }
  }
}

/**
 * Base implementation of evaluation-type actions that evaluate a single node.
 *
 * @tparam T
 *   The result type.
 */
trait DependencyTrackerActionEvaluateOneNodeBase[T]
    extends DependencyTrackerActionEvaluateBase[T]
    with DependencyTrackerActionOneNodeBase[T]

/**
 * A concrete evaluation-type action whose action consists of evaluating a given function as a Node.
 *
 * @param expr
 *   The function to nodify and evaluate as the action.
 * @tparam T
 *   The result type.
 */
abstract class DependencyTrackerActionEvaluate[T](expr: () => T) extends DependencyTrackerActionEvaluateOneNodeBase[T] {
  override def asNode: Node[T] = new DependencyTrackerActionTask[T](DependencyTrackerActionTask.evaluate, cause, expr)
}

abstract class DependencyTrackerActionUpdate[T]
    extends DependencyTrackerAction[T]
    with DependencyTrackerActionOneNodeBase[T] {

  /**
   * The update function, which is used to build the Node.
   *
   * @return
   *   The result of the action.
   */
  protected def doUpdate(): T

  /**
   * Wrap doUpdate in a DependencyTrackerActionTask, which gives us the Node for this action.
   *
   * @return
   *   The doUpdate implementation converted to a Node.
   */
  override def asNode: Node[T] =
    if (disposed)
      alreadyDisposed()
    else
      new DependencyTrackerActionTask[T](DependencyTrackerActionTask.update, cause, () => doUpdate())

  /**
   * The constant true.
   *
   * @return
   *   The constant true.
   */
  final override def isUpdate = true
  override protected final def shouldTime: Boolean = Settings.timeTrackingScenarioUpdate
  private var time = if (Settings.timeTrackingScenarioUpdate) {
    DependencyTrackerRoot.commandCreated(this)
    System.nanoTime
  } else 0L
  override protected final def logProgress(progress: DependencyTrackerActionProgress): Unit = {
    val now = System.nanoTime
    val diffRaw = now - time
    val diff = diffRaw / 1000 / 1000.0
    if (Settings.timeTrackingScenarioUpdateCommands)
      DependencyTrackerRoot.log.info(s"Update Action time to $progress ${getClass.getSimpleName} ${diff}ms $sizeString")
    if (Settings.timeTrackingScenarioCommandSummary)
      DependencyTrackerRoot.commandProgress(this, progress, diffRaw)
    time = now
  }

  /**
   * Generate logging messages and start a batch in the given queue.
   *
   * @param q
   *   Queue in which to start a batch.
   */
  final override def beforeAction(): Unit = {
    if (Settings.timeTrackingScenarioUpdate) logProgress(DependencyTrackerActionProgress.START_DELAY)
  }

  /**
   * Generate logging messages and end a batch in the queue.
   */
  final override def afterAction(): Unit = {
    if (Settings.timeTrackingScenarioUpdate) logProgress(DependencyTrackerActionProgress.END_ACTION)
    super.afterAction()
    if (Settings.timeTrackingScenarioUpdate) logProgress(DependencyTrackerActionProgress.NOTIFY_ACTION)
  }
}
