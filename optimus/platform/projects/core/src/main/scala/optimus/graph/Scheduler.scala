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

import optimus.core.CoreHelpers

import scala.annotation.nowarn
import java.util
import java.util.concurrent.atomic.AtomicInteger
import optimus.debug.InstrumentationConfig
import optimus.dist.DistPluginTags.JobTaskIdTag
import optimus.graph.OGScheduler.Context
import optimus.graph.Scheduler.ContextSnapshot
import optimus.graph.diagnostics.ProfilerUIControl
import optimus.graph.diagnostics.StallDetector
import optimus.graph.diagnostics.rtverifier.RTVerifier
import optimus.graph.diagnostics.sampling.SamplingProfiler.SchedulerCounts
import optimus.graph.diagnostics.sampling.SchedulerCounter
import optimus.platform.annotations.nodeLift
import optimus.platform.annotations.nodeLiftByName
import optimus.platform.annotations.nodeSync
import optimus.platform.annotations.nodeSyncLift
import optimus.platform.EvaluationContext
import optimus.platform.EvaluationQueue
import optimus.platform.PluginHelpers.toNode
import optimus.platform.ScenarioStack
import optimus.platform.annotations.closuresEnterGraph
import optimus.platform.annotations.deprecating
import optimus.platform.impure
import optimus.platform.util.PrettyStringBuilder
import optimus.platform.util.ThreadDumper.ThreadInfos
import org.slf4j.LoggerFactory

import java.util.function.Predicate
import scala.util.Failure
import scala.util.Success
import scala.util.Try

//noinspection ScalaUnusedSymbol // ServiceLoader
class GraphSchedulerCounter extends SchedulerCounter {
  override def getCounts: SchedulerCounts = Scheduler.getCounts
  override def numThreads: Option[Int] = Scheduler.numThreads
}

object Scheduler {
  private val log = LoggerFactory.getLogger(Scheduler.getClass)

  final case class SchedulerSnapshot(scheduler: Scheduler, contexts: ContextSnapshot)
  final case class ContextSnapshot(waiting: Array[Context], working: Array[Context], blocked: Array[Context])

  if (DiagnosticSettings.diag_showConsole) {
    ProfilerUIControl.startGraphDebugger()
    if (DiagnosticSettings.diag_stopOnGraphStart) {
      ProfilerUIControl.brk()
    }
  }
  if (DiagnosticSettings.outOfProcessAppConsole) {
    log.info("Waiting for remote Graph Debugger connection...")
    ProfilerUIControl.brk()
  }
  if (InstrumentationConfig.setExceptionHookToTraceAsNodeOnStartup)
    InstrumentationSupport.registerTraceExceptionAsNode()

  if (DiagnosticSettings.enableRTVerifier)
    RTVerifier.register()

  CoreHelpers.sendDebugInfo("")

  private val _schedulers = new util.WeakHashMap[Scheduler, AnyRef]()

  private[optimus] def schedulers: List[Scheduler] = {
    var schedulers = List.empty[Scheduler]
    _schedulers.synchronized {
      _schedulers.keySet().forEach(scheduler => schedulers ::= scheduler)
    }
    schedulers
  }

  def snapshots(schedulers: List[Scheduler] = Scheduler.schedulers): List[SchedulerSnapshot] =
    schedulers.map(scheduler => SchedulerSnapshot(scheduler, scheduler.getContexts))

  @deprecating("Only used in OGScheduler")
  def registerScheduler(scheduler: Scheduler): Unit = _schedulers.synchronized {
    StallDetector.ensureInitialised()
    _schedulers.put(scheduler, null)
  }

  // This should be shared.
  private var _default = new OGScheduler()

  private def default: Scheduler = {
    if (_default.isSingleThreaded) new OGScheduler()
    else _default
  }

  /** Current Scheduler registered to the current thread */
  final def current: Scheduler = OGSchedulerContext.current().scheduler

  /** Current Scheduler registered to the current thread or default if evaluation context is not initialized */
  final def currentOrDefault: Scheduler = {
    if (EvaluationContext.isInitialised) current
    else default
  }

  /**
   * returns the default multithreaded scheduler, or throws if in ST mode
   */
  private[optimus] def defaultMT: Scheduler = {
    Settings.assertNotSingleThreadedScheduler()
    _default
  }

  /**
   * This calls allows one-way upgrade default scheduler to MT scheduler from then on, it will just update the ideal
   * thread count
   */
  private[optimus] def upgradeDefaultScheduler(idealThreadCount: Int): Unit = {
    if (idealThreadCount == 0)
      throw new GraphException("upgradeDefaultScheduler can only be called to upgrade to MT scheduler")
    if (_default.isSingleThreaded) {
      val newScheduler = new OGScheduler()
      newScheduler.setIdealThreadCount(idealThreadCount)
      _default = newScheduler
    } else
      _default.setIdealThreadCount(idealThreadCount)
  }

  /**
   * Just like safeStackDepth in OGSchedulerContext (which is per thread), except here we need ThreadLocal since
   * Scheduler instance might be shared across threads
   */
  private val safeStackDepth: ThreadLocal[Integer] = ThreadLocal.withInitial(() => 0)

  def getCounts: SchedulerCounts = {
    val sched = snapshots()
    val counts = sched.foldLeft(SchedulerCounts.empty) { case (counts, SchedulerSnapshot(_, contexts)) =>
      SchedulerCounts(
        working = counts.working + contexts.working.length,
        waiting = counts.waiting + contexts.waiting.length,
        blocked = counts.blocked + contexts.blocked.length
      )
    }
    counts
  }

  def numThreads: Option[Int] = if (EvaluationContext.isInitialised)
    Try(currentOrDefault.getIdealThreadCount).toOption
  else None
}

private object PT {
  def unapply(ntsk: NodeTask): Option[JobTaskIdTag] = ntsk.scenarioStack.findPluginTag(JobTaskIdTag)
}

/**
 *   1. Consider extending to provide more details 2. Do NOT hold onto instances of this class they contain NodeTask
 *      that can lead to a memory leak Note - atieoc = awaitedTaskInfoEndOfChain
 */
final case class SchedulerStallSource(awaitedTasks: Array[OGScheduler.AwaitedTasks], syncStackPresent: Boolean) {
  private def sample = awaitedTasks.head
  // We pick any ctx because we want stall times to add up and don't want to double report
  def awaitedTask: NodeTask = sample.task()
  def atieoc: NodeTaskInfo = sample.endOfChainTaskInfo()
  def plugin: SchedulerPlugin = sample.endOfChainPlugin()
  def pluginType: PluginType = sample.endOfChainPluginType()
  def name: String = atieoc.name()
  def jobTaskId: Option[JobTaskIdTag] = awaitedTasks.map(_.task).collectFirst { case PT(tag) => tag }
}

abstract class Scheduler extends EvaluationQueue {

  import Scheduler.safeStackDepth

  override def delayOnChildCompleted(parent: NodeTask, child: NodeTask): Boolean = {
    val depth = safeStackDepth.get
    if (delayOnChildCompleted(parent, child, depth))
      true
    else {
      safeStackDepth.set(depth + 1)
      false
    }
  }

  override def afterOnChildCompleted(): Unit = {
    val depth = safeStackDepth.get
    safeStackDepth.set(depth - 1)
  }

  /**
   * Marks task as runnable and enqueues on scheduler queues WARNING: This is not the function you are looking for!
   * Verify the need to use this function with someone
   */
  final def markAsRunnableAndEnqueue(task: NodeTask): Unit = markAsRunnableAndEnqueue(task, trackCompletion = false)

  /**
   * Marks task as runnable and enqueues on scheduler queues WARNING: This is not the function you are looking for!
   * Verify the need to use this function with someone
   */
  def markAsRunnableAndEnqueue(task: NodeTask, trackCompletion: Boolean): Unit

  /**
   * Before exiting Scheduler's last thread, track and make sure this task is completed
   */
  def enqueueAndTrackCompletion(task: NodeTask): Unit

  final def enqueueAndTrackCompletion(task: NodeTask, doTrack: Boolean): Unit = {
    if (doTrack) enqueueAndTrackCompletion(task)
    else enqueue(task)
  }

  /**
   * Always spins on the graphs thread pool threads and enters the runAndWait WARNING: This is not the function you are
   * looking for! Verify the need to use this function with someone
   */
  def evaluateOnGraphThreadAndTrackCompletion(nodeTask: NodeTask): Unit

  /**
   * Negative count implies a portion of cores (i.e. -1 all cores, -2 half the cores)
   */
  def setIdealThreadCount(idealCount: Int): Unit
  def setIdealThreadCount(idealCount: Int, minimumIfNegative: Int, maximumIfNegative: Int): Unit
  def getIdealThreadCount: Int

  /** Currently shuts down all idle threads */
  def shutdown(): Unit

  /**
   * Will evaluate all the input nodes on a thread or threads different then the current thread Returns immediately
   * after scheduling nodes Calls doneCallback when all nodes are completed. Note: callback will be called on the
   * calling thread if there were no nodes to execute otherwise callback will be called on a different thread
   *
   * maySAS: may have sync -> async -> sync transitions
   */
  final private[graph] def evaluateNodesAsync(
      nodes: collection.Seq[NodeTask],
      doneCallback: Long => Unit,
      callbackInline: Boolean = false,
      effectiveEnqueuer: Awaitable = null): Unit = {
    // Lazy calculation of currentScenarioStack allows a thread to schedule work without initializing EvaluationContext
    var _currentScenarioStack: ScenarioStack = null
    @volatile var expectedCount = 0

    def currentScenarioStack = {
      if (_currentScenarioStack eq null) _currentScenarioStack = EvaluationContext.scenarioStack
      _currentScenarioStack
    }

    val callerScenarioStack = EvaluationContext.scenarioStackOrNull
    val scenarioStackForCallback = if (callerScenarioStack ne null) callerScenarioStack else ScenarioStack.constantNC
    val count = new AtomicInteger(-1) // Avoid race by having this counter lower until the end
    val completionCounter = new Node[Unit] {
      attach(scenarioStackForCallback)
      override def onChildCompleted(eq: EvaluationQueue, node: NodeTask): Unit = {
        if (count.incrementAndGet() == expectedCount) {
          // avoid deadlocks on ST scheduler by running inline, else on MT enqueue to avoid growing stack
          // Important: we run the callback inline since we may (briefly) block the current scheduler thread while waiting
          // in DependencyTrackerBatchUpdaterRoot#StateManager.waitForEvaluations(), which could cause deadlock if no other graph threads were available
          if (callbackInline || Settings.threadsIdeal == 0) run(null)
          else eq.enqueue(this)
        }
      }
      override def run(ec: OGSchedulerContext): Unit = doneCallback(this.getFirstStartTime)
      override def executionInfo(): NodeTaskInfo = NodeTaskInfo.SchedulerAsyncCallback
    }

    if (DiagnosticSettings.awaitStacks)
      AwaitStackManagement.setLaunchData(effectiveEnqueuer, completionCounter, false, 0)

    for (v <- nodes) {
      expectedCount += 1
      // Uses last because the nodes given to us already have awaiters and we don't want to call our done call back
      // until all of those awaiters have also been run.
      v.continueWithLast(completionCounter, this)
      if (v.scenarioStack eq null) v.attach(currentScenarioStack)
      v.poisonCausality() // We don't have any causality information for our caller
      enqueueAndTrackCompletion(v, doTrack = true)
    }

    completionCounter.onChildCompleted(Scheduler.this, null)
  }

  /**
   * There is a small benefit to be gained by using the graph internal threads, as this enables some optimisations, such
   * as slightly faster access to EvaluationContext.current, reduced use of ThreadLocals etc. If you think that your
   * application could benefit from this then please discuss this with the graph team. It is marked @deprecating to
   * avoid use without discussion There could be a penalty for using this API as well, for example if you enter graph
   * often via this API thread context switch penalty will be high.
   */
  // noinspection ScalaUnusedSymbol
  @impure
  @deprecating(suggestion = "discuss with the graph team")
  @nodeSync
  @nodeSyncLift
  final def evaluateOnGraphWorkerThread[T](@nodeLiftByName @nodeLift action: => T): T =
    evaluateOnGraphWorkerThread$withNode(toNode(action _))
  // noinspection ScalaUnusedSymbol
  final def evaluateOnGraphWorkerThread$queued[T](action: Node[T]): Node[T] =
    throw new UnsupportedOperationException("Cannot call evaluateOnGraphWorkerThread directly from a node")

  @nowarn("msg=10500 optimus.graph.Scheduler.evaluateNodeOnGraphThreadSync")
  final def evaluateOnGraphWorkerThread$withNode[T](action: Node[T]): T = {
    action.attach(EvaluationContext.scenarioStack)
    evaluateNodeOnGraphThreadSync(action)
    action.get
  }

  @deprecating(suggestion = "discuss with the graph team")
  final def evaluateNodeOnGraphThreadSync(node: Node[_]): Unit = {
    EvaluationContext.verifyOffGraph(ignoreNonCaching = false, beNice = false)
    if (node.scenarioStack() eq null)
      throw new IllegalArgumentException("cannot evaluate node without scenario stack on graph threads")
    if (OGSchedulerContext.onOgThread) node.get
    else {
      val done = new NodeAwaiterWithLatch(1)
      evaluateOnGraphThreadAndTrackCompletion(node)
      node.continueWithIfEverRuns(done, EvaluationContext.current)
      done.latch.await()
    }
  }

  final def evaluateNodeAsync(node: NodeTask): Unit =
    evaluateNodeAsync(node, maySAS = true, trackCompletion = true, cb = null)

  type CBType[T] = Try[T] => Unit

  final private[optimus] def evaluateNodeAsync[T](node: NodeTask, doneCallback: CBType[T]): Unit =
    evaluateNodeAsync(node, maySAS = true, trackCompletion = true, doneCallback)

  final private class SchedulerAsyncCallback[T](node: NodeTask, doneCallback: CBType[T], callbackInline: Boolean)
      extends Node[Unit] {

    override def onChildCompleted(eq: EvaluationQueue, node: NodeTask): Unit = {
      // avoid deadlocks on ST scheduler by running inline, else on MT enqueue to avoid growing stack
      // Important: we run the callback inline since we may (briefly) block the current scheduler thread while waiting
      // in DependencyTrackerBatchUpdaterRoot#StateManager.waitForEvaluations(), which could cause deadlock if no other graph threads were available
      if (callbackInline || Settings.threadsIdeal == 0) run(null)
      else eq.enqueue(this)
    }

    override def run(ec: OGSchedulerContext): Unit = {
      if (node.isDone) {
        doneCallback(
          if (node.isDoneWithResult) Success(node.resultObject().asInstanceOf[T])
          else Failure(node.exception())
        )
        OGTrace.completed(ec, this)
      }
    }

    override def executionInfo(): NodeTaskInfo = NodeTaskInfo.SchedulerAsyncCallback
  }

  /**
   * Evaluates node asynchronously, in its attached ScenarioStack or else the current ScenarioStack if unattached. Calls
   * doneCallback (if supplied) after node completes, either in callbackSS if specified or else the evaluation
   * ScenarioStack.
   *
   * maySAS: may have sync -> async -> sync transitions
   */
  final def evaluateNodeAsync[T](
      node: NodeTask,
      maySAS: Boolean,
      trackCompletion: Boolean,
      cb: CBType[T],
      callbackInline: Boolean = false,
      cbSS: ScenarioStack = null,
      effectiveEnqueuer: Awaitable = null): Unit = {
    if (node.scenarioStack() eq null) node.attach(EvaluationContext.scenarioStack)
    if (maySAS) node.poisonCausality()

    if (cb eq null) {
      if (DiagnosticSettings.awaitStacks)
        AwaitStackManagement.setLaunchData(effectiveEnqueuer, node, false, 0)

      enqueueAndTrackCompletion(node, trackCompletion)
    } else {
      val awaiter = new SchedulerAsyncCallback(node, cb, callbackInline)

      if (DiagnosticSettings.awaitStacks)
        AwaitStackManagement.setLaunchData(effectiveEnqueuer, awaiter, false, 0)

      awaiter.attach(if (cbSS ne null) cbSS else node.scenarioStack)
      node.continueWithLast(awaiter, this)
      enqueueAndTrackCompletion(node, trackCompletion)
    }
  }

  /**
   * Evaluates given function on one of the optimus scheduler threads and calls back once completed
   */
  @closuresEnterGraph
  private def evaluateAsyncSAS[T](scenarioStack: ScenarioStack)(
      f: => T,
      maySAS: Boolean,
      cb: CBType[T] = null,
      callbackInline: Boolean = false): Unit = {
    val node = new CompletableNodeM[T] {
      override def run(ec: OGSchedulerContext): Unit = completeWithResult(f, ec)
      override def executionInfo(): NodeTaskInfo = NodeTaskInfo.SchedulerAsyncEvaluate
    }
    node.attach(scenarioStack)
    evaluateNodeAsync[T](node, maySAS, trackCompletion = false, cb, callbackInline)
  }

  final def evaluateAsync[T](
      scenarioStack: ScenarioStack)(@closuresEnterGraph f: => T, doneCallback: CBType[T] = null): Unit = {
    evaluateAsyncSAS(scenarioStack)(f, maySAS = true, doneCallback)
  }

  final def evaluateAsync[T](@closuresEnterGraph f: => T): Unit = {
    evaluateAsyncSAS(EvaluationContext.scenarioStack)(f, maySAS = true, null)
  }

  override def scheduler: Scheduler = this

  def addOnOutOfWorkListener(callback: SchedulerCallback, autoRemove: Boolean): Unit
  def removeOutOfWorkListener(callback: SchedulerCallback): Unit
  def runOutOfWorkListeners(predicate: Predicate[SchedulerCallback]): Unit
  def reportCallsInFlightCountCleared(): Unit = ()

  /**
   * Returns Scheduler's local queue Has to to derive from ExecutionContext
   */
  def newEvaluationContext(scenarioStack: ScenarioStack): OGSchedulerContext

  def waitForCancelledNodesToStop(cs: CancellationScope): Unit

  /**
   * logWaitingStatus for all items in wait queues
   *
   * Includes stacktraces
   */
  def getAllWaitStatus(
      sb: PrettyStringBuilder,
      tempWaitQueue: Array[Context],
      tempWorkQueue: Array[Context],
      externallyBlockedQueue: Array[Context],
      threadInfos: ThreadInfos): String

  /**
   * logWaitingStatus for all items in wait queues
   *
   * Does not include stacktraces
   */
  def getAllWaitStatus(sb: PrettyStringBuilder, tempWaitQueue: Array[Context]): String

  /**
   * Take a snapshot of the current (waiters, workers, externallyBlocked)
   *
   * Used by GraphDiagnostics to dump threads for all schedulers at once, reducing the number of calls to ThreadDumper
   */
  def getContexts: ContextSnapshot

  /**
   * Returns Scheduler id
   */
  def getSchedulerId: Int

  def assertNotSingleThreaded(msg: String): Unit

  def assertAtLeastTwoThreads(msg: String): Unit
}
