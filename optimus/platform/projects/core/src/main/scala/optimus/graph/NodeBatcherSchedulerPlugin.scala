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

import optimus.core.MonitoringBreadcrumbs
import optimus.dist.DistPluginTags.GroupingNodeBatcherSchedulerPluginTag
import optimus.observability._
import optimus.platform.PluginHelpers.toNode
import optimus.platform._
import optimus.platform.annotations._

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.{ArrayList => JArrayList}
import java.util.{HashMap => JHashMap}
import scala.annotation.tailrec
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import msjava.slf4jutils.scalalog.getLogger

/**
 * Base class for a quick implementation of "batching" nodes T is result type of individual node NodeT is type of the
 * node created to @node def ...
 */
abstract class NodeBatcherSchedulerPluginBase[T, NodeT <: CompletableNode[T]] extends SchedulerPlugin {
  batcher =>

  type GroupKeyT >: Null <: AnyRef

  /**
   * Maximum time in millis that we will wait for new elements in an incomplete batch.
   *
   * If this delay is greater than zero, then we will trigger batches even if they are incomplete and there is the
   * possibility that more work is incoming. This shouldn't be needed in most cases because we automatically trigger
   * batches when the graph scheduler runs out of work. Applications that are very latency sensitive or that encounter
   * hangs when using multiple independent graph schedulers might benefit from changing this value.
   */
  def mustCallDelay: Long = Settings.defaultBatcherMustCallDelay

  /**
   * Minimum time in millis that we will wait for new elements even if the graph has completely ran out of work.
   *
   * If this delay is greater than zero, then we will wait to trigger an incomplete batch even if we are completely out
   * of work. This shouldn't be needed in most cases, and in fact it might severely impact performance by adding
   * avoidable delays on every batched call. UI applications where we are usually stalling while waiting for user input
   * can benefit from setting this to "debounce" expensive batchers under fast user action.
   */
  def mustWaitDelay: Long = Settings.defaultBatcherMustWaitDelay

  /**
   * By default, this is max number of nodes to batch. By overriding getNodeWeight you effectively turning this value
   * into a max weight to run Also see getGroupBatchSize
   */
  var maxBatchSize: Int = Int.MaxValue
  val propertyInfo: NodeTaskInfo = NodeBatcherSchedulerPluginBase.getPropertyInfo(getClass)

  /**
   * This function needs to be very quick and non-blocking or be @node ... Note: Both functions are implemented
   * with $queued returning null. This allows to either override with @node or just a regular function (which would be
   * faster) Returns Weight for a given node, when the sum of weights reaches maxBatchSize the batch is scheduled for
   * execution
   */
  protected def getNodeWeight(v: NodeT) = 1
  protected def getNodeWeight$queued(v: NodeT): NodeFuture[Int] = null

  /** Batch can report the number of outstanding nodes to batch */
  protected def getLimitTag: PluginTagKey[AtomicInteger] = null

  /**
   * This function needs to be very very quick and non-blocking
   * @return
   *   max batch size, given the group key
   */
  protected def getGroupMaxBatchSize(grp: GroupKeyT, n: NodeTask): Int = maxBatchSize

  protected def onFirstRun(bnode: BNode, grpKey: GroupKeyT): Unit = {}
  protected def run(ec: OGSchedulerContext, bnode: BNode, grpKey: GroupKeyT, inputs: Seq[NodeT]): Unit

  /**
   * Consider the following case:
   * <p>
   * def foo(arg) = x + arg  // depends on x <br>
   * foo(arg) + given(x:=3) { foo(arg) }
   * <p>
   * If foo is being batched before the dependency mask on foo is ever computed. This will result
   * in on of the proxies waiting to see if the foo can be reused. Which effectively would result in NO batching.
   * This flag combined with breakDependenciesInAdapt will tell the scheduler to detect and break those dependencies.
   * This is not a trivial task but usually for batch sensitive applications worth it (hence default)
   * <p>
   * However for small repeatable batchers this can also be a total killer. Especially because after 'learning' the
   * mask there is zero penalty on batching.
   * <p>
   * One solution would be to feed the mask via PGO
   * <p>
   * Another is to start with this flag set to true and after a few batches auto-turn it off. For example
   * After detecting that masks in system haven't changed in "some time"
   */
  protected def prioritiseConcurrency: Boolean = true
  /* Only checked if prioritiseConcurrency == true, see the comment above */
  protected def breakDependenciesInAdapt: Boolean = true

  protected case class State(count: Int, weight: Int, nodes: List[NodeT])

  def cloneNode(node: NodeT): NodeT = {
    val cloned = node.cloneTask().asInstanceOf[NodeT]
    cloned.setWaitingOn(null)
    cloned
  }

  /** For test diagnostics */
  def verifyExpectedState(scheduler: Scheduler, batchSize: Int): Unit = ()

  final protected class BNode(
      override val priority: Int,
      useScope: Boolean,
      batchScopeKeys: Set[BatchScopeKey],
      initState: State,
      grpKey: GroupKeyT)
      extends CompletableNode[Seq[T]]
      with OutOfWorkListener {

    override def mustCallDelay(): Long = batcher.mustCallDelay
    override def mustWaitDelay(): Long = batcher.mustWaitDelay

    // Set an enqueuer arbitrarily from the first node in the batch, so at least something shows up
    // in profiling.  It's misleading surprisingly infrequently.
    initState.nodes.headOption.foreach(OGTrace.enqueue(_, this, false))

    def this(
        priority: Int,
        useScope: Boolean,
        batchScopeKeys: Set[BatchScopeKey],
        node: NodeT,
        groupKeyT: GroupKeyT) = {
      this(priority, useScope, batchScopeKeys, State(count = 1, weight = 1, List(node)), groupKeyT)
      attach(node.scenarioStack().asSiStack)
    }

    override def executionInfo: NodeTaskInfo = batcher.propertyInfo
    private val state = new AtomicReference(initState)
    // set to null after run takes all the nodes
    private var inputs: List[NodeT] = _
    private var __stableCalcTags: Node[StableCalcTags] = _

    // It cannot be cloned as it has significant internal state
    // due to its batching nature (i.e., it accumulates nodes and then runs them in batch).
    override def isClonable: Boolean = false
    override def reset(): Unit = throw new GraphInInvalidState("BNode doesn't support safe cloning/reset")

    override def limitTag: AnyRef = getLimitTag

    def notAcceptingNewNodes: Boolean = state.get eq null

    override def ready(outstandingTasks: JArrayList[NodeTask]): Boolean = {
      if (Settings.useScopedBatcher && useScope) {
        val cstate = state.get
        if ((cstate ne null) && cstate.count > 0) {
          val inputs = cstate.nodes
          // if there are still DSI calls in flight, let's wait for them to finish because they may result in more nodes
          // for us to batch (and more nodes is better because we have more flexibility in batching)
          // [SEE_XSFT_BATCH_SCOPE]
          val batchScope = inputs.head.scenarioStack.batchScope
          val ready = !batchScope.hasCallsInFlight(batchScopeKeys, outstandingTasks)
          if (!ready && DiagnosticSettings.batchScopeVerboseLogging) {
            NodeBatcherSchedulerPluginBase.log.info(s"$this detected pending work in BatchScope")
            batchScope.logBatchScopeStatus
          }
          ready
        } else true
      } else true
    }

    override def checkForNewWork(scheduler: Scheduler): Int = {
      if (prioritiseConcurrency) {
        val cstate = state.get
        if ((cstate ne null) && cstate.count > 0) {
          val it = cstate.nodes.iterator
          var newWorkAdded = false
          while (it.hasNext) {
            val task = it.next()
            newWorkAdded |= task.fixWaiterChainForXSFTFromBatcher(scheduler)
          }
          if (newWorkAdded) Int.MaxValue else cstate.count
        } else 0
      } else 0
    }

    /**
     * Batch scopes example to justify hasCallsInFlight optimisation below:
     *
     * {{{
     * def a = z + b
     * def b = givenBatchScope(DALScope) { c + d }
     * def c = { val v1 = DalEntity.get(1); distributedNode(v1) }
     * def d = { val v2 = DalEntity.get(2); distributedNode(v2) }
     * }}}
     *
     * Suppose the call to DALEntity.get(1) in c returns immediately (from cache). Suppose the call to DALEntity.get(2)
     * in d actually makes a DAL call and causes a graph stall. Scheduler stall triggers onStalledCallbacks and runs
     * BNode.onGraphStalled.
     *
     * distributedNode(v1) COULD run now but we would lose out on batching the two distributed nodes. Since b runs under
     * DALScope, and hasCallsInFlight(DALScopeKey) is true (because DALEntity.get(2) is still running) we wait for the
     * second DAL call to complete.
     *
     * This means we can batch the two distributedNode calls.
     * {{{
     * a
     *    z ....
     *          // other DAL calls under a different scope
     *
     *    b  (under DALScope)
     *      c
     *        val v = DalEntity.get(arg1)  // return immediately (from cache)
     *        distributedNode(v)  // [1]
     *      d
     *        val v = DalEntity.get(arg2)  // not in cache, so this waits/stalls, and onStalledCallbacks calls Batcher
     *        distributedNode(v)  // [2]
     * }}}
     */
    override def onGraphStalled(scheduler: Scheduler, outstandingTasks: JArrayList[NodeTask]): Unit = {
      val curr = state.get
      val batchSize = if (curr ne null) curr.count else 0
      verifyExpectedState(scheduler, batchSize)
      if (ready(outstandingTasks))
        tryToSchedule(scheduler)
      else if (batchSize > 0)
        scheduler.addOutOfWorkListener(this) // Add ourselves back to queue
    }

    def tryToSchedule(eq: EvaluationQueue): Unit = {
      // It's much safer to rely on the forward chain.
      // Note: this is possible to write to a batch node that is careful to be safe from any thread.
      // If this becomes needed we can customize and optionally turn off this call poisonCausality()

      // Set null on state atomically to mark batch as done
      val cstate = state.getAndSet(null)
      // Check that we actually had batched nodes
      if ((cstate ne null) && cstate.count > 0) {
        inputs = cstate.nodes

        val distInfo = GroupingNodeBatcherSchedulerPluginTag(inputs)
        var awaitingNodes = inputs
        while (awaitingNodes.nonEmpty) {
          val waiter = awaitingNodes.head
          // expose some info about what nodes are actually contained inside this batch to distribution
          waiter.replace(waiter.scenarioStack.withPluginTag(GroupingNodeBatcherSchedulerPluginTag, distInfo))

          // we take the causality (and thread ID) of the highest causality of our waiters. this prevents
          // other threads from taking on this work unless they have no sync frames (causalityID 0) *or* they are the
          // thread that is waiting on the highest causality waiter *and* don't have a further sync frame on the stack
          // (such threads are always safe to run this without risk of dependency inversion deadlock or false critical
          // sync stack reports)
          if (causalityID <= waiter.causalityID) // <= so that we always pick up at least one causalityThreadID
            setCausality(waiter.causalityID, waiter.causalityThreadID)

          // allow batch node to be discoverable via waitingOn chain
          waiter.setWaitingOn(this)
          awaitingNodes = awaitingNodes.tail
        }
        eq.enqueue(null, this)
      }
    }

    /**
     * Loop until we add to the list or the list becomes empty because batch node started to run already Returns one of
     * the following
     *   1. [null] Success: added to this batch node
     *   1. [State(500, all nodes)] Reached max a stole all the nodes from this batch node
     *   1. [State(1, List(v))] Was not able to add to this batch node (lost race on adding)
     */
    def addNode(v: NodeT, weight: Int): State = {
      val maxGrpSize = getGroupMaxBatchSize(grpKey, v)

      @tailrec
      def updateState(): State = {
        val currentState = state.get

        if (currentState eq null) {
          // Too late to add node to batch because the batch is already running...
          State(count = 1, weight = weight, List(v))
        } else {
          // Compute proposed new state
          val updatedState = State(
            count = currentState.count + 1,
            weight = currentState.weight + weight,
            nodes = v :: currentState.nodes)

          val (returnState, retainedState) =
            // if we're over the group size, return the batch (for execution) and retain nothing
            if (updatedState.weight >= maxGrpSize) (updatedState, null)
            // otherwise return nothing (which means we handled it), and retain the updated state
            else (null, updatedState)

          if (!state.compareAndSet(currentState, retainedState)) updateState()
          else returnState
        }
      }

      updateState()
    }

    private def scheduleStableCalcTags(ec: OGSchedulerContext): Unit = {
      val nf = stableCalcTags$newNode(grpKey, inputs)
      if (nf eq null) {
        try {
          __stableCalcTags = new AlreadyCompletedNode(stableCalcTags(grpKey, inputs))
        } catch {
          case e: Throwable =>
            __stableCalcTags = new AlreadyFailedNode[StableCalcTags](e)
        }
      } else {
        __stableCalcTags = nf.asNode$
        __stableCalcTags.attach(scenarioStack)
        __stableCalcTags.enqueueAttached.continueWith(this, ec)
      }
    }

    override def run(ec: OGSchedulerContext): Unit = {
      if (__stableCalcTags eq null) scheduleStableCalcTags(ec)
      // we don't use an `else` branch because __stableCalcTags might be an Already(Completed/Failed)Node
      if (__stableCalcTags.isDoneEx) {
        if (__stableCalcTags.isDoneWithException)
          completeWithException(__stableCalcTags.exception, ec)
        else {
          // result could still be null if the method returns `null`
          if (__stableCalcTags.result ne null) {
            inputs.foreach { i =>
              val ss = i.scenarioStack()
              val newSS = ss.withPluginTag(StableCalcTags, __stableCalcTags.result)
              i.replace(newSS)
            }
          }
          // grpKey.hashCode sometimes makes graph calls!!!!!!!!!!!
          // They should be correctly parented to the current batch node
          // and executed on some graph thread, hence on the first run
          onFirstRun(this, grpKey)
          batcher.run(ec, this, grpKey, inputs)
        }
      }
    }

    def completeBatch(eq: EvaluationQueue, results: Seq[T]): Unit = {
      completeWithResult(results, eq)
      completeNodes(eq, results)
    }

    private def completeNodes(eq: EvaluationQueue, results: Seq[T]): Unit = {
      val resultsIter = results.iterator

      for (node <- inputs.iterator) {
        if (resultsIter.hasNext) {
          node.setWaitingOn(null)
          node.combineInfo(this, eq)
          node.completeWithResult(resultsIter.next(), eq)
        } else {
          node.setWaitingOn(null)
          node.combineInfo(this, eq)
          node.completeWithException(
            new GraphException("Batcher returned fewer results that number of batched nodes"),
            eq)
        }
      }
    }

    def completeTryBatch(eq: EvaluationQueue, results: Seq[Try[T]]): Unit = {
      completeWithResult(Seq.empty, eq) // no one consumes the bnode result
      completeTryNodes(eq, results)
    }

    def completeTryNodes(eq: EvaluationQueue, results: Seq[Try[T]]): Unit = {
      // Inlined version of Collection.foreach2
      // TODO (OPTIMUS-10900): A cleaner solution is to change to use indexedSeq and just check the sizes and fail all nodes if the lengths don't match
      val iit = inputs.iterator
      val rit = results.iterator
      while (iit.hasNext && rit.hasNext) {
        val v = iit.next()
        v.setWaitingOn(null)
        v.combineInfo(this, eq)
        rit.next() match {
          case Success(rVal) => v.completeWithResult(rVal, eq)
          case Failure(rEx)  => v.completeWithException(rEx, eq)
        }
      }
      while (iit.hasNext) {
        val v = iit.next()
        v.setWaitingOn(null)
        v.combineInfo(this, eq)
        v.completeWithException(new GraphException("Batcher returned fewer results that number of batched nodes"), eq)
      }
    }

    override def completeWithException(ex: Throwable, ec: EvaluationQueue): Unit = {
      super.completeWithException(ex, ec)
      var cn: List[NodeT] = Nil
      if (inputs ne null) {
        cn = inputs
      } else {
        // it's possible that when BNode is completed with Exception, it has not been started at all
        // so only complete the nodes inside inputs is not good enough (e.g. OptimusCancellationException)
        val cstate = state.get()
        if ((cstate ne null) && cstate.count > 0) {
          cn = cstate.nodes
        }
      }
      while (cn.nonEmpty) {
        val v = cn.head
        v.setWaitingOn(null)
        v.combineInfo(this, ec)
        v.completeWithException(ex, ec)
        cn = cn.tail
      }
    }
  }

  protected def getBatchScopeKeys(v: NodeT): Set[BatchScopeKey] = NodeBatcherSchedulerPluginBase.defaultBatchScopeKeys

  @nodeSync
  def stableCalcTags(grpKey: GroupKeyT, inputs: Seq[NodeT]): StableCalcTags = null
  def stableCalcTags$newNode(grpKey: GroupKeyT, inputs: Seq[NodeT]): Node[StableCalcTags] = null
  def stableCalcTags$queued(grpKey: GroupKeyT, inputs: Seq[NodeT]): NodeFuture[StableCalcTags] = null
}

/**
 * To be used as in: object MyBatcherTag extends BatchLimitTagBase class SomeBatcher extends OnOfTheBases { override val
 * limitTag = MyBatcher }
 */
class BatchLimitTagBase extends NonForwardingPluginTagKey[AtomicInteger] {
  def key: BatchLimitTagBase = this

  @nodeSync
  @nodeSyncLift
  // noinspection ScalaUnusedSymbol
  final def expect[T](count: Int)(@nodeLift @nodeLiftByName f: => T): T =
    expect$withNode(count)(toNode(f _))
  // noinspection ScalaUnusedSymbol
  final def expect$queued[T](count: Int)(f: Node[T]): Node[T] = expect$newNode(count)(f).enqueueAttached
  // noinspection ScalaUnusedSymbol
  final def expect$withNode[T](count: Int)(f: Node[T]): T = expect$newNode(count)(f).get

  final private[this] def expect$newNode[T](count: Int)(f: Node[T]): Node[T] = {
    val ss = EvaluationContext.scenarioStack
    if (ss.findPluginTag(key).isDefined)
      throw new GraphException("Nested limit tags are currently not supported!")
    f.attach(ss.withPluginTag(key, new AtomicInteger(count)))
    f
  }

  def increment(): Unit = increment(1)
  def decrement(): Unit = increment(-1)
  def decrement(count: Int): Unit = increment(-count)
  def increment(count: Int): Unit = {
    val ec = OGSchedulerContext.current()
    val oaint = ec.scenarioStack.findPluginTag[AtomicInteger](key)
    if (oaint.isDefined) {
      if (oaint.get.addAndGet(count) == 0) {
        ec.scheduler.runOutOfWorkListeners((t: OutOfWorkListener) => t.limitTag() eq key)
      }
    }
  }
}

object NodeBatcherSchedulerPluginBase {
  private val log = getLogger(this)
  private final val executionInfoMap = new ConcurrentHashMap[String, NodeTaskInfo]()

  def getExecutionInfo(name: String, plugin: SchedulerPlugin): NodeTaskInfo = {
    val registeredNTI = executionInfoMap.computeIfAbsent(
      name,
      name => {
        new NodeTaskInfo(name, NodeTaskInfo.DONT_CACHE)
      })
    registeredNTI.copyWithPlugin(plugin)
  }

  /** don't register with NodeTrace */
  def getPropertyInfo(cls: Class[_]): NodeTaskInfo = {
    val name = cls.getName
    executionInfoMap.computeIfAbsent(
      name,
      name => {
        new NodeTaskInfo(name, NodeTaskInfo.SCENARIOINDEPENDENT | NodeTaskInfo.DONT_CACHE) {
          override def isAdapter = true
        }
      })
  }

  val defaultBatchScopeKeys: Set[BatchScopeKey] = Set(DMC, DSI)
}

abstract class NodeBatcherSchedulerPlugin[T, NodeT <: CompletableNode[T]](
    val priority: Int,
    val scopedBatching: Boolean)
    extends NodeBatcherSchedulerPluginBase[T, NodeT] {
  @volatile
  private var batch: BNode = _

  def this() = this(0, true)
  def this(priority: Int) = this(priority, false)

  def run(inputs: Seq[NodeT]): Seq[T]
  override protected def run(ec: OGSchedulerContext, bnode: BNode, grpKey: GroupKeyT, inputs: Seq[NodeT]): Unit =
    bnode.completeBatch(ec, run(inputs))

  override def adapt(v: NodeTask, ec: OGSchedulerContext): Boolean = {
    if (prioritiseConcurrency && breakDependenciesInAdapt) v.fixWaiterChainForXSFTFromBatcher(ec)
    val node = v.asInstanceOf[NodeT]
    if (getNodeWeight$queued(node) ne null)
      throw new GraphInInvalidState("NodeBatcherSchedulerPlugin doesn't support @node getNodeWeight")
    val weight = getNodeWeight(node)
    val batchScopeKeys = getBatchScopeKeys(node)

    var retry = true
    while (retry) {
      retry = false
      var registerCallback = false
      var curBatch = batch
      if ((curBatch eq null) || curBatch.notAcceptingNewNodes) this.synchronized {
        curBatch = batch
        if ((curBatch eq null) || curBatch.notAcceptingNewNodes) {
          batch = new BNode(priority, scopedBatching, batchScopeKeys, node, null)
          curBatch = batch
          registerCallback = true
        }
      }
      if (registerCallback)
        ec.scheduler.addOutOfWorkListener(curBatch)
      else {
        val state = curBatch.addNode(node, weight)
        if (state ne null) {
          // addNode was not able to add `v` to the existing batch...
          if (state.count != 1) {
            val nbatch = new BNode(0, false, batchScopeKeys, state, null)
            nbatch.attach(ec.scenarioStack.asSiStack)
            nbatch.tryToSchedule(ec)
          } else {
            retry = true
          }
        }
      }
    }
    true
  }
}

/**
 * Base class for implementing a grouping batcher Usually one would override run with either @node or a regular def
 */
abstract class GroupingNodeBatcherSchedulerPluginBase[T, NodeT <: CompletableNode[T]](
    val priority: Int,
    val scopedBatching: Boolean)
    extends NodeBatcherSchedulerPluginBase[T, NodeT] {
  type GroupKeyT >: Null <: AnyRef
  type PerScenarioInfoT >: Null <: AnyRef

  private val batches = new JHashMap[GroupKeyT, BNode]()

  def this() = this(13, true)
  def this(priority: Int) = this(priority, false)

  /**
   * This function needs to be very very quick and non-blocking or be @node ... Returns Weight for a given node, when
   * the sum of weights reaches maxBatchSize the batch is scheduled for execution
   */
  protected def getGroupBy(psinfo: PerScenarioInfoT, inputNode: NodeT): GroupKeyT
  protected def getGroupBy$queued(psinfo: PerScenarioInfoT, inputNode: NodeT): NodeFuture[GroupKeyT] = null

  /**
   * When BNode starts to run we need to clean up grp key and node reference But we have to verify that some other batch
   * didn't get itself registered under the same grpKey
   */
  final protected override def onFirstRun(bnode: BNode, grpKey: GroupKeyT): Unit = {
    batches.synchronized {
      val snode = batches.get(grpKey)
      // Note the use of 'eq' because another node with the same group key could have been already building up a batch
      if (snode eq bnode) batches.remove(grpKey)
    }
  }

  /**
   * Get the ScenarioStack under which run method should be invoked, given the ScenarioStack under which the adapted
   * node was invoked. The default behavior is to use the adapted node's SI Root (so that the RuntimeEnvironment is
   * accessible but no tweaks are accessible - important since they may be different on different adapted nodes)
   */
  protected def scenarioStackForRun(inputScenarioStack: ScenarioStack): ScenarioStack = inputScenarioStack.asSiStack

  override def adapt(v: NodeTask, ec: OGSchedulerContext): Boolean = {
    if (prioritiseConcurrency && breakDependenciesInAdapt) v.fixWaiterChainForXSFTFromBatcher(ec)

    val node = v.asInstanceOf[NodeT]
    val ss = scenarioStackForRun(node.scenarioStack())

    val gnwNode = getNodeWeight$queued(node)
    val weight = if (gnwNode eq null) {
      try { getNodeWeight(node) }
      catch {
        case e: Throwable =>
          v.completeWithException(e, ec)
          return true
      }
    } else -1

    if (node.scenarioStack().isRecordingTweakUsage) MonitoringBreadcrumbs.sendBatchingInsideXSCallStackCrumb(node)

    val ggbNode = getGroupBy$queued(null, node)
    val grpKey = if (ggbNode eq null) {
      try { getGroupBy(null, node) }
      catch {
        case e: Throwable =>
          v.completeWithException(e, ec)
          return true
      }
    } else null

    // At least on of the functions are async and we need to wait for their completion
    if ((gnwNode ne null) || (ggbNode ne null)) {
      // we impose an ordering on the processing of the results of gnwNode before the results of ggbNode
      // if only one is a node, we can jump straight to completing our dependent node
      // if both are nodes, we first wait for gnwNode,then we wait for ggbNode and finally complete our dependent node when both are there
      val awaiter: NodeAwaiter = new NodeAwaiter {
        private[this] var w = weight
        private[this] var k = grpKey
        private[this] var alreadyCompletedFirstWait = false
        override def onChildCompleted(eq: EvaluationQueue, cbNode: NodeTask): Unit = {
          if (!alreadyCompletedFirstWait && (gnwNode ne null) && (ggbNode ne null)) {
            // both gnwNode and ggbNode are nodes, and we haven't been here before, so this must be an update for gnwNode
            alreadyCompletedFirstWait = true
            ggbNode.continueWith(this, eq)
          } else {
            // At this point we should have results from both nodes and can make a call to add2BatchNode
            if (gnwNode ne null) {
              if (gnwNode.isDoneWithException) {
                node.completeWithException(gnwNode.exception$, eq)
              } else {
                w = gnwNode.result$ // WARNING: We are no picking up dependencies in extra info... Should we?
              }
            }

            if (ggbNode ne null) {
              if (ggbNode.isDoneWithException) {
                node.completeWithException(ggbNode.exception$, eq)
              } else {
                k = ggbNode.result$ // WARNING: We are no picking up dependencies in extra info... Should we?
              }
            }
            // If gnwNode OR ggbNode threw an exception, we just complete the original node with exception
            // and don't even attempt at batching it
            if (!node.isDoneWithException) {
              add2BatchNode(node, k, w, eq, ss)
            }
          }
        }
      }

      // We subscribe to completion one by one to play nicely with WaitingOn
      if (gnwNode ne null) gnwNode.continueWith(awaiter, ec)
      else ggbNode.continueWith(awaiter, ec)
    } else
      add2BatchNode(node, grpKey, weight, ec, ss)

    true
  }

  /** After GroupBy and NodeWeigh this call would register the node with a batch */
  private def add2BatchNode(
      node: NodeT,
      grpKey: GroupKeyT,
      weight: Int,
      eq: EvaluationQueue,
      ss: ScenarioStack): Unit = {
    val newBatch = batches.synchronized {
      var batch = batches.get(grpKey)
      val batchScopeKeys = getBatchScopeKeys(node)
      val state = if (batch eq null) State(count = 1, weight = weight, List(node)) else batch.addNode(node, weight)
      if (state eq null) {
        // Added a new node to already registered and previously scheduled batch node
        // Effectively we are done
        null
      } else {
        if (state.count == 1 && (weight < getGroupMaxBatchSize(grpKey, node))) {
          // First time with this grpKey OR failed to add to previously scheduled batch node
          batch = new BNode(priority, scopedBatching, batchScopeKeys, state, grpKey)
          batches.put(grpKey, batch)
          batch
        } else {
          if (batch ne null) batches.remove(grpKey)
          new BNode(-1, false, batchScopeKeys, state, grpKey)
        }
      }
    }

    // Enqueue outside the lock
    if (newBatch ne null) {
      newBatch.attach(ss)
      if (newBatch.priority == -1) newBatch.tryToSchedule(eq)
      else eq.scheduler.addOutOfWorkListener(newBatch)
    }

    // Allow for semi-manual control of batcher nodes
    val limitType: PluginTagKey[AtomicInteger] = getLimitTag
    if (limitType ne null) {
      val limit = node.scenarioStack().findPluginTag[AtomicInteger](limitType)
      if (limit.isDefined) {
        val remainingItems = limit.get.decrementAndGet()
        if (remainingItems == 0) {
          eq.scheduler.runOutOfWorkListeners((t: OutOfWorkListener) => t.limitTag() eq limitType)
        }
      }
    }
  }
}

abstract class GroupingNodeBatcherSchedulerPlugin[T, NodeT <: CompletableNode[T]](
    priority: Int,
    scopedBatching: Boolean)
    extends GroupingNodeBatcherSchedulerPluginBase[T, NodeT](priority, scopedBatching)
    with JobFriendlySchedulerPlugin {

  def this() = this(0, true)
  def this(priority: Int) = this(priority, false)

  @nodeSync
  def run(grpKey: GroupKeyT, inputs: Seq[NodeT]): Seq[T]
  def run$newNode(grpKey: GroupKeyT, inputs: Seq[NodeT]): Node[Seq[T]]
  def run$queued(grpKey: GroupKeyT, inputs: Seq[NodeT]): NodeFuture[Seq[T]]

  override final protected def run(
      ec: OGSchedulerContext,
      bnode: BNode,
      grpKey: GroupKeyT,
      inputs: Seq[NodeT]): Unit = {

    // report back to the JobDecorator if the batched nodes are @job
    val rn =
      JobDecorator
        .ifPresent(bnode.scenarioStack, inputs) { jobDecorator =>
          jobDecorator.decorateBatch$queued[T, NodeT](inputs) { subInputs =>
            run$newNode(grpKey, subInputs).enqueue
          }
        }
        .getOrElse(run$queued(grpKey, inputs))
        .asNode$

    if (rn.isFSM) {
      bnode.setWaitingOn(rn)
      rn.continueWith(
        new NodeAwaiter {
          override def awaiter(): NodeCause = bnode
          override protected def onChildCompleted(eq: EvaluationQueue, node: NodeTask): Unit = {
            bnode.setWaitingOn(null)
            bnode.combineInfo(rn, eq)
            if (rn.isDoneWithResult) {
              bnode.completeBatch(eq, rn.result)
            } else {
              bnode.completeWithException(rn.exception, eq)
            }
          }
        },
        ec
      )
    } else bnode.completeBatch(ec, rn.get)
  }

}

abstract class GroupingNodeBatcherWithTrySchedulerPlugin[T, NodeT <: CompletableNode[T]](
    priority: Int,
    scopedBatching: Boolean)
    extends GroupingNodeBatcherSchedulerPluginBase[T, NodeT](priority, scopedBatching)
    with JobFriendlySchedulerPlugin {

  def this() = this(13, true)
  def this(priority: Int) = this(priority, false)

  @nodeSync
  def tryRun(grpKey: GroupKeyT, inputs: Seq[NodeT]): Seq[Try[T]]
  def tryRun$newNode(grpKey: GroupKeyT, inputs: Seq[NodeT]): Node[Seq[Try[T]]]
  def tryRun$queued(grpKey: GroupKeyT, inputs: Seq[NodeT]): NodeFuture[Seq[Try[T]]]

  override final protected def run(
      ec: OGSchedulerContext,
      bnode: BNode,
      grpKey: GroupKeyT,
      inputs: Seq[NodeT]): Unit = {

    // report back to the JobDecorator if the batched nodes are @job
    val rn =
      JobDecorator
        .ifPresent(bnode.scenarioStack, inputs) { jobDecorator =>
          jobDecorator
            .decorateTryBatch$queued[T, NodeT](inputs) { subInputs =>
              tryRun$newNode(grpKey, subInputs).enqueue
            }
            .asNode$
        }
        .getOrElse(tryRun$newNode(grpKey, inputs).enqueue)

    if (rn.isFSM) {
      bnode.setWaitingOn(rn)
      rn.continueWith(
        new NodeAwaiter {
          override def awaiter(): NodeCause = bnode

          override protected def onChildCompleted(eq: EvaluationQueue, node: NodeTask): Unit = {
            bnode.setWaitingOn(null)
            bnode.combineInfo(rn, eq)
            if (rn.isDoneWithResult) {
              bnode.completeTryBatch(eq, rn.result)
            } else {
              bnode.completeWithException(rn.exception, eq)
            }
          }
        },
        ec
      )
    } else bnode.completeTryBatch(ec, rn.get)
  }
}
