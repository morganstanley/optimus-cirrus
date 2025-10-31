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
package optimus.platform

import java.lang.ref.WeakReference
import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.ChainedID
import optimus.core.TPDMask
import optimus.graph._
import optimus.platform.PluginHelpers.toNode
import optimus.platform.dal.EntityResolver
import optimus.platform.inputs.loaders.FrozenNodeInputMap
import optimus.graph.ConvertByNameToByValueNode.removeRedundantTweaks

import scala.util.control.NonFatal

/**
 * Full thread local store This allows to avoid constant lookup for multiple thread-locals
 */
object EvaluationContext {

  private val log = getLogger(this)

  private val verifyOffGraphNonFatal = System.getProperty("optimus.verifyOffGraph.nonFatal", "false").toBoolean

  /** This is for experimental debugging UI support only. Trying to provide a reasonable default. DON'T use it! */
  private[optimus] var lastDefaultScenarioStack: WeakReference[ScenarioStack] = _

  /** User should have no reason to call this method */
  private final def set(ec: OGSchedulerContext): Unit = {
    if (ec != null) ec.assertCorrectThread()
    OGSchedulerContext.setCurrent(ec)
  }

  private def setupClassLoader(runtimeEnvironment: RuntimeEnvironment): Unit = {
    val currentThread = Thread.currentThread()
    if (currentThread.getContextClassLoader ne runtimeEnvironment.classLoader) {
      log.warn(s"Binding context classloader of $currentThread to loader for ${runtimeEnvironment.id}")
      currentThread.setContextClassLoader(runtimeEnvironment.classLoader)
    }
  }

  /**
   * This is a dangerous API that we are struggling to remove. It avoids calling verifyOffGraph, which means it will
   * modify env on a completely random node
   *
   * In fact Sequencer uses and manages to rely on this!!!!!!!!!!!!!!
   */
  private[optimus] final def resetUntweakedScenarioState(uss: UntweakedScenarioState): Unit = {
    initCoreUnsafe(Scheduler.currentOrDefault, uss.scenarioStack, requeryTime = true)
  }

  private def initCore(scheduler: Scheduler, scenarioStack: ScenarioStack, requeryTime: Boolean): Unit = {
    verifyOffGraph()
    initCoreUnsafe(scheduler, scenarioStack, requeryTime)
  }

  /**
   * Setup the basic variables for an optimus thread: scheduler and scenario stack
   *
   * Note: Attempt is made in the case of an exception to recover to the previous state.
   *
   * Not sure it actually makes sense!
   */
  private def initCoreUnsafe(scheduler: Scheduler, scenarioStack: ScenarioStack, requeryTime: Boolean): Unit = {
    val originalEC = OGSchedulerContext.current()
    var ec = originalEC
    val prevNode = if (originalEC != null) originalEC.getCurrentNodeTask else null
    val newSS = scenarioStack.withCancellationScope(CancellationScope.newScope()) // Cleanup CS
    try {
      if ((ec eq null) || (ec.scheduler ne scheduler)) {
        ec = scheduler.newEvaluationContext(newSS)
        set(ec)
      } else
        ec.setCurrentNode(new StartNode(newSS))

      if (requeryTime) {
        if (scenarioStack.env.entityResolver ne null) {
          ec.getCurrentNodeTask.replace(scenarioStack.initialRuntimeScenarioStack(requeryTime = true))
        }
        if (scenarioStack.env ne RuntimeEnvironment.minimal)
          lastDefaultScenarioStack = new WeakReference(scenarioStack)
      }

      setupClassLoader(scenarioStack.env)
    } catch {
      case NonFatal(t) =>
        try log.error("Failed to reset runtime", t)
        finally {
          set(originalEC)
          if (prevNode != null) originalEC.setCurrentNode(prevNode)
          throw t
        }
    }
  }

  /** Returns true if the optimus context was ever initialized on this thread */
  final def isInitialised: Boolean = null != OGSchedulerContext._TRACESUPPORT_unsafe_current()

  /** Initialize EC with minimal runtime (aka no dal!) */
  final def initializeWithoutRuntime(): Unit =
    initCore(Scheduler.currentOrDefault, ScenarioState.minimal.scenarioStack, requeryTime = false)

  final def initializeSingleThreadedWithoutRuntime(): Unit = {
    val scheduler = new OGScheduler(true)
    initCore(scheduler, ScenarioState.minimal.scenarioStack, requeryTime = false)
  }

  /** API for re-initializing a user thread with a state a captured already on the graph thread */
  final def initialize(scenarioState: ScenarioState): Unit = {
    initCore(Scheduler.currentOrDefault, scenarioState.scenarioStack, requeryTime = false)
  }

  final def initializeSingleThreaded(scenarioState: ScenarioState): Unit = {
    val scheduler = new OGScheduler(true)
    initCore(scheduler, scenarioState.scenarioStack, requeryTime = false)
  }

  /** API for re-initializing a user thread with a state a captured already on the graph thread */
  final def initialize(scheduler: Scheduler, scenarioState: ScenarioState): Unit =
    initCore(scheduler, scenarioState.scenarioStack, requeryTime = false)

  /**
   * Ensures that EvaluationContext is initialized from a given scheduler. RuntimeEnvironment is reset to env. If env is
   * not minimal (i.e. has a proper config and entity resolver), then a new scenario stack is created with
   * TemporalSource.initialTime set to the current At.now time (i.e. read broker last stable read time)
   */
  final def initializeWithNewInitialTime(scheduler: Scheduler, scenarioState: UntweakedScenarioState): Unit =
    initCore(scheduler, scenarioState.scenarioStack, requeryTime = true)
  final def initializeWithNewInitialTime(uss: UntweakedScenarioState): Unit =
    initCore(Scheduler.currentOrDefault, uss.scenarioStack, requeryTime = true)
  final def initializeWithNewInitialTimeSingleThreaded(uss: UntweakedScenarioState): Unit =
    initCore(new OGScheduler(true), uss.scenarioStack, requeryTime = true)
  final def initializeWithNewInitialTime(env: RuntimeEnvironment): Unit =
    initCore(Scheduler.currentOrDefault, UntweakedScenarioState(env).scenarioStack, requeryTime = true)
  final def initializeWithNewInitialTimeSingleThreaded(env: RuntimeEnvironment): Unit =
    initCore(new OGScheduler(true), UntweakedScenarioState(env).scenarioStack, requeryTime = true)

  /** Check if current thread is currently executing the graph */
  final def isThreadRunningGraph: Boolean = {
    val ec = OGSchedulerContext.current()
    if (ec eq null) false // We are not even initialized
    else isRunningGraph(false)
  }

  /**
   * Throw if current thread is executing graph beNice = true Don't throw, just log! Most often would mean no-caching
   * anywhere in the chain
   *
   * NOTES: On ignoreNonCaching... We don't currently (but will) differentiate between Node does not support caching vs.
   * don't cache. It's possible that something non-cacheable and works today but in the future user flips the flag and
   * the call would throw Currently it also doesn't work across the wire the parent chain is not visible on the server
   * side past the node that was shipped over it's not an intrinsic issue, It can be solved by shipping the info that
   * such verify request was made back to client verify it there. This way everything would be totally consistent
   */
  final def verifyOffGraph(ignoreNonCaching: Boolean = false, beNice: Boolean = false): Unit = {
    val ec = OGSchedulerContext.current()
    if (ec ne null) {
      if (isRunningGraph(ignoreNonCaching)) {
        val cn = currentNode
        val path = cn.nodeStackToCacheable()
        if (path.isEmpty) path.add(cn)
        val exception = GraphException.verifyOffGraphException(path)
        if (verifyOffGraphNonFatal || beNice)
          optimus.core.log.error("**********************************************************************", exception)
        else
          throw exception
      }
    }
  }

  /**
   * Throw if current method is invoked in a @node, even this method is not directly called by the @node
   *
   * the @node can be defined in @entity, or normal types, but it's fine for an @async method
   */
  final def verifyImpure(): Unit = verifyOffGraph(ignoreNonCaching = true)

  /**
   * Returns currently installed EvaluationContext DO NOT cache or capture this value. It will produce a very hard to
   * debug bugs
   */
  private[optimus] final def current = {
    val t = OGSchedulerContext.current()
    if (t == null)
      throw new NullPointerException(
        s"The optimus evaluation context has not been initialised correctly" +
          s" (null thread local in ${Thread.currentThread().getName}).")
    t
  }

  /** Returns current scheduler. same as Scheduler.current */
  final def scheduler: Scheduler = current.scheduler()

  /** Returns currently executing node */
  final def currentNode: NodeTask = current.getCurrentNodeTask

  final def simpleChain: String = current.getCurrentNodeTask.simpleChain()

  /**
   * Update the current node with dependencies of all the tweaks in the current scenarioStack. This WILL cause XSFT
   * cache misses! It should only be used in cases where dependencies are not reliable (e.g. currentScenarioState) or
   * when XSFT can cause cycles e.g. Auditor callbacks, see suspendAuditorCallbacks
   * @return
   *   Current OGSchedulerContext
   */
  private[platform] final def poisonTweakDependencyMask(): OGSchedulerContext = {
    val ec = current
    ec.getCurrentNodeTask.setTweakPropertyDependency(TPDMask.poison)
    ec
  }

  /** Returns currently active scenario stack (regardless if we are currently executing a node) */
  final def scenarioStack: ScenarioStack = currentNode.scenarioStack

  /**
   * Returns currently active node or null if none
   */
  private[optimus] final def currentNodeOrNull: NodeTask = {
    if (!isInitialised) null
    else currentNode
  }

  /**
   * Returns currently active scenario stack or null if none
   * @note
   *   consider adding code that always returns null if in the middle of some module constructor
   *   [[optimus.debug.InstrumentedModuleCtor]]
   */
  final def scenarioStackOrNull: ScenarioStack = {
    val current = currentNodeOrNull
    if (current eq null) null else current.scenarioStack()
  }

  final def findPluginTag[T](pKey: PluginTagKey[T]): Option[T] = {
    val ss = scenarioStackOrNull
    if (ss eq null) None
    else ss.findPluginTag(pKey)
  }

  /** Return currently active entityResolver (regardless if we are currently executing a node) */
  final def entityResolver: EntityResolver = env.entityResolver

  /** Return currently active env (regardless if we are currently executing a node) */
  final def env: RuntimeEnvironment = scenarioStack.env

  /** Return currently active scenario stack root (regardless if we are currently executing a node) */
  final def untweakedScenarioState: UntweakedScenarioState = scenarioStack.asUntweakedScenarioState

  /** Return currently active cancellation scope */
  final def cancelScope: CancellationScope = scenarioStack.cancelScope

  /** Clear initialized context */
  final def clear(): Unit = {
    verifyOffGraph()
    set(null)
  }

  // A given function that is specific for Loom
  final def givenL[T](scenario: Scenario, f: => T): T = {
    val node = toNode(f _)
    given(scenario, node).get
  }

  final def given[T](scenario: Scenario, node: Node[T]): Node[T] = {
    // Quick test against wrong API usage
    // It is scoped with DiagnosticSettings.debugging currently because test coverage is incomplete (as of 6/2/2015)
    // Can we can remove this scoping?
    // TODO (OPTIMUS-10707): commenting this out until we get a fix
    // if (Settings.schedulerAsserts && !scenario.isTrivial)
    //  throw new GraphException("Wrong API usage! Did you mean to use TrackingScenario.given()?")

    if (Settings.convertByNameToByValue && scenario.existsWithNested(_.hasReducibleToByValueTweaks))
      new ConvertByNameToByValueNode(scenarioStack, scenario, node)
    else if (Settings.removeRedundantTweaks) {
      // if we do not have nested we can just remove the tweaks directly and not make a whole new node, if there are nested ConvertByNameToByValueNode takes care of it for us
      if (scenario.nestedScenarios.isEmpty)
        given(scenarioStack.createChild(removeRedundantTweaks(scenario, scenarioStack), node), node)
      else new ConvertByNameToByValueNode(scenarioStack, scenario, node)
    } else
      given(scenarioStack.createChild(scenario, node), node)
  }

  final def given[T](scenario: Scenario, kvs: Seq[PluginTagKeyValue[_]], node: Node[T]): Node[T] =
    given(scenarioStack.createChild(scenario, FrozenNodeInputMap.empty.withExtraInputs(kvs), 0, node), node)

  final def given[T](scenarios: Seq[Scenario], node: Node[T]): Node[T] =
    given(Scenario.toNestedScenarios(scenarios), node)

  /** This is called from Debugger and other places too */
  private[optimus] final def given[T](ss: ScenarioStack)(f: => T) = {
    val node = new CompletableNode[T] {
      override def run(ec: OGSchedulerContext): Unit = completeWithResult(f, ec)
      override def executionInfo(): NodeTaskInfo = NodeTaskInfo.Given
    }
    node.attach(ss)
    node.get
  }

  final def given[T](scenarioStack: ScenarioStack, node: Node[T]): Node[T] = {
    node.attach(scenarioStack)
    node
  }

  private def trackNodeTask(node: NodeTask): NodeTask = {
    val ss = if (node.scenarioStack ne null) node.scenarioStack else EvaluationContext.scenarioStack
    val ssChild = ss.withTrackingNode // Child scenario stack
    node.replace(ssChild)
    node
  }

  final def track[T](node: Node[T]): Node[T] = {
    trackNodeTask(node)
    node
  }

  final def trackChainedID(node: NodeTask): ChainedID = trackNodeTask(node).ID()

  /**
   * Used to update current 'default' scenario stack The next call into graph will use this Scenario[Stack]
   *
   * @param scenario
   *   The new top scenario
   */
  final def push(scenario: Scenario): Unit = {
    verifyOffGraph(ignoreNonCaching = true)

    val cn = currentNode
    val nss = cn.scenarioStack.createChild(scenario, EvaluationContext.this)
    cn.replace(nss)
  }

  /**
   * Used to update current 'default' scenario stack The next call into graph will use parent of current Scenario[Stack]
   */
  final def pop(): Scenario = {
    verifyOffGraph(ignoreNonCaching = true)

    val cn = currentNode
    if (cn.scenarioStack.isRoot)
      throw new UnsupportedOperationException("Cannot pop the root scenario stack")
    val topScenario = currentNode.scenarioStack.topScenario
    cn.replace(currentNode.scenarioStack.parent)
    topScenario
  }

  /** From non-graph context it's safe to set 'default' context (aka ScenarioStack) for the entry into graph */
  private[optimus] final def setScenarioStack(ss: ScenarioStack) = {
    verifyOffGraph()
    val prev = currentNode.scenarioStack
    currentNode.replace(ss)
    prev
  }

  /**
   * True is at least one node is currently executing
   */
  private[optimus] def isRunningGraph(ignoreNonCaching: Boolean = true) = {
    val cnode = currentNode
    if (cnode.isInstanceOf[StartNode]) false
    else if (ignoreNonCaching && !cnode.scenarioStack().cachedTransitively) false
    else true // There is currently executing node on this thread
  }

  /** Should be called on a new node as a continuation of execution of existing (current) node */
  final def enqueueAndWait(v: NodeTask): Unit = {
    v.attach(scenarioStack)
    current.runAndWait(v)
  }

  /** Explicit scheduling of a node with a given scenario stack */
  final def enqueueAndWait(v: NodeTask, ss: ScenarioStack): Unit = {
    v.attach(ss)
    current.runAndWait(v)
  }

  /**
   * This API is trying to solve the following issue: <ul> <li>In the callback onChildCompleted() the 'currentNode' is
   * unpredictable. If the code tries to execute anything that queries the 'currentNode' it can result in the bugs: <ol>
   * <li>currentNode.scenarioStack is used and therefore wrong value can be produced. <li>dependency edge in the case of
   * as sync call -> runAndWait -> is not drawn correctly and can result in deadlock <li>profiler will the wrong
   * dependency edge </ol> </li> <li>What make this arguably worse is that often it is the 'right node' and that makes
   * spotting the issue hard. </ul>
   */
  private[optimus] final def asIfCalledFrom[R](v: NodeTask, eq: EvaluationQueue)(enqueueCall: => R): R = {
    if (isInitialised) {
      val sc = current
      val prevTask = sc.setCurrentNode(v)
      try enqueueCall
      finally sc.setCurrentNode(prevTask)
    } else
      eq.scheduler.asInstanceOf[OGScheduler].asIfRunning(v, () => enqueueCall)
  }

  /**
   * Plugin generates calls to this method for rawNode This is logically equivalent to getNode for regular Nodes and
   * should return 'scheduled' node Attaches scenario stack and expects 'fresh' nodes
   */
  final def ensureStarted[T](v: Node[T]): Node[T] = {
    if (v.scenarioStack ne null) throw new GraphInInvalidState()
    v.attach(scenarioStack)
    current.enqueue(v)
    v
  }

  /**
   * Looks up tweaks, cached nodes
   */
  final def lookupNode[A](nkey: NodeKey[A]): Node[A] =
    scenarioStack.getNode(nkey.asInstanceOf[PropertyNode[A]], OGSchedulerContext.current())

  /**
   * Execute all nodes in the list Faster than and logically equivalent to nodes.foreach(enqueueAndWait(_))
   */
  final def enqueueAndWaitForAll(nodes: Seq[NodeTask]): Unit = if (nodes.nonEmpty) {
    val ec = current
    val it1 = nodes.iterator
    while (it1.hasNext) {
      ec.enqueue(it1.next())
    }

    val it2 = nodes.iterator
    while (it2.hasNext) {
      try {
        ec.runAndWait(it2.next())
      } catch {
        case _: Exception => // We are swallowing exception here! It's kind of already reported per node though
      }
    }
  }

  /**
   * Executes all of the nodes in the list and waits for their continuations to complete
   */
  private[optimus] final def evaluateSync(nodes: Seq[NodeTask], waitForContinuations: Boolean = true): Unit = {
    val ec = current
    var awaiter: NodeAwaiterWithLatch = null

    // n.b. to ensure that we really run last, we need to add the awaiter *before* enqueuing. Otherwise the node
    // could already be completing (on another thread), so the waiters list is empty and we would get continued
    // concurrently
    if (waitForContinuations) {
      awaiter = new NodeAwaiterWithLatch(nodes.size)
      nodes.foreach { _.continueWithLast(awaiter, ec) }
    }

    if (nodes.size == 1) ec.runAndWait(nodes.head)
    else enqueueAndWaitForAll(nodes)

    if (waitForContinuations) awaiter.latch.await()
  }

}

/** Consider moving this class into graph */
abstract class EvaluationQueue {
  def enqueue(task: NodeTask): Unit

  /** Custom dependency propagation, fromTask can be null in the cases like re-enqueue */
  def enqueue(fromTask: NodeTask, task: NodeTask): Unit = enqueue(task)
  def scheduler: Scheduler

  /** Try to remove from the queue (used by nodes that enqueue themselves multiple times (e.g. SequenceNode) */
  def tryToRemove(task: NodeTask): Unit = {}

  /**
   * Returns true if the onChildCompleted was scheduled to be delayed Default implementation provides no stack overflow
   * protection. Schedulers currently inherit this behaviour and will need to override these 2 methods if it becomes an
   * issue.
   */
  def delayOnChildCompleted(parent: NodeTask, child: NodeTask): Boolean = false

  /** Must be paired with delayOnChildCompleted that returned true */
  def afterOnChildCompleted(): Unit = {}

  final protected def delayOnChildCompleted(parent: NodeTask, child: NodeTask, depth: Int): Boolean = {
    if (depth > DiagnosticSettings.proxyChainStackLimit) {
      val delay = new DelayOnChildCompletedNode(parent, child)
      // need the waitingOn edge here to avoid the case of a stall where [delayOnChildCompleted] ends up on the wait
      // queue of a sync-stacked thread, and that thread is waiting on a long proxy chain - if we did pick up this
      // node and run it, we'd be able to complete all proxies, but without the link between them we deadlock
      parent.setWaitingOn(delay)
      enqueue(null, delay)
      true
    } else
      false
  }

  def maybeSaveAwaitStack(ntsk: NodeTask): Unit = {}

}

/**
 * A very special EvaluationQueue that can't be actually used, and will throw on any attempt.
 * Only to be used in places where EvaluationQueue argument is required, but is effectively unused.
 */
object NoEvaluationQueue extends EvaluationQueue {

  override def enqueue(task: NodeTask): Unit = throw new IllegalStateException(
    "NoEvaluationQueue cannot be used to enqueue any nodes")

  override def scheduler: Scheduler = throw new IllegalStateException("NoEvaluationQueue does not know any scheduler")
}
