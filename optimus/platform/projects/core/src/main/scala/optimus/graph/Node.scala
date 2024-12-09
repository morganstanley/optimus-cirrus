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

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import optimus.core.CoreAPI
import optimus.graph.tracking.ttracks.TTrack
import optimus.platform.EvaluationContext
import optimus.platform.EvaluationQueue
import optimus.platform.PluginHelpers.wrapped
import optimus.platform.Scenario
import optimus.platform.ScenarioStack
import optimus.platform.TweakableListener
import optimus.platform.annotations.miscFlags
import optimus.platform.annotations.nodeSync
import optimus.platform.annotations.nodeSyncLift
import optimus.platform.util.PrettyStringBuilder
import optimus.tools.scalacplugins.entity.MiscFlags
import optimus.scalacompat.collection._
import optimus.scalacompat.collection.BuildFrom

import java.util.Objects
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.TimeoutException
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

abstract class NodeStateMachine(final val completableNode: CompletableNode[AnyRef], ec: OGSchedulerContext)
    extends NodeStateMachineBase
    with Function1[NodeFuture[Any], Unit]
    with AsyncStateMachine[NodeFuture[Any], NodeFuture[Any]] {
  final def _result: NodeTask = completableNode

  private[this] var _childNode: NodeTask = _
  final def setChildNode(nt: NodeTask): Unit = { _childNode = nt }
  final def childNode: NodeTask = _childNode

  private[this] var _state: Int = 0
  protected final def state: Int = _state
  protected final def state_=(i: Int): Unit = _state = i

  /** Scala async implements this method */
  def apply(node: NodeFuture[Any]): Unit

  /**
   * Given at_node def a = { some code ... val x = b // where b is also @node def b = .... } b is childNode a is node
   *
   * NOTE: The tryRun implementation relies on the fact that nodes are awaiting one task at a time. As the result only
   * one task can signal another task to continue
   */
  final def run(node: NodeTask, ec: OGSchedulerContext): Unit = {
    if (Settings.schedulerAsserts) {
      val curNode = _result
      if (curNode ne node) throw new GraphInInvalidState("Continuation was created for a wrong node!")
    }
    val cn = _childNode
    _childNode = null
    node.combineInfo(cn, ec)
    setEC(ec)
    apply(cn.asInstanceOf[Node[Any]])
  }

  setEC(ec)
  // Start on this thread.
  apply(null)

  /** Rethrow the exception, the scheduler deals with it, not the FSM. */
  override protected final def completeFailure(t: Throwable): Unit = {
    throw wrapped(t)
  }

  /** Complete the state machine with the given value. */
  override protected final def completeSuccess(value: AnyRef): Unit =
    completableNode.completeWithResultFSM(value, this)

  /** Register the state machine as a completion callback of the given future. */
  override protected final def onComplete(f: NodeFuture[Any]): Unit = f.continueWithFSM(this)

  /** Extract the result of the given future if it is complete, or `null` if it is incomplete. */
  override protected final def getCompleted(f: NodeFuture[Any]): NodeFuture[Any] = {
    f.completeOrElseNullWDP(this)
  }

  /**
   * Extract the success value of the given future. If the state machine detects a failure it may complete the async
   * block and return `this` as a sentinel value to indicate that the caller (the state machine dispatch loop) should
   * immediately exit.
   */
  override protected final def tryGet(tr: NodeFuture[Any]): AnyRef = {
    if (tr.isDoneWithException) {
      _result.completeWithException(tr.exception$, OGSchedulerContext.current())
      this
    } else {
      tr.result$.asInstanceOf[AnyRef]
    }
  }
}

// The new async phase expects the state machine class to structurally conform to this interface.
trait AsyncStateMachine[F, R] {

  /** Assign `i` to the state variable */
  protected def state_=(i: Int): Unit

  /** Retrieve the current value of the state variable */
  protected def state: Int

  /** Complete the state machine with the given failure. */
  protected def completeFailure(t: Throwable): Unit

  /** Complete the state machine with the given value. */
  protected def completeSuccess(value: AnyRef): Unit

  /** Register the state machine as a completion callback of the given future. */
  protected def onComplete(f: F): Unit

  /** Extract the result of the given future if it is complete, or `null` if it is incomplete. */
  protected def getCompleted(f: F): R

  /**
   * Extract the success value of the given future. If the state machine detects a failure it may complete the async
   * block and return `this` as a sentinel value to indicate that the caller (the state machine dispatch loop) should
   * immediately exit.
   */
  protected def tryGet(tr: R): AnyRef
}

//
// Trait that represents the Future-like facade of a node in the graph.
//
trait NodeFuture[+T] {

  def continueWith(awaiter: NodeAwaiter, eq: EvaluationQueue): Unit
  @miscFlags(MiscFlags.NODESYNC_ONLY)
  def get$ : T
  // Some usages require a full Node[T] beyond just the Future-like methods.
  // so a NodeFuture[T] has to be convertable to a Node[T]
  def asNode$ : Node[T]

  // Following should go away when we switch to Loom
  def completeOrElseNullWDP(currentStateMachine: NodeStateMachine): NodeFuture[T]
  def continueWithFSM(currentStateMachine: NodeStateMachine): Unit

  // Following should not be required. Usage in Node and NodeBatcherSchedulerPlugin needs to change
  // so we can get rid of them -- follow up PR will address.
  def isDoneWithException: Boolean
  def exception$ : Throwable
  def result$ : T

  /**
   * toValue$ functions for each primitive type descriptor.
   * @see jdk.internal.org.objectweb.asm.Type.PRIMITIVE_DESCRIPTORS
   *
   * Loom code gen is using this function to retrieve the value of the delayed call.
   * Note: it looks like get... but this will start changing
   * Why a new function? Because we will modify this without breaking the existing code!
   */
  final def toValue$V: Unit = get$ // void
  final def toValue$Z: Boolean = get$.asInstanceOf[Boolean]
  final def toValue$C: Char = get$.asInstanceOf[Char]
  final def toValue$B: Byte = get$.asInstanceOf[Byte]
  final def toValue$S: Short = get$.asInstanceOf[Short]
  final def toValue$I: Int = get$.asInstanceOf[Int]
  final def toValue$F: Float = get$.asInstanceOf[Float]
  final def toValue$J: Long = get$.asInstanceOf[Long]
  final def toValue$D: Double = get$.asInstanceOf[Double]
  final def toValue$ : T = get$ // object
}

/*
 * Node is:
 *   1. A node on the graph.
 *   2. Basic executable unit on graph
 *   3. Roughly corresponds to Future/Task in other frameworks
 *
 * TODO (OPTIMUS-0000): Avoid boxing by marking this class a relevant subclasses with specialized(double, int, etc...)
 */
abstract class Node[+T] extends NodeTask with NodeFuture[T] {

  override def asNode$ : Node[T] = this

  /**
   * Returns result of the Node's computation or throws an exception if Node completes with an exception. Calling result
   * before node completes is not guaranteed to return anything meaningful Note: Whoever uses this result needs to call
   * combineInfo
   */
  def result: T = throw new GraphException()
  override def result$ : T = result
  override def resultObject: Object = result.asInstanceOf[java.lang.Object]
  override def resultObjectEvenIfIncomplete: Object = null
  def safeResult: Try[T] =
    if (isDoneWithResult) Success(result)
    else if (isDoneWithException) Failure(exception)
    else throw new GraphException("Cannot request result of a node before its completion")

  /**
   * Currently there are 2 classes of everything Sync/Async (eventually this will go away) There are times where certain
   * optimizations are possible if it's known in advance what type of a Node this is This way def run can overriden and
   * customized.
   */
  def isFSM = false
  def func: T = throw new GraphException("func or funcAsync must have been overriden")

  /**
   * [PLUGIN_ENTRY] Used by rawNodes Blocking get - evaluates this node, returning result when done (or throwing any
   * evaluation exception). Note: we are about to remove the last exemption
   */
  @miscFlags(MiscFlags.NODESYNC_ONLY)
  final def get: T = getInternal
  @miscFlags(MiscFlags.NODESYNC_ONLY)
  override def get$ : T = getInternal
  override def exception$ : Throwable = exception

  /**
   * [PLUGIN_ENTRY] Looks up the node in cache, applies tweaks, blocks until node executes and returns result
   */
  def lookupAndGet: T = getInternal
  def lookupAndGetSI: T = getInternal
  def lookupAndGetJob: T = getInternalJob
  def lookupAndEnqueue: Node[T] = enqueue
  def lookupAndEnqueueJob: Node[T] = enqueueJob

  protected def getInternal: T = {
    // Note that a NullPointerException in this method generally means that the optimus runtime wasn't initialized
    // before running optimus code. For example a test class might not extend an optimus test framework, or a closure
    // might be running in a non-optimus thread.
    val ec = OGSchedulerContext.current()
    // we're assuming that unattached nodes are not shared between threads, so this should not be racy in normal usage
    if (scenarioStack() == null) attach(ec.scenarioStack)
    ec.runAndWait(this)
    ec.getCurrentNodeTask.combineInfo(this, ec)
    result
  }

  /*
   * [JOB_EXPERIMENTAL] This is the variant for @job-annotated nodes, it also publishes edge crumbs to track the dependency graph.
   * [PLUGIN_ENTRY] Used by rawNodes Blocking get - evaluates this node, returning result when done (or throwing any
   * evaluation exception). Note: we are about to remove the last exemption
   */
  final def getJob: T = getInternalJob

  protected def getInternalJob: T = {
    val ec = OGSchedulerContext.current()
    if (scenarioStack() == null) attach(ec.scenarioStack)
    Edges.ensureJobNodeTracked(this)
    ec.runAndWait(this)
    ec.getCurrentNodeTask.combineInfo(this, ec)
    result
  }

  @nodeSync
  @nodeSyncLift
  private[optimus] def await: T = get
  private[optimus] def await$withNode: T = get
  private[optimus] def await$queued: NodeFuture[T] = {
    // only attach if not already attached (matches behavior of #getInternal).
    // we're assuming that unattached nodes are not shared between threads, so this should not be racy in normal usage
    if (scenarioStack() == null) attach(EvaluationContext.scenarioStack)
    enqueueAttached
  }

  /**
   * [PLUGIN_ENTRY] Used by rawNodes Enqueues in the local scheduler queue Expected pattern: node.enqueue
   *
   * Notes: It's OK to enqueue node multiple times and even if's already completed.
   *
   * Should this be moved into NodeTask?
   */
  final def enqueue: Node[T] = {
    if (!isDone) {
      val ec = OGSchedulerContext.current()
      attach(ec.scenarioStack)
      ec.enqueueDirect(this)
    }
    this
  }

  /**
   * [JOB_EXPERIMENTAL] This is the variant for @job-annotated nodes
   */
  final def attachJob(ss: ScenarioStack): Unit = {
    attach(ss)
    Edges.ensureJobNodeTracked(this)
  }

  /*
   * [JOB_EXPERIMENTAL] This is the variant for @job-annotated nodes, it also publishes edge crumbs to track the dependency graph.
   * [PLUGIN_ENTRY] Used by rawNodes Enqueues in the local scheduler queue Expected pattern: node.enqueue
   */
  final def enqueueJob: Node[T] = {
    if (!isDone) {
      val ec = OGSchedulerContext.current()
      attachJob(ec.scenarioStack)
      ec.enqueueDirect(this)
    }
    this
  }

  /**
   * * Plugin generates calls to this method
   *
   * completeOrElseNullWithDependencyPropagation() if completed, return this - so we can continue the state machine
   * (continueWithFSM) else return null - so the state machine process in common way
   *
   * (can't define in NodeTask, the return type need to be Node[T] instead of NodeTask)
   */
  final def completeOrElseNullWDP(currentStateMachine: NodeStateMachine): NodeFuture[T] = {
    val curNode = currentStateMachine._result
    if (isDone) {
      curNode.combineInfo(this, currentStateMachine.getEC)
      this
    } else null
  }

  /**
   * Identical to enqueue except that it requires scenarioStack to be already attached e.g. given has to call this
   * version
   */
  final def enqueueAttached: Node[T] = {
    val ec = OGSchedulerContext.current()
    if (scenarioStack() eq null) throw new GraphInInvalidState()
    ec.enqueueDirect(this)
    this
  }

  // Given source node, produce a Node representing the transformed value.
  final def map[B](f: T => B): Node[B] = {
    val outer = this

    val result = new CompletableNodeM[B] {
      override def run(ec: OGSchedulerContext): Unit = {
        if (!outer.isDone) {
          outer.attach(scenarioStack())
          ec.enqueue(outer) // Schedule the result node as a continuation of the source node.
          outer.continueWith(this, ec)
        } else {
          // The outer node must be completed, so it is safe to read its info and result.
          combineInfo(outer, ec)
          if (outer.isDoneWithResult) completeWithResult(f(outer.result), ec)
          else if (outer.isDoneWithException) completeWithException(outer.exception, ec)
          else throwNodeCompletionException(outer)
        }
      }

      override def isFSM: Boolean = true
    }
    result
  }

  // Given source node, produce a Node representing the transformed value.
  final def flatMap[B](f: T => Node[B]): Node[B] = new FlatMapNode(this, f, true)

  protected def throwNodeCompletionException(otherNode: Node[_]): Unit = {
    import scala.jdk.CollectionConverters._
    val allStackThraces = Thread.getAllStackTraces
    val dumpString = allStackThraces.asScala
      .map { case (thread, traces) =>
        val threadInfo = s"${thread.toString}\n\t"
        val traceInfo = traces.map(_.toString).mkString("\n\t")
        s"$threadInfo$traceInfo"
      }
      .mkString("\n")
    throw new GraphException(s"""|Can complete from another node only when it itself is completed:
                                 |the other node: $otherNode this node: $this node
                                 |trace for the other node: \n${Node.getTraceForNodeAsString(otherNode)}
                                 |trace for this node:\n ${Node.getTraceForNodeAsString(this)}
                                 |thread dump $dumpString""".stripMargin)
  }

  def copyRemoteTweakInfoTo(targetNode: Node[_]): Unit = {}
}

abstract class CompletableNode[T] extends Node[T] {
  protected var _result: T = _
  final override def result: T = {
    if (isDoneWithResult) _result
    else if (isDoneWithException) throw wrapped(exception)
    else
      throw new GraphException("Cannot request result of a node before its completion (" + getClass.getName + ")")
  }
  override final def resultObjectEvenIfIncomplete: AnyRef = _result.asInstanceOf[AnyRef]

  final protected def initAsCompleted(value: T): Unit = { _result = value; super.initAsCompleted(); }

  // noinspection ScalaUnusedSymbol this is generated
  def funcFSM(n: CompletableNode[T], ec: OGSchedulerContext): Unit =
    throw new GraphInInvalidState("Compiler generated FSM is missing")

  /**
   * Records result and notifies waiters Note: to custom Node implementors: combineInfo has to be often used before this
   * call Note: _result is not volatile update to _state in the next step will MB
   */
  final def completeWithResult(value: T, ec: EvaluationQueue): Unit = {
    _result = value; complete(ec, NodeTask.STATE_DONE)
  }

  // call to this is generated in NodeFutureSystem
  final def completeWithResultFSM(value: T, continuation: NodeStateMachine): Unit = {
    _result = value; complete(continuation._schedulerContext, NodeTask.STATE_DONE)
  }

  /**
   * Similar API to completeWithResult, but will return silently if the node is already completed Should be used to
   * manually completed nodes and ONLY paired with other tryCompleteXXX Method also takes a lock!
   */
  final def tryCompleteWithResult(value: T, ec: EvaluationQueue): Boolean = {
    if (isDone) false
    else
      this.synchronized {
        if (isDone) false
        else {
          _result = value
          complete(ec, NodeTask.STATE_DONE)
          true
        }
      }
  }

  /** Can only be used on a brand new node */
  final protected def initAsCompleted(node: Node[T], ss: ScenarioStack): Unit = {
    // WARNING: Currently initializeAsCompleted is used by XS code and it deals with propagating
    // OOB info for DependencyTrackingScenario and XScenarios but not Auditor
    // Currently auditor and DependencyTrackingScenario don't work together, but this code has to be fixed before
    // TODO (OPTIMUS-17918): Review auditor usage with XS re-use
    // if(Settings.auditing) combineInfo(node)

    if (node.isDoneWithResult) {
      // We copy out value and NOT OOB info, the user of this function will have to deal with this explicitly
      _result = node.result
      initAsCompleted(ss) // Barrier
    } else if (node.isDoneWithException) {
      initAsFailed(node.exception(), ss) // Barrier
    } else
      throw new GraphInInvalidState()
  }

  final def completeFromNode(node: Node[T], eq: EvaluationQueue): Unit = {
    combineInfo(node, eq)
    if (node.isDoneWithResult) completeWithResult(node.result, eq)
    else if (node.isDoneWithException) completeWithException(node.exception, eq)
    else throwNodeCompletionException(node)
  }

  /**
   * toCompletedNode creates an instance of AlreadyCompletedNode from this node. The completed node will contain the
   * result, as well as all internal info (xinfo, state, etc.) This is useful in, e.g., Distribution to send the result
   * back to the client, including any info that might've been updated during the node execution. We don't want to send
   * the whole original node back as it also has the scenario stack, input arguments, etc.
   *
   * DIST/DMC which is currently the only client of this function, stores and extracts dependency manually (not sure why
   * it's better). They also have a manual 'override' interface (DmcManualCompletableNode) that can make legitimately
   * tracked node appear as scenario independent to DMC stores therefore we provide a kludge (as far as I see it) to
   * drop ttracks Much better way to break up that manual node into a regular node (tracked) and @si node that will be
   * stored in DMC
   *
   * Note: If you planning to cleanup this function consider unifying it with the toCompletedNodeWithTweakInfo
   *
   * Note: toCompletedNodeWithTweakInfo doesn't need this ttrack removal because it's called on XS nodes and those never
   * have ttracks. (However, external code should not depend on this internal implementation) Currently there is way too
   * much HIDDEN coupling between DIST and CORE
   *
   * Note: It's usefull for DMC/DIST to compute store other additional info that was computed and propagated with
   * _xinfo. e.g. Auditor information.
   *
   * See: OPTIMUS-42328
   */
  final private[optimus] def toCompletedNodeWithoutTracking: AlreadyCompletedOrFailedNode[T] = {
    val n = if (isDoneWithResult) {
      new AlreadyCompletedNode[T](result)
    } else {
      new AlreadyFailedNode[T](exception)
    }

    n._xinfo = _xinfo.info match {
      case _: TTrack => _xinfo.withoutInfo
      case _         => _xinfo
    }
    n
  }

  final private[optimus] def toCompletedNodeWithTweakInfo(converter: PropertyNode[_] => PropertyNode[_]) = {
    val completedNode = if (isDoneWithResult) {
      new AlreadyCompletedNodeWithTweakInfo[T](result, scenarioStack.tweakableListener.recordedTweakables, converter)
    } else {
      new AlreadyFailedNodeWithTweakInfo[T](exception, scenarioStack.tweakableListener.recordedTweakables, converter)
    }
    completedNode.combineInfo(this, OGSchedulerContext.current)
    completedNode
  }

  override def reset(): Unit = { super.reset(); _result = null.asInstanceOf[T] }
}

/** Base class for all raw nodes (i.e. not property nodes) */
abstract class CompletableRawNode[T] extends CompletableNode[T] {}

trait RawNodeExecutionInfo[T] {
  self: CompletableRawNode[T] =>
  override def executionInfo: NodeTaskInfo = NodeTaskInfo.NonEntityNode
}

trait RawNodeAlwaysUniqueExecutionInfo[T] {
  self: CompletableRawNode[T] =>
  override def executionInfo: NodeTaskInfo = NodeTaskInfo.NonEntityAlwaysUniqueNode
}

/*
  Direct children of CompletableNodeM will have a static profileId injected by the agent,
  as will direct children of classes declared in the `profiled` package.  I.e. we accumulate stats at the
  granularity of the (possibly synthetically) declared subclass.

  PropertyNodes get profileId from PropertyNode#profileId = propertyInfo.profile, i.e. they're unique per PropertyInfo.

  Other node classes get profileId via the default NodeTask#getProfileId=executionInfo.profile, which often means
  one per each of the static NodeTaskInfo#XXX (Default, Map, Given, etc), but any Node class can override executionInfo,
  so we could end up with unique NTI (and hence profileId) per object instance.

 */

abstract class CompletableNodeM[T] extends CompletableNode[T] {}

package profiled {

  /**
   * Base class for Node implementation created by plug-in for lifted nodes that are NOT calling other async functions.
   */
  abstract class NodeSync[T] extends CompletableRawNode[T] {
    override def run(ec: OGSchedulerContext): Unit = completeWithResult(func, ec)
  }
  abstract class NodeSyncWithExecInfo[T] extends NodeSync[T] with RawNodeExecutionInfo[T]
  abstract class NodeSyncAlwaysUnique[T] extends NodeSync[T] with RawNodeAlwaysUniqueExecutionInfo[T]
  abstract class NodeSyncStoredClosure[T] extends NodeSync[T] {
    override def executionInfo: NodeTaskInfo = NodeTaskInfo.StoredNodeFunction
  }

  abstract class NodeFSM[T] extends CompletableRawNode[T] {
    override def isFSM = true
    @transient final private var __k: NodeStateMachine = _
    final override def setContinuation(kx: NodeStateMachine): Unit = {
      __k = kx
    }

    override def reset(): Unit = {
      __k = null
      super.reset()
    }

    override def completeWithException(ex: Throwable, ec: EvaluationQueue): Unit = {
      __k = null // If cancelled via CS cancellation (i.e. while not actually running) we want to cleanup
      super.completeWithException(ex, ec)
    }

    final override def run(ec: OGSchedulerContext): Unit = {
      val lk = __k
      if (lk eq null)
        funcFSM(this, ec)
      else {
        __k = null
        lk.run(this, ec)
      }
    }
  }
  abstract class NodeFSMWithExecInfo[T] extends NodeFSM[T] with RawNodeExecutionInfo[T]
  abstract class NodeFSMAlwaysUnique[T] extends NodeFSM[T] with RawNodeAlwaysUniqueExecutionInfo[T]

  abstract class NodeFSMStoredClosure[T] extends NodeFSM[T] {
    override def executionInfo: NodeTaskInfo = NodeTaskInfo.StoredNodeFunction
  }

  abstract class NodeDelegate[A] extends CompletableRawNode[A] {
    override def isFSM = true // It's FSM as far as outside world is concerned

    override def run(ec: OGSchedulerContext): Unit = {
      val node = childNode
      node.continueWith(this, ec)
    }

    override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
      completeFromNode(child.asInstanceOf[Node[A]], eq)
    }

    /* Implemented by plug-in, must return just queued up node */
    protected def childNode: NodeFuture[A]
  }
  abstract class NodeDelegateWithExecInfo[A] extends NodeDelegate[A] with RawNodeExecutionInfo[A]
  abstract class NodeDelegateAlwaysUnique[A] extends NodeDelegate[A] with RawNodeAlwaysUniqueExecutionInfo[A]

  abstract class NodeDelegateStoredClosure[A] extends NodeDelegate[A] {
    override def executionInfo: NodeTaskInfo = NodeTaskInfo.StoredNodeFunction
  }

}

/**
 * Helper Node impl used in implementations of map etc.
 */
class ConverterNode[A, B](a: A, f: A => B) extends CompletableNode[B] {
  override def run(ec: OGSchedulerContext): Unit = completeWithResult(f(a), ec)
}

object AlreadyCompletedOrFailedNode {

  /**
   * Creates an AlreadyCompletedNode or AlreadyFailedNode with the result or exception of node. Input node must be
   * complete already. N.B. does not copy other information such as xinfo.
   */
  def withResultOf[A](node: Node[A]): Node[A] = {
    if (node.isDoneWithException) new AlreadyFailedNode[A](node.exception())
    else new AlreadyCompletedNode[A](node.result)
  }

  def apply[A](t: Try[A]): Node[A] = t match {
    case Failure(x) => new AlreadyFailedNode[A](x)
    case Success(r) => new AlreadyCompletedNode[A](r)
  }

  def apply[A](e: Either[Throwable, A]): Node[A] = apply(e.toTry)
}

trait AlreadyCompletedOrFailedNode[A] extends Node[A]

class AlreadyCompletedNode[A](val a: A) extends AlreadyCompletedOrFailedNode[A] {
  initAsCompleted()

  override def result: A = a
  override def resultObjectEvenIfIncomplete: AnyRef = a.asInstanceOf[AnyRef]
  override def reset(): Unit = {}
  override def isStable = true
  override def func: A = a
  override def run(ec: OGSchedulerContext): Unit = throw new GraphException("Can't run AlreadyCompletedNode")
  override def executionInfo: NodeTaskInfo = NodeTaskInfo.Constant

  // to avoid false positive matches in NodeFormatUI.filteredNodesFast (due to shared profileId and null subProfile)
  override def subProfile(): AnyRef = a.asInstanceOf[AnyRef]

  override def equals(that: Any): Boolean = that match {
    case acn: AlreadyCompletedNode[_] => Objects.equals(acn.a, this.a)
    case _                            => false
  }

  override def hashCode: Int = if (null == a) 0 else a.hashCode

  override def writePrettyString(sb: PrettyStringBuilder): PrettyStringBuilder = {
    sb ++= "CompletedNode[" ++=
      (a match {
        case s: Scenario        => "Scenario:" + s.hashCode // avoid possible recursion
        case Some(sc: Scenario) => "Scenario:" + sc.hashCode
        case null               => "null"
        case _                  => a.toString
      }) ++= "]"
  }
}

trait NodeWithTweakInfo[A] extends Node[A] {
  val recordedTweakables: TweakableListener
  val converter: PropertyNode[_] => PropertyNode[_]

  private[this] def applyConverter(input: Array[PropertyNode[_]]): Array[PropertyNode[_]] = {
    var i = 0
    while (i < input.length) {
      input(i) = converter(input(i))
      i += 1
    }
    input
  }

  private val recordedTweaks = recordedTweakables match {
    case rt: RecordedTweakables => new RecordedTweakables(applyConverter(rt.tweakable), rt.tweakableHashes, rt.tweaked)
    case _                      => null
  }

  override def copyRemoteTweakInfoTo(targetNode: Node[_]): Unit = if (recordedTweaks ne null) {
    targetNode.scenarioStack.combineTrackData(recordedTweaks, targetNode)
  }
}

class AlreadyFailedNodeWithTweakInfo[A](
    ex: Throwable,
    val recordedTweakables: TweakableListener,
    val converter: PropertyNode[_] => PropertyNode[_],
) extends AlreadyFailedNode[A](ex)
    with NodeWithTweakInfo[A]

class AlreadyCompletedNodeWithTweakInfo[A](
    a: A,
    val recordedTweakables: TweakableListener,
    val converter: PropertyNode[_] => PropertyNode[_])
    extends AlreadyCompletedNode[A](a)
    with NodeWithTweakInfo[A]

class AlreadyFailedNode[A](val ex: Throwable) extends AlreadyCompletedOrFailedNode[A] {
  initAsFailed(ex)
  override def result = throw wrapped(ex)
  override def reset(): Unit = {}
  override def isStable = true
  override def func = throw wrapped(ex)
  override def exception: Throwable = ex
  override def run(ec: OGSchedulerContext): Unit = throw new GraphException("Can't run AlreadyFailedNode")
  override def executionInfo: NodeTaskInfo = NodeTaskInfo.Constant
  override def equals(that: Any): Boolean = that match {
    case f: AlreadyFailedNode[_] => f.ex == this.ex
    case _                       => false
  }

  override def hashCode: Int = if (null == ex) 0 else ex.hashCode
  override def toString: String = "AlreadyFailedNode[" + ex + "]"
}

class MarkerNode(name: String) extends AlreadyCompletedNode(name) {
  override def run(ec: OGSchedulerContext): Unit = throw new GraphException("Can't run MarkerNode")
  override def executionInfo: NodeTaskInfo = NodeTaskInfo.Marker
  override def writePrettyString(sb: PrettyStringBuilder): PrettyStringBuilder = sb ++= result
}

class DelayOnChildCompletedNode(parent: NodeTask, child: NodeTask) extends NodeTask {
  attach(ScenarioStack.constantNC)
  override def executionInfo(): NodeTaskInfo = NodeTaskInfo.DelayOnChildCompleted
  override def run(ec: OGSchedulerContext): Unit = parent.onChildCompleted(ec, child)
}

class NodeAwaiterWithLatch(size: Int) extends NodeAwaiter() {
  val latch = new CountDownLatch(size)
  override def onChildCompleted(ec: EvaluationQueue, n: NodeTask): Unit = {
    latch.countDown()
  }
}

/**
 * Enable integration with other async APIs. See [[FutureNode]] and [[optimus.core.CoreAPI.delay$queued]] for example
 * usage.
 */
class NodePromise[A] private (
    executionInfo: NodeTaskInfo,
    timeoutMillis: Option[Int],
    scheduler0: Scheduler,
    ss0: ScenarioStack) {
  outer =>

  import scala.util.Failure
  import scala.util.Success
  import scala.util.Try

  private val ss: ScenarioStack = if (ss0 ne null) ss0 else EvaluationContext.scenarioStack
  private val scheduler = if (scheduler0 ne null) scheduler0 else EvaluationContext.scheduler

  /**
   * Asynchronously wait for the result from this promise. If the promise was completed with an exception, this call
   * will throw.
   *
   * This is the way to go back to optimus "direct-style" asynchrony.
   */
  @nodeSync
  @nodeSyncLift
  def await: A = promisedNode.await
  def await$withNode: A = promisedNode.await$withNode
  def await$queued: NodeFuture[A] = promisedNode.await$queued

  /**
   * Note that this doesn't propagate xInfo, and therefore should only be used to integrate with frameworks which don't
   * call back into the graph.
   */
  def complete(result: Try[A]): Boolean = promisedNode.completeWithTry(result, None)

  /**
   * Like complete, but also does a combineInfo with child.
   */
  def completeWithChild(result: Try[A], child: NodeTask): Boolean = promisedNode.completeWithTry(result, Some(child))

  def node: CompletableNode[A] = promisedNode

  // Note that this needs to be a val + class (rather than an inner object) since the PromisedNode
  // must be created eagerly, in the same thread as the NodePromise creation. Inner objects are lazy,
  // so there's a possibility they'll be created on the callback thread (via a `complete` call) which
  // would result in a null EvaluationContext.scenarioStack.
  private[this] val promisedNode = new PromisedNode

  private[this] class PromisedNode extends CompletableNode[A] {

    initAsRunning(ss)

    if (outer.executionInfo.reportingPluginType(ss) != null) {
      markAsHavingPluginType()
      // Normally this would happen in the scheduler, but this node doesn't actually get run.
      PluginType.fire(this)
    }

    // if we get cancelled at any point then abort immediately
    private[this] val cancelHandler: CancellationScope => Unit = { cs =>
      tryCompleteWithException(cs.cancellationCause, scheduler)
    }
    scenarioStack.cancelScope.addListener(cancelHandler)

    private val timeout = timeoutMillis.map { t =>
      CoreAPI.optimusScheduledThreadPool.schedule(
        () =>
          tryCompleteWithException(
            new TimeoutException(s"$executionInfo cancelled after timeout of $t millis"),
            scheduler),
        t,
        TimeUnit.MILLISECONDS
      )
    }

    override def executionInfo: NodeTaskInfo = outer.executionInfo

    private[graph] def completeWithTry(result: Try[A], child: Option[NodeTask]): Boolean = {
      scenarioStack.cancelScope.removeListener(cancelHandler)
      timeout.foreach(_.cancel(false))
      child.foreach(promisedNode.combineInfo(_, scheduler))
      val wasIncomplete = result match {
        case Success(value)     => tryCompleteWithResult(value, scheduler)
        case Failure(exception) => tryCompleteWithException(exception, scheduler)
      }
      wasIncomplete
    }
  }
}
object NodePromise {
  def apply[A](executionInfo: NodeTaskInfo = NodeTaskInfo.Promise): NodePromise[A] =
    new NodePromise[A](executionInfo, None, null, null)

  private[optimus] def createWithSpecificScheduler[A](
      executionInfo: NodeTaskInfo,
      scheduler: Scheduler,
      ss: ScenarioStack): NodePromise[A] =
    new NodePromise[A](executionInfo, None, scheduler, ss)

  def withTimeout[A](
      executionInfo: NodeTaskInfo = NodeTaskInfo.Promise,
      timeoutMillis: Int
  ): NodePromise[A] =
    new NodePromise[A](executionInfo, Some(timeoutMillis), null, null)
}

object FutureNode {
  import scala.concurrent.ExecutionContext
  import scala.concurrent.Future

  def apply[A](future: Future[A], executionContext: ExecutionContext, timeoutMillis: Option[Int] = None): Node[A] = {
    future.value match {
      case None =>
        val promise = timeoutMillis match {
          case Some(millis) => NodePromise.withTimeout[A](NodeTaskInfo.Future, millis)
          case _            => NodePromise[A](NodeTaskInfo.Future)
        }
        future.onComplete(promise.complete)(executionContext)
        promise.node
      case Some(Success(value)) =>
        new AlreadyCompletedNode[A](value) {
          override def executionInfo: NodeTaskInfo = NodeTaskInfo.Future
        }
      case Some(Failure(exception)) =>
        new AlreadyFailedNode[A](exception) {
          override def executionInfo: NodeTaskInfo = NodeTaskInfo.Future
        }
    }
  }
}

object Node {
  import scala.collection.compat.IterableOnce

  /**
   * Thread a collection of node into a node of the collection concurrently.
   */
  def sequence[A, CC[X] <: IterableOnce[X]](ns: CC[Node[A]], maxConcurrency: Int = Int.MaxValue)(implicit
      cbf: BuildFrom[CC[Node[A]], A, CC[A]]): Node[CC[A]] =
    new SequenceNode[A, CC[A]](null, maxConcurrency, knownSize(ns)) {
      private[this] val iterator = ns.toIterator
      private[this] val builder = cbf.newBuilder(ns)

      override def consumeIteration(i: Iteration): Unit = builder += i.node.result
      override def nextIteration = new Iteration(iterator.next())
      override def hasNextIteration: Boolean = iterator.hasNext

      override def getFinalResult: CC[A] = builder.result()
      override def sequenceExecutionInfo: SequenceNodeTaskInfo = NodeTaskInfo.Sequence
    }

  def getTraceForNodeAsString(node: NodeTask): String = {
    val sb = new optimus.platform.util.PrettyStringBuilder
    node.waitersToFullMultilineNodeStack(false, sb)
    sb.toString
  }
}

object MarkedAsAdaptedNode {
  private val cache = new ConcurrentHashMap[(PluginType, String), NodeTaskInfo]()
  private def info(pluginType: PluginType, infoName: String) = cache.computeIfAbsent(
    (pluginType, infoName),
    { case (pt, name) =>
      new NodeTaskInfo(name, 0) {
        override def reportingPluginType(ss: ScenarioStack): PluginType = pt
      }
    })
}

// Used by manually-written plugins to specify the 'promise' node (the graph exit point) as adapted
// Since it doesn't have a real plugin type, we're going to create a substitute.
abstract class MarkedAsAdaptedNode[A] extends CompletableNode[A] {
  markAsAdapted()
  private lazy val _executionInfo = MarkedAsAdaptedNode.info(manualPluginType, nodeTaskInfoName)
  final override def executionInfo(): NodeTaskInfo = _executionInfo
  // There will be one permanent NodeTaskInfo for each combination of the following two values, so these are
  // hopefully
  protected val nodeTaskInfoName: String
  protected val manualPluginType: PluginType
}

/* Create a new CompletableNode that maps the result of an outer node.
 *
 * If attach = false, we neither attach outer, nor the node produced by f.
 */
final class FlatMapNode[A, B](outer: Node[A], f: A => Node[B], attach: Boolean) extends CompletableNodeM[B] {
  override def run(ec: OGSchedulerContext): Unit = {
    if (attach) outer.attach(scenarioStack())
    ec.enqueue(this, outer) // Schedule the result node as a continuation of the source node.
    outer.continueWith(this, ec)
  }

  override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
    combineInfo(child, eq)
    if (child.isDoneWithException)
      completeWithException(child.exception, eq)
    else if (child eq outer) {
      // create the inner node
      try {
        val inner = f(outer.result)
        if (attach) inner.attach(scenarioStack())
        eq.enqueue(this, inner)
        inner.continueWith(this, eq)
      } catch {
        // probably "f" failed - in any case propagate the exception (don't throw it out of onChildCompleted because
        // that can crash the graph
        case NonFatal(x) => completeWithException(x, eq)
      }
    } else {
      completeWithResult(child.asInstanceOf[Node[B]].get, eq)
    }
  }
}
