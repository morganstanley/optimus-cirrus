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

import java.lang.reflect.InvocationTargetException
import java.util.Timer
import java.util.TimerTask
import optimus.exceptions.RTList.ExceptionOps
import optimus.graph._
import optimus.platform.PluginHelpers.{toNode, toNodeFactory, wrapped}
import optimus.platform.annotations._
import optimus.platform.internal.PartialFunctionMacro

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Holds the result of a computation within a nodeResult (camel case) scope.
 *
 * The result may be a value, if the computation succeeded, or an exception, if one was thrown or if cancellation
 * occurred. In any of these cases, warnings may have accrued.
 *
 * NodeResult could be, in principle, in any of 8 states, excluding warnings: cancelScope.isCancelled x
 * node.isDoneWithException x node.isDoneWithResult We disambiguate them such that hasValue and hasException are
 * mutually exclusive, and cancellation always results in an exception. If, somehow, there is both a value an exception,
 * the exception wins, and we discard the value.
 *
 * Warnings are preserved through the HOFs.
 *
 * The recommended way to extract a value from a NodeResult is by pattern matching: nodeResult { ??? } match { case
 * nr@NodeSuccess(v) => println(s"Returned $v with warnings ${nr.warnings}") case NodeFailure(e) => println(s"Failed
 * with $e") }
 */

@parallelizable
class NodeResult[+T] private[optimus] (_node: Node[T]) extends Serializable {
  private[this] var _value: T = _
  private[this] var _exception: Throwable = _
  private[this] var _cancelled: Boolean = _
  private[this] var _warnings: Set[Warning] = _

  if (_node.isDoneWithException) _exception = _node.exception
  else _value = _node.result
  _cancelled = _node.scenarioStack.cancelScope.isCancelled
  _warnings = _node.warnings

  override def toString: String = {
    val v = s"NodeResult(v=${if (hasValue) value else null}"
    if (warnings.nonEmpty || exception != null)
      v + s", w=$warnings, e=$exception)"
    else
      v + ")"
  }

  final def isCancelled: Boolean = _cancelled
  final def hasException: Boolean = _exception ne null
  final def hasValue: Boolean = !hasException

  final def valueOrElse[U >: T](e: Throwable => U): U = if (hasValue) value else e(exception)

  final def toTry: Try[T] = if (hasValue) Success(value) else Failure(exception)

  final def exception: Throwable =
    _exception match {
      case sc: ScopeWasCancelledException => sc.getCause
      case _                              => _exception
    }

  @impure
  final def warnings: Set[Warning] = _warnings

  final def notes: Set[Note] = _warnings.collect { case n: Note => n }

  // These are pure, because the warnings can't be observed using them.
  /** Watch out, this is a mutating API, it adds to _warnings */
  final def tunnelWarningsFrom(nr: NodeResult[_]): NodeResult[T] = {
    _warnings = if (nr.warnings ne warnings) _warnings.union(nr.warnings) else _warnings
    this
  }

  final def tunnelWarningsToCurrentNode(): Unit = {
    warnings.foreach { EvaluationContext.currentNode.attach(_) }
  }

  @impure
  final def hasWarnings: Boolean = warnings.nonEmpty

  final def hasNotes: Boolean = warnings.exists(_.isInstanceOf[Note])

  final def value: T = if (hasValue) _value else throw wrapped(exception)

  /**
   * Return the value if there is one, otherwise returns a default value, discarding any exception.
   */
  // TODO (OPTIMUS-0000): Re-deprecate after fixing dependent apps
  // @deprecated("Consider pattern matching instead, so you can actually check the exception.", "2015-08-01")
  def getOrElse[U >: T](default: => U): U = if (hasValue) value else default

  /**
   * Returns NodeResult if there is a value, otherwise applies f to the exception. Warnings accrue. E.g. nodeResult {
   * ... } recover { e:Throwable => doSomethingWithErrorAndCalculateAnAlternative(e) } recover { e:Throwable =>
   * thirdTimesTheCharm(e) } match { case NodeSuccess(v) => ??? case NodeFailure(e) => ??? }
   */
  @nodeSync
  @nodeSyncLift
  def recover[U >: T](@nodeLift f: Throwable => U): NodeResult[U] = recover$withNode { toNodeFactory(f) }
  @impure def recover$withNode[U >: T](f: Throwable => Node[U]): NodeResult[U] = recover$newNode(f).get
  @impure def recover$queued[U >: T](f: Throwable => Node[U]): Node[NodeResult[U]] = recover$newNode(f).enqueue
  // noinspection ScalaUnusedSymbol
  @impure def recover$queued[U >: T](f: Throwable => U): NodeFuture[NodeResult[U]] = recover$queued(toNodeFactory(f))
  @impure private def recover$newNode[U >: T](f: Throwable => Node[U]): Node[NodeResult[U]] =
    new CompletableNodeM[NodeResult[U]] {
      override def executionInfo(): NodeTaskInfo = NodeTaskInfo.Recover
      override def run(ec: OGSchedulerContext): Unit = {
        if (hasValue) completeWithResult(NodeResult.this, ec)
        else {
          val potentiallyRecoveredNode = f(NodeResult.this.exception)
          this.warnings.foreach(potentiallyRecoveredNode.attach)
          potentiallyRecoveredNode.attach(scenarioStack())
          ec.enqueue(potentiallyRecoveredNode)
          potentiallyRecoveredNode.continueWith(this, ec)
        }
      }

      override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
        val node =
          if (child.isDoneWithException) new AlreadyFailedNode[U](child.exception)
          else
            new AlreadyCompletedNode[U](child.asInstanceOf[Node[U]].result)
        (this.warnings ++ child.warnings()).foreach(node.attach)
        completeWithResult(new NodeResult(node).tunnelWarningsFrom(NodeResult.this), eq)
      }
    }

  // We've chosen not to expose the following, as they aren't consistent with the purpose of NodeResult, but
  // it seems useful to test that they work properly.
  @nodeSync
  @nodeSyncLift
  private[optimus /* platform */ ] def map[U](@nodeLift f: T => U): NodeResult[U] = map$withNode(toNodeFactory(f))
  @impure def map$withNode[U](f: T => Node[U]): NodeResult[U] = map$newNode(f).get
  @impure def map$queued[U](f: T => Node[U]): Node[NodeResult[U]] = map$newNode(f) enqueue
  @impure private def map$newNode[U](f: T => Node[U]): Node[NodeResult[U]] = new CompletableNodeM[NodeResult[U]] {
    override def executionInfo(): NodeTaskInfo = NodeTaskInfo.Map
    override def run(ec: OGSchedulerContext): Unit = {
      if (hasValue) {
        val mappedNode: Node[U] = f(value)
        mappedNode.attach(scenarioStack())
        ec.enqueue(mappedNode)
        mappedNode.continueWith(this, ec)
      } else {
        val errorNode = new AlreadyFailedNode[U](NodeResult.this.exception)
        this.warnings.foreach(errorNode.attach)
        completeWithResult(new NodeResult(errorNode), ec)
      }
    }
    override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
      val node =
        if (child.isDoneWithException) new AlreadyFailedNode[U](child.exception)
        else
          new AlreadyCompletedNode[U](child.asInstanceOf[Node[U]].result)
      (this.warnings ++ child.warnings()).foreach(node.attach)
      completeWithResult(new NodeResult(node).tunnelWarningsFrom(NodeResult.this), eq)
    }
  }

  @nodeSync
  @nodeSyncLift
  private[optimus /*platform */ ] def flatMap[U](@nodeLift f: T => NodeResult[U]): NodeResult[U] =
    flatMap$withNode(toNodeFactory(f))
  @impure def flatMap$withNode[U](f: T => Node[NodeResult[U]]): NodeResult[U] = flatMap$newNode(f).get
  @impure def flatMap$queued[U](f: T => Node[NodeResult[U]]): Node[NodeResult[U]] = flatMap$newNode(f).enqueue
  @impure private def flatMap$newNode[U](f: T => Node[NodeResult[U]]): Node[NodeResult[U]] =
    new CompletableNode[NodeResult[U]] {
      override def executionInfo(): NodeTaskInfo = NodeTaskInfo.FlatMap
      override def run(ec: OGSchedulerContext): Unit = {
        if (hasValue) {
          val mappedNodeResult: Node[NodeResult[U]] = f(value)
          mappedNodeResult.attach(scenarioStack())
          ec.enqueue(mappedNodeResult)
          mappedNodeResult.continueWith(this, ec)
        } else {
          val errorNode = new AlreadyFailedNode[U](NodeResult.this.exception)
          this.warnings.foreach(errorNode.attach)
          completeWithResult(new NodeResult(errorNode), ec)
        }
      }

      override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
        if (child.isDoneWithException) {
          val errorNode = new AlreadyFailedNode[U](child.exception)
          (this.warnings ++ child.warnings()).foreach(errorNode.attach)
          completeWithResult(new NodeResult(errorNode).tunnelWarningsFrom(NodeResult.this), eq)
        } else {
          val node = child.asInstanceOf[Node[NodeResult[U]]]
          (this.warnings ++ child.warnings()).foreach(node.attach)
          completeWithResult(node.result.tunnelWarningsFrom(NodeResult.this), eq)

        }
      }
    }

  @impure
  @nodeSync
  @nodeSyncLift
  def thenFinally(@nodeLiftByName @nodeLift f: => Unit): NodeResult[T] = thenFinally$withNode(toNode(f _))
  @impure def thenFinally$withNode(f: Node[Unit]): NodeResult[T] = thenFinally$newNode(f).get
  @impure def thenFinally$queued(f: Node[Unit]): NodeFuture[NodeResult[T]] = thenFinally$newNode(f).enqueue
  @impure def thenFinally$newNode(f: Node[Unit]): Node[NodeResult[T]] = new CompletableNodeM[NodeResult[T]] {
    override def executionInfo(): NodeTaskInfo = NodeTaskInfo.ThenFinally
    override def run(ec: OGSchedulerContext): Unit = {
      f.attach(scenarioStack.withChildCancellationScope())
      ec.enqueue(f)
      f.continueWith(this, ec)
    }
    override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
      // f might throw an exception conditionally based on tweak state, so combine tracking info for 'f' but complete
      // with original NodeResult
      combineInfo(child, eq)
      val nr = NodeResult.this
      if (child.isDoneWithException) { // ie, the finally block threw an exception
        val finallyException = child.exception
        if (nr.hasException) { // ie, the try block threw an exception
          val tryException = nr.exception
          tryException.addSuppressed(finallyException) // so that we see the app exception rather than cleanup exception
          completeWithException(tryException, eq)
        } else completeWithException(finallyException, eq)
      } else completeWithResult(nr, eq)
    }
  }
}

private[optimus] class NodeResultRethrow[T](block: Node[T], cleanup: NodeResult[T] => Node[Unit])
    extends CompletableNodeM[T] {
  override def run(ec: OGSchedulerContext): Unit = {
    block.attach(scenarioStack)
    ec.enqueue(block)
    block.continueWith(this, ec)
  }
  final override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
    if (child eq block) {
      combineInfo(child, eq)
      if (child.isDoneWithResult) this.completeWithResult(block.result, eq)
      else {
        val nr = new NodeResult(block)
        val cu = cleanup(nr)
        cu.attach(scenarioStack.withChildCancellationScope())
        eq.enqueue(this, cu)
        // although we don't need the result of cleanup, we'll wait for it to complete before we complete ourselves
        // with exception so that the exception doesn't cause cancellation of the enclosing cancellation scope
        cu.continueWith(this, eq)
      }
    } else {
      // If we're here, the block must have thrown, and the child might have.
      // Replace block exception with child's unless that would hide a non-RT exception
      val e =
        if (child.isDoneWithResult || (child.exception.isRT && !block.exception.isRT)) block.exception
        else child.exception
      this.completeWithException(e, eq)
    }
  }
}

private[optimus] class NodeResultProgressNode[T](
    cn: Node[T],
    weight: Double,
    initialMessage: String,
    listener: ProgressListener,
    reportingIntervalMs: Long)
    extends CompletableNodeM[T] {

  def this(cn: Node[T], weight: Double, message: String, reportingIntervalMs: Long) =
    this(cn, weight, message, null, reportingIntervalMs)

  def this(cn: Node[T], listener: ProgressListener, progressReportingMs: Long) =
    this(cn, 1.0, null, listener, progressReportingMs)

  override def run(ec: OGSchedulerContext): Unit = {
    val params =
      ProgressTrackerParams(
        weight = weight,
        initialMessage = initialMessage,
        listener = listener,
        reportingIntervalMs = reportingIntervalMs)

    val newSS = scenarioStack.withProgressTracker(params)

    ProgressTracker.sendInitialMessageIfPossible(newSS.progressTracker)

    cn.replace(newSS)
    ec.enqueue(cn)
    cn.continueWith(this, ec)
  }

  final override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
    val progressTracker = cn.scenarioStack.progressTracker

    // report that this child has been completed; this means its progress now is 100%
    // and if it has a weight associated, it will propagate its contribution up to parents
    ProgressTracker.reportCompleteIfPossible(progressTracker)

    completeFromNode(cn, eq)
  }
}

private[optimus] class NodeResultCancelNode[T](cn: Node[T], cs: CancellationScope, timeout: Long)
    extends CompletableNodeM[Option[T]] {
  var timer: Timer = _
  override def run(ec: OGSchedulerContext): Unit = {
    if (timeout > 0) {
      timer = new Timer()
      timer.schedule(
        new TimerTask {
          override def run(): Unit = {
            cs.cancel(s"Timed out after $timeout ms")
          }
        },
        timeout)

    }
    if (cs ne null)
      cn.attach(scenarioStack.withCancellationScope(cs))
    else
      cn.attach(scenarioStack())
    ec.enqueue(cn)
    cn.continueWith(this, ec)
  }
  final override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
    if (timer ne null) { timer.cancel() }
    combineInfo(child, eq)

    if (cs.isCancelled) {
      _xinfo = _xinfo.withoutException
      this.completeWithResult(None, eq)
    } else if (cn.isDoneWithResult)
      this.completeWithResult(Some(cn.result), eq)
    else
      this.completeWithException(cn.exception, eq)
  }
}

object NodeSuccess {
  def unapply[T](nr: NodeResult[T]): Option[T] = if (nr.hasValue) Some(nr.value) else None
}
object NodeFailure {
  def unapply(nr: NodeResult[_]): Option[Throwable] = if (!nr.hasValue) Some(nr.exception) else None
}

private[optimus /*platform*/ ] object ContainsRTException {
  @scala.annotation.tailrec
  def unapply(t: Throwable): Option[Throwable] = {
    t match {
      case ie: InvocationTargetException => unapply(ie.getTargetException)
      case NonFatal(e) if e.isRT         => Some(e)
      case _                             => None
    }
  }
}

/**
 * Node analog of Try, for exception flow control using combinators. Unlike Try, NodeTry makes it difficult to handle
 * non-RT exceptions. Unlike NodeResult, it has full combinators.
 */
sealed trait NodeTry[+T] {

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def get: T
  def get$queued: NodeFuture[T]
  def get$withNode: T

  def toOption: Option[T]

  def toTry: Try[T]

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def map[U](@nodeLift f: T => U): NodeTry[U]
  def map$queued[U](f: T => Node[U]): Node[NodeTry[U]]
  def map$withNode[U](f: T => Node[U]): NodeTry[U]

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def flatMap[U](@nodeLift f: T => NodeTry[U]): NodeTry[U]
  def flatMap$queued[U](f: T => Node[NodeTry[U]]): Node[NodeTry[U]]
  def flatMap$withNode[U](f: T => Node[NodeTry[U]]): NodeTry[U]
  /*
    1. If recovery partial function is defined and succeeds, complete with its result.
    2. If recovery partial function is not defined, complete with original exception.
    3. If recovery partial function has exception, complete with new exception.
   */
  def apply[U >: T](pf: PartialFunction[Throwable, U]): U =
    macro PartialFunctionMacro.recoverAndGetTransform[T, U]
  def getOrRecover[U >: T](pf: PartialFunction[Throwable, U]): U =
    macro PartialFunctionMacro.recoverAndGetTransform[T, U]

  def recover[U >: T](pf: PartialFunction[Throwable, U]): NodeTry[U] =
    macro PartialFunctionMacro.recoverTransform[T, U]
  def recoverWith[U >: T](pf: PartialFunction[Throwable, NodeTry[U]]): NodeTry[U] =
    macro PartialFunctionMacro.recoverWithTransform[T, U]
}

@parallelizable
class NodeTryImpl[+T] private[optimus] (private val node: Node[T]) extends NodeTry[T] with Serializable {
  // node must be a stable value (see the commentary in NodeTryNode#onChildCompleted)
  if (Settings.schedulerAsserts && !node.isStable) throw new GraphException(s"Expected stable node but got $node")

  // A new NodeTry, preserving the parent cancellation scope
  private def newNodeTry[U](nu: Node[U]): NodeTry[U] = {
    // Convert to a stable value (see the commentary in NodeTryNode#onChildCompleted)
    new NodeTryImpl(AlreadyCompletedOrFailedNode.withResultOf(nu))
  }

  private[optimus] def exception$private$testing_only: Throwable = node.exception

  private abstract class CN[A] extends CompletableNodeM[A] {

    protected def recoverUsing[U](eq: EvaluationQueue, pf: OptimusPartialFunction[Throwable, U]): Unit = {
      node.exception match {
        case ContainsRTException(e) =>
          val recovery: Node[Option[U]] = pf.tryApply$newNode(e)
          recovery.attach(scenarioStack())
          eq.enqueue(this, recovery)
          recovery.continueWith(this, eq)
        case _: Throwable =>
          completeWithException(node.exception, eq)
      }
    }

    // Explicitly strip out exception when copying info up to NodeTry node.  We will restore it
    // when the value is returned.
    def combineInfoExceptException(child: NodeTask, eq: EvaluationQueue): Unit = {
      combineInfo(child, eq)
      _xinfo = _xinfo.withoutException
    }
  }

  private abstract class NTCN[U] extends CN[NodeTry[U]] {
    def completeWithNodeTryException(child: NodeTask, eq: EvaluationQueue): Unit = {
      // "exception to hide" means a non-RT exception, and since NodeTry is not supposed to catch non-RT exceptions
      // we should let it pass straight through us. All other exceptions get boxed into a NodeTry.
      if (child.isDoneWithExceptionToHide)
        completeWithException(child.exception, eq)
      else
        this.completeWithResult(newNodeTry(new AlreadyFailedNode(child.exception)), eq)
    }
  }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def map[U](@nodeLift f: T => U): NodeTry[U] = map$withNode(toNodeFactory(f))
  def map$queued[U](f: T => Node[U]): Node[NodeTry[U]] = map$newNode(f).enqueue
  def map$withNode[U](f: T => Node[U]): NodeTry[U] = map$newNode(f).get
  def map$newNode[U](f: T => Node[U]): Node[NodeTry[U]] = new NTCN[U] {
    override def run(ec: OGSchedulerContext): Unit = {
      // NodeTry node is already completed at this point
      combineInfoExceptException(node, ec)
      if (node.isDoneWithResult) {
        val nu: Node[U] = f(node.result)
        nu.attach(scenarioStack())
        ec.enqueue(nu)
        nu.continueWith(this, ec)
      } else {
        completeWithNodeTryException(node, ec)
      }
    }

    final override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
      combineInfoExceptException(child, eq)
      if (child.isDoneWithResult) {
        val tnu: NodeTry[U] = newNodeTry(child.asInstanceOf[Node[U]])
        this.completeWithResult(tnu, eq)
      } else
        this.completeWithNodeTryException(child, eq)
    }
  }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def flatMap[U](@nodeLift f: T => NodeTry[U]): NodeTry[U] = flatMap$withNode(toNodeFactory(f))
  def flatMap$withNode[U](f: T => Node[NodeTry[U]]): NodeTry[U] = flatMap$newNode(f).get
  def flatMap$queued[U](f: T => Node[NodeTry[U]]): Node[NodeTry[U]] = flatMap$newNode(f).enqueue
  def flatMap$newNode[U](f: T => Node[NodeTry[U]]): Node[NodeTry[U]] = new NTCN[U] {
    override def run(ec: OGSchedulerContext): Unit = {
      combineInfoExceptException(node, ec)
      if (node.isDoneWithResult) {
        val nu: Node[NodeTry[U]] = f(node.result)
        nu.attach(scenarioStack())
        ec.enqueue(nu)
        nu.continueWith(this, ec)
      } else {
        completeWithNodeTryException(node, ec)
      }
    }

    final override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
      combineInfoExceptException(child, eq)
      if (child.isDoneWithResult) {
        val nu: Node[NodeTry[U]] = child.asInstanceOf[Node[NodeTry[U]]]
        this.completeWithResult(nu.result, eq)

      } else {
        this.completeWithNodeTryException(child, eq)
      }
    }
  }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def get: T = get$newNode.get
  def get$withNode: T = get$newNode.get
  def get$queued: NodeFuture[T] = get$newNode.enqueue
  def get$newNode: Node[T] = {
    // We've been combining info into the NodeTry, so anyone who continues on the .get will
    // get accumulated info.
    node
  }

  def toOption: Option[T] = {
    if (node.isDoneWithResult) Some(node.result) else None
  }

  def toTry: Try[T] = {
    if (node.isDoneWithResult) Success(node.result)
    else Failure(node.exception())
  }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def recoverAndGetPF[U >: T](pf: OptimusPartialFunction[Throwable, U]): U = recoverAndGetPF$withNode(pf)
  def recoverAndGetPF$withNode[U >: T](pf: OptimusPartialFunction[Throwable, U]): U = recoverAndGetPF$newNode(pf).get
  def recoverAndGetPF$queued[U >: T](pf: OptimusPartialFunction[Throwable, U]): NodeFuture[U] =
    recoverAndGetPF$newNode(pf).enqueue
  def recoverAndGetPF$newNode[U >: T](pf: OptimusPartialFunction[Throwable, U]): Node[U] = {
    class recoverAndGet extends CN[U] {
      override def executionInfo: NodeTaskInfo = NodeTaskInfo.RecoverAndGet
      override def run(ec: OGSchedulerContext): Unit = {
        combineInfoExceptException(node, ec)
        if (node.isDoneWithResult)
          this.completeWithResult(node.result, ec)
        else
          recoverUsing(ec, pf)
      }
      final override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
        combineInfoExceptException(child, eq)
        val recovery = child.asInstanceOf[Node[Option[U]]]
        if (recovery.isDoneWithResult) recovery.result match {
          case Some(u) => this.completeWithResult(u, eq)
          case None    => this.completeWithException(node.exception, eq)
        }
        else
          this.completeWithException(recovery.exception, eq)
      }
    }
    new recoverAndGet()
  }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def recoverPF[U >: T](pf: OptimusPartialFunction[Throwable, U]): NodeTry[U] = recoverPF$withNode(pf)
  def recoverPF$withNode[U >: T](pf: OptimusPartialFunction[Throwable, U]): NodeTry[U] = recoverPF$newNode(pf).get
  def recoverPF$queued[U >: T](pf: OptimusPartialFunction[Throwable, U]): Node[NodeTry[U]] =
    recoverPF$newNode(pf).enqueue
  def recoverPF$newNode[U >: T](pf: OptimusPartialFunction[Throwable, U]): Node[NodeTry[U]] = new NTCN[U] {
    override def run(ec: OGSchedulerContext): Unit = {
      combineInfoExceptException(node, ec)
      if (node.isDoneWithResult)
        this.completeWithResult(newNodeTry(node), ec)
      else
        recoverUsing(ec, pf)
    }
    final override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
      combineInfoExceptException(child, eq)
      val recovery = child.asInstanceOf[Node[Option[U]]]
      if (recovery.isDoneWithResult) recovery.result match {
        case Some(u) => this.completeWithResult(newNodeTry(new AlreadyCompletedNode(u)), eq)
        case None    => this.completeWithNodeTryException(node, eq)
      }
      else
        this.completeWithNodeTryException(recovery, eq)
    }
  }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def recoverWithPF[U >: T](pf: OptimusPartialFunction[Throwable, NodeTry[U]]): NodeTry[U] =
    recoverWithPF$newNode(pf).get
  def recoverWithPF$withNode[U >: T](pf: OptimusPartialFunction[Throwable, NodeTry[U]]): NodeTry[U] =
    recoverWithPF$newNode(pf).get
  def recoverWithPF$queued[U >: T](pf: OptimusPartialFunction[Throwable, NodeTry[U]]): Node[NodeTry[U]] =
    recoverWithPF$newNode(pf).enqueue
  def recoverWithPF$newNode[U >: T](pf: OptimusPartialFunction[Throwable, NodeTry[U]]): Node[NodeTry[U]] = new NTCN[U] {
    override def run(ec: OGSchedulerContext): Unit = {
      combineInfoExceptException(node, ec)
      if (node.isDoneWithResult)
        this.completeWithResult(newNodeTry(node), ec)
      else
        recoverUsing(ec, pf)
    }
    final override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
      combineInfoExceptException(child, eq)
      val recovery = child.asInstanceOf[Node[Option[NodeTry[U]]]]
      if (recovery.isDoneWithResult) recovery.result match {
        case Some(u) => completeWithResult(u, eq)
        case None    => completeWithNodeTryException(node, eq)
      }
      else
        this.completeWithNodeTryException(recovery, eq)
    }
  }

  // we behave like a value type, so delegate hashcode/equals to the node (which is an AlreadyCompletedOrFailedNode
  // so has value based equality and hashcode)
  override def hashCode: Int = node.hashCode()
  override def equals(obj: Any): Boolean = obj match {
    case n: NodeTryImpl[_] => n.node == node
    case _                 => false
  }
  override def toString: String = s"NodeTry[${node.toString}]"
}

private[optimus] class NodeTryNode[T](cn: Node[T]) extends CompletableNodeM[NodeTry[T]] {
  override def run(ec: OGSchedulerContext): Unit = {
    cn.attach(scenarioStack)
    ec.enqueue(cn)
    cn.continueWith(this, ec)
  }
  final override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
    // since the NodeTryNode is the computation of a "NodeTry { X }" block it needs to take dependency on any tracking
    // information (or warnings etc.) from the computation of X, but not any Exception (since Exceptions are treated
    // as part of our value rather than thrown up). This means that the node which ran the NodeTry { X } block will
    // correctly take dependency on { X } but not the Exception.
    combineInfo(child, eq)
    _xinfo = _xinfo.withoutException

    // "exception to hide" means a non-RT exception, and since NodeTry is not supposed to catch non-RT exceptions
    // we should let it pass straight through us
    if (child.isDoneWithExceptionToHide)
      completeWithException(child.exception, eq)
    else {
      // The *result* of a NodeTryNode is a NodeTry which is a *value* just like a Try. Using that value should *not*
      // cause any propagation of dependency tracking information, because Optimus only tracks dependency based on
      // node calls, not based on tracking the results of those node calls. For example, in "@node def f = NodeTry { X }",
      // f, depends on the dependencies X, but in "f(NodeTry { X })", f only depends on the value of X, not on the
      // dependencies of X. Therefore we convert X to an ACOFN.
      val resultOnly = AlreadyCompletedOrFailedNode.withResultOf(child.asInstanceOf[Node[T]])
      completeWithResult(new NodeTryImpl(resultOnly), eq)
    }
  }
}

private[optimus] class ExtractNotesNode[T](cn: Node[T]) extends CompletableNode[(Set[Note], T)] {
  override def run(ec: OGSchedulerContext): Unit = {
    cn.attach(scenarioStack())
    ec.enqueue(cn)
    cn.continueWith(this, ec)
  }
  final override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
    combineInfo(child, eq)
    // Notes should  be removed, irrespective of whether there's a result
    val (notes, xinfo) = _xinfo.withoutExtractedNotes
    _xinfo = xinfo
    if (child.isDoneWithResult) {
      completeWithResult((notes, child.asInstanceOf[Node[T]].result), eq)
    } else completeWithException(child.exception, eq)
  }
}
