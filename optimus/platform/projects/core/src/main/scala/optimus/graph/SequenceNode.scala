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

import optimus.graph.ConcurrentAggregatorNode.RTExceptionsOnOutOfOrderCollections
import optimus.graph.ConcurrentAggregatorNode.WillCompleteWithException
import optimus.graph.DiagnosticSettings.getBoolProperty
import optimus.platform.UnspecifiedProgressReporter
import optimus.platform._
import optimus.scalacompat.collection.IterableLike

import java.util
import scala.util.control.NonFatal

/**
 * Notes:
 *   1. This node schedules itself multiple times up to maxConcurrency
 *
 *   1. Handles the case where the nodes are not generated on the fly and can be in different stages of processing
 *
 * Assumptions:
 *   1. All callbacks do NOT throw
 *
 *   1. All callbacks do NOT take locks
 *
 *   1. All callbacks are NOT thread safe and need to be protected under MT
 *
 *   1. All callbacks are very fast (i.e. OK to execute under lock)
 *
 * Consider:
 *   1. Limiting concurrency by the number of outstanding results. Currently if the first node is the slowest and we
 *      require sequence order to be preserved we'll end up accumulating all the results before reducing
 *
 * Improvements:
 *   1. Optimize for sequence of size 1?
 *
 *   1. Optimize for concurrency = 1
 *
 *   1. Optimize for Specific collection specifically indexed collections
 *
 *   1. This class assumes that the order must be preserved, it's possible to optimize in the relaxed case
 *
 *   1. In order to avoid so much locking consider getting a few nodes at a time
 *
 *   1. Recycle Iterations
 *
 *  On RT in the face of exceptions see: OPTIMUS-69155
 */
object ConcurrentAggregatorNode {
  val RTExceptionsOnOutOfOrderCollections: Boolean = getBoolProperty("optimus.graph.rtExceptions", false)

  /**
   * This should probably be never used.
   * It's possible to get into deadlock if there is a resource dependency between iterations.
   * Setting this flag to true, would simply allow one to test this theory.
   */
  private val DEBUG_CONTINUE_GENERATE_ITERATIONS_ON_EXCEPTION: Boolean =
    getBoolProperty("optimus.graph.cgi_on_ex", false)

  private[graph] val WillCompleteWithException: AnyRef = new AnyRef

  /**
   * This node is placed on the scheduler queue by SequenceNode#bootstrapIteration - see the comments there for the
   * description of semantics and lifetime.
   *
   * It's separate from SequenceNode mainly because SequenceNode completes during onChildCompleted, so cannot safely
   * restart (it's unsafe to restart while you might be completing on a different thread), and also so that we can
   * release the heavy SequenceNode more aggressively (see detach())
   */
  private[graph] final class Bootstrap(@volatile private[this] var owner: ConcurrentAggregatorNode[_, _])
      extends CompletableNode[Unit] {

    // capture this in a val since owner can be null later
    override val executionInfo: NodeTaskInfo = owner.sequenceExecutionInfo.bootstrapInfo

    attach(owner.scenarioStack())

    override def run(ec: OGSchedulerContext): Unit = {
      val o = owner
      if (o ne null) o.bootstrapIteration(ec)
      else completeWithResult((), ec)
    }

    /**
     * 1) Tries to remove the Bootstrap from the queue (which only works if it was the previous node on the current
     * queue, which in the common non-work-stolen case it will be). If it doesn't work, we'll get cleaned up at some
     * point when the thread pops us to find other work or goes idle.
     *
     * 2) Removes reference to the owner (to allow for the large SequenceNode to be collected early even when #1 fails)
     *
     * Note that we don't complete this node here, otherwise we'd have the same bug of completing during
     * onChildCompleted
     */
    def detachAndCleanup(eq: EvaluationQueue): Unit = {
      owner = null
      eq.tryToRemove(this)
    }
  }
}

abstract class ConcurrentAggregatorNode[B, ResultT] extends CompletableNode[ResultT] {
  import ConcurrentAggregatorNode._

  def maxConcurrency: Int

  /**
   * Current concurrency level. Always updated under lock
   * Note: iterationsInFlight < 0 means stop processing. isDone() avoided because we want to complete() outside of
   * any locks and iterationsInFlight dropping below 0 is done under lock
   */
  private var iterationsInFlight: Int = _
  final protected def isDoneProcessing: Boolean = iterationsInFlight < 0
  final protected def willCompleteWithException: Boolean = {
    if (DEBUG_CONTINUE_GENERATE_ITERATIONS_ON_EXCEPTION) isDoneWithException
    else _result.asInstanceOf[AnyRef] eq WillCompleteWithException
  }

  def length: Int = -1

  /** Note: hasNextIteration can be called N times before single consume is called */
  def hasNextIteration: Boolean
  def nextIteration: Iteration
  def consumeIteration(i: Iteration): Unit
  def getFinalResult: ResultT

  protected def initializeLoopReporting(): Unit

  protected def createIteration(): Iteration
  protected def removeIteration(it: Iteration): Unit

  /** Returns true to continue. NOTE: under lock */
  protected def processCompletedIterations(iteration: Iteration, eq: EvaluationQueue): Boolean

  override def executionInfo: NodeTaskInfo = sequenceExecutionInfo
  protected def sequenceExecutionInfo: SequenceNodeTaskInfo

  protected def getAndRunNextIteration(completedIteration: Iteration, eq: EvaluationQueue): Unit = {
    var prev = completedIteration
    var continue = true

    do {
      var next: Iteration = null
      // SequenceNode.this is internal class. it's OK to sync on 'this'
      // It's important to complete node outside of synchronized block, we compute the next "state" under lock
      val observedIterationInFlight = this.synchronized {
        if (isDoneProcessing) return // Note: return to fully abort further processing/
        iterationsInFlight -= 1 // Processed 1 iteration
        if (processCompletedIterations(prev, eq)) {
          next = createIteration()
          if (next ne null) iterationsInFlight += 1
        } else iterationsInFlight = -1 // Short-circuit
        if (iterationsInFlight == 0) iterationsInFlight = -1 // Full stop
        iterationsInFlight
      }

      if (observedIterationInFlight <= 0) {
        bootstrap.detachAndCleanup(eq)
        if (this.exception ne null)
          completeWithException(exception, eq) // Pre-stashed in processCompletedIterations
        else
          try { completeWithResult(getFinalResult, eq) }
          catch { case NonFatal(e) => completeWithException(e, eq) }
      }

      if (next eq null)
        continue = false // Done with the entire sequence
      else if (runIteration(next, eq))
        prev = next // Iteration is already complete, so continue the loop
      else
        continue = false // Iteration is in progress.  We'll be called again when it finishes.
    } while (continue)
  }

  /** Returns true if the iteration is already completed */
  protected final def runIteration(it: Iteration, eq: EvaluationQueue): Boolean = {
    val node = it.node
    // Inline the call to node.continueWith(this, eq) here.
    // Done to avoid stack overflow if the nodes are already completed (and would call back right away)
    if (node.tryAddToWaiterList(it)) {
      eq.enqueue(this, node)
      false
    } else
      true
  }

  /**
   * We run only once in order to invoke bootstrapIteration (or to complete if the sequence is empty).
   *
   * It's important that we don't run again after we have enqueued iterations, because we complete during callbacks from
   * those iterations (which may run on other scheduler threads), and it's racy to complete on one thread while starting
   * on another.
   */
  override def run(ec: OGSchedulerContext): Unit = {
    initializeLoopReporting()

    if (maxConcurrency == 1) OGSchedulerLostConcurrency.updateScenarioStackForReporting(this)
    if (!hasNextIteration) {
      // There were no items to process
      try {
        val fr = getFinalResult
        completeWithResult(fr, ec)
      } catch {
        case e: Exception => completeWithException(e, ec)
      }
    } else {
      bootstrap = new Bootstrap(this)
      bootstrapIteration(ec)
    }
  }

  protected[this] var bootstrap: Bootstrap = _

  /**
   * Each time it runs the Bootstrap node calls this method to enqueue itself and then another iteration node. If the
   * iteration doesn't suspend and there are no other idle scheduler threads, we'll just repeatedly pick up the next
   * iteration (which queues the subsequent iteration as part of its completion), and when Bootstrap is next picked up
   * we're already done.
   *
   * If the iteration does suspend, we will find Bootstrap on the queue and run it to start another iteration, and/or if
   * there is an idle thread, it may work-steal Bootstrap and run it, in which case the above process starts on that
   * thread (and so forth to other idle threads).
   *
   * Note that Bootstrap is *not* waiting for the iterations. It does nothing unless it is work-stolen or the iteration
   * suspends.
   */
  protected def bootstrapIteration(ec: OGSchedulerContext): Unit = {
    var next: Iteration = null

    val stateOfInFlight = this.synchronized {
      if (iterationsInFlight < maxConcurrency) {
        next = createIteration()
        if (next ne null) iterationsInFlight += 1
      }
      iterationsInFlight
    }

    if (next ne null) {
      // If we are to re-enqueue ourself we have to do it before we enqueue the next iteration, because the "queue" is
      // actually a stack, and we want the next iteration to be run before we are run
      if (stateOfInFlight < maxConcurrency) {
        if (bootstrap.tryMarkAsRunnable) ec.enqueue(null, bootstrap)
        // Need to "wait on" bootstrap (even though we're not actually registered as a waiter) so that the scheduler can
        // find it if it's looking for work (see [WAIT_ON_BOOTSTRAP])
      }

      setWaitingOn(bootstrap)

      if (runIteration(next, ec))
        getAndRunNextIteration(next, ec) // iteration was already complete so run another
    } else bootstrap.detachAndCleanup(ec)
  }

  /** Represents a single iteration of processing of a given sequence */
  protected class Iteration(val node: Node[B]) extends NodeAwaiter with DebugWaitChainItem {
    override def awaiter: NodeCause = ConcurrentAggregatorNode.this
    var next: Iteration = _ // used to maintain doubly-linked list
    var prev: Iteration = _ //
    var progressTrackerOrReporter: AnyRef = _
    def progressTracker: ProgressTracker = progressTrackerOrReporter.asInstanceOf[ProgressTracker]
    def progressReporter: ProgressReporter = progressTrackerOrReporter.asInstanceOf[ProgressReporter]
    def item: Any = null // optional used by reporters
    override def onChildCompleted(eq: EvaluationQueue, ntsk: NodeTask): Unit = getAndRunNextIteration(this, eq)
    final override def addToWaitChain(appendTo: util.ArrayList[DebugWaitChainItem], includeProxies: Boolean): Unit = {
      ConcurrentAggregatorNode.this.addToWaitChain(appendTo, includeProxies)
    }
  }
}

abstract class CollectionAggregatorNode[B, R](
    val marker: ProgressMarker,
    final override val maxConcurrency: Int,
    override val length: Int)
    extends ConcurrentAggregatorNode[B, R] {

  private[this] var head: Iteration = _ // First iteration in flight that wasn't consumed yet
  /** Support for choice in consuming iterations */
  protected var consumeInOrder: Boolean = true

  /** Support short-circuit By default consume the entire sequence */
  protected def stopConsuming = false

  /** Returns null when done. Note: under lock */
  override protected def createIteration(): Iteration = {
    // If exception was seen, stop creating new iterations. Note that we will continue consuming
    // in order to make sure that there is no earlier exception. (This is needed to be RT)
    if (isDoneProcessing || willCompleteWithException || !hasNextIteration) null
    else {
      val it = nextIteration
      instrumentIteration(it)

      if (head eq null) {
        head = it
        it.next = it
        it.prev = it
        bootstrap.setWaitingOn(head.node) // see [WAIT_ON_BOOTSTRAP]
      } else {
        val last = head.prev
        it.next = head
        it.prev = last
        last.next = it
        head.prev = it
      }
      it
    }
  }

  /** Note: under lock */
  override def removeIteration(it: Iteration): Unit = {
    val next = it.next
    val prev = it.prev
    it.next = null // Marked as deleted
    it.prev = null
    if (next eq it)
      head = null
    else {
      prev.next = next
      next.prev = prev
      if (head eq it)
        head = next
    }
  }

  private def reportProgress(it: Iteration): Unit = {
    marker match {
      case TrackProgress =>
        val tracker = it.progressTracker
        if (tracker ne null)
          tracker.progressComplete()
      case _ =>
        if (colReporter ne null)
          Progress.onProgressCompleted(it.progressReporter, marker, it.node)
    }
  }

  /* Note: under lock */
  private def processCompletedIteration(it: Iteration, eq: EvaluationQueue): Boolean = {
    if (it.next eq null) return true // Was previously processed

    removeIteration(it)
    val node = it.node
    var continue = true // start with assuming success
    var exception: Throwable = null
    combineInfo(node, eq) // consumeIteration will just get node.result so we have to combine 'info' before
    if (node.isDoneWithResult) {
      try {
        // Note: user supplies a function called here which could throw
        consumeIteration(it) // Note: calling callback under lock
        if (stopConsuming) continue = false
      } catch {
        case NonFatal(ex) => exception = ex
      }
    } else { // node.isDoneWithException
      exception = node.exception
    }
    if (exception ne null) {
      _xinfo = _xinfo.withException(exception)
      continue = false
    }
    continue
  }

  /** Returns true to continue. NOTE: under lock */
  override protected def processCompletedIterations(it: Iteration, eq: EvaluationQueue): Boolean = {
    reportProgress(it)

    if (it.node.isDoneWithException) {
      _result = WillCompleteWithException.asInstanceOf[R] // Stop creating more work. Even if can't process it now.
      if (RTExceptionsOnOutOfOrderCollections) consumeInOrder = true
    }

    var continue = if (consumeInOrder) true else processCompletedIteration(it, eq)
    // Results must be processed in order but can arrive out of order
    while ((head ne null) && head.node.isDone && continue) {
      continue = processCompletedIteration(head, eq)
    }

    // We "wait on" bootstrap (see [WAIT_ON_BOOTSTRAP]), so make bootstrap wait on the actual iteration node so that
    // we have an (indirect) "waiting on edge" to that too
    if (!continue) {
      head = null
      bootstrap.setWaitingOn(null)
    } else if (head ne null) bootstrap.setWaitingOn(head.node)

    continue
  }

  private var colReporter: CollectionProgressReporter = _
  def collection: AnyRef = null // Optional to implement, used only for progress reporting

  override protected def initializeLoopReporting(): Unit = {
    if (marker ne null) {
      marker match {
        case TrackProgress =>
          val ss = scenarioStack()

          if ((ss ne null) && (ss.progressTracker ne null)) {
            val progressType = if (length < 0) ProgressType.ExponentialDecay else ProgressType.Exact
            replace(ss.withProgressTracker(ProgressTrackerParams(weight = 1.0, progressType = progressType)))
          }
        case _ =>
          val pr = scenarioStack().progressReporter
          if (pr ne null)
            colReporter = pr.reportCollectionStarted(marker, collection)
      }
    }
  }

  private[this] var started = false
  private final def instrumentIteration(iteration: Iteration): Unit = {
    val node = iteration.node
    var ss = node.scenarioStack()
    val pr =
      if (colReporter eq null) UnspecifiedProgressReporter
      else {
        // TODO (OPTIMUS-40449): Remove this patch and rethink async progressReporter methods
        // This is a suboptimal patch because progressReporter.updateParent and onUpdateFromChild are now async because
        // some progressReporter implementations make DAL calls.
        // This means we replace the waitingOn edge between bootstrap and iteration with one to updateParent,
        // then we drop it once updateParent completes, and we lose the edge to the iteration
        // Remember: bootstrap manages waitingOn manually, see [WAIT_ON_BOOTSTRAP]
        val waitingOn = bootstrap.getWaitingOn
        // To debug similar issues:
        //
        // Uncomment this line here
        // bootstrap.setWaitingOn(null)
        //
        // In optimus.graph.NodeAwaiter.setWaitingOn, add the following code:
        // if (_waitingOn != null && node != null &&  (_waitingOn.getClass() != node.getClass()) &&
        //     (this instanceof NodeTask) && ((NodeTask) this).executionInfo().modifier().contains("bootstrap")) {
        //        System.err.println("stop"); <--- Break point here
        // }
        val pr = colReporter.reportIterationStarted(marker, iteration.item)
        bootstrap.setWaitingOn(waitingOn)
        pr
      }

    if (ss eq null)
      ss = this.scenarioStack().withProgressReporter(pr)
    else
      ss = ss.withProgressReporter(pr)

    marker match {
      case TrackProgress =>
        if ((ss ne null) && (scenarioStack.progressTracker ne null)) {
          if (length > 0)
            ss = ss.withProgressTracker(ProgressTrackerParams(weight = 1.0 / length))
          else
            ss = ss.withProgressTracker(ProgressTrackerParams(weight = 1.0))
        }
        iteration.progressTrackerOrReporter = ss.progressTracker
      case _ =>
        iteration.progressTrackerOrReporter = pr
    }

    node.replace(ss)

    if (!started) {
      OGTrace.observer.enqueueFollowsSequenceLogic(this, maxConcurrency)
      OGSchedulerLostConcurrency.reportFirstIteration(node, maxConcurrency)
      started = true
    }
  }

}

abstract class CommutativeAggregatorNode[B, R](marker: ProgressMarker, maxConcurrency: Int, length: Int)
    extends CollectionAggregatorNode[B, R](marker, maxConcurrency, length) {
  this.consumeInOrder = false // Relax the order
}

// length=-1 if unavailable
abstract class SequenceNode[B, ResultT](marker: ProgressMarker, maxConcurrency: Int, length: Int)
    extends CollectionAggregatorNode[B, ResultT](marker, maxConcurrency, length) {

  protected class IterationWithSource(node: Node[B], override val item: Any) extends Iteration(node)
}

abstract class ConcurrencyManager extends ConcurrentAggregatorNode[Unit, Unit] {
  override def initializeLoopReporting(): Unit = {}
  override protected def createIteration(): Iteration = {
    if (!hasNextIteration || isDone) null
    else {
      val iteration = nextIteration
      val node = iteration.node
      node.replace(scenarioStack())
      bootstrap.setWaitingOn(node)
      iteration
    }
  }
  override protected def removeIteration(it: Iteration): Unit = {}
  override protected def processCompletedIterations(iteration: Iteration, eq: EvaluationQueue): Boolean = {
    val node = iteration.node
    combineInfo(node, eq)
    consumeIteration(iteration)
    true
  }
}

/** Specializes SequenceOfNodes for a common case of iterator and a node generator */
abstract class SequenceNodeOnIterator[A, B, R](
    iterable: IterableLike[A, _],
    private[graph] val f: A => Node[B],
    override val sequenceExecutionInfo: SequenceNodeTaskInfo,
    workMarker: ProgressMarker,
    maxConcurrency: Int)
    extends SequenceNode[B, R](workMarker, maxConcurrency, iterable.knownSize) {

  override def collection: AnyRef = iterable.asInstanceOf[AnyRef] // For reporting only
  private[this] val iterator: Iterator[A] = iterable.iterator

  override def nextIteration: Iteration = {
    val item = iterator.next(); new IterationWithSource(f(item), item)
  }

  override def hasNextIteration: Boolean = iterator.hasNext

  override def subProfile(): AnyRef = f
}

/** Specializes SequenceNodeOnIterator to override only consume(resultOfIteration) */
abstract class ConverterSequenceNode[A, B, R](
    iterable: IterableLike[A, _],
    f: A => Node[B],
    sequenceExecutionInfo: SequenceNodeTaskInfo,
    workMarker: ProgressMarker,
    maxConcurrency: Int = Integer.MAX_VALUE)
    extends SequenceNodeOnIterator[A, B, R](iterable, f, sequenceExecutionInfo, workMarker, maxConcurrency) {

  def consume(elem: B): Unit
  final override def consumeIteration(i: Iteration): Unit = consume(i.node.result)
}

/** Specializes SequenceNodeOnIterator to override only consume(iterationValue, resultOfIteration) */
abstract class ConverterSequenceNode2[A, B, R](
    iterable: IterableLike[A, _],
    f: A => Node[B],
    sequenceExecutionInfo: SequenceNodeTaskInfo,
    workMarker: ProgressMarker = null,
    maxConcurrency: Int = Integer.MAX_VALUE)
    extends SequenceNodeOnIterator[A, B, R](iterable, f, sequenceExecutionInfo, workMarker, maxConcurrency) {

  def consume(elem: B, source: A): Unit
  override def consumeIteration(i: Iteration): Unit = {
    val xi = i.asInstanceOf[IterationWithSource]
    consume(xi.node.result.asInstanceOf[B], xi.item.asInstanceOf[A]) // We are only called with node holding onto result
  }
}

/**
 * Collects results of all nodes
 */
abstract class NodeSequenceNode[ResultT](
    iterable: IterableLike[Node[Any], _],
    override val sequenceExecutionInfo: SequenceNodeTaskInfo,
    workMarker: ProgressMarker,
    maxConcurrency: Int)
    extends SequenceNode[Any, ResultT](workMarker, maxConcurrency, iterable.size) {

  protected var collected = 0
  protected val array = new Array[Any](maxConcurrency)
  protected val iterator: Iterator[Node[Any]] = iterable.iterator

  override def consumeIteration(it: Iteration): Unit = {
    array(collected) = it.node.result; collected += 1
  }
  override def nextIteration = new Iteration(iterator.next())
  override def hasNextIteration: Boolean = iterator.hasNext
}

abstract class PartitioningSequenceNode[A, I, B, ResultT](
    iterable: IterableLike[A, _],
    elementSourceToPartition: (() => Option[A]) => I,
    private[graph] val f: I => Node[B],
    override val sequenceExecutionInfo: SequenceNodeTaskInfo,
    workMarker: ProgressMarker,
    maxConcurrency: Int = Integer.MAX_VALUE)
    extends SequenceNode[B, ResultT](workMarker, maxConcurrency, iterable.knownSize) {

  private val iterator: Iterator[A] = iterable.iterator
  override def collection: AnyRef = iterable.asInstanceOf[AnyRef] // For reporting only

  def consume(elem: B): Unit
  override def consumeIteration(i: Iteration): Unit = consume(i.node.result)

  private def getElement: Option[A] = iterator.synchronized(if (iterator.hasNext) Some(iterator.next()) else None)

  override def nextIteration: Iteration = {
    val nitem: I = elementSourceToPartition(() => getElement)
    new Iteration(f(nitem))
  }
  override def hasNextIteration: Boolean = iterator.hasNext

  override def subProfile(): AnyRef = f
}
