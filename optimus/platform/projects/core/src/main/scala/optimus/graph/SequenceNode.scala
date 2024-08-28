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

import java.util

import optimus.platform._
import optimus.scalacompat.collection.IterableLike
import optimus.platform.UnspecifiedProgressReporter

/**
 * Notes:
 *   1. This node schedules itself multiple times up to maxConcurrency
 *
 *   2. Handles the case where the nodes are not generated on the fly and can be in different stages of processing
 *
 * Assumptions:
 *   1. All callbacks do NOT throw
 *
 *   2. All callbacks do NOT take locks
 *
 *   3. All callbacks are NOT thread safe and need to be protected under MT
 *
 *   4. All callbacks are very fast (i.e. OK to execute under lock)
 *
 * Consider:
 *   1. Limiting concurrency by the number of outstanding results. Currently if the first node is the slowest and we
 *      require sequence order to be preserved we'll end up accumulating all the results before reducing
 *
 * Improvements:
 *   1. Optimize for sequence of size 1?
 *
 *   2. Optimize for concurrency = 1
 *
 *   3. Optimize for Specific collection specifically indexed collections
 *
 *   4. This class assumes that the order must be preserved, it's possible to optimize in the relaxed case
 *
 *   5. In order to avoid so much locking consider getting a few nodes at a time
 *
 *   6. Recycle Iterations
 */
object ConcurrentAggregatorNode {

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
    override def cacheUnderlyingNode(): NodeTask = {
      val o = owner
      if (o ne null) o else new NullOwnerNode(executionInfo)
    }

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

    // When iteration completes, owner (a heavy sequence node) is wiped out from Bootstrap, which means that
    // cacheUnderlyingNode has nothing to refer to. Other code assumes that executionInfo is never null, so we
    // put this null-like node in its place, with the previously snapshotted executionInfo (note, it's a val here)
    final class NullOwnerNode(underlyingInfo: NodeTaskInfo) extends ConcurrentAggregatorNode[Unit, Unit] {
      // make sure executionInfo isn't null here (all other overrides are stubs)
      override def executionInfo: NodeTaskInfo = underlyingInfo

      private def invalid: Throwable = new GraphInInvalidState("Should never run this!")
      override def maxConcurrency: Int = throw invalid
      override def hasNextIteration: Boolean = throw invalid
      override def nextIteration: Iteration = throw invalid
      override def consumeIteration(i: Iteration): Unit = throw invalid
      override def getFinalResult: Unit = throw invalid
      override protected def initializeLoopReporting(): Unit = throw invalid
      override protected def createIteration(): Iteration = throw invalid
      override protected def processCompletedIterations(iteration: Iteration, eq: EvaluationQueue): Throwable =
        throw invalid
      override protected def sequenceExecutionInfo: SequenceNodeTaskInfo = throw invalid
    }
  }
}

abstract class ConcurrentAggregatorNode[B, ResultT] extends CompletableNode[ResultT] {
  import ConcurrentAggregatorNode._

  def maxConcurrency: Int

  def getConcurrency: Int = concurrency
  protected[this] var concurrency: Int = -1 // Current concurrency (see comments in run)

  def length: Int = -1

  def hasNextIteration: Boolean
  def nextIteration: Iteration
  def consumeIteration(i: Iteration): Unit
  def getFinalResult: ResultT

  protected def initializeLoopReporting(): Unit

  protected def createIteration(): Iteration

  protected def processCompletedIterations(iteration: Iteration, eq: EvaluationQueue): Throwable

  override def executionInfo: NodeTaskInfo = sequenceExecutionInfo
  protected def sequenceExecutionInfo: SequenceNodeTaskInfo

  protected final def reportIterationComplete(iteration: Iteration): Unit = {
    val tracker = iteration.node.scenarioStack.progressTracker

    if (tracker ne null)
      tracker.progressComplete()
  }

  protected final def getAndRunNextIteration(completedIteration: Iteration, eq: EvaluationQueue): Unit = {
    var prev = completedIteration
    var continue = true

    do {
      var next: Iteration = null
      var aftercc = -1
      var hadExceptionAlready = false
      var exception: Throwable = null

      // SequenceNode.this is internal class. it's OK to sync on 'this'
      this.synchronized {
        hadExceptionAlready = this.exception ne null
        exception = processCompletedIterations(prev, eq)
        if (exception eq null) {
          next = createIteration()
          if (next eq null) {
            concurrency -= 1
            aftercc = concurrency
          }
        }
      }

      if (exception ne null) {
        if (!hadExceptionAlready) {
          completeWithException(exception, eq)
        }
      } else if (aftercc == 0) try {
        val fr = getFinalResult
        bootstrap.detachAndCleanup(eq)
        completeWithResult(fr, eq) // We are the last on the way out
      } catch {
        case e: Exception =>
          completeWithException(e, eq)
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
    // Instead of calling node.continueWith(this, eq)
    // We expand that call here. To avoid stack overflow if the nodes
    // are already completed (and would call back right away)
    if (node.tryAddToWaiterList(it)) {
      eq.enqueue(this, node)
      false
    } else {
      true
    }
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

    concurrency = 0
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
    var aftercc = 0
    var next: Iteration = null

    this.synchronized {
      if (concurrency < maxConcurrency) {
        next = createIteration()
        if (next ne null)
          concurrency += 1
        aftercc = concurrency
      }
    }

    if (next ne null) {
      // If we are to re-enqueue ourself we have to do it before we enqueue the next iteration, because the "queue" is
      // actually a stack, and we want the next iteration to be run before we are run
      if (aftercc < maxConcurrency) {
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
    override def awaiter: ConcurrentAggregatorNode[B, ResultT] = ConcurrentAggregatorNode.this
    var next: Iteration = _ // used to maintain sl-list
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

  protected[this] var colReporter: CollectionProgressReporter = _
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

  private var started = false
  protected final def instrumentIteration(iteration: Iteration): Unit = {
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
      case _ =>
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

  // Track launched iteration nodes that we haven't processed yet
  private val launched = new util.IdentityHashMap[NodeTask, AnyRef]()

  /** Returns null when done (called under lock) */
  override protected def createIteration(): Iteration = {

    if (!hasNextIteration || isDone) null
    else {
      val iteration = nextIteration
      val node = iteration.node
      instrumentIteration(iteration)
      launched.put(node, this)
      // The bootstrap needs to wait on one iteration - it doesn't matter which, but we might as well keep waiting on the
      // same one until it completes.
      if (bootstrap.getWaitingOn eq null)
        bootstrap.setWaitingOn(node) // see [WAIT_ON_BOOTSTRAP]
      iteration
    }
  }

  /** Returns null on success or exception otherwise */
  override protected def processCompletedIterations(iteration: Iteration, eq: EvaluationQueue): Throwable = {

    marker match {
      case TrackProgress => reportIterationComplete(iteration)
      case _             =>
    }

    val node = iteration.node
    var exception: Throwable = this.exception
    combineInfo(node, eq)
    if (node.isDoneWithResult) {
      try { consumeIteration(iteration) }
      catch { case ex: Throwable => exception = ex }
    } else {
      exception = node.exception()
    }
    launched.remove(node)
    if (exception ne null) {
      _xinfo = _xinfo.withException(exception)
      bootstrap.setWaitingOn(null)
    } else if (bootstrap.getWaitingOn eq node) {
      if (launched.isEmpty)
        bootstrap.setWaitingOn(null)
      else {
        // The bootstrap needs to wait on one iteration - it doesn't matter which.
        // If there's an easier to find a random key in an IdentityHashMap, let me know...
        val someNode = launched.keySet().iterator().next
        bootstrap.setWaitingOn(someNode)
      }
    }
    exception
  }
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

  override protected def processCompletedIterations(iteration: Iteration, eq: EvaluationQueue): Throwable = {
    val node = iteration.node
    combineInfo(node, eq)
    consumeIteration(iteration)
    null
  }

}

// length=-1 if unavailable
abstract class SequenceNode[B, ResultT](marker: ProgressMarker, maxConcurrency: Int, length: Int)
    extends CollectionAggregatorNode[B, ResultT](marker, maxConcurrency, length) {
  sequenceNode =>

  private[this] var first: Iteration = _
  private[this] var last: Iteration = _

  /** Returns null when done (called under lock) */
  override protected def createIteration(): Iteration = {
    if (!hasNextIteration || isDone) null
    else {
      val iteration = nextIteration
      instrumentIteration(iteration)

      // Adjust the list
      if (last ne null) last.next = iteration
      last = iteration
      if (first eq null) {
        first = last
        bootstrap.setWaitingOn(first.node) // see [WAIT_ON_BOOTSTRAP]
      }
      iteration
    }
  }

  /** Returns null on success or exception otherwise */
  override protected def processCompletedIterations(iteration: Iteration, eq: EvaluationQueue): Throwable = {
    var exception: Throwable = this.exception // There could have been exception set from before

    marker match {
      case TrackProgress => reportIterationComplete(iteration)
      case _             =>
    }

    if (colReporter ne null)
      Progress.onProgressCompleted(marker, iteration.node)

    // Results must be processed in order but can arrive out of order
    // first/last/next form a buffer of results.
    // If the first node is completed we can process all the 'done' nodes
    if (first eq iteration) {
      // Consume all the results that are ready
      var cit = iteration
      while ((cit ne null) && cit.node.isDone) {
        val node = cit.node
        combineInfo(node, eq) // consumeIteration will just get node.result so we have to combine 'info' before
        if (node.isDoneWithResult) {
          try consumeIteration(cit) // Note: calling callback under lock
          catch {
            case ex: Throwable => exception = ex
          } // e.g. minBy; the user supplies a function called here which could throw
        } else { // node.isDoneWithException
          exception = node.exception
        }
        if (exception ne null) { // bail
          cit.next = null
          _xinfo = _xinfo.withException(exception) // "Pre-set" exception field
        }
        cit = cit.next
      }
      first = cit
      // We "wait on" bootstrap (see [WAIT_ON_BOOTSTRAP]), so make bootstrap wait on the actual iteration node so that
      // we have an (indirect) "waiting on edge" to that too
      if (cit eq null) {
        last = null
        bootstrap.setWaitingOn(null)
      } else bootstrap.setWaitingOn(cit.node)
    }
    exception
  }

  protected class IterationWithSource(node: Node[B], override val item: Any) extends Iteration(node)
}

/**
 * Specializes SequenceOfNodes for a common case of iterator and a node generator
 */
abstract class ConverterSequenceNode[A, B, ResultT](
    iterable: IterableLike[A, _],
    private[graph] val f: A => Node[B],
    override val sequenceExecutionInfo: SequenceNodeTaskInfo,
    workMarker: ProgressMarker,
    maxConcurrency: Int = Integer.MAX_VALUE)
    extends SequenceNode[B, ResultT](workMarker, maxConcurrency, if (iterable.hasDefiniteSize) iterable.size else -1) {

  val n: Int = if (iterable.hasDefiniteSize) iterable.size else -1

  val iterator: Iterator[A] = iterable.iterator
  override def collection: AnyRef = iterable.asInstanceOf[AnyRef] // For reporting only

  def consume(elem: B): Unit

  override def consumeIteration(i: Iteration): Unit =
    consume(i.node.result) // We are only called with node holding onto result

  override def nextIteration: IterationWithSource = {
    val nitem = iterator.next(); new IterationWithSource(f(nitem), nitem)
  }

  override def hasNextIteration: Boolean = iterator.hasNext

  override def subProfile(): AnyRef = f
}

/**
 * Specializes SequenceOfNodes for a common case of iterator and a node generator Also retains original element of the
 * collection
 */
abstract class ConverterSequenceNode2[A, B, ResultT](
    iterable: IterableLike[A, _],
    private[graph] val f: A => Node[B],
    override val sequenceExecutionInfo: SequenceNodeTaskInfo,
    workMarker: ProgressMarker,
    maxConcurrency: Int = Integer.MAX_VALUE)
    extends SequenceNode[B, ResultT](workMarker, maxConcurrency, if (iterable.hasDefiniteSize) iterable.size else -1) {

  val iterator: Iterator[A] = iterable.iterator

  /** Represents a single iteration of processing of a given sequence */
  class XIteration(node: Node[B], val source: A) extends Iteration(node)

  def consume(elem: B, source: A): Unit

  override def consumeIteration(i: Iteration): Unit = {
    val xi = i.asInstanceOf[XIteration]
    consume(xi.node.result, xi.source) // We are only called with node holding onto result
  }

  override def nextIteration: XIteration = {
    val v = iterator.next()
    new XIteration(f(v), v)
  }

  override def hasNextIteration: Boolean = iterator.hasNext

  override def subProfile(): AnyRef = f
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
    extends SequenceNode[B, ResultT](workMarker, maxConcurrency, if (iterable.hasDefiniteSize) iterable.size else -1) {

  private val iterator: Iterator[A] = iterable.iterator
  override def collection: AnyRef = iterable.asInstanceOf[AnyRef] // For reporting only

  def consume(elem: B): Unit

  override def consumeIteration(i: Iteration): Unit =
    consume(i.node.result) // We are only called with node holding onto result

  private def getElement: Option[A] = iterator.synchronized(if (iterator.hasNext) Some(iterator.next()) else None)

  override def nextIteration: Iteration = {
    val nitem: I = elementSourceToPartition(() => getElement)
    new Iteration(f(nitem))
  }
  override def hasNextIteration: Boolean = iterator.hasNext

  override def subProfile(): AnyRef = f
}
