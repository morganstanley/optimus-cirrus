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
package optimus.platform.throttle

import msjava.slf4jutils.scalalog.getLogger
import optimus.core.CoreAPI
import optimus.core.MonitoringBreadcrumbs
import optimus.core.TPDMask
import optimus.graph.CompletableNode
import optimus.graph._
import optimus.platform.PluginHelpers.toNodeF
import optimus.platform._
import optimus.platform.annotations.alwaysAutoAsyncArgs
import optimus.platform.annotations.nodeSync
import optimus.platform.throttle.Throttle._

import java.util.PriorityQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.{LinkedList => JLinkedList}
import java.util.{Queue => JQueue}
import scala.collection.mutable

/**
 * Unique identifier for a throttle. Used to ensure that plugin tags don't hold onto the full Throttle object.
 */
final class ThrottleId {}

private case object ThrottleKey extends JobNonForwardingPluginTagKey[List[ThrottleId]]
final class RecursiveThrottleException(msg: String, current: NodeTask) extends Exception {
  override def getMessage: String = msg + "\nNode stack:\n" + current.simpleChain()
}

/**
 * Each time Throttle.apply is called, it queues up a new ThottledNode, allowing at most limit weights of ThrottledNode
 * (by default weight is 1 so weights equals number of instances) to be scheduled at the same time.
 */
class Throttle(limiter: ThrottleLimiter, enablePriority: Boolean = false) {
  def failOnRecursiveThrottle: Boolean = Settings.failOnRecursiveThrottle
  final val id = new ThrottleId

  @alwaysAutoAsyncArgs
  def apply[R](f: => R): R = apply$queued(toNodeF(f _)).get$
  @nodeSync
  def apply[R](nf: NodeFunction0[R]): R = apply$queued(nf).get$
  def apply$queued[R](nf: NodeFunction0[R]): NodeFuture[R] = apply$queued[R](nf, 1, Int.MaxValue)

  @alwaysAutoAsyncArgs
  def apply[R](f: => R, nodeWeight: Int): R = apply$queued(toNodeF(f _), nodeWeight).get$
  @nodeSync
  def apply[R](nf: NodeFunction0[R], nodeWeight: Int): R = apply$queued(nf, nodeWeight).get$
  def apply$queued[R](nf: NodeFunction0[R], nodeWeight: Int): NodeFuture[R] =
    apply$queued[R](nf, nodeWeight, Int.MaxValue)

  /**
   * nodeWeight is used to determine the weight used calculate inflight total weight. nodes will normally only be
   * scheduled when total weight is less than limit, but we do allow the limit to be exceeded if only one node is
   * running (to avoid deadlock if a node with weight > limit is submitted)
   *
   * nodePriority is used to determine which node runs first if enablePriority is true.
   */
  @alwaysAutoAsyncArgs
  def apply[R](f: => R, nodeWeight: Int, nodePriority: Int): R =
    apply$queued(toNodeF(f _), nodeWeight, nodePriority).get$
  @nodeSync
  def apply[R](nf: NodeFunction0[R], nodeWeight: Int = 1, nodePriority: Int = Int.MaxValue): R =
    apply$queued(nf, nodeWeight, nodePriority).get$

  def apply$queued[R](nf: NodeFunction0[R], nodeWeight: Int, nodePriority: Int): NodeFuture[R] = {
    limiter.checkWeight(nodeWeight)
    val ec = EvaluationContext.current
    val currentStack = ec.scenarioStack()

    val currentThrottles = currentStack.findPluginTag(ThrottleKey).getOrElse(Nil)
    if (currentThrottles.contains(this.id)) {
      limiter.synchronized {
        limiter.notifyTestWaiters()
        nSkipped += 1
      }
      // This only catches the egregious case where of directly re-entering a throttle. It doesn't catch the more
      // pernicious lock ordering inversion case where we throttle in two different orders in two branches of the
      // code. For those, we rely on the cycle breaking code in WaitingOnThrottle.
      val exc =
        new RecursiveThrottleException(s"recursive throttle for $this is not supported", ec.getCurrentNodeTask)
      if (failOnRecursiveThrottle) throw exc
      else {
        // The default is to just ignore throttling altogether when we recursively re-enter a previous throttle, so as
        // to avoid deadlocks.
        log.warn("Throttle ignored", exc)
        MonitoringBreadcrumbs.sendThrottleIgnoredCrumb(ec.getCurrentNodeTask)
        nf.apply$queued().asNode$
      }
    } else {
      val newStack = currentStack.withPluginTag(ThrottleKey, this.id :: currentThrottles)

      val throttledNode = new ThrottledNode(nf, newStack, weight = nodeWeight, throttlePriority = nodePriority)
      tryRunNext(ec, Enqueued(throttledNode))
      throttledNode
    }
  }

  /**
   * The calling code will wait on this node which initially does nothing. Once the throttle is ready to run us, it
   * calls onChildCompleted(WaitingOnThrottle) which causes this node to get enqueued, and run, and this node then runs
   * and completes from the NodeFunction node.
   *
   * Note that these nodes are stored in a Set in WaitingOnThrottle so need to have default (identity) hashcode/equals
   */
  private[throttle] class ThrottledNode[R](
      nf: NodeFunction0[R],
      ss: ScenarioStack,
      val weight: Int,
      val throttlePriority: Int)
      extends CompletableNode[R]
      with Comparable[ThrottledNode[R]] {

    var tStarted: Long = 0 // modified under limiter.synchronized
    var debugIdx: Int = 0

    // pretend to be running so that we don't get run until onChildCompleted from WaitingOnThrottle
    initAsRunning(ScenarioStack.constantNC)
    def enqueueAndStart(ec: EvaluationQueue): Unit = onChildCompleted(ec, null)

    // We need to run our child node inside the scenario stack that was passed in. This is frustrating because $queued
    // takes the current EC's scenario stack, so one way around that is to run our node inside another custom node.
    private def createChildNode(): Node[R] = new CompletableNode[R] {
      attach(ss)
      private var child: Node[R] = _
      override def run(ec: OGSchedulerContext): Unit = {
        if (child eq null) {
          child = nf.apply$queued().asNode$
          child.continueWith(this, ec)
        } else {
          completeFromNode(child, ec)
        }
      }
    }

    private var state = 0
    private var child: Node[R] = _

    private def cleanUp(ec: EvaluationQueue): Unit = {
      state = 2
      // otherwise it was the NodeFunction which completed, so we can complete this node and potentially schedule more
      WaitingOnThrottle.removeInflightNode(this)
      tryRunNext(ec, Completed(this))
    }

    override def run(ec: OGSchedulerContext): Unit = state match {
      case 0 =>
        state = 1
        child = createChildNode()
        child.enqueueAttached
        child.continueWith(this, ec)

      case 1 =>
        cleanUp(ec)
        completeFromNode(child, ec)
    }

    override def completeWithException(ex: Throwable, ec: EvaluationQueue): Unit = {
      if (state == 1)
        cleanUp(ec)
      super.completeWithException(ex, ec)
    }

    override def compareTo(o: ThrottledNode[R]): Int = this.throttlePriority.compareTo(o.throttlePriority)
  }

  // For testing, block until the internal state passes a predicate
  private[throttle] def waitForState(p: ThrottleState => Boolean): Unit = limiter.synchronized {
    val start = System.currentTimeMillis()
    limiter.hasTestWaiter = true
    while (true) {
      val current = System.currentTimeMillis()
      if ((current - start) > waitForStateTimeout) throw new TimeoutException("wait for state timed out")
      if (p(getCounters)) return
      else limiter.wait(waitForStateUpdates)
    }
  }

  def getCounters: ThrottleState = limiter.synchronized {
    ThrottleState(
      limiter.getInflightWeightStatic,
      nInFlight = nInFlight,
      nThrottledNode = nThrottledNode,
      nQueued = queue.size,
      nSkipped = nSkipped)
  }

  // These are modified under limiter.synchronized
  private val queue: JQueue[ThrottledNode[_]] =
    if (enablePriority) new PriorityQueue[ThrottledNode[_]]() else new JLinkedList[ThrottledNode[_]]()
  private var pollerScheduled = false
  private var nInFlight = 0
  private var nThrottledNode = 0
  private var nSkipped = 0

  private sealed trait TryRunNext
  private case class Enqueued(node: ThrottledNode[_]) extends TryRunNext
  private case class Completed(node: ThrottledNode[_]) extends TryRunNext
  private case object Polled extends TryRunNext
  private case class BreakCycles(exc: RecursiveThrottleException) extends TryRunNext

  /**
   * atomically updates all of the state based on (possible) node completion and live weight, starts running another
   * node if there is capacity (in which case state is updated again to reflect this), and schedules polling if needed
   *
   * Takes one of TryRunNext depending on what has caused us to try to run things: a completed node, an enqueued
   * node, a scheduled check of throttling status or cycle breaking.
   */
  private def tryRunNext(eq: EvaluationQueue, why: TryRunNext): Unit = {
    val toRun = limiter.synchronized {
      // these all need to be modified under the lock, atomically with respect to checkRunNowAndUpdateWeights, which is
      // why we do it all here
      val forced = why match {
        case Enqueued(node) =>
          queue.add(node)
          node.awaiter.setWaitingOn(WaitingOnThrottle)
          false
        case Polled =>
          pollerScheduled = false
          false
        case Completed(node) =>
          nInFlight -= 1
          limiter.completed(node, nInFlight)
          false
        case BreakCycles(_) =>
          // If we detected a cycle, we try to avoid a hang by "forcing" the throttle open, i.e. running all the
          // queued nodes. This happens in lock ordering inversion and sometimes while batching.
          true
      }

      // we always update live state even if nothing was enqueued or completed, because this might result in weight
      // reducing enough that an already-queued node canRunNow (below)
      limiter.updateLiveState()

      // take as many nodes from the queue as the limiter allows (note that if a heavy node has completed or the
      // live weight has reduced, we may be able to run more than one light node)
      val toRun = List.newBuilder[ThrottledNode[_]]
      while ({
        val next = queue.peek()
        (next ne null) && {
          // In case of "forced", we don't care about the return value of limiter.canRunNow but we still need to call it
          // to make sure that the limiter internal state is updated
          val canRun = limiter.canRunNow(next, nInFlight, forced)
          canRun || forced
        }
      }) {
        val n = queue.remove()
        nThrottledNode += 1
        nInFlight += 1
        if (forced) nSkipped += 1
        n.debugIdx = nThrottledNode
        n.tStarted = OGTrace.nanoTime()
        log.debug(s"Will launch $nThrottledNode")
        toRun += n
      }

      // regardless of whether we are going to run a node now, if there are nodes still on the queue, schedule a poll
      // so that we can try to run those nodes up later
      if (limiter.pollIntervalMs > 0 && !queue.isEmpty && !pollerScheduled) {
        schedulePoll(eq.scheduler)
        pollerScheduled = true
      }
      toRun.result()
    }

    // We fail those nodes that happens to be queued at the time the cycle is broken, which is a somewhat random set
    // of nodes. That's fine because the recursive throttle exception isn't an RT exception.
    //
    // It would be nice to operate here in a reproducible way, however we can't necessarily predict statically that a
    // throttle deadlock will occur or where it will occur. For example, this code:
    //
    //   apar( throttle1 { throttle2 { ... } }, throttle2 { throttle1 { ... } })
    //
    // might cause a deadlock, but whether that deadlock happens because of the first branch or the second branch is
    // unknowable. To make this RT, we would have to impose a specific ordering on all possible throttles, and then
    // fail whenever that ordering is broken.
    val failWith = Option(why).filter(_ => failOnRecursiveThrottle).collect { case BreakCycles(exc) => exc }

    toRun.foreach { n =>
      // it's important that we add the node to inflight *before* we notify it to run, because addInflightNode mutates
      // node.waitingOn
      WaitingOnThrottle.addInflightNode(n)
      failWith match {
        case Some(exc) =>
          n.completeWithException(exc, eq)
        case None =>
          // pretend that the WaitingOnThrottle has completed - this will make the node run
          n.enqueueAndStart(eq)
      }
    }
  }

  // if we have a live weigher, the weight may reduce even without nodes completing, so we need to poll
  private def schedulePoll(scheduler: Scheduler): Unit = {
    CoreAPI.optimusScheduledThreadPool.schedule(
      // tryRunNext will schedule another poll if needed
      { () => tryRunNext(scheduler, Polled) }: Runnable,
      limiter.pollIntervalMs,
      TimeUnit.MILLISECONDS
    )
  }

  // All nodes queued inside the throttle wait on this, and this waits on an incomplete (running) node that is no longer
  // waiting in the throttle. That way we always have a waitingOn edge from the stuff stuck in the throttle to the
  // stuff that already made it through the throttle. The former needs the latter to finish so that it can get through the
  // throttle queue.
  private object WaitingOnThrottle extends Node[Nothing] {
    private val inflightNodes = mutable.Set.empty[ThrottledNode[_]]

    initAsCompleted(ScenarioStack.constantNC)

    override def tryToRecoverFromCycle(unused: TPDMask, eq: EvaluationQueue): Boolean = synchronized {
      val exc = new RecursiveThrottleException("Throttle involved in a cycle", this)
      log.warn(
        "This throttle was involved in cycle, either because a true cycle exists, or because of a potential lock inversion deadlock." +
          " All queued nodes are released.",
        exc
      )
      MonitoringBreadcrumbs.sendThrottleCycleBreakingCrumb(this)
      tryRunNext(eq, BreakCycles(exc))

      // must return true because this fake node should never be completed by anybody
      true
    }

    override def executionInfo(): NodeTaskInfo = NodeTaskInfo.Throttled

    def addInflightNode(node: ThrottledNode[_]): Unit =
      synchronized {
        // The now in-flight node might have previously been waiting on this throttle. If that's the case, we need to
        // remove it's waitingOn to avoid creating a cycle.
        //
        // TODO (OPTIMUS-73152): Is that not potentially race-y if the waiting on chain is obtained from another thread?
        if (node.getWaitingOn eq WaitingOnThrottle) node.setWaitingOn(null)
        WaitingOnThrottle.setWaitingOn(node)
        val added = inflightNodes.add(node)
        if (!added) throw new GraphInInvalidState(s"$node was already in flight")
      }

    def removeInflightNode(previous: ThrottledNode[_]): Unit =
      synchronized {
        val removed = inflightNodes.remove(previous)
        if (!removed)
          throw new GraphInInvalidState(s"$previous wasn't in flight")

        // ensure that we are always waiting on an (arbitrary) incomplete node
        if (WaitingOnThrottle.getWaitingOn eq previous)
          inflightNodes.headOption match {
            case Some(anyInflight) => WaitingOnThrottle.setWaitingOn(anyInflight)
            case None              => WaitingOnThrottle.setWaitingOn(null)
          }
      }
  }
}

object Throttle {
  private[throttle] val log = getLogger[Throttle]
  private val waitForStateUpdates = 1000 // in ms
  private val waitForStateTimeout: Int = 60 * waitForStateUpdates
}

/**
 * Snapshot of the throttle state
 * @param inflightWeight the current "weight" of all running nodes
 * @param nInFlight number of currently in-flight nodes
 * @param nThrottledNode number of calls ever ran from the throttle
 * @param nQueued currently queued node waiting to run
 * @param nSkipped number of nodes that were artificially un-throttled to avoid deadlocks and cycles
 */
private[optimus] final case class ThrottleState(
    inflightWeight: Double,
    nInFlight: Int,
    nThrottledNode: Int,
    nQueued: Int,
    nSkipped: Int)
