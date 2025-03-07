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
import optimus.core.needsPlugin
import optimus.graph.CompletableNode
import optimus.graph._
import optimus.platform._
import optimus.platform.annotations.alwaysAutoAsyncArgs
import optimus.platform.annotations.nodeSync
import optimus.platform.throttle.Throttle._

import java.util.PriorityQueue
import java.util.concurrent.TimeUnit
import java.util.{LinkedList => JLinkedList}
import java.util.{Queue => JQueue}
import scala.collection.mutable

/**
 * Each time Throttle.apply is called, it queues up a new ThottledNode, allowing at most limit weights of ThrottledNode
 * (by default weight is 1 so weights equals number of instances) to be scheduled at the same time.
 */
class Throttle(limiter: ThrottleLimiter, enablePriority: Boolean = false) {

  @alwaysAutoAsyncArgs
  def apply[R](f: => R): R = needsPlugin
  @nodeSync
  def apply[R](nf: NodeFunction0[R]): R = apply$queued(nf).get
  def apply$queued[R](nf: NodeFunction0[R]): Node[R] = apply$queued[R](nf, 1, Int.MaxValue)

  @alwaysAutoAsyncArgs
  def apply[R](f: => R, nodeWeight: Int): R = needsPlugin
  @nodeSync
  def apply[R](nf: NodeFunction0[R], nodeWeight: Int): R = apply$queued(nf, nodeWeight).get
  def apply$queued[R](nf: NodeFunction0[R], nodeWeight: Int): Node[R] = apply$queued[R](nf, nodeWeight, Int.MaxValue)

  /**
   * nodeWeight is used to determine the weight used calculate inflight total weight. nodes will normally only be
   * scheduled when total weight is less than limit, but we do allow the limit to be exceeded if only one node is
   * running (to avoid deadlock if a node with weight > limit is submitted)
   *
   * nodePriority is used to determine which node runs first if enablePriority is true.
   */
  @alwaysAutoAsyncArgs
  def apply[R](f: => R, nodeWeight: Int, nodePriority: Int): R = needsPlugin
  @nodeSync
  def apply[R](nf: NodeFunction0[R], nodeWeight: Int = 1, nodePriority: Int = Int.MaxValue): R =
    apply$queued(nf, nodeWeight, nodePriority).get
  def apply$queued[R](nf: NodeFunction0[R], nodeWeight: Int, nodePriority: Int): Node[R] = {
    limiter.checkWeight(nodeWeight)
    val ec = EvaluationContext.current
    val throttledNode = new ThrottledNode(nf, ec.scenarioStack, weight = nodeWeight, throttlePriority = nodePriority)
    tryRunNext(ec, toEnqueue = throttledNode)
    throttledNode
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

    // mark as running initially so that nobody else can run us until the throttle notifies us.
    // Note that ss will be used as the scenario stack of the NodeFunction node.
    initAsRunning(ss)

    override def run(ec: OGSchedulerContext): Unit =
      nf.apply$queued().continueWith(this, ec)

    override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
      // when the throttle notifies us, we should run(); super.onChildComplete arranges this to happen
      if (child eq WaitingOnThrottle) super.onChildCompleted(eq, child)
      else {
        // otherwise it was the NodeFunction which completed, so we can complete this node and potentially schedule more
        WaitingOnThrottle.removeInflightNode(this)
        tryRunNext(eq, completed = this)
        completeFromNode(child.asInstanceOf[Node[R]], eq)
      }
    }

    override def compareTo(o: ThrottledNode[R]): Int = this.throttlePriority.compareTo(o.throttlePriority)
  }

  def getCounters: ThrottleState = limiter.synchronized {
    ThrottleState(
      limiter.getInflightWeightStatic,
      nInFlight = nInFlight,
      nThrottledNode = nThrottledNode,
      nQueued = queue.size)
  }

  // These are modified under limiter.synchronized
  private val queue: JQueue[ThrottledNode[_]] =
    if (enablePriority) new PriorityQueue[ThrottledNode[_]]() else new JLinkedList[ThrottledNode[_]]()
  private var pollerScheduled = false
  private var nInFlight = 0
  private var nThrottledNode = 0 // for diagnostics only

  /**
   * atomically: updates all of the state based on (possible) node completion and live weight, starts running another
   * node if there is capacity (in which case state is updated again to reflect this), and schedules polling if needed
   *
   * @param toEnqueue a newly created ThrottleNode to add to the queue (or run immediately if possible), or null
   * @param completed a node that just completed, or null
   * @param pollerCalling true if we're being called by the poller, rather than due to node creation/completion
   */
  private def tryRunNext(
      eq: EvaluationQueue,
      toEnqueue: ThrottledNode[_] = null,
      completed: ThrottledNode[_] = null,
      pollerCalling: Boolean = false): Unit = {
    var toRun: List[ThrottledNode[_]] = Nil
    limiter.synchronized {
      // these all need to be modified under the lock, atomically with respect to checkRunNowAndUpdateWeights, which is
      // why we do it all here
      if (toEnqueue ne null) {
        queue.add(toEnqueue)
        toEnqueue.awaiter.setWaitingOn(WaitingOnThrottle)
      }

      // we always update live state even if nothing was enqueued or completed, because this might result in weight
      // reducing enough that an already-queued node canRunNow (below)
      limiter.updateLiveState()
      if (pollerCalling) pollerScheduled = false

      if (completed ne null) {
        nInFlight -= 1
        limiter.completed(completed, nInFlight)
      }

      // take as many nodes from the queue as the limiter allows (note that if a heavy node has completed or the
      // live weight has reduced, we may be able to run more than one light node)
      while ({ val next = queue.peek(); (next ne null) && limiter.canRunNow(possibleNext = next, nInFlight) }) {
        val n = queue.remove()
        nThrottledNode += 1
        nInFlight += 1
        n.debugIdx = nThrottledNode
        n.tStarted = OGTrace.nanoTime()
        log.debug(s"Will launch $nThrottledNode")
        toRun ::= n
      }

      // regardless of whether we are going to run a node now, if there are nodes still on the queue, schedule a poll
      // so that we can try to run those nodes up later
      if (limiter.pollIntervalMs > 0 && !queue.isEmpty && !pollerScheduled) {
        schedulePoll(eq.scheduler)
        pollerScheduled = true
      }
    }

    toRun.foreach { n =>
      // it's important that we add the node to inflight *before* we notify it to run, because addInflightNode mutates
      // node.waitingOn
      WaitingOnThrottle.addInflightNode(n.awaiter)
      // pretend that the WaitingOnThrottle has completed - this will make the node run
      n.onChildCompleted(eq, WaitingOnThrottle)
    }
  }

  // if we have a live weigher, the weight may reduce even without nodes completing, so we need to poll
  private def schedulePoll(scheduler: Scheduler): Unit = {
    CoreAPI.optimusScheduledThreadPool.schedule(
      // tryRunNext will schedule another poll if needed
      { () => tryRunNext(scheduler, pollerCalling = true) }: Runnable,
      limiter.pollIntervalMs,
      TimeUnit.MILLISECONDS
    )
  }

  // All nodes queued inside the throttle wait on this, and this waits on an incomplete (running) node that is no longer
  // waiting in the throttle. That way we always have a waitingOn edge from the stuff stuck in the throttle to the
  // stuff that already made it through the throttle. The former needs the latter to finish so that it can get through the
  // throttle queue.
  private object WaitingOnThrottle extends Node[Nothing] {
    // modified under own lock
    private val inflightNodes = mutable.Set[NodeTask]()

    initAsRunning(ScenarioStack.constantNC)
    override def executionInfo(): NodeTaskInfo = NodeTaskInfo.Throttled

    def addInflightNode(node: NodeTask): Unit =
      inflightNodes.synchronized {
        // WaitingOnThrottle needs to be waiting on an arbitrary inflight node - might as well be this one. We must
        // first clear the edge from node to WaitingOnThrottle to avoid a cycle.
        if (node.getWaitingOn eq WaitingOnThrottle) node.setWaitingOn(null)
        WaitingOnThrottle.setWaitingOn(node)
        inflightNodes.add(node)
      }

    def removeInflightNode(previous: NodeTask): Unit =
      inflightNodes.synchronized {
        inflightNodes.remove(previous)
        // ensure that we are always waiting on an (arbitrary) incomplete node
        if ((WaitingOnThrottle.getWaitingOn eq previous) && inflightNodes.nonEmpty)
          WaitingOnThrottle.setWaitingOn(inflightNodes.head)
      }
  }
}

object Throttle {
  private[throttle] val log = getLogger[Throttle]
}

private[optimus] final case class ThrottleState(
    inflightWeight: Double,
    nInFlight: Int,
    nThrottledNode: Int,
    nQueued: Int)
