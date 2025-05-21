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

import optimus.graph._

/**
 * Controls the limit for how many nodes (as measured by weight) can be running concurrently within a Throttle
 */
private[optimus] abstract class ThrottleLimiter {

  /**
   * Updates the live state of the throttle limiter. This is called periodically (if pollIntervalMs > 0 and there are
   * queued nodes) and also whenever a throttled node completes or is added the throttle's queue.
   */
  private[throttle] def updateLiveState(): Unit = ()

  /**
   * Adjusts the inflight weight based on the completed node. This is called every time a throttle node completes.
   */
  private[throttle] def completed(completed: Throttle#ThrottledNode[_], nInFlight: Int): Unit

  /**
   * Returns true iff the possibleNext node should run now (in which case the Throttle will always run it), and updates
   * the inflight weight accordingly.
   *
   * If "forced" is true, nodes will be run no matter what, the throttle should update the weight as if it was returning
   * "true". This happens when we try to break deadlocks or cycles.
   */
  private[throttle] def canRunNow(possibleNext: Throttle#ThrottledNode[_], nInFlight: Int, forced: Boolean): Boolean

  /** Check that the weight is valid for this policy, and throw if it is not */
  private[throttle] def checkWeight(nodeWeight: Int): Unit =
    if (nodeWeight <= 0) throw new IllegalArgumentException("Weight must be a positive number!")

  /**
   * If >0 then then tryRunNext will be called each interval when there are nodes pending to run.
   * This is useful if the policy's limit can vary independently of nodes starting and completing.
   */
  private[throttle] def pollIntervalMs: Long = -1

  /**
   * Returns current "static" weight (based only on the weight of currently running nodes, not any live factors),
   * for diagnostic purposes
   */
  private[throttle] def getInflightWeightStatic: Double

  // For "waitForState". hasWaiter is set to true whenever a waiter first connects and remains true afterwards forever.
  // This is used to test throttle behaviour without Thread.sleep().
  private[throttle] var hasTestWaiter = false
  def notifyTestWaiters(): Unit = if (hasTestWaiter) this.notifyAll()
}

/**
 * A throttle limit policy based on a simple maximum weight. Note that if no other nodes are running then we will
 * allow the limit to be breached. This means that over-limit nodes will eventually run, just not concurrently with
 * any other nodes.
 */
private[optimus] class SimpleThrottleLimiter(private val limit: Int) extends ThrottleLimiter {
  require(limit >= 0, "Limit must be positive (or 0)")
  private var inflightWeight = 0L
  override def getInflightWeightStatic: Double = inflightWeight.toDouble

  private[throttle] override def completed(completed: Throttle#ThrottledNode[_], nInFlight: Int): Unit = synchronized {
    notifyTestWaiters()
    inflightWeight -= completed.weight
  }

  private[throttle] override def canRunNow(
      possibleNext: Throttle#ThrottledNode[_],
      nInFlight: Int,
      forced: Boolean): Boolean =
    synchronized {
      notifyTestWaiters()
      // we allow breaching the limit if no other tasks are running
      val runNow = forced || nInFlight == 0 || (limit >= inflightWeight + possibleNext.weight)
      if (runNow) inflightWeight += possibleNext.weight
      runNow
    }
}

/**
 * A throttle limit policy that oscillates between two limits. When the inflight weight reaches the upper limit, the
 * limit is set to the lower limit, and vice versa. This can be useful to encourage better batching behavior. Consider
 * if there is a large set of incoming nodes and we have already hit the upperLimit: the SimpleThrottleLimiter would
 * release one more node as each inflight node completed (causing little to no batching downstream of the throttle).
 * However the OscillatingThrottleLimiter would wait until only lowerLimit nodes are inflight before releasing up
 * to (upperLimit-lowerLimit) nodes in rapid succession, allowing good opportunity for batching).
 *
 * This policy only supports nodes of weight 1
 */
private[optimus] class OscillatingThrottleLimiter(private val upperLimit: Int, lowerLimit: Int)
    extends ThrottleLimiter {
  private var limit = upperLimit

  require(lowerLimit > 0 && lowerLimit < upperLimit, "lowerLimit must be a positive number less than upperLimit!")
  require(upperLimit > 0, "upperLimit must be positive")

  private[throttle] var inflightWeight: Long = 0
  override def getInflightWeightStatic: Double = inflightWeight.toDouble

  override def checkWeight(nodeWeight: Int): Unit = {
    if (nodeWeight != 1)
      throw new IllegalArgumentException("Can only use weights of 1 when using minimum concurrency")
  }

  private[throttle] override def completed(completed: Throttle#ThrottledNode[_], nInFlight: Int): Unit = synchronized {
    notifyTestWaiters()
    val old = inflightWeight

    // Release the weight of the completed node and if we hit the lower limit, switch to the upper limit.
    // n.b. policy is only supported with all tasks having equal weight == 1 so we can do exact equality check here
    inflightWeight -= 1
    if (inflightWeight == lowerLimit) limit = upperLimit

    Throttle.log.debug(
      s"Stats: (old=$old)-> $inflightWeight, limit=$limit, lowerLimit=$lowerLimit, upperLimit=$upperLimit")
  }

  private[throttle] override def canRunNow(
      possibleNext: Throttle#ThrottledNode[_],
      nInFlight: Int,
      forced: Boolean): Boolean =
    synchronized {
      notifyTestWaiters()
      val old = inflightWeight
      val runNow = forced || (limit >= inflightWeight + 1)

      // Add in the estimated weight of the new calculation and if we hit the upper limit, switch to the lower limit
      if (runNow) inflightWeight += 1
      if (inflightWeight >= limit) limit = lowerLimit

      Throttle.log.debug(
        s"Stats: (old=$old)-> $inflightWeight, " +
          s"limit=$limit, lowerLimit=$lowerLimit, upperLimit=$upperLimit, runNow=$runNow; inFlight=$nInFlight")
      runNow
    }
}

/**
 * A throttle limit policy that allows the inflight weight to be adjusted in real-time based on a live weigher function.
 * For example the limit could be based on the amount of (native or jvm) memory actually used. To avoid starting too
 * many nodes concurrently when the live weight is low (which might happen if e.g. the memory used by the inflight nodes
 * increases some time after the start), we use the weight specified for each node as an estimate of the actual weight,
 * and this is decayed over time (with the assumption that the live weight will grow over time).
 */
private[optimus] class LiveThrottleLimiter(
    private val limit: Int,
    liveWeigher: () => Int, // compute total weight in real-time (e.g. from memory allocator)
    decayTimeMs: Long)
    extends ThrottleLimiter {
  require(limit >= 0, "limit must be positive (or 0)")

  // overridden so that we will periodically be called back on checkRunNowAndUpdateWeights when nodes are queued even
  // if no node has completed (important since our liveWeight can change independently of nodes starting and completing)
  override val pollIntervalMs: Long = decayTimeMs

  private val decayRateNs = if (decayTimeMs > 0) 1.0e-6 / decayTimeMs else 0
  private var inflightWeightStatic = 0.0
  private var prevDecayEvent: Long = 0

  private[throttle] override def getInflightWeightStatic = inflightWeightStatic

  private def decayFactor(periodNs: Long) =
    if (decayRateNs > 0.0) Math.exp(-decayRateNs * periodNs) else 1.0

  private[throttle] override def updateLiveState(): Unit = synchronized {
    notifyTestWaiters()
    val t = OGTrace.nanoTime()
    val old = inflightWeightStatic
    // Decay the inflight static weight if required and return the factor for logging purposes
    val decay = if (decayRateNs > 0.0 && prevDecayEvent > 0) {
      val d = decayFactor(t - prevDecayEvent)
      inflightWeightStatic *= d
      d
    } else 1.0
    prevDecayEvent = t

    Throttle.log.debug(s"Stats: (old=$old)*(decay=$decay) -> $inflightWeightStatic")
  }

  private[throttle] override def completed(completed: Throttle#ThrottledNode[_], nInFlight: Int): Unit = synchronized {
    notifyTestWaiters()
    // Decay the static weight of the completed node (which should match its decayed contribution to the inflight weight)
    val old = inflightWeightStatic
    val toRelease = if (completed eq null) 0.0 else completed.weight * decayFactor(prevDecayEvent - completed.tStarted)
    inflightWeightStatic -= toRelease
    if (nInFlight == 0) {
      assert(inflightWeightStatic < 0.1) // if this isn't true, our book-keeping is very off
      inflightWeightStatic = 0.0 // Clean up any roundoff error
    }

    Throttle.log.debug(s"Stats: (old=$old)-(released=$toRelease) -> $inflightWeightStatic; inFlight=$nInFlight")

  }
  private[throttle] override def canRunNow(
      possibleNext: Throttle#ThrottledNode[_],
      nInFlight: Int,
      forced: Boolean): Boolean =
    synchronized {
      notifyTestWaiters()
      val old = inflightWeightStatic
      val liveWeight = liveWeigher()
      val estimatedNew = possibleNext.weight
      val runNow = forced || nInFlight == 0 || (limit >= inflightWeightStatic + liveWeight + estimatedNew)
      if (runNow) inflightWeightStatic += estimatedNew
      Throttle.log.debug(
        s"Stats: (old=$old)+(est=$estimatedNew) -> " +
          s"$inflightWeightStatic, live=$liveWeight, limit=$limit, runNow=$runNow; inFlight=$nInFlight")
      runNow
    }
}
