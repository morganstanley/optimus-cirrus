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
package optimus.graph.tracking.ttracks

import optimus.graph.GraphException
import optimus.graph.GraphInInvalidState
import optimus.graph.NodeTask
import optimus.graph.NodeTaskInfo
import optimus.graph.NodeTrace
import optimus.graph.PropertyNode
import optimus.graph.Settings
import optimus.graph.TweakTreeNode
import optimus.graph.TweakableKey
import optimus.graph.TweakableListener
import optimus.graph.cache.CacheFilter
import optimus.graph.cache.Caches
import optimus.graph.cache.CauseUntrackedTweakableTweaked
import optimus.graph.cache.NCSupport
import optimus.graph.diagnostics.PNodeInvalidate
import optimus.graph.diagnostics.StatsSummary
import optimus.graph.tracking.CleanupScheduler.InterruptionFlag
import optimus.graph.tracking.CleanupScheduler.NoInterruption
import optimus.graph.tracking.CurrentTimeSource
import optimus.graph.tracking.DependencyTracker
import optimus.graph.tracking.DependencyTrackerQueue
import optimus.graph.tracking.DependencyTrackerRoot
import optimus.graph.tracking.EventCause
import optimus.graph.tracking.TrackedNodeInvalidationObserver
import optimus.graph.tracking.TrackingGraphCleanupTrigger
import optimus.graph.tracking.TraversalIdSource
import optimus.platform.NodeHash
import optimus.platform.Tweak
import optimus.platform.util.Log

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder

/**
 * Tracks tweakable nodes and the nodes which depend on them. Supports cache invalidation of the dependent nodes - this
 * is triggered by MutableTweakContainer (and others) when the tweakable nodes are tweaked
 */
private[optimus] final class TweakableTracker(
    val owner: DependencyTracker,
    name: String,
    val trackingDepth: Int,
    queue: DependencyTrackerQueue,
    val idSource: TraversalIdSource,
    timeSource: CurrentTimeSource,
    /**
     * returns children which are interested in invalidation notifications for NodeKey, or all children if NodeKey is
     * null
     */
    children: TweakableKey => Iterable[TweakableTracker],
    parent: TweakableListener,
    onInvalidateAll: TrackedNodeInvalidationObserver => Unit)
    extends TweakableListener {
  import DependencyTracker.log
  import TweakableTracker._

  override val respondsToInvalidationsFrom: Set[TweakableListener] = parent.respondsToInvalidationsFrom + this
  override def isDisposed: Boolean = owner.isDisposed

  override val refQ: RefCounter[NodeTask] = new RefQ(owner.root)

  private var collectedReferences: refQ.Acc = _
  protected[tracking] var ttracks: java.util.Map[TweakableKey, TTrackRoot] = new ConcurrentHashMap()
  private[tracking] var timedCleanupDelay = Settings.ttrackCleanupMinDelayFromLastCleanupMs
  private var nextTimedCleanup = timeSource.currentTimeMillis + timedCleanupDelay

  private[tracking] def cleanupRequired(): Boolean = {
    val size = ttracks.size
    (size > 0) && {
      val tracks = trackCount.sum()
      val collectedSinceLastCleanup = refQ.getUpdate(collectedReferences)

      val trackLimit = size * Settings.ttrackTrackCallToRootThresholdRatio
      val refLimit = size * Settings.ttrackClearedNodeRefToRootThresholdRatio
      val timeToNextTimedCleanup = nextTimedCleanup - timeSource.currentTimeMillis
      log.debug(
        s"[$name] GC Collected TRefs: $collectedSinceLastCleanup (threshold $refLimit), " +
          s"TrackActivity: $tracks (threshold $trackLimit), " +
          s"TimeToNextTimedCleanup: $timeToNextTimedCleanup")
      if (collectedSinceLastCleanup > refLimit || tracks > trackLimit || timeToNextTimedCleanup <= 0) {
        // reset counters regardless of which triggered the cleanup (since we're going to cleanup all types of
        // garbage). Note that the timeToNextTimedCleanup is updated after the cleanup since it looks at the stats.
        true
      } else false
    }
  }

  override def dependencyTrackerRootName: String = owner match {
    case dependencyTrackerRoot: DependencyTrackerRoot => dependencyTrackerRoot.name
    case _                                            => "[not tracking]"
  }

  def disposeTweakableTracker(): Unit = ttracks = null

  private[tracking] def clearAllTracking(): Unit = ttracks.clear()

  /* Point ttrack(node) to an executed node (but not constant nodes e.g. ACPN) to be able to invalidate that node */
  override def onTweakableNodeCompleted(node: PropertyNode[_]): Unit = track(node.trackKey, node)
  /* Point ttrack(key) to a node that used tweakable but constant node */
  override def onTweakableNodeUsedBy(key: PropertyNode[_], node: NodeTask): Unit = track(key.trackKey, node)
  /* Point ttrack(key) to a node that used tweakable but constant node */
  override def onTweakableNodeHashUsedBy(key: NodeHash, node: NodeTask): Unit = track(key, node)
  /* Point ttrack(key) to a node that used a constant tweak (as opposed to tweak by value, that would have ttrack
   * point to by the call to onTweakableNodeCompleted */
  override def onTweakUsedBy(ttn: TweakTreeNode, node: NodeTask): Unit = {
    if (ttn.trackingDepth <= this.trackingDepth) {
      track(ttn.key.trackKey, node)
    }
    // else ignoring ttn.nested... because the ptracks are already [SEE_ON_TWEAK_USED] pointing from the outer
    // scenarioStacks to tweak nodes found in them
  }

  override def reportTweakableUseBy(node: NodeTask): Unit = {}

  private[this] val trackCount = new LongAdder()

  private[this] def track(key: TweakableKey, node: NodeTask): Unit = {
    if (Settings.schedulerAsserts && node.scenarioStack.isRecordingTweakUsage && !NCSupport.isDelayedProxy(node))
      throw new GraphInInvalidState(s"Attempted to track XS node: $node")

    if (key.propertyInfo.trackForInvalidation()) {
      trackCount.increment()

      // check first to avoid unnecessary allocation
      val originalTTrack = ttracks.get(key)
      val useTTrack =
        if (originalTTrack ne null) originalTTrack
        else {
          // fully configure the ttrack. This doesnt involve any CAS as it is not visible yet
          val newTTrack = new TTrackRoot()
          val existingTTrack = ttracks.putIfAbsent(key.tidyKey, newTTrack)
          // if someone raced us and inserted it since our "get" call above fixup the parent
          if (existingTTrack ne null) existingTTrack else newTTrack
        }

      // Enabling the following assert is not correct because of the following race condition:
      // A new node (that is reported here) has been just 'published' via cache
      // Starts to run on another thread and picks up dependency on some tweakable
      // By the time we get here its node.info() will not be null anymore

      // if (node.info() ne null) throw new GraphInInvalidState("This should no longer be allowed")

      node.combineXinfo(useTTrack)
    }
  }

  def invalidateByTweaks(
      expanded: Iterable[Tweak],
      cause: EventCause,
      observer: TrackedNodeInvalidationObserver): Unit = {
    var propertyTweaks: List[Tweak] = Nil
    var untrackedTweaks: List[Tweak] = Nil
    expanded.foreach { twk =>
      if (twk.target.propertyInfo.trackForInvalidation()) {
        val key = twk.target.hashKey
        if (key ne null) invalidateByKey(key, twk, cause, observer)
        else propertyTweaks ::= twk
      } else untrackedTweaks ::= twk
    }

    if (untrackedTweaks.nonEmpty) {
      log.error(s"There were tweaks to non-tracked tweakables - we will clear the cache! $untrackedTweaks")
      invalidateAllCachedNodes(untrackedTweaks, cause, observer)
    } else if (propertyTweaks.nonEmpty) {
      invalidateByPropertyMap(propertyTweaks.iterator.map(t => (t.target.propertyInfo, t)).toMap, cause, observer)
    }
  }

  /**
   * Perform an invalidation trace starting from a given tweak for this DependencyTracker, then recursively for each
   * child scenario that contains a tweak for a given node. The tweak will be automatically derived from the NodeKey.
   *
   * @param pkey
   *   The NodeKey for which to invalidate tweaks.
   * @param tweak
   *   Tweak, used only for profiling.
   * @param cause
   *   The root cause that created this invalidation, for profiling.
   * @param preview
   *   Whether or not to do invalidation previews.
   */
  def invalidateByKey(
      pkey: TweakableKey,
      tweak: Tweak,
      cause: EventCause,
      observer: TrackedNodeInvalidationObserver): Unit = {
    // invalidateByKey could have derived the key2use inside and the only reason to do it here it to avoid extra allocation
    pkey.propertyInfo.markInstanceTweakedInTracker()
    invalidateByKey(pkey, pkey.trackKey, tweak, cause, observer)
  }

  /**
   * Perform an invalidation trace starting from a given tweak for this DependencyTracker, then recursively for each
   * child scenario that contains a tweak for a given node.
   *
   * @param pkey
   *   The NodeKey to use to check child scenarios.
   * @param key2use
   *   The tweak to use as the start of the invalidation trace.
   * @param tweak
   *   Tweak, used only for profiling.
   * @param preview
   *   Whether or not to do invalidation previews.
   */
  private def invalidateByKey(
      pkey: TweakableKey,
      key2use: TweakableKey,
      tweak: Tweak,
      cause: EventCause,
      observer: TrackedNodeInvalidationObserver): Unit = {
    val ttrack = ttracks.get(key2use)
    if (ttrack ne null) {
      ttracks.remove(key2use)
      // The null case is for TrackingTemporalContexts. We should probably find a way to deal with them.
      val trace = if (tweak != null) PNodeInvalidate(tweak, cause, owner.scenarioReference) else null
      if (trace ne null) NodeTrace.invalidate(Invalidators.invalidate(ttrack, trace, idSource, observer))
      else Invalidators.invalidate(ttrack, idSource, observer)
    }
    children(pkey).foreach { _.invalidateByKey(pkey, key2use, tweak, cause, observer) }
  }

  // returns matching parent NTI or null
  private def findPropertyTweakInParents(
      tweakedProperties: Map[NodeTaskInfo, Tweak],
      trackedProperty: NodeTaskInfo): NodeTaskInfo = {
    val parents = trackedProperty.matchOn()
    var found: NodeTaskInfo = null
    if (parents != null) {
      var i = 0
      while (found == null && i < parents.size) {
        if (tweakedProperties.contains(parents(i))) found = parents(i)
        i += 1
      }
      found
    } else null
  }

  /**
   * Perform an invalidation trace for all nodes in this DependencyTracker that match a specific PropertyInfo, then
   * perform the same trace for all child scenarios.
   *
   * @param tweakedProperties
   *   The properties to invalidate (mapped to sample Tweak which is only used for profiling)
   * @param cause
   *   The event that caused this invalidation.
   * @param preview
   *   Whether or not to do invalidation previews.
   */
  private def invalidateByPropertyMap(
      tweakedProperties: Map[NodeTaskInfo, Tweak],
      cause: EventCause,
      observer: TrackedNodeInvalidationObserver): Unit = {
    tweakedProperties.keysIterator.foreach(_.markPropertyTweakedInTracker())
    val it = ttracks.entrySet().iterator()
    while (it.hasNext) {
      val entry = it.next()
      val trackedProperty = entry.getKey.propertyInfo
      val foundProperty =
        if (tweakedProperties.contains(trackedProperty)) trackedProperty
        else findPropertyTweakInParents(tweakedProperties, trackedProperty)
      if (foundProperty != null) {
        it.remove()
        val value = entry.getValue
        val trace = PNodeInvalidate(tweakedProperties(foundProperty), cause, owner.scenarioReference)
        if (trace ne null) NodeTrace.invalidate(Invalidators.invalidate(value, trace, idSource, observer))
        else Invalidators.invalidate(value, idSource, observer)
      }
    }
    children(null).foreach { _.invalidateByPropertyMap(tweakedProperties, cause, observer) }
  }

  /**
   * Invalidates all cached nodes (including untracked nodes) for this dependency tracker and its descendants
   */
  private def invalidateAllCachedNodes(
      tweaks: List[Tweak],
      cause: EventCause,
      observer: TrackedNodeInvalidationObserver): Unit = {
    val trace = PNodeInvalidate(null, cause, owner.scenarioReference)

    // invalidate all nodes within our consistent subtree (because of overlays it's not sufficient to only invalidate
    // nodes from us and our children)
    val consistentSubtreeRoot = queue.tracker.underlayTweakableTracker

    Caches.clearCaches(
      CauseUntrackedTweakableTweaked,
      includeSiGlobal = false,
      filter = CacheFilter({ node =>
        val invalidate =
          node.scenarioStack().tweakableListener.respondsToInvalidationsFrom.contains(consistentSubtreeRoot)
        if (invalidate) {
          node.invalidateCache(observer)
          if (trace ne null) trace.add(node.propertyInfo)
        }
        invalidate
      })
    )
    // it's a bit arbitrary which tweak we should allocate the invalidations to - any individual tweak would have caused
    // all of them. We'll take the extreme approach of blaming every tweak for every invalidation
    if (trace ne null) tweaks.foreach { twk =>
      NodeTrace.invalidate(trace.copyForTweak(twk))
    }
    onInvalidateAll(observer)
  }

  private def ttracksWasDisposed = ttracks eq null

  protected[graph] def forceCleanup(): CleanupStats = cleanupTTracks(ForceCleanup, NoInterruption) match {
    // a forced cleanup should always be complete, never "partially", and never with "nothing to do" unless
    // we've been disposed
    case Complete(stats)                   => stats
    case NothingToDo if ttracksWasDisposed => CleanupStats.zero
    case r                                 => throw new GraphException(s"Unexpected result from forced cleanup: $r")
  }

  /**
   * Cleans up the TTrack graph in this TweakableTracker (does not clean descendants except where TTrack graphs reach in
   * to graphs referenced from descendants, as in the case of byName tweaks)
   */
  protected[tracking] def cleanupTTracks(request: CleanupRequest, interrupt: InterruptionFlag): CleanupResult = {
    if ((!ttracksWasDisposed) && request.shouldCleanupNow(this)) {
      val iterator = request match {
        case PartiallyComplete(it, _) => it
        case _                        => ttracks.entrySet.iterator
      }

      val visitor = new CleanupVisitor()
      visitor.clean(iterator, idSource, interrupt)
      // (note that we don't merge in the previous stats, if any, because GarbageCollectionSupport will already have
      // merged them in to the totals, so if we include them here it would cause double counting)
      val stats = CleanupStats.fromVisitor(visitor)

      if (iterator.hasNext) {
        log.debug(s"[$name] Cleanup interrupted $stats")
        PartiallyComplete(iterator, stats)
      } else {
        log.debug(s"[$name] Cleanup complete $stats")
        // update the next timeout based on how productive this cleanup was (whether or not this cleanup was triggered
        // by the timeout)
        timedCleanupDelay = recalculateTimedCleanupDelay(name, stats, timedCleanupDelay)
        nextTimedCleanup = timeSource.currentTimeMillis + timedCleanupDelay

        // update other values based on the current state
        trackCount.reset()
        collectedReferences = refQ.snapshot
        Complete(stats)
      }
    } else NothingToDo
  }

  protected[tracking] def selfAndAllDescendants: List[TweakableTracker] = {
    this :: children(null).iterator.flatMap { _.selfAndAllDescendants }.toList
  }

  override def toString: String = s"TweakableTracker($name)"

  protected[graph] def ttrackGraphString: String =
    new DigraphWriter().visit(ttracks.entrySet().iterator(), idSource)

  private[tracking] def resetTraversalIds(newId: Int): Unit = {
    new MarkingGraphTraverser().visit(ttracks.values(), newId)
  }

  private[graph] def ttrackGraphStats: StatsSummary =
    TTrackStatsSupport.ttrackGraphStats(ttracks.values(), idSource)
}

object TweakableTracker {

  /** a request for the TweakableTracker to (maybe) clean itself */
  sealed trait CleanupRequest {
    def shouldCleanupNow(tracker: TweakableTracker): Boolean = true
  }

  /** result of a cleanup request (might be complete, partially complete, or nothing to do) */
  sealed trait CleanupResult {
    def stats: CleanupStats
  }

  /** force a full cleanup even if our heuristics say it's not needed */
  case object ForceCleanup extends CleanupRequest

  /** cleanup only if our heuristics say it's needed */
  case object CleanupIfNecessary extends CleanupRequest {
    override def shouldCleanupNow(tracker: TweakableTracker): Boolean = tracker.cleanupRequired()
  }

  /**
   * a partially complete cleanup is both a Result and a Request because it can be passed back in to resume the cleanup
   */
  private final case class PartiallyComplete(
      iterator: util.Iterator[util.Map.Entry[TweakableKey, TTrackRoot]],
      stats: CleanupStats)
      extends CleanupRequest
      with CleanupResult

  /** the result of a complete cleanup */
  final case class Complete(stats: CleanupStats) extends CleanupResult

  /** the result of a CleanupIfNecessary request where we determined that cleanup wasn't actually needed */
  case object NothingToDo extends CleanupResult {
    override def stats: CleanupStats = CleanupStats.zero
  }

  /**
   * statistics from a full or partial cleanup
   *
   * @param edgesTraversed
   *   how many edges (caller -> callee links) have been traversed
   * @param edgesRemoved
   *   how many edges have been removed
   * @param rootsTraversed
   *   howe many ttrack roots (corresponding to a tweak key) have been traversed
   * @param rootsRemoved
   *   how many ttrack roots have been removed
   * @param nodeRefsRemoved
   *   how many TTrackRefs have been removed (since they were pointing to Nodes which have been GC'd)
   * @param redundantTTracksRemoved
   *   how many TTrack graph nodes (not o.g.Nodes) have been been removed
   * @param excessCapacityRemoved
   *   how many excess capacity in Ptracks has been trimmed (e.g. by replacing a big PTrack array with a smaller one)
   */
  final case class CleanupStats(
      edgesTraversed: Int,
      edgesRemoved: Int,
      rootsTraversed: Int,
      rootsRemoved: Int,
      nodeRefsRemoved: Int,
      redundantTTracksRemoved: Int,
      excessCapacityRemoved: Int) {

    def merge(o: CleanupStats): CleanupStats = CleanupStats(
      edgesTraversed = this.edgesTraversed + o.edgesTraversed,
      edgesRemoved = this.edgesRemoved + o.edgesRemoved,
      rootsTraversed = this.rootsTraversed + o.rootsTraversed,
      rootsRemoved = this.rootsRemoved + o.rootsRemoved,
      nodeRefsRemoved = this.nodeRefsRemoved + o.nodeRefsRemoved,
      redundantTTracksRemoved = this.redundantTTracksRemoved + o.redundantTTracksRemoved,
      excessCapacityRemoved = this.excessCapacityRemoved + o.excessCapacityRemoved
    )

    override def toString: String =
      if (this == CleanupStats.zero) "[zero]"
      else {
        f"[edgesTraversed=$edgesTraversed%,d, edgesRemoved=$edgesRemoved%,d, rootsTraversed=$rootsTraversed%,d, " +
          f"rootsRemoved=$rootsRemoved%,d, nodeRefsRemoved=$nodeRefsRemoved%,d, " +
          f"redundantTTracksRemoved=$redundantTTracksRemoved%,d, excessCapacityRemoved=$excessCapacityRemoved%,d]"
      }
  }

  object CleanupStats {
    def fromVisitor(v: CleanupVisitor): CleanupStats = CleanupStats(
      edgesTraversed = v.edgesTraversed,
      edgesRemoved = v.edgesRemoved,
      rootsTraversed = v.rootsTraversed,
      rootsRemoved = v.rootsRemoved,
      nodeRefsRemoved = v.nodeRefsRemoved,
      redundantTTracksRemoved = v.redundantTTracksRemoved,
      excessCapacityRemoved = v.excessCapacityRemoved
    )

    val zero: CleanupStats = CleanupStats(0, 0, 0, 0, 0, 0, 0)
  }

  /**
   * Recalculates a delay for timed cleanups depending on the cost:benefit ratio from the CleanupStats
   */
  private[ttracks] def recalculateTimedCleanupDelay(
      name: String,
      stats: TweakableTracker.CleanupStats,
      currentDelay: Long): Int = {
    // how much stuff did we remove (+1 to avoid possible divide by zero below) vs. how much did we visit?
    val totalBenefit = stats.edgesRemoved + stats.redundantTTracksRemoved + stats.excessCapacityRemoved + 1
    val costToBenefitRatio = stats.edgesTraversed.toDouble / totalBenefit.toDouble

    // if we got 1:1 cost:benefit that's not bad, so bring the delay down by half.
    // if it was really low cost:benefit bring the delay down to up to 90%
    // if it was really bad cost:benefit push the delay up by 100% (this not as aggressive as the decrease because
    // a really productive cleanup might well be followed by a less useful one and we don't want to flip flop
    // between long and short delay too much)
    val delayAdjustment = Math.max(0.1, Math.min(2.0, costToBenefitRatio * 0.5))

    // apply the adjustment but with cap and floor
    val newDelay = Math.min(
      Settings.ttrackCleanupMaxDelayFromLastCleanupMs,
      Math.max(Settings.ttrackCleanupMinDelayFromLastCleanupMs, (delayAdjustment * currentDelay).toInt))
    DependencyTracker.log.debug(
      s"[$name] Cleanup had cost:benefit ratio $costToBenefitRatio so next timed cleanup " +
        s"delay is adjusted from $currentDelay ms to $newDelay ms")
    newDelay
  }

  private object RefCounterWatermark extends TrackingGraphCleanupTrigger
  private class RefQ(root: DependencyTrackerRoot) extends RefCounter[NodeTask] with Log {
    override def nextWatermark(currentWatermark: Acc): Acc = currentWatermark + Settings.ttrackClearedNodeRefWatermark

    override def onWatermarkBreached(): Unit = {
      // Breaching watermarks enqueues high prio cleanups that will jump the queue in front of other actions. Those
      // cleanups are still subject to `cleanupRequired()` so they aren't guaranteed to run or cleanup anything, but they
      // at least ensure that we will attempt some cleanups, even if action queues are consistently busy.
      //
      // This is called from the TTrackRef constructor (rarely but still!) so it needs to return asynchronously without
      // waiting on the cleanup, otherwise there is a risk of deadlock. That is always the case because cleanups are
      // always done asynchronously (through optimus.graph.tracking.DependencyTrackerQueue.executeAsync, which
      // either enqueues or run through Scheduler.executeNodeAsync).
      //
      // The cleanup is not interruptable, which is important because the place where it becomes necessary is when
      // queues are consistently busy. Were it interruptable, it would be interrupted immediately after starting.
      log.info(s"Scheduling cleanup for ${root.name} based on reference counter watermark")
      root.runCleanupNow(RefCounterWatermark)
    }
  }
}
