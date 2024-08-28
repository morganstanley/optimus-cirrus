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
package optimus.graph.tracking

import java.lang.ref.WeakReference
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import optimus.graph._
import optimus.graph.cache.Caches
import optimus.graph.cache.ClearCacheCause
import optimus.graph.diagnostics.GraphDiagnostics
import optimus.graph.diagnostics.TTrackStats
import optimus.graph.tracking.DependencyTrackerQueue.QueueStats
import optimus.graph.tracking.ttracks.Invalidators
import optimus.graph.tracking.ttracks.TTrack
import optimus.platform._
import optimus.platform.annotations.deprecating
import optimus.platform.util.PrettyStringBuilder
import optimus.platform.storable.FakeModuleEntity
import optimus.ui.ScenarioReference
import optimus.ui.ScenarioReferenceTrackingHelper

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.Success
import scala.util.Try

/**
 * A DependencyTrackerRoot is the root of a tree of DependencyTrackers. Has various user-facing APIs which act across
 * the whole tree, and functionality for creating and finding roots.
 */
final class DependencyTrackerRoot private (
    parentScenarioStack: ScenarioStack,
    val timedScheduler: TimedScheduler,
    val appletName: String)
    extends DependencyTracker(null, ScenarioReference.Root, parentScenarioStack)
    with CallbackSupport
    with GarbageCollectionSupport
    with TraversalIdSource {

  import DependencyTrackerRoot.log

  addShutdownHook()

  if (appletName.nonEmpty)
    nodeInputs.addInput(AppletNameTag, appletName)

  override def name =
    if (appletName.nonEmpty) s"${ScenarioReference.Root.name}-${appletName}" else ScenarioReference.Root.name

  /**
   * Internal method to register a shutdown hook on the containing environment. We use a WeakReference because roots may
   * be GCed when they are no longer needed and we do not want the RuntimeEnvironment to hold a hard reference to the
   * root keeping it alive.
   */
  private def addShutdownHook(): Unit = {
    val ref = new WeakReference(this)
    root.parentScenarioStack.env.addShutdownAction {
      val root = ref.get()
      if (root != null) {
        try {
          root.dispose()
        } catch {
          case t: DependencyTrackerDisposedException =>
          // ignore, somebody has already cleaned up
        }
      }
    }
    ref.get()
  }

  private var lastPrinted = System.nanoTime()
  def logStalling(scenarioRef: ScenarioReference): Unit = {
    val now = System.nanoTime()
    val waitDuration = ((now - lastPrinted) / 1000000) / 1000.0
    if (waitDuration > Settings.trackingScenarioActionTimeLimit) {
      lastPrinted = now
      log.warn(
        s"Some dependency tracker actions on $scenarioRef have been waiting for longer than the time limit " +
          s"(${Settings.trackingScenarioActionTimeLimit}s)")
      log.info(GraphDiagnostics.getDependencyTrackerWorkInfo(false))
    }
  }

  // the last ID we used in normal traversal of the graph
  private[this] var traversalId: Int = 0
  // the last ID we used when resetting the TTrack graphs (due to rollover avoidance on traversalId)
  private[this] var resetId: Int = 0

  /**
   * Runs the specified lambda given a fresh ttrack traversal ID which is guaranteed to be greater than any existing ID
   * in any ttrack owned by this DependencyTrackerRoot. The lambda must return the highest ID that it marked any ttrack
   * with in order to preserve this invariant.
   *
   * Note that if the previous highest traversal ID is "large enough", we will automatically remark the graph to a
   * negative ID and pass zero to the lambda, to avoid the risk of the ID rolling over.
   *
   * This method must only be used from Update actions on the root, or from test code which doesn't run concurrently
   * with Updates.
   */
  // (Integer not Int because this is called from Java code and Scala erases Int => Object, not Int => Integer)
  private[tracking] def withFreshTraversalId(func: Integer => Integer): Unit = {
    val start = if (traversalId > Int.MaxValue / 2) {
      log.info(s"TTrack traversalId of $traversalId is too large - we will reset all ttrack graphs to zero")
      resetAllTraversalIds()
      1
    } else traversalId + 1

    try {
      val end = func(start)
      require(end >= start, s"TTrack traversal $func started on id $start but ended on id $end")
      traversalId = end
    } catch {
      case t: Throwable =>
        log.error(
          s"TTrack traversal failed with error $t. Resetting traversalId to zero to mitigate possible ttrack corruption...")
        resetAllTraversalIds()
        throw t
    }
  }

  /**
   * resets traversal ids for all ttracks in all tracking graphs to some negative number
   */
  private[graph] def resetAllTraversalIds(): Unit = {
    // we use a new resetId for each reset because otherwise if some parts of the graph had previously been reset
    // and other parts had not, we might not visit some parts. note that just alternating between -1 and -2 would be
    // fine, but it's easier to debug if we use a new negative id each time
    resetId -= 1
    if (resetId == Int.MinValue) resetId = -1
    traversalId = 0
    resetAllTraversalIds(resetId)
  }

  require(
    !parentScenarioStack.isTrackingOrRecordingTweakUsage,
    s"the parent scenario stack cannot be tracked - tweakableListener is ${parentScenarioStack.tweakableListener}"
  )

  override protected[optimus] def root = this

  // TODO (OPTIMUS-14037): ReportChange should be driven by the TrackingScope, or removed
  var reportChange = false

  def getScenarioReferences: collection.Seq[ScenarioReference] = {
    def getScenarioReferences(ts: DependencyTracker): collection.Seq[ScenarioReference] = {
      ts.scenarioReference +: ts.children.flatMap(getScenarioReferences)
    }
    getScenarioReferences(this)
  }

  private var namedStructureDependency: TTrack = _
  private var namedStructure: Map[String, DependencyTracker] = _

  private def takeNamedStructureDependency(): Unit = {
    if (namedStructureDependency == null) {
      namedStructureDependency = new TTrack(null)
    }
    EvaluationContext.currentNode.combineInfo(namedStructureDependency)
  }
  private def fireNamedStructureDependency(): Unit = {
    if (namedStructureDependency != null) {
      Invalidators.invalidate(namedStructureDependency, this)
      namedStructure = null
    }
    namedStructureDependency = null
  }

  private val namedStructureBuilder = mutable.Map.empty[String, DependencyTracker]
  private[tracking] def addedChild(ts: DependencyTracker): Unit = {
    if (ts.name ne null) withNameLock {
      namedStructureBuilder.put(ts.name, ts)
      fireNamedStructureDependency()
    }
  }
  addedChild(this)
  private[tracking] def removedDisposedNamedChildren(): Unit = {
    withNameLock {
      val origSize = namedStructureBuilder.size
      namedStructureBuilder.retain { case (_, ts) => !ts.isDisposed }
      if (origSize != namedStructureBuilder.size) fireNamedStructureDependency()
    }
  }

  private[optimus] def allNamedChildren(takeDependency: Boolean = true): Map[String, DependencyTracker] =
    withNameLock {
      if (takeDependency) takeNamedStructureDependency()
      if (namedStructure == null) {
        namedStructure = namedStructureBuilder.toMap
      }
      namedStructure
    }
  private[optimus] def childByName(name: String, takeDependency: Boolean = true): Option[DependencyTracker] =
    withNameLock {
      // only force the generation of the map and tracking if required
      if (takeDependency) allNamedChildren(takeDependency).get(name) else namedStructureBuilder.get(name)
    }

  private[optimus] def getOrCreateScenario(ref: ScenarioReference): DependencyTracker = getOrCreateScenario(ref, false)
  private[optimus] def getScenarioOrThrow(ref: ScenarioReference): DependencyTracker = getOrCreateScenario(ref, true)
  private[optimus] def getScenarioIfExists(ref: ScenarioReference): Option[DependencyTracker] =
    Try(getOrCreateScenario(ref, true)).toOption

  private[this] def getOrCreateScenario(ref: ScenarioReference, failRatherThanCreate: Boolean): DependencyTracker =
    withNameLock {
      if (ref == ScenarioReference.Root)
        this
      else
        ref.parent match {
          case Some(parentRef) =>
            val name = ref.name
            // n.b. various parts of code (including excel integration) require that scenario names are unique across the
            // whole tracking scenario structure, i.e. a given name should exist under at most one parent path
            childByName(name, takeDependency = false) match {
              case Some(existing) =>
                if (existing.scenarioReference == ref) existing
                else if (existing.scenarioReference.equalsIgnoringConcurrency(ref))
                  // since we got here we didn't have a full match but we DID have a matching path
                  throw new IllegalArgumentException(
                    s"Requested scenario $ref has same path but different concurrency mode to " +
                      s"existing scenario ${existing.scenarioReference}")
                else
                  throw new IllegalArgumentException(
                    s"Requested scenario $ref has same name but different parent to " +
                      s"existing scenario ${existing.scenarioReference}")

              case None =>
                if (failRatherThanCreate) throw new GraphInInvalidState(s"Requested scenario $ref does not exist")
                val parent = getOrCreateScenario(parentRef, failRatherThanCreate)
                log.debug(s"Creating scenario '${name}' as child of parent '${parent.name}'")
                parent.newChildScenario(ref)
            }
          case None =>
            // this should only happen for DummyScenarioReference, though there might be other cases if someone
            // has been hacking around. either way, nothing we can do here...
            throw new IllegalArgumentException(s"Cannot get or create DependencyTracker for reference $ref")
        }
    }

  private[optimus] def assertValidScenario(ref: ScenarioReference): Unit =
    // would be nice to use
    // withNameLock {
    // but the compiler complains because the inliner is too late on the compiler so manually unrolled
    namedStructureBuilder.synchronized {
      if (ref == ScenarioReference.Root) {} else
        ref.parent match {
          case Some(parentRef) =>
            val name = ref.name
            // n.b. various parts of code (including excel integration) require that scenario names are unique across the
            // whole tracking scenario structure, i.e. a given name should exist under at most one parent path
            childByName(name, takeDependency = false) match {
              case Some(existing) if existing.scenarioReference == ref =>
              case Some(existing) =>
                if (existing.scenarioReference.equalsIgnoringConcurrency(ref))
                  // since we got here we didn't have a full match but we DID have a matching path
                  throw new IllegalArgumentException(
                    s"Requested scenario $ref has same path but different concurrency mode to " +
                      s"existing scenario ${existing.scenarioReference}")
                else
                  throw new IllegalArgumentException(
                    s"Requested scenario $ref has same name but different parent to " +
                      s"existing scenario ${existing.scenarioReference}")

              case None =>
                assertValidScenario(parentRef)
            }
          case None =>
            // this should only happen for DummyScenarioReference, though there might be other cases if someone
            // has been hacking around. either way, nothing we can do here...
            throw new IllegalArgumentException(s"Cannot get or create DependencyTracker for reference $ref")
        }
    }

  private[optimus] def withNameLock[T](fn: => T): T = {
    namedStructureBuilder.synchronized(fn)
  }

  DependencyTrackerRoot.recordRoot(this)

  private def reevaluateTrackedNodesAsyncImpl[M](
      cause: EventCause,
      allNodes: collection.Seq[TrackedNode[_]],
      eachCallback: Try[Unit] => Unit): Int = {
    val byTracker = allNodes.groupBy(_.dependencyTracker)
    byTracker foreach { case (tracker, nodesInScenario) =>
      require(tracker.root eq this, "wrong tracking scenario")
      tracker.queue
        .executeAsync(tracker.reevaluateTrackedNodesAction(cause, nodesInScenario map (_.underlying)), eachCallback)
    }
    byTracker.size
  }

  def reevaluateTrackedNodesAsync[M](
      cause: EventCause,
      nodes: collection.Seq[TrackedNode[_]],
      callback: Try[Unit] => Unit = null): Unit = {
    if (nodes nonEmpty) {
      if (callback eq null)
        reevaluateTrackedNodesAsyncImpl(cause, nodes, null)
      else {
        val counter = new AtomicInteger(0)

        def newCallback(ignored: Try[Unit]) = {
          if (counter.decrementAndGet() == 0) callback(Success(()))
        }

        counter.addAndGet(1 + reevaluateTrackedNodesAsyncImpl(cause, nodes, newCallback))
        newCallback(null)
      }
    } else if (callback ne null) {
      callback(Success(()))
    }
  }

  private[tracking] def clearAllTrackingAsync(callback: (Try[Unit]) => Unit): Unit =
    queue.executeAsync(clearTrackingAction, callback)

  private[graph] def clearAllTracking(): Unit =
    queue.execute(clearTrackingAction)

  private def clearTrackingAction = new DependencyTrackerActionUpdate[Unit] with InScenarioAction {
    final override private[tracking] def alreadyDisposedResult: Try[Unit] = Success(())
    override protected def doUpdate(): Unit = {
      doClearTracking()
    }
  }

  private[tracking] override def notifyQueueIdle(lastAction: DependencyTrackerAction[_]): Unit = {
    // We'll enqueue a delayed cleanup every time the queue goes idle.
    // It does very little work if there is no cleanup required.
    maybeScheduleCleanup(lastAction)
  }

  private def runCleanupNow(triggeredBy: TrackingGraphCleanupTrigger): Unit =
    queue.maybeRunCleanupNow(createTrackingGraphCleanupAction(None, CleanupScheduler.NoInterruption, triggeredBy))
}

object DependencyTrackerRoot {
  private[tracking] val log = msjava.slf4jutils.scalalog.getLogger(getClass)
  private[this] val roots = new mutable.WeakHashMap[DependencyTrackerRoot, Unit]()

  private def recordRoot(root: DependencyTrackerRoot) = roots.synchronized {
    roots.update(root, ())
  }
  private[tracking] def removeRoot(ts: DependencyTrackerRoot): Unit = roots.synchronized {
    roots.remove(ts)
  }
  // we can get nulls in roots.keys.iterator as the ref can be gced during operation
  private[tracking] def rootsList = roots.synchronized {
    roots.keys.toList.filter { _ ne null }
  }

  // used in tests to make sure no roots remain between tests
  @deprecating("only for test access to make sure no roots remain between tests")
  private[optimus] def TEST_ONLY_disposeAllRoots(): Unit = {
    // we do not want to hold the roots lock whilst we wait for the
    // individual roots to dispose.  Ths can deadlock if anybody requires
    // the roots lock for other actions.
    val toDispose = roots.synchronized {
      roots.keySet.toList
    }

    toDispose.foreach { _.dispose() }
  }

  // called in client's ProcessMemoryWatcher and in core tools (GCMonitor, GCNative)
  def runCleanupNow(triggeredBy: TrackingGraphCleanupTrigger): Unit =
    rootsList.foreach(_.runCleanupNow(triggeredBy))

  def getAllWorkCount = rootsList.flatMap(walkChildren(_)).map(_.getWorkCount).sum

  def getAllWorkInfo(includeUTracks: Boolean): String = {
    val sb = new PrettyStringBuilder
    sb.appendln("Work info for all dependency tracker queues")
    val depTrackQueues = rootsList
      .flatMap(walkChildren(_))
      .map(_.queue)
      .distinct
    if (depTrackQueues.isEmpty) {
      sb.appendln("No dependency tracker queues found")
    }
    depTrackQueues.foreach { depTrackQueue =>
      sb.appendDivider()
      depTrackQueue.getQueueWorkInfo(sb, includeUTracks)
      sb.appendln("")
    }
    sb.toString
  }

  // Snap statistics from all dependency tracker queues
  def snap(): Map[String, QueueStats] =
    rootsList
      .flatMap(walkChildren(_))
      .map(_.queue)
      .distinct // multiple trackers might share a queue
      .groupBy(queue => queue.tracker.name)
      // we sum up the counts for all the queues with the same name
      .map { case (k, v) => k -> v.foldLeft(QueueStats.zero)(QueueStats.accumulate) }

  def ttrackStatsForAllRoots(cleanup: Boolean = false): Map[DependencyTrackerRoot, TTrackStats] = {
    rootsList.map { root =>
      val promise = Promise[TTrackStats]()
      root.getTTrackStats(cleanup, (t) => promise.complete(t))
      root -> Await.result(promise.future, 1 minute)
    }.toMap
  }

  private def walkChildren(tracker: DependencyTracker): collection.Seq[DependencyTracker] = {
    if (tracker.children.isEmpty) tracker :: Nil // leaf
    else tracker +: tracker.children.flatMap(walkChildren)
  }

  private[tracking] def childListForRoot(root: DependencyTrackerRoot): collection.Seq[DependencyTracker] =
    walkChildren(root)

  /**
   * Callable from evaluate in Debugger or in graph console It's more like a placeholder, while debugging feel free to
   * modify this function to do whatever you want with Edit & Continue this should help you avoid painful restarts
   */
  def debugGeneric(cmd: Int): Unit = {}

  /**
   * Clears all scenario-dependent caches, drops all tracking information, and invalidates all outputs (which will
   * typically cause the user to request full recomputation).
   *
   * Designed for use in profiler tools after dependency tracking configuration has been changed.
   */
  @deprecating("not really deprecated, but only for use in profiler")
  private[optimus] def clearCacheAndTracking(cause: ClearCacheCause): Unit = {
    // no need to clear SI cache since nodes in there definitely don't depend on tweak tracking
    Caches.clearCaches(
      cause,
      includeSiGlobal = false,
      includeGlobal = true,
      includeNamedCaches = true,
      includePerPropertyCaches = true)
    // now tell every root to clear all tracking and outputs
    rootsList.foreach { r =>
      // call via the async API because when the profiler calls us, it's not on a graph thread
      val p = Promise[Unit]()
      r.clearAllTrackingAsync(r => p.complete(r))
      Await.result(p.future, 1 minute)
    }
  }

  private class DependencyTrackerActionSummaryStats(val actionType: Class[_ <: DependencyTrackerAction[_]]) {
    // we dont want this globally, just for the trace
    import scala.jdk.CollectionConverters._

    val count = new AtomicLong
    val size = new AtomicLong
    private class DependencyTrackerActionStats {
      val totalDurationNs = new AtomicLong
      override def toString: String = s"${totalDurationNs}"
    }
    private val times = new util.EnumMap[DependencyTrackerActionProgress, DependencyTrackerActionStats](
      classOf[DependencyTrackerActionProgress])
    for (state <- DependencyTrackerActionProgress.values) {
      times.put(state, new DependencyTrackerActionStats)
    }
    def add(progress: DependencyTrackerActionProgress, duration: Long): Unit = {
      times.get(progress).totalDurationNs.addAndGet(duration)
    }
    override def toString: String = s"for ${actionType.getSimpleName} /$count = $times"
    def stats(reset: Boolean): String = {
      val count = this.count.get()
      val size = this.size.get()
      if (reset) {
        this.count.addAndGet(-count)
        this.size.addAndGet(-size)
      }
      val sb = new StringBuilder(s"for ${actionType.getSimpleName},$count,$size")
      for ((k, v) <- times.asScala) {
        val ns = v.totalDurationNs.get()
        if (reset) v.totalDurationNs.addAndGet(-ns)
        sb.append(s",${ns / 1000 / 1000.0},${if (count == 0) "" else ns / 1000 / 1000.0 / count}")
      }
      sb.toString
    }
  }
  private object DependencyTrackerActionSummaryStats {
    def statsHeading: String = {
      val sb = new StringBuilder(s"Name, count, size")
      for (name <- DependencyTrackerActionProgress.values) {
        sb.append(s",${name} total ms, ${name} ave ms")
      }
      sb.toString
    }

  }
  private val summaryStats =
    new ConcurrentHashMap[Class[_ <: DependencyTrackerAction[_]], DependencyTrackerActionSummaryStats]()
  private val counter = new AtomicInteger(0)
  private[tracking] def commandCreated(command: DependencyTrackerAction[_]): Unit = {
    // optimise for common case where the value is in the map
    val statsInitial = summaryStats.get(command.getClass)
    val stats: DependencyTrackerActionSummaryStats =
      if (statsInitial ne null) statsInitial
      else {
        val replace = new DependencyTrackerActionSummaryStats(command.getClass)
        val existing = summaryStats.putIfAbsent(command.getClass, replace)
        if (existing eq null) replace else existing
      }
    stats.count.incrementAndGet()
  }
  private[tracking] def recordSize(command: DependencyTrackerAction[_], size: Int): Unit = {
    command.size = size
    summaryStats.get(command.getClass).size.addAndGet(size)
  }
  private[tracking] def commandProgress(
      command: DependencyTrackerAction[_],
      progress: DependencyTrackerActionProgress,
      duration: Long): Unit = {
    // we dont want this globally, just for the trace
    import scala.jdk.CollectionConverters._
    summaryStats.get(command.getClass).add(progress, duration)
    if ((progress eq DependencyTrackerActionProgress.AFTER_CALLBACK) && counter.incrementAndGet() % 100 == 0) {
      log.info(s"SummaryStats - total completed $counter")
      for (value <- summaryStats.values.asScala) {
        log.info(s"SummaryStats - ${value.stats(false)}")
      }
    }
  }

  private[optimus] val defaultRootName = "root"

  def apply(scenarioState: ScenarioState): DependencyTrackerRoot = apply(scenarioState.scenarioStack)

  def apply(
      parentScenarioStack: ScenarioStack = EvaluationContext.scenarioStack,
      appletName: String = ""): DependencyTrackerRoot =
    new DependencyTrackerRoot(parentScenarioStack, CleanupScheduler.SharedTimedScheduler, appletName)

  // used in testing
  private[tracking] def apply(scheduler: TimedScheduler) =
    new DependencyTrackerRoot(EvaluationContext.scenarioStack, scheduler, appletName = "")
  /*
   * API for evaluating tweak lambdas in multiple scenarios and adding tweaks to each scenario (in an atomic action)
   */
  type TweakLambda = AsyncFunction0[collection.Seq[Tweak]]
}

/**
 * A weak transient reference to the DependencyTrackerRoot. This allows us to refer to the root iff we are in the same
 * process and it has not been disposed, without preventing serialization or disposal.
 */
private[optimus] final class DependencyTrackerRootWeakReference(
    @transient val ref: WeakReference[DependencyTrackerRoot])
    extends Serializable {
  def get: DependencyTrackerRoot = {
    if (ref eq null)
      throw new GraphInInvalidState(
        "No DependencyTrackerRoot weak ref found (are you trying to use givenOverlay on a distributed snapshot?)")
    val root = ref.get
    if (root eq null)
      throw new GraphInInvalidState(
        "DependencyTrackerRoot weak ref was cleared (are you trying to use givenOverlay on a disposed scenario?)")
    root
  }
}

import optimus.graph.tracking.DependencyTrackerRootWeakReferenceHelper.currentProperty
private[tracking] object DependencyTrackerRootWeakReference extends FakeModuleEntity(currentProperty :: Nil) {
  val Empty: DependencyTrackerRootWeakReference = new DependencyTrackerRootWeakReference(null)

  def create(root: DependencyTrackerRoot): DependencyTrackerRootWeakReference = {
    require(root ne null)
    new DependencyTrackerRootWeakReference(new WeakReference(root))
  }

  // note that this node is tweaked by DependencyTracker initialization to the appropriate value
  val current$newNode: PropertyNode[DependencyTrackerRootWeakReference] =
    new AlreadyCompletedPropertyNode[DependencyTrackerRootWeakReference](Empty, this, currentProperty)

  /**
   * Looks up the DependencyTrackerRootWeakReference for current ScenarioStack - will only work for
   * TrackingScenarioStacks, their children, and snapshots or other copies thereof
   */
  def forStack: DependencyTrackerRootWeakReference = current$newNode.lookupAndGet
}

private[tracking] object DependencyTrackerRootWeakReferenceHelper {
  // note that this is marked as TWEAKABLE so that tweaks will be looked up, but also as DONT_TRACK_FOR_INVALIDATION
  // since we never change the tweaked value and don't want the expense of a huge ttrack graph
  val currentProperty = new PropertyInfo("current", ScenarioReferenceTrackingHelper.flags)
}

private[tracking] trait TraversalIdSource {
  private[tracking] def withFreshTraversalId(func: Integer => Integer): Unit
}

trait TrackingGraphCleanupTrigger {
  override def toString: String = getClass.getSimpleName
}

object AppletNameTag extends ForwardingPluginTagKey[String]
