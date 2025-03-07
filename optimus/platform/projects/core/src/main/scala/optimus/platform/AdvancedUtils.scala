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

import com.sun.management.GarbageCollectionNotificationInfo
import msjava.base.util.uuid.MSUuid
import msjava.slf4jutils.scalalog.getLogger
import optimus.config.scoped.ScopedSchedulerPlugin
import optimus.core.MonitoringBreadcrumbs
import optimus.core.MonitoringBreadcrumbs.InGivenOverlayKey
import optimus.core.MonitoringBreadcrumbs.InGivenOverlayTag
import optimus.core.needsPlugin
import optimus.graph.CompletableNode
import optimus.graph.DiagnosticSettings.getBoolProperty
import optimus.graph.GraphInInvalidState
import optimus.graph._
import optimus.graph.cache.CacheFilter
import optimus.graph.cache.Caches
import optimus.graph.cache.CauseDisposePrivateCache
import optimus.graph.cache.ClearCacheCause
import optimus.graph.cache.NCPolicy
import optimus.graph.cache.NCSupport
import optimus.graph.cache.NodeCCache
import optimus.graph.cache.UNodeCache
import optimus.graph.tracking.SnapshotScenarioStack
import optimus.platform.PluginHelpers.toNode
import optimus.platform.PluginHelpers.toNodeFactory
import optimus.platform.annotations.closuresEnterGraph
import optimus.platform.annotations.nodeLift
import optimus.platform.annotations.nodeLiftByName
import optimus.platform.annotations.nodeSync
import optimus.platform.annotations.nodeSyncLift
import optimus.platform.inputs.NodeInputs.ScopedSINodeInput
import optimus.platform.inputs.loaders.Loaders
import optimus.platform.inputs.loaders.OptimusNodeInputStorage
import optimus.platform.inputs.loaders.OptimusNodeInputStorage.OptimusStorageUnderlying
import optimus.platform.inputs.loaders.ProcessSINodeInputMap
import optimus.platform.inputs.registry.ProcessGraphInputs
import optimus.platform.storable.Entity
import optimus.platform.throttle.LiveThrottleLimiter
import optimus.platform.throttle.SimpleThrottleLimiter
import optimus.platform.throttle.Throttle
import optimus.platform.throttle.ThrottleLimiter
import optimus.ui.ScenarioReference
import optimus.ui.ScenarioReferencePropertyHelper.currentSnapshotScenarioProp
import optimus.utils.AdvancedUtilsMacros
import optimus.utils.SystemFinalization

import java.io.File
import java.io.InputStream
import java.lang.management.ManagementFactory
import java.lang.{Long => JLong}
import java.time.Instant
import javax.management.Notification
import javax.management.NotificationEmitter
import javax.management.NotificationListener
import javax.management.openmbean.CompositeData
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.Try

object AdvancedUtils {

  def createUuidOffGraphForTesting: String = {
    EvaluationContext.verifyOffGraph(ignoreNonCaching = true)
    new MSUuid().asString()
  }

  /** Instances of this class can be conveniently used as key in the plugin tags map within the scenario stack */
  trait NamedPluginTag { self: ForwardingPluginTagKey[_] =>
    def name: String
  }

  /** Throws if currently executing on graph and it needs to be RT */
  def verifyOffGraph(): Unit = EvaluationContext.verifyOffGraph()

  private val log = getLogger(this.getClass)

  trait MemoryManager {
    def maxNativeHeap: Long
    def usedNativeHeap: Long

    def maxJvmHeap: Long
    def usedJvmHeap: Long

    def doGcAndWait(): Unit
    def doFinalizeAndWait(): Unit
    def dumpHeap(fileNamePrefix: String, live: Boolean): Unit
  }

  trait CacheManager {
    def trimCaches(cause: ClearCacheCause, trimRatio: Double, level: Int, preferNative: Boolean): Unit
  }

  object OptimusCacheManager extends CacheManager {

    val CleanupOnly = 0
    val GlobalNonSI = 1
    val GlobalIncludingSI = 2
    val Everything = 3

    override def trimCaches(
        cause: ClearCacheCause,
        suppliedCacheTrimRatio: Double,
        level: Int,
        trimNativeFirst: Boolean): Unit = {
      GCNative.invokeRegisteredCleaners(CleanupOnly)

      // TODO (OPTIMUS-58467): implement proper support for trimNativeFirst, to make trimming triggered by native heap much more effective:
      // implement clearOldest with filter and ratio, and evict nodes tagged as holding native objects first.
      // Fall back to evicting arbitrary nodes if there were not enough tagged nodes

      // TODO (OPTIMUS-58467): merge this logic with existing clearCache(level) routine, to reduce the amount of code and provide unified behaviour
      val cacheTrimRatio =
        if (ProcessGraphInputs.EnableXSFT.currentValueOrThrow()) Math.min(1, suppliedCacheTrimRatio * 2)
        else suppliedCacheTrimRatio
      Caches.clearOldestForAllSSPrivateCache(cacheTrimRatio, cause)
      if (level >= GlobalNonSI) {
        val removed = UNodeCache.global.clearOldest(cacheTrimRatio, cause).removed
        log.info(
          s"After removing $removed oldest nodes from global cache, ${UNodeCache.global.getSize} remain, of max size ${UNodeCache.global.getMaxSize}")
        GCNative.invokeRegisteredCleaners(GlobalNonSI)
      }

      if (level >= GlobalIncludingSI) {
        val removed = UNodeCache.siGlobal.clearOldest(cacheTrimRatio, cause).removed
        log.info(
          s"After removing $removed oldest nodes from SI global cache, ${UNodeCache.siGlobal.getSize} remain, of max size ${UNodeCache.siGlobal.getMaxSize}")
        GCNative.invokeRegisteredCleaners(GlobalIncludingSI)
      }

      if (level >= Everything) {
        log.info(s"Clearing all registered caches")
        Caches.clearAllCaches(cause, includeSI = true, includePropertyLocal = true)
        GCNative.invokeRegisteredCleaners(Everything)
      }
    }
  }

  object OracleJvmAndGCNativeMemoryManager extends MemoryManager with NotificationListener {

    val maxGcWaitMs: JLong = JLong.getLong("optimus.memory.maxGcWaitMs", 5000)
    val finalizationWaitMs: JLong = JLong.getLong("optimus.memory.finalizationWaitMs", 5000)

    val oldGenPoolNames: Set[String] = Set("PS Old Gen", "CMS Old Gen", "G1 Old Gen")

    private val oldGenPool =
      ManagementFactory.getMemoryPoolMXBeans.asScala.filter(pool => oldGenPoolNames.contains(pool.getName)).head

    ManagementFactory.getGarbageCollectorMXBeans.asScala
      .collect { case n: NotificationEmitter => n }
      .foreach(n => n.addNotificationListener(this, null, null))

    // NOTE: we are replicating the default GCNative logic which calculates the max native heap size at which we exit as watermark * 1.8
    // however we do it once on first use, because later in the life of the process current GCNative implementation can move watermark closer to max
    // TODO (OPTIMUS-58467): move max logic from C++ to java side in GCNative, and get the max directly instead of trying to replicate
    private val maxNativeHeapAtStart: Long =
      if (GCNative.loaded) GCNative.getNativeWatermark / 10 * 18 else Long.MaxValue

    @volatile var waitingForExplicitGc: Boolean = false

    override def maxNativeHeap: Long = maxNativeHeapAtStart
    override def usedNativeHeap: Long = if (GCNative.loaded) GCNative.getNativeAllocation else 0
    // TODO (OPTIMUS-58467): can be -1 if undefined - handle this? Actually any prod process should have the max set, and it's unclear what can be done if it isn't
    override def maxJvmHeap: Long = oldGenPool.getUsage.getMax
    override def usedJvmHeap: Long = oldGenPool.getUsage.getUsed

    override def doGcAndWait(): Unit = {
      this.synchronized {
        if (!waitingForExplicitGc) { // only request new GC if we are not waiting for one already
          waitingForExplicitGc = true
          log.info("Initiating explicit GC")
          System.gc()
        }

        // System.gc can be synchronous or not, depending on GC parameters and runtime situation, so we subscribe to GC notifications and
        // wait for GC notification handler to clear the waiting flag.
        // We don't want to cycle here (as usually recommended for wait/notify), because the GC might not actually occur even if we have requested it, and we would be cycling forever
        // this is why we specify a timeout and are ok with spurious wake ups if they occur.
        // The idea is that we will proceed when the GC actually completes, or after a fixed timeout, whichever is faster -
        // supposedly it should be better than always sleeping for fixed period and wasting time, but this needs some stats
        if (waitingForExplicitGc) {
          try {
            log.info(s"Waiting for explicit GC to finish or max wait of $maxGcWaitMs ms to lapse")
            this.wait(maxGcWaitMs)
            if (waitingForExplicitGc) {
              log.warn(
                s"Woken up but notification of explicit GC completion is still not received - either because of timeout or a spurious wake up, aborting wait")
              waitingForExplicitGc = false
            }
          } catch {
            case e: InterruptedException =>
              log.warn("Interrupted while waiting for GC to finish, aborting wait", e)
              waitingForExplicitGc = false
          }
        }
      }
    }

    override def doFinalizeAndWait(): Unit = {
      log.info(s"Count of objects pending finalization is ${SystemFinalization.getObjectPendingFinalizationCount}")
      if (SystemFinalization.getObjectPendingFinalizationCount > 0) {
        SystemFinalization.runFinalizers()
        log.info(
          s"After System.runFinalization, count of objects pending finalization is ${SystemFinalization.getObjectPendingFinalizationCount}")
        // System.runFinalization is supposed to be synchronous, and return only when finalization completes - at least wording in javadoc suggests so.
        // There seems to be no MBean notification about finalization, so best we can do to find out is check if there is still anything pending,
        // and if yes - just sleep for configurable period of time and check again.
        if (SystemFinalization.getObjectPendingFinalizationCount > 0) {
          Thread.sleep(finalizationWaitMs / 2)
          log.info(
            s"After ${finalizationWaitMs / 2} ms, count of objects pending finalization is ${SystemFinalization.getObjectPendingFinalizationCount}")

          Thread.sleep(finalizationWaitMs / 2)
          log.info(
            s"After $finalizationWaitMs ms, count of objects pending finalization is ${SystemFinalization.getObjectPendingFinalizationCount}")
        }
      }
    }

    override def dumpHeap(fileNamePrefix: String, live: Boolean): Unit = GCNative.dumpHeap(fileNamePrefix, live)

    // see http://www.fasterj.com/articles/oraclecollectors1.shtml
    val oldGenGcNames: Set[String] =
      Set("PS MarkSweep", "MarkSweepCompact", "ConcurrentMarkSweep", "G1 Mixed Generation")

    override def handleNotification(notification: Notification, handback: scala.Any): Unit = {
      this.synchronized {
        if (notification.getType == GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION) {
          val info = GarbageCollectionNotificationInfo.from(notification.getUserData.asInstanceOf[CompositeData])
          if (info.getGcCause.contains("System.gc") && oldGenGcNames.contains(info.getGcName)) {
            val usedMemoryBefore = info.getGcInfo.getMemoryUsageBeforeGc.asScala
              .filter { p =>
                oldGenPoolNames.contains(p._1)
              }
              .values
              .map(_.getUsed)
              .sum
            val usedMemoryAfter = info.getGcInfo.getMemoryUsageAfterGc.asScala
              .filter(p => oldGenPoolNames.contains(p._1))
              .values
              .map(_.getUsed)
              .sum

            log.info(
              s"${info.getGcName} caused by ${info.getGcCause} finished, reported duration is ${info.getGcInfo.getDuration} ms," +
                s" old gen before GC $usedMemoryBefore, after GC $usedMemoryAfter waitingForExplicitGc is $waitingForExplicitGc")
            waitingForExplicitGc = false
            this.notifyAll()
          }
        }
      }
    }
  }

  /**
   * Regulates if we are going to do anything at all when ensureHeapAvailable is called
   *
   * We need it as a var to be able to switch during JVM life, especially for tests
   */
  var allowSyncMemoryTrimming: Boolean = getBoolProperty("optimus.memory.allowSyncMemoryTrimming", false)

  val dumpHeapOnFailure: Boolean = getBoolProperty("optimus.memory.dumpHeapOnFailure", false)

  val DefaultTargetJvmHeapRatio: Double = JLong
    .getLong("optimus.memory.targetJvmHeapRatioPercent", 90)
    .toDouble / 100.0

  val DefaultTargetNativeHeapRatio: Double = JLong
    .getLong("optimus.memory.targetNativeHeapRatioPercent", 55)
    .toDouble / 100.0

  val DefaultCacheTrimRatio: Double = JLong.getLong("optimus.memory.cacheTrimmingRatioPercent", 25).toDouble / 100.0

  private def doProgressiveTrimming(
      cause: ClearCacheCause,
      attemptNo: Int,
      cacheTrimRatio: Double,
      nativeHeapNeedsTrimming: Boolean,
      memoryManager: MemoryManager,
      cacheManager: CacheManager): Unit = {
    // how many times to try trimming before resorting to clearing everything. E.g. if we are told to trim 25% each time, we will try 4 times, plus 1st time we will try just gc/finalize
    // NOTE: 4 times 25% is not equivalent to completely clearing the global cache, because each time we remove 25% of _remaining_ number
    val numAttemptsTillClearAll = (1.0 / cacheTrimRatio + 1).toInt

    val trimmingLevel: Int =
      if (attemptNo > numAttemptsTillClearAll) OptimusCacheManager.Everything
      else if (attemptNo > numAttemptsTillClearAll / 2)
        OptimusCacheManager.GlobalIncludingSI // half way through, start trimming the SI cache as well
      else if (attemptNo > 1) OptimusCacheManager.GlobalNonSI
      else OptimusCacheManager.CleanupOnly

    cacheManager.trimCaches(cause, cacheTrimRatio, trimmingLevel, nativeHeapNeedsTrimming)
    memoryManager.doGcAndWait()

    if (nativeHeapNeedsTrimming) // only bother to finalize explicitly if we have actual native heap pressure
      memoryManager.doFinalizeAndWait()
  }

  var numCalls: Long = 0
  var numCallsWithTrimming = 0
  var totalDurationMs: Long = 0
  var maxDurationMs: Long = 0
  var maxAttempts: Long = 0
  var totalAttempts: Long = 0
  var totalDurationWithTrimming: Long = 0

  /**
   * Queries JVM and native heap stats, and attempts to progressively trim various Optimus caches as necessary to bring
   * the free memory to target ratios. It also attempts to wait until GC and finalizer catch up with actually removing
   * the objects evicted from caches
   * @return
   *   true if target heap ratios were reached, false otherwise
   */
  def ensureHeapAvailable(
      cause: ClearCacheCause,
      targetJvmHeapRatio: Double = DefaultTargetJvmHeapRatio,
      targetNativeHeapRatio: Double = DefaultTargetNativeHeapRatio,
      cacheTrimRatio: Double = DefaultCacheTrimRatio,
      memoryManager: MemoryManager = OracleJvmAndGCNativeMemoryManager,
      cacheManager: CacheManager = OptimusCacheManager,
      forceSyncCacheTrimming: Boolean = false): (Boolean, Int) = {
    @tailrec
    def checkAndTrim(cause: ClearCacheCause, attemptNo: Int, maxAttempts: Int): (Boolean, Int) = {
      val maxJvmHeap = memoryManager.maxJvmHeap
      val usedJvmHeap = memoryManager.usedJvmHeap
      val currJvmHeapRatio = usedJvmHeap.toDouble / maxJvmHeap
      val jvmHeapNeedsTrimming = currJvmHeapRatio > targetJvmHeapRatio
      if (jvmHeapNeedsTrimming)
        log.info(
          s"Old gen heap ratio is $currJvmHeapRatio, higher than threshold of $targetJvmHeapRatio (used $usedJvmHeap out of $maxJvmHeap), attempt #$attemptNo")

      val maxNativeHeap = memoryManager.maxNativeHeap
      val usedNativeHeap = memoryManager.usedNativeHeap
      val currNativeHeapRatio = usedNativeHeap.toDouble / maxNativeHeap
      val nativeHeapNeedsTrimming = currNativeHeapRatio > targetNativeHeapRatio
      if (nativeHeapNeedsTrimming)
        log.info(
          s"Native heap ratio is $currNativeHeapRatio, higher than threshold of $targetNativeHeapRatio (used $usedNativeHeap out of $maxNativeHeap), attempt #$attemptNo")

      if (jvmHeapNeedsTrimming || nativeHeapNeedsTrimming) {
        if (attemptNo > maxAttempts) {
          log.error(s"Failed to bring heap ratio(s) to specified target(s) after $maxAttempts attempts - giving up")
          if (dumpHeapOnFailure)
            memoryManager.dumpHeap(
              System.getProperty("java.io.tmpdir") + File.separator + "ensureHeapAvailable_failed",
              live = true)
          (false, attemptNo - 1)
        } else {
          doProgressiveTrimming(cause, attemptNo, cacheTrimRatio, nativeHeapNeedsTrimming, memoryManager, cacheManager)
          checkAndTrim(cause, attemptNo + 1, maxAttempts)
        }
      } else {
        if (attemptNo > 1)
          log.info(
            s"Success after ${attemptNo - 1} attempts, native heap ratio is $currNativeHeapRatio vs target of $targetNativeHeapRatio (used $usedNativeHeap out of $maxNativeHeap, " +
              s"old gen heap ratio is $currJvmHeapRatio, vs target of $targetJvmHeapRatio (used $usedJvmHeap out of $maxJvmHeap)")
        (true, attemptNo - 1)
      }
    }

    if (allowSyncMemoryTrimming || forceSyncCacheTrimming) {
      val startTime = System.currentTimeMillis()

      val (success, attemptsTaken) = AdvancedUtils.synchronized {

        try {
          // 1 attempt to just do gc/finalize, then 1/trimratio attempts to trim the global caches by specified %, then 1 final attempt to clear everything there is left to clear
          val numAttempts = (1.0 + 1.0 / cacheTrimRatio + 1.0).toInt
          checkAndTrim(cause, 1, numAttempts)
        } catch {
          case e: UnsatisfiedLinkError =>
            log.error(
              "Could not perform heap trimming due to the following exception. This usually means that GCNative is misconfigured. Will attempt to proceed to avoid breaking legacy apps",
              e
            )
            (false, 0)
        }
      }

      val timeSpent = System.currentTimeMillis() - startTime
      numCalls += 1
      totalDurationMs += timeSpent
      maxDurationMs = Math.max(maxDurationMs, timeSpent)

      if (attemptsTaken > 0) {
        numCallsWithTrimming += 1
        totalDurationWithTrimming += timeSpent
        totalAttempts += attemptsTaken
        maxAttempts = Math.max(maxAttempts, attemptsTaken)
        log.info(s"ensureHeapAvailable finished, trimAttemptsTaken=$attemptsTaken  timeSpent=$timeSpent ms, \n"
          + s"ensureHeapAvailableTotalCalls=$numCalls, \n"
          + s"ensureHeapAvailableTotalCallsWithTrimming=$numCallsWithTrimming (${numCallsWithTrimming * 100 / numCalls}%),\n"
          + "\n"
          + s"ensureHeapAvailableTotalTime=$totalDurationMs ms, \n"
          + s"ensureHeapAvailableTotalTimeWithTriming=$totalDurationWithTrimming ms, \n"
          + s"ensureHeapAvailableNoTrimming=${totalDurationMs - totalDurationWithTrimming} ms, \n"
          + "\n"
          + s"ensureHeapAvailableAvgTime=${totalDurationMs / numCalls} ms, \n"
          + s"ensureHeapAvailableAvgTimeWithTrimming=${totalDurationWithTrimming / numCallsWithTrimming} ms, \n"
          + (if (numCalls != numCallsWithTrimming)
               s"ensureHeapAvailableAvgTimeNoTrimming=${(totalDurationMs - totalDurationWithTrimming) / (numCalls - numCallsWithTrimming)} ms, \n"
             else "")
          + s"ensureHeapAvailableMaxTime=$maxDurationMs ms, \n"
          + "\n"
          + s"ensureHeapAvailableMaxAttempts=$maxAttempts, \n"
          + s"ensureHeapAvailableAvgAttempts=${totalAttempts.toDouble / numCallsWithTrimming}, \n")
      }

      (success, attemptsTaken)
    } else
      (true, 0)
  }

  object TargetHeapRatiosTag extends ForwardingPluginTagKey[TargetHeapRatiosData] {}

  final case class TargetHeapRatiosData(
      targetJvmHeapRatio: Double,
      targetNativeHeapRatio: Double,
      cacheTrimRatio: Double)

  private val defaultTargets =
    TargetHeapRatiosData(DefaultTargetJvmHeapRatio, DefaultTargetNativeHeapRatio, DefaultCacheTrimRatio)

  def targetHeapRatiosFromTagOrDefault(node: NodeTask): TargetHeapRatiosData = {
    val tagData = node.scenarioStack().findPluginTag(TargetHeapRatiosTag)

    tagData match {
      case Some(targets: TargetHeapRatiosData) => targets
      case None                                => defaultTargets
      case other =>
        log.error(s"Unexpected data received for TargetHeapRatiosTag, expected TargetHeapRatiosData but got $other")
        defaultTargets
    }
  }

  /** Clears all caches optionally including SI caches and local caches */
  def clearCache(cause: ClearCacheCause, includeSI: Boolean, includeLocal: Boolean): Unit = {
    Caches.clearAllCaches(cause, includeSI, includeLocal)
    // if non existent setup is called, let's fail back to level 3
    val level = if (!(!includeSI && includeLocal)) GCNative.convertToLevel(includeSI, includeLocal) else 3
    GCNative.invokeRegisteredCleaners(level)
  }

  /**
   * Clears caches on given cleanup level: Level 0 cleanup: just run gc Level 1 cleanup: "clear all caches" Level 2
   * cleanup: clear all caches with si Level 3 cleanup: clear all caches with si and per-property caches. This also
   * calls the default callbacks
   */
  def clearCache(cause: ClearCacheCause, level: Int): Unit = {
    require(level >= 0)
    require(level < 4)
    val (cleanSi, cleanLocal) = GCNative.convertLevelToCleans(level)
    Caches.clearAllCaches(cause, cleanSi, cleanLocal)
    GCNative.invokeRegisteredCleaners(level)
  }

  /** Clears all cached nodes that have a native marker */
  def clearAllCachesWithNativeMarker(cause: ClearCacheCause): Unit = {
    Caches.clearAllCachesWithNativeMarker(cause)
  }

  /** Clears all cached nodes that retain an object with a finalizer or cleaner */
  def clearAllCachesWithFinalizers(
      cause: ClearCacheCause,
      includeCtor: Boolean,
      includeSI: Boolean,
      includeLocal: Boolean,
      seconds: Int): Long = {
    val caches = Caches.allCaches(
      includePerPropertyCaches = includeLocal,
      includeCtorGlobal = includeCtor,
      includeSiGlobal = includeSI
    )
    NCSupport.tagNativeNodes(caches.toArray[AnyRef], seconds)
    Caches.clearCaches(
      cause,
      includePerPropertyCaches = includeLocal,
      includeCtorGlobal = includeSI,
      includeSiGlobal = includeSI,
      filter = CacheFilter({ _.executionInfo().getHoldsNativeMemory() }, "NativeMarker")
    )
  }

  /**
   * Used when users want the codes to be included into temporal context permitted list
   *
   * @param comment
   *   description how why this block of codes need permitted list
   * @param f
   *   Node function to run in the permitted-listed block
   * @tparam T
   *   Return type of user code
   * @return
   */
  @nodeSync
  @nodeSyncLift
  final def temporalContextAddToPermittedList[T](comment: String)(@nodeLift @nodeLiftByName f: => T): T = needsPlugin
  // noinspection ScalaUnusedSymbol
  final def temporalContextAddToPermittedList$queued[T](comment: String)(f: Node[T]): Node[T] =
    temporalContextAddToPermittedList$newNode(comment)(f).enqueueAttached
  // noinspection ScalaUnusedSymbol
  final def temporalContextAddToPermittedList$withNode[T](comment: String)(f: Node[T]): T =
    temporalContextAddToPermittedList$newNode(comment)(f).get
  final private[this] def temporalContextAddToPermittedList$newNode[T](comment: String)(f: Node[T]) = {
    if (Settings.traceTemporalContextMigration) {
      // we need scenario private cache here because non-permitted nodes can't retrieve cached permitted nodes
      val ss = EvaluationContext.scenarioStack.withPluginTag(TemporalContextPermittedListTag, comment)
      val cache = new UNodeCache(
        s"${UNodeCache.scenarioStackPrivateCachePrefix}-temporalContextAddToPermitted-${ss.toShortString}",
        Settings.cacheSSPrivateSize,
        Settings.cacheConcurrency,
        cacheBatchSize = NodeCCache.defaultCacheBatchSize,
        cacheBatchSizePadding = NodeCCache.defaultCacheBatchSizePadding
      )
      runInPrivateCache(ss, cache, readFromOuterCaches = true, disposeOnCompleted = true)(f)
    } else {
      f.attach(EvaluationContext.scenarioStack)
      f
    }
  }

  /**
   * A temporal context permited version of given method. When it is used, the scenario applying process will be
   * permitted, but not the body of this block (i.e., f)
   *
   * @param comment
   *   description how why this scenario applying process need to be permitted
   * @param scenario
   *   new scenario to be applied to current scenario stack
   * @param f
   *   Node function to run
   * @tparam T
   *   Return type of user code
   * @return
   */
  @nodeSync
  @nodeSyncLift
  final def temporalContextAddToPermittedListGiven[T](comment: String)(scenario: Scenario)(
      @nodeLift @nodeLiftByName f: => T): T = needsPlugin
  // noinspection ScalaUnusedSymbol
  final def temporalContextAddToPermittedListGiven$queued[T](comment: String)(scenario: Scenario)(f: Node[T]): Node[T] =
    temporalContextAddToPermittedListGivenInternal(comment)(scenario)(f).enqueueAttached
  final def temporalContextAddToPermittedListGiven$withNode[T](comment: String)(scenario: Scenario)(f: Node[T]): T =
    temporalContextAddToPermittedListGivenInternal(comment)(scenario)(f).get

  final private def temporalContextAddToPermittedListGivenInternal[T](comment: String)(scenario: Scenario)(
      f: Node[T]) = {
    if (Settings.traceTemporalContextMigration) {
      temporalContextAddToPermittedList$newNode(comment) {
        new CompletableNode[T] {
          override def run(ec: OGSchedulerContext): Unit = {
            val nodeToRun =
              if (Settings.convertByNameToByValue && scenario.existsWithNested(_.hasReducibleToByValueTweaks))
                new ConvertByNameToByValueNode(scenarioStack, scenario, f, true)
              else {
                val ss = scenarioStack.createChild(scenario, f)
                // we remove the temporal context permitted tag here because it only applies to the tweaks, not to the body of the given block
                EvaluationContext.given(withoutTemporalContextPermitted(ss), f)
              }
            ec.enqueue(nodeToRun)
            nodeToRun.continueWith(this, ec)
          }
          override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
            completeFromNode(f, EvaluationContext.current)
          }
        }
      }
    } else EvaluationContext.given(scenario, f)
  }

  // remove TemporalContextPermittedListTag and private cache
  private[optimus] def withoutTemporalContextPermitted(scenarioStack: ScenarioStack): ScenarioStack = {
    if (Settings.traceTemporalContextMigration) {
      scenarioStack.withoutPluginTag(TemporalContextPermittedListTag).withoutPrivateCache
    } else scenarioStack
  }

  @nodeSync
  @nodeSyncLift
  final def withProvidedCache[T](
      cache: UNodeCache,
      readFromOuterCaches: Boolean = true,
      clearWhenCompleted: Boolean = false)(@nodeLift @nodeLiftByName f: => T): T =
    withProvidedCache$withNode(cache, readFromOuterCaches, clearWhenCompleted)(toNode(f _))
  // noinspection ScalaUnusedSymbol (compiler plugin forwards to this def)
  final def withProvidedCache$queued[T](cache: UNodeCache, readFromOuterCaches: Boolean, clearWhenCompleted: Boolean)(
      f: Node[T]): Node[T] =
    withProvidedCache$newNode(cache, readFromOuterCaches, clearWhenCompleted)(f).enqueueAttached
  // noinspection ScalaUnusedSymbol (compiler plugin forwards to this def)
  final def withProvidedCache$queued[T](c: UNodeCache, rfc: Boolean, cwc: Boolean, f: => T): NodeFuture[T] =
    withProvidedCache$queued(c, rfc, cwc)(toNode(f _))
  final def withProvidedCache$withNode[T](
      cache: UNodeCache,
      readFromOuterCaches: Boolean = true,
      clearWhenCompleted: Boolean = false)(f: Node[T]): T =
    withProvidedCache$newNode(cache, readFromOuterCaches, clearWhenCompleted)(f).get
  final def withProvidedCache$newNode[T](
      cache: UNodeCache,
      readFromOuterCaches: Boolean = true,
      clearWhenCompleted: Boolean = false)(f: Node[T]): Node[T] =
    runInPrivateCache(EvaluationContext.scenarioStack, cache, readFromOuterCaches, clearWhenCompleted)(f)

  // Wrap a node and all its children in a private cache.
  private[this] def runInPrivateCache[T](
      ss: ScenarioStack,
      cache: UNodeCache,
      readFromOuterCaches: Boolean,
      disposeOnCompleted: Boolean)(node: Node[T]): Node[T] = {
    val nodeToRun = new CompletableNode[T] {
      private var state = 0
      override def run(ec: OGSchedulerContext): Unit = {
        if (state == 0) {
          state = 1
          node.attach(scenarioStack())
          ec.enqueue(node)
          node.continueWith(this, ec)
        } else {
          if (disposeOnCompleted) {
            // Required for GC to drop it
            Caches.unregisterSharedCache(cache.getName)
            cache.clear(CauseDisposePrivateCache)
          }
          completeFromNode(node, ec)
        }
      }
    }
    nodeToRun.attach(ss.withPrivateProvidedCache(cache, readFromOuterCaches))
    nodeToRun
  }

  @nodeSync
  @nodeSyncLift
  final def givenScenarioOf[T](node: NodeTask)(@nodeLift @nodeLiftByName f: => T): T =
    givenScenarioOf$withNode(node)(toNode(f _))
  // noinspection ScalaUnusedSymbol (compiler plugin forwards to this def)
  final def givenScenarioOf$queued[T](node: NodeTask)(f: Node[T]): Node[T] =
    givenScenarioOf$newNode(node)(f).enqueueAttached
  // noinspection ScalaUnusedSymbol (compiler plugin forwards to this def)
  final def givenScenarioOf$withNode[T](node: NodeTask)(f: Node[T]): T = givenScenarioOf$newNode(node)(f).get
  final private[this] def givenScenarioOf$newNode[T](node: NodeTask)(f: Node[T]): Node[T] = {
    EvaluationContext.given(node.scenarioStack.withBatchScope(new BatchScope), f)
  }

  @nodeSync
  @nodeSyncLift
  final def givenFullySpecifiedScenario[T](scenario: Scenario, inheritSIParams: Boolean)(
      @nodeLift @nodeLiftByName f: => T): T =
    givenFullySpecifiedScenario$withNode(scenario, inheritSIParams)(toNode(f _))
  final def givenFullySpecifiedScenario$withNode[T](scenario: Scenario, inheritSIParams: Boolean)(f: Node[T]): T =
    givenFullySpecifiedScenarioWithInitialTime$newNode(scenario, null, inheritSIParams)(f).get
  final def givenFullySpecifiedScenario$queued[T](scenario: Scenario, inheritSIParams: Boolean)(
      f: Node[T]): NodeFuture[T] =
    givenFullySpecifiedScenarioWithInitialTime$newNode(scenario, null, inheritSIParams)(f).enqueueAttached
  final def givenFullySpecifiedScenario$queued[T](
      scenario: Scenario,
      inheritSIParams: Boolean,
      f: => T): NodeFuture[T] =
    givenFullySpecifiedScenarioWithInitialTime$newNode(scenario, null, inheritSIParams)(toNode(f _)).enqueueAttached

  @nodeSync
  @nodeSyncLift
  final def givenFullySpecifiedScenario[T](scenario: Scenario)(@nodeLift @nodeLiftByName f: => T): T =
    givenFullySpecifiedScenario$withNode(scenario)(toNode(f _))
  final def givenFullySpecifiedScenario$withNode[T](scenario: Scenario)(f: Node[T]): T =
    givenFullySpecifiedScenario$withNode(scenario, inheritSIParams = true)(f)
  final def givenFullySpecifiedScenario$queued[T](scenario: Scenario)(f: Node[T]): NodeFuture[T] =
    givenFullySpecifiedScenario$queued(scenario, inheritSIParams = true)(f)
  final def givenFullySpecifiedScenario$queued[T](scenario: Scenario, f: => T): NodeFuture[T] =
    givenFullySpecifiedScenario$queued(scenario, inheritSIParams = true)(toNode(f _))

  @nodeSync
  @nodeSyncLift
  final def givenFullySpecifiedScenarioWithInitialTime[T](scenario: Scenario, initialTime: Instant)(
      @nodeLift @nodeLiftByName f: => T): T =
    givenFullySpecifiedScenarioWithInitialTime$withNode(scenario, initialTime)(toNode(f _))
  // noinspection ScalaUnusedSymbol
  final def givenFullySpecifiedScenarioWithInitialTime$withNode[T](scenario: Scenario, initialTime: Instant)(
      f: Node[T]): T =
    givenFullySpecifiedScenarioWithInitialTime$newNode(scenario, initialTime, inheritSIParams = true)(f).get
  // noinspection ScalaUnusedSymbol
  final def givenFullySpecifiedScenarioWithInitialTime$queued[T](scenario: Scenario, initialTime: Instant)(
      f: Node[T]): Node[T] =
    givenFullySpecifiedScenarioWithInitialTime$newNode(scenario, initialTime, inheritSIParams = true)(f).enqueueAttached

  final private[this] def givenFullySpecifiedScenarioWithInitialTime$newNode[T](
      scenario: Scenario,
      initialTime0: Instant,
      inheritSIParams: Boolean)(f: Node[T]) = {
    val ss0 = EvaluationContext.scenarioStack
    val initSS =
      if (initialTime0 eq null) {
        if (ss0.isScenarioIndependent)
          throw new IllegalArgumentException(
            "You cannot use givenFullySpecifiedScenario in a scenario independent context. " +
              "Instead use givenFullySpecifiedScenarioWithInitialTime and specify initialTime")

        ss0.initialRuntimeScenarioStack
      } else ss0.env.initialRuntimeScenarioStack(ss0, initialTime0)

    // given that we are fully replacing the scenario there is no need to treat this new stack as SI (even if ss0 was
    // SI) because we will never be reading tweaks from ss0. There is also no point recording or tracking tweaks for
    // the same reason.
    val ss = initSS
      .createChild(scenario, f)

    val maybeInheritedSIParamsSS =
      if (inheritSIParams)
        ss.withNonScenarioPropertiesFrom(
          ss0,
          clearFlags = EvaluationState.ALL_SI_FLAGS | EvaluationState.TRACK_OR_RECORD_TWEAKS)
      else ss
    EvaluationContext.given(maybeInheritedSIParamsSS, f)
  }

  // format: off
  /**
   * Used in application code (specifically UI) to 'overlay' a scenario of tweaks on top of the current one (or some
   * other one). For example, the scenario structure on the left will be transformed as follows when givenOverlay(TS2b)
   * {} is called in scenario TS1a:
   *
   *     originalSS:         overlaySS:
   *
   *        TSR                 TSR
   *        / \                  |
   *     TS1   TS2       --->   TS1
   *     /     /\                |
   *  TS1a  TS2a TS2b           TS1a
   *                             |
   *                            TS2'  (TS2 TOSS)
   *                             |
   *                            TS2b' (TS2b TOSS)
   *
   * Any code in the block will be evaluated under the new overlay ScenarioStack, where TS2' and TS2b' are copies of
   * TS2b and its parent, TS2, transformed to BasicScenarioStacks. Only these need to be added to the overlay, as the
   * common parent of TS1a and TS2b is root.
   *
   * Invalidation in the original scenarios (ie, adding or removing tweaks) will be reflected in the overlaySS. This
   * works for byName and byValue tweaks.
   *
   * Limitations: the trackers must share the same DependencyTrackerRoot, and a parent cannot be overlayed onto a
   * tracker (this would be a no-op because common parent would be the scenario we are trying to overlay, and nothing
   * would be added).
   *
   * @param scenRef
   *   A reference to the DependencyTracker to overlay on top of current one
   * @param f
   *   Node function to run in the new scenario
   * @tparam T
   *   Return type of user code
   * @return
   */
  // format: on
  @nodeSync
  @nodeSyncLift
  final def givenOverlay[T](scenRef: ScenarioReference)(@nodeLift @nodeLiftByName f: => T): T =
    givenOverlay$withNode(scenRef)(toNode(f _))
  // noinspection ScalaUnusedSymbol (compiler plugin forwards to this def)
  final def givenOverlay$withNode[T](scenRef: ScenarioReference)(f: Node[T]): T = givenOverlay$newNode(scenRef)(f).get
  final def givenOverlay$queued[T](scenRef: ScenarioReference)(f: Node[T]): Node[T] =
    givenOverlay$newNode(scenRef)(f).enqueueAttached
  // noinspection ScalaUnusedSymbol (compiler plugin forwards to this def)
  final def givenOverlay$queued[T](scenRef: ScenarioReference, f: => T): NodeFuture[T] =
    givenOverlay$queued(scenRef)(toNode(f _))
  final private[this] def givenOverlay$newNode[T](scenRef: ScenarioReference)(f: Node[T]): Node[T] = {
    val (currentScenarioReference, ss) = verifyEvaluationPolicyAndGetCurrentScenario()

    if (currentScenarioReference.rootOfConsistentSubtree != scenRef.rootOfConsistentSubtree)
      throw new IllegalArgumentException(
        s"Cannot overlay scenario $scenRef in scenario $currentScenarioReference " +
          s"as it is in a different concurrent subtree")

    MonitoringBreadcrumbs.sendGivenOverlayCrumb(currentScenarioReference, scenRef)

    // If we are executing in a snapshot
    val overlaySS =
      if (!ss.isTrackingIndividualTweakUsage) ss.overlay(scenRef)
      else
        // Need to evaluate in overlaySS so that we see the combined tweaks of overlayTS overlayed on currentSS.
        // Note that it's fine to run this node directly (without acquiring an evaluate lock on the target DepTracker)
        // because we already checked above that we're in the same consistent subtree (i.e. we already have the
        // appropriate lock)
        ss.overlay(scenRef.getTracker)

    // currentScenario in the overlay will be defined as the overlay scenarioReference. We need to correct this by
    // overriding the tweak in the childmost scenario to refer to the entry-point scenarioStack (ie, the scenario that
    // the handler is bound to in UI)
    val correctScenarioOverlaySS = correctCurrentScenario(overlaySS, currentScenarioReference)
    addCurrentSnapshotScenarioTweakDependency(f)
    f.replace(correctScenarioOverlaySS.withPluginTag(InGivenOverlayKey, InGivenOverlayTag))
    f
  }

  private def addCurrentSnapshotScenarioTweakDependency[T](f: Node[T]): Unit =
    // [SEE_SCENARIO_REF_ARGUMENT] adding this as a dependency so that we do not get false cache reuse between calls
    // that take in scenario references (the references might have changed in meaning between calls)
    f.setTweakPropertyDependency(currentSnapshotScenarioProp.tweakMask())

  // [SEE_CURRENT_SCENARIO_REF]
  private def correctCurrentScenario(stack: ScenarioStack, current: ScenarioReference): ScenarioStack = {
    val currentScenRefTweak = SimpleValueTweak(ScenarioReference.current$newNode)(current)
    stack.createChild(Scenario(currentScenRefTweak), EvaluationContext.currentNode)
  }

  /** for evaluateIn and givenOverlay, which are currently not supported in SI or XS nodes */
  private def verifyEvaluationPolicyAndGetCurrentScenario(): (ScenarioReference, ScenarioStack) = {
    val currentScenarioRef =
      try { ScenarioReference.current }
      catch {
        // IllegalScenarioDependenceException is thrown in optimus.platform.ScenarioStack.getNode, when we look for a tweak
        // to currentScenario$newNode in our SI stack, since this node is tweakable. Re-throw more sensible exception here
        case e: IllegalScenarioDependenceException => throw new IllegalEvaluationInOtherScenarioInSIException(e)
      }

    val ss = EvaluationContext.scenarioStack
    if (ss.isRecordingTweakUsage)
      throw new IllegalEvaluationInOtherScenarioInXSException(ss.tweakableListener.trackingProxy.nodeName)
    (currentScenarioRef, ss)
  }

  /**
   * Simpler implementation of givenOverlay for the (most common?) use case of overlaying a child within the consistent
   * subtree or overlaying a parent from any of its children. Note that this means ScenarioReference.current will refer
   * to 'scenRef' while this evaluation runs (unlike in givenOverlay, where ScenarioReference.current will refer to the
   * ScenarioReference the handler is bound to). This results in more potential cache reuse between calls to evaluateIn.
   *
   * Limitations (for extension if required):
   *   1. Can only be used to evaluate in a child within the consistent subtree (so no need to take a different lock) or
   *      to evaluate in any parent of a child in its subtree
   *   1. Cannot be used in an SI node
   *   1. Cannot be used in an XS node
   *   1. Limited reuse with InBackground handlers, as in overlay (because we rely on a tweak to capture snapshotted
   *      state) [SEE_SNAPSHOT_REUSE]
   */
  // noinspection ScalaUnusedSymbol (matching overrides need scenRef and f)
  @nodeSync
  @nodeSyncLift
  final def evaluateIn[T](scenRef: ScenarioReference)(@nodeLift @nodeLiftByName f: => T): T =
    evaluateIn$withNode(scenRef)(toNode(f _))
  // noinspection ScalaUnusedSymbol,ScalaWeakerAccess
  final def evaluateIn$withNode[T](scenRef: ScenarioReference)(f: Node[T]): T = evaluateIn$newNode(scenRef)(f).get
  // noinspection ScalaUnusedSymbol
  final def evaluateIn$queued[T](scenRef: ScenarioReference)(f: Node[T]): Node[T] =
    evaluateIn$newNode(scenRef)(f).enqueueAttached
  final private[this] def evaluateIn$newNode[T](scenRef: ScenarioReference)(f: Node[T]): Node[T] = {
    val (currentScenarioReference, ss) = verifyEvaluationPolicyAndGetCurrentScenario()

    if (scenRef == currentScenarioReference) {
      log.warn(s"Called evaluateIn on same scenario as currentScenarioReference $scenRef, this is a no-op")
      f.replace(ss)
    } else {
      // is parent of not isConsistentParentOf (this is allowed because we always have an evaluate lock on all parents)
      val isParent = scenRef.isParentOf(currentScenarioReference)
      val isChild = scenRef.isConsistentDescendantOf(currentScenarioReference)
      if (!(isChild || isParent))
        throw new IllegalArgumentException(
          s"Cannot evaluateIn $scenRef as it is not a parent or a consistent child of $currentScenarioReference")

      if (ss.isTrackingIndividualTweakUsage)
        f.replace(scenRef.getTracker.scenarioStack)
      else { // InBackground step
        val newSS = if (isChild) {
          val snapshot = SnapshotScenarioStack.current(ss)
          val (scenario, evalInSnapshot) = snapshot.nestScenariosUpTo(scenRef)
          if ((evalInSnapshot eq null) || evalInSnapshot.ref != scenRef) {
            val msg =
              s"ScenarioReference $scenRef was not found in the snapshot created when your InBackground step ran. See logs for detail."
            val detail =
              s"ScenarioReference $scenRef was not found in the snapshot created when your InBackground step ran. " +
                "This is probably because it was created too late, and we can't evaluateIn that scenario in this inconsistent state. " +
                "Make sure all ScenarioReferences you intend to refer to from an evaluateIn running InBackground already " +
                s"exist at the point the InBackground step starts running. Current snapshot:\n${snapshot.prettyString}"
            log.error(detail)
            throw new IllegalArgumentException(msg)
          }
          ss.rootScenarioStack.fromScenarioAndSnapshot(scenario, evalInSnapshot)
        } else ss.findParentByScenarioReferenceId(scenRef)
        f.replace(newSS)
      }
    }
    addCurrentSnapshotScenarioTweakDependency(f)
    f
  }

  /**
   * Returns fully specified evaluation environment Note: updates the current node with dependencies of all the tweaks
   * in the current scenarioStack
   */
  def currentScenarioState(): ScenarioState = {
    val ec = EvaluationContext.poisonTweakDependencyMask()
    ec.scenarioStack.asScenarioState
  }

  // noinspection ScalaUnusedSymbol
  @nodeSync
  @nodeSyncLift
  final def givenFullySpecifiedScenario[T](scenarioState: ScenarioState, addScenario: Scenario = null)(
      @nodeLift @nodeLiftByName f: => T): T =
    givenFullySpecifiedScenario$withNode(scenarioState, addScenario)(toNode(f _))
  final def givenFullySpecifiedScenario$withNode[T](scenarioState: ScenarioState, addScenario: Scenario)(
      f: Node[T]): T =
    givenFullySpecifiedScenario$newNode(scenarioState, addScenario)(f).get
  final def givenFullySpecifiedScenario$queued[T](scenarioState: ScenarioState, addScenario: Scenario)(
      f: Node[T]): NodeFuture[T] =
    givenFullySpecifiedScenario$newNode(scenarioState, addScenario)(f).enqueueAttached
  final def givenFullySpecifiedScenario$queued[T](ss: ScenarioState, addScenario: Scenario, f: => T): NodeFuture[T] =
    givenFullySpecifiedScenario$queued(ss, addScenario)(toNode(f _))
  final def givenFullySpecifiedScenario$newNode[T](scenarioState: ScenarioState, addScenario: Scenario)(
      f: Node[T]): Node[T] = {
    if (!EvaluationContext.isInitialised) EvaluationContext.initializeWithoutRuntime()
    val newScenarioStack = scenarioState.scenarioStack
    val curScenarioStack = EvaluationContext.scenarioStack
    if (
      (curScenarioStack.tweakableListener ne newScenarioStack.tweakableListener) &&
      (newScenarioStack.tweakableListener ne NoOpTweakableListener)
    )
      throw new GraphException(
        s"Cannot restore state across different tracking contexts! \nContext 1: ${curScenarioStack.tweakableListener}\n Context 2: ${newScenarioStack.tweakableListener}"
      ) // Currently

    val newSS: ScenarioStack =
      newScenarioStack.withNonScenarioPropertiesFrom(curScenarioStack, clearFlags = EvaluationState.CONSTANT)
    val ss = if (addScenario eq null) newSS else newSS.createChild(addScenario, f)
    EvaluationContext.given(ss, f)
  }

  @nodeSync
  @nodeSyncLift
  final def givenWithNestedPluginTag[T, V <: AnyRef](key: NestedTag[V], tag: V, s: Scenario)(
      @nodeLift @nodeLiftByName f: => T): T =
    givenWithNestedPluginTag$withNode(key, tag, s)(toNode(f _))
  // noinspection ScalaUnusedSymbol
  final def givenWithNestedPluginTag$queued[T, V <: AnyRef](key: NestedTag[V], tag: V, s: Scenario)(
      f: Node[T]): Node[T] =
    givenWithNestedPluginTag$newNode(key, tag, s)(f).enqueueAttached
  // noinspection ScalaUnusedSymbol
  final def givenWithNestedPluginTag$queued[T](key: NestedTag[_], tag: AnyRef, s: Scenario, f: => T): NodeFuture[T] =
    givenWithNestedPluginTag$queued(key.asInstanceOf[NestedTag[AnyRef]], tag, s)(toNode(f _))
  final def givenWithNestedPluginTag$withNode[T, V <: AnyRef](key: NestedTag[V], tag: V, s: Scenario)(f: Node[T]): T =
    givenWithNestedPluginTag$newNode(key, tag, s)(f).get
  final private[this] def givenWithNestedPluginTag$newNode[T, V <: AnyRef](key: NestedTag[V], tag: V, s: Scenario)(
      f: Node[T]) = {
    val tagToInsert = EvaluationContext.scenarioStack.findPluginTag[V](key) map { v =>
      key.resolve(v, tag)
    } getOrElse tag
    val ss = EvaluationContext.scenarioStack.createChild(s, this)
    EvaluationContext.given(ss.withPluginTag(key, tagToInsert), f)
  }

  /**
   * simply marks the block as @impure (e.g. to prevent reordering) and runs it
   */
  @impure
  @nodeSync
  @nodeSyncLift
  def impure[T](@nodeLiftByName @nodeLift f: => T): T = f
  @impure
  // noinspection ScalaUnusedSymbol
  def impure$withNode[T](f: Node[T]): T = f.get
  @impure
  // noinspection ScalaUnusedSymbol
  def impure$queued[T](f: Node[T]): Node[T] = f.enqueue

  @nodeSync
  @nodeSyncLift
  final def givenWithPluginTag[T, P](key: PluginTagKey[P], tag: P, s: Scenario)(@nodeLift @nodeLiftByName f: => T): T =
    givenWithPluginTag$withNode(key, tag, s)(toNode(f _))
  final def givenWithPluginTag$queued[T, P](key: PluginTagKey[P], tag: P, s: Scenario)(f: Node[T]): Node[T] =
    givenWithPluginTag$newNode(key, tag, s)(f).enqueueAttached
  final def givenWithPluginTag$queued[T, P](key: PluginTagKey[P], tag: P, s: Scenario, f: => T): NodeFuture[T] =
    givenWithPluginTag$queued(key, tag, s)(toNode(f _))
  final def givenWithPluginTag$withNode[T, P](key: PluginTagKey[P], tag: P, s: Scenario)(f: Node[T]): T =
    givenWithPluginTag$newNode(key, tag, s)(f).get
  final private[this] def givenWithPluginTag$newNode[T, P](key: PluginTagKey[P], tag: P, s: Scenario)(f: Node[T]) = {
    val ss = EvaluationContext.scenarioStack.createChild(s, this)
    EvaluationContext.given(ss.withPluginTag(key, tag), f)
  }

  @nodeSync
  @nodeSyncLift
  final def givenWithPluginTags[T](kvs: collection.Seq[PluginTagKeyValue[_]], s: Scenario)(
      @nodeLift @nodeLiftByName f: => T): T = needsPlugin
  final def givenWithPluginTags$queued[T](kvs: collection.Seq[PluginTagKeyValue[_]], s: Scenario)(f: Node[T]): Node[T] =
    givenWithPluginTags$newNode(kvs, s)(f).enqueueAttached
  final def givenWithPluginTags$withNode[T](kvs: collection.Seq[PluginTagKeyValue[_]], s: Scenario)(f: Node[T]): T =
    givenWithPluginTags$newNode(kvs, s)(f).get
  final private[this] def givenWithPluginTags$newNode[T](kvs: collection.Seq[PluginTagKeyValue[_]], s: Scenario)(
      f: Node[T]) = {
    val ss = EvaluationContext.scenarioStack.createChild(s, f)
    EvaluationContext.given(ss.withPluginTags(kvs), f)
  }

  /**
   * Sets up the inner code to be sensitive to configured policies that specified targetPath
   */
  @nodeSync
  @nodeSyncLift
  final def withConfigTargetPath[T](targetPath: String)(@nodeLift @nodeLiftByName f: => T): T =
    withConfigTargetPath$withNode(targetPath)(toNode(f _))
  // noinspection ScalaUnusedSymbol
  final def withConfigTargetPath$queued[T](targetPath: String)(f: Node[T]): Node[T] =
    withConfigTargetPath$newNode(targetPath)(f).enqueueAttached
  final def withConfigTargetPath$withNode[T](targetPath: String)(f: Node[T]): T =
    withConfigTargetPath$newNode(targetPath)(f).get
  final private[this] def withConfigTargetPath$newNode[T](targetPath: String)(f: Node[T]) = {
    val ss = EvaluationContext.scenarioStack.withPathMask(NCPolicy.scopeMaskFromPath(targetPath))
    EvaluationContext.given(ss, f)
  }

  /**
   * Function that will merge node inputs from an input stream into the existing SS's scoped node input map and use that
   * as the new scoped node input map for your scenario stack
   *
   * This will crash if the input stream contains any process (non scoped) node inputs!!
   */
  @nodeSync
  @nodeSyncLift
  final def givenWithScopedNodeInputsFromFile[T](is: InputStream)(@nodeLift @nodeLiftByName f: => T): T =
    givenWithScopedNodeInputsFromFile$withNode(is)(toNode(f _))
  // noinspection ScalaUnusedSymbol
  final def givenWithScopedNodeInputsFromFile$queued[T](is: InputStream)(f: Node[T]): Node[T] =
    givenWithScopedNodeInputsFromFile$newNode(is)(f).enqueueAttached
  // noinspection ScalaUnusedSymbol
  final def givenWithScopedNodeInputsFromFile$withNode[T](is: InputStream)(f: Node[T]): T =
    givenWithScopedNodeInputsFromFile$newNode(is)(f).get
  final private[this] def givenWithScopedNodeInputsFromFile$newNode[T](is: InputStream)(f: Node[T]) = {
    val ss = EvaluationContext.scenarioStack
    val OptimusStorageUnderlying(scopedMap, processMap) =
      Loaders.inputStreamProperties(OptimusNodeInputStorage.empty, is).underlying
    if (processMap != ProcessSINodeInputMap.empty)
      throw new GraphInInvalidState("No process inputs should be present in your file!")
    EvaluationContext.given(
      ss.withFullySpecifiedScopedNodeInputMap(ss.siParams.nodeInputs.freeze.mergeWith(scopedMap)),
      f)
  }

  @nodeSync
  @nodeSyncLift
  final def givenWithScopedNodeInput[S, T](ni: ScopedSINodeInput[S], v: S)(@nodeLift @nodeLiftByName f: => T): T =
    needsPlugin
  // noinspection ScalaUnusedSymbol
  final def givenWithScopedNodeInput$queued[S, T](ni: ScopedSINodeInput[S], v: S)(f: Node[T]): Node[T] =
    givenWithScopedNodeInput$newNode(ni, v)(f).enqueueAttached
  // noinspection ScalaUnusedSymbol
  final def givenWithScopedNodeInput$withNode[S, T](ni: ScopedSINodeInput[S], v: S)(f: Node[T]): T =
    givenWithScopedNodeInput$newNode(ni, v)(f).get
  final private[this] def givenWithScopedNodeInput$newNode[S, T](ni: ScopedSINodeInput[S], v: S)(f: Node[T]) = {
    val ss = EvaluationContext.scenarioStack
    EvaluationContext.given(ss.withScopedNodeInput(ni, v), f)
  }

  @nodeSync
  @nodeSyncLift
  final def givenWithPlugin[T, P](info: NodeTaskInfo, plugin: ScopedSchedulerPlugin)(
      @nodeLift @nodeLiftByName f: => T): T =
    givenWithPlugin$withNode(info, plugin)(toNode(f _))
  // noinspection ScalaUnusedSymbol
  final def givenWithPlugin$queued[T, P](info: NodeTaskInfo, plugin: ScopedSchedulerPlugin)(f: Node[T]): Node[T] =
    givenWithPlugin$newNode(info, plugin)(f).enqueueAttached
  final def givenWithPlugin$withNode[T, P](info: NodeTaskInfo, plugin: ScopedSchedulerPlugin)(f: Node[T]): T =
    givenWithPlugin$newNode(info, plugin)(f).get
  final private[this] def givenWithPlugin$newNode[T, P](info: NodeTaskInfo, plugin: ScopedSchedulerPlugin)(
      f: Node[T]) = {
    val ss = EvaluationContext.scenarioStack
    val prevPlugins = ss.siParams.scopedPlugins
    val newPlugins = if (prevPlugins eq null) Map(info -> plugin) else prevPlugins + (info -> plugin)
    val newSIParams = ss.siParams.copy(scopedPlugins = newPlugins)
    EvaluationContext.given(ss.withSIParams(newSIParams), f)
  }

  // Add a nested comment - useful for logging/crumbs
  object Comment extends ForwardingPluginTagKey[String]
  @nodeSync
  @nodeSyncLift
  final def givenWithComment[T](comment: String)(@nodeLift @nodeLiftByName f: => T): T =
    needsPlugin
  // noinspection ScalaUnusedSymbol (compiler plugin forwards to this def)
  final def givenWithComment$queued[T](comment: String)(f: Node[T]): Node[T] =
    givenWithComment$newNode(comment)(f).enqueueAttached
  // noinspection ScalaUnusedSymbol (compiler plugin forwards to this def)
  final def givenWithComment$withNode[T](comment: String)(f: Node[T]): T =
    givenWithComment$newNode(comment)(f).get
  final private[this] def givenWithComment$newNode[T](comment: String)(f: Node[T]) = {
    val ss0 = EvaluationContext.scenarioStack
    val c = ss0.findPluginTag(Comment).fold(comment)(_ + ":" + comment)
    val kvs = PluginTagKeyValue(Comment, c) :: Nil
    val ss = EvaluationContext.scenarioStack.createChild(Scenario.empty, f)
    EvaluationContext.given(ss.withPluginTags(kvs), f)
  }

  @nodeSync
  @nodeSyncLift
  final def suspendAuditorCallbacks[T](@nodeLift @nodeLiftByName f: => T): T = needsPlugin
  // noinspection ScalaUnusedSymbol
  final def suspendAuditorCallbacks$queued[T](f: Node[T]): Node[T] = suspendAuditorCallbacks$newNode(f).enqueueAttached
  // noinspection ScalaUnusedSymbol
  final def suspendAuditorCallbacks$withNode[T](f: Node[T]): T = suspendAuditorCallbacks$newNode(f).get
  final private[this] def suspendAuditorCallbacks$newNode[T](f: Node[T]) = {

    /**
     * XSFT proxy cycles (which are recoverable) can cause real CircularReferenceExceptions with Auditor.
     * suspendAuditorCallbacks is used to disable auditing on code that is running as part of an Auditor callback
     * already. With XSFT, this causes a real cycle in as follows:
     * -> run some audited code
     * -> create XSFT proxy for evaluation of some audited node in SS[N]
     * -> enter Auditor callback
     * -> callback suspends auditor callbacks and enters a new given block, SS[N+1], e.g.
     * AdvancedUtils.suspendAuditorCallbacks { given(someTweakable := something) { .. } }
     * -> XSFT proxy in SS[N+1] waits to reuse XSFT proxy in SS[N]
     * -> But AUDITOR_CALLBACKS_DISABLED flag was not set on SS[N]! We re-enter the auditor callback and crash with CRE
     */
    val ec = EvaluationContext.poisonTweakDependencyMask()
    // [SEE_AUDIT_TRACE_SI] consider SI patch here? Currently in AuditTrace.visitBeforeRun
    EvaluationContext.given(ec.scenarioStack.withAuditorCallbacksDisabled, f)
  }

  @nodeSync
  @nodeSyncLift
  final def trackDependencies[T, K](c: TrackDependencyCollector[K])(@nodeLift @nodeLiftByName f: => T): (T, K) =
    trackDependencies$withNode(c)(toNode(f _))
  // noinspection ScalaUnusedSymbol
  final def trackDependencies$queued[T, K](c: TrackDependencyCollector[K])(f: Node[T]): Node[(T, K)] =
    trackDependencies$newNode(c)(f).enqueue
  // noinspection ScalaUnusedSymbol
  final def trackDependencies$withNode[T, K](c: TrackDependencyCollector[K])(f: Node[T]): (T, K) =
    trackDependencies$newNode(c)(f).get
  final private[this] def trackDependencies$newNode[T, K](c: TrackDependencyCollector[K])(f: Node[T]): Node[(T, K)] =
    new TDNode(c, f)

  object experimental {

    /**
     * Changes the target entity of an existing tweak.
     *
     * This method can only be used for tweaks to individual entity properties on vals/defs with no arguments. Any other
     * use should fail with an exception but may somehow fail silently as those cases are not yet tested.
     */
    def retargetInstanceTweak(twk: Tweak, ent: Entity): Option[Tweak] = {
      val key = twk.target.asInstanceOf[InstancePropertyTarget[_, _]].key
      val newKeyOp = Try {
        key.propertyInfo.asInstanceOf[PropertyInfo0[Entity, _]].createNodeKey(ent)
      }.toOption
      val newTarget = newKeyOp map (newKey => new InstancePropertyTarget(newKey))
      newTarget map twk.retarget
    }

    def gatherNullaryTweaks(ent: Entity): Iterable[Tweak] = {
      val props = ent.$info.properties.collect { case x: PropertyInfo0[_, _] =>
        x.asInstanceOf[PropertyInfo0[Entity, _]]
      }

      val nodes = props map { _.createNodeKey(ent) }
      val ss = EvaluationContext.scenarioStack
      nodes flatMap { n =>
        Option(ss.getTweakNoWhenClauseTracking(n))
      }
    }

    /**
     * returns the original result of the enclosed block plus the NodeExtendedInfo of type I (if it exists) else None.
     * note that the info will also propagate out of this block (i.e. we do not strip the info)
     */
    def interceptInfo[I <: NodeExtendedInfo]: InfoInterceptor[I] = new InfoInterceptor[I]
    // (only exists as a class so that we can curry the type parameters to make the user API nicer)
    class InfoInterceptor[I <: NodeExtendedInfo] protected[AdvancedUtils] {
      @nodeSync
      @nodeSyncLift
      def apply[T](@nodeLift @nodeLiftByName fn: => T)(implicit ct: ClassTag[I]): (T, Option[I]) =
        apply$withNode(toNode[T](fn _))(ct)
      def apply$withNode[T](fn: Node[T])(implicit ct: ClassTag[I]): (T, Option[I]) = apply$queued(fn).get
      def apply$queued[T](fn: Node[T])(implicit ct: ClassTag[I]): Node[(T, Option[I])] =
        new CompletableNodeM[(T, Option[I])] {
          override def run(ec: OGSchedulerContext): Unit = {
            fn.enqueue
            fn.continueWith(this, ec)
          }
          override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
            combineInfo(child, eq)
            if (child.isDoneWithException) completeWithException(child.exception(), eq)
            else {
              val infoOpt = child.info() match {
                case i: I => Some(i)
                case _    => None
              }
              completeWithResult((child.asInstanceOf[Node[T]].result, infoOpt), eq)
            }
          }
        }.enqueue
    }
  }

  /**
   * Returns result and the time it took to execute. Async friendly
   */
  final def timed[T](f: T): (Long, T) = macro AdvancedUtilsMacros.timedImpl

  /**
   * The only goal of this function is to introduce dependency between calling nanoTime() and consuming (sinking) result
   * Macro expands inline the code you see in Impl. Timing something is effectively measuring the side effect of
   * executing something (our reordering doesn't expect that) And it will reorder so that endTime = ... would move
   * before the call to f is ever made! This function is used to create a false dependency. It has to be public, because
   * once inlined it's was being called directly by user code
   */
  final def timedValueSink[T](startTime: Long, computedValue: T): (Long, T) =
    (OGTrace.nanoTime() - startTime, computedValue)

  final def newThrottle(maxConcurrent: Int): Throttle = newThrottle(new SimpleThrottleLimiter(maxConcurrent))
  final def newThrottle(policy: ThrottleLimiter): Throttle = new Throttle(policy)

  final def newThrottleWithPriority(maxConcurrent: Int): Throttle =
    new Throttle(new SimpleThrottleLimiter(maxConcurrent), enablePriority = true)

  final def newNativeMemoryThrottle(frac: Double, decayTimeMs: Long = 1000): Throttle =
    newNativeMemoryThrottle((GCNative.getEmergencyWatermarkMB * frac).toInt, decayTimeMs)

  final def newNativeMemoryThrottle(limitMB: Int, decayTimeMs: Long): Throttle = {
    assert(GCNative.altMallocLoaded())
    new Throttle(
      new LiveThrottleLimiter(
        limit = limitMB,
        liveWeigher = GCNative.getNativeAllocationMB _,
        decayTimeMs = decayTimeMs))
  }

  final def newManagedMemoryThrottle(limitMB: Int, decayTimeMs: Long): Throttle =
    new Throttle(
      new LiveThrottleLimiter(limit = limitMB, liveWeigher = GCNative.managedSizeMB _, decayTimeMs = decayTimeMs))

  /**
   * Suppress sync-stack warnings and assertions while executing f. Only for use inside optimus.platform code where such
   * stacks are expected and known to not be harmful
   */
  @closuresEnterGraph // which is the entire point of this method
  private[optimus /*platform*/ ] def suppressSyncStackDetection[T](f: => T): T = {
    if (Settings.syncStacksDetectionEnabled || OGTrace.observer.recordLostConcurrency) {
      // This is not an RT violation as we are just temporarily enabling
      // ignore sync stack on the scenario stack and then restore it
      val cn = OGSchedulerContext._TRACESUPPORT_unsafe_current().getCurrentNodeTask
      val prevStack = cn.scenarioStack()
      try {
        cn.replace(prevStack.withIgnoreSyncStack)
        f
      } finally {
        cn.replace(prevStack)
      }
    } else f
  }

  /**
   * applies translator to first input, enters the resulting scenario in a given block, applies translator the to second
   * input, enters the resulting scenario in a nested given block, and so forth recursively. Returns the corresponding
   * nesting of the resulting scenarios
   */
  @nodeSync @nodeSyncLift
  def translateScenariosRecursively[T](inputs: Seq[T])(@nodeLift translator: T => Scenario): Scenario =
    translateScenariosRecursively$withNode[T](inputs)(toNodeFactory(translator))
  def translateScenariosRecursively$withNode[T](inputs: Seq[T])(translator: T => Node[Scenario]): Scenario =
    translateScenariosRecursively$queued[T](inputs)(translator).get
  def translateScenariosRecursively$queued[T](inputs: Seq[T])(translator: T => Node[Scenario]): Node[Scenario] = {
    new CompletableNode[Scenario] {
      private var toTranslate: List[T] = inputs.toList // remaining inputs to process
      private var translatorNode: Node[Scenario] = _ // currently running translation
      private var scenarioAccumulator: Scenario = Scenario.empty // nested scenario of translated inputs (result)

      override def executionInfo(): NodeTaskInfo = NodeTaskInfo.RecursiveTranslate

      override def run(ec: OGSchedulerContext): Unit = {
        // on all but the first run, we have been called because the current translator node has completed
        if (translatorNode ne null) {
          if (translatorNode.isDoneWithException) completeFromNode(translatorNode, ec)
          else {
            // accumulate result into nested scenario
            combineInfo(translatorNode, ec)
            val scenario = translatorNode.result
            scenarioAccumulator = scenarioAccumulator.nest(scenario)
            // if there is more translation to do, update the current scenario stack (each translation is supposed to
            // run with the previous scenario applied)
            if (toTranslate.nonEmpty) replace(scenarioStack.createChild(scenario, this))
          }
        }
        if (!isDone) {
          // on all but the final run, we need to kick off the next translation (which will use our current scenarioStack
          // as per the usual rules)
          toTranslate match {
            case next :: rest =>
              toTranslate = rest
              translatorNode = translator(next)
              translatorNode.enqueue
              translatorNode.continueWith(this, ec)
            case Nil =>
              completeWithResult(scenarioAccumulator, ec)
          }
        }
      }
    }.enqueue
  }

  /**
   * Like [[IO.using]] but for asynchronous scopes.
   *
   * The Resource will be closed automatically when the async closure terminates.
   */
  @nodeSync
  @nodeSyncLift
  def asyncUsing[R <: AutoCloseable, B](resource: => R)(@nodeLift f: R => B): B =
    asyncUsing$withNode(resource)(toNodeFactory(f))
  def asyncUsing$withNode[R <: AutoCloseable, B](resource: => R)(f: R => Node[B]): B =
    new AsyncUsingNode[R, B](() => resource, f).get
  // noinspection ScalaWeakerAccess
  def asyncUsing$queued[R <: AutoCloseable, B](resource: => R)(f: R => Node[B]): Node[B] =
    new AsyncUsingNode[R, B](() => resource, f).enqueue
  // noinspection ScalaUnusedSymbol
  def asyncUsing$queued[R <: AutoCloseable, B](resource: => R, f: R => B): NodeFuture[B] =
    asyncUsing$queued(resource)(toNodeFactory(f))

}
