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

import com.sun.management.GarbageCollectionNotificationInfo
import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.Properties.Elem
import optimus.breadcrumbs.crumbs.Properties.Elems
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.graph.DiagnosticSettings.getBoolProperty
import optimus.graph.DiagnosticSettings.getIntProperty
import optimus.graph.cache.Caches
import optimus.graph.cache.CauseGCMonitor
import optimus.graph.cache.CleanupStats
import optimus.graph.cache.UNodeCache
import optimus.graph.diagnostics.GCMonitorDiagnostics
import optimus.graph.diagnostics.InfoDumper
import optimus.graph.tracking.DependencyTrackerRoot
import optimus.graph.tracking.TrackingGraphCleanupTrigger
import optimus.logging.LoggingInfo
import optimus.platform.util.Log
import optimus.platform.util.ProcessExitCodes
import optimus.platform.util.Version
import optimus.scalacompat.collection._
import optimus.utils.SystemFinalization

import java.lang.management.ManagementFactory
import java.lang.management.MemoryPoolMXBean
import java.lang.management.MemoryType
import java.lang.management.MemoryUsage
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import javax.management.Notification
import javax.management.NotificationEmitter
import javax.management.NotificationListener
import javax.management.openmbean.CompositeData
import scala.annotation.varargs
import scala.collection.{mutable => m}
import scala.jdk.CollectionConverters._

/*
 * Installs callbacks in Java GC that clear a part of optimus graph @node cache
 *
 * @param forceGCOnEviction : execute System.gc after evicting @nodes. When true, this also causes GCMonitor to ignore System.gc as a potential trigger
 */
class GCMonitor(allowForceGCOnEviction: Boolean = GCMonitor.defaultForceGCAfter) {
  import GCMonitor._
  private[graph] var cleanupsTriggered = 0L
  // Number of minor collections with heap above threshold, since last major collection
  private var sadMinorsSinceMajor = 0L

  // begin monitoring
  def monitor(): Unit = {
    log.info(
      "will evict {}% cached nodes if old gen heap usage exceeds {}% of max or stop-the-world GC consumes {}% of the last {} seconds",
      "%.2f".format(cacheClearRatio * 100.0),
      "%.2f".format(heapTriggerRatio * 100.0),
      "%.2f".format(timeTriggerRatio * 100.0),
      statisticsWindowSeconds
    )
    ManagementFactory.getGarbageCollectorMXBeans.asScala
      .collect { case n: NotificationEmitter => n }
      .foreach(n =>
        n.addNotificationListener(listeners.getOrElseUpdate(n.getName, createListener(n.getName)), null, null))
  }

  // stop monitoring
  def stop() =
    ManagementFactory.getGarbageCollectorMXBeans.asScala
      .collect { case n: NotificationEmitter => n }
      .foreach(n => n.removeNotificationListener(listeners(n.getName), null, null))

  private val log = getLogger(this.getClass)
  // list of end timestamps (gcStartTime, gcEndTIme)
  private var stamps: collection.Seq[(Long, Long)] = Nil
  private val listeners: m.Map[String, NotificationListener] = m.Map.empty

  def prettyPools(info: GarbageCollectionNotificationInfo) = {
    val m1 = info.getGcInfo.getMemoryUsageBeforeGc.asScala
    val m2 = info.getGcInfo.getMemoryUsageAfterGc.asScala
    (m1.mapValuesNow(_.toString).toMap, m2.mapValuesNow(_.toString).toMap)
  }

  // because our different consumers' reporting happens on different schedules, we need separate
  // stats objects (note that we can't do delta-based reporting because min/max fields are not delta-able)
  private val gcStatsByConsumer = new ConcurrentHashMap[String, CumulativeGCStats]()

  def snapAndResetStats(consumer: String): CumulativeGCStats =
    Option(gcStatsByConsumer.put(consumer, CumulativeGCStats.empty)).getOrElse(CumulativeGCStats.empty)

  def snapAndResetStatsForSamplingProfiler(): CumulativeGCStats = snapAndResetStats("SamplingProfiler")

  @volatile private var tNextStats = 0L
  private val cumulativeClears = new AtomicReference(CleanupStats(0, 0))

  private def createListener(x: String) = new NotificationListener {
    override def handleNotification(notification: Notification, handback: AnyRef): Unit =
      notification match {
        case n if n.getType == GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION => {
          val info = GarbageCollectionNotificationInfo.from(notification.getUserData.asInstanceOf[CompositeData])
          val cause = info.getGcCause
          val action = info.getGcAction
          val major = action.contains("major")
          val stats = CumulativeGCStats(info)
          gcStatsByConsumer.keys.asScala.foreach { k =>
            gcStatsByConsumer.compute(k, (_, v) => v.combine(stats))
          }

          // Don't handle notification of a gc we forced ourselves.  Java (at least as of 11) does not always mark System.gc() as
          // with cause=System..., so we'll err on the side of sometimes ignoring one real GC.
          if (wasForced && major) {
            wasForced = false
            Breadcrumbs(
              ChainedID.root,
              PropertiesCrumb(
                _,
                Crumb.GCSource,
                Properties.description -> "System.gc",
                Properties.gcCause -> cause,
                Properties.gcAction -> action,
                Properties.gcDuration -> info.getGcInfo.getDuration,
                Properties.gcPools -> prettyPools(info),
                Properties.logMsg -> "wasForced=true,resetting"
              )
            )
          } else {
            // Strategies make their own decision about major vs minor GC.
            val t = System.currentTimeMillis()
            if (major || t > tNextStats) {
              snapAndResetStats("GCMonitor").publish()
              tNextStats = t + minStatIntervalMs
            }
            timeBasedStrategy(info)
            heapBasedStrategy(info)
          }
        }
      }
  }

  val CacheClearTimeTriggerDescription = "GCMonitor.cacheClear.timeTrigger"
  private def timeBasedStrategy(info: GarbageCollectionNotificationInfo): Unit = {
    if (info.getGcAction.contains("major GC")) {
      // time based strategy: if time spent on GC is greater than specified percentage of total time
      val GCEndMillis = info.getGcInfo.getEndTime // units are milliseconds after 1.7.0_51, ticks earlier.
      val GCStartMillis =
        info.getGcInfo.getStartTime // (GCStartMillis - GCEndMillis) aka getDuration is what [Full GC... real=nn.nn secs] prints, but in ms
      val windowLengthMillis = statisticsWindowSeconds * 1000L // to millis
      val windowStartMillis = Math.max(0L, GCEndMillis - windowLengthMillis)
      stamps = stamps :+ (GCStartMillis, GCEndMillis)
      stamps = stamps.dropWhile(_._2 < (GCEndMillis - windowLengthMillis)) // roll the statistics-gathering window
      // normal case
      //              |xx|    |xxx| // gcs
      //           <--------------> // window
      //               ++      +++ // values to add up, ratio = 5/window
      // GC can be long
      //    |xxxxxxxxxxxx|    |xxx| // gcs
      //            <-------------> // window
      //            +++++      +++ // values to add up, ratio = 8/window
      // GC can be very long
      //   |xxxxxxxxxxxxxxxxxxxxxx| // gc
      //            <-------------> // window
      //            +++++++++++++++ // values to add up, ratio = 100%
      val gcTime = stamps.aggregate(0L)((a, b) => a + (b._2 - Math.max(b._1, windowStartMillis)), _ + _)
      val timeRatio = gcTime.toDouble / windowLengthMillis
      // if the application spent 50% of the last 30 seconds running GC and not making progress, evict 10% of nodes from optimus caches
      if (timeRatio > timeTriggerRatio) {
        log.info(
          "After GC due to {}, time spent in GC in the last {} seconds was {}%.",
          info.getGcCause,
          statisticsWindowSeconds,
          "%.2f".format(timeRatio * 100.0))
        GCMonitorDiagnostics.incrementCounter(GCMonitorDiagnostics.timeBasedTriggerRatioHit)
        // Don't actually clear cache unless we're also close to running out of memory.
        val gcUsages = getGCHeapUsages(info, Some(SystemFinalization.getObjectPendingFinalizationCount))
        val heapRatio = getHeapRatio(gcUsages, None)
        val triggered = timeRatio > timeBasedKillTrigger
        val willExit = triggered && timeBaseKillAllowed
        // Don't bother doing cleanup if we're about to exit, and if we do clean cache, don't force a GC, since
        // this risks detecting time spent in our own GC if java fails to label it properly as System.gc.
        val cleanup =
          if (heapRatio > timeTriggerHeapRatioMin && !willExit)
            Some(doCleanup(Some(gcUsages), includeSI = true, includeCtor = true, forceGCOnEviction = false))
          else None

        val msgs =
          if (triggered) {
            val msg =
              "After GC due to %s, time spent in GC in the last %d seconds was %.2f%%, greater than the kill limit of %.2f%%"
                .format(info.getGcCause, statisticsWindowSeconds, timeRatio * 100.0, timeBasedKillTrigger * 100.0)
            GCMonitorDiagnostics.incrementCounter(GCMonitorDiagnostics.timeBasedKillTriggerHit)
            msg :: Nil
          } else Nil

        sendCrumb(
          CacheClearTimeTriggerDescription,
          info,
          includeSI = true,
          gcUsages,
          timeRatio,
          timeTriggerRatio,
          cleanup,
          msgs,
          willExit)

        if (triggered && timeBaseKillAllowed || cleanup.exists(_.kill))
          kill(msgs ::: cleanup.fold[List[String]](Nil)(_.msgs))
      }
    }
  }

  private def sendCrumb(
      description: String,
      info: GarbageCollectionNotificationInfo,
      includeSI: Boolean,
      gcUsages: GCHeapUsagesMb,
      ratio: Double,
      triggerRatio: Double,
      cleanup: Option[Cleanup] = None,
      msgs: List[String] = Nil,
      kill: Boolean = false): Unit = {
    import gcUsages._
    val msg = (s"wasForced=$wasForced" :: msgs ::: cleanup.fold[List[String]](Nil)(_.msgs)).mkString(";")
    val desc = if (kill || cleanup.exists(_.kill)) description + ".kill" else description

    val props: Properties.Elems = Version.verboseProperties ++ Seq(
      Properties.host -> LoggingInfo.getHost,
      Properties.description -> desc,
      Properties.gcCause -> info.getGcCause,
      Properties.gcUsedHeapBeforeGC -> before.total,
      Properties.gcUsedHeapAfterGC -> after.total,
      Properties.gcUsedOldGenBeforeGC -> before.oldGen,
      Properties.gcUsedOldGenAfterGC -> after.oldGen,
      Properties.gcUsedHeapAfterCleanup -> cleanup.fold(-1.0)(_.heap.total),
      Properties.gcUsedOldGenAfterCleanup -> cleanup.fold(-1.0)(_.heap.oldGen),
      Properties.gcRatio -> ratio,
      Properties.triggerRatio -> heapTriggerRatio,
      Properties.gcMaxOldGen -> after.maxOldGen,
      Properties.gcMaxHeap -> maxHeapMB,
      Properties.gcFinalizerCountAfter -> cleanup.fold(-1)(_.heap.finalizerCount),
      Properties.gcPools -> prettyPools(info),
      Properties.includeSI -> includeSI,
      Properties.gcAction -> info.getGcAction,
      Properties.gcName -> info.getGcName,
      Properties.gcDuration -> info.getGcInfo.getDuration,
      Properties.gcCacheRemoved -> cleanup.fold(-1)(_.removed),
      Properties.gcCacheRemaining -> cleanup.fold(-1)(_.remaining),
      Properties.gcCleanupsFired -> cleanupsTriggered,
      Properties.gcMinorsSinceMajor -> sadMinorsSinceMajor,
      Properties.logMsg -> msg
    )

    Breadcrumbs(
      ChainedID.root,
      PropertiesCrumb(
        _,
        Crumb.GCSource,
        props
      )
    )
  }

  private def getHeapRatio(gcUsage: GCHeapUsagesMb, usage: Option[HeapUsageMb]): Double = {
    val numerator = (useActualMaxHeap, usage) match {
      case (true, None)     => gcUsage.after.total
      case (true, Some(u))  => u.total
      case (false, None)    => gcUsage.after.oldGen
      case (false, Some(u)) => u.oldGen
    }
    val denominator = (useActualMaxHeap, usage) match {
      case (true, _)        => maxHeapMB
      case (false, Some(u)) => u.maxOldGen
      case (false, None)    => gcUsage.after.maxOldGen
    }
    numerator / denominator
  }

  val CacheClearHeapTriggerDescription = "GCMonitor.cacheClear.heapTrigger"
  private def heapBasedStrategy(info: GarbageCollectionNotificationInfo) = {
    // trim cache based on heap usage

    val gcUsages = getGCHeapUsages(info, Some(SystemFinalization.getObjectPendingFinalizationCount))
    import gcUsages._

    val heapRatio = getHeapRatio(gcUsages, None)

    // if the heap after GC is below 75% of max heap, reset statistics counter
    if (heapRatio < heapOKRatio)
      cleanupsTriggered = 0

    val action = info.getGcAction
    val treatAsMajorGC =
      if (action.contains("major")) {
        // Really, truly major
        sadMinorsSinceMajor = 0L
        true
      } else if (heapCleanupOnlyOnMajor || sadMinorsSinceMajor < heapCleanupAfterMinors) {
        // Nope, not old or numerous enough
        sadMinorsSinceMajor += 1
        false
      } else {
        // Looks like we've hit our minor collection limit
        sadMinorsSinceMajor = 0L
        true
      }

    // if the heap after GC is 90% of max heap, evict 10% of nodes from optimus caches
    if (heapRatio > heapTriggerRatio) {
      log.info(
        "After GC due to {}, old gen was changed from {} MB to {} MB ({}% used of max {} MB), action={}, msm={}, triggered={}, backOffAfter={}",
        info.getGcCause,
        "%.2f".format(before.oldGen),
        "%.2f".format(after.oldGen),
        "%.2f".format(heapRatio * 100.0),
        "%.2f".format(after.maxOldGen),
        action,
        sadMinorsSinceMajor,
        cleanupsTriggered,
        backOffAfter
      )

      if (!treatAsMajorGC) {
        log.info(s"Skipping heap cleanup on minor GC ($sadMinorsSinceMajor since last major)")
        sendCrumb(CacheClearHeapTriggerDescription, info, false, gcUsages, heapRatio, heapTriggerRatio)
      } else if (cleanupsTriggered < includeSIAfter) {
        val cleanup = doCleanup(Some(gcUsages), includeSI = false, includeCtor = false, forceGCOnEviction = true)
        sendCrumb(
          CacheClearHeapTriggerDescription,
          info,
          includeSI = false,
          gcUsages,
          heapRatio,
          heapTriggerRatio,
          Some(cleanup))
        if (cleanup.kill) kill(cleanup.msgs)
        GCMonitorDiagnostics.incrementCounter(GCMonitorDiagnostics.heapBasedTriggerHit)
      } else if (cleanupsTriggered < backOffAfter) {
        val cleanup = doCleanup(Some(gcUsages), includeSI = true, includeCtor = true, forceGCOnEviction = true)
        sendCrumb(
          CacheClearHeapTriggerDescription,
          info,
          includeSI = true,
          gcUsages,
          heapRatio,
          heapTriggerRatio,
          Some(cleanup))
        if (cleanup.kill) kill(cleanup.msgs)
        GCMonitorDiagnostics.incrementCounter(GCMonitorDiagnostics.heapBasedIncludeSITriggerHit)
      } else if (cleanupsTriggered == backOffAfter) {
        // if 20 triggers in a row never saw heapOKRatio after GC, assume optimus cache manipulations aren't helping and stop trying
        log.info(
          "In the last {} cache clears, heap usage never fell below {}. Suspending cache clearing",
          backOffAfter,
          "%.2f".format(heapOKRatio * 100.0))
        sendCrumb(
          CacheClearHeapTriggerDescription,
          info,
          false,
          gcUsages,
          heapRatio,
          heapTriggerRatio,
          None,
          "suspending" :: Nil)
        cleanupsTriggered += 1 // at backOffAfter+1, it won't keep repeating the message above
        GCMonitorDiagnostics.incrementCounter(GCMonitorDiagnostics.heapBasedBackoffTriggerHit)
      } else
        log.info(s"Skipping cleanup due to backoff")
    }
  }

  private[optimus] def doCleanup(
      gcUsage: Option[GCHeapUsagesMb],
      includeSI: Boolean,
      includeCtor: Boolean,
      forceGCOnEviction: Boolean): Cleanup = {
    Caches.clearOldestForAllSSPrivateCache(cacheClearRatio, CauseGCMonitor)
    var stats: CleanupStats = UNodeCache.global.clearOldest(cacheClearRatio, CauseGCMonitor)
    if (includeSI) {
      stats += UNodeCache.siGlobal.clearOldest(cacheClearRatio, CauseGCMonitor)
    }
    if (includeCtor) {
      stats += UNodeCache.constructorNodeGlobal.clearOldest(cacheClearRatio, CauseGCMonitor)
    }
    cumulativeClears.updateAndGet(_ + stats)
    if (stats.removed > 0) { // no logging if nothing was done
      log.info(
        "Trimming @node cache by {} percent removed {} nodes from optimus graph cache",
        "%.2f".format(cacheClearRatio * 100.0),
        stats.removed)
      GCMonitorDiagnostics.incrementCounter(GCMonitorDiagnostics.totalNumberNodesCleared, stats.removed)
      // in production, forceGCOnEviction is true, so that after releasing @nodes from the cache, we force them out with a System.gc here, which means we can't trigger on System.gc
      // in testing, forceGCOnEviction is false so that the tests can trigger gcmonitor on demand by using System.gc, which means we cannot call System.gc here
      if (allowForceGCOnEviction && forceGCOnEviction) forceGC()
    }
    DependencyTrackerRoot.runCleanupNow(GCMonitorTrigger)
    cleanupsTriggered += 1
    val heap = HeapUsageMb()
    // Possibly reset backoff if we have enough information to decide.
    for (gu <- gcUsage) {
      val heapRatio = getHeapRatio(gu, Some(heap))
      if (heapRatio < heapOKRatio)
        cleanupsTriggered = 0
    }

    if (backOffKill && cleanupsTriggered == backOffAfter) {
      val msg =
        "Killing the process since the last %d cache clears did not help with heap usage"
          .format(cleanupsTriggered)
      Cleanup(heap = heap, removed = stats.removed.toInt, remaining = stats.remaining.toInt, msg :: Nil, true)
    } else
      Cleanup(heap = heap, removed = stats.removed.toInt, remaining = stats.remaining.toInt)
  }
}

private[graph] object GCMonitorTrigger extends TrackingGraphCleanupTrigger

object GCMonitor extends Log {
  val heapCleanupAfterMinors = getIntProperty("optimus.gc.heap.cleanup.after.n.minors", -1)
  val heapCleanupOnlyOnMajor = getBoolProperty("optimus.gc.heap.cleanup.only.major", heapCleanupAfterMinors < 0)
  assert(heapCleanupAfterMinors < 0 || !heapCleanupOnlyOnMajor, "Incompatible heap cleanup properties")
  val defaultForceGCAfter = getBoolProperty("optimus.gc.force.after.clear", true)
  // If the heap usage after GC is above this much of max heap, start nuking the @nodes from the cache (default: 90% of max. Set to 100 to disable.)
  private val heapTriggerPercent = getIntProperty("optimus.gc.heapTriggerPercent", 90)
  val heapTriggerRatio = heapTriggerPercent / 100.0
  // If the JVM spends this much time GC-ing, start nuking the @nodes from the cache (default: 50% of time. Set to 100 to disable.)
  private val timeTriggerPercent = getIntProperty("optimus.gc.timeTriggerPercent", 50)
  val timeTriggerRatio = timeTriggerPercent / 100.0
  // but don't actually clear cache unless, in addition to the time trigger, we're also above a heap limit, which by default
  // is twice as much available heap as implied by heapTriggerRatio.  Doesn't matter if this goes negative.
  private val timeTriggerHeapPercentMin =
    getIntProperty("optimus.gc.timeTriggerPrecent.heapPercentMin", 100 - 2 * (100 - heapTriggerPercent))
  val timeTriggerHeapRatioMin = timeTriggerHeapPercentMin / 100.0
  // This sets now many nodes to drop from the cache when the simple LRU algorithm is used (default: 10% of nodes)
  val cacheClearRatio = getIntProperty("optimus.gc.cacheClearPercent", 10).toDouble / 100.0
  // Only GCs that occurred in the last N seconds are included in the decision making (default: 30 seconds)
  val statisticsWindowSeconds = getIntProperty("optimus.gc.slidingWindowSeconds", 30)
  // if the JVM spends this much time GC-ing, kill the app (if permitted by optimus.gc.kill)
  val timeBasedKillTrigger = getIntProperty("optimus.gc.timeKillTriggerPercent", 95).toDouble / 100.0
  // allow System.exit when kill conditions are met
  val timeBaseKillAllowed = getBoolProperty("optimus.gc.kill", false)
  // if jvm heap is below this limit (default 75% max heap), reset all statistics
  val heapOKRatio = getIntProperty("optimus.gc.heapOKPercent", 75).toDouble / 100.0
  // if optimus cache trimming was performed this many times and never once saw heapOKRatio, start including SI cache
  val includeSIAfter = getIntProperty("optimus.gc.includeSIAfter", 10)
  // if optimus cache trimming was performed this many times and never once saw heapOKRatio, stop trying
  val backOffAfter = getIntProperty("optimus.gc.backOffAfter", 20)
  // allow System.exit when the backOffAfter limit is reached
  val backOffKill = getBoolProperty("optimus.gc.backOffKill", false)
  private val memoryBean = ManagementFactory.getMemoryMXBean
  val maxHeapMB = memoryBean.getHeapMemoryUsage.getMax.toDouble / (1024 * 1024)
  private val useActualMaxHeap = getBoolProperty("optimus.gc.useActualMaxHeap", false)

  private val minStatIntervalMs: Long = getIntProperty("optimus.gc.min.stat.interval.sec", 15) * 1000L

  private var _instance: GCMonitor = null
  def instance: GCMonitor = GCMonitor.synchronized {
    if (_instance eq null) _instance = new GCMonitor()
    _instance
  }

  private val crumbBase =
    Properties.Elems(Properties.host -> LoggingInfo.getHost) ++ LoggingInfo.gsfControllerId.map(gsfControllerId =>
      Properties.Elem(Properties.gsfControllerId, gsfControllerId))
  @varargs
  def gcCrumb(elems: Properties.Elem[_]*): Unit = {
    if (Breadcrumbs.collecting) {
      Breadcrumbs.send(PropertiesCrumb(ChainedID.root, Crumb.GCSource, crumbBase ++ elems))
    }
  }

  def kill(msg: String): Unit = kill(msg :: Nil)

  def kill(msgs: List[String]) =
    InfoDumper.kill("gcMonitor", "Forced exit" :: msgs, ProcessExitCodes.OOM, Crumb.GCSource)

  // n.b. these are accumulated during the sampling period but then reset after every publication (so not cumulative
  // over all time)
  final case class CumulativeGCStats(
      nMinor: Int,
      nMajor: Int,
      duration: Long,
      durationMajor: Long,
      durationMinor: Long,
      minBefore: Double,
      maxBefore: Double,
      minAfter: Double,
      maxAfter: Double) {
    def combine(gc: CumulativeGCStats): CumulativeGCStats = {
      CumulativeGCStats(
        nMinor = nMinor + gc.nMinor,
        nMajor = nMajor + gc.nMajor,
        duration = duration + gc.duration,
        durationMajor = durationMajor + gc.durationMajor,
        durationMinor = durationMinor + gc.durationMinor,
        minBefore = Math.min(minBefore, gc.minBefore),
        maxBefore = Math.max(maxBefore, gc.maxBefore),
        minAfter = Math.min(minAfter, gc.minAfter),
        maxAfter = Math.max(maxAfter, gc.maxAfter)
      )
    }

    def elems: Elems = {
      val always = Elems(
        Properties.gcDuration -> duration,
        Properties.gcNumMajor -> nMajor,
        Properties.gcNumMinor -> nMinor,
      )

      // don't report min/max if we are empty (because they are 0 and MaxValue which is not useful to see)
      if (this == CumulativeGCStats.empty) always
      else
        always ::: Elems(
          Properties.gcMaxUsedHeapAfterGC -> maxAfter,
          Properties.gcMinUsedHeapAfterGC -> minAfter,
          Properties.gcMaxUsedHeapBeforeGC -> maxBefore,
          Properties.gcMinUsedHeapBeforeGC -> minBefore
        )
    }

    def publish(): Unit = {
      Breadcrumbs(
        ChainedID.root,
        PropertiesCrumb(
          _,
          Crumb.GCSource,
          (elems + Elem(Properties.description, "stats")).m: _*
        )
      )

    }
  }

  object CumulativeGCStats {
    val empty: CumulativeGCStats = new CumulativeGCStats(0, 0, 0L, 0, 0, Double.MaxValue, 0.0, Double.MaxValue, 0.0)
    def apply(info: GarbageCollectionNotificationInfo): CumulativeGCStats = {
      val isMajor = info.getGcAction.contains("major")
      val gcInfo = info.getGcInfo
      val usages = getGCHeapUsages(info)
      CumulativeGCStats(
        nMinor = if (isMajor) 0 else 1,
        nMajor = if (isMajor) 1 else 0,
        duration = gcInfo.getDuration,
        durationMajor = if (isMajor) gcInfo.getDuration else 0,
        durationMinor = if (isMajor) 0 else gcInfo.getDuration,
        maxBefore = usages.before.total,
        minBefore = usages.before.total,
        maxAfter = usages.after.total,
        minAfter = usages.after.total
      )
    }
  }

  // Heap usage from beans.
  private[graph] final case class HeapUsageMb(total: Double, oldGen: Double, maxOldGen: Double, finalizerCount: Int)
  object HeapUsageMb {

    private def toMb[A](as: Iterable[A], toUsage: A => MemoryUsage, get: MemoryUsage => Long): Double =
      as.map(toUsage(_)).map(get(_)).sum.toDouble / (1024.0 * 1024.0)

    private def mapToMb(mu: Map[String, MemoryUsage], ends: String, get: MemoryUsage => Long): Double =
      toMb[MemoryUsage](mu.filter(_._1.endsWith(ends)).values, identity, get)

    private def beansToMb(pools: collection.Seq[MemoryPoolMXBean], ends: String, get: MemoryUsage => Long): Double =
      toMb[MemoryPoolMXBean](pools.filter(_.getName.endsWith(ends)), _.getUsage, get)

    def apply(): HeapUsageMb = {
      val pools = ManagementFactory.getMemoryPoolMXBeans.asScala.filter(b => b.isValid && b.getType == MemoryType.HEAP)
      HeapUsageMb(
        total = beansToMb(pools, "", _.getUsed),
        oldGen = beansToMb(pools, "Old Gen", _.getUsed),
        maxOldGen = beansToMb(pools, "Old Gen", _.getMax),
        finalizerCount = SystemFinalization.getObjectPendingFinalizationCount
      )
    }
    def apply(usageMap: Map[String, MemoryUsage], finalizers: Option[Int]): HeapUsageMb =
      HeapUsageMb(
        total = mapToMb(usageMap, "", _.getUsed),
        oldGen = mapToMb(usageMap, "Old Gen", _.getUsed),
        maxOldGen = mapToMb(usageMap, "Old Gen", _.getMax),
        finalizerCount = finalizers.getOrElse(-1)
      )
  }

  // Heap usage before and after GC, from notification info
  private[graph] final case class GCHeapUsagesMb(before: HeapUsageMb, after: HeapUsageMb)

  private[graph] def getGCHeapUsages(info: GarbageCollectionNotificationInfo, finalizers: Option[Int] = None) = {
    GCHeapUsagesMb(
      before = HeapUsageMb(info.getGcInfo.getMemoryUsageBeforeGc.asScala.toMap, None),
      after = HeapUsageMb(info.getGcInfo.getMemoryUsageAfterGc.asScala.toMap, finalizers)
    )
  }

  private[graph] final case class Cleanup(
      heap: HeapUsageMb,
      removed: Int,
      remaining: Int,
      msgs: List[String] = Nil,
      kill: Boolean = false)

  @volatile private[graph] var wasForced = false
  def forceGC(): Unit = this.synchronized {
    wasForced = true
    System.gc()
  }
}
