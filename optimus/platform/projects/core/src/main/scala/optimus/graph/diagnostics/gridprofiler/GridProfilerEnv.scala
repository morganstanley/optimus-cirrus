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
package optimus.graph.diagnostics.gridprofiler

import java.io.CharArrayWriter
import java.lang.management.ManagementFactory
import java.time.Instant
import java.time.temporal.ChronoUnit.MILLIS
import java.util.concurrent.atomic.AtomicLong
import com.opencsv.CSVWriter
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.spi.FilterReply
import com.sun.management.GarbageCollectionNotificationInfo
import com.sun.management.OperatingSystemMXBean

import javax.management.Notification
import javax.management.NotificationEmitter
import javax.management.openmbean.CompositeData
import optimus.graph.DiagnosticSettings
import optimus.graph.OGSchedulerTimes
import optimus.graph.cache.Caches
import optimus.graph.cache.CacheCountersSnapshot
import optimus.graph.cache.CacheGroup
import optimus.graph.cache.CacheGroupCtorGlobal
import optimus.graph.cache.CacheGroupGlobal
import optimus.graph.cache.CacheGroupOthers
import optimus.graph.cache.CacheGroupSiGlobal
import optimus.graph.cache.NodeCCache
import optimus.graph.cache.UNodeCache

import java.lang.management.ManagementFactory
import optimus.graph.diagnostics.GCMonitorStats
import optimus.graph.diagnostics.GCNativeStats
import optimus.graph.diagnostics.GCStats
import optimus.graph.diagnostics.ThreadStatUtil
import optimus.graph.diagnostics.ThreadStatUtil.CacheStateSummary
import optimus.graph.diagnostics.ThreadStatUtil.CacheStats
import optimus.graph.diagnostics.ThreadStatUtil.ThreadStateSnapshot
import optimus.graph.diagnostics.ThreadStatUtil.ThreadStateSummary
import optimus.graph.diagnostics.pgo.Profiler
import optimus.platform.util.Log
import optimus.utils.ClassLoaderStats
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.collection.{mutable => m}

final case class DistEnvMetrics(
    numTasks: Int, // number of completed tasks including "<client>" [SEE_DIST_TASK_REPORT]
    totalWallTimeMs: Long, // sum of wall times of all tasks included in this metric
    minWallTimeMs: Long, // smallest remote wall time seen
    maxWallTimeMs: Long, // largest remote wall time seen
    freshJVM: Int, // number of dist tasks contributed to this aggregate that were executed on a fresh JVM (aka number of times this engine-id was seen, typically 1)
    engineStartupMs: Long, // uptime to the start of dist task on every fresh JVMs (aka engine startup time)
    // these three are filled out by aggregateFromDist
    distOverhead: Long, // total distribution overhead (wall time on client minus wall time on engine)
    gsfBytesSent: Long,
    gsfBytesRcvd: Long
) {
  def combine(other: DistEnvMetrics) = DistEnvMetrics(
    numTasks = numTasks + other.numTasks,
    totalWallTimeMs = totalWallTimeMs + other.totalWallTimeMs,
    minWallTimeMs = math.min(minWallTimeMs, other.minWallTimeMs),
    maxWallTimeMs = math.max(maxWallTimeMs, other.maxWallTimeMs),
    freshJVM = freshJVM + other.freshJVM,
    engineStartupMs = engineStartupMs + other.engineStartupMs,
    distOverhead = distOverhead + other.distOverhead,
    gsfBytesSent = gsfBytesSent + other.gsfBytesSent,
    gsfBytesRcvd = gsfBytesRcvd + other.gsfBytesRcvd
  )
}

object DistEnvMetrics {
  def empty = DistEnvMetrics(
    numTasks = 0,
    totalWallTimeMs = 0,
    minWallTimeMs = 0,
    maxWallTimeMs = 0,
    freshJVM = 0,
    engineStartupMs = 0,
    distOverhead = 0,
    gsfBytesSent = 0,
    gsfBytesRcvd = 0
  )
}

// JVM-wide (or even system-wide) metrics
final case class ProcessMetricsEntry(
    // JVM-wide metrics consist of...
    // Optimus Scheduler metrics (available when profileTime = true)
    totalGraphTime: Long, // graph time (OGSchedulerTimes.getInGraphWallTime)
    totalUnderUtilizedTime: Long, // lost concurrency time (OGSchedulerTimes.getUnderUtilizedTime)
    // cache stats
    cacheStateSummary: CacheStateSummary,
    // Operating System metrics (available by default)
    maxHeap: Long, // max post-GC heap seen
    jvmCPUTime: Long, // JVM CPU time
    jvmCPULoad: Double, // JVM CPU load 0.0 - 1.0
    systemCPULoad: Double, // System CPU load 0.0 - 1.0
    loadAvg: Double, // system load average
    gcStats: GCStats,
    jitTime: Long, // JIT compiler time (always parallel, this contributes to the CPU time)
    clTime: Long, // Class loading time
    // Distribution metrics (available by default)
    dist: DistEnvMetrics,
    // more JVM metrics
    threadStatesSummary: ThreadStateSummary,
    logSize: Long
) {

  def remoteWallTime: Long = dist.totalWallTimeMs

  def asSummaryTable: SummaryTable = {
    val gcTimeStopTheWorld = gcStats.gcTimeStopTheWorld
    val gcTimeAll = gcStats.gcTimeAll
    SummaryTable(
      wallTime = dist.totalWallTimeMs,
      distOverhead = dist.distOverhead,
      engineStartupMs = dist.engineStartupMs,
      remoteWallTime = remoteWallTime,
      distTasks = dist.numTasks,
      maxHeap = maxHeap,
      loadAvg = loadAvg,
      jvmCPUTime = jvmCPUTime,
      jvmCPULoad = jvmCPULoad,
      gcTimeStopTheWorld = gcTimeStopTheWorld,
      gcTimeAll = gcTimeAll,
      jitTime = jitTime,
      clTime = clTime,
      threadStatesSummary = threadStatesSummary
    )
  }
  // used by the shutdown hook in GridProfiler
  private[diagnostics] def formatSummary: String = {
    val summary = asSummaryTable
    val cols = collection.Seq(
      SummaryTable.wallTime,
      SummaryTable.distOverhead,
      SummaryTable.distTasks,
      SummaryTable.maxHeap,
      SummaryTable.loadAvg,
      SummaryTable.jvmCPUTime,
      SummaryTable.jvmCPULoad,
      SummaryTable.systemCPULoad,
      SummaryTable.gcTimeStopTheWorld,
      SummaryTable.gcTimeAll,
      SummaryTable.jitTime
    )
    val table = new StatTable(cols.map(_.statCol(summary)) ++ SummaryTable.threadStatTableCols(summary))
    table.toString()
  }

  // average the CPU loads and loadavgs, except if one of them was not available (e.g. originated in the windows client), then just use the other
  private final def averager(l: Double, r: Double, lCount: Int, rCount: Int) =
    if (l < 0 && r < 0) -1.0 else if (l < 0) r else if (r < 0) l else (l * lCount + r * rCount) / (lCount + rCount)

  // used by GridProfiler when aggregating metrics that arrived from the grid
  private[diagnostics] def combine(y: ProcessMetricsEntry) = ProcessMetricsEntry(
    // Optimus Scheduler metrics (available when profileTime = true)
    totalGraphTime + y.totalGraphTime,
    totalUnderUtilizedTime + y.totalUnderUtilizedTime,
    cacheStateSummary.combine(y.cacheStateSummary),
    // Operating System metrics (available by default)
    Math.max(maxHeap, y.maxHeap),
    jvmCPUTime + y.jvmCPUTime,
    averager(jvmCPULoad, y.jvmCPULoad, dist.numTasks, y.dist.numTasks),
    averager(systemCPULoad, y.systemCPULoad, dist.numTasks, y.dist.numTasks),
    averager(loadAvg, y.loadAvg, dist.numTasks, y.dist.numTasks),
    gcStats.combine(y.gcStats),
    jitTime + y.jitTime,
    clTime + y.clTime,
    // Distribution metrics (available by default)
    dist.combine(y.dist),
    threadStatesSummary.combine(y.threadStatesSummary),
    logSize + y.logSize
  )
}

class SizeTrackingFilter extends ch.qos.logback.core.filter.Filter[ILoggingEvent] {
  private val size = new AtomicLong(0L)
  def getSize: Long = size.get
  override def decide(event: ILoggingEvent): FilterReply = {
    val msg = event.getFormattedMessage
    if (msg ne null) size.getAndAdd(msg.length)
    FilterReply.NEUTRAL
  }
}

object ProcessMetricsEntry extends Log {
  val hostStartTime: Long = System.currentTimeMillis()
  def now(): Long = System.currentTimeMillis()

  // average heap usage (in MB) so far. Written from gc thread.
  @volatile private[diagnostics] var maxHeapUsage = -1L
  private val sizeCountingFilter = new SizeTrackingFilter // one filter instance for all appenders
  // marker for whether JVM was launched just for this task or reused
  private[diagnostics] var freshJVM = true
  // snapshot of the JVM-wide metrics at the beginning of a task/profiled block
  private[diagnostics] var processMetricsBegin = ProcessMetricsEntry.start(log = true)
  // process metrics that belong to the DistTask that was suspended by the node at this id (only used in recursive distribution)
  private[diagnostics] val suspendedProcessMetrics = m.Map.empty[Int, ProcessMetricsEntry]

  private[optimus] def reset(): Unit = {
    processMetricsBegin = ProcessMetricsEntry.start(log = true)
  }

  private[graph] def isFreshJvm: Boolean = freshJVM

  // Called to ensure that class is fully initialised.
  // This calls def start via processMetricsBegin and records heap/cpu/etc at start time
  def ensureLoaded(): Unit = ()

  // snapshot the JVM-wide metrics at the start of a block/distribution, possibly aggregating with metrics from an earlier suspended task
  // Note: this resets GridProfiler.maxHeapUsage to zero
  private[diagnostics] def start(
      suspended: Option[ProcessMetricsEntry] = None,
      log: Boolean = false): ProcessMetricsBeginSnapshot = {
    // if this is the first time we're starting measurement on this JVM, attach gc callback
    if (maxHeapUsage < 0) {
      // statistics callback runs on every gc (including minor gc), and records the highest post-gc heap usage seen
      ManagementFactory.getGarbageCollectorMXBeans.asScala
        .collect { case n: NotificationEmitter => n }
        .foreach(n =>
          n.addNotificationListener(
            (notification: Notification, _: Any) =>
              notification match {
                case n if n.getType == GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION =>
                  val info =
                    GarbageCollectionNotificationInfo.from(notification.getUserData.asInstanceOf[CompositeData])
                  val usedHeap = info.getGcInfo.getMemoryUsageAfterGc.values.asScala.map(_.getUsed).sum
                  if (usedHeap > maxHeapUsage)
                    maxHeapUsage = usedHeap
              },
            null,
            null
          ))
    }
    maxHeapUsage = 0L

    if (!DiagnosticSettings.getBoolProperty("optimus.profiler.disableLogSize", false))
      setupLogSizeCountingFilter()

    val result = ProcessMetricsBeginSnapshot(
      suspended,
      // Distribution metrics
      freshJVM,
      if (freshJVM) DiagnosticSettings.jvmUpTimeInMs() else 0L,
      snap(log)
    )
    result
  }

  private[diagnostics] def snap(log: Boolean = false): ProcessMetricsSnapshot = {
    // common counter snapping to share between start and end methods
    val osBean = ManagementFactory.getOperatingSystemMXBean().asInstanceOf[OperatingSystemMXBean]
    val gcBeans = ManagementFactory.getGarbageCollectorMXBeans
    // unclear why these were pre-called
    // osBean.getCpuLoad // cpuLoad is system cpu load
    // osBean.getProcessCpuLoad // force the bean to read /proc/self/stat now
    def sumCounters(caches: Seq[NodeCCache]) = {
      caches.map(_.getCountersSnapshot).foldLeft(CacheCountersSnapshot.Empty)(CacheCountersSnapshot.sum(_, _))
    }
    def cacheSnapshot(cache: NodeCCache) =
      Option(cache).map(_.getCountersSnapshot).getOrElse(CacheCountersSnapshot.Empty)
    val cacheCountersMap: Map[CacheGroup, CacheCountersSnapshot] = Map(
      CacheGroupGlobal -> cacheSnapshot(UNodeCache.global),
      CacheGroupSiGlobal -> cacheSnapshot(UNodeCache.siGlobal),
      CacheGroupCtorGlobal -> cacheSnapshot(UNodeCache.constructorNodeGlobal),
      CacheGroupOthers -> sumCounters(
        Caches.allCaches(includeSiGlobal = false, includeGlobal = false, includeCtorGlobal = false))
    )

    val snapshot = ProcessMetricsSnapshot(
      patch.MilliInstant.now,
      // Optimus scheduler metrics (available when profileTime = true)
      OGSchedulerTimes.getInGraphWallTime,
      OGSchedulerTimes.getUnderUtilizedTime,
      cacheCountersMap,
      // OS metrics
      osBean.getProcessCpuTime,
      osBean.getProcessCpuLoad,
      osBean.getCpuLoad, // presentation compiler marks as red, but alternative method getSystemCpuLoad is deprecated
      osBean.getSystemLoadAverage, {
        val stwBeans = gcBeans.asScala.filter(x =>
          x.getName.contains("PS MarkSweep") || x.getName.contains("MarkSweepCompact") || x.getName.contains(
            "G1 Mixed"))
        val sum = stwBeans.map(_.getCollectionTime).sum
        if (log)
          Profiler.log.info(
            s"recording STW GC time $sum composed of " + stwBeans.map(z => z.getName + " : " + z.getCollectionTime))
        sum
      }, {
        val sum = gcBeans.asScala.map(_.getCollectionTime).sum
        if (log)
          Profiler.log.info(s"recording All GC time $sum composed of " + gcBeans.asScala.map(z =>
            z.getName + " : " + z.getCollectionTime))
        sum
      },
      gcBeans.asScala.map(_.getCollectionCount).sum,
      Option(ManagementFactory.getCompilationMXBean)
        .filter(_.isCompilationTimeMonitoringSupported)
        .map(_.getTotalCompilationTime)
        .getOrElse(0L),
      ClassLoaderStats.snap().findClassTime,
      // Distribution metrics
      GCMonitorStats.snap(),
      GCNativeStats.snap(),
      ThreadStatUtil.threadsSnapshot(),
      sizeCountingFilter.getSize
    )
    snapshot
  }

  // complete a measurement of process (environment) metrics. This is executed at the end of a dist task/profiled block
  private[diagnostics] def end(
      start: ProcessMetricsBeginSnapshot = processMetricsBegin,
      log: Boolean = false,
      task: String = GridProfiler.clientKey,
      engine: String = ""): ProcessMetricsEntry = {
    val snapAtStart = start.metricsSnapshot
    val snapAtEnd = snap(log)
    val threadSummary = ThreadStatUtil.threadsSummary(snapAtStart.threadStateSnapshot, snapAtEnd.threadStateSnapshot)
    // Optimus Scheduler metrics (available when profileTime = true)
    val totalGraphTime = snapAtEnd.totalGraphTime - snapAtStart.totalGraphTime
    val totalUnderUtilizedTime = snapAtEnd.totalUnderUtilizedTime - snapAtStart.totalUnderUtilizedTime
    // cache metrics
    val cacheStatMap = CacheGroup.All
      .map(k =>
        k -> {
          val start = snapAtStart.cacheCountersMap(k)
          val end = snapAtEnd.cacheCountersMap(k)
          val diff = CacheCountersSnapshot.diff(start, end)
          val bulkRemove = diff.nodesRemoved
          // implied removals diff.insertCount - change in cacheSize - bulkRemove
          val cacheSizeChange = diff.indicativeCacheSize
          val cacheInserted = diff.insertCount
          val lruRemove = cacheInserted - cacheSizeChange - bulkRemove
          CacheStats(
            end.cacheSlots,
            end.indicativeCacheSize,
            cacheSizeChange,
            cacheInserted,
            bulkRemove,
            if (lruRemove < 0) 0 else lruRemove
          )
        })
      .toMap
    val cacheSummary = CacheStateSummary(cacheStatMap)

    // Operating System metrics (available by default)
    val jvmCPUTime = snapAtEnd.jvmCPUTime - snapAtStart.jvmCPUTime
    val loadAvg = (snapAtEnd.loadAvg + snapAtStart.loadAvg) / 2.0 // average the load average
    // blakedav - previously these two were just values at end, but this provides little information of value
    // ideally these would be sampled and averaged continuously over duration of a grid task
    val jvmCPULoad = (snapAtEnd.jvmLoad + snapAtStart.jvmLoad) / 2.0
    val systemCPULoad = (snapAtEnd.systemLoad + snapAtStart.systemLoad) / 2.0
    // GC stats
    val gcTimeStopTheWorld = snapAtEnd.gcTimeStopTheWorld - snapAtStart.gcTimeStopTheWorld
    val gcTimeAll = snapAtEnd.gcTimeAll - snapAtStart.gcTimeAll
    val gcCount = snapAtEnd.gcCount - snapAtStart.gcCount
    val gcMonitorStats = GCMonitorStats.diff(snapAtStart.gcMonitorStats, snapAtEnd.gcMonitorStats)
    val gcNativeStats = GCNativeStats.diff(snapAtStart.gcNativeStats, snapAtEnd.gcNativeStats)
    val gcStats = GCStats(gcTimeStopTheWorld, gcTimeAll, gcCount, gcMonitorStats, gcNativeStats)
    // cpu timings
    val jitTime = snapAtEnd.jitTime - snapAtStart.jitTime
    val clTime = snapAtEnd.clTime - snapAtStart.clTime

    // Distribution metrics (available by default)
    val taskTime = MILLIS.between(snapAtStart.snapTime, snapAtEnd.snapTime)
    val distMetrics = DistEnvMetrics(
      numTasks = 1,
      totalWallTimeMs = taskTime,
      minWallTimeMs = taskTime,
      maxWallTimeMs = taskTime,
      freshJVM = if (start.freshJVM) 1 else 0,
      engineStartupMs = if (task != GridProfiler.clientKey) start.engineStartupMs else 0L,
      // these are filled in by aggregateFromDist
      distOverhead = 0L,
      gsfBytesSent = 0L,
      gsfBytesRcvd = 0L
    )
    val logSize = snapAtEnd.logSize - snapAtStart.logSize

    val result = ProcessMetricsEntry(
      totalGraphTime,
      totalUnderUtilizedTime,
      cacheSummary,
      maxHeapUsage,
      jvmCPUTime,
      jvmCPULoad,
      systemCPULoad,
      loadAvg,
      gcStats,
      jitTime,
      clTime,
      distMetrics,
      threadSummary,
      logSize
    )
    if (start.suspended.isDefined) {
      val sus = start.suspended.get
      // the suspended task was not finished yet, adjust to avoid over-counting number of tasks
      val r = result.combine(sus)
      r.copy(dist = r.dist.copy(
        numTasks = 1,
        maxWallTimeMs = r.dist.totalWallTimeMs,
        minWallTimeMs = r.dist.totalWallTimeMs
      ))
    } else result
  }

  private[diagnostics] def pop(depth: Int): Unit = {
    val old: Option[ProcessMetricsEntry] = suspendedProcessMetrics.remove(depth)
    if (old.isEmpty) GridProfiler.log.error(s"Suspended process metrics not found at local recursion depth $depth")
    processMetricsBegin = start(old, log = true)
  }

  private[diagnostics] def push(depth: Int, task: String, engine: String): Unit = {
    val pme = ProcessMetricsEntry.end(processMetricsBegin, log = false, task, engine)
    val old = suspendedProcessMetrics.put(depth, pme)
    if (old.nonEmpty) GridProfiler.log.error(s"Process metrics were suspended twice at local recursion depth $depth")
    ProcessMetricsEntry.freshJVM = false // not fresh for the metric we're about to create
    processMetricsBegin = ProcessMetricsEntry.start(log = true)
  }

  val empty = ProcessMetricsEntry(
    totalGraphTime = 0L,
    totalUnderUtilizedTime = 0L,
    cacheStateSummary = CacheStateSummary.empty,
    maxHeap = 0L,
    jvmCPUTime = 0L,
    jvmCPULoad = 0.0,
    systemCPULoad = 0.0,
    loadAvg = 0.0,
    gcStats = GCStats.empty,
    jitTime = 0L,
    clTime = 0L,
    dist = DistEnvMetrics.empty,
    threadStatesSummary = ThreadStateSummary.empty,
    logSize = 0L
  )

  private def setupLogSizeCountingFilter(): Unit = {
    val context = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    for (logger <- context.getLoggerList.asScala; ap <- logger.iteratorForAppenders.asScala) {
      val maybeFilter = ap.getCopyOfAttachedFiltersList.asScala.find(_.isInstanceOf[SizeTrackingFilter])
      if (maybeFilter.isEmpty) {
        Profiler.log.info(s"installing SizeTrackingFilter on logger ${logger.getName} appender ${ap.getName}")
        ap.addFilter(sizeCountingFilter)
      }
    }
  }

  def printCSV(agg: Map[String, ProcessMetricsEntry]): (String, Iterable[Map[String, String]]) = {
    val writer = new CharArrayWriter
    val csvWriter = new CSVWriter(writer)
    // NOTE: You must never, EVER, re-order these columns, or insert or delete a column.  You can only append,
    // because it's impossible to know what downstream processes count on ordinal column position.
    val header = Array(
      "Engine",
      "CPU Time (ms)",
      "CPU Load (%)",
      "System Load (%)",
      "Wall Graph Time (ms)",
      "Saturated Time (ms)",
      "Max Heap post-GC (MB)",
      "OS Load Average",
      "Full GC Time (ms)",
      "All GC Time (ms)",
      "JIT Time (ms)",
      "Dist Overhead Time (ms)",
      "Number of Dist Tasks",
      "Dist Task Wall Time, Min (ms)",
      "Dist Task Wall Time, Avg (ms)",
      "Dist Task Wall Time, Max (ms)",
      "JVM starts",
      "Time Based Ratio Trigger Hit",
      "Time Based Kill Trigger Hit",
      "Heap Based Trigger Hit",
      "Heap Based Include SI Trigger Hit",
      "Heap Based Backoff Trigger Hit",
      "Total Number Nodes Cleared",
      "Engine Startup Time (ms)",
      "Log Size, MB",
      "Class Load Time (ms)",
      "GSF Sent, MB",
      "GSF Received, MB"
    )

    val data = for ((engineId, v) <- agg.toSeq.sortBy(-_._2.totalGraphTime)) yield {
      Array(
        s"$engineId",
        "%.2f".format(v.jvmCPUTime * 1e-6), // cpu time (ms)
        if (v.jvmCPULoad > 0) "%.2f".format(v.jvmCPULoad * 100.0) else "n/a", // cpu load (%)
        if (v.systemCPULoad > 0) "%.2f".format(v.systemCPULoad * 100.0) else "n/a", // System Load (%)
        "%.2f".format(v.totalGraphTime * 1e-6), // Wall Graph Time (ms)
        "%.2f".format(v.totalUnderUtilizedTime * 1e-6), //  Under Utilized Time (ms)
        "%.2f".format(v.maxHeap / (1024.0 * 1024.0)), // Max Heap post-GC (MB)
        if (v.loadAvg > 0) "%.2f".format(v.loadAvg) else "n/a", // OS Load Average
        s"${v.gcStats.gcTimeStopTheWorld}", // Full GC Time (ms)
        s"${v.gcStats.gcTimeAll}", // All GC Time (ms)
        s"${v.jitTime}", // JIT Time (ms)
        "%.2f".format(v.dist.distOverhead * 1e-6),
        s"${v.dist.numTasks}",
        s"${v.dist.minWallTimeMs}",
        s"${v.dist.totalWallTimeMs / v.dist.numTasks}",
        s"${v.dist.maxWallTimeMs}",
        s"${v.dist.freshJVM}",
        s"${v.gcStats.gcMonitorStats.timeBasedTriggerRatioHit}",
        s"${v.gcStats.gcMonitorStats.timeBasedKillTriggerHit}",
        s"${v.gcStats.gcMonitorStats.heapBasedTriggerHit}",
        s"${v.gcStats.gcMonitorStats.heapBasedIncludeSITriggerHit}",
        s"${v.gcStats.gcMonitorStats.heapBasedBackoffTriggerHit}",
        s"${v.gcStats.gcMonitorStats.totalNumberNodesCleared}",
        s"${v.dist.engineStartupMs}",
        s"${v.logSize / (1024.0 * 1024.0)}",
        s"${v.clTime / 1000000}", // Class Load time (ms)
        s"${v.dist.gsfBytesSent / (1024.0 * 1024.0)}",
        s"${v.dist.gsfBytesRcvd / (1024.0 * 1024.0)}"
      )
    }

    if (data.nonEmpty) {
      csvWriter.writeAll((header +: data).asJava)
    }
    (writer.toString, GridProfiler.toMaps(header, data))

  }
}

// set of values collected at the start of a dist task/profiled block
// When the task/block completes, these values will be used to build a ProcessMetricsEntry
private[diagnostics] final case class ProcessMetricsBeginSnapshot(
    suspended: Option[
      ProcessMetricsEntry
    ], // metrics from previous execution(s) of this DistTask that were preempted by other tasks
    // Distribution metrics
    freshJVM: Boolean,
    engineStartupMs: Long,
    metricsSnapshot: ProcessMetricsSnapshot
)

// have one case class that captures all the counter snapshots, and use same logic to populate at task begin and end
// to avoid having duplicate logic for many of the different counter types
private[diagnostics] final case class ProcessMetricsSnapshot(
    snapTime: Instant,
    // Optimus scheduler metrics (available when profileTime = true)
    totalGraphTime: Long,
    totalUnderUtilizedTime: Long,
    // cache counters
    cacheCountersMap: Map[CacheGroup, CacheCountersSnapshot],
    // OS metrics
    jvmCPUTime: Long, // JVM CPU time taken from OperatingSystemMXBean.getProcessCpuTime
    jvmLoad: Double, // taken from OperatingSystemMXBean.getProcessCpuLoad [not a counter, should be averaged]
    systemLoad: Double, // taken from OperatingSystemMXBean.getSystemCpuLoad [not a counter, should be averaged]
    loadAvg: Double, // taken from OperatingSystemMXBean.getSystemLoadAverage [not a counter, should be averaged]
    gcTimeStopTheWorld: Long, // stop the world GC time
    gcTimeAll: Long, // all GC time
    gcCount: Long, // all GC count
    jitTime: Long, // JIT compiler time
    clTime: Long, // Class loading time
    // Distribution metrics
    gcMonitorStats: GCMonitorStats,
    gcNativeStats: GCNativeStats,
    threadStateSnapshot: ThreadStateSnapshot,
    logSize: Long
)
