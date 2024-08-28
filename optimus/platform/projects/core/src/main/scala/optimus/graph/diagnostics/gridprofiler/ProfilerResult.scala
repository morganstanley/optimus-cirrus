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

import java.io.BufferedWriter
import java.io.OutputStream
import java.io.OutputStreamWriter
import java.io.PrintStream
import optimus.graph.CancellationScope
import optimus.graph.Node
import optimus.graph.NodeResultNodeImpl
import optimus.graph.NodeTaskInfo
import optimus.graph.OGTrace
import optimus.graph.diagnostics.Report
import optimus.graph.diagnostics.gridprofiler.GridProfiler.DALStatsEntry
import optimus.graph.diagnostics.gridprofiler.GridProfiler.defaultCombOp
import optimus.graph.diagnostics.gridprofiler.GridProfilerUtils._
import optimus.graph.diagnostics.gridprofiler.GridProfiler.Metric
import optimus.graph.diagnostics.pgo.Profiler
import optimus.platform.NodeResult

import scala.jdk.CollectionConverters._
import scala.collection.{mutable => m}
import optimus.scalacompat.collection._

import java.nio.file.Paths

// This is the result of a profiled() { ... } block
// It exposes the metrics recorded for its scope, and for all nested scopes.
// Technically, this is a proxy into GridProfiler.data
class ProfilerResult[T](node: Node[T], envStart: Option[ProcessMetricsBeginSnapshot], val wallTime: Long)
    extends NodeResult(node) {

  // the top-level scope of this profiled block
  private val scope: Int = GridProfiler.scopeForNode(node)
  private val scopeName: collection.Seq[ProfilerScope] = GridProfiler.blockIDToScope(scope).get
  def getScope: collection.Seq[ProfilerScope] = scopeName

  // the top-level scope and all of the nested scopes of this profiled block
  private[diagnostics] val scopes: collection.Seq[Int] =
    GridProfiler.getScopeRegistry.filter(_._1.startsWith(scopeName)).map(_._2)
  def getScopes: collection.Seq[collection.Seq[ProfilerScope]] = scopes.map(x => GridProfiler.blockIDToScope(x).get)

  // this is the golden source of local scoped hotspots for this profiled block. Remote hotspots are in GridProfiler data
  // Note: Profiler.getProfilerData is live, and may change if more hotspots are entered under the same scope
  // likewise, GridProfiler data is live and may change if more remote metrics arrive over distribution under the same scope
  private def localHotspots: Map[Int, Metric.Hotspots] =
    scopes.map(s => s -> Profiler.getLocalHotspots(s)).toMap

  // inclusive metrics:
  // these metrics are for attribution to nested scopes. On the outermost scope, they miss the total graph time
  private val immediateLocalEnvMetrics = {
    val start = envStart.getOrElse(ProcessMetricsEntry.processMetricsBegin)
    val result = ProcessMetricsEntry.end(start)
    result.copy(totalGraphTime = wallTime)
  }

  def flat: SingleScopeProfilerResult = {
    // local hotspots come from a different source
    def flatHotspots: Metric.Hotspots =
      Profiler.combinePNTIs(localHotspots.values.toSeq.flatMap(_.values))

    new SingleScopeProfilerResult(
      scopes,
      immediateLocalEnvMetrics,
      flatHotspots
    )
  }

  def get(sc: collection.Seq[ProfilerScope]): SingleScopeProfilerResult = get(GridProfiler.scopeToBlockID(sc))

  private def get(sc: Int): SingleScopeProfilerResult =
    new SingleScopeProfilerResult(
      collection.Seq(sc),
      immediateLocalEnvMetrics,
      localHotspots(sc)
    )

  def flatAt(sc: collection.Seq[ProfilerScope]): SingleScopeProfilerResult = {
    val subScopes = getScopes.filter(_.startsWith(sc)).map(GridProfiler.scopeToBlockID)

    // local hotspots come from a different source
    def flatHotspots: Metric.Hotspots =
      Profiler.combinePNTIs(localHotspots.filterKeysNow(subScopes.toSet).values.toSeq.flatMap(_.values))

    new SingleScopeProfilerResult(
      subScopes,
      immediateLocalEnvMetrics,
      flatHotspots
    )
  }

  // returns the formatted summaries as Seq(Scope, Option(Table))
  def getSummary: collection.Seq[(collection.Seq[ProfilerScope], Option[String])] =
    scopes map (s => GridProfiler.blockIDToScope(s).get -> get(s).getSummary)

  // Prints the summaries as
  //
  // scope1:
  // header
  // -----
  // content
  //
  // scope1.nested1:
  // header
  // -----
  // content
  def printSummary(w: OutputStream): ProfilerResult[T] = {
    getSummary foreach (str => {
      val bw = new BufferedWriter(new OutputStreamWriter(w))
      str._2.foreach(s => bw.write(s"${str._1}:\n$s\n\n"))
      bw.flush()
    })
    this
  }

  // drops the same set of CSVs as the shutdown hook, but as relating to this profiled block only
  def writeCSV(dir: String): Unit = {
    val tm1 = System.currentTimeMillis()
    val csvFolder = outputPath(generateFileName(), dir)
    val csvExtractor = new CSVResultsExtractor(GridProfiler.getCSV(Option(this)))
    Report.writeCSVFilesAndCrumbs(Paths.get(csvFolder), generateFileName(), csvExtractor)
    val tm2 = System.currentTimeMillis()
    GridProfiler.log.info(s"elapsed in writeCSV: ${tm2 - tm1} ms. metrics: ${GridProfiler.statistics()}")
  }
}

private[optimus] class ProfilerResultNode[T](
    cn: Node[T],
    cs: CancellationScope,
    startEnvMetrics: Option[ProcessMetricsBeginSnapshot])
    extends NodeResultNodeImpl[T, ProfilerResult[T]](cn, cs) {

  private val startTime: Long = OGTrace.nanoTime()

  override def executionInfo: NodeTaskInfo = NodeTaskInfo.ProfilerResult

  /** Override this method since we are wrapping the result of another node and don't want to share profileId */
  override def subProfile(): AnyRef = cn
  override protected def resultFromNode(node: Node[T]): ProfilerResult[T] = {
    val wallTime = OGTrace.nanoTime() - startTime
    GridProfiler.recordWallTime(wallTime)
    new ProfilerResult[T](node, startEnvMetrics, wallTime)
  }
}

// ProfilerResult.flat and ProfilerResult.get return this object representing single-scope (or flattened across all scopes) results.
class SingleScopeProfilerResult(
    scopes: collection.Seq[Int], // just one scope for ProfilerResult.get, a sequence for ProfilerResult.flat
    localEnvArg: => Metric.ProcessMetrics,
    localHotArg: => Metric.Hotspots) {

  private def extractMetric[T](key: Int): Map[String, T] = {
    val res = m.Map.empty[String, T]

    for (
      (_, row) <- GridProfilerData.get[T](scopes, key); // Map[Scope, Map[Task, T]]
      (task, v) <- row
    )
      res.get(task) match {
        case Some(old) => res(task) = defaultCombOp(old, v)
        case None      => res(task) = v
      }

    res.toMap
  }

  private def extractCustomMetrics: Map[String, Map[String, Any]] = {
    val res = m.Map.empty[String, m.Map[String, Any]]
    def empty = m.Map.empty[String, Any]

    for (
      (_, row) <- GridProfilerData.getCustom(scopes); // Map[Scope, Map[Task, Map[Key, Any]]]
      (task, custom) <- row; //  Map[Task, Map[Key, Any]]
      (key, v) <- custom
    ) {
      val newRow = res.getOrElseUpdate(task, empty)
      newRow.get(key) match {
        case Some(old) => newRow(key) = defaultCombOp(old, v)
        case None      => newRow(key) = v
      }
    }

    res.mapValuesNow(_.toMap).toMap
  }

  private[diagnostics] def allDalStats = extractMetric[Metric.DALStats](Metric.DALSTATS.id)
  def dalStats(agg: AggregationType.Value): Map[String, Metric.DALStats] =
    GridProfilerData.aggregateSingle(allDalStats, None, agg)
  def dalStats: Metric.DALStats =
    dalStats(AggregationType.AGGREGATED).getOrElse(GridProfiler.aggregatedKey, DALStatsEntry(0, 0, 0))

  private lazy val localSchedStats: Metric.Scheduler = OGTrace.getSchedulerProfiles.asScala.toMap
  private[diagnostics] def allSchedStats: Map[String, Metric.Scheduler] =
    extractMetric[Metric.Scheduler](Metric.SCHEDULER.id)
  def schedStats(agg: AggregationType.Value): Map[String, Metric.Scheduler] =
    GridProfilerData.aggregateSingle(allSchedStats, Some(localSchedStats), agg)
  def schedStats: Metric.Scheduler =
    schedStats(AggregationType.AGGREGATED).getOrElse(GridProfiler.aggregatedKey, Map.empty)

  // Returns the critical sync stacks for this id and scope
  private[diagnostics] def allSyncStacks: Map[String, Metric.SyncStacks] =
    extractMetric[Metric.SyncStacks](Metric.SYNCSTACKS.id)
  def syncStacks(agg: AggregationType.Value): Map[String, Metric.SyncStacks] =
    GridProfilerData.aggregateSingle(allSyncStacks, None, agg)
  def syncStacks: Metric.SyncStacks =
    syncStacks(AggregationType.AGGREGATED).getOrElse(GridProfiler.aggregatedKey, Map.empty)

  private[diagnostics] def allVTTTStacks: Map[String, Metric.VTTTStacks] =
    extractMetric[Metric.VTTTStacks](Metric.VTTTSTACKS.id)
  def vtTtStacks(agg: AggregationType.Value): Map[String, Metric.VTTTStacks] =
    GridProfilerData.aggregateSingle(allVTTTStacks, None, agg)
  def vtTtStacks: Metric.VTTTStacks =
    vtTtStacks(AggregationType.AGGREGATED).getOrElse(GridProfiler.aggregatedKey, Map.empty)

  // Returns the user-defined custom metrics for this id and scope
  private[diagnostics] def allCustomCounters: Map[String, Map[String, Any]] = extractCustomMetrics
  def customCounters(agg: AggregationType.Value): Map[String, Map[String, Any]] =
    GridProfilerData.aggregateSingle(allCustomCounters, None, agg)
  def customCounters: Map[String, Any] =
    customCounters(AggregationType.AGGREGATED).getOrElse(GridProfiler.aggregatedKey, Map.empty)

  // graph stall events (no threads made progress) for this id and scope
  private[diagnostics] def allStallTimes: Map[String, Metric.StallTime] =
    extractMetric[Metric.StallTime](Metric.STALLTIME.id)
  def stallTimes(agg: AggregationType.Value): Map[String, Map[String, Long]] =
    GridProfilerData.aggregateSingle(allStallTimes, None, agg)
  def stallTimes: Metric.StallTime =
    stallTimes(AggregationType.AGGREGATED).getOrElse(GridProfiler.aggregatedKey, Map.empty)

  // Environment metrics. See GridProfiler.getSummary for details on how to interpret the values returned by this API
  // lazy because at the time this constructor runs, graph is not yet exited even for top-level profiled block, and NodeTrace.totalGraphTime is not ready
  private[diagnostics] def allEnvMetrics: Map[String, Metric.ProcessMetrics] =
    extractMetric[Metric.ProcessMetrics](Metric.PROCESS.id)
  def envMetrics(agg: AggregationType.Value): Map[String, Metric.ProcessMetrics] =
    GridProfilerData.aggregateSingle(allEnvMetrics, Some(localEnvArg), agg)
  def envMetrics: Metric.ProcessMetrics =
    envMetrics(AggregationType.AGGREGATED).getOrElse(GridProfiler.aggregatedKey, ProcessMetricsEntry.empty)

  // Hotspots: time/cache profiling details per node type
  private[diagnostics] def allHotspots: Map[String, Metric.Hotspots] =
    extractMetric[Metric.Hotspots](Metric.HOTSPOTS.id)
  def hotspots(agg: AggregationType.Value): Map[String, Metric.Hotspots] =
    GridProfilerData.aggregateSingle(allHotspots, Some(localHotArg), agg)
  def hotspots: Metric.Hotspots =
    hotspots(AggregationType.AGGREGATED).getOrElse(GridProfiler.aggregatedKey, Map.empty)

  def getKeyToSummaryTable(agg: AggregationType.Value): Map[String, SummaryTable] = {
    val keyToDalStats = dalStats(agg)
    val keyToSchedStats = schedStats(agg)
    val keyToSyncStacks = syncStacks(agg)
    val keyToStallTimes = stallTimes(agg)
    val keyToEnvMetrics = envMetrics(agg)
    val keyToHotspots = hotspots(agg)
    val keys =
      keyToDalStats.keySet ++ keyToSchedStats.keySet ++ keyToSyncStacks.keySet ++ keyToStallTimes.keySet ++ keyToEnvMetrics.keySet ++ keyToHotspots.keySet
    keys.map { key =>
      key -> GridProfiler.getSummaryTable(
        keyToDalStats.getOrElse(key, DALStatsEntry.empty),
        keyToSchedStats.getOrElse(key, Map.empty),
        keyToSyncStacks.getOrElse(key, Map.empty),
        keyToStallTimes.getOrElse(key, Map.empty),
        keyToEnvMetrics.getOrElse(key, ProcessMetricsEntry.empty),
        keyToHotspots.getOrElse(key, Map.empty)
      )
    }.toMap
  }

  /* Pretty-prints the profiler summary. Returns None if profiling is turned off
   */
  def getSummary: Option[String] =
    if (GridProfiler.getDefaultLevel != Level.NONE) {
      val table = GridProfiler.getSummaryTable(dalStats, schedStats, syncStacks, stallTimes, envMetrics, hotspots)
      Some(GridProfiler.getSummary(table))
    } else None // do nothing if there is no tag, or the tag is NONE

  /* Pretty-prints the profiler summary. Usage: profiled {..}.printSummary(System.out)
   * Note to have local totalGraphTime available, this must be called off-graph, that is, outside of any given block.
   */
  def printSummary(w: OutputStream): SingleScopeProfilerResult = {
    getSummary foreach (str => {
      val bw = new BufferedWriter(new OutputStreamWriter(w))
      bw.write(str)
      bw.flush()
    })
    this
  }

  /*  Dump profiler info to a CSV file. All times are in ms
   *  Usage: profiled {..}.saveProfile(new FileOutputStream("somefile.csv")) (the stream is closed by this function)
   */
  def saveProfile(w: OutputStream): SingleScopeProfilerResult = {
    if (GridProfiler.getDefaultLevel != Level.NONE) {
      val ps = new PrintStream(w)
      try {
        val res = GridProfiler.getCSV(None)
        ps.print(res.dalStats)
        ps.print(res.customCountersNonPriql)
        ps.print(res.customCountersPriql)
        ps.print(res.syncStacks)
        ps.print(res.vtTtStacks)
        ps.print(res.stallTimes)
        ps.print(res.environMetrics)
        ps.print(res.threadStats)
      } finally ps.close()
    }
    this
  }
}
