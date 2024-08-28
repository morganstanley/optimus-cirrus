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

import optimus.graph.OGSchedulerLostConcurrency.CriticalSyncStack
import optimus.graph.OGTrace
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.diagnostics.SchedulerProfileEntry
import optimus.graph.diagnostics.gridprofiler.GridProfiler.Metric
import optimus.graph.diagnostics.gridprofiler.GridProfiler.getDefaultLevel
import optimus.graph.diagnostics.pgo.Profiler
import optimus.platform.temporalSurface.tsprofiler.TemporalSurfaceProfilingData

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer

// a ProfilerData object is constructed on the grid and returned with the results of calculating a node
final case class ProfilerData(
    distStartTime: Long, // start of the dist call on the caller side
    gridWallTime: Long, // wall time on the called side
    gridEngineID: String, // engine ID, used as optional key for aggregation
    tag: Option[GridProfiler.Tag], // supplies aggregation and filtering
    taskID: String, // dist job id, the lowest-level aggregation key
    parents: Array[String], // list of parent ids of this task
    tsProfiling: Option[TemporalSurfaceProfilingData]) // temporal surface related stats
{
  // This constructor runs in the context of the engine, just before it ships back the NodeDistResult.
  // It
  // *. deletes the metrics from the engine
  // *. applies filtering (to avoid e.g. sending low self-time hotspots nobody wants to see)
  // *. applies aggregation (by depth, engine, or perhaps total)
  // *. prepares the data for serialization

  private def scope = tag.map(_.scope).getOrElse(collection.Seq())
  private def recursive = scope.lastOption.contains(RecursiveDistProfilerScope())
  private def extractedData =
    if (recursive) {
      val blk = GridProfiler.scopeToBlockID(scope)
      val outer = GridProfiler.scopeToBlockID(scope.dropRight(1))
      Map(outer -> GridProfilerData.removeByScope(blk))
    } else {
      GridProfilerData.removeAll()
    }

  val data: Map[Int, Map[String, ArrayBuffer[Any]]] = extractedData.transform((_, v) =>
    GridProfilerData
      .aggregate(v, tag.map(_.aggregation).getOrElse(AggregationType.DEFAULT))) // eager aggregation

  val processMetrics: ProcessMetricsEntry = ProcessMetricsEntry.end(log = true, task = taskID, engine = gridEngineID)

  val schedStats: Map[String, SchedulerProfileEntry] = OGTrace.getSchedulerProfiles.asScala.toMap

  // map blkId -> hotspots
  val hotspots: Map[Int, Map[String, PNodeTaskInfo]] = {
    // TODO (OPTIMUS-28314): eager filtering could be applied here as
    //  "GridProfiler.filterHotspots(row, filtering)", where filtering is
    //  "tag.map(_.hotspotFilter).getOrElse(GridProfiler.defaultHotspotFilters)", but it conflicts with PGO
    def combine(rows: collection.Seq[PNodeTaskInfo]): Map[String, PNodeTaskInfo] =
      Profiler.combinePNTIs(rows).transform { (_, pnti) =>
        pnti.nti = null // NTI does not make sense across JVM boundaries (cf. PNTi.readObject)
        pnti
      }

    val level = getDefaultLevel
    // These >= comparisons assume strictly linear increase in profiler modes capabilities with direct capability checks
    // we will move to something like OGTraceMode.find(level).supportsProfileBlocks(), which needs a new find() that take a Level
    if (level >= Level.HOTSPOTSLIGHT) {
      // raw ogtrace representation of scoped hotspots: array of arrays, where
      // outer index is block id (per-scope), inner index is profile id (per-property). Contains nulls.
      val raw = OGTrace.liveReader().getRawScopedTaskInfos

      // Main loop: converting the array of PNTI[] into a map BlockID -> Option(ScopeName), PNTI{}
      val rFiltered = raw.asScala.zipWithIndex.filter { case (pntis, scope) =>
        (pntis ne null) && // skip scopes for which no hotspots were recorded
        (scope != OGTrace.BLOCK_ID_PROPERTY) && // skip the special row (val props)
        // for recursive dist only report for recursive scope (and the ones nested)
        // for non-recursive dist, don't report recursive (and further nested) since they are now in GridProfilerData
        (recursive == GridProfiler.blockIDToScope(scope).flatMap(_.lastOption).contains(RecursiveDistProfilerScope()))
      }
      val rByScope = rFiltered
        .map { case (_, blockID) => (blockID, OGTrace.liveReader().getScopedTaskInfos(blockID, false)) }
        .filter { case (_, pntis) => pntis.size() > 0 }
        .map { case (blockID, pntis) => (blockID, combine(pntis.asScala)) }

      rByScope.toMap
    } else Map.empty[Int, Map[String, PNodeTaskInfo]]
  }
  // The maps above have Ints as keys, which are only meaningful in this JVM
  // these registries offer mapping to portable representations
  val scopeMap: Map[Int, collection.Seq[ProfilerScope]] = GridProfiler.getScopeRegistry.map(p => p._2 -> p._1).toMap
  val metricMap: Map[Int, String] = GridProfiler.getMetricsRegistry.map(p => p._2 -> p._1).toMap

  // all future tasks that return from this JVM will have freshJVM = false
  ProcessMetricsEntry.freshJVM = false // review mutation in constructor

  def cacheTime: Long = schedStats.map { case (_, stats) => stats.cacheTime }.sum

  // for this taskID only (aggregates all entries)
  def getDalStatsSummary: GridProfiler.Metric.DALStats = metricsForTask(Metric.DALSTATS)
  def perSchedulerSummary: Map[String, SchedulerProfileEntry] =
    GridProfiler.getPerSchedulerProfiles(Map(taskID -> schedStats))
  def syncStacksSummary: Map[String, CriticalSyncStack] = metricsForTask(Metric.SYNCSTACKS)
  def stallTimeSummary: Map[String, Long] = metricsForTask(Metric.STALLTIME)
  def hotSpotSummary: Map[String, PNodeTaskInfo] = Metric.HOTSPOTS.sum(hotspots.values)

  // Incoming metrics can have
  // taskid = over.taskId, for what was collected within that engine process on-graph
  // taskid = clientKey, for what was collected within that engine process off-graph
  // or some other taskid, for what was gathered and passed on from recursive distribution
  private[optimus] def matches(task: String): Boolean = task == GridProfiler.clientKey || task == taskID

  /** Filter out data matching this task only and the given metric id */
  private def metricsForTask[T <: AnyRef](metric: GridProfiler.Metric.MetricVal[T]): T = {
    val ts1 = data.flatMap { case (_, taskMetrics) =>
      taskMetrics.filter { case (task, _) => matches(task) }.flatMap { case (_, metrics) =>
        val id = metric.id
        if (id < metrics.length) Some(metrics(id).asInstanceOf[T]) else None
      }
    }
    val ts = ts1.filter(metric.checkClass)
    metric.sum(ts)
  }
}
