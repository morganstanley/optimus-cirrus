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

import optimus.breadcrumbs.crumbs.Properties.profDmcCacheAttemptCount
import optimus.breadcrumbs.crumbs.Properties.profDmcCacheComputeCount
import optimus.breadcrumbs.crumbs.Properties.profDmcCacheDeduplicationCount
import optimus.breadcrumbs.crumbs.Properties.profDmcCacheHitCount
import optimus.breadcrumbs.crumbs.Properties.profDmcCacheMissCount

import java.text.NumberFormat
import optimus.graph.NodeTask
import optimus.graph.diagnostics.gridprofiler.DmcProfilingLevel.DmcProfilingLevel
import optimus.graph.diagnostics.gridprofiler.GridProfiler.printString
import optimus.graph.diagnostics.gridprofiler.Level.Level
import optimus.graph.diagnostics.sampling.BaseSamplers
import optimus.platform._

object DmcClientMetrics {
  private val log = msjava.slf4jutils.scalalog.getLogger(this)

  val DmcCacheReuseRatio = "dmcCacheReuseRatio"
  val DmcCacheChurnRate = "dmcCacheChurnRate"
  val DmcCacheAttempt = "dmcCacheAttempt"
  val DmcCacheMiss = "dmcCacheMiss"
  val DmcCacheHit = "dmcCacheHit"
  val DmcCacheCompute = "dmcCacheCompute"
  val DmcCacheDeduplication = "dmcCacheDeduplication"
  val DmcCacheConsumption = "dmcCacheConsumption" // actual consumption
  val DmcComputedKeyHash = "dmcComputedKeyHash"

  val DmcCacheComputeRatio = CustomRegressionMetrics.dmcCacheComputeRatio
  val DmcCacheMaxConsumption =
    CustomRegressionMetrics.dmcCacheMaxConsumption // max potential consumption, assume no reuse

  val ProfilingLevelProp = "optimus.dmc.profilingLevel"
  private var defaultProfilingLevel: Option[DmcProfilingLevel] = None
  def setDefaultProfilingLevel(d: DmcProfilingLevel): Unit = defaultProfilingLevel = Some(d)
  def getProfilingLevel(node: NodeTask): DmcProfilingLevel = {
    val ss = node.scenarioStack() // nullable
    val maybeDetail = Option(ss)
      .filter(_.hasPluginTag(DmcDetailedProfiling))
      .map(_ => DmcProfilingLevel.detailed)
    val maybeBasic = Option(ss)
      .filter(_.hasPluginTag(DmcBasicProfiling))
      .map(_ => DmcProfilingLevel.basic)
    maybeDetail orElse maybeBasic orElse defaultProfilingLevel getOrElse DmcProfilingLevel.off
  }
  def getProfilingLevel(profilingTags: Seq[PluginTagKeyValue[Level]]): DmcProfilingLevel = {
    val maybeDetail = profilingTags
      .find(_.key == DmcDetailedProfiling)
      .map(_ => DmcProfilingLevel.detailed)
    val maybeBasic = profilingTags
      .find(_.key == DmcBasicProfiling)
      .map(_ => DmcProfilingLevel.basic)
    maybeDetail orElse maybeBasic orElse defaultProfilingLevel getOrElse DmcProfilingLevel.off
  }

  def tagForLevel(level: DmcProfilingLevel): Option[ForwardingPluginTagKey[Level]] = level match {
    case DmcProfilingLevel.basic    => Some(DmcBasicProfiling)
    case DmcProfilingLevel.detailed => Some(DmcDetailedProfiling)
    case _                          => None
  }

  def customMetricsForLevel(level: DmcProfilingLevel): Seq[String] = level match {
    case DmcProfilingLevel.basic =>
      Seq(
        DmcCacheReuseRatio,
        DmcCacheComputeRatio,
        DmcCacheChurnRate,
        DmcCacheAttempt,
        DmcCacheMiss,
        DmcCacheHit,
        DmcCacheCompute,
        DmcCacheDeduplication,
        DmcCacheConsumption,
        DmcCacheMaxConsumption
      )
    case DmcProfilingLevel.detailed =>
      Seq(
        DmcCacheReuseRatio,
        DmcCacheComputeRatio,
        DmcCacheChurnRate,
        DmcCacheAttempt,
        DmcCacheMiss,
        DmcCacheHit,
        DmcCacheCompute,
        DmcCacheDeduplication,
        DmcCacheConsumption,
        DmcCacheMaxConsumption,
        DmcComputedKeyHash
      )
    case _ => Seq.empty
  }

  def logCacheAttempt(node: NodeTask): Unit = {
    BaseSamplers.increment(profDmcCacheAttemptCount, 1)
    if (getProfilingLevel(node) >= DmcProfilingLevel.basic) {
      GridProfiler.recordCustomCounter(DmcCacheAttempt, 1, node = node)
    }
  }

  def logCacheMiss(node: NodeTask): Unit = {
    BaseSamplers.increment(profDmcCacheMissCount, 1)
    if (getProfilingLevel(node) >= DmcProfilingLevel.basic) {
      GridProfiler.recordCustomCounter(DmcCacheMiss, 1, node = node)
    }
  }

  def logCacheHit(node: NodeTask, valueSize: Int): Unit = {
    BaseSamplers.increment(profDmcCacheHitCount, 1)
    if (getProfilingLevel(node) >= DmcProfilingLevel.basic) {
      GridProfiler.recordCustomCounter(DmcCacheHit, 1, node = node)
      GridProfiler.recordCustomCounter(DmcCacheMaxConsumption, valueSize.toLong, node = node)
    }
  }

  def logCacheCompute(node: NodeTask, keyspaceWithScenario: Int, keyHash: String, valueSize: Int): Unit = {
    BaseSamplers.increment(profDmcCacheComputeCount, 1)
    if (getProfilingLevel(node) >= DmcProfilingLevel.basic) {
      val hash = keyspaceWithScenario.toHexString + "_" + keyHash
      GridProfiler.recordCustomCounter(DmcCacheCompute, 1, node = node)
      GridProfiler.recordCustomCounter(DmcCacheConsumption, valueSize.toLong, node = node)
      GridProfiler.recordCustomCounter(DmcCacheMaxConsumption, valueSize.toLong, node = node)

      if (getProfilingLevel(node) >= DmcProfilingLevel.detailed) {
        GridProfiler.recordCustomCounter(DmcComputedKeyHash, Set(hash), node = node)
      }
    }
  }

  def logCacheDeduplication(nodes: Seq[NodeTask], valueSizes: Seq[Int]): Unit = {
    BaseSamplers.increment(profDmcCacheDeduplicationCount, 1)
    // record count for every deduplicated node
    (nodes zip valueSizes).foreach { case (node, valueSize) =>
      if (getProfilingLevel(node) >= DmcProfilingLevel.basic) {
        GridProfiler.recordCustomCounter(DmcCacheDeduplication, 1, node = node)
        GridProfiler.recordCustomCounter(DmcCacheMaxConsumption, valueSize.toLong, node = node)
      }
    }
  }

  def keyToSummary(
      flatPR: SingleScopeProfilerResult,
      agg: AggregationType.Value): Map[String, DmcClientMetricsSummary] =
    flatPR.customCounters(agg).map { case (key, customCounters) => key -> summary(customCounters) }

  def summary(pr: ProfilerResult[_]): DmcClientMetricsSummary = summary(pr.flat.customCounters)

  // DmcCacheAttempt is a metric in both basic and detailed DmcProfilingLevel
  def hasDmcMetrics(customMetrics: Array[String]): Boolean = customMetrics.contains(DmcCacheAttempt)

  def summary(customCounters: Map[String, Any]): DmcClientMetricsSummary = {
    val attempts = customCounters.get(DmcCacheAttempt).collect { case c: Int => c }.getOrElse(0)
    val hits = customCounters.get(DmcCacheHit).collect { case c: Int => c }.getOrElse(0)
    val misses = customCounters.get(DmcCacheMiss).collect { case c: Int => c }.getOrElse(0)
    val computes = customCounters.get(DmcCacheCompute).collect { case c: Int => c }.getOrElse(0)
    val deduplications = customCounters.get(DmcCacheDeduplication).collect { case c: Int => c }.getOrElse(0)
    val consumptions = customCounters.get(DmcCacheConsumption).collect { case c: Long => c }.getOrElse(0L)
    val maxConsumptions = customCounters.get(DmcCacheMaxConsumption).collect { case c: Long => c }.getOrElse(0L)
    val hashes = customCounters
      .get(DmcComputedKeyHash)
      .collect { case hs: Set[String] @unchecked => hs }
      .getOrElse(Set.empty)
      .toSeq
    DmcClientMetricsSummary(attempts, hits, misses, computes, deduplications, consumptions, maxConsumptions, hashes)
  }

  final case class DmcClientMetricsSummary(
      attempts: Int,
      hits: Int,
      misses: Int,
      computes: Int,
      deduplications: Int,
      consumptions: Long,
      maxConsumptions: Long,
      hashes: Seq[String]) {

    private val pf = NumberFormat.getPercentInstance
    pf.setMaximumFractionDigits(1)
    private val nf = NumberFormat.getNumberInstance
    nf.setMaximumFractionDigits(2)

    def +(e: DmcClientMetricsSummary): DmcClientMetricsSummary = DmcClientMetricsSummary(
      attempts + e.attempts,
      hits + e.hits,
      misses + e.misses,
      computes + e.computes,
      deduplications + e.deduplications,
      consumptions + e.consumptions,
      maxConsumptions + e.maxConsumptions,
      hashes ++ e.hashes
    )
    def reuses: Int = hits + deduplications
    def reuseRatio: Double = {
      if (attempts > 0) reuses.toDouble / attempts.toDouble
      else -1
    }
    def computeRatio: Double = {
      if (attempts > 0) computes.toDouble / attempts.toDouble
      else -1
    }
    def churnRate: Double = {
      // calculate churn rate (if data is available)
      if (hashes.nonEmpty) hashes.length.toDouble / hashes.distinct.length.toDouble
      else -1
    }

    def reuseRatioReport: String = if (reuseRatio >= 0) pf.format(reuseRatio) else "n/a"
    def computeRatioReport: String = if (computeRatio >= 0) pf.format(computeRatio) else "n/a"
    def churnRateReport: String = if (churnRate >= 0) nf.format(churnRate) + "x" else "n/a"
    def consumptionsReport: String = nf.format(consumptions.toDouble / 1048576)
    def maxConsumptionsReport: String = nf.format(maxConsumptions.toDouble / 1048576)

    def toPrettyString: String = {

      val dmcMetrics = printString { ps =>
        ps.println()
        ps.println("=========== DMC Cache Stats ============")

        ps.println(String.format("%-20s: %s", "Reuse Ratio", reuseRatioReport))
        ps.println(String.format("%-20s: %s", "Compute Ratio", computeRatioReport))
        ps.println(String.format("%-20s: %s", "Churn Rate", churnRateReport))
        ps.println(String.format("%-20s: %s MB", "Consumptions", consumptionsReport))
        ps.println(String.format("%-20s: %s MB", "Max Consumptions", maxConsumptionsReport))

        ps.println()
        ps.println("Drill down:")
        ps.println(String.format("  %-18s: %s", "Attempts", String.valueOf(attempts)))
        ps.println(String.format("  %-18s: %s", "Reuses", String.valueOf(hits + deduplications))) // reuses
        ps.println(String.format("  %-18s: %s", "Computes", String.valueOf(computes))) // computes
        ps.println(String.format("  %-18s: %s", "Deduplications", String.valueOf(deduplications))) // deduplications
        ps.println(String.format("  %-18s: %s", "Hits", String.valueOf(hits))) // hits
        ps.println(String.format("  %-18s: %s", "Misses", String.valueOf(misses))) // misses

        ps.println("=========================================")
      }
      dmcMetrics
    }

    def post(node: NodeTask): Unit = {
      GridProfiler.recordCustomCounter(DmcCacheReuseRatio, reuseRatioReport, node = node)
      GridProfiler.recordCustomCounter(DmcCacheComputeRatio, computeRatioReport, node = node)
      GridProfiler.recordCustomCounter(DmcCacheChurnRate, churnRateReport, node = node)
    }
  }
  object DmcClientMetricsSummary {
    val Empty: DmcClientMetricsSummary = DmcClientMetricsSummary(0, 0, 0, 0, 0, 0L, 0L, Seq.empty)

    def toMap(summary: DmcClientMetricsSummary): Map[String, Long] = Map(
      DmcCacheAttempt -> summary.attempts,
      DmcCacheHit -> summary.hits,
      DmcCacheMiss -> summary.misses,
      DmcCacheCompute -> summary.computes,
      DmcCacheDeduplication -> summary.consumptions,
      DmcCacheMaxConsumption -> summary.maxConsumptions
    )

    def fromMap(map: Map[String, Long]): DmcClientMetricsSummary = {
      DmcClientMetricsSummary(
        attempts = map.getOrElse(DmcCacheAttempt, 0L).toInt,
        hits = map.getOrElse(DmcCacheHit, 0L).toInt,
        misses = map.getOrElse(DmcCacheMiss, 0L).toInt,
        computes = map.getOrElse(DmcCacheCompute, 0L).toInt,
        deduplications = map.getOrElse(DmcCacheDeduplication, 0L).toInt,
        consumptions = map.getOrElse(DmcCacheConsumption, 0L),
        maxConsumptions = map.getOrElse(DmcCacheMaxConsumption, 0L),
        hashes = Seq.empty
      )
    }
  }

}

@embeddable case object DmcBasicProfiling extends ForwardingPluginTagKey[Level]
@embeddable case object DmcDetailedProfiling extends ForwardingPluginTagKey[Level]

object DmcProfilingLevel extends Enumeration {
  type DmcProfilingLevel = Value
  val off, basic, detailed = Value
}
