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
package optimus.graph.diagnostics.configmetrics

import java.io.File
import java.io.StringWriter
import java.nio.file.Paths
import com.opencsv.CSVWriter
import optimus.config.NodeCacheConfigs
import optimus.graph.diagnostics.gridprofiler.GridProfiler.log
import optimus.graph.diagnostics.gridprofiler.GridProfiler.printString
import optimus.graph.diagnostics.gridprofiler.GridProfilerUtils._
import optimus.graph.diagnostics.pgo.CacheOffStatistics

import scala.jdk.CollectionConverters._
import scala.io.Source

final case class ConfigMetrics(
    wallTime: Long,
    cpuTime: Long,
    cacheTime: Long,
    maxHeap: Long,
    cacheHits: Long,
    cacheMisses: Long,
    evictions: Long,
    dalRequests: Long,
    enginesWallTime: Long) {

  val cacheHitRatio = {
    val totalRequests = cacheHits + cacheMisses
    if (totalRequests > 0) cacheHits.toDouble / totalRequests else 0
  }

  override def toString =
    s"wall time = ${wallTime / 1e9}, CPU time = ${cpuTime / 1e9}, cache time = ${cacheTime / 1e9}, " +
      s"max heap (MB) = ${maxHeap / (1024.0 * 1024.0)}, " +
      s"cache hits = $cacheHits, cache misses = $cacheMisses, cache hit ratio = $cacheHitRatio, evictions = $evictions, " +
      s"dalRequests = $dalRequests"

  def toJson(): String = {
    def writeNFStr(value: Long): String = if (value == Long.MinValue) "NF" else value.toString

    s"{ wallTime: '${writeNFStr(wallTime)}', cpuTime: '${writeNFStr(cpuTime)}', cacheTime: '${writeNFStr(cacheTime)}', " +
      s"cacheHits: '${writeNFStr(cacheHits)}', cacheMisses: '${writeNFStr(cacheMisses)}', cacheHitRatio: ${cacheHitRatio}, " +
      s"evictions: '${writeNFStr(evictions)}', maxHeap: '${writeNFStr(maxHeap)}', dalRequests: '${writeNFStr(dalRequests)}', " +
      s"engineWallTime: '${writeNFStr(enginesWallTime)}' }"
  }
}

final case class CacheMetrics(
    cacheTime: Double,
    cacheHits: Double,
    cacheMisses: Double,
    evictions: Double,
    selfTime: Double,
    totalTime: Double) {

  val cacheHitRatio = {
    val totalRequests = cacheHits + cacheMisses
    if (totalRequests > 0) cacheHits / totalRequests else 0
  }
}

final case class CacheMetricDiffSummary(
    percentageChangeCacheTime: Double,
    percentageChangeCacheHits: Double,
    percentageChangeCacheMisses: Double,
    percentageChangeCacheHitRatio: Double,
    percentageChangeEvictions: Double,
    percentageChangeSelfTime: Double,
    percentageChangeTotalTime: Double)

final case class ThresholdDiffSummary(
    acceptWallTimeChange: Option[(Int, Boolean)],
    acceptCpuTimeChange: Option[(Int, Boolean)],
    acceptCacheTimeChange: Option[(Int, Boolean)],
    acceptMaxHeapChange: Option[(Int, Boolean)],
    acceptCacheHitsChange: Option[(Int, Boolean)],
    acceptCacheMissesChange: Option[(Int, Boolean)],
    acceptCacheHitRatioChange: Option[(Int, Boolean)],
    acceptEvictionsChange: Option[(Int, Boolean)],
    acceptDalRequestsChange: Option[(Int, Boolean)],
    acceptEngineWallTimeChange: Option[(Int, Boolean)]) {
  def applyThresholdAndToString(s: DiffSummary): (String) = {
    def formatPercentage(number: Double): String = {
      if (number == Double.MinValue) "NF" else f"${number * 100}%.2f%%"
    }
    def writeHighlight(change: Double, threshold: Option[(Int, Boolean)]): String = {
      (change == Double.MinValue, threshold) match {
        case (true, _) | (_, None) => "},"
        case (false, Some((num, checkExceed))) =>
          if (checkExceed) {
            if (change * 100 > num)
              ", highlight: true},"
            else
              "},"
          } else {
            if (change * 100 < num) ", highlight: true}," else "},"
          }
      }
    }

    s"""{ wallTime: {value: '${formatPercentage(s.percentageChangeWallTime)}'
       |${writeHighlight(s.percentageChangeWallTime, acceptWallTimeChange)}
       | cpuTime: {value: '${formatPercentage(s.percentageChangeCpuTime)}'
       |${writeHighlight(s.percentageChangeCpuTime, acceptCpuTimeChange)}
       | cacheTime: {value: '${formatPercentage(s.percentageChangeCacheTime)}'
       |${writeHighlight(s.percentageChangeCacheTime, acceptCacheTimeChange)}
       | cacheHits: {value: '${formatPercentage(s.percentageChangeCacheHits)}'
       |${writeHighlight(s.percentageChangeCacheHits, acceptCacheHitsChange)}
       | cacheMisses: {value: '${formatPercentage(s.percentageChangeCacheMisses)}'
       |${writeHighlight(s.percentageChangeCacheMisses, acceptCacheMissesChange)}
       | cacheHitRatio: {value: '${formatPercentage(s.percentageChangeCacheHitRatio)}'
       |${writeHighlight(s.percentageChangeCacheHitRatio, acceptCacheHitRatioChange)}
       | evictions: {value: '${formatPercentage(s.percentageChangeEvictions)}'
       |${writeHighlight(s.percentageChangeEvictions, acceptEvictionsChange)}
       | maxHeap: {value: '${formatPercentage(s.percentageChangeMaxHeap)}'
       |${writeHighlight(s.percentageChangeMaxHeap, acceptMaxHeapChange)}
       | dalRequests: {value: '${formatPercentage(s.percentageChangeDalRequests)}'
       |${writeHighlight(s.percentageChangeDalRequests, acceptDalRequestsChange)}
       | engineWallTime: {value: '${formatPercentage(s.percentageChangeEngineWallTime)}'
       |${writeHighlight(s.percentageChangeEngineWallTime, acceptEngineWallTimeChange)}
       |}""".stripMargin

  }
}

final case class DiffSummary(
    percentageChangeWallTime: Double,
    percentageChangeCpuTime: Double,
    percentageChangeCacheTime: Double,
    percentageChangeMaxHeap: Double,
    percentageChangeCacheHits: Double,
    percentageChangeCacheMisses: Double,
    percentageChangeCacheHitRatio: Double,
    percentageChangeEvictions: Double,
    percentageChangeDalRequests: Double,
    percentageChangeEngineWallTime: Double) {
  override def toString =
    s"Wall time ${improvementOrRegression(percentageChangeWallTime)}\n" +
      s"CPU time ${improvementOrRegression(percentageChangeCpuTime)}\n" +
      s"Cache time ${improvementOrRegression(percentageChangeCacheTime)}\n" +
      s"Max heap ${improvementOrRegression(percentageChangeMaxHeap)}\n" +
      s"Cache hits ${improvementOrRegression(percentageChangeCacheHits)}\n" +
      s"Cache misses ${improvementOrRegression(percentageChangeCacheMisses)}\n" +
      s"Cache hit ratio ${improvementOrRegression(percentageChangeCacheHitRatio)}\n" +
      s"Evictions ${improvementOrRegression(percentageChangeEvictions)}\n" +
      s"Dal Requests ${improvementOrRegression(percentageChangeDalRequests)}\n" +
      s"Engines Wall Time ${improvementOrRegression(percentageChangeEngineWallTime)}"

  private def improvementOrRegression(perc: Double): String =
    if (perc == 0) "didn't change"
    else if (perc > 0) f"increased by ${perc * 100}%2.2f%%"
    else f"decreased by ${-perc * 100}%2.2f%%"
}

final case class MetricDiff(firstRun: ConfigMetrics, secondRun: ConfigMetrics) {
  import MetricDiff._
  val percentageChangeWallTime = percentageChange(firstRun.wallTime, secondRun.wallTime)
  val percentageChangeCpuTime = percentageChange(firstRun.cpuTime, secondRun.cpuTime)
  val percentageChangeCacheTime = percentageChange(firstRun.cacheTime, secondRun.cacheTime)
  val percentageChangeMaxHeap = percentageChange(firstRun.maxHeap, secondRun.maxHeap)
  val percentageChangeCacheHits = percentageChange(firstRun.cacheHits, secondRun.cacheHits)
  val percentageChangeCacheMisses = percentageChange(firstRun.cacheMisses, secondRun.cacheMisses)
  val percentageChangeCacheHitRatio = percentageChange(firstRun.cacheHitRatio, secondRun.cacheHitRatio)
  val percentageChangeEvictions = percentageChange(firstRun.evictions, secondRun.evictions)
  val percentageChangeDalRequests = percentageChange(firstRun.dalRequests, secondRun.dalRequests)
  val percentageChangeEngineWallTime = percentageChange(firstRun.enginesWallTime, secondRun.enginesWallTime)
  val summary = DiffSummary(
    percentageChangeWallTime,
    percentageChangeCpuTime,
    percentageChangeCacheTime,
    percentageChangeMaxHeap,
    percentageChangeCacheHits,
    percentageChangeCacheMisses,
    percentageChangeCacheHitRatio,
    percentageChangeEvictions,
    percentageChangeDalRequests,
    percentageChangeEngineWallTime
  )
  val pgoDecisionSummary = Seq(
    DecisionMetric("CPU Change", percentageChangeCpuTime),
    DecisionMetric("Cache Time", percentageChangeCacheTime),
    DecisionMetric("Max Heap", percentageChangeMaxHeap),
    DecisionMetric("Dal Requests", percentageChangeDalRequests)
  )
  def prettyPrintStats: String = {
    val stats = s"${pgoDecisionSummary.mkString("\n")}\n -------------------------\n" +
      s"${collection
          .Seq(
            DecisionMetric("Wall Time", percentageChangeWallTime),
            DecisionMetric("Cache Hits", percentageChangeCacheHits),
            DecisionMetric("Cache Misses", percentageChangeCacheMisses),
            DecisionMetric("Cache Hit Ratio", percentageChangeCacheHitRatio),
            DecisionMetric("Evictions", percentageChangeEvictions),
            DecisionMetric("Engine Wall Time", percentageChangeEngineWallTime)
          )
          .mkString("\n")}\n*************************\n\n"
    log.info(stats)
    stats
  }
  val conditionalHeapImprovement = -0.01 // at least 1% improvement in heap when relaxing accepting criteria around heap
//  maybe relax here to 4% when tests are done
  val conditionalCpuImprovement = -0.05 // at least 5% improvement in CPU required when compared with heap

  def regressedMeasures(decisionMetrics: Seq[DecisionMetric]): Seq[DecisionMetric] =
    decisionMetrics.filter(metric => metric.metricValue > 0.0)

  def nbRegressedMeasures(decisionMetrics: Seq[DecisionMetric]): Int =
    regressedMeasures(decisionMetrics).length

  def breachingMeasures(decisionMetrics: Seq[DecisionMetric], threshold: Double): Seq[DecisionMetric] =
    decisionMetrics.filter(metric => metric.metricValue > threshold)

  def nbBreachingMeasures(decisionMetrics: Seq[DecisionMetric], threshold: Double): Int =
    breachingMeasures(decisionMetrics, threshold).length

  def improvedMeasuresBy(decisionMetrics: Seq[DecisionMetric], threshold: Double): Seq[DecisionMetric] =
    decisionMetrics.filter(metric => metric.metricValue <= -threshold)

  def nbImprovedMeasures(decisionMetrics: Seq[DecisionMetric], threshold: Double): Int =
    improvedMeasuresBy(decisionMetrics, threshold).length

  override def toString = s"$firstRun\n$secondRun\n" + summary.toString
}

final case class DecisionMetric(displayName: String, metricValue: Double) {
  override def toString: String = f"$displayName = ${metricValue * 100}%.2f%%"
}

final case class CacheMetricDiff(firstRun: CacheMetrics, secondRun: CacheMetrics) {
  import MetricDiff._
  val percentageChangeCacheTime = percentageChange(firstRun.cacheTime, secondRun.cacheTime)
  val percentageChangeCacheHits = percentageChange(firstRun.cacheHits, secondRun.cacheHits)
  val percentageChangeCacheMisses = percentageChange(firstRun.cacheMisses, secondRun.cacheMisses)
  val percentageChangeCacheHitRatio = percentageChange(firstRun.cacheHitRatio, secondRun.cacheHitRatio)
  val percentageChangeEvictions = percentageChange(firstRun.evictions, secondRun.evictions)
  val percentageChangeSelfTime = percentageChange(firstRun.selfTime, secondRun.selfTime)
  val percentageChangeTotalTime = percentageChange(firstRun.totalTime, secondRun.totalTime)
  val summary = CacheMetricDiffSummary(
    percentageChangeCacheTime,
    percentageChangeCacheHits,
    percentageChangeCacheMisses,
    percentageChangeCacheHitRatio,
    percentageChangeEvictions,
    percentageChangeSelfTime,
    percentageChangeTotalTime,
  )
}

/**
 * The time metrics such as cache time, wall time, cpu time, engine wall time
 * are in milliseconds
 */
final case class EffectSummary(
    testName: String,
    nodeName: Option[String],
    before: ConfigMetrics,
    after: ConfigMetrics,
    diffs: DiffSummary,
    isBeforeDisabledCache: Option[Boolean] = None)

object EffectSummary {
  def fromNodeCacheEffectSummary(
      testName: String,
      nodeName: Option[String],
      before: CacheMetrics,
      after: CacheMetrics,
      diffs: CacheMetricDiffSummary,
      isBeforeDisabledCache: Option[Boolean] = None) = {
    val defaultConfigMetrics = ConfigMetrics(
      wallTime = Long.MinValue,
      cpuTime = Long.MinValue,
      cacheTime = Long.MinValue,
      maxHeap = Long.MinValue,
      cacheHits = Long.MinValue,
      cacheMisses = Long.MinValue,
      evictions = Long.MinValue,
      dalRequests = Long.MinValue,
      enginesWallTime = Long.MinValue
    )
    val beforeConfigMetrics =
      if (before.cacheHits == Double.MinValue)
        defaultConfigMetrics
      else {
        // because the time metrics parsed from hotspots file is in ms, keep at ms
        ConfigMetrics(
          wallTime = before.totalTime.toLong,
          cpuTime = before.selfTime.toLong,
          cacheTime = before.cacheTime.toLong,
          maxHeap = Long.MinValue,
          cacheHits = before.cacheHits.toLong,
          cacheMisses = before.cacheMisses.toLong,
          evictions = before.evictions.toLong,
          dalRequests = Long.MinValue,
          enginesWallTime = Long.MinValue
        )
      }
    val afterConfigMetrics =
      if (after.cacheHits == Double.MinValue)
        defaultConfigMetrics
      else
        ConfigMetrics(
          wallTime = after.totalTime.toLong,
          cpuTime = after.selfTime.toLong,
          cacheTime = after.cacheTime.toLong,
          maxHeap = Long.MinValue,
          cacheHits = after.cacheHits.toLong,
          cacheMisses = after.cacheMisses.toLong,
          evictions = after.evictions.toLong,
          dalRequests = Long.MinValue,
          enginesWallTime = Long.MinValue
        )
    val diffsSummary = DiffSummary(
      percentageChangeWallTime = diffs.percentageChangeTotalTime,
      percentageChangeCpuTime = diffs.percentageChangeSelfTime,
      percentageChangeCacheTime = diffs.percentageChangeCacheTime,
      percentageChangeMaxHeap = Double.MinValue,
      percentageChangeCacheHits = diffs.percentageChangeCacheHits,
      percentageChangeCacheMisses = diffs.percentageChangeCacheMisses,
      percentageChangeCacheHitRatio = diffs.percentageChangeCacheHitRatio,
      percentageChangeEvictions = diffs.percentageChangeEvictions,
      percentageChangeDalRequests = Double.MinValue,
      percentageChangeEngineWallTime = Double.MinValue
    )
    EffectSummary(
      testName,
      nodeName,
      beforeConfigMetrics,
      afterConfigMetrics,
      diffsSummary,
      isBeforeDisabledCache = isBeforeDisabledCache)
  }

}

object ConfigMetricsStrings {
  val configFileExtension = "configMetrics"
  val withConfig = "withConfig"
  val withoutConfig = "withoutConfig"
  val wallTime = "Wall time"
  val cpuTime = "CPU time"
  val cacheTime = "Cache time"
  val maxHeap = "Max heap"
  val cacheHits = "Cache hits"
  val cacheMisses = "Cache misses"
  val cacheHitRatio = "Cache hit ratio"
  val evictions = "Evictions"
  val dalRequests = "Dal Requests"
  val enginesWallTime = "Engines Wall Time"
  val wallTimeCrumb = "wall"
  val cpuTimeCrumb = "cpu"
  val cacheTimeCrumb = "cache"
  val maxHeapCrumb = "heap"
  val cacheHitsCrumb = "hits"
  val cacheMissesCrumb = "misses"
  val cacheHitRatioCrumb = "hit_ratio"
  val evictionsCrumb = "evictions"
  val dalRequestsCrumb = "dal_req"
  val enginesWallTimeCrumb = "engine_wall"
  val headerNames: Array[String] =
    Array(wallTime, cpuTime, cacheTime, maxHeap, cacheHits, cacheMisses, evictions, dalRequests, enginesWallTime)
}

object MetricDiff {
  def percentageChange[T: Numeric](first: T, second: T): Double = {
    import Numeric.Implicits._
    val diff = (second - first).toDouble
    if (diff == 0) 0
    else if (first != 0) diff / first.toDouble
    else Double.PositiveInfinity // original value was 0, so any increase is +inf
  }
}

object ConfigMetrics {
  import ConfigMetricsStrings._

  def writeDiffSummaryCsv(path: String, diffSummaries: DiffSummary*): Unit = {
    val contents = printString(ps => {
      val writer = new StringWriter
      val csvWriter = new CSVWriter(writer)
      val data = for (run <- diffSummaries) yield diffSummaryColumns(run)
      csvWriter.writeAll((percentageChangeHeaderNames +: data).asJava)
      ps.print(writer.toString)
    })
    val correctPath = if (path.endsWith(File.separator)) path else s"$path${File.separator}"
    writeFile(s"${correctPath}diffSummary.csv", contents)
  }

  private def readCacheConfigSummary(content: String): Option[(String, CacheOffStatistics)] =
    try {
      val Array(countDisabled, missesSaved, cacheTimeSaved) = content.split(",").map(_.trim.replace("\"", ""))
      Some("dummy", CacheOffStatistics(countDisabled.toInt, missesSaved.toLong, cacheTimeSaved.toDouble * 1e6 toLong))
    } catch {
      case e: Exception =>
        log.warn(s"Could not parse content $content", e)
        None
    }

  private def readDiffSummary(content: String): Option[(String, DiffSummary)] =
    try {
      // read test name and percentage changes (pc) in metrics
      val Array(
        test,
        pcWallTime,
        pcCpuTime,
        pcCacheTime,
        pcMaxHeap,
        pcCacheHits,
        pcCacheMisses,
        pcCacheHitRatio,
        pcEvictions,
        pcDalRequests,
        pcEnginesWallTime) =
        content.split(",").map(_.trim.replace("\"", ""))

      Some(
        (
          test,
          DiffSummary(
            pcWallTime.toDouble,
            pcCpuTime.toDouble,
            pcCacheTime.toDouble,
            pcMaxHeap.toDouble,
            pcCacheHits.toDouble,
            pcCacheMisses.toDouble,
            pcCacheHitRatio.toDouble,
            pcEvictions.toDouble,
            pcDalRequests.toDouble,
            pcEnginesWallTime.toDouble
          )))
    } catch {
      case e: Exception =>
        log.warn(s"Could not parse content $content", e)
        None
    }

  private def readEffectSummary(content: String): Option[(String, EffectSummary)] =
    try {
      // To work around case class 22 limit
      val array: Array[String] = content.split(",").map(_.trim.replace("\"", ""))
      val testName = array(0)
      val beforeWallTime = array(1)
      val beforeCpuTime = array(2)
      val beforeCacheTime = array(3)
      val beforeMaxHeap = array(4)
      val beforeCacheHits = array(5)
      val beforeCacheMisses = array(6)
      val beforeCacheHitRatio = array(7)
      val beforeEvictions = array(8)
      val beforeDalRequests = array(9)
      val beforeEnginesWallTime = array(10)
      val afterWallTime = array(11)
      val afterCpuTime = array(12)
      val afterCacheTime = array(13)
      val afterMaxHeap = array(14)
      val afterCacheHits = array(15)
      val afterCacheMisses = array(16)
      val afterCacheHitRatio = array(17)
      val afterEvictions = array(18)
      val afterDalRequests = array(19)
      val afterEnginesWallTime = array(20)
      val pcWallTime = array(21)
      val pcCpuTime = array(22)
      val pcCacheTime = array(23)
      val pcMaxHeap = array(24)
      val pcCacheHits = array(25)
      val pcCacheMisses = array(26)
      val pcCacheHitRatio = array(27)
      val pcEvictions = array(28)
      val pcDalRequests = array(29)
      val pcEnginesWallTime = array(30)

      val before = ConfigMetrics(
        beforeWallTime.toLong,
        beforeCpuTime.toLong,
        beforeCacheTime.toLong,
        beforeMaxHeap.toLong,
        beforeCacheHits.toLong,
        beforeCacheMisses.toLong,
        beforeEvictions.toLong,
        beforeDalRequests.toLong,
        beforeEnginesWallTime.toLong
      )
      val after = ConfigMetrics(
        afterWallTime.toLong,
        afterCpuTime.toLong,
        afterCacheTime.toLong,
        afterMaxHeap.toLong,
        afterCacheHits.toLong,
        afterCacheMisses.toLong,
        afterEvictions.toLong,
        afterDalRequests.toLong,
        afterEnginesWallTime.toLong
      )
      val diff = DiffSummary(
        pcWallTime.toDouble,
        pcCpuTime.toDouble,
        pcCacheTime.toDouble,
        pcMaxHeap.toDouble,
        pcCacheHits.toDouble,
        pcCacheMisses.toDouble,
        pcCacheHitRatio.toDouble,
        pcEvictions.toDouble,
        pcDalRequests.toDouble,
        pcEnginesWallTime.toDouble
      )
      Some(
        (
          testName,
          EffectSummary(testName, None, before, after, diff)
        ))
    } catch {
      case e: Exception =>
        log.warn(s"Could not parse content $content", e)
        None
    }

  def readMetricDiffsValidationCsv(pathAndFileName: String): Map[String, DiffSummary] =
    readCsvToCaseClass(pathAndFileName, readDiffSummary(_))

  def readEffectSummaryCsv(pathAndFileName: String): Map[String, EffectSummary] =
    readCsvToCaseClass(pathAndFileName, readEffectSummary(_))

  def readCacheConfigSummaryCsv(pathAndFileName: String): CacheOffStatistics = {
    val result = readCsvToCaseClass(pathAndFileName, readCacheConfigSummary(_))
    assert(result.size == 1, s"Cache config summary CSV at $pathAndFileName could not be read")
    result.values.toList.head
  }

  // reader should read csv entries into a case class, sometimes with test name (hence (String, T))
  private def readCsvToCaseClass[T](pathAndFileName: String, reader: String => Option[(String, T)]): Map[String, T] = {
    val file = Paths.get(pathAndFileName)
    val bufferedSource = Source.fromFile(file.toUri)
    try {
      val lines = bufferedSource.getLines().toSeq.tail // skip headers
      lines.flatMap { line =>
        reader(line)
      }.toMap
    } catch {
      case e: Exception =>
        log.warn(s"Could not read file $pathAndFileName", e)
        Map.empty
    } finally bufferedSource.close
  }

  def writeMetricDiffsValidationCsv(
      defaultFilePath: String,
      resultsByTest: Map[String, MetricDiff],
      groupedTotals: Map[String, MetricDiff]): Unit = {
    val contents = printString(ps => {
      val writer = new StringWriter
      val csvWriter = new CSVWriter(writer)
      val header = "" /* blank cell for test name */ +: percentageChangeHeaderNames
      val data = for ((test, metrics) <- resultsByTest) yield diffSummaryColumns(test, metrics)
      val groupedTotalRows = groupedTotals map { case (groupName, diff) => diffSummaryColumns(groupName, diff) }
      val rows = data.toSeq ++ groupedTotalRows.toSeq
      csvWriter.writeAll((header +: rows).asJava)
      ps.print(writer.toString)
    })
    writeFile(defaultFilePath, contents)
    log.info(
      s"Validation CSV containing percentage changes for ${resultsByTest.size} tests and ${groupedTotals.keySet.size} groups written to $defaultFilePath")
  }

  def writeMetricIndivAndDiffsCsv(
      defaultFilePath: String,
      beforeResults: Map[String, ConfigMetrics],
      afterResults: Map[String, ConfigMetrics],
      diffResultsByTest: Map[String, MetricDiff]): Unit = {
    val contents = printString(ps => {
      val writer = new StringWriter
      val csvWriter = new CSVWriter(writer)
      val header =
        ("" /* blank cell for test name */ +: beforeHeaderNames) ++ afterHeaderNames ++ percentageChangeHeaderNames
      val diffData = for ((test, metrics) <- diffResultsByTest) yield diffSummaryColumns(test, metrics)
      val beforeData =
        for ((test, metrics) <- beforeResults) yield configMetricsColumns(test, metrics)
      val afterData =
        for ((test, metrics) <- afterResults) yield configMetricsColumns(test, metrics)
      val combineBeforeAndAfter = combineMetrics(beforeData.zip(afterData))
      val rows = combineMetrics(combineBeforeAndAfter.zip(diffData))
      val data = header +: rows.toSeq
      csvWriter.writeAll(data.asJava)
      ps.print(writer.toString)
    })
    writeFile(defaultFilePath + ".csv", contents)
    log.info(
      s"Cache Analysis CSV containing results of before, after and percentage changes for applying Optconf to ${diffResultsByTest.size} tests written to $defaultFilePath")
  }

  // helper methods for the two CSV writers above
  private def diffSummaryColumns(test: String, metrics: MetricDiff): Array[String] =
    s"$test" +: diffSummaryColumns(metrics.summary)

  private def formatPercChange(metric: Double): String = f"${metric * 100}%2.2f"

  def metricsDiffCrumb(grp: String, metrics: (MetricDiff, Boolean)): Map[String, String] = {
    val changesSummary = metrics._1.summary
    Map(
      "grp" -> grp,
      "accepted" -> metrics._2.toString,
      wallTimeCrumb -> formatPercChange(changesSummary.percentageChangeWallTime),
      cpuTimeCrumb -> formatPercChange(changesSummary.percentageChangeCpuTime),
      cacheTimeCrumb -> formatPercChange(changesSummary.percentageChangeCacheTime),
      maxHeapCrumb -> formatPercChange(changesSummary.percentageChangeMaxHeap),
      cacheHitsCrumb -> formatPercChange(changesSummary.percentageChangeCacheHits),
      cacheMissesCrumb -> formatPercChange(changesSummary.percentageChangeCacheMisses),
      cacheHitRatioCrumb -> formatPercChange(changesSummary.percentageChangeCacheHitRatio),
      evictionsCrumb -> formatPercChange(changesSummary.percentageChangeEvictions),
      dalRequestsCrumb -> formatPercChange(changesSummary.percentageChangeDalRequests),
      enginesWallTimeCrumb -> formatPercChange(changesSummary.percentageChangeEngineWallTime)
    )
  }

  private def diffSummaryColumns(metrics: DiffSummary): Array[String] = {
    Array(
      s"${metrics.percentageChangeWallTime}",
      s"${metrics.percentageChangeCpuTime}",
      s"${metrics.percentageChangeCacheTime}",
      s"${metrics.percentageChangeMaxHeap}",
      s"${metrics.percentageChangeCacheHits}",
      s"${metrics.percentageChangeCacheMisses}",
      s"${metrics.percentageChangeCacheHitRatio}",
      s"${metrics.percentageChangeEvictions}",
      s"${metrics.percentageChangeDalRequests}",
      s"${metrics.percentageChangeEngineWallTime}"
    )
  }

  private def percentageChangeHeaderNames: Array[String] = {
    import ConfigMetricsStrings._
    headerNames.map(m => s"$m (% change)")
  }

  private def combineMetrics(arrayPair: Iterable[(Array[String], Array[String])]): Iterable[Array[String]] =
    arrayPair map { case (arr1, arr2) => arr1.head +: (arr1.tail ++ arr2.tail) }

  private def configMetricsColumns(test: String, metrics: ConfigMetrics): Array[String] =
    s"$test" +: configMetricsColumns(metrics)

  private def configMetricsColumns(metrics: ConfigMetrics): Array[String] = {
    Array(
      s"${metrics.wallTime}",
      s"${metrics.cpuTime}",
      s"${metrics.cacheTime}",
      s"${metrics.maxHeap}",
      s"${metrics.cacheHits}",
      s"${metrics.cacheMisses}",
      s"${metrics.cacheHitRatio}",
      s"${metrics.evictions}",
      s"${metrics.dalRequests}",
      s"${metrics.enginesWallTime}"
    )
  }

  private def beforeHeaderNames: Array[String] = {
    import ConfigMetricsStrings._
    headerNames.map(m => s"$m (before)")
  }

  private def afterHeaderNames: Array[String] = {
    import ConfigMetricsStrings._
    headerNames.map(m => s"$m (after)")
  }

  private[diagnostics] def writeConfigMetricsCsv(path: String, configMetrics: ConfigMetrics*): Unit = {
    val runWithConfig = if (NodeCacheConfigs.getOptconfProviderPaths.nonEmpty) withConfig else withoutConfig
    val outputPath = s"$path$configFileExtension-$runWithConfig.csv"
    writeConfigMetricsCsvFullPath(outputPath, configMetrics: _*)
  }

  // used in tests to pass path without appending the with/withoutConfig markers
  def writeConfigMetricsCsvFullPath(path: String, configMetrics: ConfigMetrics*): Unit = {
    val contents = printString(ps => {
      val writer = new StringWriter
      val csvWriter = new CSVWriter(writer)
      val header = headerNames
      val data =
        for (run <- configMetrics)
          yield Array(
            s"${run.wallTime}",
            s"${run.cpuTime}",
            s"${run.cacheTime}",
            s"${run.maxHeap}",
            s"${run.cacheHits}",
            s"${run.cacheMisses}",
            s"${run.evictions}",
            s"${run.dalRequests}",
            s"${run.enginesWallTime}"
          )
      csvWriter.writeAll((header +: data).asJava)
      ps.print(writer.toString)
    })
    writeFile(path, contents)
  }
}
