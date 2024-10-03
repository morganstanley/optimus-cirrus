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
package optimus.profiler
import msjava.slf4jutils.scalalog.getLogger
import optimus.graph.diagnostics.configmetrics.CacheMetricDiffSummary
import optimus.graph.diagnostics.configmetrics.CacheMetrics
import optimus.graph.diagnostics.configmetrics.ConfigMetrics
import optimus.graph.diagnostics.configmetrics.ConfigMetricsStrings
import optimus.graph.diagnostics.configmetrics.DiffSummary
import optimus.graph.diagnostics.configmetrics.EffectSummary
import optimus.profiler.ConfigMetricsDiff.filenameToConfigMetrics
import optimus.profiler.RegressionsConfigApps.Group
import optimus.profiler.RegressionsConfigApps.TestCasesToPaths
import optimus.profiler.RegressionsConfigApps.findGroupedFiles
import optimus.profiler.RegressionsConfigApps.moduleIncluded

import java.nio.file.Files
import java.nio.file.Paths
import scala.jdk.CollectionConverters._

object CacheAnaysisDiffUtils {
  private lazy val log = getLogger(this)
  private[optimus] def isConfigMetricsCsv(fileName: String): Boolean =
    fileName.contains(ConfigMetricsStrings.configFileExtension)
  private[optimus] def isConfigMetricsCsvWithoutConf(fileName: String): Boolean =
    isConfigMetricsCsv(fileName) && fileName.contains(ConfigMetricsStrings.withoutConfig)
  private[optimus] def isConfigMetricsCsvWithConf(fileName: String): Boolean =
    isConfigMetricsCsv(fileName) && fileName.contains(ConfigMetricsStrings.withConfig)

  private[optimus] def checkLengths[T](afterResultsPaths: Iterable[T], beforeResultsPaths: Iterable[T]): Unit = {
    val nAfter = afterResultsPaths.size
    val nBefore = beforeResultsPaths.size
    if (nAfter != nBefore)
      log.warn(s"Found $nAfter result files for reruns and $nBefore result files for original runs")
  }
  private[optimus] val csvFileExtension = ".csv"

  private[optimus] def metricsFromFiles(files: Seq[String], profilerDirName: String): Map[String, ConfigMetrics] =
    filenameToConfigMetrics(files.map(Paths.get(_)), profilerDirName)

  private[optimus] def findTests(pathStr: String): Seq[String] = {
    val path = Paths.get(pathStr)
    val subDirectories = Files.list(path).filter(x => Files.isDirectory(x))
    subDirectories.iterator().asScala.toList.map(_.getFileName.toString)
  }

  private[optimus] def includeTest(tests: Seq[String])(testName: String): Boolean = tests.contains(testName)

  private[optimus] def getTestsToCompare(
      afterTestsWithMetrics: Set[String],
      beforeTestsWithMetrics: Set[String]): Set[String] =
    if (afterTestsWithMetrics != beforeTestsWithMetrics) {
      val intersection = afterTestsWithMetrics.intersect(beforeTestsWithMetrics)
      log.warn(
        s"Before and after tests did not match, comparing only the intersection:\n${intersection.mkString("\n")}")
      intersection
    } else afterTestsWithMetrics

  private[optimus] def findCorrespondingOriginalRuns(
      firstRunProfilerDirName: String,
      profilerDirName: String,
      firstRunRootDir: String,
      rerunRootDir: String,
      enforceWithConfComparison: Boolean,
      groups: Seq[Group] = Nil): (TestCasesToPaths, TestCasesToPaths) = {

    val rerunTests = findTests(rerunRootDir)
    val beforeFileNameFiler =
      if (enforceWithConfComparison) isConfigMetricsCsvWithoutConf _ else isConfigMetricsCsv _
    val afterFileNameFilter =
      if (enforceWithConfComparison) isConfigMetricsCsvWithConf _ else isConfigMetricsCsv _

    // we want to derive the mapping from test cases to modules so that we can report total improvement per group later
    val beforeResultsGroupedPaths = findGroupedFiles(
      firstRunRootDir,
      ".csv",
      firstRunProfilerDirName,
      beforeFileNameFiler,
      includeTest(rerunTests),
      moduleIncluded(groups))

    val afterResultsGroupedPaths =
      findGroupedFiles(rerunRootDir, ".csv", profilerDirName, afterFileNameFilter, _ => true, moduleIncluded(groups))

    (afterResultsGroupedPaths, beforeResultsGroupedPaths)
  }

  private[optimus] val defaultCacheMetrics = CacheMetrics(
    cacheTime = Double.MinValue,
    cacheHits = Double.MinValue,
    cacheMisses = Double.MinValue,
    evictions = Double.MinValue,
    selfTime = Double.MinValue,
    totalTime = Double.MinValue
  )
  private[optimus] val defaultCacheMetricDiff = CacheMetricDiffSummary(
    percentageChangeCacheTime = Double.MinValue,
    percentageChangeCacheHits = Double.MinValue,
    percentageChangeCacheMisses = Double.MinValue,
    percentageChangeCacheHitRatio = Double.MinValue,
    percentageChangeEvictions = Double.MinValue,
    percentageChangeSelfTime = Double.MinValue,
    percentageChangeTotalTime = Double.MinValue
  )
  private[optimus] val defaultConfigMetrics = ConfigMetrics(
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
  private[optimus] val defaultDiffSummary = DiffSummary(
    percentageChangeWallTime = Double.MinValue,
    percentageChangeCpuTime = Double.MinValue,
    percentageChangeCacheTime = Double.MinValue,
    percentageChangeMaxHeap = Double.MinValue,
    percentageChangeCacheHits = Double.MinValue,
    percentageChangeCacheMisses = Double.MinValue,
    percentageChangeCacheHitRatio = Double.MinValue,
    percentageChangeEvictions = Double.MinValue,
    percentageChangeDalRequests = Double.MinValue,
    percentageChangeEngineWallTime = Double.MinValue
  )
  private[optimus] def getDiffSummaryWithMinValue(diff: DiffSummary): DiffSummary = DiffSummary(
    percentageChangeWallTime = diff.percentageChangeWallTime,
    percentageChangeCpuTime = diff.percentageChangeCpuTime,
    percentageChangeCacheTime = diff.percentageChangeCacheTime,
    percentageChangeMaxHeap = Double.MinValue,
    percentageChangeCacheHits = diff.percentageChangeCacheHits,
    percentageChangeCacheMisses = diff.percentageChangeCacheMisses,
    percentageChangeCacheHitRatio = diff.percentageChangeCacheHitRatio,
    percentageChangeEvictions = diff.percentageChangeEvictions,
    percentageChangeDalRequests = Double.MinValue,
    percentageChangeEngineWallTime = Double.MinValue
  )
  private[optimus] def getConfigSumFromTestSummaries(
      testSumamries: List[EffectSummary],
      extractor: EffectSummary => ConfigMetrics,
      needMinValue: Boolean): ConfigMetrics = ConfigMetrics(
    cacheTime = testSumamries.map(extractor(_).cacheTime).sum,
    cacheHits = testSumamries.map(extractor(_).cacheHits).sum,
    cacheMisses = testSumamries.map(extractor(_).cacheMisses).sum,
    evictions = testSumamries.map(extractor(_).evictions).sum,
    wallTime = testSumamries.map(extractor(_).wallTime).sum,
    cpuTime = testSumamries.map(extractor(_).cpuTime).sum,
    maxHeap = if (needMinValue) Long.MinValue else testSumamries.map(extractor(_).maxHeap).sum,
    dalRequests = if (needMinValue) Long.MinValue else testSumamries.map(extractor(_).dalRequests).sum,
    enginesWallTime = if (needMinValue) Long.MinValue else testSumamries.map(extractor(_).enginesWallTime).sum
  )
  private[optimus] def convertConfigMetricsTimeInMs(configMetric: ConfigMetrics): ConfigMetrics = ConfigMetrics(
    wallTime = (configMetric.wallTime / 1e6).toLong,
    cpuTime = (configMetric.cpuTime / 1e6).toLong,
    cacheTime = (configMetric.cacheTime / 1e6).toLong,
    maxHeap = configMetric.maxHeap,
    cacheHits = configMetric.cacheHits,
    cacheMisses = configMetric.cacheMisses,
    evictions = configMetric.evictions,
    dalRequests = configMetric.dalRequests,
    enginesWallTime = (configMetric.enginesWallTime / 1e6).toLong
  )
  private[optimus] def getDefaultEffectSummaryIfNoDiscrepancy(testName: String) =
    EffectSummary.fromNodeCacheEffectSummary(
      testName,
      Some("No Discrepancy Node"),
      defaultCacheMetrics,
      defaultCacheMetrics,
      defaultCacheMetricDiff)

  private[optimus] val appletInfoFileName = "appletInfo"

  def formatPercentage(number: Double): java.lang.Double = {
    if (number == Double.MinValue) null else number * 100
  }
  def formatBooleanToNumber(boolOpt: Option[Boolean]): java.lang.Double = {
    // option.map.getOrElse(null) for some reason doesn't work
    boolOpt match {
      case Some(bool) => if (bool) 1.0 else 0.0
      case None       => null
    }
  }
  def formatValue(value: Long): java.lang.Double =
    if (value == Long.MinValue) null else value.toDouble
  def formatTimeNsToMs(value: Long): java.lang.Double =
    if (value == Long.MinValue) null else value / 1e6
}
