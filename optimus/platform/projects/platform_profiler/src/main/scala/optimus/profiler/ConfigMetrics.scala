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

import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes
import msjava.slf4jutils.scalalog.getLogger
import optimus.graph.diagnostics.configmetrics.CacheMetrics
import optimus.graph.diagnostics.configmetrics.ConfigMetrics
import optimus.graph.diagnostics.configmetrics.ConfigMetricsStrings
import optimus.graph.diagnostics.configmetrics.DiffSummary
import optimus.graph.diagnostics.configmetrics.MetricDiff
import optimus.utils.PathHandler
import org.apache.commons.math3.stat.inference.TestUtils
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

private class ConfigMetricsCmdLine {
  @Args4jOption(
    name = "-l",
    aliases = Array("--logDir"),
    handler = classOf[PathHandler],
    required = true,
    usage = "Path to log directory to contain output files")
  private var varLogDir: Path = _
  def logDir = varLogDir

  @Args4jOption(
    name = "-m",
    aliases = Array("--metricsDir"),
    handler = classOf[PathHandler],
    required = true,
    usage = "Full path to directory containing profiler data from runs with and without config"
  )
  private var varMetricsDir: Path = _
  def metricsDir = varMetricsDir

  @Args4jOption(
    name = "-p",
    aliases = Array("--profilerDirectoryName"),
    usage = "Name of directory containing profiling data ('profiler' by default)",
    required = false
  )
  val profilerDirectoryName: String = "profiler"
}

object ConfigMetricsDiff extends App {
  import Numeric.Implicits._

  private lazy val log = getLogger(this)
  private lazy val cmdLine = new ConfigMetricsCmdLine
  private lazy val parser = new CmdLineParser(cmdLine)
  try {
    parser.parseArgument(args.toArray: _*)
  } catch {
    case x: CmdLineException =>
      log.info(x.getMessage)
      parser.printUsage(System.err)
      System.exit(1)
  }

  private[profiler] def getDiffs(
      runsWithoutConfig: Seq[ConfigMetrics],
      runsWithConfig: Seq[ConfigMetrics]): Seq[MetricDiff] = {
    runsWithoutConfig.zip(runsWithConfig) map { case (firstRun: ConfigMetrics, secondRun: ConfigMetrics) =>
      MetricDiff(firstRun, secondRun)
    }
  }

  private def getTotals(metrics: Seq[ConfigMetrics]): ConfigMetrics = {
    val totalWallTime = metrics.map(_.wallTime).sum
    val totalCpuTime = metrics.map(_.cpuTime).sum
    val totalCacheTime = metrics.map(_.cacheTime).sum
    val totalMaxHeap = metrics.map(_.maxHeap).sum
    val totalCacheHits = metrics.map(_.cacheHits).sum
    val totalCacheMisses = metrics.map(_.cacheMisses).sum
    val totalEvictions = metrics.map(_.evictions).sum
    val totalDalRequests = metrics.map(_.dalRequests).sum
    val totalEnginesWallTime = metrics.map(_.enginesWallTime).sum
    ConfigMetrics(
      totalWallTime,
      totalCpuTime,
      totalCacheTime,
      totalMaxHeap,
      totalCacheHits,
      totalCacheMisses,
      totalEvictions,
      totalDalRequests,
      totalEnginesWallTime)
  }

  private[profiler] def totalImprovementsWithAndWithoutConfig(
      runsWithoutConfig: Seq[ConfigMetrics],
      runsWithConfig: Seq[ConfigMetrics]): MetricDiff = {
    val totalsWithout = getTotals(runsWithoutConfig)
    val totalsWith = getTotals(runsWithConfig)
    MetricDiff(totalsWithout, totalsWith)
  }

  private[profiler] def averageImprovementsWithAndWithoutConfig(
      runsWithoutConfig: Seq[ConfigMetrics],
      runsWithConfig: Seq[ConfigMetrics]): DiffSummary = {
    val diffs = getDiffs(runsWithoutConfig, runsWithConfig)
    val avgWallTimeChange = average(diffs.map(_.percentageChangeWallTime))
    val avgCpuTimeChange = average(diffs.map(_.percentageChangeCpuTime))
    val avgCacheTimeChange = average(diffs.map(_.percentageChangeCacheTime))
    val avgMaxHeapChange = average(diffs.map(_.percentageChangeMaxHeap))
    val avgCacheHitChange = average(diffs.map(_.percentageChangeCacheHits))
    val avgCacheMissChange = average(diffs.map(_.percentageChangeCacheMisses))
    val avgCacheHitRatioChange = average(diffs.map(_.percentageChangeCacheHitRatio))
    val avgEvictionChange = average(diffs.map(_.percentageChangeEvictions))
    val avgDalRequestChange = average(diffs.map(_.percentageChangeDalRequests))
    val avgEngineWallTimeChange = average(diffs.map(_.percentageChangeEngineWallTime))
    DiffSummary(
      avgWallTimeChange,
      avgCpuTimeChange,
      avgCacheTimeChange,
      avgMaxHeapChange,
      avgCacheHitChange,
      avgCacheMissChange,
      avgCacheHitRatioChange,
      avgEvictionChange,
      avgDalRequestChange,
      avgEngineWallTimeChange
    )
  }

  private def average[T: Numeric](xs: Iterable[T]): Double = {
    if (xs.nonEmpty) xs.sum.toDouble / xs.size
    else {
      log.warn("Tried to compute average for 0 samples")
      0
    }
  }

  // significance level is 0.05 (ie, the probability of observing the differences seen in the data by chance is just 5%)
  private def changeIsSignificant(
      runsWithoutConfig: Seq[ConfigMetrics],
      runsWithConfig: Seq[ConfigMetrics],
      metric: (ConfigMetrics => Double)): Boolean =
    TestUtils.pairedTTest(runsWithoutConfig.map(metric).toArray, runsWithConfig.map(metric).toArray, 0.05)

  private def findConfigMetricsFiles(pathToMetricsDir: Path): Seq[Path] = {
    val filePaths = ArrayBuffer[Path]()

    class FileVisitor extends SimpleFileVisitor[Path] {
      private def isMetricsFile(fileName: String) = fileName.contains(ConfigMetricsStrings.configFileExtension)
      override def visitFile(file: Path, attrs: BasicFileAttributes) = {
        val fileName = file.getFileName.toString
        if (isMetricsFile(fileName)) filePaths += file
        FileVisitResult.CONTINUE
      }
    }

    Files.walkFileTree(pathToMetricsDir, new FileVisitor)
    filePaths.toList
  }

  private[profiler] def fileNameToTestName(fileName: String, profilerDirName: String): String = {
    RegressionsConfigApps.extractTestAndFileName(fileName, "", profilerDirName) match {
      case Some((name, _)) => name
      case _ =>
        log.warn(s"Could not extract regression test name for file $fileName, using file name instead")
        fileName
    }
  }

  private def parseMetricsForFile(
      content: String,
      file: String,
      profilerDirName: String): Option[(String, ConfigMetrics)] = {
    try {
      val Array(
        wallTime,
        cpuTime,
        cacheTime,
        maxHeap,
        cacheHits,
        cacheMisses,
        evictions,
        dalRequests,
        enginesWallTime) = content.split(",").map(_.trim.replace("\"", ""))
      val testName = fileNameToTestName(file, profilerDirName)
      Some(
        testName,
        ConfigMetrics(
          wallTime.toLong,
          cpuTime.toLong,
          cacheTime.toLong,
          maxHeap.toLong,
          cacheHits.toLong,
          cacheMisses.toLong,
          evictions.toLong,
          dalRequests.toLong,
          enginesWallTime.toLong
        )
      )
    } catch {
      case e: NumberFormatException =>
        log.warn(s"Could not parse file ${file}", e)
        None
      case e: Exception =>
        log.warn(s"Could not parse file ${file}", e)
        None
    }
  }

  private def parseCacheMetricsForFile(
      lines: Seq[String],
      file: String,
      profilerDirName: String): Option[(String, Map[String, CacheMetrics])] = {

    var testNodeMap = Map.empty[String, CacheMetrics]
    val testName = fileNameToTestName(file, profilerDirName)
    lines.tail.foreach(line =>
      try {
        val Array(
          engine,
          nodeStarted,
          evictions,
          invalidates,
          cacheHits,
          cacheMisses,
          cacheTime,
          nodeReusedTime,
          cacheBenefit,
          avgCacheTime,
          wallTime,
          selfTime,
          ancTime,
          pcsTime,
          propertyName,
          cacheable,
          isScenarioIndependent,
          favorReuse) = line.split(",").map(_.trim.replace("\"", ""))
        testNodeMap +=
          (propertyName ->
            CacheMetrics(
              cacheTime = cacheTime.toDouble,
              cacheHits = cacheHits.toDouble,
              cacheMisses = cacheMisses.toDouble,
              evictions = evictions.toDouble,
              selfTime = selfTime.toDouble,
              totalTime = selfTime.toDouble + ancTime.toDouble
            ))
      } catch {
        case e: Exception =>
          log.warn(s"Could not parse file ${file}", e)
          None
      })
    Some(testName, testNodeMap)

  }

  /** Parses an appletInfo.csv file and returns an option of applet names */
  def fileNameToAppletNames(file: Path): Option[Set[String]] = {
    val bufferedSource = Source.fromFile(file.toFile)
    try {
      val lines = bufferedSource.getLines().toSeq
      if (lines.length < 2) {
        log.warn("File format incorrect (expected a header and at least a single row of data)")
        None
      } else parseAppletNamesForFile(lines, file.toAbsolutePath.toString)
    } catch {
      case e: Exception =>
        log.warn(s"Could not read file ${file.getFileName}", e)
        None
    } finally bufferedSource.close
  }

  private def parseAppletNamesForFile(lines: Seq[String], filePath: String): Option[Set[String]] =
    Some(lines.tail.map(_.replace("\"", "")).toSet)

  private[profiler] def parseMetricsFromString(
      content: String,
      fileName: String,
      profilerDirName: String): Option[ConfigMetrics] =
    parseMetricsForFile(content, fileName, profilerDirName).map(_._2)

  private[profiler] def filenameToConfigMetrics(
      files: Seq[Path],
      profilerDirName: String): Map[String, ConfigMetrics] = {
    files.flatMap { file =>
      val bufferedSource = Source.fromFile(file.toFile)
      try {
        val lines = bufferedSource.getLines().toSeq
        if (lines.length != 2) {
          log.warn("File format incorrect (expected a header and a single row of data)")
          None
        } else parseMetricsForFile(lines.last, file.toAbsolutePath.toString, profilerDirName)
      } catch {
        case e: Exception =>
          log.warn(s"Could not read file ${file.getFileName}", e)
          None
      } finally bufferedSource.close
    }.toMap
  }

  private[profiler] def filenameToCacheMetrics(
      files: Seq[Path],
      profilerDirName: String): Map[String, Map[String, CacheMetrics]] = {
    files.flatMap { file =>
      val bufferedSource = Source.fromFile(file.toFile)
      try {
        val lines = bufferedSource.getLines().toSeq
        if (lines.length < 2) {
          log.warn("File format incorrect (expected a header and at least a single row of data)")
          None
        } else parseCacheMetricsForFile(lines, file.toAbsolutePath.toString, profilerDirName)
      } catch {
        case e: Exception =>
          log.warn(s"Could not read file ${file.getFileName}", e)
          None
      } finally bufferedSource.close
    }.toMap
  }

  private def readConfigMetrics(files: Seq[Path], profilerDirName: String): Seq[ConfigMetrics] =
    filenameToConfigMetrics(files, profilerDirName).map(_._2).toSeq

  private[profiler] def significanceScores(
      runsWithoutConfig: Seq[ConfigMetrics],
      runsWithConfig: Seq[ConfigMetrics]): Map[String, Boolean] = {
    def ttest(metric: (ConfigMetrics => Double)) = changeIsSignificant(runsWithoutConfig, runsWithConfig, metric)

    val tWallTime = ConfigMetricsStrings.wallTime -> ttest((cfg: ConfigMetrics) => cfg.wallTime.toDouble)
    val tCpuTime = ConfigMetricsStrings.cpuTime -> ttest((cfg: ConfigMetrics) => cfg.cpuTime.toDouble)
    val tCacheTime = ConfigMetricsStrings.cacheTime -> ttest((cfg: ConfigMetrics) => cfg.cacheTime.toDouble)
    val tMaxHeap = ConfigMetricsStrings.maxHeap -> ttest((cfg: ConfigMetrics) => cfg.maxHeap.toDouble)
    val tCacheHits = ConfigMetricsStrings.cacheHits -> ttest((cfg: ConfigMetrics) => cfg.cacheHits.toDouble)
    val tCacheMisses = ConfigMetricsStrings.cacheMisses -> ttest((cfg: ConfigMetrics) => cfg.cacheMisses.toDouble)
    val tCacheHitRatio = ConfigMetricsStrings.cacheHitRatio -> ttest((cfg: ConfigMetrics) => cfg.cacheHitRatio.toDouble)
    val tEvictions = ConfigMetricsStrings.evictions -> ttest((cfg: ConfigMetrics) => cfg.evictions.toDouble)
    val tDalRequests = ConfigMetricsStrings.dalRequests -> ttest((cfg: ConfigMetrics) => cfg.dalRequests.toDouble)

    Map(tWallTime, tCpuTime, tCacheTime, tMaxHeap, tCacheHits, tCacheMisses, tCacheHitRatio, tEvictions, tDalRequests)
  }

  private def sampleSizesValid(numRunsWithConfig: Int, numRunsWithoutConfig: Int): Boolean = {
    val sameLength = numRunsWithConfig == numRunsWithoutConfig
    if (!sameLength) {
      log.warn("Sample sizes must match")
      false
    } else {
      val correctLength = numRunsWithConfig >= 2
      if (!correctLength) log.warn("Sample size must be at least 2 to calculate significance")
      correctLength
    }
  }

  private def getConfigMetrics(metricsDir: Path, profilerDirName: String): (Seq[ConfigMetrics], Seq[ConfigMetrics]) = {
    val groupedFiles = findConfigMetricsFiles(cmdLine.metricsDir)
      .groupBy(_.getFileName.toString.contains(ConfigMetricsStrings.withConfig))
    (
      readConfigMetrics(groupedFiles.get(true).getOrElse(Nil), profilerDirName),
      readConfigMetrics(groupedFiles.get(false).getOrElse(Nil), profilerDirName))
  }

  private lazy val (runsWithConfig, runsWithoutConfig) =
    getConfigMetrics(cmdLine.metricsDir, cmdLine.profilerDirectoryName)
  private lazy val summary = averageImprovementsWithAndWithoutConfig(runsWithoutConfig, runsWithConfig)
  log.info(s"Averages:\n$summary")
  ConfigMetrics.writeDiffSummaryCsv(cmdLine.logDir.toString, summary)
  log.info(s"Saved summary to ${cmdLine.logDir}")

  if (!sampleSizesValid(runsWithConfig.length, runsWithoutConfig.length)) {
    log.warn("Cannot calculate significance scores")
    System.exit(0)
  }

  try {
    significanceScores(runsWithConfig, runsWithoutConfig).foreach { case (metric: String, significantChange: Boolean) =>
      if (significantChange) log.info(f"Statistically significant change in $metric")
    }
  } catch { // TestUtils.pairedTTest can throw various different exceptions - catch them so we can exit nicely
    case e: Exception => log.error("Could not calculate statistical significance", e)
  }
}
