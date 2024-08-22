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
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.diagnostics.configmetrics.CacheMetricDiff
import optimus.graph.diagnostics.configmetrics.CacheMetrics
import optimus.graph.diagnostics.configmetrics.ConfigMetrics
import optimus.graph.diagnostics.configmetrics.ConfigMetricsStrings.configFileExtension
import optimus.graph.diagnostics.configmetrics.EffectSummary
import optimus.graph.diagnostics.configmetrics.MetricDiff
import optimus.graph.diagnostics.configmetrics.ThresholdDiffSummary
import optimus.graph.diagnostics.gridprofiler.GridProfilerUtils.copyFileFromResources
import optimus.graph.diagnostics.gridprofiler.GridProfilerUtils.hotspotsFileName
import optimus.graph.diagnostics.pgo.Profiler.combinePNTIs
import optimus.graph.diagnostics.pgo.Profiler.pgoWouldDisableCache
import optimus.platform.AsyncImplicits._
import optimus.platform.async
import optimus.profiler.CacheAnaysisDiffUtils._
import optimus.profiler.ConfigMetricsDiff.fileNameToAppletNames
import optimus.profiler.MergeTraces.mergeTraces
import optimus.profiler.RegressionsConfigApps.extractTestAndFileName
import optimus.profiler.RegressionsConfigApps.profilerDirName
import optimus.scalacompat.collection._
import optimus.utils.ErrorIgnoringFileVisitor
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser

import java.lang.{StringBuilder => JStringBuilder}
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes
import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * This app takes a path to a root directory containing the output for the original run of the regressions,
 * a root directory containing the output for another regressions run to be compared with the original run,
 * an output directory for generating a summary CSV and html report.
 *
 * It writes a CSV in the following format, at $outputPath/runcalc-optconf-effect.csv
 *
 * (Stats of before and after optconf)
 * Wall time  	CPU time  	Cache time   Max heap  	Cache hits   Cache misses   Cache hit ratio   Evictions   Dal Requests   Engines Wall Time
 *
 * (percentage differences)
 * Wall time (% change) CPU time (% change) Cache time (% change) Max heap (% change) Cache hits (% change) Cache misses (% change) Cache hit ratio (% change) Evictions (% change) Dal Requests (% change) Engines Wall Time (% change)
 *
 * It also create a optconfEffect.html inside folder 'html-report' along with js, css resources that are loaded in the html file.
 *
 * To run script: ./CacheAnalysisTool -r /path/to/original/regressions -s
 * /path/to/regressions/to/compare/With -o /path/to/output/dir
 */

object CacheAnalysisTool extends App {
  private lazy val log = getLogger(this)
  private lazy val cmdLine = new CompareCacheStatsCmdLine
  private lazy val parser = new CmdLineParser(cmdLine)
  try {
    parser.parseArgument(args.toArray: _*)
  } catch {
    case x: CmdLineException =>
      log.info(x.getMessage)
      parser.printUsage(System.err)
      System.exit(1)
  }

  private lazy val prefix = "optconfEffect"
  lazy val unknownApplet = "unknown"

  @async def getComparisonDrsTableContent(
      firstProfilerDirName: String,
      secondProfilerDirName: String,
      firstProfilerDir: String,
      secondProfilerDir: String,
      enforceWithConfComparison: Boolean = false,
      enableAppletGrouping: Boolean): Map[String, OverallPgoEffect] = {

    val ((combinedSummaryCommon, combinedSummaryDistinct), appletToTestMap) = combineAndWriteSummary(
      firstProfilerDirName,
      secondProfilerDirName,
      firstProfilerDir,
      secondProfilerDir,
      enforceWithConfComparison,
      enableAppletGrouping)

    val combinedTestToNodesMap = combinedSummaryCommon.map { case (test, testAndNodesEffectSummary) =>
      val (testSummary, commonNodeSummary) =
        (testAndNodesEffectSummary.testSummary, testAndNodesEffectSummary.nodesSummaries)
      val distinctNodes = combinedSummaryDistinct(test).nodesSummaries
      test -> (testSummary +: (commonNodeSummary ++ distinctNodes))
    }

    appletToPgoEffect(combinedTestToNodesMap, appletToTestMap)
  }
  private def appletToPgoEffect(
      combinedTestMap: Map[String, List[EffectSummary]],
      appletToTestMap: Option[Map[String, Set[String]]]): Map[String, OverallPgoEffect] = {
    appletToTestMap match {
      case Some(appletMap) =>
        appletMap.map { case (applet, tests) =>
          val testMap = tests.map(test => test -> combinedTestMap.getOrElse(test, List.empty)).toMap
          applet -> OverallPgoEffect(None, testMap)
        }
      case None => Map(unknownApplet -> OverallPgoEffect(None, combinedTestMap))
    }
  }

  def updatePerTestPntisAndGlobalPntis(
      testName: String,
      allTestsNames: Set[String],
      path: String,
      globalPnti: Seq[PNodeTaskInfo],
      testToMergedPntiMap: Map[String, Seq[PNodeTaskInfo]]) = {
    val updatedAllTestNames = allTestsNames + testName
    val testPnti = mergeTraces(Seq(path), combinePntis)._1
    val prevGlobalPnti = globalPnti
    val updatedGlobalPnti = combinePNTIs(prevGlobalPnti ++ testPnti).values.toSeq
    val updatedPerTestPnti = if (testToMergedPntiMap.contains(testName)) {
      val prevMergedPnti = testToMergedPntiMap(testName)
      combinePNTIs(prevMergedPnti ++ testPnti).values.toSeq
    } else testPnti
    (updatedAllTestNames, updatedPerTestPnti, updatedGlobalPnti)
  }

  def updateAppletToTestMap(
      testName: String,
      path: String,
      testsWithApplet: Set[String],
      appletToTestPntiMap: AppletToTestPntiMap): (Set[String], AppletToTestPntiMap) = {
    val appletNames = fileNameToAppletNames(Paths.get(path)).getOrElse(Set.empty)
    val updatedTestsWithApplet = testsWithApplet + testName
    val updatedAppletMap: mutable.Map[String, mutable.Map[String, Seq[PNodeTaskInfo]]] = mutable.Map()
    // copy the original appletToTestPntiMap to updatedAppletMap first
    for ((applet, testToPntiMap) <- appletToTestPntiMap) {
      updatedAppletMap(applet) = mutable.Map() ++= testToPntiMap
    }
    // then update
    appletNames.foreach { appletName =>
      if (!updatedAppletMap.contains(appletName)) {
        updatedAppletMap(appletName) = mutable.Map()
      }
      updatedAppletMap(appletName) += (testName -> Seq.empty)
    }
    (updatedTestsWithApplet, updatedAppletMap.mapValuesNow(_.toMap).toMap)
  }

  def mergeAppletAndPerTestPntis(
      allTestsNames: Set[String],
      testsWithApplet: Set[String],
      testToMergedPntiMap: Map[String, Seq[PNodeTaskInfo]],
      appletToTestPntiMap: AppletToTestPntiMap): AppletToTestPntiMap = {
    val testsWithoutApplets = allTestsNames.diff(testsWithApplet)
    val updatedAppletMap: mutable.Map[String, Map[String, Seq[PNodeTaskInfo]]] = mutable.Map() ++= appletToTestPntiMap
    // construct applet -> test -> mergedPnti for tests with applet only
    updatedAppletMap.foreach { case (applet, testMap) =>
      val updatedTestMap: mutable.Map[String, Seq[PNodeTaskInfo]] = mutable.Map()
      // update the testToPntiMap for this applet
      testMap.foreach { case (test, _) =>
        if (testToMergedPntiMap.contains(test))
          updatedTestMap(test) = testToMergedPntiMap(test)
      }
      // update: applet -> testToPntiMap
      if (updatedTestMap.nonEmpty)
        updatedAppletMap(applet) = updatedTestMap.toMap
    }
    // group tests with no applet
    if (testsWithoutApplets.nonEmpty) {
      val updatedTestMap: mutable.Map[String, Seq[PNodeTaskInfo]] = mutable.Map()
      testsWithoutApplets.foreach { test =>
        updatedTestMap += (test -> testToMergedPntiMap(test))
      }
      updatedAppletMap(unknownApplet) = updatedTestMap.toMap
    }
    updatedAppletMap.toMap
  }

  def mergingPntiInFileWalking(
      profilerDirName: String,
      profilerDir: String,
      enableAppletGrouping: Boolean): AppletToTestMapAndGlobalPnti = {
    val appletToTestPntiMap: mutable.Map[String, mutable.Map[String, Seq[PNodeTaskInfo]]] = mutable.Map()
    val testToMergedPntiMap: mutable.Map[String, Seq[PNodeTaskInfo]] = mutable.Map()
    var globalPnti: Seq[PNodeTaskInfo] = Seq.empty
    val testsWithApplet, allTestsNames: mutable.Set[String] = mutable.Set()
    val path = Paths.get(profilerDir)

    /** replace the old appletToTestPnti map with the content from the immutable updatedMap */
    def replaceAppletMap(updatedMap: AppletToTestPntiMap): Unit = {
      appletToTestPntiMap.clear()
      for ((applet, testToPntiMap) <- updatedMap) {
        appletToTestPntiMap(applet) = mutable.Map() ++= testToPntiMap
      }
    }
    Files.walkFileTree(
      path,
      new ErrorIgnoringFileVisitor {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          val path = file.toString
          // extract pnti from ogtrace file
          extractTestAndFileName(path, ".ogtrace", profilerDirName) match {
            case Some((testName, fileName)) =>
              log.info(s"Found ogtrace file $fileName for test $testName")
              val (updatedAllTestNames, updatedPerTestPnti, updatedGlobalPnti) =
                updatePerTestPntisAndGlobalPntis(
                  testName,
                  allTestsNames.toSet,
                  path,
                  globalPnti,
                  testToMergedPntiMap.toMap)
              allTestsNames.clear()
              allTestsNames ++= updatedAllTestNames
              testToMergedPntiMap(testName) = updatedPerTestPnti
              globalPnti = updatedGlobalPnti
            case _ =>
          }
          if (enableAppletGrouping) {
            // extract applet info
            extractTestAndFileName(path, ".csv", profilerDirName) match {
              case Some((testName, fileName)) if isRequiredFile(fileName, appletInfoFileName) =>
                log.info(s"Found applet file for test $testName")
                val (updatedTestsWithApplet, updatedAppletMap) = updateAppletToTestMap(
                  testName,
                  path,
                  testsWithApplet.toSet,
                  appletToTestPntiMap.mapValuesNow(_.toMap).toMap)
                testsWithApplet.clear()
                testsWithApplet ++= updatedTestsWithApplet
                replaceAppletMap(updatedAppletMap)
              case _ =>
            }
          } // don't do anything if not enable applet grouping
          FileVisitResult.CONTINUE
        }
      }
    )
    // if no applet grouping
    if (!enableAppletGrouping) {
      appletToTestPntiMap += (unknownApplet -> mutable.Map())
      appletToTestPntiMap(unknownApplet) = testToMergedPntiMap
    } else { // with applet grouping
      val updatedMap = mergeAppletAndPerTestPntis(
        allTestsNames.toSet,
        testsWithApplet.toSet,
        testToMergedPntiMap.toMap,
        appletToTestPntiMap.mapValuesNow(_.toMap).toMap)
      replaceAppletMap(updatedMap)
    }
    AppletToTestMapAndGlobalPnti(appletToTestPntiMap.mapValuesNow(_.toMap).toMap, globalPnti)
  }

  @async def getAppletPntiMapIfEnableAppletGrouping(
      enableAppletGrouping: Boolean,
      perAppletMergedPnti: Seq[PNodeTaskInfo]) = {
    if (enableAppletGrouping) {
      val appletPntiPgoDecisionMap =
        perAppletMergedPnti.apar.map(pnti => pnti.fullName() -> pgoWouldDisableCache(pnti)).toMap
      Some(appletPntiPgoDecisionMap)
    } else
      None
  }

  @async def getPGODrsTableContent(
      profilerDirName: String,
      profilerDir: String,
      enableAppletGrouping: Boolean): Map[String, OverallPgoEffect] = {
    val fileWalkResult =
      mergingPntiInFileWalking(profilerDirName, profilerDir, enableAppletGrouping)
    val (appletToPerTestMergedPntis, globalMergedPnti) = (fileWalkResult.appletToTestMap, fileWalkResult.globalPntis)
    // (pntiName -> (pnti, Boolean: disable cache = true; false otherwise))
    val pntiGlobalMap =
      globalMergedPnti.apar.map(pnti => pnti.fullName() -> PntiWithPgoDecision(pnti, pgoWouldDisableCache(pnti))).toMap

    appletToPerTestMergedPntis.apar.map { case (appletName, perTestMergedPntiMap) =>
      val perAppletMergedPnti = combinePNTIs(perTestMergedPntiMap.values.toSeq.flatten).values.toSeq
      // Option of map of (pntiName -> Boolean: disable cache or not by applet pgo) if enable applet grouping
      val appletPntiMap: Option[Map[String, Boolean]] =
        getAppletPntiMapIfEnableAppletGrouping(enableAppletGrouping, perAppletMergedPnti)

      // test -> (testLevelEffectSummary + discrepancyNodesEffectSummary)
      val testMap: Map[String, List[EffectSummary]] = perTestMergedPntiMap.apar.map {
        case (testName, perTestMergedPnti) =>
          getTestAndNodesSummary(testName, perTestMergedPnti, appletPntiMap, pntiGlobalMap)
      }
      val appletSummary: Option[EffectSummary] = getAppletSummaryFromTests(enableAppletGrouping, testMap)
      appletName -> OverallPgoEffect(appletSummary, testMap)
    }

  }

  private def getAppletSummaryFromTests(
      enableAppletGrouping: Boolean,
      testMap: Map[String, List[EffectSummary]]): Option[EffectSummary] = {
    if (enableAppletGrouping) {
      // test summary is always the first element in the list
      val allTestSummaries = testMap.values.map(x => x.head).toList
      val beforeConfigByAppletPgo = getConfigSumFromTestSummaries(allTestSummaries, s => s.before)
      val afterConfigByGlobalPgo = getConfigSumFromTestSummaries(allTestSummaries, s => s.after)
      val diffSummary = MetricDiff(beforeConfigByAppletPgo, afterConfigByGlobalPgo).summary
      // Set min value for metrics information we do not have, so can be shown null on UI
      val diffWithMinValue = getDiffSummaryWithMinValue(diffSummary)
      Some(
        EffectSummary(
          testName = "",
          nodeName = None,
          before = beforeConfigByAppletPgo,
          after = afterConfigByGlobalPgo,
          diffs = diffWithMinValue))
    } else None
  }

  @async def getTestAndNodesSummary(
      testName: String,
      perTestMergedPnti: Seq[PNodeTaskInfo],
      appletPntiMap: Option[Map[String, Boolean]],
      pntiGlobalMap: Map[String, PntiWithPgoDecision]): (String, List[EffectSummary]) = {
    val ncTestPntiByGlobal, cTestPntiByGlobal, ncTestPntiByApplet, cTestPntiByApplet =
      mutable.ArrayBuffer[PNodeTaskInfo]()
    val discrepancyNodes = mutable.ArrayBuffer[(PNodeTaskInfo, PNodeTaskInfo)]()

    perTestMergedPnti.foreach { pnti =>
      // if we care about applet grouping, else we care only test pgo decision
      val appletOrTestPgoWouldDisable =
        appletPntiMap.map(appletMap => appletMap(pnti.fullName())).getOrElse(pgoWouldDisableCache(pnti))
      val pntiWithGlobalPgoDecision = pntiGlobalMap(pnti.fullName())
      // categorising pnti into don't cache or cache by applet and global pgo decision
      // and get nodes where decisions differ: applet pgo decision says don't cache, global's says cache
      (appletOrTestPgoWouldDisable, pntiWithGlobalPgoDecision.pgoWouldDisableCache) match {
        case (true, false) =>
          val x = (pnti, pntiWithGlobalPgoDecision.pnti)
          discrepancyNodes += x
          cTestPntiByGlobal += pnti
          ncTestPntiByApplet += pnti
        case (true, true) =>
          ncTestPntiByGlobal += pnti
          ncTestPntiByApplet += pnti
        case (false, false) =>
          cTestPntiByApplet += pnti
          cTestPntiByGlobal += pnti
        case (false, true) =>
          cTestPntiByApplet += pnti
          ncTestPntiByGlobal += pnti
      }
    }
    val testSummary =
      getTopLevelSummary(testName, ncTestPntiByApplet, cTestPntiByApplet, ncTestPntiByGlobal, cTestPntiByGlobal)
    val discrepancyNodesSummary =
      if (discrepancyNodes.nonEmpty)
        getDiscrepancyNodesSummary(testName, discrepancyNodes)
      else
        // so UI still show something if there's no discrepancy nodes,
        // otherwise it'll not show this test row if no nodes
        Seq(getDefaultEffectSummaryIfNoDiscrepancy(testName))
    testName -> (testSummary +: discrepancyNodesSummary).toList
  }

  /**
   * EffectSummary require time metrics in before and after ConfigMetrics to be in ms, and type Long
   * however for each node the wall time, cache time etc are very small so converting to ms would result
   * in 0ms and lose precision, so for discrepancy nodes we convert to ms on UI side,
   * and keep them in ns here
   */
  @async def getDiscrepancyNodesSummary(testName: String, discrepancyNodes: Seq[(PNodeTaskInfo, PNodeTaskInfo)]) =
    discrepancyNodes.apar.map { case (beforeNode, afterNode) =>
      val beforeCacheMetrics = getCacheMetricsFromPnti(beforeNode)
      val afterCacheMetrics = getCacheMetricsFromPnti(afterNode)
      val diff = CacheMetricDiff(beforeCacheMetrics, afterCacheMetrics)
      EffectSummary.fromNodeCacheEffectSummary(
        testName,
        Some(beforeNode.fullName()),
        beforeCacheMetrics,
        afterCacheMetrics,
        diff.summary)
    }

  private def getTopLevelSummary(
      testName: String,
      nonCachedPnti: Seq[PNodeTaskInfo],
      cachedPnti: Seq[PNodeTaskInfo],
      nonCachedPntiGlobal: Seq[PNodeTaskInfo],
      cachedPntiGlobal: Seq[PNodeTaskInfo]): EffectSummary = {
    val before = generateCacheConfig(nonCachedPnti, cachedPnti)
    val after = generateCacheConfig(nonCachedPntiGlobal, cachedPntiGlobal)
    val diff = CacheMetricDiff(before, after)
    // fromNodeCacheEffectSummary assumes the time int cacheMetrics is in ms
    EffectSummary.fromNodeCacheEffectSummary(testName, None, before, after, diff.summary)
  }

  /**
   * This is the estimation calculation of PGO effect, given the non-cached and cached nodes determined by PGO,
   * produce an estimated CacheConfig. This is only used to produce effectsummary at applet or test level,
   * and we need the time metrics to be in ms instead of in ns
   */
  private[optimus] def generateCacheConfig(
      nonCachedNodes: Seq[PNodeTaskInfo],
      cachedNodes: Seq[PNodeTaskInfo]): CacheMetrics = {
    val increasedTotalTime = nonCachedNodes.map(n => n.ancAndSelfTime / 1e6 * n.cacheHit).sum
    CacheMetrics(
      cacheTime = cachedNodes.map(_.cacheTime / 1e6).sum,
      cacheHits = cachedNodes.map(_.cacheHit).sum.toDouble,
      cacheMisses = cachedNodes.map(_.cacheMiss).sum.toDouble,
      evictions = cachedNodes.map(_.evicted).sum.toDouble,
      // workaround to calculate total time
      selfTime = increasedTotalTime,
      ancTime = cachedNodes.map(_.ancAndSelfTime / 1e6).sum,
    )
  }
  @async def writeSummaryReport(
      firstProfilerDirName: String,
      secondProfilerDirName: String,
      firstProfilerDir: String,
      secondProfilerDir: String,
      defaultFilePath: String,
      enforceWithConfComparison: Boolean): Unit = {

    val (combinedSummary, _) = combineAndWriteSummary(
      firstProfilerDirName,
      secondProfilerDirName,
      firstProfilerDir,
      secondProfilerDir,
      enforceWithConfComparison,
      false)

    writeHtmlReport(combinedSummary, defaultFilePath)
  }
  @async def combineAndWriteSummary(
      firstProfilerDirName: String,
      secondProfilerDirName: String,
      firstProfilerDir: String,
      secondProfilerDir: String,
      enforceWithConfComparison: Boolean,
      enableAppletGrouping: Boolean): (
      (Map[String, TestAndNodesEffectSummary], Map[String, TestAndNodesEffectSummary]),
      Option[Map[String, Set[String]]]) = {
    val (beforeAllPaths, afterAllPaths) =
      findCorrespondingMultipleFiles(
        firstProfilerDirName,
        secondProfilerDirName,
        firstProfilerDir,
        secondProfilerDir,
        enforceWithConfComparison,
        enableAppletGrouping)

    val (beforeConfigMetrics, afterConfigMetrics) =
      (beforeAllPaths(configFileExtension), afterAllPaths(configFileExtension))
    val (beforeHotspots, afterHotspots) = (beforeAllPaths(hotspotsFileName), afterAllPaths(hotspotsFileName))

    val (beforeByTest, afterByTest, diffsByTest) =
      parseAndCombineConfigMetrics(firstProfilerDirName, secondProfilerDirName, beforeConfigMetrics, afterConfigMetrics)

    val (beforeByNode, afterByNode, diffsByNode) =
      parseAndCombineNodeStats(firstProfilerDirName, secondProfilerDirName, beforeHotspots, afterHotspots)

    val appletToTestMap = getAppletToTestMap(enableAppletGrouping, diffsByTest.keySet, beforeAllPaths)

    val summaryByTest = beforeByTest.keys.map { testName =>
      testName -> combineMetrics(testName, None, beforeByTest, afterByTest, diffsByTest)
    } toMap

    val summaryByTestNode = beforeByNode.keys.map { testName =>
      val beforeNodeMap = beforeByNode(testName)
      val afterNodeMap = afterByNode(testName)
      val commonNodeMap = beforeNodeMap.keySet.intersect(afterNodeMap.keySet)
      val distinctBeforeNodeMap = beforeNodeMap -- commonNodeMap
      val distinctAfterNodeMap = afterNodeMap -- commonNodeMap
      val commonNodeSummaryList = commonNodeMap.map { nodeName =>
        combineCacheMetrics(testName, nodeName, beforeByNode, afterByNode, diffsByNode)
      }.toList
      val distinctBeforeNodeSummaryList = distinctBeforeNodeMap.map { case (nodeName, cacheMetrics) =>
        combineCacheMetricsForDistinctNode(testName, nodeName, Some(cacheMetrics), None)
      }.toList
      val distinctAfterNodeSummaryList = distinctAfterNodeMap.map { case (nodeName, cacheMetrics) =>
        combineCacheMetricsForDistinctNode(testName, nodeName, None, Some(cacheMetrics))
      }.toList
      testName -> (commonNodeSummaryList, distinctBeforeNodeSummaryList ++ distinctAfterNodeSummaryList)
    } toMap

    val combinedSummaryCommon = summaryByTest
      .map { case (testName, summary) =>
        val (commonNodesSummary, distinctNodesSummary) = summaryByTestNode(testName)
        testName -> TestAndNodesEffectSummary(summary, commonNodesSummary)
      }
    val combinedSummaryDistinct = summaryByTest
      .map { case (testName, summary) =>
        val (commonNodesSummary, distinctNodesSummary) = summaryByTestNode(testName)
        testName -> TestAndNodesEffectSummary(summary, distinctNodesSummary)
      }

    ((combinedSummaryCommon, combinedSummaryDistinct), appletToTestMap)
  }
  @async def getAppletToTestMap(
      enableAppletGrouping: Boolean,
      allTestNames: Set[String],
      resultsGroupedPaths: GroupedFilePathsMapByFileFilter) = {
    if (enableAppletGrouping) {
      val appletNamesPaths = resultsGroupedPaths(appletInfoFileName)
      val testsWithAppletMap = parseAndGetAppletNameMap(appletNamesPaths)
      val testsWithoutApplet = allTestNames.filterNot(testsWithAppletMap.values.flatten.toSeq.contains(_))
      Some(testsWithAppletMap ++ Map(unknownApplet -> testsWithoutApplet))
    } else None
  }
  @async def parseAndGetAppletNameMap(groupedPaths: Map[String, Seq[String]]): AppletToTestMapping = {
    // Seq of (applet, tests)
    val result = groupedPaths.apar
      .map { case (test, appletFilePaths) =>
        val appletNames = appletNamesFromFiles(appletFilePaths)
        appletNames.apar.map(_ -> test)
      }
      .toSeq
      .flatten

    val resultGroupedByApplet = result.groupBy(_._1) // e.g. Map(applet1 -> Seq((applet1, test1), (applet1, test2))
    resultGroupedByApplet.mapValuesNow(_.map(_._2).toSet) // e.g. Map(applet1 -> Set(test1, test2))
  }
  private def combineMetrics(
      test: String,
      nodeName: Option[String],
      before: Map[String, ConfigMetrics],
      after: Map[String, ConfigMetrics],
      diffs: Map[String, MetricDiff]): EffectSummary = {
    val beforeMetrics = before(test)
    val afterMetrics = after(test)
    val diffMetrics = diffs(test)
    // time should be in ms, was in ns
    val beforeMetricsWithTimeInMilliSecond = convertConfigMetricsTimeInMs(beforeMetrics)
    val afterMetricsWithTimeInMilliSecond = convertConfigMetricsTimeInMs(afterMetrics)
    EffectSummary(
      test,
      nodeName,
      beforeMetricsWithTimeInMilliSecond,
      afterMetricsWithTimeInMilliSecond,
      diffMetrics.summary)
  }
  private def combineCacheMetrics(
      testName: String,
      nodeName: String,
      before: Map[String, Map[String, CacheMetrics]],
      after: Map[String, Map[String, CacheMetrics]],
      diffs: Map[String, Map[String, CacheMetricDiff]]): EffectSummary = {
    val beforeMetrics = before(testName)(nodeName)
    val afterMetrics = after(testName)(nodeName)
    val diffMetrics = diffs(testName)(nodeName)
    EffectSummary.fromNodeCacheEffectSummary(testName, Some(nodeName), beforeMetrics, afterMetrics, diffMetrics.summary)
  }
  private def combineCacheMetricsForDistinctNode(
      testName: String,
      nodeName: String,
      before: Option[CacheMetrics],
      after: Option[CacheMetrics]): EffectSummary = {

    (before, after) match {
      case (Some(beforeM), None) =>
        EffectSummary.fromNodeCacheEffectSummary(
          testName,
          Some(nodeName),
          beforeM,
          defaultCacheMetrics,
          defaultCacheMetricDiff)
      case (None, Some(afterM)) =>
        EffectSummary.fromNodeCacheEffectSummary(
          testName,
          Some(nodeName),
          defaultCacheMetrics,
          afterM,
          defaultCacheMetricDiff)
      case (_, _) =>
        EffectSummary.fromNodeCacheEffectSummary(
          testName,
          Some(nodeName),
          defaultCacheMetrics,
          defaultCacheMetrics,
          defaultCacheMetricDiff)
    }
  }
  private def parseAndCombineConfigMetrics(
      beforeProfilerDirName: String,
      afterProfilerDirName: String,
      beforeResultsGroupedPaths: Map[String, Seq[String]],
      afterResultsGroupedPaths: Map[String, Seq[String]])
      : (Map[String, ConfigMetrics], Map[String, ConfigMetrics], Map[String, MetricDiff]) = {
    val afterResults = metricsFromFiles(afterResultsGroupedPaths.values.flatten.toSeq, afterProfilerDirName)
    val beforeResults = metricsFromFiles(beforeResultsGroupedPaths.values.flatten.toSeq, beforeProfilerDirName)
    val testsToCompare = getTestsToCompare(afterResults.keySet, beforeResults.keySet)
    val after = afterResults.filterKeysNow(testsToCompare.contains)
    val before = beforeResults.filterKeysNow(testsToCompare.contains)
    val diffs = testsToCompare.map { test => test -> MetricDiff(before(test), after(test)) }.toMap

    (before, after, diffs)
  }
  private def parseAndCombineNodeStats(
      beforeProfilerDirName: String,
      afterProfilerDirName: String,
      beforeResultsGroupedPaths: Map[String, Seq[String]],
      afterResultsGroupedPaths: Map[String, Seq[String]]): (
      Map[String, Map[String, CacheMetrics]],
      Map[String, Map[String, CacheMetrics]],
      Map[String, Map[String, CacheMetricDiff]]) = {
    val afterResults = cacheMetricsFromFiles(afterResultsGroupedPaths.values.flatten.toSeq, afterProfilerDirName)
    val beforeResults = cacheMetricsFromFiles(beforeResultsGroupedPaths.values.flatten.toSeq, beforeProfilerDirName)
    val testsToCompare = getTestsToCompare(afterResults.keySet, beforeResults.keySet)
    val after = afterResults.filterKeysNow(testsToCompare.contains)
    val before = beforeResults.filterKeysNow(testsToCompare.contains)

    def filterNodeMap(map1: Map[String, CacheMetrics], map2: Map[String, CacheMetrics]): Map[String, CacheMetrics] = {
      val commonKeys = map1.keySet.intersect(map2.keySet)
      val commonNodes = map1.filterKeysNow(commonKeys)
      commonNodes
    }

    val filteredBefore = before.map { case (test, nodeMap) => test -> filterNodeMap(nodeMap, after(test)) }
    val filteredAfter = after.map { case (test, nodeMap) => test -> filterNodeMap(nodeMap, filteredBefore(test)) }

    val diffs = testsToCompare.map { test =>
      val nodeMap = filteredBefore(test)
      val nodeCacheMetricMap = nodeMap.map { case (nodeName, cacheMetric) =>
        nodeName -> CacheMetricDiff(cacheMetric, filteredAfter(test)(nodeName))
      }
      test -> nodeCacheMetricMap
    }.toMap

    (before, after, diffs)
  }
  private def writeHtmlReport(
      contentForJs: (Map[String, TestAndNodesEffectSummary], Map[String, TestAndNodesEffectSummary]),
      rootDir: String): Unit = {
    val htmlReportDir = Paths.get(rootDir).resolve("html-report")
    Files.createDirectories(htmlReportDir)
    def write(filename: String, data: String): Unit = {
      val absolutePath = htmlReportDir.resolve(filename).toAbsolutePath
      try {
        Files.write(absolutePath, data.getBytes)
        log.info(s"Wrote effect summary data at $absolutePath")
      } catch {
        case NonFatal(ex) =>
          log.warn(s"Could not write file at $absolutePath", ex)
      }
    }
    val htmlResources = "html" :: "css" :: "js" :: Nil
    htmlResources.foreach { resource =>
      copyFileFromResources(htmlReportDir.resolve(s"$prefix.$resource"), s"/$resource/$prefix.$resource")
    }

    val (commonNodesContent, distinctNodesContent) = contentForJs
    write(s"${prefix}_result.js", optconfEffectJsContent("cacheConfigImpactDiff", commonNodesContent))
    write(s"${prefix}_result_extra.js", optconfEffectJsContent("cacheConfigImpactDiffExtra", distinctNodesContent))

    val htmlLocation = htmlReportDir.resolve(prefix).toString.replace('\\', '/')
    log.info(s"See profiling report at file:///${htmlLocation}.html")
  }
  private def optconfEffectJsContent(contentName: String, tests: Map[String, TestAndNodesEffectSummary]): String = {
    // Option tuple arguments: number, ifCheckExceed
    val threshold = ThresholdDiffSummary(
      acceptWallTimeChange = None,
      acceptCpuTimeChange = Some(10, true),
      acceptCacheTimeChange = Some(10, true),
      acceptMaxHeapChange = Some(10, true),
      acceptCacheHitsChange = Some(0, false), // if cache hits fewer than before then not acceptable
      acceptCacheMissesChange = Some(0, true), // if cache misses more than before then not acceptable
      acceptCacheHitRatioChange = None,
      acceptEvictionsChange = None,
      acceptDalRequestsChange = None,
      acceptEngineWallTimeChange = None
    )
    val sb = new JStringBuilder
    sb.append(s"${contentName} = [")
    tests.foreach { case (test, testAndNodesEffectSummary) =>
      val (testSummary, nodesSummary) =
        (testAndNodesEffectSummary.testSummary, testAndNodesEffectSummary.nodesSummaries)
      sb.append("\n{ testName: \'")
      sb.append(testSummary.testName)
      sb.append("\', before: ")
      sb.append(testSummary.before.toJson())
      sb.append(",\n  after: ")
      sb.append(testSummary.after.toJson())
      sb.append(",\n  diff: ")
      sb.append(threshold.applyThresholdAndToString(testSummary.diffs))
      sb.append(",\n nodes: [ ")
      nodesSummary.foreach { node =>
        sb.append("{ nodeName: \'")
        sb.append(node.nodeName.getOrElse(""))
        sb.append("\', before: ")
        sb.append(node.before.toJson())
        sb.append(",\n  after: ")
        sb.append(node.after.toJson())
        sb.append(",\n  diff: ")
        sb.append(threshold.applyThresholdAndToString(node.diffs))
        sb.append("},\n")
      }
      sb.append("]},")
    }
    sb.append("]\n")
    sb.toString
  }

  private[optimus] def isRequiredFile(fileName: String, requiredContainedName: String): Boolean =
    fileName.contains(requiredContainedName)
  private def combinePntis(pntis1: Seq[PNodeTaskInfo], pntis2: Seq[PNodeTaskInfo]): Seq[PNodeTaskInfo] =
    combinePNTIs(pntis1 ++ pntis2).values.toSeq

  /** Pnti time metric is in ns */
  private[optimus] def getCacheMetricsFromPnti(pnti: PNodeTaskInfo): CacheMetrics = CacheMetrics(
    cacheTime = pnti.cacheTime.toDouble,
    cacheHits = pnti.cacheHit.toDouble,
    cacheMisses = pnti.cacheMiss.toDouble,
    evictions = pnti.evicted.toDouble,
    selfTime = pnti.selfTime.toDouble,
    ancTime = pnti.ancSelfTime().toDouble
  )
  final case class PntiWithPgoDecision(
      pnti: PNodeTaskInfo,
      pgoWouldDisableCache: Boolean
  )
  final case class OverallPgoEffect(
      appletSummary: Option[EffectSummary],
      testToNodesMap: Map[String, List[EffectSummary]]
  )
  final case class AppletToTestMapAndGlobalPnti(appletToTestMap: AppletToTestPntiMap, globalPntis: Seq[PNodeTaskInfo])
  final case class TestAndNodesEffectSummary(testSummary: EffectSummary, nodesSummaries: List[EffectSummary])
  type AppletToTestPntiMap = Map[String, Map[String, Seq[PNodeTaskInfo]]]
  type AppletToTestMapping = Map[String, Set[String]]

  getPGODrsTableContent(profilerDirName, cmdLine.rootDir, true)
}
