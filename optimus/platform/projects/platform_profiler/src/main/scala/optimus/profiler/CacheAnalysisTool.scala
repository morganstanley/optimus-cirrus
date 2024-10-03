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
import optimus.graph.diagnostics.configmetrics.EffectSummary
import optimus.graph.diagnostics.configmetrics.MetricDiff
import optimus.graph.diagnostics.pgo.Profiler.combinePNTIs
import optimus.graph.diagnostics.pgo.Profiler.pgoWouldDisableCache
import optimus.platform.AsyncImplicits._
import optimus.platform.async
import optimus.profiler.CacheAnaysisDiffUtils._
import optimus.profiler.ConfigMetricsDiff.fileNameToAppletNames
import optimus.profiler.ConfigMetricsDiff.filenameToConfigMetrics
import optimus.profiler.MergeTraces.mergeTraces
import optimus.profiler.RegressionsConfigApps.extractTestAndFileName
import optimus.profiler.RegressionsConfigApps.profilerDirName
import optimus.scalacompat.collection._
import optimus.utils.ErrorIgnoringFileVisitor
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser

import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes
import scala.collection.mutable

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

  lazy val unknownApplet = "unknown"

  @async def getComparisonDrsTableContent(
      firstProfilerDirName: String,
      secondProfilerDirName: String,
      firstProfilerDir: String,
      secondProfilerDir: String,
      enableAppletGrouping: Boolean): Map[String, OverallPgoEffect] = {

    val (nodeSummaryByApplet, summaryByTest) = combineAndWriteSummary(
      firstProfilerDirName,
      secondProfilerDirName,
      firstProfilerDir,
      secondProfilerDir,
      enableAppletGrouping)

    appletToPgoEffect(nodeSummaryByApplet, summaryByTest, enableAppletGrouping)
  }

  @async def appletToPgoEffect(
      nodeSummaryByApplet: Map[String, TestToEffectSummariesMap],
      summaryByTest: Map[String, EffectSummary],
      enableAppletGrouping: Boolean): Map[String, OverallPgoEffect] = {
    nodeSummaryByApplet.apar.map { case (applet, testToNodesMap) =>
      val testAndNodeSummary = combineTestAndNodeSummary(testToNodesMap, summaryByTest)
      val appletSummary = getAppletSummaryFromTests(enableAppletGrouping, testAndNodeSummary, ifNeedMinValue = false)
      applet -> OverallPgoEffect(appletSummary, testAndNodeSummary)
    }
  }

  @async def combineTestAndNodeSummary(
      testToNodeSummary: TestToEffectSummariesMap,
      summaryByTest: Map[String, EffectSummary]): TestToEffectSummariesMap =
    testToNodeSummary.apar.map { case (test, nodeSummary) =>
      val testSummary = summaryByTest(test)
      test -> (testSummary +: nodeSummary)
    }

  // ------------------------Helper FUnctions for File Walk Algorithm (start) -----------------//
  def updatePerTestPntisAndGlobalPntis(
      testName: String,
      allTestsNames: Set[String],
      path: String,
      globalPnti: Seq[PNodeTaskInfo],
      enableGlobalPnti: Boolean,
      testToMergedPntiMap: Map[String, Seq[PNodeTaskInfo]]) = {
    val updatedAllTestNames = allTestsNames + testName
    val testPnti = mergeTraces(Seq(path), combinePntis)._1
    val updatedGlobalPnti: Seq[PNodeTaskInfo] = if (enableGlobalPnti) {
      val prevGlobalPnti = globalPnti
      combinePNTIs(prevGlobalPnti ++ testPnti).values.toSeq
    } else Seq.empty
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

  def getFinalAppletToTestPntiMap(
      enableAppletGrouping: Boolean,
      allTestsNames: Set[String],
      testsWithApplet: Set[String],
      testToMergedPntiMap: Map[String, Seq[PNodeTaskInfo]],
      appletToTestPntiMap: Map[String, Map[String, Seq[PNodeTaskInfo]]]): AppletToTestPntiMap = {
    // if no applet grouping
    if (!enableAppletGrouping) {
      Map(unknownApplet -> testToMergedPntiMap)
    } else { // with applet grouping
      mergeAppletAndPerTestPntis(allTestsNames, testsWithApplet, testToMergedPntiMap, appletToTestPntiMap)
    }
  }
  // ------------------------Helper Functions for File Walk Algorithm (end) ^ -----------------//

  /**
   * File walk algorithm that are used in both comparison regressions and PGO estimation
   * If enableAppletGrouping = true, parses appletInfo.csv;
   * If enableGlobalPnti = true, then combines all pntis from all ogtrace files (used in PGO estimation;
   * If enableTestConfigMEtrics = true, parses configMetrics.csv to get test summary (used in comparison)
   */
  def mergingPntiInFileWalking(
      profilerDirName: String,
      profilerDir: String,
      enableAppletGrouping: Boolean,
      enableGlobalPnti: Boolean,
      enableTestConfigMetrics: Boolean): (AppletToTestMapAndGlobalPnti, Option[TestToConfigMetrics]) = {
    val appletToTestPntiMap: mutable.Map[String, mutable.Map[String, Seq[PNodeTaskInfo]]] = mutable.Map()
    val testToMergedPntiMap: mutable.Map[String, Seq[PNodeTaskInfo]] = mutable.Map()
    var globalPnti: Seq[PNodeTaskInfo] = Seq.empty
    val testsWithApplet, allTestsNames: mutable.Set[String] = mutable.Set()
    val testToConfigMetrics: mutable.Map[String, ConfigMetrics] = mutable.Map()
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
                  testName = testName,
                  allTestsNames = allTestsNames.toSet,
                  path = path,
                  globalPnti = globalPnti,
                  enableGlobalPnti = enableGlobalPnti,
                  testToMergedPntiMap = testToMergedPntiMap.toMap
                )
              allTestsNames.clear()
              allTestsNames ++= updatedAllTestNames
              testToMergedPntiMap(testName) = updatedPerTestPnti
              globalPnti = updatedGlobalPnti
            case _ =>
          }
          // extract applet info, test config metrics if needed
          extractTestAndFileName(path, csvFileExtension, profilerDirName) match {
            case Some((testName, fileName)) if enableAppletGrouping && isRequiredFile(fileName, appletInfoFileName) =>
              log.info(s"Found applet file for test $testName")
              val (updatedTestsWithApplet, updatedAppletMap) = updateAppletToTestMap(
                testName,
                path,
                testsWithApplet.toSet,
                appletToTestPntiMap.mapValuesNow(_.toMap).toMap)
              testsWithApplet.clear()
              testsWithApplet ++= updatedTestsWithApplet
              replaceAppletMap(updatedAppletMap)
            case Some((testName, fileName)) if enableTestConfigMetrics && isConfigMetricsCsv(fileName) =>
              log.info(s"Found config metrics csv for test $testName")
              val configMetrics = filenameToConfigMetrics(Seq(Paths.get(path)), profilerDirName)
              testToConfigMetrics ++= configMetrics
            case _ =>
          }
          FileVisitResult.CONTINUE
        }
      }
    )
    val finalAppletToTestPntiMap = getFinalAppletToTestPntiMap(
      enableAppletGrouping,
      allTestsNames.toSet,
      testsWithApplet.toSet,
      testToMergedPntiMap.toMap,
      appletToTestPntiMap.mapValuesNow(_.toMap).toMap)
    val finalTestToConfigMetrics = if (enableTestConfigMetrics) Some(testToConfigMetrics.toMap) else None
    (AppletToTestMapAndGlobalPnti(finalAppletToTestPntiMap, globalPnti), finalTestToConfigMetrics)
  }

  // ---------------------- Helper Functions For PGO Estimation (Start) -------------------------//

  @async def getAppletPntiMapIfEnableAppletGrouping(
      enableAppletGrouping: Boolean,
      perAppletMergedPnti: Seq[PNodeTaskInfo]): Option[Map[String, PntiWithPgoDecision]] = {
    if (enableAppletGrouping) {
      val appletPntiPgoDecisionMap =
        getPntiPGODecisionMap(perAppletMergedPnti)
      Some(appletPntiPgoDecisionMap)
    } else
      None
  }

  /** pntiName -> (pnti, Boolean: disable cache = true; false otherwise) */
  @async def getPntiPGODecisionMap(pntis: Seq[PNodeTaskInfo]) =
    pntis.apar.map(pnti => pnti.fullName() -> PntiWithPgoDecision(pnti, pgoWouldDisableCache(pnti))).toMap

  private[optimus] def mergePntiFromTestToPntiMap(testToPntiMap: Map[String, Seq[PNodeTaskInfo]]): Seq[PNodeTaskInfo] =
    combinePNTIs(testToPntiMap.values.toSeq.flatten).values.toSeq

  @async def getGlobalOrFixedAppletPGODecisionMap(
      specifiedApplet: String,
      globalMergedPnti: Seq[PNodeTaskInfo],
      appletToPerTestMergedPntis: AppletToTestPntiMap): Map[String, PntiWithPgoDecision] = {
    if (specifiedApplet.isEmpty) {
      getPntiPGODecisionMap(globalMergedPnti)
    } else {
      val mergedPntisForApplet = appletMergedPntis(specifiedApplet, appletToPerTestMergedPntis)
      getPntiPGODecisionMap(mergedPntisForApplet)
    }
  }

  @async def appletMergedPntis(
      appletName: String,
      appletToPerTestMergedPntis: AppletToTestPntiMap): Seq[PNodeTaskInfo] = {
    appletToPerTestMergedPntis
      .get(appletName)
      .map { testToPntiMap => mergePntiFromTestToPntiMap(testToPntiMap) }
      .getOrElse(Seq.empty)
  }

  @async def getTableResultFromAppletMap(
      appletToPerTestMergedPntis: AppletToTestPntiMap,
      enableAppletGrouping: Boolean,
      showAppletMergedRawData: Boolean,
      pntiPGODecisionsMap: Map[String, PntiWithPgoDecision]): Map[String, OverallPgoEffect] = {
    appletToPerTestMergedPntis.apar.map { case (appletName, perTestMergedPntiMap) =>
      val perAppletMergedPnti = mergePntiFromTestToPntiMap(perTestMergedPntiMap)
      // Option of map of (pntiName -> (pnti, Boolean: disable cache or not by applet pgo)) if enable applet grouping
      val appletPntiMap: Option[Map[String, PntiWithPgoDecision]] =
        getAppletPntiMapIfEnableAppletGrouping(enableAppletGrouping, perAppletMergedPnti)

      // test -> (testLevelEffectSummary + discrepancyNodesEffectSummary)
      val testMap: Map[String, List[EffectSummary]] = perTestMergedPntiMap.apar.map {
        case (testName, perTestMergedPnti) =>
          getTestAndNodesSummary(
            testName,
            perTestMergedPnti,
            appletPntiMap,
            pntiPGODecisionsMap,
            showAppletMergedRawData)
      }
      val appletSummary: Option[EffectSummary] =
        getAppletSummaryFromTests(enableAppletGrouping, testMap, ifNeedMinValue = true)
      appletName -> OverallPgoEffect(appletSummary, testMap)
    }
  }

  // ---------------------- Helper Functions For PGO Estimation (End ) -------------------------//

  @async def getPGODrsTableContent(
      profilerDirName: String,
      profilerDir: String,
      enableAppletGrouping: Boolean,
      showAppletMergedRawData: Boolean = false,
      specifiedApplet: String = ""): Map[String, OverallPgoEffect] = {
    val (fileWalkResult, _) =
      mergingPntiInFileWalking(
        profilerDirName,
        profilerDir,
        enableAppletGrouping,
        enableGlobalPnti = true,
        enableTestConfigMetrics = false
      ) // care about global pnti, don't care about test config metrics
    val (appletToPerTestMergedPntis, globalMergedPnti) = (fileWalkResult.appletToTestMap, fileWalkResult.globalPntis)

    val pntiPGODecisionsMap: Map[String, PntiWithPgoDecision] =
      getGlobalOrFixedAppletPGODecisionMap(specifiedApplet, globalMergedPnti, appletToPerTestMergedPntis)

    getTableResultFromAppletMap(
      appletToPerTestMergedPntis - specifiedApplet,
      enableAppletGrouping,
      showAppletMergedRawData,
      pntiPGODecisionsMap)
  }

  /**
   * Summing over test effectSummaries to work out applet effectSummary, for PGO estimation some fields are kept null
   * for unavailable info such as max heap, dal requests, so need min value to represent those fields.
   * For comparison where test summaries are grabbed from config metrics csv, no null fields are needed
   */
  private def getAppletSummaryFromTests(
      enableAppletGrouping: Boolean,
      testMap: Map[String, List[EffectSummary]],
      ifNeedMinValue: Boolean): Option[EffectSummary] = {
    if (enableAppletGrouping) {
      // test summary is always the first element in the list
      val allTestSummaries = testMap.values.map(x => x.head).toList
      if (allTestSummaries.length == 1) {
        val testSummary = allTestSummaries.head
        // I noticed if applet just contains 1 test, the diffSummary calculated below using beforeConfig and afterConfig
        // for the applet summary metric can deviate from the only test summary it contains by 0.001,
        // when it should be equal to test summary,
        // therefore setting them equal to each other here
        Some(
          EffectSummary(
            testName = "",
            nodeName = None,
            before = testSummary.before,
            after = testSummary.after,
            diffs = testSummary.diffs))
      } else {
        val beforeConfig = getConfigSumFromTestSummaries(allTestSummaries, s => s.before, ifNeedMinValue)
        val afterConfig = getConfigSumFromTestSummaries(allTestSummaries, s => s.after, ifNeedMinValue)
        val diffSummary = MetricDiff(beforeConfig, afterConfig).summary
        // Set min value for metrics information we do not have, so can be shown null on UI
        val diffConfig = if (ifNeedMinValue) getDiffSummaryWithMinValue(diffSummary) else diffSummary
        Some(
          EffectSummary(testName = "", nodeName = None, before = beforeConfig, after = afterConfig, diffs = diffConfig))
      }
    } else None
  }

  private def getPntiWithAppletOrTestPgoDecision(
      pnti: PNodeTaskInfo,
      appletPntiMap: Option[Map[String, PntiWithPgoDecision]],
      showAppletMergedRawData: Boolean) = {
    appletPntiMap match {
      case Some(appletMap) => // meaning running per applet pgo
        val pntiWithAppletDecision = appletMap.get(pnti.fullName())
        pntiWithAppletDecision
          .map { appletMergedPnti => // pnti is found in appletPntiMap
            if (showAppletMergedRawData) appletMergedPnti
            else PntiWithPgoDecision(pnti, appletMergedPnti.pgoWouldDisableCache) // showing per test merged raw data
          }
          .getOrElse(
            PntiWithPgoDecision(pnti, pgoWouldDisableCache(pnti))
          ) // deals with if pnti not found, should never happen
      case None => PntiWithPgoDecision(pnti, pgoWouldDisableCache(pnti)) // meaning running per test pgo
    }
  }
  @async def getTestAndNodesSummary(
      testName: String,
      perTestMergedPnti: Seq[PNodeTaskInfo],
      appletPntiMap: Option[Map[String, PntiWithPgoDecision]],
      pntiPGODecisionsMap: Map[String, PntiWithPgoDecision],
      showAppletMergedRawData: Boolean = false): (String, List[EffectSummary]) = {
    val ncTestPntiByGlobal, cTestPntiByGlobal, ncTestPntiByApplet, cTestPntiByApplet =
      mutable.ArrayBuffer[PNodeTaskInfo]()
    val discrepancyNodes = mutable.ArrayBuffer[ComparisonNodes]()

    perTestMergedPnti.foreach { pnti =>
      // if we care about applet grouping, else we care only test pgo decision
      val pntiWithAppletOrTestPgoDecision: PntiWithPgoDecision =
        getPntiWithAppletOrTestPgoDecision(pnti, appletPntiMap, showAppletMergedRawData)

      // This is the pgo decision we compare with, global PGO decision or fixed applet decision
      val pntiWithComparisonPgoDecision =
        pntiPGODecisionsMap.getOrElse(pnti.fullName(), pntiWithAppletOrTestPgoDecision)
      // categorising pnti into don't cache or cache by applet and global pgo decision
      // and get nodes where decisions differ: applet pgo decision says don't cache, global's says cache
      (pntiWithAppletOrTestPgoDecision.pgoWouldDisableCache, pntiWithComparisonPgoDecision.pgoWouldDisableCache) match {
        case (true, false) =>
          discrepancyNodes += ((
            Some(pntiWithAppletOrTestPgoDecision),
            Some(pntiWithComparisonPgoDecision)): ComparisonNodes)
          cTestPntiByGlobal += pnti
          ncTestPntiByApplet += pnti
        case (true, true) =>
          ncTestPntiByGlobal += pnti
          ncTestPntiByApplet += pnti
        case (false, false) =>
          cTestPntiByApplet += pnti
          cTestPntiByGlobal += pnti
        case (false, true) =>
          discrepancyNodes += ((
            Some(pntiWithAppletOrTestPgoDecision),
            Some(pntiWithComparisonPgoDecision)): ComparisonNodes)
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
  @async def getDiscrepancyNodesSummary(testName: String, discrepancyNodes: Seq[ComparisonNodes]) =
    discrepancyNodes.apar.map { case (before, after) =>
      (before, after) match {
        case (Some(beforeNode), Some(afterNode)) =>
          val beforeCacheMetrics = getCacheMetricsFromPnti(beforeNode.pnti)
          val afterCacheMetrics = getCacheMetricsFromPnti(afterNode.pnti)
          val diff = CacheMetricDiff(beforeCacheMetrics, afterCacheMetrics)
          EffectSummary.fromNodeCacheEffectSummary(
            testName,
            Some(beforeNode.pnti.fullName()),
            beforeCacheMetrics,
            afterCacheMetrics,
            diff.summary,
            isBeforeDisabledCache = Some(beforeNode.pgoWouldDisableCache)
          )
        case (Some(beforeNode), _) =>
          val beforeCacheMetrics = getCacheMetricsFromPnti(beforeNode.pnti)
          combineCacheMetricsForDistinctNode(testName, beforeNode.pnti.fullName(), Some(beforeCacheMetrics), None)
        case (_, Some(afterNode)) =>
          val afterCacheMetrics = getCacheMetricsFromPnti(afterNode.pnti)
          combineCacheMetricsForDistinctNode(testName, afterNode.pnti.fullName(), None, Some(afterCacheMetrics))
        case (_, _) => combineCacheMetricsForDistinctNode(testName, "", None, None)
      }
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
      selfTime = cachedNodes.map(_.selfTime / 1e6).sum,
      totalTime = cachedNodes.map(_.ancAndSelfTime / 1e6).sum + increasedTotalTime,
    )
  }

  private def combineTestToConfigMetrics(beforeResults: TestToConfigMetrics, afterResults: TestToConfigMetrics)
      : (Map[String, ConfigMetrics], Map[String, ConfigMetrics], Map[String, MetricDiff]) = {
    val testsToCompare = getTestsToCompare(afterResults.keySet, beforeResults.keySet)
    val after = afterResults.filterKeysNow(testsToCompare.contains)
    val before = beforeResults.filterKeysNow(testsToCompare.contains)
    val diffs = testsToCompare.map { test => test -> MetricDiff(before(test), after(test)) }.toMap

    (before, after, diffs)
  }

  def filterMap(
      commonApplets: Set[String],
      originalMap: AppletToTestPntiMap,
      secondMap: AppletToTestPntiMap): (AppletToTestPntiMap, AppletToTestPntiMap) = {
    val filteredMap1: mutable.Map[String, Map[String, Seq[PNodeTaskInfo]]] = mutable.Map()
    val filteredMap2: mutable.Map[String, Map[String, Seq[PNodeTaskInfo]]] = mutable.Map()
    commonApplets.foreach { applet =>
      val testMap1 = originalMap(applet)
      val testMap2 = secondMap(applet)
      val commonTests = testMap1.keySet.intersect(testMap2.keySet)
      filteredMap1(applet) = testMap1.filter { case (test, _) => commonTests.contains(test) }
      filteredMap2(applet) = testMap2.filter { case (test, _) => commonTests.contains(test) }
    }
    (filteredMap1.toMap, filteredMap2.toMap)
  }

  @async def combineNodeStats(
      testName: String,
      beforeHotspotsNodes: Seq[PNodeTaskInfo],
      afterHotspotsNodes: Seq[PNodeTaskInfo]): List[EffectSummary] = {
    val allNodes = mutable.ArrayBuffer[ComparisonNodes]()
    // hotspots nodes are all cached
    val beforeNodeMap =
      beforeHotspotsNodes.apar.map(x => x.fullName() -> PntiWithPgoDecision(x, pgoWouldDisableCache = false)).toMap
    val afterNodeMap =
      afterHotspotsNodes.apar.map(x => x.fullName() -> PntiWithPgoDecision(x, pgoWouldDisableCache = false)).toMap
    val allNodeName = beforeNodeMap.keySet.union(afterNodeMap.keySet)
    allNodeName.foreach { nodeName =>
      allNodes += ((beforeNodeMap.get(nodeName), afterNodeMap.get(nodeName)): ComparisonNodes)
    }
    getDiscrepancyNodesSummary(testName, allNodes).toList
  }

  @async def filterAndCombineNodeStats(
      beforeAppletToPerTestHotspots: AppletToTestPntiMap,
      afterAppletToPerTestHotspots: AppletToTestPntiMap): Map[String, TestToEffectSummariesMap] = {
    val appletsToCompare = getTestsToCompare(beforeAppletToPerTestHotspots.keySet, afterAppletToPerTestHotspots.keySet)
    log.info(s"Found ${appletsToCompare.size} common applets")
    val after = afterAppletToPerTestHotspots.filterKeysNow(appletsToCompare.contains)
    val before = beforeAppletToPerTestHotspots.filterKeysNow(appletsToCompare.contains)

    val (filteredBefore, filteredAfter) = filterMap(appletsToCompare, before, after)
    filteredBefore.apar.map { case (applet, beforeTestToPntis) =>
      val afterTestToPntis = filteredAfter(applet)
      val testToNodeSummary = beforeTestToPntis.apar.map { case (testName, beforeNodes) =>
        val afterNodes = afterTestToPntis(testName)
        val nodeSummary = combineNodeStats(testName, beforeNodes, afterNodes)
        testName -> nodeSummary
      }
      applet -> testToNodeSummary
    }
  }

  @async def combineAndWriteSummary(
      firstProfilerDirName: String,
      secondProfilerDirName: String,
      firstProfilerDir: String,
      secondProfilerDir: String,
      enableAppletGrouping: Boolean): (Map[String, TestToEffectSummariesMap], Map[String, EffectSummary]) = {

    val (beforeAppletToPerTestHotspots, beforeTestConfigMetrics) =
      mergingPntiInFileWalking(
        firstProfilerDirName,
        firstProfilerDir,
        enableAppletGrouping,
        enableGlobalPnti = false,
        enableTestConfigMetrics = true)
    val (afterAppletToPerTestHotspots, afterTestConfigMetrics) =
      mergingPntiInFileWalking(
        secondProfilerDirName,
        secondProfilerDir,
        enableAppletGrouping,
        enableGlobalPnti = false,
        enableTestConfigMetrics = true)

    val (beforeByTest, afterByTest, diffsByTest) =
      combineTestToConfigMetrics(beforeTestConfigMetrics.get, afterTestConfigMetrics.get)
    val summaryByTest = beforeByTest.keys.apar.map { testName =>
      testName -> combineMetrics(testName, None, beforeByTest, afterByTest, diffsByTest)
    } toMap

    val nodeSummaryByApplet = filterAndCombineNodeStats(
      beforeAppletToPerTestHotspots.appletToTestMap,
      afterAppletToPerTestHotspots.appletToTestMap)

    (nodeSummaryByApplet, summaryByTest)
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
    totalTime = pnti.ancAndSelfTime.toDouble
  )
  final case class PntiWithPgoDecision(
      pnti: PNodeTaskInfo,
      pgoWouldDisableCache: Boolean
  )
  final case class OverallPgoEffect(
      appletSummary: Option[EffectSummary],
      testToNodesMap: TestToEffectSummariesMap
  )
  final case class AppletToTestMapAndGlobalPnti(appletToTestMap: AppletToTestPntiMap, globalPntis: Seq[PNodeTaskInfo])
  type AppletToTestPntiMap = Map[String, Map[String, Seq[PNodeTaskInfo]]]
  type TestToConfigMetrics = Map[String, ConfigMetrics]
  type TestToEffectSummariesMap = Map[String, List[EffectSummary]]
  type ComparisonNodes = (Option[PntiWithPgoDecision], Option[PntiWithPgoDecision])

  getPGODrsTableContent(profilerDirName, cmdLine.rootDir, true)
}
