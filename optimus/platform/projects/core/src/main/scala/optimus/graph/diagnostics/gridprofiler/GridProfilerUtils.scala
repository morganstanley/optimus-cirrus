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

import msjava.tools.util.MSProcess
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.CrumbHint
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.graph.DiagnosticSettings
import optimus.graph.FileInterceptor
import optimus.graph.OGSchedulerLostConcurrency
import optimus.graph.OGTrace
import optimus.graph.Settings
import optimus.graph.diagnostics.JsonMapper
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.diagnostics.ReportOnShutdown
import optimus.graph.diagnostics.configmetrics.ConfigMetrics
import optimus.graph.diagnostics.gridprofiler.GridProfiler.Metric
import optimus.graph.diagnostics.gridprofiler.GridProfiler.ProfilerOutputStrings
import optimus.breadcrumbs.crumbs.Crumb.ProfilerSource
import optimus.graph.diagnostics.Report
import optimus.graph.diagnostics.gridprofiler.GridProfiler.getConfigMetricSummary
import optimus.graph.diagnostics.gridprofiler.GridProfiler.getDefaultLevel
import optimus.graph.diagnostics.gridprofiler.GridProfiler.getHotspots
import optimus.graph.diagnostics.gridprofiler.GridProfiler.getHotspotsSummary
import optimus.graph.diagnostics.gridprofiler.GridProfiler.getSummaryTable
import optimus.graph.diagnostics.gridprofiler.GridProfiler.log
import optimus.graph.diagnostics.gridprofiler.GridProfilerDefaults._
import optimus.graph.diagnostics.gridprofiler.HotspotFilter.filterHotspots
import optimus.graph.diagnostics.gridprofiler.Level._
import optimus.graph.diagnostics.pgo.AutoPGOThresholds
import optimus.graph.diagnostics.pgo.CacheFilterTweakUsageDecision
import optimus.graph.diagnostics.pgo.ConfigWriterSettings
import optimus.graph.diagnostics.pgo.DisableCache
import optimus.graph.diagnostics.pgo.PGOMode
import optimus.graph.diagnostics.pgo.Profiler
import optimus.platform.inputs.registry.ProcessGraphInputs
import optimus.platform.util.PrettyStringBuilder
import optimus.platform.util.Version
import optimus.scalacompat.collection._
import spray.json.JsObject
import spray.json.JsString
import spray.json.JsValue
import spray.json.JsonParser

import java.io.File
import java.io.{StringWriter, Writer}
import java.lang.{StringBuilder => JStringBuilder}
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.util.{ArrayList => JArrayList}
import scala.collection.compat._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/*
 Are you interested in adding things to this object?

 *BEWARE THE STATIC INITIALIZATION!*

 This class is initialized statically (in <clinit>), which means that *ANY* exceptions thrown therein will produce an
 ExceptionInInitializer error and kill the JVM.

 Furthermore, static initialization is a minefield of poorly understood JVM race conditions and weird bugs! Avoid as
 much as possible the usage of vals and vars in this Object, because they will be instantiated during <clinit>. Bugs
 like these are way too much fun: hard to repro, intermittent, probably JVM version dependent. If you can avoid vals,
 please do!
 */

object GridProfilerUtils {
  // Stuff we initialize statically:
  val hotspotsFileName = "profiler_hotspots"

  /** The MiniGrid profiler writes a file with extra properties to be included in the summary crumb */
  val MiniGridMetaDataFileSysProp = "optimus.minigrid.metaDataFile"

  private val hardCrumbMax = DiagnosticSettings.getIntProperty("optimus.graph.hotstpot.crumbs.max", 1000)

  private val fileInterceptorCrumbSizeLimit = 8192 // typical size is 4k-ish

  private var summaryTable: Option[SummaryTable] = None
  private var autoGenerateConfig: Boolean = _
  private var openFiles: Seq[String] = Nil
  private var openConnections: Seq[String] = Nil
  private var fileStamp: Option[String] = None

  @volatile private var appIdForCrumbs: String = "unknown"
  private[optimus] def setAppIdForCrumbs(id: String): Unit = {
    appIdForCrumbs = id
  }

  import optimus.breadcrumbs.crumbs.{Properties => P}
  // not a val because Version.properties is already lazily evaluated so the whole call is very fast after the
  // first invocation.
  def appProperties: Properties.Elems = Version.verboseProperties + (P.appId -> appIdForCrumbs)

  def fallBackName: String = {
    // First optimus/app/etc class name on the stack
    val clsNames = new Exception().getStackTrace map (_.getClassName)
    val idx = clsNames.lastIndexWhere(x => !x.matches("(scala|java|sun|com|org).*"))
    val clsName = if (idx >= 0) clsNames(idx) else ""
    val cls = if (clsName.indexOf("$") > 0) clsName.take(clsName.indexOf("$")) else clsName

    // The PID
    val vmName = java.lang.management.ManagementFactory.getRuntimeMXBean.getName
    val pid = if (vmName.indexOf('@') >= 0) "_" + vmName.take(vmName.indexOf('@')) else ""

    // example: "myapp.app.common.apprunner.RunCalc_7848"
    if (cls.isEmpty) s"AppNameUnknown$pid" else s"$cls$pid"
  }

  // generally the app name and pid shouldn't be overridden, but profiled UI tests run out of process so we use the
  // session name and pid (instead of irrelevant HttpServletRequestWrapper details)
  def generateFileName(appNameOverride: Option[String] = None, pidOverride: Option[Int] = None): String = {
    val maybeAppName = appNameOverride orElse sys.env.get("APP_NAME")
    val pid = pidOverride.getOrElse(MSProcess.getPID)
    val fmt = DateTimeFormatter.ofPattern("yyyyMMddHHmmss", Locale.ENGLISH)
    val baseFName =
      // either {APP_NAME}_PID or f.q.class.name_PID
      maybeAppName
        .map(_.concat("_" + pid))
        .getOrElse(fallBackName) +
        // append username
        "_" + System.getProperty("user.name") +
        // append day/time
        "_" + LocalDateTime.now.format(fmt)

    baseFName
  }

  private[optimus] def outputPath(fileName: String, folder: String): String = s"$folder${File.separator}$fileName."

  private[optimus] def copyFileFromResources(filename: String, resourcePath: String): Unit = {
    copyFileFromResources(Paths.get(filename), resourcePath)
  }

  private[optimus] def copyFileFromResources(path: Path, resourcePath: String): Unit = {
    val file = path.toFile
    try {
      file.getParentFile.mkdirs()
      val in = getClass.getResource(resourcePath).openStream()
      Files.copy(in, file.toPath, StandardCopyOption.REPLACE_EXISTING)
      in.close()
    } catch {
      case e: Exception => log.error("Failed to write GridProfiler report", e)
    }
  }

  private def configAsJson(title: String, result: Any, sb: PrettyStringBuilder, last: Boolean = false): Unit = {
    sb.append(s""""$title": """).startBlock().append(s""""result": "$result"""").endBlock()
    if (!last) sb.append(",")
  }

  private def configAsJson: String = {
    val sb = new PrettyStringBuilder().setBlockMarkers("{", "}")
    sb.startBlock()
    sb.append(""""title": "App Config",""")
    configAsJson("Profiler mode", getDefaultLevel, sb)
    configAsJson("Ideal threads", Settings.idealThreadCountToCount, sb)
    val xmx = DiagnosticSettings.getJvmArg("-Xmx")
    configAsJson("Configured max heap", if (xmx ne null) xmx else "(not set)", sb, last = true)
    sb.endBlock()
    sb.toString
  }

  // time format - display ns in ms to 3 dp
  private def fmt(t: Long): String = "%8.3f".format(t * 1e-9)

  /**
   * Returns top 10 nodes by selfTime as json (consider writing all and only displaying 10 in javascript) hotspots = [ {
   * self: 0.0689, postComplete: 0.0010, hits: 0, misses: 0, benefit: 0.0, name: "a.b.c.node" }, { self: 0.2354,
   * postComplete: 0.5537, hits: 11, misses: 1, benefit: 1.0, name: "x.y.z.otherNode" } ]
   */
  private def hotspotsAsJson(pntis: JArrayList[PNodeTaskInfo]): String = {
    val top = pntis.asScala.sortBy(-_.selfTime).take(10).filter(p => p.selfTime > 0 || p.postCompleteAndSuspendTime > 0)
    val sb = new JStringBuilder
    def writeTime(name: String, v: Long): Unit = sb.append(name).append(if (v != 0) fmt(v) else "\"\"").append(",")
    def writeCount(name: String, v: Long): Unit = sb.append(name).append(if (v != 0) v else "\"\"").append(",")

    sb.append("hotspots = [")
    top.foreach { pnti =>
      sb.append("\n")
      sb.append("{ ")
      sb.append(" name: \"").append(pnti.fullName()).append('\"').append(",")
      val disableCache = DisableCache.autoConfigure(pnti, AutoPGOThresholds())
      sb.append(if (disableCache) s""" highlight: "true", """ else "")
      writeTime(" self: ", pnti.selfTime)
      writeTime(" postComplete: ", pnti.postCompleteAndSuspendTime)
      writeCount(" hits: ", pnti.cacheHit)
      writeCount(" misses: ", pnti.cacheMiss)
      writeTime(" benefit: ", pnti.cacheBenefit())
      sb.setLength(sb.length() - 1); // drop last comma
      sb.append(" },")
    }
    if (top.nonEmpty) sb.setLength(sb.length() - 1); // drop last comma
    sb.append("\n]")
    sb.toString
  }

  def writeDataOnShutdown(fileName: String, autoGenerateConfig: Boolean): Unit =
    writeData(fileName, getSummaryTable, autoGenerateConfig = autoGenerateConfig, onShutdown = true)

  def writeData(fileName: String, autoGenerateConfig: Boolean): Unit =
    writeData(fileName, getSummaryTable, autoGenerateConfig = autoGenerateConfig, onShutdown = false)

  def writeDataToDirectory(dir: String, fileName: String): Unit =
    writeData(fileName, getSummaryTable, suppliedDir = dir, autoGenerateConfig = false, onShutdown = false)

  private def writePGOData(baseFileName: String, modes: collection.Seq[PGOMode]): Unit =
    writePGOData(baseFileName, baseFileName, getHotspotsSummary.values.toSeq, modes)

  // minimum required for auto pgo is dumping the optconf and the csv we use for validation.
  // Might want to consider dumping more data (eg, hotspots csv and html report)
  private[optimus] def writePGOData(
      configMetricFile: String,
      pgoGenFile: String,
      hotspots: collection.Seq[PNodeTaskInfo],
      modes: collection.Seq[PGOMode] = collection.Seq(DisableCache)): Unit = {
    ConfigMetrics.writeConfigMetricsCsv(configMetricFile, getConfigMetricSummary(hotspots))
    val settings = ConfigWriterSettings(modes)
    Profiler.autoGenerateConfig(pgoGenFile, hotspots, settings)
  }

  private def miniGridMeta: Option[P.Elem[JsObject]] = sys.props.get(MiniGridMetaDataFileSysProp).map { filename =>
    P.miniGridMeta := JsonParser(Files.readAllBytes(Paths.get(filename))).asJsObject
  }

  // Accumulate the data until optimus.graph.diagnostics.Report requests it
  private[diagnostics] def writeData(
      fileStamp: String,
      summaryTable: SummaryTable,
      autoGenerateConfig: Boolean,
      onShutdown: Boolean,
      suppliedDir: String = ""): Unit = {
    this.fileStamp = Some(fileStamp)
    this.summaryTable = Some(summaryTable)
    this.autoGenerateConfig = autoGenerateConfig
    this.openFiles = FileInterceptor.openedFiles.asScala
    this.openConnections = FileInterceptor.connectionsRaw().asScala

    // the registered data will be written to file and crumbs will be sent...
    if (onShutdown) ReportOnShutdown.register(suppliedDir) // ...either on shutdown
    else Report.writeReportsAndCrumbs(suppliedDir) // ...or immediately
  }

  def sendSummaryCrumb(): Unit = summaryTable.foreach { summary =>
    val meta = miniGridMeta
    // if miniGrid has passed us metadata, we write the crumb to long term storage so that we can see a long history
    val hints: Set[CrumbHint] = if (miniGridMeta.isDefined) Set(CrumbHint.LongTermRetentionHint) else Set.empty
    sendSummaryCrumb(ChainedID.root, summary, hints, meta.toSeq)
  }

  private[optimus] def getLocalAndRemoteHotspots: (Metric.Hotspots, Map[String, Metric.Hotspots]) =
    (Profiler.getLocalHotspots(), getHotspots.filterKeysNow(_ != GridProfiler.clientKey))

  private[gridprofiler] def writeHotspots(writer: Writer): Unit = {
    val (rawLocal, rawRemote) = getLocalAndRemoteHotspots
    val aggregated =
      GridProfilerData.aggregateSingle(rawRemote, Some(rawLocal), GridProfilerDefaults.defaultAggregationType)
    val filtered: Map[String, ArrayBuffer[PNodeTaskInfo]] =
      aggregated.mapValuesNow { x =>
        val pntis = x.values
        val filtered = filterHotspots(pntis.to(ArrayBuffer), ProcessGraphInputs.HotspotsFilter.currentValueOrThrow())
        filtered
      }
    val filteredCnt = aggregated.values.map(_.size).sum - filtered.values.map(_.size).sum
    PNodeTaskInfo.printCSV(filtered.mapValuesNow(_.asJava).asJava, filteredCnt, writer)
    sendHotspotCrumbs(filtered)
    Breadcrumbs.flush()
  }

  def publishTaskLevel(nid: ChainedID, taskId: String, engine: String, pd: ProfilerData, nodesExecuted: Long): Unit = {
    val taskSummary = getSummaryTable(pd)
    sendSummaryCrumb(
      nid,
      taskSummary,
      more = collection.Seq(P.taskId -> taskId, P.engine -> engine, P.tasksExecutedOnEngine -> nodesExecuted))
  }

  private def sendHotspotCrumbs(agg: Map[String, ArrayBuffer[PNodeTaskInfo]]): Unit = {
    if (Breadcrumbs.isInfoEnabled) {
      def ms(us: Long): Long = us / 1000000L
      for {
        (engine, pntis) <- agg
        pnti <- pntis.take(hardCrumbMax)
      } {
        val enq = pnti.enqueuingPropertyName
        val pname = pnti.fullName()
        val debug = mutable.ArrayBuffer.empty[String]
        val (propertyNode, subNode) =
          if (enq eq null) {
            debug += "enq=null"
            (pname, pname)
          } else
            (enq, pname)
        Breadcrumbs.info(
          ChainedID.root,
          PropertiesCrumb(
            _,
            ProfilerSource,
            appProperties + P.Elems(
              P.hotspotEngine -> engine,
              P.hotspotPropertyName -> propertyNode,
              P.hotspotStart -> pnti.start,
              P.hotspotEvicted -> pnti.evicted,
              P.hotspotInvalidated -> pnti.invalidated,
              P.hotspotCacheMiss -> pnti.cacheMiss,
              P.hotspotCacheHit -> pnti.cacheHit,
              P.hotspotCacheHitFromDifferentTasks -> pnti.cacheHitFromDifferentTask,
              P.hotspotNodeReusedTime -> ms(pnti.nodeReusedTime),
              P.hotspotCacheBenefit -> ms(pnti.cacheBenefit),
              P.hotspotAvgCacheTime -> ms(pnti.avgCacheTime),
              P.hotspotAncSelfTime -> ms(pnti.ancSelfTime),
              P.hotspotPostCompleteTime -> ms(pnti.postCompleteAndSuspendTime),
              P.hotspotWallTime -> ms(pnti.wallTime),
              P.hotspotSelfTime -> ms(pnti.selfTime),
              P.hotspotTweakLookupTime -> ms(pnti.tweakLookupTime),
              P.hotspotChildNodeLookupCount -> pnti.childNodeLookupCount,
              P.hotspotChildNodeLookupTime -> pnti.childNodeLookupTime,
              P.hotspotCollisionCount -> pnti.collisionCount,
              P.hotspotCacheable -> Some(pnti.getCacheable),
              P.hotspotFavorReuse -> Some(pnti.getFavorReuse),
              P.hotspotIsScenarioIndependent -> Some(pnti.isScenarioIndependent),
              P.hotspotPropertyFlags -> pnti.flags,
              P.hotspotPropertyFlagsString -> pnti.flagsAsString,
              P.hotspotRunningNode -> subNode,
              P.hotspotPropertySource -> pnti.source,
              P.debug -> debug.mkString(";")
            )
          )
        )
      }
    }
  }

  def toExtractedGroups(summary: SummaryTable): Map[String, JsObject] = {
    def extract(parentPath: collection.Seq[String], colGrp: ColGroup): collection.Seq[(String, JsValue)] = {
      val path = parentPath :+ colGrp.col.name
      val value = colGrp.col.extractor(summary)
      val formatted = colGrp.col.format.format(value).trim
      val extractedChildren = colGrp.children.flatMap(extract(path, _))
      (path.mkString("/"), JsString(formatted)) +: extractedChildren
    }
    val colGroups = SummaryTable.allGroups(summary)
    colGroups.map { grp =>
      grp.name -> JsObject(grp.children.flatMap(extract(Nil, _)).toMap)
    }.toMap
  }

  private def sendSummaryCrumb(
      id: ChainedID,
      summary: SummaryTable,
      hints: Set[CrumbHint] = Set.empty,
      more: collection.Seq[P.Elem[_]] = Nil): Unit = {
    val extractedGroups = toExtractedGroups(summary)
    val props = appProperties ++ more.toList + P.Elem(P.profSummary, extractedGroups)
    Breadcrumbs.info(id, u => PropertiesCrumb(u, ProfilerSource, hints, props.m: _*))
  }

  private def filterAndTrimFilesList(files: collection.Seq[String]): Seq[String] = {
    // trim up to limit
    var limit = 0
    files.takeWhile { s =>
      limit += s.length
      limit < fileInterceptorCrumbSizeLimit
    }
  }

  // this version is called at app end
  // it receives a file list because we fetch it there for another reason
  // and it does not erase the recorded files because the app is about to end anyway
  private[optimus] def sendFileInterceptorCrumb(files: collection.Seq[String]): Unit = {
    if (files.nonEmpty)
      Breadcrumbs.info(
        ChainedID.root,
        PropertiesCrumb(_, ProfilerSource, P.profOpenedFiles -> filterAndTrimFilesList(files) :: appProperties))
  }

  // this version is called in dist at the end of task
  // For the very first dist task, the one for which the engine was spun up,
  // its crumb will include files opened at engine startup (which includes optimus init)
  // For subsequent tasks - only what they opened
  private[optimus] def sendFileInterceptorCrumb(id: ChainedID): Unit = {
    if (FileInterceptor.isEnabled) {
      val files = FileInterceptor.openedFiles().asScala
      FileInterceptor.eraseFiles()
      if (files.nonEmpty)
        Breadcrumbs.info(
          id,
          PropertiesCrumb(_, ProfilerSource, P.profOpenedFiles -> filterAndTrimFilesList(files) :: appProperties))
    }
  }

  def summaryTableContent: Option[String] =
    if (getDefaultLevel >= LIGHT) summaryTable.map(JsonMapper.mapper.writeValueAsString)
    else None

  def hotspotsContent: Option[String] =
    if (getDefaultLevel >= HOTSPOTSLIGHT && !DiagnosticSettings.profilerDisableHotspotsCSV) {
      val sw = new StringWriter()
      writeHotspots(sw)
      Some(sw.toString)
    } else None

  def pgoDecisionContent: Option[String] =
    if (
      getDefaultLevel >= HOTSPOTSLIGHT && !DiagnosticSettings.profilerDisableHotspotsCSV && DiagnosticSettings.explainPgoDecision
    ) {
      Some(CacheFilterTweakUsageDecision.csvFormat())
    } else None

  final case class CrumbStats(rootUuid: String, counts: Map[Crumb.Source, Int])

  def crumbStats: Option[String] = Some(
    JsonMapper.mapper.writeValueAsString(CrumbStats(ChainedID.root.repr, Breadcrumbs.getCounts)))

  def sendCSVResultCrumb(csvRes: CSVResults): Unit = {
    if (Breadcrumbs.isInfoEnabled) for {
      (statType, stats) <- csvRes.all
      stat <- stats
    } Breadcrumbs.info(ChainedID.root, PropertiesCrumb(_, ProfilerSource, P.profStatsType -> statType :: P.profStats -> stat :: appProperties))
  }

  def copyExistingOGTrace(target: Path): Boolean = {
    val condition = getDefaultLevel == RECORDING
    if (condition) {
      OGTrace.copyTraceTo(target.toFile.getAbsolutePath)
    }
    condition
  }

  def hotspotsSummary: Option[JArrayList[PNodeTaskInfo]] = {
    // check getDefaultLevel and trace mode (for local tracing) independently, since setTraceMode does not set
    // GridProfiler.getDefaultLevel. If either is set then we want to get the hotspots
    if (getDefaultLevel >= HOTSPOTSLIGHT || OGTrace.getTraceMode.collectsHotspots) {
      val hotspots: collection.Seq[PNodeTaskInfo] = getHotspotsSummary.values.toSeq
      Some(new JArrayList(hotspots.asJava))
    } else None
  }

  def writeAutogeneratedPgoData(dir: Path, prefix: String): Unit =
    if (autoGenerateConfig) {
      val baseFileName = outputPath(prefix, dir.toString)
      val configFolder = Option(defaultConfigFolder).filter(_.nonEmpty).map(outputPath(prefix, _))
      writePGOData(baseFileName, configFolder.getOrElse(baseFileName), getHotspotsSummary.values.toSeq)
    }

  def lostCCContent: Option[String] =
    Some(OGSchedulerLostConcurrency.reportAsJson(OGSchedulerLostConcurrency.lostCCEntries))

  def openFilesContent: Option[String] =
    Option(openFiles).filter(_.nonEmpty).map(_.sorted.mkString("\n"))

  def sendOpenFilesCrumb(): Unit = sendFileInterceptorCrumb(openFiles)

  def openConnectionsContent: Option[String] =
    Option(openConnections).filter(_.nonEmpty).map(_.sorted.mkString("\n"))

  def sendOpenConnectionsCrumb(): Unit = sendFileInterceptorCrumb(openConnections)

  def writePgoData(prefix: String): Unit = {
    import ProfilerOutputStrings._
    if (defaultPgoModes.nonEmpty) {
      val fileName =
        if (defaultPgoFolder.endsWith(optconfExtension)) defaultPgoFolder // someone supplied the file
        else s"$defaultPgoFolder${File.separator}$prefix" // someone supplied a folder
      writePGOData(fileName, defaultPgoModes)
    }
  }

  def gridProfilerJsContent: Option[String] = summaryTable.map { summary =>
    val sb = new JStringBuilder
    sb.append("gridprofiler = {")
    SummaryTable.allGroups(summary).foreach { g =>
      sb.append(g.toJson(summary))
      sb.append(",")
    }
    sb.append(s""""config":$configAsJson""")
    sb.append("}")
    sb.toString
  }

  def lostCCJsContent: Option[String] = {
    val lostCC = OGSchedulerLostConcurrency.lostCCEntries()
    if (lostCC.isEmpty) None else Some(OGSchedulerLostConcurrency.reportAsJs(lostCC))
  }

  def hotspotsJsContent: Option[String] = Some(hotspotsAsJson(OGTrace.liveReader.getHotspots()))

  def reportCSVFolder(suppliedDir: String): Option[Path] = {
    val csvPath = if (suppliedDir.nonEmpty) suppliedDir else GridProfiler.getDefaultCSVFolder
    Option(csvPath).filter(_.nonEmpty).map(s => Paths.get(s).toAbsolutePath)
  }

  def reportUniqueName: String = fileStamp.getOrElse(generateFileName())

  private[optimus] def writeFile(fname: String, contents: String): Unit = {
    val absolutePath = Paths.get(fname).toAbsolutePath
    try {
      Files.write(absolutePath, contents.getBytes)
      log.info(s"Wrote profiling data at $absolutePath")
    } catch {
      case NonFatal(ex) =>
        log.warn(s"Could not write file at $absolutePath", ex)
    }
  }

  def defaultCSVResultsExtractor: CSVResultsExtractor =
    new CSVResultsExtractor(GridProfiler.getCSV(None))
}

final class CSVResultsExtractor(val csvRes: CSVResults) {
  def dalStatsContent: Option[String] = Option(csvRes.dalStats).filter(_.nonEmpty)

  def customCountersNonPriqlContent: Option[String] = Option(csvRes.customCountersNonPriql).filter(_.nonEmpty)

  def customCountersPriqlContent: Option[String] = Option(csvRes.customCountersPriql).filter(_.nonEmpty)

  def syncStacksContent: Option[String] =
    if (getDefaultLevel >= LIGHT) Option(csvRes.syncStacks)
    else None

  def vtTtStacksContent: Option[String] =
    if (Settings.profileTemporalContext) Option(csvRes.vtTtStacks)
    else None

  def stallTimesContent: Option[String] = Option(csvRes.stallTimes).filter(_.nonEmpty)

  def environMetricsContent: Option[String] = Option(csvRes.environMetrics).filter(_.nonEmpty)

  def threadStatsContent: Option[String] = Option(csvRes.threadStats).filter(_.nonEmpty)
}
