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

import com.opencsv.CSVWriter
import msjava.slf4jutils.scalalog.getLogger
import optimus.graph.NodeTaskInfo
import optimus.graph.XSFTRemapper
import optimus.graph.cache.Caches
import optimus.graph.cache.UNodeCache
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.diagnostics.gridprofiler.GridProfiler.ProfilerOutputStrings
import optimus.graph.diagnostics.gridprofiler.GridProfilerDefaults
import optimus.graph.diagnostics.gridprofiler.GridProfilerUtils
import optimus.graph.diagnostics.pgo.ConfigWriterSettings
import optimus.graph.diagnostics.pgo.DisableCache
import optimus.graph.diagnostics.pgo.DisableXSFT
import optimus.graph.diagnostics.pgo.PGOMode
import optimus.graph.diagnostics.pgo.Profiler
import optimus.graph.diagnostics.pgo.SuggestXSFT
import optimus.platform.inputs.GraphInputConfiguration
import optimus.profiler.MergeTraces.csvSuffix
import optimus.profiler.RegressionsConfigApps.extractTestAndFileName
import optimus.profiler.RegressionsConfigApps.findFiles
import optimus.profiler.RegressionsConfigApps.outputConfigFilePath
import optimus.scalacompat.collection._
import optimus.utils.app.DelimitedStringOptionHandler
import optimus.utils.app.GroupedDelimitedStringOptionHandler
import optimus.utils.ErrorIgnoringFileVisitor
import org.kohsuke.args4j
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser

import java.io.File
import java.io.FileWriter
import java.io.IOException
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes
import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.BufferedSource
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex
import scala.util.matching.Regex.MatchIterator

final case class RegressionInfo(
    testCase: String,
    appName: String,
    testDate: String,
    moduleGroupName: String,
    moduleName: String)

final case class EnhancedPNodeInfo(
    regressionInfo: RegressionInfo,
    engine: String,
    nodeCalls: String,
    evictions: String,
    invalidates: String,
    cacheHits: String,
    cacheMisses: String,
    cacheTime: String,
    xsLookupTime: String,
    nodeReusedTime: String,
    cacheBenefit: String,
    avgCacheTime: String,
    wallTime: String,
    selfTime: String,
    ancSelfTime: String,
    nodeFullName: String,
    isCacheable: String,
    isScenarioIndependent: String,
    favorReuse: String
) {
  def asCSVPrintable: Array[String] = {
    Array(
      regressionInfo.testCase,
      regressionInfo.appName,
      regressionInfo.testDate,
      regressionInfo.moduleGroupName,
      regressionInfo.moduleName,
      engine,
      nodeCalls,
      evictions,
      invalidates,
      cacheHits,
      cacheMisses,
      cacheTime,
      xsLookupTime,
      nodeReusedTime,
      cacheBenefit,
      avgCacheTime,
      wallTime,
      selfTime,
      ancSelfTime,
      nodeFullName,
      isCacheable,
      isScenarioIndependent,
      favorReuse
    )
  }
}

private[optimus] object RegressionsConfigApps {
  private lazy val log = getLogger(this)

  val defaultFileName = "runcalc-default"
  val moduleName = "MODULE_NAME" // see environment repo qaCalcrunner.sh
  val moduleGroupName = "MODULE_GROUP_NAME" // see environment repo qaCalcrunner.sh
  val testplanDetailsFileName = "testplan_details" // see environment repo qaAppConfig.sh
  val metaDirName = "META"
  val profilerDirName = "profiler"

  // group 1 = path, group 2 = testName, group 3 = fileName (for ogtrace or configMetrics.csv files)
  private lazy val groupNames = Seq("path", "testName", "fileName")
  private def createPathRegex(profilerDirName: String) =
    new Regex(
      regex = s"(.*)\\${File.separator}(.*)\\${File.separator}$profilerDirName\\${File.separator}(.*)\\.",
      groupNames = groupNames: _*)

  private def parsePath(path: String, pathRegex: Regex): MatchIterator = pathRegex.findAllIn(path)
  private def isCorrectFile(path: String, extension: String): Boolean = path.endsWith(extension)
  private def isInCorrectDirectory(matches: MatchIterator): Boolean =
    matches.hasNext && matches.groupNames == groupNames &&
      !matches
        .group(groupNames(1))
        .contains(File.separator) && // make sure we're not in a subdirectory of the test dir (other than profiler)
      !matches
        .group(groupNames(2))
        .contains(File.separator) // make sure we're not in a subdirectory of the one we are interested in

  private def foundCorrectFileInProfilerDirectory(path: String, extension: String, matches: MatchIterator): Boolean =
    isCorrectFile(path, extension) && isInCorrectDirectory(matches)

  // returns regression test name and file name (if there was a match)
  def extractTestAndFileName(
      path: String,
      requiredExtension: String,
      profilerDirName: String): Option[(String, String)] =
    extractTestAndFileName(path, requiredExtension, createPathRegex(profilerDirName))

  private def extractTestAndFileName(
      path: String,
      requiredExtension: String,
      regex: Regex): Option[(String, String)] = {
    val matchesIter = parsePath(path, regex)
    if (foundCorrectFileInProfilerDirectory(path, requiredExtension, matchesIter)) {
      val testName = matchesIter.group(groupNames(1))
      val fileName = matchesIter.group(groupNames(2))
      Some((testName, fileName))
    } else None
  }

  object Group {
    val defaultName = "runcalc-default"
    val defaultGroupNames: Seq[String] = Seq(defaultName)
    val default: Group = Group(defaultName, Nil)
    val defaultGroups: Seq[Group] = Seq(default)
  }

  final case class Group(groupName: String, moduleNames: Seq[String]) {
    def contains(moduleName: String): Boolean = moduleNames.contains(moduleName)
  }

  final case class TestCase(test: String, module: String, groups: Seq[String]) {
    def isInGroup(group: Group): Boolean = groups.contains(group.groupName)
  }

  final case class TestCasesToPaths(testToPaths: Map[TestCase, Seq[String]]) {
    def containsModule(moduleName: String): Boolean = modules.contains(moduleName)
    def containsTest(testName: String): Boolean = tests.contains(testName)

    def getByModuleOrElse(moduleName: String, default: Seq[String]): Seq[String] =
      if (containsModule(moduleName)) {
        val allTestsForModule = testToPaths.filterKeysNow(
          _.module.toLowerCase == moduleName.toLowerCase
        ) // ignore test name when filtering by module
        allTestsForModule.values.flatten.toSeq
      } else default

    def getByTestOrElse(testName: String, default: Seq[String]): Seq[String] =
      if (containsTest(testName)) {
        val allTestsForTestName = testToPaths.filterKeysNow(
          _.test.toLowerCase == testName.toLowerCase
        ) // ignore module name when filtering by test
        allTestsForTestName.values.flatten.toSeq
      } else default

    // for testing
    def asByTestNameMap: Map[String, Seq[String]] = testToPaths.map { case (testCase, paths) => testCase.test -> paths }

    val tests: Set[String] = testToPaths.keySet.map(_.test)
    val testCases: Set[TestCase] = testToPaths.keySet
    val modules: Set[String] = testToPaths.keySet.map(_.module)
    val paths: Iterable[String] = testToPaths.values.flatten
  }

  def findFiles(
      pathStr: String,
      extensionRequired: String,
      profilerDirName: String,
      includeFile: String => Boolean = _ => true,
      includeTest: String => Boolean = _ => true, // need this to distinguish between reruns
      moduleInGroup: String => Seq[String] = _ => Group.defaultGroupNames)
      : Seq[String] = { // need this to allow grouping by module in regressions
    val grouped = findGroupedFiles(pathStr, extensionRequired, profilerDirName, includeFile, includeTest, moduleInGroup)
    grouped.paths.toList
  }

  private def modulePath(path: String): Path = {
    val file = new File(path)
    val parentDir = file.getParentFile.getParent // our parent dir is profiler, and its parent dir is test_case
    val testplanPathStr = s"$parentDir${File.separator}$metaDirName${File.separator}$testplanDetailsFileName"
    Paths.get(testplanPathStr)
  }

  // when we are in root_dir/test_dir/test_case/profiler, we can figure out our module
  def readModuleName(path: String): Option[String] = {
    val file = modulePath(path)
    val bufferedSourceOpt =
      try { Some(Source.fromFile(file.toFile)) }
      catch {
        case e: Exception =>
          log.warn(s"Could not read module from ${file.getFileName}", e)
          None
      }
    bufferedSourceOpt match {
      case Some(bufferedSource) =>
        try {
          val lines = bufferedSource.getLines().toSeq
          val testplanDetails = lines.map { content =>
            val Array(key, value) = content.split(",").map(_.trim.replace("\"", ""))
            key -> value
          }.toMap
          Some(testplanDetails(moduleName))
        } catch {
          case e: Exception =>
            log.warn(s"Could not read module from ${file.getFileName}", e)
            None
        } finally bufferedSource.close
      case None => None
    }
  }

  def findGroupedFiles(
      pathStr: String,
      extensionRequired: String,
      profilerDirName: String,
      includeFile: String => Boolean = _ => true,
      includeTest: String => Boolean = _ => true,
      moduleInGroup: String => Seq[String] = _ => Group.defaultGroupNames): TestCasesToPaths = {
    // keep track of the file paths that correspond to a given test
    val groupedFilePaths: mutable.Map[TestCase, ArrayBuffer[String]] = mutable.Map()
    val path = Paths.get(pathStr)
    Files.walkFileTree(
      path,
      new ErrorIgnoringFileVisitor {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          val path = file.toString
          extractTestAndFileName(path, extensionRequired, profilerDirName) match {
            case Some((testName, fileName)) if includeFile(fileName) && includeTest(testName) =>
              // we've found a test case so now let's look up the module from META/testplan_details
              val moduleNameOpt = readModuleName(path)
              val moduleName = moduleNameOpt.getOrElse(Group.defaultName)
              // if we didn't manage to read our module, this will go into the default 'catch all' group
              val groupNames = moduleInGroup(moduleName)
              log.info(
                s"Found file $fileName for test $testName (module: $moduleName, groups: ${groupNames.mkString(",")})")
              val testCase = TestCase(testName, moduleName, groupNames)
              if (groupedFilePaths.contains(testCase)) groupedFilePaths(testCase) += path
              else groupedFilePaths(testCase) = ArrayBuffer[String](path)
            case Some(_) => // correct extension and directory, but didn't pass predicate in includeTest - skip
            case None    => // this was not the file required, or in the wrong directory - either way, skip
          }
          FileVisitResult.CONTINUE
        }
      }
    )
    val testCasesToPaths = groupedFilePaths.map { case (testCase, arrayBuffer) => testCase -> arrayBuffer.toList }.toMap
    TestCasesToPaths(testCasesToPaths)
  }

  def findGroupedFilesMultiple(
      pathStr: String,
      profilerDirName: String,
      fileFilterMap: Map[String, (String => Boolean, String)],
      includeTest: String => Boolean = _ => true): Map[String, Map[String, Seq[String]]] = {
    // keep track of the file paths that correspond to a given test
    // (ogTrace -> test1 -> Seq[og trace files paths]])
    // (applet file -> test1 -> Seq[applet file paths]])
    val groupedFilePaths: mutable.Map[String, mutable.Map[String, ArrayBuffer[String]]] = mutable.Map()
    val path = Paths.get(pathStr)
    Files.walkFileTree(
      path,
      new ErrorIgnoringFileVisitor {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          val path = file.toString
          fileFilterMap.map { case (requiredName, (includeFile, extensionRequired)) =>
            extractTestAndFileName(path, extensionRequired, profilerDirName) match {
              case Some((testName, fileName)) if includeFile(fileName) && includeTest(testName) =>
                log.info(s"Found file $fileName for test $testName")
                if (groupedFilePaths.contains(requiredName)) {
                  val wantedFileMap = groupedFilePaths(requiredName)
                  if (wantedFileMap.contains(testName)) wantedFileMap(testName) += path
                  else wantedFileMap(testName) = ArrayBuffer[String](path)
                } else groupedFilePaths(requiredName) = mutable.Map(testName -> ArrayBuffer[String](path))
              case Some(_) => // correct extension and directory, but didn't pass predicate in includeTest - skip
              case None    => // this was not the file required, or in the wrong directory - either way, skip
            }
          }
          FileVisitResult.CONTINUE
        }
      }
    )
    groupedFilePaths.mapValuesNow(_.toMap.mapValuesNow(ArraySeq.from)).toMap
  }

  def outputConfigFilePath(outputPath: String, artifactsPath: String, fileName: String, suffix: String): String = {
    val prefix = if (outputPath.endsWith(File.separator)) outputPath else s"$outputPath${File.separator}"
    s"$prefix$artifactsPath${File.separator}$fileName$suffix"
  }

  // parse cmd line options defining groups of modules that run together to produce an optconf, along with group names
  def groups(groups: Seq[Seq[String]], groupNames: Seq[String]): Seq[Group] = {
    groupNames.zip(groups).map { case (name, group) => Group(name, group) }
  }

  // if at least one grouping included this testName then we should collect the corresponding path in the file tree walk
  def moduleIncluded(groups: Seq[Group])(moduleName: String): Seq[String] =
    if (groups.isEmpty || groups == Group.defaultGroups) Group.defaultGroupNames
    else groups.filter(_.moduleNames.contains(moduleName)).map(_.groupName)
}

/**
 * RunCalc regression output is dumped in the regression directory for the VI filer.
 *
 * For each of the ~1400 tests, profiler output is written to $test/profiler/
 *
 * This app takes the root directory, finds all hotspots CSV files and merges them to produce an aggregated CSV file,
 * which is saved under the output directory passed as parameter (specifically to $outputDirectory/$artifactsPath/)
 *
 * To run script (in install/common/bin): ./MergeHotspotsApp -r /path/to/original/regressions -o
 * /path/to/output/dir/merged_csv_files -a 1000-codetree-staging
 */
object MergeHotspotsApp extends App {
  private val log = getLogger(this)
  private val cmdLine = new MergeTracesCmdLine
  private val parser = new CmdLineParser(cmdLine)

  private lazy val csvHeader = Array(
    "Test Case",
    "App Name",
    "Test Date",
    "Module Group Name",
    "Module Name",
    "Engine",
    "Node Started",
    "Evictions",
    "Invalidates",
    "Cache Hits",
    "Cache Misses",
    "Total Cache Time (ms)",
    "Cross Scenario Lookup Time (ms)",
    "Node Reused Time (ms)",
    "Cache Benefit (ms)",
    "Avg Cache Time (ms)",
    "Wall Time (ms)",
    "Self Time (ms)",
    "ANC Self Time (ms)",
    "Property Name",
    "Cacheable",
    "isScenarioIndependent",
    "favorReuse"
  )

  try {
    parser.parseArgument(args.toArray: _*)
  } catch {
    case x: CmdLineException =>
      log.info(x.getMessage)
      parser.printUsage(System.err)
      System.exit(1)
  }

  def sourceFromCSV(csvFile: String): Option[BufferedSource] = {
    try {
      Some(Source.fromFile(csvFile))
    } catch {
      case _: IOException =>
        log.error(s"CSV file not found: $csvFile")
        None
    }
  }

  def readNextLine(bufferedCSVSrc: BufferedSource): String = {
    val linesIter = bufferedCSVSrc.getLines()
    if (linesIter.isEmpty) ""
    else linesIter.next()
  }

  def cacheableColIndex(csvHeader: Option[String]): Option[Int] =
    csvHeader.map { header =>
      val cleanedHeader = cleanCSVLine(header)
      cleanedHeader.indexOf("Cacheable")
    }

  def cleanCSVLine(csvLine: String): Seq[String] = csvLine.split(",").map(_.replace("\"", "").trim)

  def keepNA(rowEntry: String): Boolean = rowEntry == "n/a" || rowEntry.toBoolean

  def mergeCSVs(rootDir: String, outputDir: String, artifactsPath: String, profilerDirName: String): Unit = {
    val csvFiles = hotspotsCSVFiles(rootDir, profilerDirName)

    val outputCSVFile = new File(outputConfigFilePath(outputDir, artifactsPath, artifactsPath, csvSuffix))
    outputCSVFile.getParentFile.mkdirs()
    val writer = new FileWriter(outputCSVFile)
    val outCSV = new CSVWriter(writer)

    csvFiles.zipWithIndex.foreach { case (file, idx) =>
      val bufferedCSV = sourceFromCSV(file)
      val header = bufferedCSV.map(readNextLine)
      log.info(s"Processing csv: $file")

      val cacheableColIdx = cacheableColIndex(header)
      val testCaseName = extractTestAndFileName(file, "csv", profilerDirName) match {
        case Some((testName, _)) => testName
        case _                   => ""
      }
      val moduleName = RegressionsConfigApps.readModuleName(file).getOrElse("")

      try {
        if (idx == 0) outCSV.writeNext(csvHeader)
        for {
          remainingBufferedCSV <- bufferedCSV
          line <- remainingBufferedCSV.getLines()
          rowEntries = cleanCSVLine(line)
          if cmdLine.keepUncached || cacheableColIdx.fold(true)(colIdx => keepNA(rowEntries(colIdx)))
        } {
          val parsedCSVLine = parseCSVRow(rowEntries, testCaseName, moduleName).asCSVPrintable
          outCSV.writeNext(parsedCSVLine)
        }
      } catch {
        case _: Exception => log.error("Error when trying to write line to CSV")
      }
    }

    outCSV.close()
  }

  private[profiler] def parseCSVRow(
      rowEntries: Seq[String],
      testCase: String,
      moduleName: String): EnhancedPNodeInfo = {
    // noinspection ZeroIndexToHead
    EnhancedPNodeInfo(
      RegressionInfo(testCase = testCase, appName = "", testDate = "", moduleGroupName = "", moduleName = moduleName),
      engine = rowEntries(0),
      nodeCalls = rowEntries(1),
      evictions = rowEntries(2),
      invalidates = rowEntries(3),
      cacheHits = rowEntries(4),
      cacheMisses = rowEntries(5),
      cacheTime = rowEntries(6),
      xsLookupTime = rowEntries(7),
      nodeReusedTime = rowEntries(8),
      cacheBenefit = rowEntries(9),
      avgCacheTime = rowEntries(10),
      wallTime = rowEntries(11),
      selfTime = rowEntries(12),
      ancSelfTime = rowEntries(13),
      nodeFullName = rowEntries(14),
      isCacheable = rowEntries(15),
      isScenarioIndependent = rowEntries(16),
      favorReuse = rowEntries(17)
    )
  }

  private[profiler] def hotspotsCSVFiles(rootDir: String, profilerDirName: String): Seq[String] = {
    val hotspotsFilter = (fName: String) => fName.contains(GridProfilerUtils.hotspotsFileName)
    val hotspotsCSVFiles = findFiles(rootDir, "csv", profilerDirName, hotspotsFilter)
    log.info(s"Total hotspots CSV files found: ${hotspotsCSVFiles.size}")
    hotspotsCSVFiles
  }

  mergeCSVs(cmdLine.rootDir, cmdLine.outputDir, cmdLine.artifactsPath, cmdLine.profilerDirectoryName)
}

class RegressionsConfigCmdLine {
  import org.kohsuke.args4j.Option

  @Option(
    name = "-r",
    aliases = Array("--rootDirectory"),
    usage = "Root directory containing output directories for all regressions")
  val rootDir: String = ""

  @Option(name = "-o", aliases = Array("--outputDirectory"), usage = "Output directory for any generated result files")
  val outputDir: String = ""

  @Option(
    name = "-a",
    aliases = Array("--artifactsPath"),
    usage = "Corresponding artifacts directory under VI filer artifact path"
  )
  val artifactsPath: String = ""

  @Option(
    name = "-p",
    aliases = Array("--profilerDirectoryName"),
    usage = "Name of directory containing profiling data ('profiler' by default)",
    required = false)
  val profilerDirectoryName: String = RegressionsConfigApps.profilerDirName

  @Option(
    name = "-f",
    aliases = Array("--fileName"),
    usage = "Results filename ('runcalc' by default)", // used for optconf and summary csv files
    required = false)
  val fileName: String = RegressionsConfigApps.defaultFileName // this produces runcalc-default.optconf for regressions
}

trait GroupedCmdLine {
  import org.kohsuke.args4j.Option

  @Option(
    name = "-g",
    depends = Array("-n"), // --groups is not required but if it's provided, then --names becomes required
    aliases = Array("--groups"),
    usage =
      "Groups of regression tests or UI suites to include in merged optconfs. Each group is a comma-separated list" +
        "of suites, and groups are separated by semi-colons (e.g. uiTest1,uiTest2; reg1,reg2,reg3 will produce two optconfs," +
        "one created by merging traces from uiTest1 and uiTest2, and the other by merging traces from reg1, reg2, and reg3)",
    required = false,
    handler = classOf[GroupedDelimitedStringOptionHandler]
  )
  val groups: Seq[Seq[String]] = Nil

  @Option(
    name = "-n",
    forbids = Array("-f"), // --names overrides --fileName (each group output file will be named after the group)
    aliases = Array("--names"),
    usage = "Comma-separated group names corresponding to groups (must be provided if --groups is specified)",
    required = false,
    handler = classOf[DelimitedStringOptionHandler]
  )
  val groupNames: Seq[String] = Nil
}

class CompareCacheStatsCmdLine extends RegressionsConfigCmdLine {
  import org.kohsuke.args4j.Option
  @Option(
    name = "-s",
    aliases = Array("--dirToCompareWith"),
    usage = "Path to base directory to be compared to, containing performance cache stats for regressions"
  )
  val rerunDir: String = ""

  @Option(
    name = "-withConfComparison",
    aliases = Array("--enforceWithConfComparison"),
    usage =
      "Enforce comparison between metrics CSV files where one ran without an optconf and the rerun used an optconf",
    required = false
  )
  val enforceWithConfComparison: Boolean = false
}

class MergeTracesCmdLine extends RegressionsConfigCmdLine with GroupedCmdLine {
  import org.kohsuke.args4j.Option

  @Option(
    name = "--dumpCSV",
    usage =
      "Generate a CSV file from all merged ogtrace files. These files don't contain any scenario or cacheable columns")
  val dumpCSV: Boolean = false

  @args4j.Option(
    name = "--pgo-mode",
    usage = "Comma separated list specifying how to generate optconf (supported: disableCache, tweakUsage)",
    handler = classOf[DelimitedStringOptionHandler]
  )
  val pgoModes: Seq[String] = Seq("disableCache")

  @args4j.Option(
    name = "--withDisableXSFT",
    usage = "Comma separated list of PGO groups that will use disableXSFT and disableCache pgo modes",
    handler = classOf[DelimitedStringOptionHandler]
  )
  val withDisableXSFT: Seq[String] = Seq.empty

  @Option(
    name = "--keepUncached",
    usage = "Don't remove uncached entries from CSV files"
  )
  val keepUncached: Boolean = false
}

/**
 * RunCalc regression output is dumped in the regression directory for the VI filer.
 *
 * For each of the ~1300 tests, profiler output is written to $test/profiler/
 *
 * This app takes the root directory, finds all trace files, and merges them to produce a default config file, which is
 * saved under the output directory specified on the command line, with the filename matching the path prefix.
 *
 * To run script (in install/common/bin): ./MergeTraces -r /path/to/original/regressions -o
 * /path/to/output/dir/optimus_graph_config -a 1000-codetree-staging
 */
object MergeTraces extends App {
  import RegressionsConfigApps._
  import TraceHelper._

  private lazy val log = getLogger(this)
  private lazy val cmdLine = new MergeTracesCmdLine
  private lazy val parser = new CmdLineParser(cmdLine)

  try {
    parser.parseArgument(args.toArray: _*)
    val nGroups = cmdLine.groups.size
    val nGroupNames = cmdLine.groupNames.size
    if (nGroups != nGroupNames) {
      log.error(s"Specify $nGroups names corresponding to $nGroups groups (not $nGroupNames)")
      parser.printUsage(System.err)
      System.exit(1)
    }
  } catch {
    case x: CmdLineException =>
      log.info(x.getMessage)
      parser.printUsage(System.err)
      System.exit(1)
  }

  lazy val optconfSuffix = s".${ProfilerOutputStrings.optconfExtension}"
  lazy val ogtraceExtension = s".${ProfilerOutputStrings.ogtraceExtension}"
  private[profiler] lazy val csvSuffix = "-merged-hotspots.csv"

  private def combineCaches(combined: Seq[PNodeTaskInfo]): Set[String] = Profiler.getCaches(combined)

  // combine(trace) and mergeTraces(files) extracted for testing
  private[optimus] def mergeTraces(files: Seq[String]): (Seq[PNodeTaskInfo], Set[String]) =
    mergeTraces(files, XSFTRemapper.combineDependencies)

  type CombineFn = (Seq[PNodeTaskInfo], Seq[PNodeTaskInfo]) => Seq[PNodeTaskInfo]

  private[optimus] def mergeTraces(files: Seq[String], combine: CombineFn): (Seq[PNodeTaskInfo], Set[String]) = {
    val cacheNames = mutable.Set.empty[String]

    // use iterator to avoid opening over a thousand trace files at once
    val filePntis: Iterator[Seq[PNodeTaskInfo]] = files.iterator
      .map { file =>
        try {
          val pntis = getReaderFromFile(file).getHotspots.asScalaUnsafeImmutable
          log.debug(s"Processing ${pntis.size} hotspots from $file")
          pntis.map(resetOgTraceConfig)
        } catch {
          case e: Exception =>
            log.error(s"Skipping file $file because OGTraceReader failed with exception:")
            e.printStackTrace()
            Nil
        }
      }
      val allPntis = filePntis
      .reduceOption { (trace, otherTrace) =>
        val combined = combine(trace, otherTrace) // merge traces as we go
        val cacheNamesFromTraces = combineCaches(combined)
        cacheNames ++= cacheNamesFromTraces // keep track of configured custom caches as we go
        combined
      }

    (allPntis.getOrElse(Nil), Set[String](cacheNames.toSeq: _*))
  }

  private[profiler] def resetOgTraceConfig(profNode: PNodeTaskInfo): PNodeTaskInfo = {
    profNode.flags &= ~NodeTaskInfo.DONT_CACHE // drop configs (if any) that were in effect in each ogtrace
    profNode.flags &= ~NodeTaskInfo.EXTERNALLY_CONFIGURED_CUSTOM_CACHE
    profNode.flags &= ~NodeTaskInfo.EXTERNALLY_CONFIGURED_POLICY
    profNode
  }

  private[profiler] def dumpCombinedCSV(pntis: Seq[PNodeTaskInfo], outputDir: String, artifactsPath: String): Unit = {
    val outputCSVFile = new File(outputConfigFilePath(outputDir, artifactsPath, csvSuffix, artifactsPath))
    outputCSVFile.getParentFile.mkdirs()
    val writer = new FileWriter(outputCSVFile)
    try { PNodeTaskInfo.printCSV(Map("" -> pntis).mapValuesNow(_.asJava).asJava, 0, writer) }
    finally { writer.close() }
  }

  private[profiler] def integrityCheck(confPath: Path): Unit =
    integrityCheck(new String(Files.readAllBytes(confPath)), confPath.toString)
  private[profiler] def integrityCheck(conf: String, path: String = ""): Unit = {
    val where = if (path.nonEmpty) s" at $path" else ""
    // try applying - this shouldn't throw, but if it does, MergeTraces fails and PGO job rejects optconf
    try {
      GraphInputConfiguration.configureCachePoliciesFromContent(conf) // [SEE_MERGE_TRACES_INTEGRITY_CHECK]
      Caches.getSharedCache(UNodeCache.globalCacheName) match {
        case Some(globalCache) =>
          if (globalCache ne UNodeCache.global)
            throw new IllegalArgumentException(s"Managed to disconnect global state from named cache")
          if (globalCache.getMaxSize == 0)
            throw new IllegalArgumentException(s"Rejecting optconf$where, global cache maxSize is 0")
        case None =>
          throw new IllegalArgumentException(s"No global cache specified in optconf$where")
      }
    } catch {
      case e: Exception => throw new IllegalArgumentException(s"Rejecting optconf $where, error when applying", e)
    } finally GraphInputConfiguration.resetCachePolicies()
  }

  private def mergeTracesFromFiles(
      files: Seq[String],
      outputDir: String,
      artifactsPath: String,
      fileName: String,
      withDisableXSFT: Seq[String]): Unit = {
    val (allProfileData, cacheNames) = mergeTraces(files)
    val outputPath = outputConfigFilePath(outputDir, artifactsPath, fileName, optconfSuffix)
    val modes: Seq[PGOMode] = getPgoModes(fileName, withDisableXSFT)
    GridProfilerDefaults.configurePGOModes(modes, outputPath) // [SEE_CONFIGURE_PGO]
    val settings = ConfigWriterSettings(modes)
    Profiler.autoGenerateConfigWithCacheNames(outputPath, allProfileData, cacheNames, settings, isPgoGen = true)
    integrityCheck(Paths get outputPath)
    if (cmdLine.dumpCSV) dumpCombinedCSV(allProfileData, cmdLine.outputDir, cmdLine.fileName)
  }

  private def getPgoModes(
      groupName: String,
      withDisableXSFT: Seq[String],
      defaultMode: Seq[PGOMode] = Seq.empty): Seq[PGOMode] = {
    log.info(s"groups that use disableXSFT + disableCache: $groupName, ${withDisableXSFT.mkString(",")} ")
    // allow to configure per group pgo modes in MergeTraces
    if (withDisableXSFT.contains(groupName)) {
      log.info(s"Using disableCache,disableXSFT pgo modes for $groupName")
      Seq(DisableCache, DisableXSFT)
    } else {
      if (defaultMode eq Seq.empty)
        cmdLine.pgoModes.map(PGOMode.fromName)
      else
        defaultMode
    }
  }

  def mergeTraces(
      rootDir: String,
      outputDir: String,
      artifactsPath: String,
      fileName: String,
      profilerDirectoryName: String,
      cmdLineGroups: Seq[Group] = Group.defaultGroups,
      withDisableXSFT: Seq[String] = Seq.empty): Unit = {
    val groups = if (cmdLineGroups.contains(Group.default)) cmdLineGroups else Group.default +: cmdLineGroups
    val testNamesToPaths =
      findGroupedFiles(rootDir, ogtraceExtension, profilerDirectoryName, moduleInGroup = moduleIncluded(groups))
    groups foreach { group =>
      val name = group.groupName
      val files =
        if (group == Group.default) testNamesToPaths.paths.toSeq
        else
          group.moduleNames.flatMap { moduleName =>
            testNamesToPaths.getByModuleOrElse(moduleName, Nil)
          }

      // if we have groups other than the default one, use the group name provided. Otherwise, output optconf to the
      // filename provided on command line args
      val fileNameForOutput = if (group == Group.default) fileName else name

      log.info(s"Merging traces in group $name from files:\n${files.mkString("\n")}")
      mergeTracesFromFiles(files, outputDir, artifactsPath, fileNameForOutput, withDisableXSFT)
    }
  }

  def mergeTracesConfigPreview(files: Seq[String]): (String, Seq[PNodeTaskInfo]) = {
    log.info(s"Merging traces in from files:\n${files.mkString("\n")}")
    val (allProfileData, cacheNames) = mergeTraces(files)
    val config = Profiler.autoGenerateConfigWithCacheNames(
      allProfileData,
      cacheNames,
      ConfigWriterSettings.default,
      isPgoGen = true)
    (config, allProfileData)
  }

  final def convert(s: String): Seq[String] = s.split(",")

  def mergeJobTraces(
      rootDir: String,
      artifactsPath: String,
      groupNames: Seq[String],
      withDisableXSFT: Seq[String],
      profilerDirectoryName: String = "profiler"): Seq[(String, String, Seq[PNodeTaskInfo])] = {
    val groups = if (groupNames equals Seq.empty) Group.defaultGroups else getGroups(groupNames)

    log.info(s"Searching in root directory $rootDir")
    val testNamesToPaths =
      findGroupedFiles(rootDir, ogtraceExtension, profilerDirectoryName, moduleInGroup = moduleIncluded(groups))
    groups.map(group => {
      val name = group.groupName
      val files =
        if (group == Group.default) testNamesToPaths.paths.toSeq
        else
          group.moduleNames.flatMap { moduleName =>
            testNamesToPaths.getByModuleOrElse(moduleName, Nil)
          }

      log.info(s"Merging traces in group $name from files:\n${files.mkString("\n")}")
      mergeTracesFromFiles(files, name, withDisableXSFT)
    })
  }

  private def mergeTracesFromFiles(
      files: Seq[String],
      fileName: String,
      withDisableXSFT: Seq[String]): (String, String, Seq[PNodeTaskInfo]) = {
    val (allProfileData, cacheNames) = MergeTraces.mergeTraces(files)
    val modes: Seq[PGOMode] = getPgoModes(fileName, withDisableXSFT, Seq(DisableCache))
    val settings = ConfigWriterSettings(modes)
    val config = Profiler.autoGenerateConfigWithCacheNames(allProfileData, cacheNames, settings, isPgoGen = true)
    (fileName, config, allProfileData)
  }

  private def getGroups(groupNames: Seq[String]): Seq[Group] =
    groupNames.map(name => Group(name, PGOModuleMap.get(name).get))

  mergeTraces(
    cmdLine.rootDir,
    cmdLine.outputDir,
    cmdLine.artifactsPath,
    cmdLine.fileName,
    cmdLine.profilerDirectoryName,
    groups(cmdLine.groups, cmdLine.groupNames),
    cmdLine.withDisableXSFT
  )
}
