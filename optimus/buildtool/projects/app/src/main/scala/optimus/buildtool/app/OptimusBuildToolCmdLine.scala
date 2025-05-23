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
package optimus.buildtool
package app

import optimus.buildtool.app.OptimusBuildToolCmdLineT.NoneArg
import optimus.buildtool.builders.postinstallers.uploaders.AssetUploader.UploadFormat
import optimus.buildtool.cache.CacheMode
import optimus.buildtool.compilers.zinc.ZincIncrementalMode
import optimus.buildtool.config.AfsNamingConventions
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.utils.TypeClasses._
import optimus.buildtool.utils.Utils
import optimus.platform._
import optimus.utils.app.StringOptionOptionHandler
import optimus.rest.bitbucket.MergeCommitUtils
import optimus.utils.app.DelimitedStringOptionHandler
import optimus.utils.app.PositiveIntOptionOptionHandler
import optimus.utils.MemSize
import optimus.utils.MemUnit
import optimus.utils.app.MemSizeOptionHandler
import org.kohsuke.{args4j => args}

import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.{util => ju}
import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._

object OptimusBuildToolCmdLineT {
  val NoneArg = "none"
  val AllArg = "all"
}

private[buildtool] trait InstallDirCmdLine {
  @args.Option(
    name = "--installDir",
    required = false,
    aliases = Array("--installPath"),
    usage = "Install directory (defaults to <workspace root>/install)"
  )
  val installDir: String = ""

  lazy val onAfs: Boolean = installDir.startsWith(AfsNamingConventions.AfsDistStr) ||
    installDir.startsWith(AfsNamingConventions.AfsDistStr drop 1) // drop the leading '/', just in case

  @args.Option(name = "--installVersion", required = false, usage = "Install version (defaults to local)")
  val installVersion: String = "local"

}

/** Arguments about the layout of a workspace, shared between various buildtooly apps. */
private[buildtool] trait WorkspaceCmdLine extends InstallDirCmdLine {
  @args.Option(
    name = "--sourceDir",
    required = false,
    aliases = Array("--sourcePath"),
    usage = "Source directory (defaults to the current directory)"
  )
  protected val sourceDir: String = ""

  @args.Option(
    name = "--workspaceDir",
    required = false,
    aliases = Array("--workspace", "--workspaceRoot"),
    usage = "Workspace root (defaults to the parent of the source directory)"
  )
  protected val workspaceDir: String = ""

  @args.Option(
    name = "--depCopyDir",
    required = false,
    aliases = Array("--depcopyDir", "--depCopyPath"),
    usage = "Dep copy root (defaults to <workspace root>/../.stratosphere/depcopy"
  )
  val depCopyDir: String = ""

  @args.Option(name = "--scalaDir", required = false, aliases = Array("--scalaPath"), usage = "Scala directory")
  val scalaDir: String = ""

  @args.Option(
    name = "--outputDir",
    required = false,
    aliases = Array("--outputPath", "--buildDir"),
    usage = "Output directory (defaults to <workspace root>/build_obt)"
  )
  val outputDir: String = ""

  @args.Option(
    name = "--reportDir",
    required = false,
    aliases = Array("--buildReportDir"),
    usage = "Build report output directory (defaults to <workspace root>/build_obt/.build-report)"
  )
  val buildReportDir: String = ""

  @args.Option(
    name = "--checkMapping",
    required = false,
    usage = "Validate jvm dependencies mapping rules (defaults to true)"
  )
  val enableMappingValidation: Boolean = true

  @args.Option(
    name = "--useMavenLibs",
    required = false,
    usage = "Decide to use mavenLibs of external dependencies or not (defaults to false)"
  )
  val useMavenLibs: Boolean = false

  @args.Option(
    name = "--generatePoms",
    required = false,
    usage = "Generates .pom files for maven libs."
  )
  val generatePoms: Boolean = false

  @args.Option(
    name = "--artifactVersion",
    required = false,
    aliases = Array("--obtVersion"),
    usage = "Artifact version (defaults to version defined in code)"
  )
  val artifactVersion: String = OptimusBuildTool.DefaultArtifactVersionNumber

  @args.Option(
    name = "--artifactVersionSuffix",
    required = false,
    aliases = Array("--obtVersionSuffix"),
    usage = "Suffix to append to artifact version (defaults to none)"
  )
  val artifactVersionSuffix: String = ""

  @args.Option(
    name = "--locatorSuffix",
    required = false,
    handler = classOf[StringOptionOptionHandler],
    usage = "Suffix to append to artifact locators (defaults to none)"
  )
  val locatorSuffix: Option[String] = None

  @args.Option(
    name = "--logDir",
    required = false,
    usage = "Directory to which to write logs (defaults to workspace/logs/obt)"
  )
  private val _logDir: String = ""

  // Non-@Option vals need to be lazy so that they pick up the fully initialized state of this class
  lazy val (workspaceRoot, workspaceSourceRoot): (Directory, Directory) =
    Utils.resolveWorkspaceAndSrcRoots(workspaceDir.asDirectory, sourceDir.asDirectory)
  lazy val buildDir: Directory = outputDir.asDirectory.getOrElse(workspaceRoot.resolveDir("build_obt"))
  lazy val reportDir: Directory = buildReportDir.asDirectory.getOrElse(buildDir.resolveDir(".build-report"))
  lazy val errorsDir: Directory = reportDir.resolveDir("compilation-errors")

  // Non-@Option vals need to be lazy so that they pick up the fully initialized state of this class
  lazy val logDir: Path =
    if (_logDir.nonEmpty) Paths.get(_logDir)
    else workspaceRoot.resolveDir("logs/obt").path

  @args.Option(
    name = "--dockerDir",
    required = false,
    usage = "Output directory for docker (defaults to <workspace root>/docker-out)"
  )
  val dockerDir: String = ""

  @args.Option(
    name = "--sandboxDir",
    required = false,
    usage = "Temporary directory for sandboxes (defaults to <output dir>/sandboxes)"
  )
  val sandboxDir: String = ""
}

private[buildtool] trait GitCmdLine { this: OptimusAppCmdLine =>
  @args.Option(name = "--useGit", required = false, usage = "Use git (defaults to false)")
  val useGit = false

  @args.Option(
    name = "--gitLength",
    required = false,
    usage = "Length of commit history for git matching (defaults to 30)"
  )
  val gitLength = 30

  @args.Option(
    name = "--gitFilterRe",
    required = false,
    usage = "Regex for matching git merge commit messages"
  )
  val gitFilterRe: String = MergeCommitUtils.stagingPrMessagePattern.toString

  @args.Option(
    name = "--gitAwareMessages",
    required = false,
    usage = "Filter compiler messages based on git history (defaults to true)"
  )
  val gitAwareMessages = true

  @args.Option(name = "--allowSparse", required = false, usage = "Allow sparse workspaces (defaults to false)")
  val allowSparse = false
}

private[buildtool] trait RemoteStoreCmdLine { this: OptimusAppCmdLine =>

  @args.Option(
    name = "--dhtRemoteStore",
    required = false,
    usage = "DHT Location (defaults to DHT disabled)"
  )
  val dhtRemoteStore: String = NoneArg

  @args.Option(
    name = "--remoteCacheMode",
    required = false,
    usage = "Remote cache mode. Available modes: readWrite, readOnly, writeOnly, forceWrite, forceWriteOnly"
  )
  val remoteCacheMode: String = NoneArg

  @args.Option(
    name = "--remoteCacheWritable",
    required = false,
    usage = "Write artifacts to remote cache (defaults to false)"
  )
  val remoteCacheWritable: Boolean = false

  @args.Option(
    name = "--remoteCacheForceWrite",
    required = false,
    depends = Array("--remoteCacheWritable"),
    usage = "Force writing of artifacts to remote cache even on cache hits (defaults to false)"
  )
  val remoteCacheForceWrite: Boolean = false

  lazy val cacheMode: CacheMode =
    if (remoteCacheMode == NoneArg) {
      if (remoteCacheForceWrite) CacheMode.ForceWrite
      else if (remoteCacheWritable) CacheMode.ReadWrite
      else CacheMode.ReadOnly
    } else if (remoteCacheMode == "readWrite") CacheMode.ReadWrite
    else if (remoteCacheMode == "readOnly") CacheMode.ReadOnly
    else if (remoteCacheMode == "writeOnly") CacheMode.WriteOnly
    else if (remoteCacheMode == "forceWrite") CacheMode.ForceWrite
    else if (remoteCacheMode == "forceWriteOnly") CacheMode.ForceWriteOnly
    else throw new IllegalArgumentException(s"Unrecognized cache mode: $remoteCacheMode")

  @args.Option(
    name = "--crossRegionReadThroughDHTLocations",
    required = false,
    handler = classOf[DelimitedStringOptionHandler],
    usage =
      "DHT locations (comma delimited) for cross-region read-through after write to current region (defaults to none)"
  )
  val crossRegionReadThroughDHTLocations: Seq[String] = Seq.empty

  @args.Option(
    name = "--crossRegionDHTSizeThreshold",
    handler = classOf[MemSizeOptionHandler],
    required = false,
    usage =
      "Threshold for cross-region read-through after write to current region (defaults to 50 MB). Artifacts larger than threshold value will be read through DHT from the remote DHTs."
  )
  val crossRegionDHTSizeThreshold: MemSize = MemSize.of(50, MemUnit.MB)
}

private[buildtool] class OptimusBuildToolCmdLine extends OptimusBuildToolCmdLineT
private[buildtool] trait OptimusBuildToolCmdLineT
    extends OptimusAppCmdLine
    with GitCmdLine
    with RemoteStoreCmdLine
    with WorkspaceCmdLine {
  import OptimusBuildToolCmdLineT.NoneArg

  @args.Option(name = "--debug", required = false, usage = "Increase logging verbosity (defaults to false)")
  val debug: Boolean = false

  @args.Option(
    name = "--histoFreqSecs",
    required = false,
    handler = classOf[PositiveIntOptionOptionHandler],
    usage = "Dump periodic heap histograms"
  )
  val histoFreqSecs: Option[Int] = None

  @args.Option(
    name = "--scopesToBuild",
    required = false,
    aliases = Array("--modulesToBuild"),
    usage = "Scopes to build"
  )
  private val scopesToBuildOpt: String = ""

  @args.Option(
    name = "--scopesToExclude",
    required = false,
    aliases = Array("--exclude"),
    usage = "Scopes to exclude from the build"
  )
  private val _scopesToExclude: String = ""
  lazy val scopesToExclude: Set[String] = _scopesToExclude.split(",").toSet.filter(_.nonEmpty)

  @args.Option(
    name = "--scopeFilter",
    required = false,
    usage = "Scopes to include by attribute (eg. 'cpp')"
  )
  val scopeFilter: String = "none"

  @args.Option(
    name = "--buildModifiedScopes",
    required = false,
    usage = "Include scopes with git changes when building (defaults to false)"
  )
  val buildModifiedScopes: Boolean = false

  @args.Option(
    name = "--buildDownstreamScopes",
    required = false,
    usage = "Include downstream scopes when building (defaults to false)"
  )
  val buildDownstreamScopes: Boolean = false

  @args.Option(
    name = "--traceDir",
    aliases = Array("--traceFile"),
    required = false,
    usage = "Trace file directory (defaults to not writing trace files)"
  )
  val traceFile: String = ""

  @args.Option(
    name = "--statsDir",
    required = false,
    usage = "OBT stats directory (defaults to not writing OBT stats)"
  )
  val statsDir: String = ""

  @args.Option(
    name = "--cacheClassloaders",
    required = false,
    aliases = Array("--cacheclassloaders"),
    usage = "Cache classloaders (defaults to true)"
  )
  val cacheClassloaders: Boolean = true

  @args.Option(
    name = "--interactive",
    required = false,
    aliases = Array("-i"),
    usage = "Interactive mode (defaults to false)"
  )
  val interactive: Boolean = false

  @args.Option(
    name = "--rebuild",
    required = false,
    usage = "Rebuild without incremental artifacts (defaults to false)"
  )
  private val rebuild: Boolean = false

  @args.Option(
    name = "--bspServer",
    required = false,
    aliases = Array("--bsp", "--bspserver"),
    usage = "BSP server mode (defaults to false)"
  )
  val bspServer: Boolean = false

  @args.Option(
    name = "--bspDebug",
    required = false,
    depends = Array("--bspServer"),
    usage = "Increase BSP logging verbosity (defaults to false)"
  )
  val bspDebug: Boolean = false

  @args.Option(name = "--clean", required = false, usage = "Clean all artifacts before building (defaults to false)")
  val clean: Boolean = false

  @args.Option(
    name = "--install",
    required = false,
    usage = "Write artifacts to the install directory (defaults to false)"
  )
  val install: Boolean = false

  @args.Option(
    name = "--verifyInstall",
    required = false,
    usage = "Verify state of the install directory (defaults to false)"
  )
  val verifyInstall: Boolean = false

  @args.Option(
    name = "--installCpp",
    required = false,
    usage = "Install generated C++ artifacts (defaults to none). Format: [<scope>:<directory][,<scope>:<directory]..."
  )
  val installCpp: String = NoneArg

  @args.Option(
    name = "--installAppScripts",
    required = false,
    usage = "[Deprecated] Write generated application scripts artifacts to the install directory (always true)"
  )
  val installAppScripts: Boolean = true

  @args.Option(
    name = "--copyGeneratedSources",
    required = false,
    aliases = Array("--copyScalaSources"),
    usage = "Copy generated scala sources to the source directory (defaults to false)"
  )
  val copyScalaSources: Boolean = false

  @args.Option(
    name = "--installDocs",
    required = false,
    usage = "Used to install documentations per scope (defaults to false)"
  )
  val installDocs: Boolean = false

  @args.Option(
    name = "--docBundle",
    required = false,
    usage = "Used to configure the metabundle for the docs (defaults to optimus.codetree)"
  )
  val docBundle: String = "optimus.codetree"

  @args.Option(
    name = "--zincNonIncremental",
    required = false,
    usage = "Never incrementally compile using zinc (defaults to false)"
  )
  private val zincNonIncremental: Boolean = false

  @args.Option(
    name = "--zincIncremental",
    required = false,
    usage = "Zinc incremental mode. One of 'none', 'dry-run' or 'full' (defaults to 'full')"
  )
  private val zincIncremental: String = "full"

  @args.Option(
    name = "--zincTrackLookups",
    required = false,
    usage = "Use Zinc to track dependency usage in scala and java compilations."
  )
  val zincTrackLookups: Boolean = false

  @args.Option(name = "--profileScalac", required = false, usage = "Profile scalac (defaults to false)")
  val profileScalac: Boolean = false

  @args.Option(
    name = "--sendLongTermBreadcrumbs",
    required = false,
    usage = "Send summary breadcrumbs to long-term Splunk (for long term performance trend analysis; defaults to false)"
  )
  val sendLongTermBreadcrumbs: Boolean = false

  @args.Option(
    name = "--sendPerScopeBreadcrumbs",
    required = false,
    usage =
      "Send per-scope breadcrumbs to Splunk (this sends a lot of data - don't use it lightly! note that these always go to short-term splunk; defaults to false)"
  )
  val sendPerScopeBreadcrumbs: Boolean = false

  @args.Option(
    name = "--obtBenchmarkScenario",
    required = false,
    usage = "Add benchmark breadcrumbs label to Splunk"
  )
  val obtBenchmarkScenario: String = NoneArg

  // Backward-compatibility
  @args.Option(
    name = "--cppOsVersion",
    required = false,
    usage = "Use a custom OS version for native artifact cache lookups (deprecated)"
  )
  val cppOsVersion: String = NoneArg

  @args.Option(
    name = "--cppOsVersions",
    required = false,
    handler = classOf[DelimitedStringOptionHandler],
    usage = "OS versions for native artifact cache lookups"
  )
  val cppOsVersions: Seq[String] = Nil

  @args.Option(
    name = "--requiredCppOsVersions",
    required = false,
    handler = classOf[DelimitedStringOptionHandler],
    usage = "OS versions for which to require C++ artifacts"
  )
  val requiredCppOsVersions: Seq[String] = Nil

  @args.Option(
    name = "--cppFallback",
    required = false,
    usage = "Fallback to disted C++ artifacts, eg. for running on grid from Windows (defaults to false)"
  )
  val cppFallback: Boolean = false

  @args.Option(
    name = "--bundleClassJars",
    required = false,
    usage = "Create a single class jar per metabundle (defaults to false)"
  )
  val bundleClassJars: Boolean = false

  @args.Option(
    name = "--uploadLocations",
    required = false,
    aliases = Array("--uploadTo"),
    handler = classOf[DelimitedStringOptionHandler],
    usage = "A comma separated list of remote and ssh locations where to upload the installed artifacts"
  )
  val uploadLocations: Seq[String] = Seq.empty

  @args.Option(
    name = "--maxConcurrentUploads",
    required = false,
    usage = "The maximum number of uploads to run at the same time (defaults to 6)"
  )
  val maxConcurrentUploads: Int = 6

  @args.Option(
    name = "--minUploadBatchSize",
    required = false,
    usage = "The minimum number of files that we upload in batches (defaults to 500)")
  val minUploadBatchSize: Int = 500

  @args.Option(
    name = "--maxUploadBatchSize",
    required = false,
    usage = "The maximum number of files that we upload in batches (defaults to 1000)")
  val maxUploadBatchSize: Int = 1000

  @args.Option(
    name = "--maxUploadBatchBytes",
    required = false,
    usage = "The maximum size of the batch file that we upload (defaults to 100,000,000 bytes = 100 MB)")
  val maxUploadBatchBytes: Long = 1e+8.toLong

  @args.Option(
    name = "--maxUploadRetry",
    required = false,
    usage = "The number of maximum retries for a file to upload before giving up (defaults to 10)")
  val maxUploadRetry: Int = 10

  @args.Option(
    name = "--uploadSources",
    required = false,
    usage = "Upload source files as well as artifacts"
  )
  val uploadSources: Boolean = true

  @args.Option(
    name = "--uploadSourcePrefix",
    required = false,
    usage = "Relative path for sources"
  )
  val uploadSourcePrefix: String = NoneArg

  @args.Option(
    name = "--uploadFormat",
    required = false,
    usage = "Upload format: raw, tar or zip (default is tar)"
  )
  private val _uploadFormat: String = "tar"
  lazy val uploadFormat: UploadFormat = UploadFormat(_uploadFormat)

  @args.Option(
    name = "--decompressAfterUpload",
    required = false,
    usage = "Decompress uploaded zip files"
  )
  val decompressAfterUpload: Boolean = true

  @args.Option(
    name = "--toolsDir",
    required = false,
    handler = classOf[StringOptionOptionHandler],
    usage = "Location of standard UNIX tools (eg. rsync, tar)"
  )
  val toolsDir: Option[String] = None

  @args.Option(
    name = "--images",
    required = false,
    aliases = Array("--imagesToBuild"),
    usage = "Create an OCI/Docker image from the provided scopes at the provided locations"
  )
  private val _imagesToBuild: String = ""
  lazy val imagesToBuild: Set[String] = _imagesToBuild.split(",").toSet.filter(_.nonEmpty)

  @args.Option(
    name = "--imageTag",
    required = false,
    usage = "Tag for docker image to be built",
    depends = Array("--images")
  )
  val imageTag: String = "latest"

  @args.Option(
    name = "--strictEmptySources",
    required = false,
    usage = "Fails the build if non empty scopes have flag isEmpty = true and vice versa (defaults to true)"
  )
  val strictEmptySources: Boolean = true

  private def zincIncrementalMode: ZincIncrementalMode =
    if (zincNonIncremental) ZincIncrementalMode.DryRun
    else
      zincIncremental.toLowerCase match {
        case NoneArg   => ZincIncrementalMode.None
        case "dry-run" => ZincIncrementalMode.DryRun
        case "full"    => ZincIncrementalMode.Full
      }

  // Non-@Option vals need to be lazy so that they pick up the fully initialized state of this class
  lazy val incrementalMode =
    if (rebuild) IncrementalMode(ZincIncrementalMode.None, defaultUseIncrementalArtifacts = false)
    else IncrementalMode(zincIncrementalMode, defaultUseIncrementalArtifacts = true)

  @args.Option(name = "--zincAnalysisCacheSize", required = false, usage = "Zinc analysis cache size (defaults to 50)")
  val zincAnalysisCacheSize = 50

  @args.Option(
    name = "--zincIgnorePluginHash",
    required = false,
    usage = "Ignore plugin hash for zinc (defaults to false)"
  )
  val zincIgnorePluginHash = false

  @args.Option(
    name = "--zincIgnoreJarChangesRegexp",
    required = false,
    usage = "Path regex for ignoring jar changes (defaults to 'jre/lib/rt.jar$')"
  )
  val zincIgnoreChangesRegexp = "jre/lib/rt.jar$"

  @args.Option(
    name = "--zincRecompileAllFraction",
    required = false,
    usage = "Fraction of invalidations which will cause zinc to recompile from scratch (defaults to 0.5)"
  )
  val zincRecompileAllFraction = 0.5

  // mainly for integration testing
  @args.Option(
    name = "--zincInterfaceDir",
    required = false,
    usage = "Zinc interface dir (defaults to <build dir>/zincCompilerInterface)"
  )
  val zincInterfaceDir: String = ""

  @args.Option(
    name = "--zincStrictMapping",
    required = false,
    usage = "Abort compilation if we detect //tmp in data produced by the zinc write mapper"
  )
  val zincStrictMapping: Boolean = false

  @args.Option(
    name = "--maxNumZincs",
    required = false,
    usage =
      "Deprecated: Maximum number of concurrently running Zinc compilers or 0 if unlimited (defaults to unlimited)."
  )
  val maxNumZincs: Int = 0

  @args.Option(
    name = "--maxZincCompileBytes",
    required = false,
    usage =
      "If positive, maximum size of source code to compile concurrently using Zinc compilers; if negative, maxHeap/-maxZincCompileBytes or 0 if unlimited (defaults to unlimited)."
  )
  val maxZincCompileBytes: Int = 0

  @args.Option(
    name = "--backgroundCmd",
    required = false,
    usage = "Command to run in background during build (defaults to none)")
  val backgroundCmd: String = NoneArg

  @args.Option(
    name = "--bspClientInitializationTimeoutMs",
    required = false,
    usage = "BSP client initialization timeout in ms (defaults to 120,000)"
  )
  val bspClientInitializationTimeoutMs: Int = 120 * 1000

  @args.Option(
    name = "--useNioFileWatcher",
    required = false,
    aliases = Array("-n"),
    usage = "Use NIO file watcher to track directory changes (defaults to false)"
  )
  val useNioFileWatcher = false

  @args.Option(
    name = "--statusIntervalSec",
    required = false,
    usage = "Status logging interval in s (defaults to 0, ie. logging disabled)"
  )
  val statusIntervalSec = 0

  @args.Option(
    name = "--memConfig",
    required = false,
    usage = "Memory throttling config of the form 'freeMemGbDelay,freeMemGbGC,delaySec,maxDelays' " +
      "(defaults to value defined in 'optimus.buildtool.memconfig', typically '3,3,10,100')"
  )
  val memConfig: String = ""

  @args.Option(
    name = "--breadcrumbs",
    required = false,
    usage = "Write breadcrumbs with build information (defaults to true)"
  )
  val breadcrumbs = true

  @args.Option(
    name = "--mischief",
    required = false,
    usage = "[Deprecated] Read mischief.obt for additional \"mischievous\" effects. (Now always true.)"
  )
  val _mischief = false

  @args.Option(
    name = "--maxBuildDirSize",
    required = false,
    usage = "Delete files until the workspace is no larger than provided size (MB)"
  )
  private val _maxBuildDirSize: Int = Int.MaxValue
  def maxBuildDirSize: Option[Int] = Some(_maxBuildDirSize) filter (_ > 0)

  @args.Option(
    name = "--freeDiskSpaceTrigger",
    required = false,
    usage = "Run disk space cleanup when the free disk space is less than the provided size (MB)"
  )
  private val _freeDiskSpaceTriggerMb: Int = -1
  def freeDiskSpaceTriggerMb: Option[Int] = Some(_freeDiskSpaceTriggerMb) filter (_ > 0)

  @args.Option(name = "--gitPinDepth", required = false, depends = Array("--maxBuildDirSize"), usage = "Git pin depth")
  val gitPinDepth = 8

  @args.Option(name = "--maxLogAge", required = false, usage = "Delete logs older than specified number of days")
  private val _maxLogAge: Int = 7 // days
  def maxLogAge: Duration = Duration ofDays _maxLogAge

  @args.Option(
    name = "--configParam",
    required = false,
    // n.b. "-P" is the convention from Gradle
    aliases = Array("-P"),
    usage = "Pass config parameters in the format attribute=value into the .obt config"
  )
  val configParams: ju.List[String] = new ju.ArrayList[String]()

  @args.Option(
    name = "--warningsReport",
    required = false,
    usage = "Generates CSV files that report on warnings and optimus alerts"
  )
  val warningsReport: Boolean = false

  @args.Option(
    name = "--credentialFiles",
    required = false,
    usage = "A comma separated list of SBT and/or JFrog format credentials files to load"
  )
  val credentialFiles: String = NoneArg

  @args.Option(
    name = "--codeReviewAnalysis",
    required = false,
    depends = Array("--prCommit"),
    aliases = Array("--overlordAnalysis"), // TODO (OPTIMUS-56087) rename Overlord references
    usage = "It generates a JSON file containing compiler messages for code review tools to process (defaults to false)"
  )
  val codeReviewAnalysis: Boolean = false

  @args.Option(
    name = "--prCommit",
    required = false,
    usage =
      "The latest commit from the current branch. It is needed in the code review analysis to detect what files changed"
  )
  val prCommit: String = "unknown"

  @args.Option(
    name = "--targetCommit",
    required = false,
    usage = "The commit from the target branch. It is needed in the code review analysis to detect what files changed")
  val targetCommit: String = "PLACEHOLDER_TARGET_COMMIT"

  @args.Option(
    name = "--generateMetadata",
    required = false,
    depends = Array("--buildId"),
    // TODO (OPTIMUS-56086) rename Train references
    aliases = Array("--generateTrainMetadata"),
    usage = "Generates metadata reports for each bundle"
  )
  val generateMetadata: Boolean = false

  @args.Option(
    name = "--generateTestplans",
    required = false,
    depends = Array("--install"),
    usage = "Generates testplans and testmodules files"
  )
  val generateTestplans: Boolean = false

  @args.Option(name = "--buildId", required = false, usage = "It identifies the CI build")
  val buildId: String = "unknown"

  @args.Option(
    name = "--wars",
    required = false,
    usage = "Scopes for which to build .war files"
  )
  private val _warScopes: String = ""
  // Non-@Option vals need to be lazy so that they pick up the fully initialized state of this class
  lazy val warScopes: Set[String] = _warScopes.split(',').toSet.filter(_.nonEmpty)

  @args.Option(
    name = "--postInstallApps",
    required = false,
    depends = Array("--install"),
    usage = "Runs post install apps as soon as artifacts are available during installation"
  )
  val postInstallApps: Boolean = false

  @args.Argument(usage = "Scopes to build")
  private val scopesToBuildArg: ju.List[String] = new ju.ArrayList[String]()

  // Non-@Option vals need to be lazy so that they pick up the fully initialized state of this class
  lazy val scopesToBuild: Set[String] =
    (scopesToBuildOpt +: scopesToBuildArg.asScala).flatMap(_.split(',')).toSet.filter(_.nonEmpty)

  @args.Option(
    name = "--stripDependencies",
    required = false,
    usage = "Strips debug symbols from dependencies when building a docker image (defaults to false)"
  )
  val stripDependencies: Boolean = false

  @args.Option(
    name = "--failOnAnomalousTrace",
    required = false,
    usage = "Fails the build if anomalies were detected in the build trace (e.g. duplicate or unfinished tasks)"
  )
  val failOnAnomalousTrace = false

  @args.Option(
    name = "--gitTag",
    required = false,
    handler = classOf[StringOptionOptionHandler],
    usage = "Adds a git tag to HEAD if all writes to the global cache are successful. Only forward tagging is allowed"
  )
  val gitTag: Option[String] = None

  @args.Option(
    name = "--minimal",
    required = false,
    depends = Array("--install"),
    usage =
      "Install app scripts and pathing jars only for requested scopes, excluding their transitive scope dependencies"
  )
  val minimalInstall: Boolean = false

  @args.Option(
    name = "--pypiCredentials",
    required = false,
    usage = "Location of the cached pip.ini credential file to load, used by pip to connect to Pypi Artifactory",
    handler = classOf[StringOptionOptionHandler]
  )
  private val pypiCredentials: Option[String] = None
  lazy val pypiCredentialsFile: Option[FileAsset] = pypiCredentials.map(f => FileAsset(Paths.get(f)))
  @args.Option(
    name = "--pypiUvCredentials",
    required = false,
    usage = "Location of the cached uv.toml credential file to load, used by uv to connect to Pypi Artifactory",
    handler = classOf[StringOptionOptionHandler]
  )
  private val pypiUvCredentials: Option[String] = None
  lazy val pypiUvCredentialsFile: Option[FileAsset] = pypiUvCredentials.map(f => FileAsset(Paths.get(f)))
}
