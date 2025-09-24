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

import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.CrumbHint
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.buildtool.app.OptimusBuildToolCmdLineT._
import optimus.buildtool.artifacts._
import optimus.buildtool.bsp.BuildServerProtocolServer
import optimus.buildtool.bsp.BuildServerProtocolService.PythonBspConfig
import optimus.buildtool.builders._
import optimus.buildtool.builders.postbuilders.DocumentationInstaller
import optimus.buildtool.builders.postbuilders.PostBuilder
import optimus.buildtool.builders.postbuilders.codereview.CodeReviewSettings
import optimus.buildtool.builders.postbuilders.installer.BundleFingerprintsCache
import optimus.buildtool.builders.postbuilders.installer.Installer
import optimus.buildtool.builders.postbuilders.installer.component.DockerGenericRunnerGenerator
import optimus.buildtool.builders.postbuilders.installer.component.InstallTpa
import optimus.buildtool.builders.postbuilders.installer.component.InstallTpaWithWheels
import optimus.buildtool.builders.postbuilders.metadata.MetadataSettings
import optimus.buildtool.builders.postbuilders.sourcesync.CppSourceSync
import optimus.buildtool.builders.postbuilders.sourcesync.GeneratedScalaSourceSync
import optimus.buildtool.builders.postinstallers.PostInstaller
import optimus.buildtool.builders.postinstallers.apprunner.PostInstallAppRunner
import optimus.buildtool.builders.postinstallers.uploaders.AssetUploader
import optimus.buildtool.builders.postinstallers.uploaders.UploadLocation
import optimus.buildtool.builders.reporter.ErrorReporter
import optimus.buildtool.builders.reporter.MessageReporter
import optimus.buildtool.cache.ArtifactCache
import optimus.buildtool.cache.FilesystemCache
import optimus.buildtool.cache.FreezerCache
import optimus.buildtool.cache.FreezerStoreConf
import optimus.buildtool.cache.HasArtifactStore
import optimus.buildtool.cache.MultiLevelCache
import optimus.buildtool.cache.MultiLevelStore
import optimus.buildtool.cache.NoOpRemoteAssetStore
import optimus.buildtool.cache.NodeCaching
import optimus.buildtool.cache.RemoteAssetStore
import optimus.buildtool.cache.remote.RemoteCacheProvider
import optimus.buildtool.compilers.AsyncCppCompilerImpl
import optimus.buildtool.compilers.AsyncElectronCompilerImpl
import optimus.buildtool.compilers.AsyncJavaCompiler
import optimus.buildtool.compilers.AsyncJmhCompilerImpl
import optimus.buildtool.compilers.AsyncPythonCompilerImpl
import optimus.buildtool.compilers.AsyncRunConfCompilerImpl
import optimus.buildtool.compilers.AsyncScalaCompiler
import optimus.buildtool.compilers.AsyncWebCompilerImpl
import optimus.buildtool.compilers.GenericFilesPackager
import optimus.buildtool.compilers.JarPackager
import optimus.buildtool.compilers.ManifestGenerator
import optimus.buildtool.compilers.cpp.CppCompilerFactory
import optimus.buildtool.compilers.cpp.CppCompilerFactoryImpl
import optimus.buildtool.compilers.runconfc.Templates
import optimus.buildtool.compilers.venv.PythonEnvironment
import optimus.buildtool.compilers.zinc.AnalysisLocatorImpl
import optimus.buildtool.compilers.zinc.CompilerThrottle
import optimus.buildtool.compilers.zinc.ScalacProvider
import optimus.buildtool.compilers.zinc.ZincAnalysisCache
import optimus.buildtool.compilers.zinc.ZincClassLoaderCaches
import optimus.buildtool.compilers.zinc.ZincCompilerFactory
import optimus.buildtool.config.ExternalDependencies
import optimus.buildtool.config.GlobalConfig
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.ModuleSet
import optimus.buildtool.config.NamingConventions._
import optimus.buildtool.config.ObtConfig
import optimus.buildtool.config.RunConfConfiguration
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.StaticConfig
import optimus.buildtool.config.StratoConfig
import optimus.buildtool.config.VersionConfiguration
import optimus.buildtool.files.Directory.Not
import optimus.buildtool.files._
import optimus.buildtool.generators.CppBridgeGenerator
import optimus.buildtool.generators.FixGenerator
import optimus.buildtool.generators.FlatbufferGenerator
import optimus.buildtool.generators.JaxbGenerator
import optimus.buildtool.generators.JsonSchemaGenerator
import optimus.buildtool.generators.JxbGenerator
import optimus.buildtool.generators.ProtobufGenerator
import optimus.buildtool.generators.AvroSchemaGenerator
import optimus.buildtool.generators.PodcastGenerator
import optimus.buildtool.generators.DomainGenerator
import optimus.buildtool.generators.ScalaxbGenerator
import optimus.buildtool.processors.DeploymentScriptProcessor
import optimus.buildtool.processors.FreemarkerProcessor
import optimus.buildtool.processors.OpenApiProcessor
import optimus.buildtool.processors.VelocityProcessor
import optimus.buildtool.resolvers.CoursierArtifactResolverSource
import optimus.buildtool.resolvers.DependencyCopier
import optimus.buildtool.resolvers.DependencyMetadataResolvers
import optimus.buildtool.resolvers.WebDependencyResolver
import optimus.buildtool.resolvers.WebScopeInfo
import optimus.buildtool.rubbish.ArtifactRecency
import optimus.buildtool.rubbish.RubbishTidyer
import optimus.buildtool.rubbish.RubbishTidyerImpl
import optimus.buildtool.scope.CompilationNode
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.FingerprintHasher
import optimus.buildtool.scope.ScopedCompilationImpl
import optimus.buildtool.trace._
import optimus.buildtool.utils.AsyncUtils.asyncTry
import optimus.buildtool.utils.TypeClasses._
import optimus.buildtool.utils._
import optimus.graph.Settings
import optimus.graph.tracking.DependencyTrackerRoot
import optimus.platform._
import optimus.rest.bitbucket.MergeCommitUtils
import optimus.stratosphere.artifactory.ArtifactoryToolInstaller
import optimus.stratosphere.artifactory.Credential
import optimus.stratosphere.config.StratoWorkspace

import java.io.InputStream
import java.io.OutputStream
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Paths
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import scala.collection.compat._
import scala.io.StdIn
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

object OptimusBuildToolImpl {
  private val ScopeIdDir = "([^:]*):(.*)".r

  // the scopes get requested many times throughout the build, so do our best to cache them
  OptimusBuildToolImpl.ScopedCompilationFactoryImpl_lookupScope.setCustomCache(NodeCaching.optimizerCache)
}

@entity private[buildtool] class OptimusBuildToolImpl(
    cmdLine: OptimusBuildToolCmdLineT,
    instrumentation: BuildInstrumentation,
    errorReporter: Option[ErrorReporter] = None
) {
  private val requireCredentials = OptimusBuildToolProperties.getOrFalse("private.requireCredentials")
  // do this first to avoid mistakes (failing to write to logs is unhelpful)
  Files.createDirectories(cmdLine.logDir)
  private val logDir: Directory = Directory(cmdLine.logDir)
  private val obtErrorReporter = errorReporter.getOrElse(new ErrorReporter(cmdLine.errorsDir))

  // parameters for maven
  private val useMavenLibs = cmdLine.useMavenLibs
  private val generatePoms = cmdLine.generatePoms
  private val enableMappingValidation = cmdLine.enableMappingValidation

  private val useCrumbs = cmdLine.breadcrumbs
  private val version: String =
    if (cmdLine.artifactVersionSuffix.isEmpty) cmdLine.artifactVersion
    else s"${cmdLine.artifactVersion}-${cmdLine.artifactVersionSuffix}"
  private val implId = UUID.randomUUID()
  private val workspaceRoot: Directory = cmdLine.workspaceRoot
  private val workspaceSourceRoot: WorkspaceSourceRoot = WorkspaceSourceRoot(cmdLine.workspaceSourceRoot)

  private val workingDir = Directory(Paths.get(sys.props("user.dir")))

  private val buildDir: Directory = cmdLine.buildDir
  private val outputDir: Directory = buildDir.resolveDir(version)
  private val installDir: Directory = cmdLine.installDir.asDirectory.getOrElse(buildDir.parent.resolveDir("install"))
  private val sandboxDir: Directory =
    cmdLine.sandboxDir.asDirectory.getOrElse(buildDir.resolveDir(Sandboxes))

  private val compilerThrottle = new CompilerThrottle(cmdLine.maxZincCompileBytes, cmdLine.maxNumZincs)

  private val traceRecorder = new TraceRecorder(cmdLine.traceFile.asDirectory)
  private val timingsRecorder = new TimingsRecorder(logDir)
  val countingTrace = new CountingTrace(Some(cmdLine.statusIntervalSec))
  private val longRunningTraceListener = new LongRunningTraceListener(useCrumbs = useCrumbs)
  private val buildSummaryRecorder = new BuildSummaryRecorder(cmdLine.statsDir.asDirectory, compilerThrottle)

  private val memoryMonitor = FreeMemoryMonitor.fromProperties()
  private val memoryThrottle = MemoryThrottle.fromConfigStringOrSysProp(cmdLine.memConfig.nonEmptyOption)

  private val baseListeners = timingsRecorder :: traceRecorder :: countingTrace :: longRunningTraceListener ::
    buildSummaryRecorder :: memoryMonitor :: memoryThrottle.toList

  private val uploadLocations: Seq[UploadLocation] =
    cmdLine.uploadLocations.toIndexedSeq.map(UploadLocation(_, cmdLine.decompressAfterUpload))

  private val dockerDir = cmdLine.dockerDir.asDirectory.getOrElse(workspaceRoot.resolveDir("docker-out"))

  private val cacheFlushTimeout = 60000 // ms

  private def gbString(bytes: Long): String = {
    f"${bytes * 1.0 / (1024 * 1024 * 1024)}%,.1f"
  }

  private val cppOsVersions =
    if (cmdLine.cppOsVersions.isEmpty) {
      if (cmdLine.cppOsVersion != NoneArg) Seq(cmdLine.cppOsVersion)
      else Seq(OsUtils.Linux7Version, OsUtils.Linux8Version, OsUtils.WindowsVersion)
    } else if (cmdLine.cppOsVersions == Seq(NoneArg)) Nil
    else cmdLine.cppOsVersions

  private val requiredCppOsVersions =
    if (cmdLine.requiredCppOsVersions.isEmpty) {
      if (cmdLine.cppOsVersion != NoneArg) Seq(cmdLine.cppOsVersion)
      else Seq(OsUtils.osVersion)
    } else if (cmdLine.requiredCppOsVersions == Seq(NoneArg)) Nil
    else cmdLine.requiredCppOsVersions

  private val appDir = sys.env.getOrElse("APP_DIR", "unknown")

  log.info(Utils.LogSeparator)
  log.info(s"Hostname: ${Utils.hostName.getOrElse("unknown")}")
  log.info(s"Process ID: ${Utils.processId.getOrElse("unknown")}")
  log.info(s"User: ${sys.props("user.name")}")
  log.info(s"OS version: ${OsUtils.osVersion}")
  log.info(s"OBT app directory: $appDir")
  log.info(s"OBT artifact version: $version")
  log.info(s"Working directory: $workingDir")
  log.info(s"Workspace root: $workspaceRoot")
  log.info(s"Source directory: $workspaceSourceRoot")
  log.info(s"Docker directory: $dockerDir")
  log.info(s"Build directory: $buildDir")
  if (cmdLine.install) log.info(s"Install directory: $installDir")
  log.info(s"Java home: ${Utils.javaHome}")
  log.info(s"Java class version: ${Utils.javaClassVersion}")
  log.info(
    s"Graph threads: ${EvaluationContext.current.scheduler.getIdealThreadCount} (configured: ${Settings.threadsIdeal})"
  )
  log.info(s"Max heap: ${gbString(Runtime.getRuntime.maxMemory)}GB")
  log.info(s"DHT (${cmdLine.cacheMode}): ${cmdLine.dhtRemoteStore}")

  if (appDir.replace("\\", "/").startsWith(installDir.toString) && cmdLine.install) {
    throw new IllegalArgumentException(
      s"""Build can't be continued because OBT is attempting to build the workspace
         |into the install directory which contains OBT artifacts used by this build.
         |
         |This will result in runtime filesystem errors and is most likely unintended.
         |In order to fix it make sure to clean your global `internal.obt.install` config.
         |
         |OBT app directory: $appDir
         |Install directory: $installDir""".stripMargin)
  }

  val obtRunDetails: Map[String, String] = Map(
    "hostname" -> Utils.hostName.getOrElse("unknown"),
    "processID" -> Utils.processId.getOrElse("unknown"),
    "user" -> sys.props("user.name"),
    "osVersion" -> OsUtils.osVersion,
    "appDir" -> appDir,
    "artifactVersion" -> version,
    "workingDir" -> workingDir.pathString,
    "workspaceRoot" -> workspaceRoot.pathString,
    "sourceDir" -> workspaceSourceRoot.pathString,
    "dockerDir" -> dockerDir.pathString,
    "installDir" -> installDir.pathString,
    "javaHome" -> Utils.javaHome.pathString,
    "classVersion" -> Utils.javaClassVersion,
    "configuredGraphThreads" -> Settings.threadsIdeal.toString,
    "actualGraphThreads" -> EvaluationContext.current.scheduler.getIdealThreadCount.toString,
    "maxHeap" -> s"${gbString(Runtime.getRuntime.maxMemory())}GB"
  )

  Breadcrumbs.info(
    ChainedID.root,
    PropertiesCrumb(
      _,
      ObtCrumbSource,
      Set.empty[CrumbHint],
      Properties.obtRunDetails -> obtRunDetails
    )
  )

  // intentionally sending this to debug as this is a bit verbose for users to see
  private def optimusSysProps = sys.props.collect { case (k, v) if k.toLowerCase.contains("optimus") => s"$k=$v" }
  log.debug(s"Optimus System Props: ${optimusSysProps.mkString(", ")}")

  if (uploadLocations.nonEmpty) log.info(s"Upload locations: ${uploadLocations.mkString(", ")}")
  log.info(Utils.LogSeparator)

  private val depCopyRoot = cmdLine.depCopyDir.asDirectory.getOrElse {
    workspaceRoot.parent.resolveDir(".stratosphere").resolveDir("depcopy")
  }
  val venvCacheDir: Directory = depCopyRoot.resolveDir("venv-cache")
  val uvCacheDir: Directory = depCopyRoot.resolveDir("uv-cache")

  val pythonEnvironment: PythonEnvironment =
    PythonEnvironment(venvCacheDir, uvCacheDir, cmdLine.pypiCredentialsFile, cmdLine.pypiUvCredentialsFile)

  private val depCopyFileSystemAsset = Utils.isWindows && cmdLine.depCopyDir != NoneArg

  // scopes => build "scopes"; Set.empty => build previous scopes; Set("q") => quit
  private val autoQueue = new BlockingQueue[Set[String]]

  private[buildtool] val localDirectoryFactory =
    if (cmdLine.bspServer || cmdLine.interactive) {
      val directoryWatcher =
        if (cmdLine.useNioFileWatcher) {
          val listener = if (cmdLine.interactive) Some(() => autoQueue.put(Set.empty)) else None
          new WatchServiceDirectoryWatcher(FileSystems.getDefault.newWatchService(), listener)
        } else new ScanningDirectoryWatcher()
      ReactiveDirectoryFactory(directoryWatcher)
    } else NonReactiveDirectoryFactory

  @node @scenarioIndependent private def gitUtils: Option[GitUtils] =
    if (cmdLine.useGit) GitUtils.find(workspaceSourceRoot, localDirectoryFactory)
    else None

  @node @scenarioIndependent protected def gitLog: Option[GitLog] =
    gitUtils.map(u =>
      GitLog(
        u,
        workspaceSourceRoot,
        stratoWorkspace(),
        cmdLine.targetBranch,
        cmdLine.gitFilterRe.getOrElse(MergeCommitUtils.prMessagePattern(cmdLine.targetBranch).toString()),
        cmdLine.gitLength
      ))

  @node private def latestCommit: Option[Commit] = gitLog.flatMap(_.HEAD)

  @node @scenarioIndependent private def recency: Option[ArtifactRecency] = gitLog.map(new ArtifactRecency(buildDir, _))

  @node @scenarioIndependent private def gitSourceFolderFactory =
    if (cmdLine.allowSparse) gitUtils.map(u => GitSourceFolderFactory(u))
    else None

  @node @scenarioIndependent private[buildtool] def directoryFactory = (gitUtils, gitSourceFolderFactory) match {
    case (Some(utils), Some(gsff)) =>
      GitAwareDirectoryFactory(utils, localDirectoryFactory, gsff)
    case _ =>
      localDirectoryFactory
  }

  @node @scenarioIndependent private def pathBuilder = CompilePathBuilder(outputDir)

  @node @scenarioIndependent def mischiefOpts: MischiefOptions =
    MischiefOptions.load(workspaceSrcRoot = workspaceSourceRoot)

  @node @scenarioIndependent private[buildtool] def remoteCaches =
    RemoteCacheProvider(cmdLine, version, pathBuilder)

  @node @scenarioIndependent private def combined(
      caches: Seq[ArtifactCache with HasArtifactStore]
  ): Option[ArtifactCache with HasArtifactStore] = {
    if (caches.size > 1) Some(MultiLevelCache(caches: _*))
    else caches.headOption
  }

  @node @scenarioIndependent private def workspaceName = workspaceRoot.name

  @node def globalConfig: GlobalConfig = {
    val depCopyRootOption: Option[Directory] = if (depCopyFileSystemAsset) Some(depCopyRoot) else None
    GlobalConfig.load(
      workspaceName,
      workspaceRoot,
      workspaceSourceRoot,
      depCopyRootOption,
      directoryFactory,
      cmdLine.installVersion
    )
  }

  @node private def installPathBuilder: InstallPathBuilder = {
    def installPathBuilder: (Directory, String) => InstallPathBuilder =
      if (generatePoms) InstallPathBuilder.mavenRelease else InstallPathBuilder.dev
    installPathBuilder(installDir, globalConfig.versionConfig.installVersion)
  }

  @node private def dependencyMetadataResolvers: DependencyMetadataResolvers = obtConfig.depMetadataResolvers

  @node @scenarioIndependent protected def rubbishTidyer: Option[RubbishTidyer] =
    cmdLine.maxBuildDirSize.map { maxSizeMegabytes =>
      val gitLog = gitUtils.map { u =>
        GitLog(u, workspaceSourceRoot, stratoWorkspace(), cmdLine.targetBranch, commitLength = cmdLine.gitPinDepth)
      }
      val freeDiskSpaceTriggerBytes = cmdLine.freeDiskSpaceTriggerMb.map(_.toLong << 20L)
      new RubbishTidyerImpl(maxSizeMegabytes.toLong << 20L, freeDiskSpaceTriggerBytes, buildDir, sandboxDir, gitLog)
    }

  @node @scenarioIndependent private def zincAnalysisCache =
    new ZincAnalysisCache(cmdLine.zincAnalysisCacheSize, instrumentation)

  @node def scalaResolver: ScalaResolver = new ScalaResolver(
    dependencyResolver.resolver(ModuleSet.empty),
    dependencyCopier,
    globalConfig.scalaVersion
  )

  val interfaceDir = cmdLine.zincInterfaceDir.asDirectory.getOrElse {
    buildDir.resolveDir("zincCompilerInterface")
  }

  @node protected def scalacProvider: ScalacProvider = ScalacProvider.instance(
    dependencyResolver.resolver(ModuleSet.empty),
    dependencyCopier,
    scalaResolver.scalaVersionConfig,
    ZincClassLoaderCaches,
    interfaceDir
  )

  @node private def underlyingCompilerFactory: ZincCompilerFactory = {

    ZincCompilerFactory(
      jdkPath = globalConfig.javaPath,
      scalaConfig = scalaResolver.scalaVersionConfig,
      scalacProvider = scalacProvider,
      workspaceRoot = workspaceRoot,
      buildDir = outputDir,
      interfaceDir = interfaceDir,
      depCopyRoot = depCopyRoot,
      cachePluginAndMacroClassLoaders = cmdLine.cacheClassloaders,
      zincIgnorePluginHash = cmdLine.zincIgnorePluginHash,
      zincIgnoreChangesRegexp = cmdLine.zincIgnoreChangesRegexp,
      zincRecompileAllFraction = cmdLine.zincRecompileAllFraction,
      analysisCache = zincAnalysisCache,
      instrumentation = instrumentation,
      bspServer = cmdLine.bspServer,
      localArtifactStore = localStore,
      remoteArtifactReader = remoteCaches.remoteBuildCache.map(_.store),
      remoteArtifactWriter = remoteCaches.remoteBuildCache.map(_.store),
      classLoaderCaches = ZincClassLoaderCaches,
      scalacProfileDir = scalacProfileDir,
      strictErrorTolerance = cmdLine.zincStrictMapping,
      zincTrackLookups = cmdLine.zincTrackLookups,
      depCopyFileSystemAsset = depCopyFileSystemAsset,
      compilerThrottle = compilerThrottle
    )
  }

  @node @scenarioIndependent private def cppCompilerFactory: CppCompilerFactory = new CppCompilerFactoryImpl

  private val configParams: Map[String, String] = cmdLine.configParams.asScala.map { c =>
    val parts = c.split("=")
    (parts.head, parts.tail.mkString)
  }.toMap

  @node def obtConfig: ObtConfig = {
    // Git is currently only used for loading config for sparse workspaces
    val git = if (cmdLine.allowSparse) gitUtils else None
    ObtConfig.load(
      workspaceName,
      directoryFactory,
      git,
      workspaceSourceRoot,
      configParams,
      cppOsVersions,
      useMavenLibs,
      Some(obtErrorReporter)
    )
  }

  @node @scenarioIndependent private def stratoWorkspace(): StratoWorkspace =
    StratoConfig.stratoWorkspace(workspaceRoot)

  @node @scenarioIndependent private def credentials: Seq[Credential] =
    if (cmdLine.credentialFiles == NoneArg && !requireCredentials) Nil
    else if (cmdLine.credentialFiles == NoneArg) {
      val ws = stratoWorkspace()
      def existCredentialFile: Option[Credential] = Credential.fromJfrogConfFile(ws.internal.jfrog.configFile)
      existCredentialFile match {
        case Some(file) => Seq(file)
        case None =>
          log.warn(s"No credential files defined! obt will try generate one now")
          ArtifactoryToolInstaller.presetArtifactoryCredentials(ws)
          existCredentialFile.to(Seq)
      }
    } else
      cmdLine.credentialFiles
        .split(",")
        .toIndexedSeq
        .filter(_.trim.nonEmpty)
        .flatMap { p =>
          if (p.contains("jfrog-cli.conf")) Credential.fromJfrogConfFile(Paths.get(p))
          else Credential.fromPropertiesFile(Paths.get(p))
        }

  // This is used for maven url file cache into remote store.
  @node @scenarioIndependent private def remoteAssetStore: RemoteAssetStore =
    remoteCaches.remoteBuildCache.map(_.store).getOrElse(NoOpRemoteAssetStore)

  @node @scenarioIndependent private def dependencyCopier = {
    DependencyCopier(depCopyRoot, credentials, remoteAssetStore, depCopyFileSystemAsset)
  }

  // This hasher is only used for the obt configuration files.
  @node @scenarioIndependent private def rootFingerprintHasher =
    FingerprintHasher(ScopeId.RootScopeId, pathBuilder, fsCache.store, None, mischief = false)

  @node def dependencyResolver: CoursierArtifactResolverSource = externalDependencyResolver(dependencyMetadataResolvers)

  @node private def validateCredentials(
      loaded: Seq[Credential],
      externalDependencies: ExternalDependencies): Seq[Credential] = {
    val mavenLibs = externalDependencies.mavenDependencies.allMavenDeps
    val withMavenLibs = mavenLibs.nonEmpty
    val withRemoteCache = remoteCaches.remoteBuildCache.nonEmpty
    val withLocalCache =
      Files.exists(depCopyRoot.resolveDir("https").path) || Files.exists(depCopyRoot.resolveDir("http").path)

    if (requireCredentials && withMavenLibs && loaded.isEmpty && !withRemoteCache && !withLocalCache)
      throw new IllegalArgumentException(
        s"Build failed: no credential and cache available, unable to download ${mavenLibs.size} maven libraries!")
    else if (requireCredentials && withMavenLibs && loaded.isEmpty && (withRemoteCache || withLocalCache))
      log.warn(
        s"Maven server offline mode: no credential found! maven libs: ${mavenLibs.size}, local cache: $withLocalCache, remote cache: $withRemoteCache")
    loaded
  }

  @node private def externalDependencyResolver(resolvers: DependencyMetadataResolvers): CoursierArtifactResolverSource =
    CoursierArtifactResolverSource(
      resolvers = resolvers,
      dependencies = obtConfig.jvmDependencies,
      dependencyCopier = dependencyCopier,
      globalExcludes = obtConfig.globalExcludes,
      credentials = validateCredentials(credentials, obtConfig.jvmDependencies.allExternalDependencies),
      remoteAssetStore = remoteAssetStore,
      enableMappingValidation = enableMappingValidation,
      throttle = Some(AdvancedUtils.newThrottle(cmdLine.resolverThrottle))
    )

  @node private def webDependencyResolver: WebDependencyResolver = {
    val webScopes = obtConfig.scopeDefinitions.collect {
      case (id, definition) if definition.configuration.webConfig.isDefined => id -> definition.configuration
    }
    val webInfoBeforeBuild: Map[ScopeId, WebScopeInfo] =
      WebDependencyResolver.resolveWebInfo(webScopes, obtConfig.jvmDependencies)
    WebDependencyResolver(webInfoBeforeBuild)
  }

  @node private def sourceGenerators =
    Seq(
      CppBridgeGenerator(scalaCompiler, pathBuilder),
      FlatbufferGenerator(workspaceSourceRoot),
      JaxbGenerator(directoryFactory, workspaceSourceRoot),
      JxbGenerator(workspaceSourceRoot),
      JsonSchemaGenerator(directoryFactory, workspaceRoot),
      PodcastGenerator(workspaceSourceRoot, obtConfig),
      ProtobufGenerator(dependencyResolver.resolver(ModuleSet.empty), dependencyCopier, workspaceSourceRoot),
      ScalaxbGenerator(workspaceSourceRoot),
      AvroSchemaGenerator(workspaceSourceRoot),
      FixGenerator(workspaceSourceRoot),
      DomainGenerator(workspaceSourceRoot)
    ).map(g => g.tpe -> g).toMap

  @node private def processors =
    Seq(
      VelocityProcessor(),
      OpenApiProcessor(logDir, useCrumbs),
      FreemarkerProcessor(),
      DeploymentScriptProcessor(sandboxFactory, logDir))
      .map(g => g.tpe -> g)
      .toMap

  @node @scenarioIndependent private def sandboxFactory: SandboxFactory = SandboxFactory(sandboxDir)
  @node private def scalaCompiler: AsyncScalaCompiler = AsyncScalaCompiler(underlyingCompilerFactory)
  @node private def javaCompiler: AsyncJavaCompiler = AsyncJavaCompiler(underlyingCompilerFactory)
  @node @scenarioIndependent private def jmhCompiler = AsyncJmhCompilerImpl(sandboxFactory, dependencyCopier)
  @node @scenarioIndependent private def cppCompiler =
    AsyncCppCompilerImpl(cppCompilerFactory, sandboxFactory, requiredCppOsVersions.toSet)
  @node @scenarioIndependent private def pythonCompiler =
    AsyncPythonCompilerImpl(
      workspaceSourceRoot,
      sandboxFactory,
      logDir,
      pythonEnvironment,
      asNode(() => globalConfig.pythonEnabled),
      asNode(() => globalConfig.buildAfsMapping))
  @node @scenarioIndependent private def webCompiler =
    AsyncWebCompilerImpl(depCopyRoot.resolveDir("pnpm-store"), sandboxFactory, logDir, useCrumbs)
  @node @scenarioIndependent private def electronCompiler =
    AsyncElectronCompilerImpl(depCopyRoot.resolveDir("pnpm-store"), sandboxFactory, logDir, useCrumbs)
  @node private def stratoConfig = StratoConfig.load(directoryFactory, workspaceSourceRoot)
  @node private def runconfCompiler = AsyncRunConfCompilerImpl.load(obtConfig, stratoConfig, dependencyCopier)
  @node @scenarioIndependent private def resourcePackager = JarPackager()
  @node @scenarioIndependent private def genericFilesPackager = GenericFilesPackager(cmdLine.installVersion)
  @node @scenarioIndependent private def manifestGenerator = ManifestGenerator(dependencyCopier, cmdLine.cppFallback)

  @node private def freezerCache = FreezerCache(FreezerStoreConf.load(mischiefOpts), pathBuilder)

  @node @scenarioIndependent private def fsCache =
    FilesystemCache(pathBuilder, cmdLine.incrementalMode)

  @node private def localStore = MultiLevelStore(freezerCache.store, fsCache.store)

  @node private def buildCache = {
    val caches = remoteCaches.remoteBuildCache match {
      case Some(remoteCache) =>
        if (cmdLine.cacheMode.canWrite) {
          // If we've enabled remote cache writes, then write to it even if we've found it in our
          // filesystem cache. Among other benefits, this works around a bug that can cause artifacts not to
          // be written to remote cache if they've been written to the filesystem as a side-effect of compiling for a different
          // artifact type.
          Seq(remoteCaches.writeOnlyRemoteBuildCache, Some(fsCache), remoteCaches.readOnlyRemoteBuildCache).flatten
        } else Seq(fsCache, remoteCache)
      case None =>
        Seq(fsCache)
    }
    combined(freezerCache +: caches).get
  }

  @node private[buildtool] def scopeSelector =
    ScopeSelector(ScopeArgs(cmdLine), obtConfig, workspaceSourceRoot, dockerDir)

  @node @scenarioIndependent private def scalacProfileDir: Option[Directory] =
    if (cmdLine.profileScalac) {
      val dirPath = pathBuilder.outputDir.resolveDir("scalac-profile")
      Files.createDirectories(dirPath.path)
      Some(dirPath)
    } else None

  private[buildtool] val dependencyTracker = DependencyTrackerRoot()

  private[buildtool] val scopedCompilationFactory: ScopedCompilationFactory = ScopedCompilationFactoryImpl()
  @entity private[buildtool] class ScopedCompilationFactoryImpl extends CompilationNodeFactory {
    @node def globalMessages: Seq[MessagesArtifact] = {
      obtConfig.messages.map { case ((tpe, trace), messages) =>
        val id = InternalArtifactId(ScopeId.RootScopeId, tpe, None)
        InMemoryMessagesArtifact(id, messages, trace)
      }.toIndexedSeq
    }
    @node def scopeIds: Set[ScopeId] = scopeConfigSource.compilationScopeIds
    @node override def freezeHash: Option[String] = mischiefOpts.freezerHash
    @node override def mischiefScope(scope: ScopeId): Boolean = mischiefOpts.mischief(scope).nonEmpty
    @node override def scopeConfigSource: ScopeConfigurationSource = obtConfig

    @node private def asSourceFolders(scopeDir: Directory, sources: Seq[RelativePath]) =
      sources.apar.map(s => directoryFactory.lookupSourceFolder(workspaceSourceRoot, scopeDir.resolveDir(s)))

    @node override def lookupScope(id: ScopeId): Option[CompilationNode] = {
      if (scopeConfigSource.compilationScopeIds.contains(id)) ObtTrace.traceTask(id, InitializeScope) {
        val scopeConfig = scopeConfigSource.scopeConfiguration(id)

        val runConfConfig = RunConfConfiguration(
          asSourceFolders(scopeConfig.absScopeConfigDir, Templates.potentialLocationsFromScopeConfigDir) ++
            asSourceFolders(scopeConfig.absScopeConfigDir.parent, Templates.potentialLocationsFromScopeConfigDir) ++
            asSourceFolders(scopeConfig.paths.workspaceSourceRoot, Templates.potentialLocationsFromScopeConfigDir),
          directoryFactory.lookupSourceFolder(
            workspaceSourceRoot,
            scopeConfig.absScopeConfigDir.copy(
              dirFilter = Not(Directory.fileNamePredicate(GeneratedObt)),
              maxDepth = 1 // condition is to have a .runconf file in the scope root, otherwise we report nothing
            )
          )
        )
        val compilationScope = CompilationScope(
          id,
          config = scopeConfig,
          sourceFolders = asSourceFolders(scopeConfig.paths.absScopeRoot, scopeConfig.paths.sources),
          resourceFolders = asSourceFolders(scopeConfig.paths.absScopeRoot, scopeConfig.paths.resources),
          webSourceFolders = asSourceFolders(scopeConfig.paths.absScopeRoot, scopeConfig.paths.webSources),
          electronSourceFolders = asSourceFolders(scopeConfig.paths.absScopeRoot, scopeConfig.paths.electronSources),
          pythonSourceFolders = asSourceFolders(scopeConfig.paths.absScopeRoot, scopeConfig.paths.pythonSources),
          archiveContentFolders = asSourceFolders(scopeConfig.paths.absScopeRoot, scopeConfig.paths.archiveContents),
          genericFileFolders = asSourceFolders(scopeConfig.paths.absScopeRoot, Seq(RelativePath("files"))),
          runConfConfig = Some(runConfConfig),
          pathBuilder = pathBuilder,
          compilers = Seq(scalaCompiler, javaCompiler),
          dependencyCopier = dependencyCopier,
          externalDependencyResolver = dependencyResolver.resolver(scopeConfig.moduleSet),
          cache = buildCache,
          factory = this,
          directoryFactory = directoryFactory,
          mischief = mischiefOpts.mischief(id),
          buildAfsMapping = globalConfig.buildAfsMapping
        )

        Some(
          ScopedCompilationImpl(
            scope = compilationScope,
            scopeConfigSource = scopeConfigSource,
            sourceGenerators = sourceGenerators,
            scalac = scalaCompiler,
            javac = javaCompiler,
            jmhc = jmhCompiler,
            cppc = cppCompiler,
            pythonc = pythonCompiler,
            webc = webCompiler,
            electronc = electronCompiler,
            manifestGenerator = manifestGenerator,
            runconfc = runconfCompiler,
            jarPackager = resourcePackager,
            genericFilesPackager = genericFilesPackager,
            analysisLocator = Some(
              new AnalysisLocatorImpl(
                gitLog = gitLog,
                localArtifactStore = underlyingCompilerFactory.localArtifactStore,
                remoteArtifactReader = underlyingCompilerFactory.remoteArtifactReader,
                remoteArtifactWriter = underlyingCompilerFactory.remoteArtifactWriter,
                suppressPersistentStore = mischiefScope(id),
                additionalDiscriminator = cmdLine.locatorSuffix
              )),
            incrementalMode = cmdLine.incrementalMode,
            processors = processors,
            cmdLine.installVersion,
            cmdLine.cppFallback,
            strictEmptySources = cmdLine.strictEmptySources
          )
        )
      }
      else {
        log.debug(s"Compilation scope: '$id' not found.")
        None
      }
    }
  }

  @node private[buildtool] def bspInstaller(bspInstallDir: Option[Directory]): PostBuilder = {
    val sparseWorkspace = obtConfig.compilationScopeIds.apar.exists(!obtConfig.local(_))
    val standardBspInstaller =
      bspInstallDir
        .map(bspSingleInstaller(_, globalConfig.versionConfig, sparseOnly = false))
        .getOrElse(PostBuilder.zero)
    if (!sparseWorkspace)
      standardBspInstaller
    else
      PostBuilder.merge(
        Seq(
          standardBspInstaller,
          bspSingleInstaller(
            Utils.sparseJarDir(buildDir),
            globalConfig.versionConfig.copy(installVersion = Utils.SparseJarInstallVersion),
            sparseOnly = true
          )
        )
      )
  }

  @node private def bspSingleInstaller(
      bspInstallDir: Directory,
      versionConfig: VersionConfiguration,
      sparseOnly: Boolean
  ): Installer = {
    val bspInstallPathBuilder: InstallPathBuilder =
      if (generatePoms) InstallPathBuilder.mavenRelease(bspInstallDir, versionConfig.installVersion)
      else InstallPathBuilder.dev(bspInstallDir, versionConfig.installVersion)

    val bundleFingerprintsCache = new BundleFingerprintsCache(bspInstallPathBuilder, cmdLine.verifyInstall)

    new Installer(
      scopeConfigSource = obtConfig,
      factory = scopedCompilationFactory,
      installDir = bspInstallDir,
      buildDir = buildDir,
      depCopyRoot = depCopyRoot,
      sourceDir = workspaceSourceRoot,
      versionConfig = versionConfig,
      testplanConfig = globalConfig.testplanConfig,
      genericRunnerConfiguration = globalConfig.genericRunnerConfig,
      postInstaller = PostInstaller.zero,
      gitLog = gitLog,
      installTestplans = false,
      directoryFactory = directoryFactory,
      sparseOnly = sparseOnly,
      warScopes = scopeSelector.warScopes,
      bundleFingerprintsCache = bundleFingerprintsCache,
      generatePoms = generatePoms,
      useMavenLibs = useMavenLibs,
      pythonConfiguration = InstallTpa,
      externalDependencies = obtConfig.jvmDependencies,
      fingerprintsConfiguration = obtConfig.fingerprintsDiffConfig
    )
  }

  @entersGraph def start(): Boolean = {
    if (cmdLine.clean) {
      if (cmdLine.scopesToBuild.isEmpty) {
        log.info(s"Cleaning $buildDir...")
        AssetUtils.recursivelyDelete(buildDir)
      } else {
        try {
          val snippets: Set[String] = scopeSelector.scopes().map(_.properPath)
          if (snippets.nonEmpty) {
            log.info(s"Cleaning $buildDir contents matching ${snippets.mkString("|")}")
            AssetUtils.recursivelyDelete(buildDir, d => snippets.exists(d.getFileName.toString.contains))
          }
        } catch {
          case t: Throwable =>
            val msg = CompilationMessage.errorMsg(obtConfig.messages.values.flatten.toList)
            throw new IllegalStateException(msg, t)
        }
      }
    }

    RubbishTidyer.tidyLogs(logDir.path, cmdLine.maxLogAge)
    if (cmdLine.bspServer) {
      bspServer.runServer()
    } else if (cmdLine.interactive) {
      buildInteractive()
    } else {
      rubbishTidyer.foreach { t =>
        val artifacts = t.storedArtifacts
        t.tidy(artifacts, Nil)
      }
      build()._1
    }
  }

  @entersGraph private def buildInteractive(): Boolean = {
    val stdInReader = new Thread(
      () => {
        while (true) {
          val str = StdIn.readLine()
          autoQueue.put(str.split("""[,\s]""").map(_.trim).toSet)
        }
      },
      "OBT StdIn Reader"
    )
    stdInReader.setDaemon(true)
    stdInReader.start()

    val buildCount = new AtomicInteger(0)
    val builder = trackingBuilder

    @entersGraph @tailrec def buildLoop(scopeSelector: ScopeSelector): Boolean = {
      val buildResult = asyncResult(build(builder, buildCount.incrementAndGet(), scopeSelector)._1) valueOrElse {
        case NonFatal(ex) => // just say the build failed and let the user try again
          log.error("An exception occurred during the build", ex)
          false
      }
      if (cmdLine.useNioFileWatcher)
        log.warn(
          "Autobuild enabled; press Enter to manually trigger build, or provide a new set of scopes, or 'q' to quit..."
        )
      else
        log.warn("Press Enter to build the same scopes again, or provide a new set of scopes, or 'q' to quit...")
      // Wait for a 500ms quiet period after first message to conflate updates
      val requestedScopes: Set[String] =
        // regex-ignore-line: thread-blocking
        (autoQueue.take() +: { Thread.sleep(500); autoQueue.pollAll() }).flatten.toSet - ""

      if (requestedScopes contains "q") { // quit requested
        buildResult // stop looping and return
      } else {
        val ss = if (requestedScopes.nonEmpty) scopeSelector.withIncludedScopes(requestedScopes) else scopeSelector
        buildLoop(ss)
      }
    }

    buildLoop(scopeSelector)
  }
  @entersGraph def build(): (Boolean, Option[BuildStatistics]) = {
    val ss = ObtTrace.withListeners(baseListeners) { scopeSelector }
    build(standardBuilder(), 0, ss)
  }

  @async private def build(
      builder: Builder,
      buildCount: Int,
      scopeSelector: ScopeSelector
  ): (Boolean, Option[BuildStatistics]) = {
    val buildId = s"$implId-$buildCount"
    val crumbListener = BreadcrumbTraceListener(
      useCrumbs,
      workspaceRoot,
      buildId,
      sendSummaryToLongTermIndex = cmdLine.sendLongTermBreadcrumbs,
      sendPerScopeStats = cmdLine.sendPerScopeBreadcrumbs
    )(baseListeners)
    var success = false
    var traceOk = false
    ObtTrace.withListeners(baseListeners ++ crumbListener) {
      asyncTry {
        ObtTrace.startBuild()
        track {
          val modifiedFiles = if (cmdLine.gitAwareMessages) gitLog.flatMap(_.fileDiff()) else None
          val scopesToBuild = scopeSelector.scopes(modifiedFiles)
          val result = builder.build(scopesToBuild, modifiedFiles = modifiedFiles)
          success = result.successful
          if (success) writeTagFile(remoteCaches.remoteBuildCache, cmdLine.gitTag)
        }
      } thenFinally {
        localStore.flush(cacheFlushTimeout)
      } asyncFinally {
        // note that endBuild may have side effects so we always call it
        traceOk = ObtTrace.endBuild(success) || !cmdLine.failOnAnomalousTrace
        if (!traceOk) log.error("BUILD FAILED due to anomalies detected in trace")
      }
    }

    (success && traceOk, crumbListener.map(_.buildStatistics))
  }

  @async private def writeTagFile(
      buildCache: Option[RemoteCacheProvider.RemoteArtifactCache],
      tagName: Option[String] = None): Unit = {
    for {
      git <- gitLog
      tag <- tagName
      bc <- buildCache
    } {
      if (bc.store.incompleteWrites == 0) {
        // Unfortunately, we cannot create the git tag ourselves as we don't have the permission to push the git tag
        // so we delegate this for a more powerful user id (via Jenkins)
        val taggingFile = installDir.resolveFile("tag.properties")
        val properties =
          Map("tag.name" -> tag) ++
            git
              .updatedTagCommit(tag)
              .map { currentHead =>
                log.info(s"${taggingFile.pathString} created: tag $tagName should be moved to $currentHead (HEAD)")
                Map("tag.hash" -> currentHead)
              }
              .getOrElse {
                Map("tag.skip" -> "true")
              }
        Utils.writePropertiesToFile(taggingFile, properties)
      } else {
        log.warn(s"Skipping $tag write due to ${bc.store.incompleteWrites} incomplete writes")
      }
    }
  }

  @node @scenarioIndependent private def workspace: TrackedWorkspace =
    new TrackedWorkspace(dependencyTracker, directoryFactory, rubbishTidyer, mischiefOpts)

  // Hooks for BSP test wireup
  protected def stdin: InputStream = System.in
  protected def stdout: OutputStream = OptimusBuildTool.originalStdOut
  protected def traceFilter: Option[TraceFilter] =
    if (OptimusBuildToolProperties.getOrFalse("showAllWarnings"))
      Some(TraceFilter.AllWarningsFilter) // override
    else TraceFilter.parse(OptimusBuildToolProperties.get("showWarnings"))

  protected def extraBspPostBuilders: Seq[PostBuilder] = Nil

  // we need this to be @si to guarantee we're not capturing anything on process startup that we shouldn't
  @node @scenarioIndependent private[buildtool] def bspServer: BuildServerProtocolServer = {
    new BuildServerProtocolServer(
      workspace = workspace,
      builder = asNode(() =>
        standardBuilder(
          Some(asAsync.apply0(remoteCaches.remoteBuildCache.foreach(_.store.logStatus()))),
          Some(asAsync.apply0(buildCache.store.flush(cacheFlushTimeout))),
          extraBspPostBuilders
        )),
      scopeFactory = scopedCompilationFactory,
      workspaceName = workspaceName,
      workspaceRoot = workspaceRoot,
      workspaceSourceRoot = workspaceSourceRoot,
      buildDir = buildDir,
      outputDir = outputDir,
      stdin = stdin,
      stdout = stdout,
      clientInitializationTimeoutMs = cmdLine.bspClientInitializationTimeoutMs,
      sendCrumbs = useCrumbs,
      installerFactory = asNode(bspInstaller),
      scalaVersionConfig = asNode(() => scalaResolver.scalaVersionConfig),
      pythonBspConfig = PythonBspConfig(
        asNode(() => globalConfig.pythonEnabled),
        asNode(() => globalConfig.extractVenvs),
        pythonEnvironment
      ),
      depMetadataResolvers = asNode(() => dependencyMetadataResolvers.allResolvers),
      directoryFactory = directoryFactory,
      dependencyCopier = dependencyCopier,
      incrementalMode = cmdLine.incrementalMode,
      hasher = rootFingerprintHasher,
      gitLog = gitLog,
      gitAwareMessages = cmdLine.gitAwareMessages,
      listeners = baseListeners,
      traceFilter = traceFilter,
      recency = recency,
      osVersion = OsUtils.osVersion
    )
  }

  // we need this to be @si to guarantee we're not capturing anything on process startup that we shouldn't
  @node @scenarioIndependent private[buildtool] def trackingBuilder: TrackingBuilder = new TrackingBuilder(
    asNode(() => standardBuilder()),
    workspace
  )

  @node private[buildtool] def standardBuilder(
      onBuildStart: Option[AsyncFunction0[Unit]] = None,
      onBuildEnd: Option[AsyncFunction0[Unit]] = None,
      extraPostBuilders: Seq[PostBuilder] = Nil
  ): StandardBuilder = {

    val scopeFilter = ScopeFilter(cmdLine.scopeFilter)

    val backgroundBuilder = if (cmdLine.backgroundCmd != NoneArg) {
      Some(BackgroundProcessBuilder.onDemand(logDir, cmdLine.backgroundCmd, useCrumbs))
    } else None

    val bundleFingerprintsCache = new BundleFingerprintsCache(installPathBuilder, cmdLine.verifyInstall)

    val assetUploader: Option[AssetUploader] = if (uploadLocations.nonEmpty) {
      val uploadDir = outputDir.resolveDir("upload")

      Files.createDirectories(uploadDir.path)
      val srcPrefix =
        if (cmdLine.uploadSourcePrefix != NoneArg) Some(RelativePath(cmdLine.uploadSourcePrefix))
        else None
      Some(
        new AssetUploader(
          uploadDir = uploadDir,
          sourceDir = workspaceSourceRoot,
          installDir = installDir,
          logDir = logDir,
          toolsDir = cmdLine.toolsDir.map(d => Directory(Paths.get(d))),
          uploadLocations,
          maxConcurrentUploads = cmdLine.maxConcurrentUploads,
          minBatchSize = cmdLine.minUploadBatchSize,
          maxBatchSize = cmdLine.maxUploadBatchSize,
          maxBatchBytes = cmdLine.maxUploadBatchBytes,
          maxRetry = cmdLine.maxUploadRetry,
          srcPrefix = srcPrefix,
          srcFilesToUpload = globalConfig.defaultSrcFilesToUpload,
          cmdLine.uploadFormat,
          useCrumbs = useCrumbs,
          filesToExclude = globalConfig.filesToExcludeFromUpload,
        )
      )
    } else None

    val appRunner =
      if (cmdLine.postInstallApps)
        Some(
          new PostInstallAppRunner(
            workspaceSourceRoot,
            obtConfig,
            installDir,
            cmdLine.installVersion,
            logDir = logDir,
            latestCommit = latestCommit,
            bundleClassJars = cmdLine.bundleClassJars,
            useCrumbs = useCrumbs
          )
        )
      else None

    val installer = if (cmdLine.install) {
      Some(
        new Installer(
          scopeConfigSource = obtConfig,
          factory = scopedCompilationFactory,
          installDir = installDir,
          buildDir = buildDir,
          depCopyRoot = depCopyRoot,
          sourceDir = workspaceSourceRoot,
          versionConfig = globalConfig.versionConfig,
          testplanConfig = globalConfig.testplanConfig,
          genericRunnerConfiguration = globalConfig.genericRunnerConfig,
          postInstaller = PostInstaller.merge(assetUploader ++ appRunner),
          gitLog = gitLog,
          installTestplans = cmdLine.generateTestplans,
          directoryFactory = directoryFactory,
          warScopes = scopeSelector.warScopes,
          bundleFingerprintsCache = bundleFingerprintsCache,
          generatePoms = generatePoms,
          useMavenLibs = useMavenLibs,
          minimal = cmdLine.minimalInstall,
          bundleClassJars = cmdLine.bundleClassJars,
          pythonConfiguration = if (globalConfig.installWheels) InstallTpaWithWheels(pythonEnvironment) else InstallTpa,
          externalDependencies = obtConfig.jvmDependencies,
          fingerprintsConfiguration = obtConfig.fingerprintsDiffConfig
        )
      )
    } else None

    val imageBuilders = {
      val timestamp: String = OptimusBuildToolBootstrap.generateLogFilePrefix()
      scopeSelector.imagesToBuild.toIndexedSeq.apar.map { dockerImg =>
        new oci.ImageBuilder(
          scopeConfigSource = obtConfig,
          versionConfig = globalConfig.versionConfig,
          sourceDir = workspaceSourceRoot,
          dockerImage = dockerImg,
          workDir = buildDir.resolveDir("docker").resolveDir(dockerImg.location.repo).resolveDir(timestamp),
          scopedCompilationFactory = scopedCompilationFactory,
          stripDependencies = cmdLine.stripDependencies,
          latestCommit = latestCommit,
          directoryFactory = directoryFactory,
          useMavenLibs = useMavenLibs,
          dockerImageCacheDir =
            buildDir.resolveDir("docker_cache").resolveDir(dockerImg.location.repo).resolveDir(timestamp),
          depCopyDir = depCopyRoot,
          useCrumbs = useCrumbs,
          minimalInstallScopes = scopeSelector.minimalInstallScopes,
          genericRunnerGenerator = DockerGenericRunnerGenerator(installPathBuilder, globalConfig.genericRunnerConfig),
          gitLog = gitLog,
        )
      }
    }

    val cppSourceInstaller = if (cmdLine.installCpp != NoneArg) {
      val targets = cmdLine.installCpp
        .split(",")
        .toSeq
        .apar
        .map { s =>
          val OptimusBuildToolImpl.ScopeIdDir(partialId, dirStr) = s
          partialId -> dirStr
        }
        .toMap
      Some(CppSourceSync(obtConfig, installDir, cmdLine.installVersion, targets))
    } else None

    val scalaSourceCopier = if (cmdLine.copyScalaSources) {
      Some(new GeneratedScalaSourceSync(obtConfig))
    } else None

    val documentationInstaller =
      if (cmdLine.installDocs) {
        val docMetaBundle = MetaBundle.parse(cmdLine.docBundle)
        Some(
          new DocumentationInstaller(
            obtConfig,
            workspaceSourceRoot,
            directoryFactory,
            docMetaBundle,
            installPathBuilder,
            bundleFingerprintsCache
          )
        )
      } else None

    val postBuilder = PostBuilder.merge(
      List(
        installer,
        cppSourceInstaller,
        scalaSourceCopier,
        documentationInstaller,
        recency
      ).flatten ++ imageBuilders ++ extraPostBuilders
    )

    val messageReporter = {
      val reportDir = cmdLine.reportDir
      val codeReviewSettings = if (cmdLine.codeReviewAnalysis) {
        Some(
          CodeReviewSettings(
            dir = reportDir.resolveDir("compilation-analysis"),
            prCommit = cmdLine.prCommit,
            targetCommit = cmdLine.targetCommit
          )
        )
      } else None

      val metadataSettings = if (cmdLine.generateMetadata || generatePoms) {
        Some(
          MetadataSettings(
            stratoConfig = stratoConfig,
            extraLibs = obtConfig.jvmDependencies.allExternalDependencies.definitions.filter(_.isExtraLib),
            webDependencyResolver = webDependencyResolver,
            installDir = installDir,
            dockerDir = dockerDir,
            installVersion = cmdLine.installVersion,
            leafDir = RelativePath(StaticConfig.string("metadataLeaf")),
            buildId = cmdLine.buildId,
            generatePoms = generatePoms,
            images = scopeSelector.imagesToBuild
          )
        )
      } else None

      new MessageReporter(
        obtConfig,
        errorReporter = obtErrorReporter,
        buildDir = buildDir,
        warningsDir = if (cmdLine.warningsReport) Some(reportDir.resolveDir("compilation-warnings")) else None,
        lookupsDir = if (cmdLine.zincTrackLookups) Some(reportDir.resolveDir("compilation-errors")) else None,
        codeReviewSettings = codeReviewSettings,
        metadataSettings = metadataSettings
      )
    }

    new StandardBuilder(
      factory = scopedCompilationFactory,
      scopeFilter = scopeFilter,
      backgroundBuilder = backgroundBuilder,
      defaultPostBuilder = postBuilder,
      onBuildStart = onBuildStart,
      onBuildEnd = onBuildEnd,
      messageReporter = Some(messageReporter),
      assetUploader = assetUploader,
      uploadSources = cmdLine.uploadSources,
      minimalInstallScopes = scopeSelector.minimalInstallScopes
    )
  }

  @entersGraph def close(): Unit = {
    (AsyncUtils.asyncTry {
      directoryFactory.close()
    } thenFinally {
      dependencyTracker.dispose()
    } thenFinally {
      buildCache.close()
    }).run()
  }
}
