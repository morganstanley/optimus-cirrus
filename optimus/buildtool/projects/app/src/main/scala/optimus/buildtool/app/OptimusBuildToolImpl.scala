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

import optimus.buildtool.app.OptimusBuildToolCmdLineT._
import optimus.buildtool.artifacts._
import optimus.buildtool.bsp.BuildServerProtocolServer
import optimus.buildtool.builders._
import optimus.buildtool.builders.postbuilders.DocumentationInstaller
import optimus.buildtool.builders.postbuilders.PostBuilder
import optimus.buildtool.builders.postbuilders.codereview.CodeReviewSettings
import optimus.buildtool.builders.postbuilders.installer.BundleFingerprintsCache
import optimus.buildtool.builders.postbuilders.installer.Installer
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
import optimus.buildtool.cache.silverking.CacheOperationRecorder
import optimus.buildtool.cache.silverking.CacheProvider
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
import optimus.buildtool.compilers.RegexScanner
import optimus.buildtool.compilers.cpp.CppCompilerFactory
import optimus.buildtool.compilers.cpp.CppCompilerFactoryImpl
import optimus.buildtool.compilers.runconfc.Templates
import optimus.buildtool.compilers.zinc.AnalysisLocatorImpl
import optimus.buildtool.compilers.zinc.RootLocatorWriter
import optimus.buildtool.compilers.zinc.ZincAnalysisCache
import optimus.buildtool.compilers.zinc.ZincClassLoaderCaches
import optimus.buildtool.compilers.zinc.ZincClasspathResolver
import optimus.buildtool.compilers.zinc.ZincCompilerFactory
import optimus.buildtool.config.DockerImage
import optimus.buildtool.config.ExternalDependencies
import optimus.buildtool.config.GlobalConfig
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.NamingConventions._
import optimus.buildtool.config.ObtConfig
import optimus.buildtool.config.RunConfConfiguration
import optimus.buildtool.config.ScalaVersionConfig
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.StaticConfig
import optimus.buildtool.config.StaticLibraryConfig.scalaJarNamesForZinc
import optimus.buildtool.config.StratoConfig
import optimus.buildtool.config.VersionConfiguration
import optimus.buildtool.files.Directory.Not
import optimus.buildtool.files._
import optimus.buildtool.generators.CppBridgeGenerator
import optimus.buildtool.generators.FlatbufferGenerator
import optimus.buildtool.generators.JaxbGenerator
import optimus.buildtool.generators.JsonSchemaGenerator
import optimus.buildtool.generators.JxbGenerator
import optimus.buildtool.generators.ProtobufGenerator
import optimus.buildtool.generators.ScalaxbGenerator
import optimus.buildtool.generators.ZincGenerator
import optimus.buildtool.processors.DeploymentScriptProcessor
import optimus.buildtool.processors.FreemarkerProcessor
import optimus.buildtool.processors.OpenApiProcessor
import optimus.buildtool.processors.VelocityProcessor
import optimus.buildtool.resolvers.CoursierArtifactResolver
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
import optimus.stratosphere.artifactory.ArtifactoryToolDownloader
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
import scala.collection.immutable.Seq
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
  private val dockerImageCacheDir = buildDir.resolveDir("docker_cache")

  private val traceRecorder = new TraceRecorder(cmdLine.traceFile.asDirectory)
  private val timingsRecorder = new TimingsRecorder(logDir)
  val countingTrace = new CountingTrace(Some(cmdLine.statusIntervalSec))
  private val longRunningTraceListener = new LongRunningTraceListener(useCrumbs = useCrumbs)
  private val buildSummaryRecorder = new BuildSummaryRecorder(cmdLine.statsDir.asDirectory)

  private val memoryThrottle = MemoryThrottle.fromConfigStringOrSysProp(cmdLine.memConfig.nonEmptyOption)
  private val baseListeners = timingsRecorder :: traceRecorder :: countingTrace :: longRunningTraceListener ::
    buildSummaryRecorder :: memoryThrottle.toList

  private val uploadLocations: Seq[UploadLocation] =
    cmdLine.uploadLocations.toIndexedSeq.map(UploadLocation(_, cmdLine.decompressAfterUpload))

  private val dockerDir = cmdLine.dockerDir.asDirectory.getOrElse(workspaceRoot.resolveDir("docker-out"))

  private val cacheFlushTimeout = 60000 // ms

  private val zincInstanceThrottle =
    if (cmdLine.maxNumZincs > 0) Some(new AdvancedUtils.Throttle(cmdLine.maxNumZincs))
    else None

  private val zincSizeThrottle =
    if (cmdLine.maxZincCompileBytes > 0) Some(new AdvancedUtils.Throttle(cmdLine.maxZincCompileBytes))
    else None

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

  log.info(Utils.LogSeparator)
  log.info(s"Hostname: ${Utils.hostName.getOrElse("unknown")}")
  log.info(s"Process ID: ${Utils.processId.getOrElse("unknown")}")
  log.info(s"User: ${sys.props("user.name")}")
  log.info(s"OS version: ${OsUtils.osVersion}")
  log.info(s"OBT app directory: ${sys.env.getOrElse("APP_DIR", "unknown")}")
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

  // intentionally sending this to debug as this is a bit verbose for users to see
  private def optimusSysProps = sys.props.collect { case (k, v) if k.toLowerCase.contains("optimus") => s"$k=$v" }
  log.debug(s"Optimus System Props: ${optimusSysProps.mkString(", ")}")

  if (uploadLocations.nonEmpty) log.info(s"Upload locations: ${uploadLocations.mkString(", ")}")

  private val depCopyRoot = cmdLine.depCopyDir.asDirectory.getOrElse {
    workspaceRoot.parent.resolveDir(".stratosphere").resolveDir("depcopy")
  }
  val venvCacheDir: Directory = depCopyRoot.resolveDir("venv-cache")
  val pipCacheDir: Directory = depCopyRoot.resolveDir("pip-cache")

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
    gitUtils.map(u => GitLog(u, workspaceSourceRoot, stratoWorkspace(), cmdLine.gitFilterRe, cmdLine.gitLength))

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

  private val cacheOperationRecorder = new CacheOperationRecorder

  @node @scenarioIndependent private[buildtool] def remoteCaches =
    CacheProvider(cmdLine, version, pathBuilder, stratoWorkspace(), cacheOperationRecorder)

  @node @scenarioIndependent private def combined(
      caches: Seq[ArtifactCache with HasArtifactStore]
  ): Option[ArtifactCache with HasArtifactStore] = {
    if (caches.size > 1) Some(MultiLevelCache(caches.toSeq: _*))
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

  @node private def scalaPath: Directory = cmdLine.scalaDir.asDirectory.getOrElse(globalConfig.scalaPath)

  @node private def scalaLibPath: ReactiveDirectory =
    directoryFactory.reactive(scalaPath.resolveDir("lib"))

  @node private def scalaJars: Seq[JarAsset] = {
    def isScalaJar(f: FileAsset) = {
      val name = f.name
      name.endsWith(".jar") && !name.endsWith(".src.jar") && scalaJarNamesForZinc.exists(name.startsWith)
    }
    scalaLibPath.listFiles.filter(isScalaJar).map(_.asJar)
  }

  @node private def dependencyMetadataResolvers: DependencyMetadataResolvers = obtConfig.depMetadataResolvers

  @node def scalaVersionConfig: ScalaVersionConfig =
    ScalaVersionConfig(globalConfig.scalaVersion, scalaLibPath, scalaJars)

  @node @scenarioIndependent protected def rubbishTidyer: Option[RubbishTidyer] =
    cmdLine.maxBuildDirSize.map { maxSizeMegabytes =>
      val gitLog =
        gitUtils.map(u => GitLog(u, workspaceSourceRoot, stratoWorkspace(), commitLength = cmdLine.gitPinDepth))
      val freeDiskSpaceTriggerBytes = cmdLine.freeDiskSpaceTriggerMb.map(_.toLong << 20L)
      new RubbishTidyerImpl(maxSizeMegabytes.toLong << 20L, freeDiskSpaceTriggerBytes, buildDir, sandboxDir, gitLog)
    }

  @node @scenarioIndependent private def zincAnalysisCache =
    new ZincAnalysisCache(cmdLine.zincAnalysisCacheSize, instrumentation)

  @node private def zincResolver: ZincClasspathResolver = new ZincClasspathResolver(
    workspaceRoot,
    dependencyResolver,
    dependencyCopier,
    obtConfig.externalDependencies,
    scalaVersionConfig.scalaMajorVersion
  )

  @node private def underlyingCompilerFactory: ZincCompilerFactory = {
    val interfaceDir = cmdLine.zincInterfaceDir.asDirectory.getOrElse {
      buildDir.resolveDir("zincCompilerInterface")
    }

    ZincCompilerFactory(
      jdkPath = globalConfig.javaPath,
      scalaConfig = scalaVersionConfig,
      zincLocator = zincResolver,
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
      remoteArtifactWriter =
        combined(remoteCaches.remoteBuildCache.to(Seq) ++ remoteCaches.remoteBuildDualWriteCache.to(Seq)).map(_.store),
      classLoaderCaches = ZincClassLoaderCaches,
      scalacProfileDir = scalacProfileDir,
      strictErrorTolerance = cmdLine.zincStrictMapping,
      zincTrackLookups = cmdLine.zincTrackLookups,
      depCopyFileSystemAsset = depCopyFileSystemAsset,
      instanceThrottle = zincInstanceThrottle,
      sizeThrottle = zincSizeThrottle
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
      regexScanner,
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
    if (cmdLine.credentialFiles == NoneArg) {
      val ws = stratoWorkspace()
      def existCredentialFile: Option[Credential] = Credential.fromJfrogConfFile(ws.internal.jfrog.configFile)
      existCredentialFile match {
        case Some(file) => Seq(file)
        case None =>
          log.warn(s"No credential files defined! obt will try generate one now")
          ArtifactoryToolDownloader.presetArtifactoryCredentials(ws)
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

  // This is used for maven url file cache into silverking.
  @node @scenarioIndependent private def remoteAssetStore: RemoteAssetStore =
    remoteCaches.remoteBuildCache.map(_.store).getOrElse(NoOpRemoteAssetStore)

  @node @scenarioIndependent private def dependencyCopier = {
    DependencyCopier(depCopyRoot, credentials, remoteAssetStore, depCopyFileSystemAsset)
  }

  // This hasher is only used for the obt configuration files.
  @node @scenarioIndependent private def rootFingerprintHasher =
    FingerprintHasher(ScopeId.RootScopeId, pathBuilder, fsCache.store, None, mischief = false)

  @node def dependencyResolver: CoursierArtifactResolver = externalDependencyResolver(dependencyMetadataResolvers)

  @node private def validateCredentials(
      loaded: Seq[Credential],
      externalDependencies: ExternalDependencies): Seq[Credential] = {
    val mavenLibs = externalDependencies.mavenDependencies.allMavenDeps
    val withMavenLibs = mavenLibs.nonEmpty
    val withRemoteCache = remoteCaches.remoteBuildCache.nonEmpty
    val withLocalCache =
      Files.exists(depCopyRoot.resolveDir("https").path) || Files.exists(depCopyRoot.resolveDir("http").path)

    if (withMavenLibs && loaded.isEmpty && !withRemoteCache && !withLocalCache)
      throw new IllegalArgumentException(
        s"Build failed: no credential and cache available, unable to download ${mavenLibs.size} maven libraries!")
    else if (withMavenLibs && loaded.isEmpty && (withRemoteCache || withLocalCache))
      log.warn(
        s"Maven server offline mode: no credential found! maven libs: ${mavenLibs.size}, local cache: $withLocalCache, remote cache: $withRemoteCache")
    loaded
  }

  @node private def externalDependencyResolver(resolvers: DependencyMetadataResolvers): CoursierArtifactResolver =
    CoursierArtifactResolver(
      resolvers = resolvers,
      externalDependencies = obtConfig.externalDependencies,
      dependencyCopier = dependencyCopier,
      globalExcludes = obtConfig.globalExcludes,
      globalSubstitutions = obtConfig.globalSubstitutions,
      credentials = validateCredentials(credentials, obtConfig.externalDependencies),
      remoteAssetStore = remoteAssetStore,
      enableMappingValidation = enableMappingValidation
    )

  @node private def webDependencyResolver: WebDependencyResolver = {
    val webScopes = obtConfig.scopeDefinitions.collect {
      case (id, definition) if definition.configuration.webConfig.isDefined => id -> definition.configuration
    }
    val webInfoBeforeBuild: Map[ScopeId, WebScopeInfo] =
      WebDependencyResolver.resolveWebInfo(webScopes, obtConfig.externalDependencies.definitions)
    WebDependencyResolver(webInfoBeforeBuild)
  }

  @node private def sourceGenerators =
    Seq(
      CppBridgeGenerator(scalaCompiler),
      FlatbufferGenerator(workspaceSourceRoot),
      JaxbGenerator(directoryFactory, workspaceSourceRoot),
      JxbGenerator(workspaceSourceRoot),
      JsonSchemaGenerator(directoryFactory, workspaceRoot),
      ProtobufGenerator(workspaceSourceRoot),
      ScalaxbGenerator(workspaceSourceRoot),
      ZincGenerator(zincResolver, depCopyRoot)
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
  @node @scenarioIndependent private def jmhCompiler = AsyncJmhCompilerImpl(sandboxFactory)
  @node @scenarioIndependent private def cppCompiler =
    AsyncCppCompilerImpl(cppCompilerFactory, sandboxFactory, requiredCppOsVersions.toSet)
  @node @scenarioIndependent private def pythonCompiler =
    AsyncPythonCompilerImpl(
      pipCacheDir,
      venvCacheDir,
      sandboxFactory,
      logDir,
      cmdLine.pypiCredentials,
      asNode(() => globalConfig.pythonEnabled))
  @node @scenarioIndependent private def webCompiler =
    AsyncWebCompilerImpl(depCopyRoot.resolveDir("pnpm-store"), sandboxFactory, logDir, useCrumbs)
  @node @scenarioIndependent private def electronCompiler =
    AsyncElectronCompilerImpl(depCopyRoot.resolveDir("pnpm-store"), sandboxFactory, logDir, useCrumbs)
  @node private def stratoConfig = StratoConfig.load(directoryFactory, workspaceSourceRoot)
  @node private def runconfCompiler =
    AsyncRunConfCompilerImpl.load(obtConfig, scalaPath, stratoConfig, buildDir)
  @node @scenarioIndependent private def resourcePackager = JarPackager()
  @node @scenarioIndependent private def regexScanner = RegexScanner()
  @node @scenarioIndependent private def genericFilesPackager = GenericFilesPackager(cmdLine.installVersion)

  @node private def freezerCache = FreezerCache(FreezerStoreConf.load(mischiefOpts), pathBuilder)

  @node @scenarioIndependent private def fsCache =
    FilesystemCache(pathBuilder, cmdLine.incrementalMode)

  @node private def localStore = MultiLevelStore(freezerCache.store, fsCache.store)

  @node private def buildCache = {
    val caches = remoteCaches.remoteBuildCache match {
      case Some(remoteCache) =>
        if (cmdLine.remoteCacheWritable) {
          // If we've enabled silverking writes, then write to SK even if we've found it in our
          // filesystem cache. Among other benefits, this works around a bug that can cause artifacts not to
          // be written to SK if they've been written to the filesystem as a side-effect of compiling for a different
          // artifact type.
          Seq(
            remoteCaches.remoteBuildDualWriteCache,
            remoteCaches.writeOnlyRemoteBuildCache,
            Some(fsCache),
            remoteCaches.readOnlyRemoteBuildCache).flatten
        } else Seq(fsCache, remoteCache)
      case None =>
        Seq(fsCache)
    }
    combined(freezerCache +: caches).get
  }

  @node @scenarioIndependent private def rootLocatorWriter(
      git: GitLog,
      writerCache: HasArtifactStore): RootLocatorWriter =
    new RootLocatorWriter(git, pathBuilder, writerCache.store)

  @node private[buildtool] def scopesToInclude = {
    if (cmdLine.scopesToBuild == Set(NoneArg))
      Set.empty[ScopeId]
    else if (cmdLine.scopesToBuild == Set(AllArg))
      obtConfig.compilationScopeIds
    else if (cmdLine.scopesToBuild.isEmpty && cmdLine.imagesToBuild.isEmpty && cmdLine.warScopes.isEmpty)
      obtConfig.compilationScopeIds.apar.filter(obtConfig.local(_))
    else
      cmdLine.scopesToBuild.apar.flatMap(obtConfig.resolveScopes(_)) ++ scopesFromImages ++ warScopes
  }

  @node private[buildtool] def minimalInstallScopes: Option[Set[ScopeId]] =
    if (cmdLine.minimalInstall)
      Some(scopesToInclude -- scopesFromImages ++ predefinedDockerImages.flatten(_.directScopeIds))
    else None

  @node private[buildtool] def scopes: Set[ScopeId] = {
    val scopesToExclude = cmdLine.scopesToExclude.apar.flatMap(obtConfig.resolveScopes(_))
    scopesToInclude -- scopesToExclude
  }

  @node private def scopesFromImages: Set[ScopeId] = predefinedDockerImages.flatten(_.relevantScopeIds)

  @node private def predefinedDockerImages: Set[DockerImage] =
    if (cmdLine.imagesToBuild.nonEmpty) {
      obtConfig.parseImages(dockerDir, cmdLine.imagesToBuild, cmdLine.imageTag)
    } else Set.empty

  @node private def warScopes = cmdLine.warScopes.apar.flatMap(obtConfig.resolveScopes(_))

  @node @scenarioIndependent private def scalacProfileDir: Option[Directory] =
    if (cmdLine.profileScalac) {
      val dirPath = pathBuilder.outputDir.resolveDir("scalac-profile")
      Files.createDirectories(dirPath.path)
      Some(dirPath)
    } else None

  private[buildtool] val dependencyTracker = DependencyTrackerRoot()

  private[buildtool] val scopedCompilationFactory: ScopedCompilationFactory = ScopedCompilationFactoryImpl()
  @entity private[buildtool] class ScopedCompilationFactoryImpl() extends CompilationNodeFactory {
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
          externalDependencyResolver = dependencyResolver,
          cache = buildCache,
          factory = this,
          directoryFactory = directoryFactory,
          mischief = mischiefOpts.mischief(id)
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
            runconfc = runconfCompiler,
            jarPackager = resourcePackager,
            regexScanner = regexScanner,
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
      warScopes = warScopes,
      bundleFingerprintsCache = bundleFingerprintsCache,
      generatePoms = generatePoms
    )
  }

  @entersGraph def start(): Boolean = {
    if (cmdLine.clean) {
      if (cmdLine.scopesToBuild.isEmpty) {
        log.info(s"Cleaning $buildDir...")
        AssetUtils.recursivelyDelete(buildDir)
      } else {
        val snippets: Set[String] = scopes.map(_.properPath)
        if (snippets.nonEmpty) {
          log.info(s"Cleaning $buildDir contents matching ${snippets.mkString("|")}")
          AssetUtils.recursivelyDelete(buildDir, d => snippets.exists(d.getFileName.toString.contains))
        }
      }
    }

    RubbishTidyer.tidyLogs(logDir.path, cmdLine.maxLogAge)
    if (cmdLine.bspServer) {
      bspServer.runServer()
    } else if (cmdLine.interactive) {
      buildInteractive()
    } else {
      rubbishTidyer.foreach(_.tidy())
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

    @entersGraph @tailrec def buildLoop(scopesToBuild: Set[ScopeId]): Boolean = {
      val buildResult = asyncResult(build(builder, buildCount.incrementAndGet(), scopesToBuild)._1) valueOrElse {
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
        (autoQueue.take() +: { Thread.sleep(500); autoQueue.pollAll() }).flatten.toSet - ""

      if (requestedScopes contains "q") { // quit requested
        buildResult // stop looping and return
      } else
        buildLoop {
          if (requestedScopes.nonEmpty) { // update scopes
            // run in tracker to pick up potentially-changed obt files
            dependencyTracker.executeEvaluate { () =>
              NodeTry {
                requestedScopes.apar.flatMap(obtConfig.resolveScopes(_))
              }.getOrRecover { case e: IllegalArgumentException =>
                log.error(e.getMessage)
                Set.empty
              }
            }
          } else scopesToBuild
        }
    }

    buildLoop(scopes)
  }
  @entersGraph def build(): (Boolean, Option[BuildStatistics]) = {
    val scopesToBuild = ObtTrace.withListeners(baseListeners) { scopes }
    build(standardBuilder(), 0, scopesToBuild)
  }

  @async private def build(
      builder: Builder,
      buildCount: Int,
      scopes: Set[ScopeId]
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
          val modifiedFiles = if (cmdLine.gitAwareMessages) gitLog.flatMap(_.modifiedFiles()) else None
          val result = builder.build(scopes, modifiedFiles = modifiedFiles)
          success = result.successful
          if (success && cmdLine.writeRootLocator) {
            require(cmdLine.remoteCacheWritable, "You must use --remoteCacheWritable")
            writeRootLocator(
              remoteCaches.remoteBuildCache,
              remoteCaches.remoteRootLocatorCache,
              "write",
              cmdLine.gitTag)
            writeRootLocator(
              remoteCaches.remoteBuildDualWriteCache,
              remoteCaches.remoteRootLocatorDualWriteCache,
              "dual-write"
            )
          }
        }
      } thenFinally {
        localStore.flush(cacheFlushTimeout)
      } thenFinally {
        if (success && cmdLine.writeRootLocator)
          (remoteCaches.remoteRootLocatorCache ++ remoteCaches.remoteRootLocatorDualWriteCache).foreach(
            _.store.flush(cacheFlushTimeout)
          )
      } asyncFinally {
        // note that endBuild may have side effects so we always call it
        traceOk = ObtTrace.endBuild(success) || !cmdLine.failOnAnomalousTrace
        if (!traceOk) log.error("BUILD FAILED due to anomalies detected in trace")
      }
    }

    (success && traceOk, crumbListener.map(_.buildStatistics))
  }

  @async private def writeRootLocator(
      buildCache: Option[CacheProvider.RemoteArtifactCache],
      rootLocatorCache: Option[HasArtifactStore],
      writeType: String,
      tagName: Option[String] = None
  ): Unit =
    for {
      bc <- buildCache
      rlc <- rootLocatorCache
      git <- gitLog
    } {
      if (bc.store.incompleteWrites == 0) {
        rootLocatorWriter(git, rlc).writeRootLocator(version)
        tagName.foreach(tag => git.reportTagMovingForward(tag, installDir))
      } else {
        log.warn(s"Skipping root locator $writeType due to ${bc.store.incompleteWrites} incomplete writes")
        tagName.foreach { tag =>
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
    if (sys.props.get("optimus.buildtool.showAllWarnings").contains("true"))
      Some(TraceFilter.AllWarningsFilter) // override
    else TraceFilter.parse(sys.props.get("optimus.buildtool.showWarnings"))

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
      scalaVersionConfig = asNode(() => scalaVersionConfig),
      pythonEnabled = asNode(() => globalConfig.pythonEnabled),
      extractVenvs = asNode(() => globalConfig.extractVenvs),
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
          depCopyRoot = depCopyRoot,
          sourceDir = workspaceSourceRoot,
          versionConfig = globalConfig.versionConfig,
          testplanConfig = globalConfig.testplanConfig,
          genericRunnerConfiguration = globalConfig.genericRunnerConfig,
          postInstaller = PostInstaller.merge(assetUploader ++ appRunner),
          gitLog = gitLog,
          installTestplans = cmdLine.generateTestplans,
          directoryFactory = directoryFactory,
          warScopes = warScopes,
          bundleFingerprintsCache = bundleFingerprintsCache,
          generatePoms = generatePoms,
          minimal = cmdLine.minimalInstall,
          bundleClassJars = cmdLine.bundleClassJars
        )
      )
    } else None

    val imageBuilders = {
      predefinedDockerImages.toIndexedSeq.apar.map { dockerImg =>
        new oci.ImageBuilder(
          scopeConfigSource = obtConfig,
          versionConfig = globalConfig.versionConfig,
          sourceDir = workspaceSourceRoot,
          dockerImage = dockerImg,
          workDir = buildDir.resolveDir("docker"),
          scopedCompilationFactory = scopedCompilationFactory,
          stripDependencies = cmdLine.stripDependencies,
          latestCommit = latestCommit,
          directoryFactory = directoryFactory,
          useMavenLibs = useMavenLibs,
          dockerImageCacheDir = dockerImageCacheDir,
          depCopyDir = depCopyRoot,
          useCrumbs = useCrumbs,
          minimalInstallScopes = minimalInstallScopes
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
            dependencyResolver = dependencyResolver,
            webDependencyResolver = webDependencyResolver,
            installDir = installDir,
            installVersion = cmdLine.installVersion,
            leafDir = RelativePath(StaticConfig.string("metadataLeaf")),
            buildId = cmdLine.buildId,
            generatePoms = generatePoms
          )
        )
      } else None

      new MessageReporter(
        obtConfig,
        errorReporter = obtErrorReporter,
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
      minimalInstallScopes = minimalInstallScopes
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
