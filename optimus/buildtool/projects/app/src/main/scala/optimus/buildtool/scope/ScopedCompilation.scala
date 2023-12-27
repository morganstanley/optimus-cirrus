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
package optimus.buildtool.scope

import optimus.buildtool.app.IncrementalMode
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.ArtifactType.PathingFingerprint
import optimus.buildtool.artifacts.Artifacts
import optimus.buildtool.artifacts.ClassFileArtifact
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.InMemoryMessagesArtifact
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.InternalClassFileArtifact
import optimus.buildtool.artifacts.MessagesArtifact
import optimus.buildtool.artifacts.PathingArtifact
import optimus.buildtool.artifacts.{ArtifactType => AT}
import optimus.buildtool.compilers.AsyncClassFileCompiler
import optimus.buildtool.compilers.AsyncCppCompiler
import optimus.buildtool.compilers.AsyncCppCompiler.BuildType
import optimus.buildtool.compilers.AsyncElectronCompiler
import optimus.buildtool.compilers.AsyncJmhCompiler
import optimus.buildtool.compilers.AsyncPythonCompiler
import optimus.buildtool.compilers.AsyncRunConfCompiler
import optimus.buildtool.compilers.AsyncSignaturesCompiler
import optimus.buildtool.compilers.AsyncWebCompiler
import optimus.buildtool.compilers.GenericFilesPackager
import optimus.buildtool.compilers.JarPackager
import optimus.buildtool.compilers.RegexScanner
import optimus.buildtool.compilers.cpp.CppLibrary
import optimus.buildtool.compilers.cpp.CppUtils
import optimus.buildtool.compilers.zinc.AnalysisLocator
import optimus.buildtool.config._
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.generators.GeneratorType
import optimus.buildtool.generators.SourceGenerator
import optimus.buildtool.processors.ProcessorType
import optimus.buildtool.processors.ScopeProcessor
import optimus.buildtool.runconf.RunConf
import optimus.buildtool.scope.partial.ArchivePackaging
import optimus.buildtool.scope.partial.CppScopedCompilation
import optimus.buildtool.scope.partial.ElectronScopedCompilation
import optimus.buildtool.scope.partial.GenericFilesScopedCompilation
import optimus.buildtool.scope.partial.JavaScopedCompilation
import optimus.buildtool.scope.partial.JmhScopedCompilation
import optimus.buildtool.scope.partial.PythonScopedCompilation
import optimus.buildtool.scope.partial.RegexMessagesScopedCompilation
import optimus.buildtool.scope.partial.ResourcePackaging
import optimus.buildtool.scope.partial.RunconfAppScopedCompilation
import optimus.buildtool.scope.partial.ScalaCompilationInputs
import optimus.buildtool.scope.partial.ScalaScopedCompilation
import optimus.buildtool.scope.partial.SignatureScopedCompilation
import optimus.buildtool.scope.partial.SourcePackaging
import optimus.buildtool.scope.partial.WebResourcePackaging
import optimus.buildtool.scope.sources.GenericFilesCompilationSources
import optimus.buildtool.scope.sources.JavaAndScalaCompilationSources
import optimus.buildtool.scope.sources.RegexMessagesCompilationSources
import optimus.buildtool.scope.sources.ResourceCompilationSourcesImpl
import optimus.buildtool.scope.sources.RunconfCompilationSources
import optimus.buildtool.scope.sources.SourceCompilationSources
import optimus.buildtool.scope.sources.ArchivePackageSources
import optimus.buildtool.scope.sources.CppCompilationSources
import optimus.buildtool.scope.sources.ElectronCompilationSources
import optimus.buildtool.scope.sources.PythonCompilationSources
import optimus.buildtool.scope.sources.WebCompilationSources
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.Pathing
import optimus.buildtool.trace.Validation
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.JarUtils
import optimus.buildtool.utils.Jars
import optimus.buildtool.utils.OsUtils
import optimus.buildtool.utils.Utils.distinctLast
import optimus.core.needsPlugin
import optimus.platform._
import optimus.platform.annotations.alwaysAutoAsyncArgs

import scala.collection.immutable.Seq
import scala.collection.compat._

/**
 * An artifact source which generates artifacts by compiling folders of source code (after first compiling or retrieving
 * required upstream artifacts). This is the main expression of the "compilation graph" within OBT.
 *
 * A key design principle is that although many of the @node methods have external side effects (mostly writing files)
 * these are all idempotent and independent. This is achieved by including hashes of the node dependencies in the output
 * file names, and ensuring that all writes are atomic (this is mostly done by the compiler wrappers that we call). The
 * consequence is that if we get a node hit in the graph cache, the corresponding side effect in the filesystem is known
 * to already be present and unchanged (unless the user deliberately messes with the build directory).
 */
trait ScopedCompilation {
  def id: ScopeId
  def config: ScopeConfiguration
  def allCompileDependencies: Seq[ScopeDependencies]
  def runtimeDependencies: ScopeDependencies

  @node def runConfigurations: Seq[RunConf]
  @node def runconfArtifacts: Seq[Artifact]

  @node def runtimeArtifacts: Artifacts
  @node def allArtifacts: Artifacts
  @node def bundlePathingArtifacts(compiledArtifacts: Seq[Artifact]): Seq[Artifact]
}

/**
 * A ScopedCompilation which participates as a node in the "compilation graph". Methods on CompilationNode are intended
 * only for use by other CompilationNodes, in order to recursively construct the full set of artifacts for a particular
 * compilation or runtime.
 */
trait CompilationNode extends ScopedCompilation {
  // These (and the methods on ScopedCompilation) are the only methods visible outside of a ScopedCompilationImpl
  // (even to other ScopedCompilationImpls) since in practice nobody gets hold of a ScopedCompilationImpl directly.
  // Ideally we would mark the other methods on ScopedCompilationImpl as private[this] but you can't do that with
  // @nodes.
  private[scope] def upstream: UpstreamArtifacts
  @node private[buildtool] def signaturesForDownstreamCompilers: Seq[Artifact]
  @node private[buildtool] def classesForDownstreamCompilers: Seq[Artifact]
  @node private[buildtool] def pluginsForDownstreamCompilers: Seq[Artifact]
  @node private[buildtool] def cppForDownstreamCompilers: Seq[Artifact]
  @node private[buildtool] def artifactsForDownstreamRuntimes: Seq[Artifact]
  @node private[buildtool] def scopeMessages: MessagesArtifact
}

@entity private[buildtool] class ScopedCompilationImpl(
    scope: CompilationScope,
    sources: JavaAndScalaCompilationSources,
    sourcePackaging: SourcePackaging,
    signatures: SignatureScopedCompilation,
    scala: ScalaScopedCompilation,
    java: JavaScopedCompilation,
    cpp: CppScopedCompilation,
    python: PythonScopedCompilation,
    web: WebResourcePackaging,
    electron: ElectronScopedCompilation,
    resources: ResourcePackaging,
    packaging: ArchivePackaging,
    jmh: JmhScopedCompilation,
    runconf: RunconfAppScopedCompilation,
    genericFiles: GenericFilesScopedCompilation,
    regexMessages: RegexMessagesScopedCompilation,
    processing: ScopeProcessing,
    strictEmptySources: Boolean
) extends CompilationNode {

  override def id: ScopeId = scope.id
  override def config: ScopeConfiguration = scope.config
  override private[scope] def upstream = scope.upstream

  override def allCompileDependencies: Seq[ScopeDependencies] = upstream.allCompileDependencies
  override def runtimeDependencies: ScopeDependencies = upstream.runtimeDependencies
  override def toString: String = s"ScopedCompilation($id)"

  @node private[buildtool] def signaturesForDownstreamCompilers: Seq[Artifact] = distinctArtifacts() {
    // if we have macros (or we've disabled pipelining), any downstream compilers need our jars
    // and analysis
    if (config.containsMacros || !config.usePipelining) {
      classesForDownstreamCompilers
    }
    // otherwise they just need our signatures plus our own typing dependencies (note that scalac produces java signatures too)
    else {
      val (ourArtifacts, upstreamSignatures) =
        apar(
          signatureErrorsOr(signatures.javaAndScalaSignatures ++ signatures.messages ++ signatures.analysis),
          upstream.signaturesForDownstreamCompilers
        )
      log.debug(s"[$id] Returning ${ourArtifacts.size} signature artifacts for downstreams: $ourArtifacts")
      ourArtifacts ++ upstreamSignatures
    }
  }

  @node private[buildtool] def classesForDownstreamCompilers: Seq[Artifact] = distinctArtifacts() {
    val (ourArtifacts, theirArtifacts, relevantResources) = apar(
      if (config.usePipelining) signatureErrorsOr(signatures.analysis ++ ourClasses)
      else ourClasses ++ scala.analysis ++ java.analysis,
      upstream.classesForDownstreamCompilers,
      resources.relevantResourcesForDownstreams
    )

    log.debug(s"[$id] Returning ${ourArtifacts.size} class artifacts for downstreams: $ourArtifacts")

    // if we have macros, we re-package our upstream artifacts to reflect this so that
    // downstream compilers know to treat them specially (eg. by putting them on the macro classpath)
    val upstreamArtifacts = if (config.containsMacros) {
      theirArtifacts.apar.map {
        case c: ClassFileArtifact => c.copy(containsOrUsedByMacros = true)
        case a                    => a
      }
    } else theirArtifacts

    ourArtifacts ++ relevantResources ++ upstreamArtifacts
  }

  @node override private[buildtool] def pluginsForDownstreamCompilers: Seq[Artifact] = {
    val (ourArtifacts, upstreamPlugins) = apar(
      if (config.containsPlugin) ourClasses else Nil,
      upstream.pluginsForOurCompiler
    )
    log.debug(s"[$id] Returning ${ourArtifacts.size} plugin artifacts for downstreams: $ourArtifacts")
    ourArtifacts ++ upstreamPlugins
  }

  @node private def ourClasses: Seq[Artifact] = scala.classes ++ scala.messages ++ java.classes ++ java.messages

  @node def runconfArtifacts: Seq[Artifact] = distinctArtifacts() {
    runconf.runConfArtifacts
  }

  @node private[buildtool] def cppForDownstreamCompilers: Seq[Artifact] = distinctArtifacts() {
    val (ourArtifacts, upstreamCpp) = apar(
      cpp.artifacts,
      upstream.cppForOurCompiler
    )

    log.debug(s"[$id] Returning ${ourArtifacts.size} cpp artifacts for downstreams: $ourArtifacts")
    ourArtifacts ++ upstreamCpp
  }

  @node private[buildtool] def artifactsForDownstreamRuntimes: Seq[Artifact] = distinctArtifacts(track = true) {
    val (ourArtifacts, theirArtifacts) = apar(signatureErrorsOr(ourRuntimeArtifacts), upstream.artifactsForOurRuntime)
    log.debug(s"[$id] Returning ${ourArtifacts.size} runtime artifacts for downstreams: $ourArtifacts")
    ourArtifacts ++ theirArtifacts
  }

  @node def runtimeArtifacts: Artifacts = distinct(track = true) {
    val (ourArtifacts, theirArtifacts) =
      apar(signatureErrorsOr(pathingArtifact +: ourRuntimeArtifacts), upstream.artifactsForOurRuntime)

    log.debug(s"[$id] Returning ${ourArtifacts.size} runtime artifacts: $ourArtifacts")
    Artifacts(ourArtifacts, theirArtifacts)
  }

  @node private def pathingArtifact: PathingArtifact =
    buildPathingArtifact(ourRuntimeArtifacts ++ upstream.artifactsForOurRuntime)

  @node def allArtifacts: Artifacts = distinct(track = true) {
    // we include the compile and runtime dependencies artifacts because these may contain errors about resolution
    val (ourArtifacts, theirArtifacts) =
      apar(
        signatureErrorsOr {
          Seq(pathingArtifact) ++
            ourRuntimeArtifacts ++ {
              if (config.usePipelining) signatures.javaAndScalaSignatures ++ signatures.messages ++ signatures.analysis
              else Nil
            } ++ scala.analysis ++ scala.locator ++ java.analysis ++ java.locator ++
            processing.process(pathingArtifact)
        } ++
          cpp.artifacts ++
          python.artifacts ++
          web.artifacts ++
          electron.artifacts ++
          sources.fingerprintArtifact ++
          sourcePackaging.packagedSources ++
          // always copy generated sources and regex messages (among others) so that we can make use of
          // them even if we've got signature errors
          sources.generatedSourceArtifacts ++
          regexMessages.messages ++
          sources.externalCompileDependencies :+ scopeMessages :+
          runtimeDependencies.transitiveExternalDependencies,
        upstream.allUpstreamArtifacts
      )
    log.debug(s"[$id] Returning ${ourArtifacts.size} total artifacts: $ourArtifacts")
    Artifacts(ourArtifacts, theirArtifacts)
  }

  @node def bundlePathingArtifacts(compiledArtifacts: Seq[Artifact]): Seq[Artifact] = if (config.pathingBundle) {
    val scopesForBundle = upstream.runtimeDependencies.transitiveScopeDependencies.map(_.id).toSet + scope.id
    val artifactsForBundle = compiledArtifacts.collect {
      case a @ InternalClassFileArtifact(id, _) if scopesForBundle.contains(id.scopeId) => a
    }
    Seq(buildPathingArtifact(artifactsForBundle))
  } else Nil

  // noinspection ScalaUnusedSymbol
  @alwaysAutoAsyncArgs
  private def distinct(track: Boolean = false)(f: => Artifacts): Artifacts = needsPlugin
  // noinspection ScalaUnusedSymbol
  @node private def distinct$NF(track: Boolean = false)(f: NodeFunction0[Artifacts]): Artifacts = {
    import optimus.platform.{track => doTrack}
    distinctLast(if (track) doTrack(f()) else f())
  }

  // noinspection ScalaUnusedSymbol
  @alwaysAutoAsyncArgs
  private def distinctArtifacts(track: Boolean = false)(f: => Seq[Artifact]): Seq[Artifact] = needsPlugin
  // noinspection ScalaUnusedSymbol
  @node private def distinctArtifacts$NF(track: Boolean = false)(f: NodeFunction0[Seq[Artifact]]): Seq[Artifact] = {
    import optimus.platform.{track => doTrack}
    distinctLast(if (track) doTrack(f()) else f())
  }

  // noinspection ScalaUnusedSymbol
  // All artifact methods (directly or otherwise) depends on successful signatures, so short-circuit if we have
  // signature errors (which also includes errors from signature upstreams).
  @alwaysAutoAsyncArgs
  private def signatureErrorsOr(res: => Seq[Artifact]): Seq[Artifact] = needsPlugin
  // noinspection ScalaUnusedSymbol
  @node private def signatureErrorsOr$NF(res: NodeFunction0[Seq[Artifact]]): Seq[Artifact] =
    if (config.usePipelining) Artifact.onlyErrors(signatures.messages) getOrElse res()
    else
      res()

  @node private def ourRuntimeArtifacts: Seq[Artifact] =
    scala.classes ++ scala.messages ++
      java.classes ++ java.messages ++
      resources.resources ++ packaging.archiveContents ++
      jmh.classes ++ jmh.messages ++
      cpp.artifacts ++ web.artifacts ++ electron.artifacts ++
      runconf.runConfArtifacts ++ runconf.messages ++
      genericFiles.files

  @node private def buildPathingArtifact(allRuntimeArtifacts: Seq[Artifact]): PathingArtifact = {
    val internalClassFileArtifacts = allRuntimeArtifacts.collect { case c: ClassFileArtifact =>
      c
    }
    val externalClassFileArtifacts =
      runtimeDependencies.transitiveExternalDependencies.result.resolvedArtifacts.apar.map { a =>
        scope.dependencyCopier.atomicallyDepCopyExternalClassFileArtifactsIfMissing(a)
      }
    val classFileArtifacts = internalClassFileArtifacts ++ externalClassFileArtifacts

    val extraFiles = runtimeDependencies.transitiveExtraFiles

    val externalJniPaths = runtimeDependencies.transitiveJniPaths.distinct

    val cppLibs: Seq[CppLibrary] =
      CppUtils.libraries(allRuntimeArtifacts, runtimeDependencies.transitiveScopeDependencies)
    val preloadLibs = cppLibs.filter(_.preload).groupBy(_.buildType)
    val preloadReleaseLibs = preloadLibs.getOrElse(BuildType.Release, Nil)
    val preloadDebugLibs = preloadLibs.getOrElse(BuildType.Debug, Nil)

    // Native artifact paths are used in several different places in the jar file:
    // - JniScopes: Contains the scopes with native artifacts. This is used by OAR/OTR when running apps/tests
    //     (if fallback is false)
    // - PackagedJniLibs: Contains build_obt paths to native libs within jars. This is used by IntelliJ
    //     when running apps (if fallback is false)
    // - JniFallbackPaths: Contains paths to disted native artifacts. This is used by OAR/OTR/IntellliJ when
    //     running apps/tests (if fallback is true)
    // - Preload[Type]Scopes: Contains the scopes with native artifacts for the build type (Release or Debug). This is used
    //     by OAR/OTR when running apps/tests (if fallback is false)
    // - PackagedPreload[Type]Libs: Contains build_obt paths to native libs within jars for the build type (Release or Debug).
    //     This is used by IntelliJ when running apps (if fallback is false)
    // - NativePreload[Type]FallbackPath: Contains paths to disted native artifacts for the build type (Release or Debug).
    //     This is used by OAR/OTR/IntellliJ when running apps/tests (if fallback is true, or we're running a
    //     windows app/test on the grid)

    // Include both release and debug scopes (in general these will be the same) - the app/test
    // will decide at runtime which lib to load with a `System.loadLibrary` call
    val jniScopes = cppLibs.map(_.scopeId).distinct
    // Include both release and debug version of packaged native libs - the app/test will decide at runtime
    // which to load with a `System.loadLibrary` call. Note that these are OS-specific, but are only used locally
    // by intellij
    val packagedJniLibs = cppLibs.filter(_.osVersion == OsUtils.osVersion).flatMap(_.localFile)

    // Include both release and debug fallback paths (in general these will be the same) - the app/test
    // will decide at runtime which lib to load with a `System.loadLibrary` call
    val jniFallbackPaths = cppLibs.flatMap(_.fallbackPath).distinct

    // Include both release and debug scopes - OAR/OTR will select the appropriate one at runtime
    val preloadReleaseScopes = preloadReleaseLibs.map(_.scopeId).distinct
    val preloadDebugScopes = preloadDebugLibs.map(_.scopeId).distinct

    // Include both release and debug version of packaged preload libs - IntelliJ will select the
    // appropriate one at runtime
    val packagedPreloadReleaseLibs = preloadReleaseLibs.filter(_.osVersion == OsUtils.osVersion).flatMap(_.localFile)
    val packagedPreloadDebugLibs = preloadDebugLibs.filter(_.osVersion == OsUtils.osVersion).flatMap(_.localFile)

    // Include both release and debug version of preload fallback paths - OAR/OTR/IntelliJ will select the appropriate
    // one at runtime
    def fallbackPaths(libs: Seq[CppLibrary]): Seq[FileAsset] = (for {
      lib <- libs
      fallbackPath <- lib.fallbackPath
    } yield fallbackPath.resolveFile(CppUtils.linuxNativeLibrary(lib.scopeId, lib.buildType))).distinct

    val preloadReleaseFallbackPaths = fallbackPaths(preloadReleaseLibs)
    val preloadDebugFallbackPaths = fallbackPaths(preloadDebugLibs)

    val moduleLoads = runtimeDependencies.transitiveExternalDependencies.result.moduleLoads

    val (premainOption, filteredArtifacts) = config.agentConfig
      .map { a =>
        (
          Some(a.agentClass),
          classFileArtifacts.flatMap {
            case ia: InternalClassFileArtifact if a.excluded.contains(ia.id.scopeId) && ia.id.tpe == AT.Scala =>
              None
            case artifact =>
              Some(artifact)
          }
        )
      }
      .getOrElse(None, classFileArtifacts)
    val manifest = Jars.updateManifest(
      Jars.createPathingManifest(filteredArtifacts.map(_.path), premainOption),
      JarUtils.nme.ExtraFiles -> extraFiles.map(_.pathString).mkString(";"),
      JarUtils.nme.ExternalJniPath -> externalJniPaths.mkString(";"),
      JarUtils.nme.JniScopes -> jniScopes.map(_.properPath).mkString(";"),
      JarUtils.nme.PackagedJniLibs -> packagedJniLibs.map(_.pathString).mkString(";"),
      JarUtils.nme.JniFallbackPath -> jniFallbackPaths.map(_.pathString).mkString(";"),
      JarUtils.nme.PreloadReleaseScopes -> preloadReleaseScopes.map(_.properPath).mkString(";"),
      JarUtils.nme.PreloadDebugScopes -> preloadDebugScopes.map(_.properPath).mkString(";"),
      JarUtils.nme.PackagedPreloadReleaseLibs -> packagedPreloadReleaseLibs.map(_.pathString).mkString(";"),
      JarUtils.nme.PackagedPreloadDebugLibs -> packagedPreloadDebugLibs.map(_.pathString).mkString(";"),
      JarUtils.nme.PreloadReleaseFallbackPath -> preloadReleaseFallbackPaths.map(_.pathString).mkString(";"),
      JarUtils.nme.PreloadDebugFallbackPath -> preloadDebugFallbackPaths.map(_.pathString).mkString(";"),
      JarUtils.nme.CppFallback -> cpp.cppFallback.toString,
      JarUtils.nme.ModuleLoads -> moduleLoads.mkString(";"),
      // backward compatibility
      JarUtils.nme.JNIPath -> (jniFallbackPaths.map(_.pathString) ++ externalJniPaths).mkString(";"),
      JarUtils.nme.PreloadPath -> preloadReleaseFallbackPaths.map(_.pathString).mkString(";")
    )
    val fingerprint = Jars.fingerprint(manifest)
    val pathingHash = scope.hasher.hashFingerprint(fingerprint, PathingFingerprint)

    val jarPath = scope.pathBuilder.outputPathFor(id, pathingHash, AT.Pathing, None, incremental = false)
    AssetUtils.atomicallyWriteIfMissing(jarPath) { tmpName =>
      ObtTrace.traceTask(scope.id, Pathing) { Jars.writeManifestJar(JarAsset(tmpName), manifest) }
    }
    AT.Pathing.fromAsset(id, jarPath)
  }

  @node def runConfigurations: Seq[RunConf] = runconf.runConfigurations

  @node private def sourcesAreEmpty: Boolean = {
    val searchFolders =
      scope.sourceFolders ++ scope.archiveContentFolders ++ scope.genericFileFolders ++ scope.resourceFolders ++ scope.webSourceFolders ++ scope.electronSourceFolders ++ scope.pythonSourceFolders
    searchFolders.apar.map(_.sourceFiles).forall(_.isEmpty)
  }

  @node private def validateSources: Option[CompilationMessage] =
    if (strictEmptySources) {
      val isEmpty = scope.config.empty
      val message = {
        if (!isEmpty && sourcesAreEmpty)
          Some(s"[${scope.id}] Source folders are empty. If this is expected, please add 'empty=true' in its obt file.")
        else if (isEmpty && !sourcesAreEmpty)
          Some(s"[${scope.id}] Source folders are not empty. Please remove 'empty=true' in its obt file.")
        else None
      }
      message.map(CompilationMessage.error)
    } else None

  @node private[buildtool] def scopeMessages: MessagesArtifact = {
    val allMessages = validateSources.to(Seq)
    InMemoryMessagesArtifact(InternalArtifactId(id, ArtifactType.ConfigMessages, None), allMessages, Validation)
  }

}

private[buildtool] object ScopedCompilationImpl {
  import optimus.buildtool.cache.NodeCaching.optimizerCache

  @node def apply(
      scope: CompilationScope,
      sourceGenerators: Map[GeneratorType, SourceGenerator],
      scalac: AsyncSignaturesCompiler,
      javac: AsyncClassFileCompiler,
      jmhc: AsyncJmhCompiler,
      cppc: AsyncCppCompiler,
      pythonc: AsyncPythonCompiler,
      webc: AsyncWebCompiler,
      electronc: AsyncElectronCompiler,
      runconfc: AsyncRunConfCompiler,
      jarPackager: JarPackager,
      regexScanner: RegexScanner,
      genericFilesPackager: GenericFilesPackager,
      analysisLocator: Option[AnalysisLocator],
      incrementalMode: IncrementalMode,
      processors: Map[ProcessorType, ScopeProcessor],
      installVersion: String,
      cppFallback: Boolean,
      strictEmptySources: Boolean
  ): ScopedCompilationImpl = {
    val generation = SourceGeneration(scope, sourceGenerators)

    val sources = SourceCompilationSources(scope, generation)
    val javaAndScalaSources = JavaAndScalaCompilationSources(scope, sources)
    val scalacInputs = ScalaCompilationInputs(scope, javaAndScalaSources, analysisLocator, incrementalMode)
    val signatures = SignatureScopedCompilation(scope, scalac, scalacInputs)
    val scala = ScalaScopedCompilation(scope, scalac, scalacInputs)
    val java =
      JavaScopedCompilation(scope, javaAndScalaSources, javac, analysisLocator, incrementalMode, signatures, scala)
    val jmh = JmhScopedCompilation(scope, javaAndScalaSources, jmhc, scala, java)

    val cppSources = scope.config.cppConfigs.map(cfg => CppCompilationSources(cfg.osVersion, scope))
    val cpp = CppScopedCompilation(scope, cppSources, cppc, cppFallback)

    val pythonSources = PythonCompilationSources(scope)
    val python = PythonScopedCompilation(scope, pythonSources, pythonc)

    val webSources = WebCompilationSources(scope)
    val web = WebResourcePackaging(scope, webSources, webc)

    val electronSources = ElectronCompilationSources(scope)
    val electron = ElectronScopedCompilation(scope, electronSources, electronc)

    val sourcePackaging = SourcePackaging(scope, sources, jarPackager)

    val resourceSources = ResourceCompilationSourcesImpl(scope, generation)
    val resources = ResourcePackaging(scope, resourceSources, jarPackager)

    val archiveSources = ArchivePackageSources(scope)
    val archivePackaging = ArchivePackaging(scope, archiveSources, jarPackager)

    val regexSources = RegexMessagesCompilationSources(scope, sources, resourceSources)
    val regexMessages = RegexMessagesScopedCompilation(scope, regexSources, regexScanner)

    val runconfSources = RunconfCompilationSources(
      runconfc.obtWorkspaceProperties,
      runconfc.runConfSubstitutionsValidator,
      scope,
      installVersion,
      cppFallback
    )
    val runconf = RunconfAppScopedCompilation(scope, runconfSources, runconfc)

    val genericSources = GenericFilesCompilationSources(scope)
    val genericFiles = GenericFilesScopedCompilation(scope, genericSources, genericFilesPackager)

    val processing = ScopeProcessing(scope, javaAndScalaSources, processors)

    ScopedCompilationImpl(
      scope,
      javaAndScalaSources,
      sourcePackaging,
      signatures,
      scala,
      java,
      cpp,
      python,
      web,
      electron,
      resources,
      archivePackaging,
      jmh,
      runconf,
      genericFiles,
      regexMessages,
      processing,
      strictEmptySources = strictEmptySources
    )
  }

  // These are the nodes through which artifacts for a scope are generated. We'd like to keep these for longer
  // than other nodes to minimize unnecessary computation.
  signaturesForDownstreamCompilers.setCustomCache(optimizerCache)
  classesForDownstreamCompilers.setCustomCache(optimizerCache)
  cppForDownstreamCompilers.setCustomCache(optimizerCache)
  artifactsForDownstreamRuntimes.setCustomCache(optimizerCache)
  allArtifacts.setCustomCache(optimizerCache)
}
