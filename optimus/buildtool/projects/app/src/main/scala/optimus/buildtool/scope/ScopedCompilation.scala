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
import optimus.buildtool.app.OptimusBuildToolCmdLineT.NoneArg
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.Artifacts
import optimus.buildtool.artifacts.ClassFileArtifact
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.InMemoryMessagesArtifact
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.InternalClassFileArtifact
import optimus.buildtool.artifacts.MessagesArtifact
import optimus.buildtool.artifacts.PathingArtifact
import optimus.buildtool.artifacts.{ArtifactType => AT}
import optimus.buildtool.builders.postbuilders.metadata.{Compile, Runtime}
import optimus.buildtool.builders.postbuilders.metadata.QualifierReport
import optimus.buildtool.compilers.AsyncClassFileCompiler
import optimus.buildtool.compilers.AsyncCppCompiler
import optimus.buildtool.compilers.AsyncElectronCompiler
import optimus.buildtool.compilers.AsyncJmhCompiler
import optimus.buildtool.compilers.AsyncPythonCompiler
import optimus.buildtool.compilers.AsyncRunConfCompiler
import optimus.buildtool.compilers.AsyncSignaturesCompiler
import optimus.buildtool.compilers.AsyncWebCompiler
import optimus.buildtool.compilers.GenericFilesPackager
import optimus.buildtool.compilers.JarPackager
import optimus.buildtool.compilers.ManifestGenerator
import optimus.buildtool.compilers.zinc.AnalysisLocator
import optimus.buildtool.config._
import optimus.buildtool.generators.GeneratorType
import optimus.buildtool.generators.SourceGenerator
import optimus.buildtool.processors.ProcessorType
import optimus.buildtool.processors.ScopeProcessor
import optimus.buildtool.resolvers.ResolutionResult
import optimus.buildtool.runconf.RunConf
import optimus.buildtool.scope.partial.ArchivePackaging
import optimus.buildtool.scope.partial.CppScopedCompilation
import optimus.buildtool.scope.partial.ElectronScopedCompilation
import optimus.buildtool.scope.partial.ConfigurationMessagesScopedCompilation
import optimus.buildtool.scope.partial.GenericFilesScopedCompilation
import optimus.buildtool.scope.partial.JavaScopedCompilation
import optimus.buildtool.scope.partial.JmhScopedCompilation
import optimus.buildtool.scope.partial.PathingScopedCompilation
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
import optimus.buildtool.scope.sources.ConfigurationMessagesCompilationSources
import optimus.buildtool.scope.sources.PythonCompilationSources
import optimus.buildtool.scope.sources.WebCompilationSources
import optimus.buildtool.trace.Validation
import optimus.buildtool.utils.Utils.distinctLast
import optimus.core.needsPlugin
import optimus.platform._
import optimus.platform.annotations.alwaysAutoAsyncArgs

import scala.collection.immutable.{IndexedSeq, Seq}
import scala.collection.compat._

object ScopedCompilation {
  private val artifactFilter: Option[Set[ArtifactType]] = sys.props.get("optimus.buildtool.artifacts").map { s =>
    s.split(",").filter(_ != NoneArg).map(ArtifactType.parse).toSet
  }

  // Default to true if artifactFilter is `None`
  def generate(at: ArtifactType): Boolean = artifactFilter.forall(_.contains(at))
}

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
  @node def allResolutions: Map[QualifierReport, Seq[ResolutionResult]] =
    Map(
      Compile -> allCompileDependencies.apar.flatMap(_.resolution).map(_.result).to(Seq),
      Runtime -> runtimeDependencies.resolution.map(_.result).to(Seq)
    )

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
  @node private[buildtool] def signaturesForDownstreamCompilers: IndexedSeq[Artifact]
  @node private[buildtool] def classesForDownstreamCompilers: IndexedSeq[Artifact]
  @node private[buildtool] def pluginsForDownstreamCompilers: IndexedSeq[Artifact]
  @node private[buildtool] def agentsForDownstreamRuntimes: IndexedSeq[Artifact]
  @node private[buildtool] def cppForDownstreamCompilers: IndexedSeq[Artifact]
  @node private[buildtool] def artifactsForDownstreamRuntimes: IndexedSeq[Artifact]
  @node private[buildtool] def scopeMessages: MessagesArtifact
}

@entity private[buildtool] class ScopedCompilationImpl(
    scope: CompilationScope,
    sources: SourceCompilationSources,
    jvmSources: JavaAndScalaCompilationSources,
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
    pathing: PathingScopedCompilation,
    runconf: RunconfAppScopedCompilation,
    genericFiles: GenericFilesScopedCompilation,
    regexMessages: RegexMessagesScopedCompilation,
    processing: ScopeProcessing,
    manifestGenerator: ManifestGenerator,
    strictEmptySources: Boolean,
    configurationValidationMessages: ConfigurationMessagesScopedCompilation
) extends CompilationNode {

  override def id: ScopeId = scope.id
  override def config: ScopeConfiguration = scope.config
  override private[scope] def upstream = scope.upstream

  override def allCompileDependencies: Seq[ScopeDependencies] = upstream.allCompileDependencies
  override def runtimeDependencies: ScopeDependencies = upstream.runtimeDependencies
  private def allDependencies = allCompileDependencies :+ runtimeDependencies
  override def toString: String = s"ScopedCompilation($id)"

  @node private[buildtool] def signaturesForDownstreamCompilers: IndexedSeq[Artifact] =
    // if we have macros (or we've disabled pipelining), any downstream compilers need our jars
    // and analysis
    if (config.containsMacros || !config.usePipelining) {
      classesForDownstreamCompilers
    }
    // otherwise they just need our signatures plus our own typing dependencies (note that scalac produces java signatures too)
    else {
      distinctArtifacts("signature artifacts for downstreams") {
        apar(
          signatureErrorsOr(signatures.javaAndScalaSignatures.toVector ++ signatures.messages ++ signatures.analysis),
          upstream.signaturesForDownstreamCompilers
        )
      }
    }

  @node private[buildtool] def classesForDownstreamCompilers: IndexedSeq[Artifact] =
    distinctArtifacts("class artifacts for downstreams") {
      val (ourArtifacts, theirArtifacts, relevantResources) = apar(
        if (config.usePipelining) signatureErrorsOr(signatures.analysis ++ ourClasses)
        else ourClasses ++ scala.analysis ++ java.analysis,
        upstream.classesForDownstreamCompilers,
        resources.relevantResourcesForDownstreams
      )

      // if we have macros, we re-package our upstream artifacts to reflect this so that
      // downstream compilers know to treat them specially (eg. by putting them on the macro classpath)
      val upstreamArtifacts = if (config.containsMacros) {
        theirArtifacts.apar.map {
          case c: ClassFileArtifact => c.copy(containsOrUsedByMacros = true)
          case a                    => a
        }
      } else theirArtifacts

      (ourArtifacts ++ relevantResources, upstreamArtifacts)
    }

  @node override private[buildtool] def agentsForDownstreamRuntimes: IndexedSeq[Artifact] =
    distinctArtifacts("agents artifacts for downstreams") {
      apar(
        // we use the pathing jar for agents because it contains the required classpath and agent manifest
        if (config.containsAgent) pathing.pathing else Nil,
        upstream.internalAgentsForOurRuntime
      )
    }

  @node override private[buildtool] def pluginsForDownstreamCompilers: IndexedSeq[Artifact] =
    distinctArtifacts("plugin artifacts for downstreams") {
      apar(
        if (config.containsPlugin) ourClasses ++ resources.resources else Nil,
        upstream.pluginsForOurCompiler
      )
    }

  @node private def ourClasses: IndexedSeq[Artifact] = scala.classes ++ scala.messages ++ java.classes ++ java.messages

  @node def runconfArtifacts: IndexedSeq[Artifact] = distinctArtifacts("runconf artifacts") {
    (runconf.runConfArtifacts, Nil)
  }

  @node private[buildtool] def cppForDownstreamCompilers: IndexedSeq[Artifact] =
    distinctArtifacts("cpp artifacts for downstreams") {
      apar(
        cpp.artifacts,
        upstream.cppForOurCompiler
      )
    }

  @node private[buildtool] def artifactsForDownstreamRuntimes: IndexedSeq[Artifact] =
    distinctArtifacts("runtime artifacts for downstreams", track = true) {
      apar(
        signatureErrorsOr(ourJvmRuntimeArtifacts) ++ ourOtherRuntimeArtifacts,
        upstream.artifactsForOurRuntime
      )
    }

  @node def runtimeArtifacts: Artifacts = distinct("runtime artifacts", track = true) {
    apar(
      signatureErrorsOr(pathing.pathing ++ ourJvmRuntimeArtifacts) ++ ourOtherRuntimeArtifacts,
      upstream.artifactsForOurRuntime
    )
  }

  @node private def sourceFingerprint =
    if (ScopedCompilation.generate(AT.SourceFingerprint)) Some(sources.compilationFingerprint) else None

  @node def allArtifacts: Artifacts = distinct("total artifacts", track = true, includeFingerprints = true) {
    // we include the compile and runtime dependencies artifacts because these may contain errors about resolution
    apar(
      signatureErrorsOr {
        pathing.pathing ++
          ourJvmRuntimeArtifacts ++ {
            if (config.usePipelining) signatures.javaAndScalaSignatures ++ signatures.messages ++ signatures.analysis
            else Nil
          } ++ scala.analysis ++ scala.locator ++ java.analysis ++ java.locator ++
          pathing.pathing.collectInstancesOf[PathingArtifact].apar.flatMap(processing.process)
      } ++
        ourOtherRuntimeArtifacts ++
        sourcePackaging.packagedSources ++
        sourceFingerprint ++
        // always copy generated sources and regex messages (among others) so that we can make use of
        // them even if we've got signature errors
        jvmSources.generatedSourceArtifacts ++
        regexMessages.messages ++
        configurationValidationMessages.messages ++
        allDependencies.apar.flatMap(_.transitiveExternalArtifacts) :+
        scopeMessages,
      upstream.allUpstreamArtifacts
    )
  }

  @node def bundlePathingArtifacts(compiledArtifacts: Seq[Artifact]): Seq[Artifact] = if (config.pathingBundle) {
    val scopesForBundle = upstream.runtimeDependencies.transitiveScopeDependencies.map(_.id).toSet + scope.id
    val artifactsForBundle = compiledArtifacts.collect {
      case a @ InternalClassFileArtifact(id, _) if scopesForBundle.contains(id.scopeId) => a
    }.toVector
    PathingScopedCompilation.artifacts(manifestGenerator, scope, artifactsForBundle)
  } else Nil

  // noinspection ScalaUnusedSymbol
  @alwaysAutoAsyncArgs
  private def distinctArtifacts(artifactType: String, track: Boolean = false, includeFingerprints: Boolean = false)(
      f: => (Seq[Artifact], Seq[Artifact])
  ): IndexedSeq[Artifact] = needsPlugin
  // noinspection ScalaUnusedSymbol
  @node private def distinctArtifacts$NF(
      artifactType: String,
      track: Boolean = false,
      includeFingerprints: Boolean = false
  )(
      f: NodeFunction0[(Seq[Artifact], Seq[Artifact])]
  ): IndexedSeq[Artifact] = {
    def fingerprintFilter(as: Seq[Artifact]) =
      if (includeFingerprints) as else as.filter(!_.isInstanceOf[FingerprintArtifact])
    import optimus.platform.{track => doTrack}
    val (scope, upstream) = distinctLast(if (track) doTrack(f()) else f())
    val filteredScope = fingerprintFilter(scope)
    log.debug(s"[$id] Returning ${filteredScope.size} $artifactType: $filteredScope")
    Vector(filteredScope, upstream).flatten
  }

  // noinspection ScalaUnusedSymbol
  @alwaysAutoAsyncArgs
  private def distinct(artifactType: String, track: Boolean = false, includeFingerprints: Boolean = false)(
      f: => (Seq[Artifact], Seq[Artifact])
  ): Artifacts = needsPlugin
  // noinspection ScalaUnusedSymbol
  @node private def distinct$NF(artifactType: String, track: Boolean = false, includeFingerprints: Boolean = false)(
      f: NodeFunction0[(Seq[Artifact], Seq[Artifact])]
  ): Artifacts = {
    def fingerprintFilter(as: Seq[Artifact]) =
      if (includeFingerprints) as else as.filter(!_.isInstanceOf[FingerprintArtifact])
    import optimus.platform.{track => doTrack}
    val (scope, upstream) = distinctLast(if (track) doTrack(f()) else f())
    val filteredScope = fingerprintFilter(scope)
    log.debug(s"[$id] Returning ${scope.size} $artifactType: $scope")
    Artifacts(filteredScope, upstream)
  }

  // noinspection ScalaUnusedSymbol
  // All artifact methods (directly or otherwise) depends on successful signatures, so short-circuit if we have
  // signature errors (which also includes errors from signature upstreams).
  @alwaysAutoAsyncArgs
  private def signatureErrorsOr(res: => IndexedSeq[Artifact]): IndexedSeq[Artifact] = needsPlugin
  // noinspection ScalaUnusedSymbol
  @node private def signatureErrorsOr$NF(res: NodeFunction0[IndexedSeq[Artifact]]): IndexedSeq[Artifact] =
    if (config.usePipelining) Artifact.onlyErrors(signatures.messages) getOrElse res()
    else
      res()

  @node private def ourJvmRuntimeArtifacts: IndexedSeq[Artifact] =
    Vector(
      scala.classes,
      scala.messages,
      java.classes,
      java.messages,
      resources.resources,
      packaging.archiveContents,
      jmh.classes,
      jmh.messages).flatten

  @node private def ourOtherRuntimeArtifacts: IndexedSeq[Artifact] =
    Vector(
      cpp.artifacts,
      web.artifacts,
      electron.artifacts,
      python.artifacts,
      runconf.runConfArtifacts,
      runconf.messages,
      genericFiles.files).flatten

  @node def runConfigurations: IndexedSeq[RunConf] = runconf.runConfigurations

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
      scopeConfigSource: ScopeConfigurationSource,
      sourceGenerators: Map[GeneratorType, SourceGenerator],
      scalac: AsyncSignaturesCompiler,
      javac: AsyncClassFileCompiler,
      jmhc: AsyncJmhCompiler,
      cppc: AsyncCppCompiler,
      pythonc: AsyncPythonCompiler,
      webc: AsyncWebCompiler,
      electronc: AsyncElectronCompiler,
      manifestGenerator: ManifestGenerator,
      runconfc: AsyncRunConfCompiler,
      jarPackager: JarPackager,
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

    val cppSources = scope.config.cppConfigs.map(cfg => CppCompilationSources(cfg.osVersion, scope, cppFallback))
    val cpp = CppScopedCompilation(scope, cppSources, cppc, cppFallback)

    val pythonSources = PythonCompilationSources(scope)
    val python = PythonScopedCompilation(scope, pythonSources, pythonc)

    val webSources = WebCompilationSources(scope)
    val web = WebResourcePackaging(scope, webSources, webc)

    val electronSources = ElectronCompilationSources(scope)
    val electron = ElectronScopedCompilation(scope, electronSources, electronc)

    val sourcePackaging = SourcePackaging(scope, sources, scopeConfigSource, jarPackager)

    val resourceSources = ResourceCompilationSourcesImpl(scope, generation)
    val resources = ResourcePackaging(scope, resourceSources, jarPackager)

    val archiveSources = ArchivePackageSources(scope)
    val archivePackaging = ArchivePackaging(scope, archiveSources, jarPackager)

    val globalRules = scopeConfigSource.globalRules
    val regexSources = RegexMessagesCompilationSources(scope, sources, resourceSources, globalRules)
    val regexMessages = RegexMessagesScopedCompilation(scope, regexSources, globalRules)

    val forbiddenDependencies = scope.config.dependencies.forbiddenDependencies
    val allDependencies = scope.upstream.allCompileDependencies :+ scope.upstream.runtimeDependencies
    val configurationSources =
      ConfigurationMessagesCompilationSources(
        scope,
        allDependencies,
        forbiddenDependencies,
        scope.externalDependencyResolver)
    val configurationMessages =
      ConfigurationMessagesScopedCompilation(scope, configurationSources, forbiddenDependencies, allDependencies)

    val pathing = PathingScopedCompilation(scope, manifestGenerator, scala, java, resources, jmh, cppFallback)

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
      sources,
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
      pathing,
      runconf,
      genericFiles,
      regexMessages,
      processing,
      manifestGenerator,
      strictEmptySources = strictEmptySources,
      configurationMessages
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
