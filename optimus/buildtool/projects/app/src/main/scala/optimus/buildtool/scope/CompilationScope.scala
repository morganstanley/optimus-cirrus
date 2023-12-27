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

import optimus.buildtool.app.CompilationNodeFactory
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType.CompileOnlyResolution
import optimus.buildtool.artifacts.ArtifactType.CompileResolution
import optimus.buildtool.artifacts.ArtifactType.RuntimeResolution
import optimus.buildtool.artifacts.CachedArtifactType
import optimus.buildtool.artifacts.ClassFileArtifact
import optimus.buildtool.artifacts.CppArtifact
import optimus.buildtool.artifacts.ExternalArtifactType
import optimus.buildtool.artifacts.ExternalClassFileArtifact
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.InternalClassFileArtifact
import optimus.buildtool.artifacts.PathedExternalArtifactId
import optimus.buildtool.artifacts.ResolutionArtifactType
import optimus.buildtool.artifacts.SignatureArtifact
import optimus.buildtool.artifacts.VersionedExternalArtifactId
import optimus.buildtool.cache.ArtifactCache
import optimus.buildtool.cache.HasArtifactStore
import optimus.buildtool.compilers.LanguageCompiler
import optimus.buildtool.compilers.SyncCompiler
import optimus.buildtool.config.Dependencies
import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.DependencyDefinitions
import optimus.buildtool.config.LocalDefinition
import optimus.buildtool.config.ModuleType
import optimus.buildtool.config.NamingConventions._
import optimus.buildtool.config.NativeDependencyDefinition
import optimus.buildtool.config.RunConfConfiguration
import optimus.buildtool.config.ScopeConfiguration
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.buildtool.dependencies.PythonDefinition
import optimus.buildtool.files.DirectoryFactory
import optimus.buildtool.files.SourceFolder
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.format.MischiefArgs
import optimus.buildtool.resolvers.DependencyCopier
import optimus.buildtool.resolvers.ExternalDependencyResolver
import optimus.buildtool.utils.CompilePathBuilder
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.TypeClasses._
import optimus.buildtool.utils.Utils
import optimus.buildtool.utils.Utils.distinctLast
import optimus.core.needsPlugin
import optimus.platform._
import optimus.platform.annotations.alwaysAutoAsyncArgs

import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap

@entity private[buildtool] class CompilationScope(
    val id: ScopeId,
    val config: ScopeConfiguration,
    val sourceFolders: Seq[SourceFolder],
    val resourceFolders: Seq[SourceFolder],
    val webSourceFolders: Seq[SourceFolder],
    val electronSourceFolders: Seq[SourceFolder],
    val pythonSourceFolders: Seq[SourceFolder],
    val archiveContentFolders: Seq[SourceFolder],
    val genericFileFolders: Seq[SourceFolder],
    val runConfConfig: Option[RunConfConfiguration[SourceFolder]],
    val pathBuilder: CompilePathBuilder,
    compilers: Seq[LanguageCompiler],
    val dependencyCopier: DependencyCopier,
    val externalDependencyResolver: ExternalDependencyResolver,
    val hasher: FingerprintHasher,
    cache: ArtifactCache with HasArtifactStore,
    val factory: CompilationNodeFactory,
    val scopeConfigSource: ScopeConfigurationSource,
    val directoryFactory: DirectoryFactory,
    val upstream: UpstreamArtifacts,
    val mischief: Option[MischiefArgs]
) {

  @node def scalaDependenciesFingerprint: Seq[String] = {
    // We can safely just depend on signatures here (even though java needs classes
    // rather than signatures) because the only changes to classes which impact our scala or
    // java compilation will also change signatures (macros are dealt with separately within the
    // upstream's signaturesForDownstreamCompilers).
    val regularDependencyFingerprints = fingerprintDeps(scalaInputArtifacts, "Dependency")
    val pluginDependencyFingerprints = fingerprintDeps(pluginArtifacts.flatten, "Plugin")
    val compilersFingerprint = compilers.apar.flatMap(_.fingerprint)

    val scalaParams = config.scalacConfig
    val relevantScalaParams = fingerprintParams(scalaParams.resolvedOptions, "MiscScalaParam")
    val warningsFingerprint = scalaParams.warnings.fingerprint.map(s => s"[ScalaWarnings]$s")
    val miscFingerPrint = Seq(
      s"[ContainsMacros]${config.containsMacros}",
      // Java release is also passed to ScalaC
      s"[JavaRelease]${config.javacConfig.release}"
    )

    regularDependencyFingerprints ++ pluginDependencyFingerprints ++
      compilersFingerprint ++ relevantScalaParams ++ miscFingerPrint ++ warningsFingerprint
  }

  @node def pythonDependenciesFingerprint: Seq[String] = {
    def versionString(name: String, version: String): String = s"[Version:$name]$version"
    def pythonDef(definition: PythonDefinition, moduleType: ModuleType): Seq[String] = Seq(
      Some(s"[PythonPath]${definition.path}"),
      Some(s"[venvPack2]${definition.venvPack}"),
      Some(s"[moduleType]${moduleType.label}")
    ).flatten

    val libs = config.pythonConfig
      .map(_.libs)
      .getOrElse(Set.empty)
      .map(lib => versionString(lib.name, lib.version))
      .toSeq
      .sorted

    val pythonDefinitions = config.pythonConfig
      .map(c => pythonDef(c.python, c.moduleType))
      .getOrElse(Seq.empty)

    pythonDefinitions ++ libs
  }

  @node private def loadWebDependency(
      group: String,
      name: String,
      variant: Option[String]): Option[DependencyDefinition] = {
    externalDependencyResolver.dependencyDefinitions
      .find(d => d.group == group && d.name == name && d.variant.map(_.name) == variant)
  }

  @node private def getWebDependenciesFingerprint(
      node: Option[DependencyDefinition],
      pnpm: Option[DependencyDefinition]): Seq[String] = {
    def ver(name: String, dep: Option[DependencyDefinition]): Option[String] =
      dep.map(d => s"[Version:$name]${d.version}")

    config.webConfig.map(_.fingerprint).getOrElse(Nil) ++
      ver(NpmName, node) ++ ver(PnpmName, pnpm)
  }

  @node def webNodeDependency: Option[DependencyDefinition] =
    loadWebDependency(NpmGroup, NpmName, config.webConfig.flatMap(_.nodeVariant))
  @node def webPnpmDependency: Option[DependencyDefinition] =
    loadWebDependency(PnpmGroup, PnpmName, config.webConfig.flatMap(_.pnpmVariant))
  @node def webDependenciesFingerprint: Seq[String] =
    getWebDependenciesFingerprint(webNodeDependency, webPnpmDependency)

  @node def electronNodeDependency: Option[DependencyDefinition] =
    loadWebDependency(NpmGroup, NpmName, config.electronConfig.flatMap(_.nodeVariant))
  @node def electronPnpmDependency: Option[DependencyDefinition] =
    loadWebDependency(PnpmGroup, PnpmName, config.electronConfig.flatMap(_.pnpmVariant))
  @node def electronDependenciesFingerprint: Seq[String] =
    getWebDependenciesFingerprint(electronNodeDependency, electronPnpmDependency)

  @node def scalaInputArtifacts: Seq[Artifact] =
    upstream.signaturesForOurCompiler ++
      upstream.allCompileDependencies.apar.flatMap(_.transitiveExternalDependencies.result.resolvedArtifacts)

  @node def pluginArtifacts: Seq[Seq[ClassFileArtifact]] = {
    // 1. resolve the set of class jars which directly contain plugins
    val inputArtifacts = (
      upstream.pluginsForOurCompiler ++
        upstream.allCompileDependencies.apar.flatMap(_.transitiveExternalDependencies.result.resolvedArtifacts)
    ).collect {
      // for internal deps strip out messages, non-plugin artifacts (eg. java class jars) etc.
      // for external deps strip out any non-plugin artifacts
      case c: ClassFileArtifact if c.containsPlugin => c
    }

    // 2. convert each plugin jar into the runtime classpath for that jar
    val allClasspaths = pluginClasspath(inputArtifacts)
    // distinction needed because an artifact can show up both for the macro classpath and for the normal classpath
    // (so they differ only in `ClassFileArtifact#containsOrUsedByMacros`)
    var distinctClasspaths = distinctLast(allClasspaths)
    allClasspaths.foreach {
      // remove any plugin classpaths which are contained by other plugin classpaths (e.g. when stagingplugin is on
      // the entityplugin classpath), because we assume that such plugins are loaded by the containing plugin
      c =>
        if (distinctClasspaths.exists(d => d.contains(c.head) && d != c))
          distinctClasspaths = distinctClasspaths.filterNot(_ == c)
    }
    distinctClasspaths
  }

  @node private def pluginClasspath(pluginArtifacts: Seq[ClassFileArtifact]): Seq[Seq[ClassFileArtifact]] =
    distinctLast(
      pluginArtifacts.apar.map {
        case InternalClassFileArtifact(InternalArtifactId(scopeId, _, _), _) =>
          // get the runtime classpath for each internal plugin jar
          factory.lookupScope(scopeId).toIndexedSeq.apar.flatMap { s =>
            s.runtimeArtifacts.all.collectAll[ClassFileArtifact] ++
              s.runtimeDependencies.transitiveExternalDependencies.result.resolvedArtifacts
          }
        case c @ ExternalClassFileArtifact(_: PathedExternalArtifactId, _) =>
          Seq(c)
        case ExternalClassFileArtifact(
              VersionedExternalArtifactId(group, name, version, _, ExternalArtifactType.ClassJar),
              _
            ) =>
          val deps = DependencyDefinitions(
            directIds = Seq(DependencyDefinition(group, name, version, LocalDefinition)),
            indirectIds = Seq.empty
          )
          // get the transitive dependencies for each external plugin jar (note that we don't distinguish
          // between runtime and compile-time transitivity for external jars)
          externalDependencyResolver.resolveDependencies(deps).resolvedArtifacts
        case _ => Nil
      }
    )

  @node def fingerprintDeps(deps: Seq[Artifact], tpe: String): Seq[String] = {
    // TODO (OPTIMUS-25547): Remove this distinctLast when we understand the
    // nondeterministic redundancy.
    distinctLast(deps).apar
      .collect {
        // other artifact types (currently) don't affect the result of our compilation, so only include these:
        case c: ClassFileArtifact => c.fingerprint
        case p: SignatureArtifact => p.fingerprint
        case c: CppArtifact       => c.fingerprint
      }
      .map(f => s"[$tpe]$f")
  }

  private[scope] def fingerprintParams(params: Seq[String], tpe: String): Seq[String] = {
    params
      .filterNot(x => SyncCompiler.purelyDiagnosticScalaParamPrefixes.exists(y => x.startsWith(y)))
      .map {
        case CompilationScope.PluginRequire(prefix, pluginStr) =>
          val plugins = pluginStr.split(",")
          s"$prefix${plugins.sorted.mkString(",")}"
        case x =>
          Utils.replaceDirectory(x, config.paths.workspaceSourceRoot, "<workspace-src>")
      }
      .map(x => s"[$tpe]$x")
  }

  @alwaysAutoAsyncArgs def cached[A <: CachedArtifactType](
      tpe: A,
      discriminator: Option[String],
      fingerprintHash: String
  )(
      nf: => Option[A#A]
  ): Seq[A#A] = needsPlugin

  // noinspection ScalaUnusedSymbol
  @node def cached$NF[A <: CachedArtifactType](tpe: A, discriminator: Option[String], fingerprintHash: String)(
      nf: NodeFunction0[Option[A#A]]): Seq[A#A] =
    if (factory.mischiefScope(id)) nf().toIndexedSeq
    else cache.getOrCompute$NF(id, tpe, discriminator, fingerprintHash)(nf).toIndexedSeq

  @node def fingerprint[A <: SourceUnitId](
      content: SortedMap[A, HashedContent],
      tpe: String,
      prefix: String = ""
  ): Seq[String] =
    content.map { case (f, c) => PathUtils.fingerprintElement(tpe, f.id, c.hash, prefix) }.toIndexedSeq

  override def toString: String = s"${getClass.getSimpleName}($id)"
}

object CompilationScope {
  private val PluginRequire = "(-Xplugin-require:)(.*)".r

  @node def apply(
      id: ScopeId,
      config: ScopeConfiguration,
      sourceFolders: Seq[SourceFolder],
      resourceFolders: Seq[SourceFolder],
      webSourceFolders: Seq[SourceFolder],
      electronSourceFolders: Seq[SourceFolder],
      pythonSourceFolders: Seq[SourceFolder],
      archiveContentFolders: Seq[SourceFolder],
      genericFileFolders: Seq[SourceFolder],
      runConfConfig: Option[RunConfConfiguration[SourceFolder]],
      pathBuilder: CompilePathBuilder,
      compilers: Seq[LanguageCompiler],
      dependencyCopier: DependencyCopier,
      externalDependencyResolver: ExternalDependencyResolver,
      cache: ArtifactCache with HasArtifactStore,
      factory: CompilationNodeFactory,
      scopeConfigSource: ScopeConfigurationSource,
      directoryFactory: DirectoryFactory,
      mischief: Option[MischiefArgs]
  ): CompilationScope = {
    val hasher = FingerprintHasher(id, pathBuilder, cache.store, factory.freezeHash, mischief.nonEmpty)

    def mkScopeDeps(tpe: ResolutionArtifactType, deps: Dependencies, nativeDeps: Seq[NativeDependencyDefinition]) =
      ScopeDependencies(id, deps, nativeDeps, tpe, pathBuilder, externalDependencyResolver, factory, cache, hasher)

    val compileDependencies = mkScopeDeps(CompileResolution, config.compileDependencies, Nil)
    val compileOnlyDependencies = mkScopeDeps(CompileOnlyResolution, config.compileOnlyDependencies, Nil)
    val runtimeDependencies =
      mkScopeDeps(RuntimeResolution, config.runtimeDependencies, config.externalNativeDependencies)

    val upstream = UpstreamArtifacts(compileDependencies, compileOnlyDependencies, runtimeDependencies)

    CompilationScope(
      id,
      config,
      sourceFolders,
      resourceFolders,
      webSourceFolders,
      electronSourceFolders,
      pythonSourceFolders,
      archiveContentFolders,
      genericFileFolders,
      runConfConfig,
      pathBuilder,
      compilers,
      dependencyCopier,
      externalDependencyResolver,
      hasher,
      cache,
      factory,
      scopeConfigSource,
      directoryFactory,
      upstream,
      mischief
    )
  }
}
