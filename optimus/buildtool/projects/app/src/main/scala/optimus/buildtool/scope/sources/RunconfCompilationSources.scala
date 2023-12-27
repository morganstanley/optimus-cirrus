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
package optimus.buildtool.scope.sources

import java.nio.file.Path
import com.typesafe.config.Config
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.compilers.AsyncCppCompiler.BuildType
import optimus.buildtool.compilers.StaticRunscriptCompilationBindingSources
import optimus.buildtool.compilers.cpp.CppUtils
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.NativeUpstreamFallbackPaths
import optimus.buildtool.files.NativeUpstreamScopes
import optimus.buildtool.files.NativeUpstreams
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.format.ApplicationValidation
import optimus.buildtool.format.ConfigUtils._
import optimus.buildtool.format.RunConfSubstitutions
import optimus.buildtool.format.RunConfSubstitutionsValidator
import optimus.buildtool.format.WorkspaceConfig
import optimus.buildtool.runconf.RunConfFile
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.TypeClasses._
import optimus.buildtool.utils.Utils.distinctLast
import optimus.platform.util.Log
import optimus.platform._

import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap
import scala.util.matching.Regex

final case class RunConfSourceSubstitution(category: String, key: String, value: Option[String], optionalCheck: Boolean)
    extends Ordered[RunConfSourceSubstitution] {
  val expression: String = s"$${${if (optionalCheck) "?" else ""}$category.$key}"
  val name: String = s"$category.$key"

  def fingerprint: Option[(String, String)] = value.map(v => (category, s"$key=$v"))

  override def compare(that: RunConfSourceSubstitution): Int = name.compareTo(that.name)
}

object RunConfSourceSubstitutions {
  val workspaceDependencies: Seq[String] = Seq(
    "javaProject",
    "javaVersion",
    "javaMeta"
  )
}

class RunConfSourceSubstitutions(val obtWorkspaceProperties: Config, val validator: RunConfSubstitutionsValidator)
    extends Log {
  import RunConfSourceSubstitutions._

  @node def hashableSubstitutionRules: String =
    validator.toHashableString

  @node def allowedSubstitution(substitution: RunConfSourceSubstitution): Boolean =
    validator.isAllowed(substitution.category, substitution.key)

  @node def ignoredSubstitution(substitution: RunConfSourceSubstitution): Boolean =
    validator.isIgnored(substitution.category, substitution.key)

  private val substitutionPattern: Regex =
    new Regex("""\$\{([?]?)(([\w]+)\.([\w.]+))\}""", "optional", "whole", "category", "name")
  // TODO (OPTIMUS-36135): Capture position (filepath, line and column)
  @node def sourceSubstitutions(content: String): Seq[RunConfSourceSubstitution] =
    substitutionPattern
      .findAllMatchIn(content)
      .toIndexedSeq
      .apar
      .flatMap { m =>
        val name = m.group("name")
        val category = m.group("category")
        val optional = m.group("optional").nonEmpty
        category match {
          case "sys" =>
            Some(RunConfSourceSubstitution(category, name, sys.props.get(name), optional))
          case "env" =>
            Some(RunConfSourceSubstitution(category, name, sys.env.get(name), optional))
          case "config" =>
            Some(workspaceDependency(category, name, optional))
          case "properties" | "versions" =>
            Some(
              RunConfSourceSubstitution(
                category,
                name,
                obtWorkspaceProperties.optionalString(m.group("whole")),
                optional))
          case "gradle" =>
            None // Intrinsic to OBT and is essentially empty
          case "module" | "this" =>
            None // Reflexive access to runconf during compilation
          case "os" =>
            None // Stay platform agnostic
          case unknownCategory =>
            log.trace(s"WARN: we do not know how to handle '$unknownCategory' properties")
            Some(RunConfSourceSubstitution(unknownCategory, name, None, optional))
        }
      }
      .sorted

  @node private[sources] def workspaceDependency(
      category: String,
      path: String,
      optionalCheck: Boolean): RunConfSourceSubstitution =
    RunConfSourceSubstitution(category, path, obtWorkspaceProperties.optionalString(s"workspace.$path"), optionalCheck)

  @node def obtHardCodedWorkspaceDependencies: Seq[RunConfSourceSubstitution] =
    workspaceDependencies.apar.map(workspaceDependency("workspace", _, optionalCheck = false))
}

final case class UpstreamRunconfInputs(
    upstreamScopes: Seq[ScopeId],
    nativeUpstreams: NativeUpstreams[Directory],
    nativeUpstreamReleasePreloads: NativeUpstreams[FileAsset],
    nativeUpstreamDebugPreloads: NativeUpstreams[FileAsset],
    transitiveJniPaths: Seq[String],
    transitiveModuleLoads: Seq[String]
)

private[sources] final case class HashedRunconfSources(
    content: Seq[(String, SortedMap[SourceUnitId, HashedContent])],
    upstreamInputs: UpstreamRunconfInputs,
    installVersion: String,
    allSourceSubstitutions: Seq[RunConfSourceSubstitution],
    allBlockedSubstitutions: Seq[RunConfSourceSubstitution],
    fingerprintHash: String
) extends HashedSources {
  override def generatedSourceArtifacts: Seq[Artifact] = Nil
}

object RunconfCompilationSources {
  import optimus.buildtool.cache.NodeCaching.reallyBigCache

  // since hashedSources holds the source files and the hash, it's important that it's frozen for the duration of a
  // compilation (so that we're sure what we hashed is what we compiled)
  hashedSources.setCustomCache(reallyBigCache)

  val appScriptsFolderName: String = "appscripts"
  val obtConfigFilesWeCareAbout: Seq[String] =
    "stratosphere.conf" :: List(ApplicationValidation, WorkspaceConfig, RunConfSubstitutions).map(_.path.toString)

  // runConfRelativeLocation is relative to workspace source root
  def isCommonRunConf(runConfRelativeLocation: Path): Boolean = {
    // structure assumed to remain src/<meta>/<bundle>/projects/<project>
    // common runconf files can only be in
    //   - src/                  (name count is 1, including filename)
    //   - src/<meta>/<bundle>   (name count is 3, including filename)
    runConfRelativeLocation.getNameCount <= 3
  }
}

@entity class RunconfCompilationSources(
    val obtWorkspaceProperties: Config,
    val validator: RunConfSubstitutionsValidator,
    scope: CompilationScope,
    _installVersion: String,
    cppFallback: Boolean
) extends CompilationSources {
  val substitutions = new RunConfSourceSubstitutions(obtWorkspaceProperties, validator)

  override def id: ScopeId = scope.id

  @node def allSourceSubstitutions: Seq[RunConfSourceSubstitution] = hashedSources.allSourceSubstitutions

  @node def allBlockedSubstitutions: Seq[RunConfSourceSubstitution] = hashedSources.allBlockedSubstitutions

  @node def upstreamInputs: UpstreamRunconfInputs = hashedSources.upstreamInputs

  @node def installVersion: String = hashedSources.installVersion

  @node protected def hashedSources: HashedRunconfSources = {
    // the source files could be changing while the build is running (e.g. developer is still editing files in the IDE),
    // so we are careful here to ensure that the source file content that we pass in to the compiler is the same
    // as the content that we hash.
    val sourceFileContent = this.sourceFileContent

    val upstreamScopes = scope.upstream.runtimeDependencies.transitiveScopeDependencies.map(_.id)

    val nativeUpstreamCompilationScopes =
      scope.upstream.runtimeDependencies.transitiveScopeDependencies.filter(_.config.cppConfigs.exists(!_.isEmpty))

    // Include both release and debug fallback paths (in general these will be the same)
    val nativeUpstreams: NativeUpstreams[Directory] =
      if (cppFallback) {
        NativeUpstreamFallbackPaths(
          (for {
            s <- nativeUpstreamCompilationScopes
            cfg <- s.config.cppConfigs.flatMap(c => c.release.to(Seq) ++ c.debug.to(Seq))
            d <- cfg.fallbackPath
          } yield d).distinct
        )
      } else {
        NativeUpstreamScopes(nativeUpstreamCompilationScopes.map(_.id))
      }

    def upstreamPreload(debug: Boolean): NativeUpstreams[FileAsset] =
      if (cppFallback) {
        NativeUpstreamFallbackPaths(
          (for {
            s <- nativeUpstreamCompilationScopes
            cfg <- s.config.cppConfigs
            buildCfg <- (if (debug) cfg.debug else cfg.release).to(Seq) if buildCfg.preload
            // hacky - only include the first path for each scope (on the assumption that it's the linux one), to avoid
            // runtime warnings about missing preloads
            d <- buildCfg.fallbackPath.headOption
          } yield {
            d.resolveFile(CppUtils.linuxNativeLibrary(s.id, if (debug) BuildType.Debug else BuildType.Release))
          }).distinct
        )
      } else {
        NativeUpstreamScopes(
          (for {
            s <- nativeUpstreamCompilationScopes
            cfg <- s.config.cppConfigs
            buildCfg <- if (debug) cfg.debug else cfg.release if buildCfg.preload
          } yield s.id).distinct
        )
      }

    val upstreamReleasePreload = upstreamPreload(debug = false)
    val upstreamDebugPreload = upstreamPreload(debug = true)

    val transitiveJniPaths = scope.upstream.runtimeDependencies.transitiveJniPaths
    val transitiveModuleLoads = scope.upstream.runtimeDependencies.transitiveExternalDependencies.result.moduleLoads

    val allSourceSubstitutions =
      // Excludes hardcoded obt substitution from the workspace
      sourceFileContent._2.apar
        .collect {
          case (n, c) if n.suffix == RunConfFile.extension =>
            substitutions.sourceSubstitutions(c.utf8ContentAsString)
        }
        .to(Seq)
        .flatten

    val allBlockedSubstitutions =
      allSourceSubstitutions.apar
        .filterNot(substitutions.ignoredSubstitution)
        .apar
        .filterNot(substitutions.allowedSubstitution)

    val obtHardCodedWorkspaceDependencies: Seq[RunConfSourceSubstitution] =
      RunConfSourceSubstitutions.workspaceDependencies.apar.map(
        substitutions.workspaceDependency("workspace", _, optionalCheck = false)
      )

    // Note here that we're deliberately excluding `ignoredSubstitutions` from the fingerprint, even though
    // they can change the runconf output
    val substitutionFingerprint = (
      allSourceSubstitutions.apar.filterNot(substitutions.ignoredSubstitution).flatMap(_.fingerprint) ++
        obtHardCodedWorkspaceDependencies.flatMap(_.fingerprint)
    ).map { case (category, kv) =>
      s"[Substitution:$category]$kv"
    }

    val blockedSubstitutionFingerprint = allBlockedSubstitutions.flatMap(_.fingerprint).map { case (category, kv) =>
      s"[BlockedSubstitution:$category]$kv"
    }

    val staticBindingSources = new StaticRunscriptCompilationBindingSources(obtWorkspaceProperties)

    // === CAUTION =================================================================================
    //  This is sensitive part. It is hard to make sure we do not overcompile or undercompile.
    //  The relationship between the .runconf, and select .obt files are understood.
    //
    //  But we also hash for the runscript, which relies on runtime values from the workspace,
    //  including profiles.
    //
    //  Note: We do not track java/scala dependency changes since they are contained in the pathing jar, and
    //        runscript generation only refers to the pathing jar.
    //
    //  This function shares information used by the AsyncRunconfCompilerImpl as input to both
    //  **runconf compilation** AND **runscript generation**.
    //
    // TODO (OPTIMUS-37134): a test that this picks up all that is needed
    val anythingThatMattersNotFromRunConfHashes = {
      // Note we don't need the scope hashes here, since we're just constructing the runconf script
      upstreamScopes.map(s => s"[Dependency]$s") ++
        nativeUpstreams.fingerprint() ++
        upstreamReleasePreload.fingerprint("Preload(release)") ++
        upstreamDebugPreload.fingerprint("Preload(debug)") ++
        distinctLast(transitiveJniPaths).map(p => s"[JniPath]$p") ++
        distinctLast(transitiveModuleLoads)
          .map(p => s"[ModuleLoad]$p") ++
        Seq(s"[SubstitutionRules]${substitutions.hashableSubstitutionRules}") ++
        substitutionFingerprint ++
        blockedSubstitutionFingerprint ++
        staticBindingSources.hashableInputs :+
        s"[InstallVersion]${_installVersion}"
    }

    val inputsFingerprint: Seq[String] =
      (sourceFileContent match {
        case (tpe, content) =>
          scope.fingerprint(content, tpe)
      }) ++ anythingThatMattersNotFromRunConfHashes

    val fingerprintHash = scope.hasher.hashFingerprint(inputsFingerprint, ArtifactType.RunconfFingerprint)

    // Note: Anything added to UpstreamRunconfInputs or HashedRunconfSources must be made part
    // of the inputsFingerprint above
    val upstreamInputs = UpstreamRunconfInputs(
      upstreamScopes = upstreamScopes,
      nativeUpstreams = nativeUpstreams,
      nativeUpstreamReleasePreloads = upstreamReleasePreload,
      nativeUpstreamDebugPreloads = upstreamDebugPreload,
      transitiveJniPaths = transitiveJniPaths,
      transitiveModuleLoads = transitiveModuleLoads
    )
    HashedRunconfSources(
      content = Seq(sourceFileContent),
      upstreamInputs = upstreamInputs,
      installVersion = _installVersion,
      allSourceSubstitutions = allSourceSubstitutions,
      allBlockedSubstitutions = allBlockedSubstitutions,
      fingerprintHash = fingerprintHash
    )
  }

  @node private def sourceFileContent: (String, SortedMap[SourceUnitId, HashedContent]) = {
    // All folders will lead to the same project folder, so the first one is fine
    val sourceFileContent = scope.runConfConfig
      .map { rcConfig =>
        SortedMap.from(
          rcConfig.runConfFolder.runconfSourceFiles.toVector ++
            rcConfig.templateFolders.apar.map(_.appScriptsTemplateSourceFiles).merge[SourceUnitId] ++
            rcConfig.runConfFolder.obtConfigSourceFiles(RunconfCompilationSources.obtConfigFilesWeCareAbout))
      }
      .getOrElse(SortedMap.empty[SourceUnitId, HashedContent])

    "Source" -> sourceFileContent
  }

  // Excludes case where there is only the common runconfs
  @node def containsRunconf: Boolean =
    hashedSources.sourceFiles.keys.exists(_.suffix == RunConfFile.extension)
}
