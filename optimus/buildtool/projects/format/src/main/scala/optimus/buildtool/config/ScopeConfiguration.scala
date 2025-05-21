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
package optimus.buildtool.config

import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory._
import optimus.buildtool.files.RelativePath
import optimus.buildtool.generators.GeneratorType
import optimus.buildtool.processors.ProcessorType

import scala.collection.immutable.Seq
import scala.util.matching.Regex

private[buildtool] final case class AgentConfiguration(agentClass: String, excluded: Set[ScopeId])
private[buildtool] final case class GeneratorConfiguration(
    name: String,
    internalTemplateFolders: Seq[RelativePath],
    externalTemplateFolders: Seq[Directory],
    files: Seq[RelativePath],
    includes: Option[String],
    excludes: Option[String],
    configuration: Map[String, String]
) {
  def sourceFilter: PathFilter = {
    val fileNameFilter = if (files.nonEmpty) EndsWithFilter(files: _*) else NoFilter
    val includeFilter = includes.map(r => PathRegexFilter(r)).getOrElse(NoFilter)
    val excludeFilter = excludes.map(r => Not(PathRegexFilter(r))).getOrElse(NoFilter)
    fileNameFilter && includeFilter && excludeFilter
  }
}

private[buildtool] final case class RunConfConfiguration[A](
    templateFolders: Seq[A],
    runConfFolder: A
)

/**
 * loaded dependencies from module .obt file
 * @param modules internal scopeIds defined in "modules=[]"
 * @param libs external libs(could be maven or afs) defined in "libs=[]"
 * @param mavenLibs maven libs defined in "mavenLibs=[]"
 */
private[buildtool] final case class Dependencies(
    modules: Seq[ScopeId],
    // prevent use these directly
    private val libs: Seq[DependencyDefinition],
    private val mavenLibs: Seq[DependencyDefinition]
) {
  // default dependencies in used for current scope compile
  def externalDeps(useMavenLibsFromDualLibsModule: Boolean = false): Seq[DependencyDefinition] =
    if (useMavenLibsFromDualLibsModule || libs.isEmpty) mavenLibs else libs
  // for fingerprint
  val dualExternalDeps: Seq[DependencyDefinition] = (libs ++ mavenLibs).distinct

  def listExternalLibs(withMavenLibs: Boolean = true): String = {
    val libsMsg = s"libs=[${libs.map(_.key).sorted.mkString(", ")}]"
    val mavenLibsMsg = if (withMavenLibs) s", mavenLibs=[${mavenLibs.map(_.key).mkString(", ")}]" else ""
    s"$libsMsg$mavenLibsMsg"
  }

  def toJsonInput: (Seq[ScopeId], Seq[DependencyDefinition], Seq[DependencyDefinition]) =
    (modules, libs, mavenLibs)

  def withParent(parent: Dependencies): Dependencies = parent ++ this

  def isEmpty: Boolean = mavenLibs.isEmpty && libs.isEmpty

  def withLibs: Boolean = libs.nonEmpty

  def hasMavenLibsOrEmpty: Boolean = mavenLibs.nonEmpty || isEmpty

  def externalMap(f: DependencyDefinition => DependencyDefinition): Dependencies =
    this.copy(libs = this.libs.map(f), mavenLibs = this.mavenLibs.map(f))

  def ++(other: Dependencies): Dependencies = Dependencies(
    modules ++ other.modules,
    libs ++ other.libs,
    mavenLibs ++ other.mavenLibs
  )

  def distinct: Dependencies = Dependencies(modules.distinct, libs.distinct, mavenLibs.distinct)
}

private[buildtool] object Dependencies {
  val empty: Dependencies = Dependencies(Nil, Nil, Nil)

  implicit val dependenciesValueCodec: JsonValueCodec[Dependencies] = JsonCodecMaker.make
}

private[buildtool] final case class AllDependencies(
    compileDependencies: Dependencies,
    compileOnlyDependencies: Dependencies,
    runtimeDependencies: Dependencies,
    externalNativeDependencies: Seq[NativeDependencyDefinition],
    extraLibs: Dependencies,
    substitutions: Seq[Substitution],
    forbiddenDependencies: Seq[ForbiddenDependencyConfiguration]
) {
  def allInternal: Seq[ScopeId] =
    compileDependencies.modules ++ compileOnlyDependencies.modules ++ runtimeDependencies.modules
  def allExternal: Dependencies = (compileDependencies ++ compileOnlyDependencies ++ runtimeDependencies).distinct
  // OBT doesn't add compile dependencies to runtime dependencies; that's our job
  def appendCompile(deps: Dependencies): AllDependencies = {
    val cd = (compileDependencies ++ deps).distinct
    copy(compileDependencies = cd, runtimeDependencies = (runtimeDependencies ++ cd).distinct)
  }
  def appendRuntime(deps: Dependencies): AllDependencies = {
    copy(runtimeDependencies = (runtimeDependencies ++ deps).distinct)
  }
}

private[buildtool] final case class ScopePaths(
    workspaceSourceRoot: Directory,
    scopeRoot: RelativePath,
    configurationFile: RelativePath,
    sources: Seq[RelativePath],
    resources: Seq[RelativePath],
    webSources: Seq[RelativePath],
    electronSources: Seq[RelativePath],
    pythonSources: Seq[RelativePath],
    archiveContents: Seq[RelativePath],
    docs: Seq[RelativePath]
) {
  def absResourceRoots: Seq[Directory] = resources.map(absScopeRoot.resolveDir)
  def absSourceRoots: Seq[Directory] = sources.map(absScopeRoot.resolveDir)
  def absWebSourceRoot: Seq[Directory] = webSources.map(absScopeRoot.resolveDir)
  def absElectronRoot: Seq[Directory] = electronSources.map(absScopeRoot.resolveDir)
  def absDocRoots: Seq[Directory] = docs.map(absScopeRoot.resolveDir)
  def absScopeConfigDir: Directory = workspaceSourceRoot.resolveDir(configurationFile.parent)
  def absScopeRoot: Directory = workspaceSourceRoot.resolveDir(scopeRoot)
}
object ScopePaths {
  def empty: ScopePaths =
    ScopePaths(
      workspaceSourceRoot = Directory.temporary(),
      scopeRoot = RelativePath.empty,
      configurationFile = RelativePath.empty,
      sources = Nil,
      resources = Nil,
      webSources = Nil,
      electronSources = Nil,
      pythonSources = Nil,
      archiveContents = Nil,
      docs = Nil
    )
}

private[buildtool] final case class ScopeFlags(
    open: Boolean, // Can be a dependency of other scopes
    public: Boolean, // Can be a dependency of scopes in other module sets
    containsPlugin: Boolean,
    containsAgent: Boolean,
    definesMacros: Boolean, // contains def foo: Int = macro foo_impl
    containsMacros: Boolean, // contains def foo_impl(c: Context): c.Tree = ...
    javaOnly: Boolean,
    usePipelining: Boolean,
    jmh: Boolean,
    empty: Boolean,
    installSources: Boolean,
    installAppScripts: Boolean,
    installIvy: Boolean,
    pathingBundle: Boolean,
    mavenOnly: Boolean,
    skipDependencyMappingValidation: Boolean
)
object ScopeFlags {
  val empty: ScopeFlags = ScopeFlags(
    open = false,
    public = false,
    containsPlugin = false,
    containsAgent = false,
    definesMacros = false,
    containsMacros = false,
    javaOnly = true,
    usePipelining = true,
    jmh = false,
    empty = false,
    installSources = false,
    installAppScripts = false,
    installIvy = false,
    pathingBundle = false,
    mavenOnly = false,
    skipDependencyMappingValidation = false
  )
}

private[buildtool] final case class ScopeConfiguration(
    moduleSet: ModuleSet,
    paths: ScopePaths,
    flags: ScopeFlags,
    generatorConfig: Seq[(GeneratorType, GeneratorConfiguration)],
    resourceTokens: Map[String, String],
    runConfConfig: Option[RunConfConfiguration[RelativePath]],
    private[buildtool] val sourceExclusionsStr: Seq[String],
    private[buildtool] val dependencies: AllDependencies,
    scalacConfig: ScalacConfiguration,
    javacConfig: JavacConfiguration,
    cppConfigs: Seq[CppConfiguration],
    webConfig: Option[WebConfiguration],
    pythonConfig: Option[PythonConfiguration],
    electronConfig: Option[ElectronConfiguration],
    agentConfig: Option[AgentConfiguration],
    processorConfig: Seq[(ProcessorType, ProcessorConfiguration)],
    interopConfig: Option[InteropConfiguration],
    useMavenLibs: Boolean // global setting to force scope use mavenLibs instead of libs
) {

  def absResourceRoots: Seq[Directory] = paths.absResourceRoots
  def absSourceRoots: Seq[Directory] = paths.absSourceRoots
  def absWebSourceRoots: Seq[Directory] = paths.absWebSourceRoot
  def absElectronSourceRoots: Seq[Directory] = paths.absElectronRoot
  def absDocRoots: Seq[Directory] = paths.absDocRoots
  def absScopeConfigDir: Directory = paths.absScopeConfigDir

  def open: Boolean = flags.open
  def containsPlugin: Boolean = flags.containsPlugin
  def containsAgent: Boolean = flags.containsAgent
  def definesMacros: Boolean = flags.definesMacros
  def containsMacros: Boolean = flags.containsMacros
  def javaOnly: Boolean = flags.javaOnly
  def usePipelining: Boolean = flags.usePipelining
  def jmh: Boolean = flags.jmh
  def empty: Boolean = flags.empty
  def pathingBundle: Boolean = flags.pathingBundle

  // Make this a member rather than a constructor arg since Regexes with matching
  // patterns don't compare equal, which causes cache misses
  def sourceExclusions: Seq[Regex] = sourceExclusionsStr.map(_.r)

  def compileDependencies: Dependencies = dependencies.compileDependencies
  def compileOnlyDependencies: Dependencies = dependencies.compileOnlyDependencies
  def runtimeDependencies: Dependencies = dependencies.runtimeDependencies
  def externalNativeDependencies: Seq[NativeDependencyDefinition] = dependencies.externalNativeDependencies

  def internalCompileDependencies: Seq[ScopeId] = compileDependencies.modules
  def externalCompileDependencies: Seq[DependencyDefinition] = compileDependencies.externalDeps(useMavenLibs)
  def internalRuntimeDependencies: Seq[ScopeId] = runtimeDependencies.modules
  def externalRuntimeDependencies: Seq[DependencyDefinition] = runtimeDependencies.externalDeps(useMavenLibs)

  def cppConfig(osVersion: String): CppConfiguration = {
    val cppConfigMap = cppConfigs.map(c => c.osVersion -> c).toMap
    cppConfigMap.getOrElse(
      osVersion,
      throw new IllegalArgumentException(s"No C++ configuration found for OS '$osVersion''"))
  }
}
