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

private[buildtool] final case class Dependencies(
    internal: Seq[ScopeId],
    externalAfs: Seq[DependencyDefinition],
    externalMaven: Seq[DependencyDefinition]) {
  val external: Seq[DependencyDefinition] = externalAfs ++ externalMaven

  def withParent(parent: Dependencies): Dependencies = parent ++ this
  def externalIsMaven: Boolean = externalMaven.nonEmpty && externalAfs.isEmpty
  def externalMap(f: DependencyDefinition => DependencyDefinition): Dependencies =
    this.copy(externalAfs = this.externalAfs.map(f), externalMaven = this.externalMaven.map(f))
  def ++(other: Dependencies): Dependencies = Dependencies(
    internal ++ other.internal,
    externalAfs ++ other.externalAfs,
    externalMaven ++ other.externalMaven
  )
  def distinct: Dependencies = Dependencies(internal.distinct, externalAfs.distinct, externalMaven.distinct)
}

private[buildtool] object Dependencies {
  val empty: Dependencies = Dependencies(Nil, Nil, Nil)

  def apply(internal: Seq[ScopeId], external: Seq[DependencyDefinition]): Dependencies = {
    val (af, afs) = external.partition(_.isMaven)
    Dependencies(internal, afs, af)
  }
}

private[buildtool] final case class AllDependencies(
    compileDependencies: Dependencies,
    compileOnlyDependencies: Dependencies,
    runtimeDependencies: Dependencies,
    externalNativeDependencies: Seq[NativeDependencyDefinition]
) {
  def allInternal: Seq[ScopeId] =
    compileDependencies.internal ++ compileOnlyDependencies.internal ++ runtimeDependencies.internal
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
    open: Boolean,
    containsPlugin: Boolean,
    definesMacros: Boolean, // contains def foo: Int = macro foo_impl
    containsMacros: Boolean, // contains def foo_impl(c: Context): c.Tree = ...
    javaOnly: Boolean,
    usePipelining: Boolean,
    jmh: Boolean,
    empty: Boolean,
    installSources: Boolean,
    installAppScripts: Boolean,
    pathingBundle: Boolean,
    mavenOnly: Boolean
)
object ScopeFlags {
  val empty: ScopeFlags = ScopeFlags(
    open = false,
    containsPlugin = false,
    definesMacros = false,
    containsMacros = false,
    javaOnly = true,
    usePipelining = true,
    jmh = false,
    empty = false,
    installSources = false,
    installAppScripts = false,
    pathingBundle = false,
    mavenOnly = false
  )
}

private[buildtool] final case class ScopeConfiguration(
    paths: ScopePaths,
    flags: ScopeFlags,
    generatorConfig: Seq[(GeneratorType, GeneratorConfiguration)],
    resourceTokens: Map[String, String],
    runConfConfig: Option[RunConfConfiguration[RelativePath]],
    regexConfig: Option[RegexConfiguration],
    private val sourceExclusionsStr: Seq[String],
    private[buildtool] val dependencies: AllDependencies,
    scalacConfig: ScalacConfiguration,
    javacConfig: JavacConfiguration,
    cppConfigs: Seq[CppConfiguration],
    webConfig: Option[WebConfiguration],
    pythonConfig: Option[PythonConfiguration],
    electronConfig: Option[ElectronConfiguration],
    agentConfig: Option[AgentConfiguration],
    targetBundles: Seq[MetaBundle],
    processorConfig: Seq[(ProcessorType, ProcessorConfiguration)]
) {

  def absResourceRoots: Seq[Directory] = paths.absResourceRoots
  def absSourceRoots: Seq[Directory] = paths.absSourceRoots
  def absWebSourceRoots: Seq[Directory] = paths.absWebSourceRoot
  def absElectronSourceRoots: Seq[Directory] = paths.absElectronRoot
  def absDocRoots: Seq[Directory] = paths.absDocRoots
  def absScopeConfigDir: Directory = paths.absScopeConfigDir

  def open: Boolean = flags.open
  def containsPlugin: Boolean = flags.containsPlugin
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

  def internalCompileDependencies: Seq[ScopeId] = compileDependencies.internal
  def externalCompileDependencies: Seq[DependencyDefinition] = compileDependencies.external
  def internalRuntimeDependencies: Seq[ScopeId] = runtimeDependencies.internal
  def externalRuntimeDependencies: Seq[DependencyDefinition] = runtimeDependencies.external

  def cppConfig(osVersion: String): CppConfiguration = {
    val cppConfigMap = cppConfigs.map(c => c.osVersion -> c).toMap
    cppConfigMap.getOrElse(
      osVersion,
      throw new IllegalArgumentException(s"No C++ configuration found for OS '$osVersion''"))
  }
}
