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
package optimus.buildtool.format

import optimus.buildtool.config.CopyFilesConfiguration
import optimus.buildtool.config.CppConfiguration.CompilerFlag
import optimus.buildtool.config.CppConfiguration.LinkerFlag
import optimus.buildtool.config.CppConfiguration.OutputType
import optimus.buildtool.config.CppToolchain
import optimus.buildtool.config.Dependencies
import optimus.buildtool.config.ExtensionConfiguration
import optimus.buildtool.config.ForbiddenDependencyConfiguration
import optimus.buildtool.config.InteropConfiguration
import optimus.buildtool.config.NativeDependencyDefinition
import optimus.buildtool.config.ScalacConfiguration
import optimus.buildtool.config.Substitution
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.RelativePath

import scala.collection.immutable.Seq

final case class PostInstallApp(name: String, args: Seq[String], afterInstall: Boolean)

final case class InheritableScopeDefinition(
    compile: Dependencies,
    compileOnly: Dependencies,
    runtime: Dependencies,
    native: Seq[NativeDependencyDefinition],
    rawRoot: Option[String],
    sourcesRoots: Seq[String],
    resourcesRoots: Seq[String],
    webSourcesRoots: Seq[String],
    electronSourcesRoots: Seq[String],
    pythonSourcesRoots: Seq[String],
    archiveContentRoots: Seq[String],
    docRoots: Seq[String],
    scalac: ScalacConfiguration,
    javac: InheritableJavacConfiguration,
    cpp: Seq[InheritableCppConfiguration],
    parents: Seq[String], // eg. test extends main
    usePipelining: Option[Boolean],
    resourceTokens: Map[String, String],
    copyFiles: Option[CopyFilesConfiguration],
    extensions: Option[ExtensionConfiguration],
    postInstallApps: Seq[Set[PostInstallApp]],
    empty: Option[Boolean],
    installSources: Option[Boolean],
    installAppScripts: Option[Boolean],
    installIvy: Option[Boolean],
    bundle: Option[Boolean],
    includeInClassBundle: Option[Boolean],
    mavenOnly: Option[Boolean],
    skipDependencyMappingValidation: Option[Boolean],
    allowUnorderedAndDuplicateDependencies: Option[Boolean],
    relationships: Seq[ScopeRelationship],
    extraLibs: Dependencies,
    forbiddenDependencies: Seq[ForbiddenDependencyConfiguration],
    substitutions: Seq[Substitution],
    interop: Option[InteropConfiguration]
) {
  def allExternalDependencies: Dependencies = (compile ++ compileOnly ++ runtime ++ extraLibs).distinct

  def withParent(parent: InheritableScopeDefinition): InheritableScopeDefinition = {
    InheritableScopeDefinition(
      compile = compile.withParent(parent.compile),
      compileOnly = compileOnly.withParent(parent.compileOnly),
      runtime = runtime.withParent(parent.runtime),
      native = (native ++ parent.native).distinct,
      rawRoot = rawRoot.orElse(parent.rawRoot),
      sourcesRoots = (parent.sourcesRoots ++ sourcesRoots).distinct,
      resourcesRoots = (parent.resourcesRoots ++ resourcesRoots).distinct,
      webSourcesRoots = (parent.webSourcesRoots ++ webSourcesRoots).distinct,
      electronSourcesRoots = (parent.electronSourcesRoots ++ electronSourcesRoots).distinct,
      pythonSourcesRoots = (parent.pythonSourcesRoots ++ pythonSourcesRoots).distinct,
      archiveContentRoots = (parent.archiveContentRoots ++ archiveContentRoots).distinct,
      docRoots = (parent.docRoots ++ docRoots).distinct,
      scalac = scalac.withParent(parent.scalac),
      javac = javac.withParent(parent.javac),
      cpp = withParent(parent.cpp),
      parents = (parent.parents ++ parents).distinct,
      usePipelining = usePipelining.orElse(parent.usePipelining),
      resourceTokens = parent.resourceTokens ++ resourceTokens,
      copyFiles = CopyFilesConfiguration.merge(copyFiles, parent.copyFiles),
      extensions = ExtensionConfiguration.merge(current = extensions, parent = parent.extensions),
      postInstallApps = postInstallApps ++ parent.postInstallApps,
      empty = empty.orElse(parent.empty),
      installSources = installSources.orElse(parent.installSources),
      installAppScripts = installAppScripts.orElse(parent.installAppScripts),
      installIvy = installIvy.orElse(parent.installIvy),
      bundle = bundle.orElse(parent.bundle),
      includeInClassBundle = includeInClassBundle.orElse(parent.includeInClassBundle),
      mavenOnly = mavenOnly.orElse(parent.mavenOnly),
      skipDependencyMappingValidation = skipDependencyMappingValidation.orElse(parent.skipDependencyMappingValidation),
      allowUnorderedAndDuplicateDependencies =
        allowUnorderedAndDuplicateDependencies.orElse(parent.allowUnorderedAndDuplicateDependencies),
      relationships = relationships ++ parent.relationships,
      extraLibs = extraLibs ++ parent.extraLibs,
      forbiddenDependencies = forbiddenDependencies ++ parent.forbiddenDependencies,
      substitutions = substitutions ++ parent.substitutions,
      interop = interop
    )
  }

  private def withParent(parents: Seq[InheritableCppConfiguration]) = {
    val parentMap = parents.map(p => p.osVersion -> p).toMap
    cpp.map { child => parentMap.get(child.osVersion).map(child.withParent).getOrElse(child) }
  }

}
object InheritableScopeDefinition {
  val empty: InheritableScopeDefinition = InheritableScopeDefinition(
    compile = Dependencies.empty,
    compileOnly = Dependencies.empty,
    runtime = Dependencies.empty,
    native = Nil,
    rawRoot = None,
    sourcesRoots = Nil,
    resourcesRoots = Nil,
    webSourcesRoots = Nil,
    electronSourcesRoots = Nil,
    pythonSourcesRoots = Nil,
    archiveContentRoots = Nil,
    docRoots = Nil,
    scalac = ScalacConfiguration.empty,
    javac = InheritableJavacConfiguration.empty,
    cpp = Nil,
    parents = Nil,
    usePipelining = None,
    resourceTokens = Map.empty,
    copyFiles = None,
    extensions = None,
    postInstallApps = Nil,
    empty = None,
    installSources = None,
    installAppScripts = None,
    installIvy = None,
    bundle = None,
    includeInClassBundle = None,
    mavenOnly = None,
    skipDependencyMappingValidation = None,
    allowUnorderedAndDuplicateDependencies = None,
    relationships = Nil,
    extraLibs = Dependencies.empty,
    forbiddenDependencies = Nil,
    substitutions = Nil,
    interop = None
  )
}

final case class InheritableCppConfiguration(
    osVersion: String,
    release: Option[InheritableCppBuildConfiguration],
    debug: Option[InheritableCppBuildConfiguration]
) {
  def withParent(parent: InheritableCppConfiguration): InheritableCppConfiguration =
    InheritableCppConfiguration(
      osVersion: String,
      release = combine(parent.release, release),
      debug = combine(parent.debug, debug)
    )

  private def combine(
      parent: Option[InheritableCppBuildConfiguration],
      child: Option[InheritableCppBuildConfiguration]
  ): Option[InheritableCppBuildConfiguration] = (parent, child) match {
    case (Some(p), Some(c)) => Some(c.withParent(p))
    case (p @ Some(_), _)   => p
    case (_, c @ Some(_))   => c
    case _                  => None
  }
}

final case class InheritableCppBuildConfiguration(
    toolchain: Option[CppToolchain],
    outputType: Option[OutputType],
    preload: Option[Boolean],
    compilerFlags: Set[CompilerFlag],
    symbols: Seq[String],
    includes: Seq[Directory],
    warningLevel: Option[Int], // 0 to 4
    precompiledHeader: Map[RelativePath, RelativePath],
    compilerArgs: Seq[String],
    linkerFlags: Set[LinkerFlag],
    libs: Seq[String],
    libPath: Seq[Directory],
    systemLibs: Seq[FileAsset],
    manifest: Option[FileAsset],
    linkerArgs: Seq[String],
    fallbackPath: Seq[Directory]
) {
  def withParent(parent: InheritableCppBuildConfiguration): InheritableCppBuildConfiguration =
    InheritableCppBuildConfiguration(
      toolchain = toolchain orElse parent.toolchain,
      outputType = outputType orElse parent.outputType,
      preload = preload orElse parent.preload,
      compilerFlags = parent.compilerFlags ++ compilerFlags,
      symbols = parent.symbols ++ symbols,
      includes = parent.includes ++ includes,
      warningLevel = warningLevel orElse parent.warningLevel,
      precompiledHeader = parent.precompiledHeader ++ precompiledHeader,
      compilerArgs = parent.compilerArgs ++ compilerArgs,
      linkerFlags = parent.linkerFlags ++ linkerFlags,
      libs = parent.libs ++ libs,
      libPath = parent.libPath ++ libPath,
      systemLibs = parent.systemLibs ++ systemLibs,
      manifest = manifest orElse parent.manifest,
      linkerArgs = parent.linkerArgs ++ linkerArgs,
      fallbackPath = parent.fallbackPath ++ fallbackPath
    )
}
