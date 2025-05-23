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

import optimus.buildtool.config.NamingConventions.LibsKey
import optimus.buildtool.config.NamingConventions.MavenOnlyKey
import optimus.buildtool.config.NamingConventions.IsAgentKey
import optimus.buildtool.dependencies.JvmDependenciesLoader
import optimus.buildtool.dependencies.JvmDependenciesLoader._
import optimus.buildtool.dependencies.MultiSourceDependenciesLoader.Afs
import optimus.buildtool.dependencies.MultiSourceDependenciesLoader.Extends
import optimus.buildtool.dependencies.MultiSourceDependenciesLoader.Maven
import optimus.buildtool.dependencies.MultiSourceDependenciesLoader.NoAfs
import optimus.buildtool.dependencies.MultiSourceDependenciesLoader.Scala
import optimus.buildtool.format.MavenDefinition.MavenOnlyExcludeKey

import scala.collection.immutable

//noinspection TypeAnnotation
object Keys {
  val ignoredKeysForValidation =
    sys.props.get("optimus.buildtool.ignoredInvalidKeys").map(_.split(",").toSet).getOrElse(Set.empty)

  // Order of this keys is used to format files
  final case class KeySet(order: immutable.Seq[String]) {
    val all = order.toSet
    def ++(other: KeySet) = KeySet(order ++ other.order)
  }
  val empty = KeySet()

  object KeySet {
    def apply() = new KeySet(Nil)
    def apply(order0: String, order: String*) = new KeySet(order0 :: order.toList)
  }

  // module and bundle file
  val inheritableScopeDefinition =
    KeySet(
      MavenOnlyKey,
      "docs",
      "root",
      "extends",
      "compile",
      "compileOnly",
      "cpp",
      "runtime",
      "native",
      "sources",
      "resources",
      "webSources",
      "electronSources",
      "pythonSources",
      "archiveContents",
      "scalac",
      "javac",
      "warnings",
      "usePipelining",
      "tokens",
      "copyFiles",
      "postInstallApps",
      "extensions",
      "empty",
      "installSources",
      "requiredAppScripts",
      "installIvy",
      "bundle",
      "includeInClassBundle",
      "forbiddenDependencies",
      "allowUnorderedAndDuplicateDependencies",
      "skipDependencyMappingValidation",
      IsAgentKey,
      Substitutions
    )
  val regexDefinition = KeySet("rules")
  val scopeDefinition = KeySet(
    "processors",
    "generators",
    "sourceExcludes",
    "agent",
    "open",
    "implementsMacros",
    "hasMacros",
    "isCompilerPlugin",
    "transitiveScalacPlugins",
    "jar",
    "jmh",
    "archive",
    "cpp",
    "web",
    "python",
    "electron",
    "interop"
  ) ++
    inheritableScopeDefinition
  val jarDefinition = KeySet("manifest")
  val scopeDepsTemplate = KeySet("scopes", /*back compat*/ "modules", LibsKey, "mavenLibs")
  val scopeDeps = scopeDepsTemplate ++ KeySet("customScopes", /*back compat*/ "customModules")

  val generator = KeySet("type", "templates", "files", "includes", "excludes", "configuration")
  val processorConfiguration =
    KeySet("type", "template", "templateHeader", "templateFooter", "objects", "installLocation", "configuration")
  val scalac = KeySet("options", "ignoredPlugins", "target", "warnings")
  val javac = KeySet("options", "release", "warnings")
  val cpp =
    KeySet(
      "toolchain",
      "outputType",
      "preload",
      "includes",
      "compilerFlags",
      "symbols",
      "warningLevel",
      "compileHeader",
      "precompiledHeader",
      "compilerArgs",
      LibsKey,
      "libPath",
      "systemLibs",
      "linkerFlags",
      "manifest",
      "linkerArgs",
      "fallbackPath"
    )
  val agent = KeySet("agentClass", "excludes")
  val projectFile = KeySet("all", "main", "test")

  val customModules = KeySet("includes", "excludes")
  // copy files
  val copyFileTaskFrom = KeySet("from", "fromExternal", "fromResources")
  val copyFileTask = copyFileTaskFrom ++ KeySet(
    "into",
    "targetBundle",
    "fileMode",
    "dirMode",
    "includes",
    "excludes",
    "skipIfMissing",
    "compressAs",
    "filters"
  )

  // extensions
  val extensionProperties = KeySet("mode", "type")

  val archiveProperties = KeySet("type", "tokens", "excludes")

  val webProperties = KeySet("mode", LibsKey, "mavenLibs", "npmCommandTemplate", "npmBuildCommands")
  val electronProperties =
    KeySet("executables", "mode", LibsKey, "mavenLibs", "npmCommandTemplate", "npmBuildCommands")

  val postInstallApp = KeySet("name", "args", "afterInstall")

  val codeFlaggingRule =
    KeySet("key", "title", "file-patterns", "filter", "description", "severity-level", "patterns", "new", "upToLine")
  val pattern = KeySet("pattern", "exclude", "message")

  // filters file
  val groupsAndFiltersDefinition = KeySet("groups", "filters")
  val groupsDefinition = KeySet("name", "file-paths", "in-scopes")
  val filtersDefinition = KeySet("name", "all", "any", "exclude")

  // bundles file
  val bundleFileDefinition = KeySet(Names.ModulesRoot, Names.EonId, Names.Root)
  val moduleOwnership = KeySet("owner", "group")

  val bundleDefinition = KeySet("modulesRoot", Names.ForbiddenDependencies)

  val ModuleSets = "module-sets"
  val moduleSetsDefinition = KeySet(ModuleSets)

  val ModuleSet = "module-set"
  val moduleSetFile = KeySet(ModuleSet)

  val Public = "public"
  val Private = "private"
  val CanDependOn = "can-depend-on"
  val DependencySets = "dependency-sets"
  val VariantSets = "variant-sets"
  val moduleSetDefinition = KeySet(Public, Private, CanDependOn, DependencySets, VariantSets)

  // from ProjectProperties, needed below since we merge this with regular dependency configs for variable
  // substitution
  private val propertyKeys = KeySet("properties", "scriptDir", "versions", "workspace")

  val Boms = "boms"

  val DefaultVariant = "default-variant"
  val dependencySet = KeySet(JvmDependenciesLoader.JvmDependenciesKey, Boms) ++ propertyKeys
  val dependencySetVariant = KeySet(DefaultVariant, Boms) ++ propertyKeys
  val variantSet = KeySet(JvmDependenciesLoader.JvmDependenciesKey, Boms) ++ propertyKeys

  // TODO (OPTIMUS-65072): Delete legacy forbidden dependency keys
  val legacyForbiddenDependencyKeys = KeySet(
    Names.Name,
    Names.Configurations,
    Names.AllowedIn,
    Names.AllowedPatterns,
    Names.InternalOnly,
    Names.ExternalOnly)

  val forbiddenDependencyKeys =
    KeySet("dependencyId", "dependencyRegex", "configurations", "transitive", "isExternal", "allowedIn")
  val dependencyAllowedInKeys = KeySet("ids", "patterns")

  // dependencies file
  val dependenciesFile =
    KeySet(Dependencies, Excludes, NativeDependencies, Groups, ExtraLibs) ++ propertyKeys
  val mavenDependenciesFile =
    KeySet(Dependencies, "mavenExcludes", "mavenIncludes", ExtraLibs, Excludes) ++ propertyKeys
  val buildDependenciesFile = KeySet(Dependencies) ++ propertyKeys
  val jvmDependenciesFile = KeySet(Dependencies, Excludes, Substitutions, ExtraLibs) ++ propertyKeys
  val artifactConfig = KeySet("type", "ext")

  private val commonDependencyDefinition =
    KeySet(
      Version,
      Excludes,
      Configuration,
      "transitive",
      Force,
      ContainsMacros,
      IsScalacPlugin,
      Artifacts,
      "keySuffix",
      Names.Name // be used for maven lib name override
    )

  val dependencyDefinition = commonDependencyDefinition ++
    KeySet(Variants, Names.Configurations, Resolvers, Classifier, Scala)
  val variantDefinition = commonDependencyDefinition ++ KeySet("reason")

  val nativeDependencyDefinition = KeySet("paths", "extraFiles")
  val jvmDependencyDefinition = KeySet(Maven, Afs, NoAfs, Variants, Scala, IvyConfigurations, IsAgentKey)
  val substitutionsConfig = KeySet("fromGroup", "fromName", "fromConfig", "toGroup", "toName", "toConfig")
  val excludesConfig = KeySet(Group, Name)
  val excludesWithIvyConfig = KeySet(Group, Name, IvyConfiguration)
  val ivyConfiguration = KeySet(Extends, Maven)

  // resolvers file
  val resolversFile =
    KeySet(
      ResolverDefinition.Name,
      ResolverDefinition.Ivys,
      ResolverDefinition.Poms,
      ResolverDefinition.Artifacts,
      ResolverDefinition.Root)

  // docker file
  val dockerFile = KeySet("images", "configuration")
  val dockerConfiguration = KeySet("defaults", "extraDependencies", "excludes")
  val dockerDefaults = KeySet("registry", "baseImage", "imageSysName")
  val imageDefinition = KeySet("scopes", "extraImages", "baseImage", "imageSysName")

  val mavenDefinition =
    KeySet("all", "main", "test", "release-main", "release-test", "release-all", MavenOnlyExcludeKey)

  // cpp toolchain file
  val cppToolchain = KeySet(
    "type",
    "compiler",
    "compilerPath",
    "compilerFlags",
    "symbols",
    "warningLevel",
    "includes",
    "compilerArgs",
    "linker",
    "linkerPath",
    "linkerFlags",
    "libPath",
    "linkerArgs"
  )

  val warnings = KeySet(
    WarningsConfiguration.fatalWarningsName,
    WarningsConfiguration.overridesName
  )

  // mischief & freezer compilation
  val freezer = KeySet(
    FreezerStructure.active,
    FreezerStructure.save,
    FreezerStructure.compile
  )
  val mischief = KeySet(
    "scope",
    MischiefStructure.extraMsgs,
    MischiefStructure.extraScalacArgs,
    MischiefStructure.invalidateOnly
  )

  val interopObtFile = KeySet("py-module", "jvm-module")

  val pythonTopLevel = KeySet("python", Dependencies)
  val pythonDependencyLevel = KeySet(Afs, "pypi", Variants)
  val pythonDefinition = KeySet(Version, Variants, "path", "thin-pyapp", "ruff")
  val pythonVariant = pythonDefinition ++ KeySet("reason")
  val pythonObtFile = KeySet(LibsKey, "variant", "type", "pythonVenvCmd", "pipInstallCmd")
}
