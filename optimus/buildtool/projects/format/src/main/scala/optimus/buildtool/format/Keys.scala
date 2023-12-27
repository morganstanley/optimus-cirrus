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

import optimus.buildtool.dependencies.JvmDependenciesLoader._
import scala.collection.immutable

//noinspection TypeAnnotation
object Keys {
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
      "mavenOnly",
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
      "rules",
      "postInstallApps",
      "extensions",
      "empty",
      "installSources",
      "requiredAppScripts",
      "bundle",
      "includeInClassBundle"
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
    "targetBundles",
    "jar",
    "jmh",
    "archive",
    "cpp",
    "web",
    "python",
    "electron"
  ) ++
    inheritableScopeDefinition
  val jarDefinition = KeySet("manifest")
  val scopeDepsTemplate = KeySet("scopes", /*back compat*/ "modules", "libs", "mavenLibs")
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
      "libs",
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

  val webProperties = KeySet("mode", "libs", "mavenLibs", "npmCommandTemplate", "npmBuildCommands")
  val electronProperties = KeySet("npmCommandTemplate", "libs", "mavenLibs", "npmBuildCommands")

  val postInstallApp = KeySet("name", "args", "afterInstall")

  val codeFlaggingRule = KeySet("key", "title", "file-patterns", "description", "severity-level", "patterns", "new")
  val pattern = KeySet("pattern", "exclude")

  // bundles file
  val moduleOwnership = KeySet("owner", "group")

  val bundleDefinition = KeySet("modulesRoot", Names.forbiddenDependencies)

  val forbiddenDependencyKeys = KeySet(
    Names.name,
    Names.configurations,
    Names.allowedIn,
    Names.allowedPatterns,
    Names.internalOnly,
    Names.externalOnly)

  // dependencies file
  val dependenciesFile =
    KeySet(Dependencies, DepManagement, Excludes, NativeDependencies, Groups, ExtraLibs)
  val artifactConfig = KeySet("type", "ext")
  val dependencyDefinition =
    KeySet(
      Version,
      Configuration,
      Configurations,
      Classifier,
      Excludes,
      "transitive",
      Force,
      ContainsMacros,
      IsScalacPlugin,
      Artifacts,
      "reason",
      "keySuffix",
      Variants,
      Name // be used for maven lib name override
    )
  val nativeDependencyDefinition = KeySet("paths", "extraFiles")
  val jvmDependencyDefinition = KeySet("maven", "afs", "variants")
  val excludesConfig = KeySet("group", "name")

  // resolvers file
  val resolversFile = KeySet(Name, "ivys", Artifacts, "root")

  // docker file
  val dockerFile = KeySet("images", "configuration")
  val dockerConfiguration = KeySet("defaults", "extraDependencies", "excludes")
  val dockerDefaults = KeySet("registry", "baseImage")
  val imageDefinition = KeySet("scopes", "extraImages", "baseImage")

  // maven setup in maven-dependencies.obt
  val mavenDependenciesFile =
    KeySet("dependencies", "mavenExcludes", "mavenIncludes", "extraLibs")
  val mavenDefinition = KeySet("all", "main", "test", "release-main", "release-test", "release-all")

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

  val pythonTopLevel = KeySet("python", "dependencies")
  val pythonDependencyLevel = KeySet("afs", "pypi", "variants")
  val pythonDefinition = KeySet("version", "variants", "path", "venv-pack")
  val pythonVariant = pythonDefinition ++ KeySet("reason")
}
