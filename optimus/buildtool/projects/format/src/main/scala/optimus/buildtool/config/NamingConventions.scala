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

import java.nio.file.Path
import java.nio.file.Paths
import java.util.UUID
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset

import scala.util.matching.Regex

object NamingConventions {

  val ConfigPrefix = "optimus.buildtool"

  val AfsRootStr = StaticConfig.string("afsRoot")
  val AfsRootStrWindows = AfsRootStr.replace('/', '\\')

  val AfsDistStr = s"${AfsRootStr}dist/"
  lazy val AfsDist = Directory(Paths.get(AfsDistStr))

  val AfsRootMapping = StaticConfig.string("afsRootMapping")
  val AfsRootMappingWindows = AfsRootMapping.replace('/', '\\')

  val MsWebDependencyMeta = "@morgan-stanley/"
  val MsWebDependencyDefaultMeta = "npm"
  val OutsideWebDependencyDefaultMeta = "3rd"

  val AfsNamespace = "VMS"
  val NpmNamespace = "NPM"
  val MavenNamespace = "MAVEN"

  // extra libs are for some special use cases that not involved in normal obt dependencies resolver process.
  // obt won't download these libs, but will generate related metadata for release purposes
  val ExtraLibsTypes = Seq("web", "electron")
  val NpmGroup = "ossjs"
  val NpmName = "node"
  val PnpmGroup = "pnpm"
  val PnpmName = "pnpm"

  val HttpPrefix = "http://"

  val HttpsPrefix = "https://"

  val WindowsDrive: Regex = "[A-Z]:.*".r

  lazy val TempDir = Directory(Paths.get(sys.props("java.io.tmpdir")))

  val LocalVersion = "local"

  val InstallPathComponents = 5 // meta/bundle/version/install/common
  val Common = "common"
  val InstallPattern = s"install/$Common"

  val ArtifactoryStr = "artifactory"
  val MavenUploadDirectory = "packages/maven/com/ms"
  val MavenCIScope = "optimus/codetree"
  val MavenInstallScope = "main"
  val MavenUrlRoot = s".com/$ArtifactoryStr/maven"
  val MavenDepsCentralMeta = "optimus"
  val MavenDepsCentralBundle = "artifactory-deps"
  val MavenLibsKey = "mavenLibs"
  val MavenUnzipFileKey = ".tar.gz"

  // These are a bit hacky, since they assume a certain format of the dep copy directory
  val DepCopyDistRoot: Regex = ".*/\\.stratosphere/depcopy/dist/(.*)".r
  val MsjavaCopyRoot: Regex = ".*/\\.stratosphere/msjava/(.*)".r
  val OssjavaCopyRoot: Regex = ".*/\\.stratosphere/ossjava/(.*)".r
  val DepCopyMavenRoot: Regex = ".*/\\.stratosphere/depcopy/https?/(.*)".r

  val HASH = "HASH"
  val COMMIT = "COMMIT"
  val TEMP = "TEMP"
  val LATEST = "LATEST"
  val EMPTYHASH = "NOHASH"
  val INCR = "INCR"
  val IMMUTABLE = "ASSUMED_IMMUTABLE"
  val WORKSPACE = "WORKSPACE"
  val BUILD: Path = Paths.get("BUILD")
  lazy val BUILD_DIR: Directory = Directory(BUILD)
  val BUILD_DIR_STR: String = BUILD_DIR.pathString
  val DEPCOPY = "DEPCOPY"
  val HTTPCOPY = "HTTPCOPY"
  val HTTPSCOPY = "HTTPSCOPY"
  val DUMMY = "DUMMY"

  val Sandboxes = "sandboxes"

  val ANALYSIS = "analysis"
  val SIGNATURE_ANALYSIS = "signature-analysis"

  val ClassPathMapping = "classpath-mapping.txt"
  val MischiefConfig = "mischief.obt"

  val GeneratedObt = "generated-obt"
  val Sparse = "sparse"

  val bundleRunConfsJar = "bundle-runtimeRunConf.jar"

  val runConfInventory = "inventory.txt"
  val runConfInventoryHeader = "# ObtScopeId / RunconfScopedName"
  val capturedPropertiesExtension = "properties"

  val dockerMetadataProperties: Path = Paths.get("/etc/obt/version.properties")

  def isHttpOrHttps(url: String): Boolean =
    url.startsWith(NamingConventions.HttpPrefix) || url.startsWith(NamingConventions.HttpsPrefix)

  private def toPathingJarName(baseName: String): String =
    s"$baseName-runtimeAppPathing.jar"

  def pathingJarName(scopeId: ScopeId): String = toPathingJarName(baseNameForScope(scopeId))

  def scopeOutputName(scopeId: ScopeId, suffix: String = "jar"): String =
    if (suffix.nonEmpty) s"${baseNameForScope(scopeId)}.$suffix" else baseNameForScope(scopeId)

  def scopeOutputName(scopeId: ScopeId, midFix: String, suffix: String): String =
    if (suffix.nonEmpty) s"${baseNameForScope(scopeId)}$midFix.$suffix" else s"${baseNameForScope(scopeId)}$midFix"

  val baseNamePattern: Regex = """(.*)(?:[.](\w+))""".r
  private def baseNameForScope(scopeId: ScopeId): String =
    s"${scopeId.module}${if (scopeId.isMain) "" else s".${scopeId.tpe}"}"

  def isIncremental(file: FileAsset): Boolean = file.pathString.contains(s"$INCR.")
  def incrString(incremental: Boolean): String = if (incremental) s"$INCR." else ""

  def tempFor(file: FileAsset): FileAsset = tempFor(UUID.randomUUID(), file)
  def tempFor(file: FileAsset, prefix: String): FileAsset = tempFor(UUID.randomUUID(), file, prefix = prefix)
  def tempFor(file: FileAsset, local: Boolean): FileAsset = tempFor(UUID.randomUUID(), file, local = local)
  def tempFor(uuid: UUID, file: FileAsset, local: Boolean = false, prefix: String = TEMP): FileAsset = {
    val tempFile = s"$prefix$uuid-${file.path.getFileName}"
    if (local) TempDir.resolveFile(tempFile)
    else FileAsset(file.path.resolveSibling(tempFile))
  }

  def prefix(fileName: String): String = {
    val dot = fileName.lastIndexOf('.')
    if (dot < 0) fileName
    else fileName.substring(0, dot)
  }

  def suffix(file: FileAsset): String = suffix(file.path)

  def suffix(file: Path): String = suffix(file.getFileName.toString)

  def suffix(fileName: String): String = {
    val dot = fileName.lastIndexOf('.')
    if (dot < 0) ""
    else fileName.substring(dot + 1)
  }

  // Example: convert("calling-convention.c-decl", Seq('-' -> "", '.' -> ".")) => "CallingConvention.CDecl"
  def convert(str: String, replacements: Seq[(Char, String)]): String = {
    replacements.foldLeft(str) { case (s, (in, out)) =>
      s.split(in).map(_.capitalize).mkString(out)
    }
  }

  val ScopeLocator = "ScopeLocator"

  private val textFileExtns = Set(
    "conf",
    "config",
    "cpp",
    "cs",
    "csproj",
    "css",
    "csv",
    "env",
    "gradle",
    "h",
    "htm",
    "html",
    "java",
    "jil",
    "js",
    "json",
    "ksh",
    "log",
    "MF",
    "md",
    "optconf",
    "pl",
    "properties",
    "proto",
    "py",
    "q",
    "scala",
    "sh",
    "sql",
    "txt",
    "xaml",
    "xml",
    "xsd",
    "xsl",
    "yaml",
    "yml"
  )

  // we don't want to mess with the CRLF line endings for Windows scripts
  private val windowsTextFileExtns = Set(
    "bat",
    "cmd"
  )
  // regex-ignore-start (open-sourcing) persistent exception for open-source-able output file types.
  private val binaryFileExtns = Set(
    "bin",
    "bson",
    "cacerts",
    "class",
    "dat",
    "db",
    "eot",
    "exe",
    "gif",
    "grain",
    "gz",
    "ico",
    "jar",
    "jpg",
    "ogtrace",
    "parquet",
    "png",
    "pyc",
    "pyo",
    "riskdump",
    "ser",
    "ttf",
    "wav",
    "woff",
    "xlsm",
    "xlsx",
    "zip"
  ) // regex-ignore-end

  def isBinaryExtension(extn: String): Boolean = binaryFileExtns(extn)

  def isTextExtension(extn: String): Boolean = textFileExtns(extn)

  def isWindowsTextExtension(extn: String): Boolean = windowsTextFileExtns(extn)
}
