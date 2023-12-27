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

import optimus.buildtool.files.Directory
import optimus.buildtool.files.DirectoryFactory
import optimus.buildtool.format.ConfigUtils._
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.TypeClasses._
import optimus.buildtool.utils.Utils
import optimus.platform._

import scala.collection.immutable.Seq
import scala.util.matching.Regex

final case class GlobalConfig(
    scalaPath: Directory,
    javaPath: Directory,
    installVersion: String,
    obtVersion: String,
    scalaVersion: String,
    stratoVersion: String,
    workspaceName: String,
    workspaceRoot: Directory,
    workspaceSourceRoot: Directory,
    depCopyRoot: Option[Directory],
    testplanConfig: TestplanConfiguration,
    defaultSrcFilesToUpload: Seq[Regex],
    genericRunnerAppDirOverride: Option[Directory],
    pythonEnabled: Boolean
) {

  val versionConfig: VersionConfiguration =
    VersionConfiguration(
      installVersion = installVersion,
      obtVersion = obtVersion,
      stratosphereVersion = stratoVersion,
      scalaVersion: String)

  val genericRunnerConfig: GenericRunnerConfiguration =
    GenericRunnerConfiguration(genericRunnerAppDirOverride = genericRunnerAppDirOverride)
}

/**
 * Loads global config (e.g. compiler and dependency versions) from the legacy json files.
 *
 * TODO (OPTIMUS-30257): Remove this once we fully migrate to .obt files
 */
@entity object GlobalConfig {
  @node def load(
      workspaceName: String,
      workspaceRoot: Directory,
      workspaceSourceRoot: Directory,
      depCopyRoot: Option[Directory],
      directoryFactory: DirectoryFactory,
      installVersion: String
  ): GlobalConfig = {
    val stratoConfig = StratoConfig.load(directoryFactory, workspaceSourceRoot)
    val scalaPath = Directory(mapDepCopyPath(depCopyRoot, stratoConfig.scalaHome, normalizeLib = false))
    // don't use stratoConfig.scalaHome (for now) -- java.home is the version we use to compile java (set by OBT runscript)
    val javaPath = Directory(mapDepCopyPath(depCopyRoot, tweakedJavaHome.path.toString, normalizeLib = false))
    val pythonEnabled = stratoConfig.config.booleanOrDefault("internal.obt.python-enabled", default = false)

    def obtTestplansConfigBoolean(key: String) = stratoConfig.config.getBoolean(s"obt.testplans.$key")
    def obtTestplansConfigString(key: String, default: String) =
      stratoConfig.config.stringOrDefault(s"obt.testplans.$key", default)

    val testplanConfig = TestplanConfiguration(
      cell = obtTestplansConfigString("cell", "in"),
      cpu = obtTestplansConfigString("cpu", "100%"),
      disk = obtTestplansConfigString("disk", "11G"),
      mem = obtTestplansConfigString("mem", "6G"),
      ignoredPaths = stratoConfig.config.stringListOrEmpty("obt.testplans.ignoredPaths"),
      priority = stratoConfig.config.intOrDefault("obt.testplans.priority", 40),
      useTestCaching = obtTestplansConfigBoolean("enableTestCaching"),
      useDynamicTests = obtTestplansConfigBoolean("enableScopedTests")
    )

    GlobalConfig(
      scalaPath = scalaPath,
      javaPath = javaPath,
      installVersion = installVersion,
      obtVersion = stratoConfig.obtVersion,
      scalaVersion = stratoConfig.scalaVersion,
      stratoVersion = stratoConfig.stratoVersion,
      pythonEnabled = pythonEnabled,
      workspaceName = workspaceName,
      workspaceRoot = workspaceRoot,
      workspaceSourceRoot = workspaceSourceRoot,
      depCopyRoot = depCopyRoot,
      testplanConfig = testplanConfig,
      defaultSrcFilesToUpload = stratoConfig.config.stringListOrEmpty("obt.defaultSrcFilesToUpload").map(_.r),
      genericRunnerAppDirOverride =
        stratoConfig.config.stringOrDefault("internal.obt.genericRunnerAppDir", "").asDirectory
    )
  }

  /* On java 8, java.home can be .../exec/jre, but Zinc wants the folder containing "jre" */
  val tweakedJavaHome: Directory =
    if (Utils.javaHome.name == "jre") Utils.javaHome.parent else Utils.javaHome

  // Note: Some dependencies may be files and others may be directories, so we can't use resolveFile / resolveDir here
  private[config] def mapDepCopyPath(depCopyRoot: Option[Directory], pathStr: String, normalizeLib: Boolean): Path = {
    def resolveMpr(mprPath: String): Path = depCopyRoot match {
      case Some(dc) => dc.resolveDir("dist").path.resolve(mprPath)
      case None     => NamingConventions.AfsDist.path.resolve(mprPath)
    }

    def resolvePr(meta: String, prPath: String): Path = depCopyRoot match {
      case Some(dc) => dc.parent.resolveDir(meta).path.resolve(prPath)
      case None     => NamingConventions.AfsDist.resolveDir(meta).resolveDir("PROJ").path.resolve(prPath)
    }

    val platformIndependentPathStr =
      if (normalizeLib) PathUtils.platformIndependentString(pathStr).replace("/common/lib/", "/lib/")
      else PathUtils.platformIndependentString(pathStr)

    platformIndependentPathStr match {
      case NamingConventions.DepCopyDistRoot(mprPath) => resolveMpr(mprPath)
      case NamingConventions.MsjavaCopyRoot(prPath)   => resolvePr("msjava", prPath)
      case NamingConventions.OssjavaCopyRoot(prPath)  => resolvePr("ossjava", prPath)
      case p                                          => Paths.get(p)
    }
  }

}
