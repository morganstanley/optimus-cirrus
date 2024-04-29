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
package optimus.stratosphere.common

import optimus.stratosphere.bootstrap.OsSpecific
import optimus.stratosphere.bootstrap.WorkspaceRoot
import optimus.stratosphere.bootstrap.config.StratosphereConfig
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.filesanddirs.PathsOpts._
import optimus.stratosphere.sparse.SparseProfile
import optimus.stratosphere.utils.EnvironmentUtils

import java.nio.file.Path
import java.nio.file.Paths
import scala.collection.immutable.Seq

class CommonDirectoryStructure(
    val stratosphereHomeDir: Path,
    val stratosphereRemoteInstall: Option[Path],
    val stratosphereWorkspaceName: String,
    proidPaths: Seq[String]) {

  def this(workspace: StratoWorkspaceCommon) = {
    this(
      workspace.stratosphereHome,
      workspace.stratosphereInstallDir,
      workspace.stratosphereWorkspace,
      workspace.internal.paths.proidHomes
    )
  }

  // <STRATOSPHERE_HOME>/<WORKSPACE_NAME>
  val stratosphereWorkspaceDir: Path = stratosphereHomeDir.resolve(stratosphereWorkspaceName)

  // <STRATOSPHERE_HOME>/<WORKSPACE_NAME>/build_obt
  val buildDirectory: Path = stratosphereWorkspaceDir.resolve("build_obt")

  // <STRATOSPHERE_HOME>/<WORKSPACE_NAME>/<install or remote-install>
  val installDir: Path = stratosphereRemoteInstall.getOrElse(stratosphereWorkspaceDir.resolve("install"))

  // <STRATOSPHERE_HOME>/<WORKSPACE_NAME>/src
  val sourcesDirectory: Path = stratosphereWorkspaceDir.resolve("src")

  // <STRATOSPHERE_HOME>/<WORKSPACE_NAME>/src/conf.obt
  val obtVersionConf: Path = sourcesDirectory.resolve("conf.obt")

  // <STRATOSPHERE_HOME>/<WORKSPACE_NAME>/src/stratosphere.conf
  val stratosphereConf: Path = sourcesDirectory.resolve(WorkspaceRoot.STRATOSPHERE_CONFIG_FILE)

  // <STRATOSPHERE_HOME>/<WORKSPACE_NAME>/src/profiles
  val profilesDir: Path = sourcesDirectory.resolve("profiles")

  // <STRATOSPHERE_HOME>/<WORKSPACE_NAME>/src/profiles/sparse-profiles
  val sparseProfilesDir: Path = profilesDir.resolve("sparse-profiles")

  def sparseProfile(name: String): Path = {
    if (name == SparseProfile.customProfileName) {
      localSparseProfileConfFile
    } else sparseProfilesDir.resolve(s"$name.conf")
  }

  def allSparseProfiles: Seq[Path] = sparseProfilesDir.dir.listFiles().filter(_.name.endsWith(".conf"))

  // src/.git
  val gitDirectory: Path = sourcesDirectory.resolve(".git")

  // src/.git/hooks
  val gitHooksDirectory: Path = gitDirectory.resolve("hooks")

  // <STRATOSPHERE_HOME>/<WORKSPACE_NAME>/logs
  val logsDirectory: Path = stratosphereWorkspaceDir.resolve("logs")

  // <STRATOSPHERE_HOME>/<WORKSPACE_NAME>/ide_config
  val ideConfigStoreDir: Path = stratosphereWorkspaceDir.resolve("ide_config")

  // <STRATOSPHERE_HOME>/<WORKSPACE_NAME>/ide_workspace
  val ideWorkspaceStoreDir: Path = stratosphereWorkspaceDir.resolve("ide_workspace")

  // <STRATOSPHERE_HOME>/<WORKSPACE_NAME>/config
  val workspaceConfigDir: Path = stratosphereWorkspaceDir.resolve("config")

  val ideVersion: Path = workspaceConfigDir.resolve("ideVersion")

  // <STRATOSPHERE_HOME>/<WORKSPACE_NAME>/backups
  val workspaceBackupsDir: Path = stratosphereWorkspaceDir.resolve("backups")

  val backupsIntellij: Path = workspaceBackupsDir.resolve("intellij")

  val backupsSparse: Path = workspaceBackupsDir.resolve("sparse")

  val workspaceMigrationDir: Path = stratosphereWorkspaceDir.resolve("migration")

  val customConfFile: Path = workspaceConfigDir.resolve("custom.conf")

  val sparseProfilesConfFile: Path = workspaceConfigDir.resolve("sparse-profiles.conf")

  val localSparseProfileConfFile: Path = workspaceConfigDir.resolve(SparseProfile.customProfileName + ".conf")

  val intellijConfigFile: Path = workspaceConfigDir.resolve("intellij.conf")

  val setupParamsFile: Path = workspaceConfigDir.resolve(".setup-params.txt")

  val userConfFile: Path = OsSpecific.stratoUserHome().resolve(StratosphereConfig.userConfigName)

  // <STRATOSPHERE_HOME>/.stratosphere
  val stratosphereHiddenDir: Path = stratosphereHomeDir.resolve(".stratosphere")

  val artifactoryCache: Path = stratosphereHiddenDir.resolve("artifactory-cache")

  val toolsDir: Path = stratosphereHiddenDir.resolve("tools")

  val ideClientStoreDir: Path = toolsDir.resolve("gateway_client")

  // <STRATOSPHERE_HOME>/.stratosphere/depcopy
  val depcopyDir: Path = stratosphereHiddenDir.resolve("depcopy")

  val intellijBuildDir: Path = stratosphereWorkspaceDir.resolve("ide_build_intellij")

  val fastDeleteConfigDir: Path = OsSpecific.stratoUserHome().resolve("fastDelete")

  val classpathMapping: Path = buildDirectory.resolve("classpath-mapping.txt")

  val obtLogs: Path = logsDirectory.resolve("obt")

  val obtCatchupDir: Path = obtLogs.resolve("catchup")

  val autoFetchDir: Path = OsSpecific.stratoUserHome().resolve("auto_fetch")

  val forkSyncDir: Path = OsSpecific.stratoUserHome().resolve("fork_sync")

  val referenceReposDir: Path = stratosphereHiddenDir.resolve("reference_repos")

  val gitAlternates: Path = gitDirectory.resolve("objects/info/alternates")

  val userHomeDir: Path = Paths.get {
    val homeDir = System.getProperty("user.home")
    // proids have home dirs that are read-only and we need to override them
    if (proidPaths.exists(homeDir.startsWith)) "/var/tmp/" + EnvironmentUtils.userName else homeDir
  }

  object windows {
    val startMenuRootDir: Path = OsSpecific.userHome().resolve("Start Menu/Programs/Workspaces")

    val startMenuWorkspaceDir: Path = startMenuRootDir.resolve(stratosphereWorkspaceName)
  }

}
