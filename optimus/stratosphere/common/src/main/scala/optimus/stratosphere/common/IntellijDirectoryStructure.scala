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

import com.typesafe.config.Config

import java.nio.file.Path
import java.nio.file.Paths
import optimus.stratosphere.bootstrap.OsSpecific.isLinux
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.filesanddirs.PathsOpts._

import scala.collection.immutable.Seq

final class RemoteIntellijLocation(
    config: Config,
    ultimateVersion: Boolean,
    intellijVersion: String,
    versionSeparator: String) {
  private def extension: String = config.getString(if (isLinux) ("linuxExt") else "winExt")
  private def edition: String = config.getString(if (ultimateVersion) "ultimate" else "community")

  def fileName: String = edition + versionSeparator + intellijVersion + extension
  def remoteLocation: String = config.getString("root") + fileName
}

final case class IntellijDirectoryStructure(config: StratoWorkspaceCommon, intellijVersion: String)
    extends CommonDirectoryStructure(config) {

  val afsIntellijLocation: RemoteIntellijLocation = config.intellij.location.afs
  val artifactoryIntellijLocation: RemoteIntellijLocation = config.intellij.location.artifactory

  val ultimateVersion: Boolean = config.intellij.ultimate

  def licenseServerUrl: String = config.intellij.licenseServer

  private val ultimatePostfix: String = if (ultimateVersion) "-Ultimate" else ""

  private val intellijName = "IntelliJ"

  def versionWithName(version: String = intellijVersion): String = s"$intellijName$version$ultimatePostfix"

  def installedIntellijs(): Seq[String] =
    ideConfigStoreDir.dir
      .listDirs()
      .filter(_.name.contains(intellijName))
      .map(_.name.drop(intellijName.length))
      .sorted
      .reverse

  val ideaConfigurationStore: Path = sourcesDirectory.resolve(".idea")

  val workspaceXml: Path = ideaConfigurationStore.resolve("workspace.xml")

  val intellijInstanceConfigStoreDir: Path = intellijInstanceConfigStoreDirForVersion(intellijVersion)

  def intellijInstanceConfigStoreDirForVersion(version: String): Path =
    ideConfigStoreDir.resolve(versionWithName(version))

  // ide_config/<INTELLIJ_VERSION>/install/...
  val intellijInstallDirectory: Path = intellijInstanceConfigStoreDir.resolve("install")

  val intellijJbrDir: Path = intellijInstallDirectory.resolve("jbr")

  val intellijFontsDir: Path = intellijJbrDir.resolve("lib/fonts")

  val intellijPluginsDir: Path = intellijInstallDirectory.resolve("plugins")

  val remoteIntellijDir: Path = Paths.get(afsIntellijLocation.remoteLocation)

  val remoteIntellijArtifactoryDir: String = artifactoryIntellijLocation.remoteLocation

  val pluginsVersionProperties: Path = intellijPluginsDir.resolve("installedPlugins.properties")

  val pluginsExtensionsVersionProperties: Path = intellijPluginsDir.resolve("extendedPlugins.properties")

  val intellijBinDirectory: Path = intellijInstallDirectory.resolve("bin")

  val firstStartMarker: Path = intellijInstallDirectory.resolve("first_start.txt")

  val intellijExecutable: Path = intellijBinDirectory.resolve(config.internal.files.ideaExecutable)

  val intellijScript: Path = intellijBinDirectory.resolve(config.internal.files.ideaScript)

  val fsNotifierExecutable: Path = intellijBinDirectory.resolve(config.internal.files.fsNotifierExecutable)

  val intellijVMOptions: Path = intellijBinDirectory.resolve(config.internal.files.vmOptionsProperties)

  val intellijProperties: Path = intellijBinDirectory.resolve("idea.properties")

  val intellijWorkspaceLocation: Path = intellijWorkspaceLocationForVersion(intellijVersion)

  val intellijBackups: Path = workspaceBackupsDir.resolve("intellij")

  // ide_workspace/<INTELLIJ_VERSION>/...
  def intellijWorkspaceLocationForVersion(version: String): Path =
    ideWorkspaceStoreDir.resolve(versionWithName(version))

  val intellijWorkspaceDataDir: Path = intellijWorkspaceLocation.resolve("system")

  // <STRATOSPHERE_HOME>/<WORKSPACE_NAME>/logs/intellij
  val intellijLogDir: Path = logsDirectory.resolve("intellij")

  // <STRATOSPHERE_HOME>/<WORKSPACE_NAME>/logs/intellij
  val intellijMainLogFile: Path = intellijLogDir.resolve("idea.log")

  // ide_config/<INTELLIJ_VERSION>/config/...
  val intellijInstanceConfigDir: Path = intellijInstanceConfigStoreDir.resolve("config")

  // ide_config/<INTELLIJ_VERSION>/install/...
  val intellijInstanceInstallDir: Path = intellijInstanceInstallDirForVersion(intellijVersion)

  def intellijInstanceInstallDirForVersion(version: String): Path =
    intellijInstanceConfigStoreDirForVersion(version).resolve("install")

  val jdkDirectory: Path = config.internal.java.home

  val intellijConfigOptionsDir: Path = intellijInstanceConfigDir.resolve("options")

  val intellijPortFile: Path = intellijInstanceConfigDir.resolve("port")

  val intellijDisabledPluginsFile: Path = intellijInstanceConfigDir.resolve("disabled_plugins.txt")

  def intellijJdkTable: Path = intellijConfigOptionsDir.resolve("jdk.table.xml")

  def IntellijIdeGeneral: Path = intellijConfigOptionsDir.resolve("ide.general.xml")

  val intellijScalaConfig: Path = intellijConfigOptionsDir.resolve("scala.xml")

  val intellijGitConfig: Path = intellijConfigOptionsDir.resolve("git.xml")

  val stratosphereCompilationSettings: Path = intellijConfigOptionsDir.resolve("compilation.settings.xml")

  val trustedPathsSettings: Path = intellijConfigOptionsDir.resolve("trusted-paths.xml")

  val intellijCodeStylesDir: Path = intellijInstanceConfigDir.resolve("codestyles")

  val intelliStylesheetsDir: Path = intellijInstanceConfigDir.resolve("styles")

  val intellijWorkspaceDir: Path = intellijInstanceConfigDir.resolve("workspace")

  val intellijOptimusCodeStyle: Path = intellijCodeStylesDir.resolve("Optimus _default_.xml")

  val intellijMergedOverridesStylesheet: Path = intelliStylesheetsDir.resolve("overrides.css")

  /** Intended to store exported selected files from configuration to be able to import them to other workspaces. */
  val commonIntellijConfigStore: Path =
    stratosphereHiddenDir.resolve("intellijConfigStore").resolve(versionWithName()).resolve("config")

  def intellijConfigStoreSection(name: String, version: String = versionWithName()): Option[Path] =
    ideConfigStoreDir.resolve(version).resolve("config").resolve(name).existsOption()

  val ideaKey: Path = intellijInstanceConfigDir.resolve("idea.key")

  def intellijResource(name: String) = s"/etc/intellij/$name"

  def intellijBuildProjectAppMsgsFile: Path = intellijBuildDir.resolve("recompiled_msgs.txt")
}

object IntellijDirectoryStructure {
  def apply(config: StratoWorkspaceCommon): IntellijDirectoryStructure = {
    IntellijDirectoryStructure(config, config.intellij.version)
  }
}
