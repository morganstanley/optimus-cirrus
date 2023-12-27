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
package optimus.stratosphere.updater

import optimus.stratosphere.artifactory.ArtifactoryToolDownloader
import optimus.stratosphere.bootstrap.GitProcess
import optimus.stratosphere.bootstrap.OsSpecific
import optimus.stratosphere.bootstrap.StratosphereException
import optimus.stratosphere.common.SemanticVersion
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.filesanddirs.PathsOpts._
import optimus.stratosphere.utils.CommonProcess
import optimus.stratosphere.utils.ConfigUtils

import java.nio.file.Path
import java.nio.file.Paths
import java.util
import scala.util.control.NonFatal

object GitUpdater {
  def gitVersion(stratoWorkspace: StratoWorkspaceCommon): Option[SemanticVersion] = {
    if (!stratoWorkspace.hasGitInWorkspace) {
      None
    } else {
      Some(
        SemanticVersion.parse(
          new CommonProcess(stratoWorkspace)
            .runGit(stratoWorkspace.directoryStructure.sourcesDirectory)("--version")
            .stripPrefix("git version ")
            .trim
        ))
    }
  }
}

class GitUpdater(stratoWorkspace: StratoWorkspaceCommon) {
  private val overwrittenHookNames = List("pre-commit", "prepare-commit-msg", "pre-push")

  def configureRepo(): Unit = {
    adjustSettings()
    copyHooks()
  }

  def ensureGitInstalled(useGitFromArtifactory: Boolean): Unit =
    if (OsSpecific.isWindows && !OsSpecific.isCi) {
      if (stratoWorkspace.tools.git.isDefault != useGitFromArtifactory) {
        stratoWorkspace.log.info("Updating git settings...")
        setGitVersionInConfig(useGitFromArtifactory)
      }

      val gitProcess = new GitProcess(stratoWorkspace.config)
      val gitPath = gitProcess.getGitPath

      if (useGitFromArtifactory && !Paths.get(gitPath).exists()) {
        val downloader = new ArtifactoryToolDownloader(stratoWorkspace, name = "git")
        if (!downloader.install(forceReinstall = false))
          throw new StratosphereException("Could not install git: please see the logs above for more information")
      }

      val stratoPathFile = sys.env.get("STRATO_PATH_FILE")
      stratoPathFile.map(Paths.get(_)).foreach { path =>
        val envCopy = new util.HashMap[String, String](System.getenv())
        val pathEntry = gitProcess.addGitToPath(envCopy)
        path.file.deleteIfExists()
        path.file.write(s"set ${pathEntry.getKey}=${pathEntry.getValue}")
      }
    }

  private def setGitVersionInConfig(useGitFromArtifactory: Boolean): Unit = {
    val customConf = stratoWorkspace.directoryStructure.customConfFile
    ConfigUtils.updateConfigProperty(customConf)("tools.git.isDefault", useGitFromArtifactory)(stratoWorkspace)
    stratoWorkspace.reload()
  }

  private def adjustSettings(): Unit = {
    stratoWorkspace.log.info("Configuring git aliases...")
    val configFileName = "strato.config"
    val blameIgnoreRevsFileName = "git-blame-ignore-revs"
    try {
      // populate if missing
      Seq(configFileName, blameIgnoreRevsFileName).foreach { file =>
        val sourceFile = s"/git/$file"
        if (getClass.getResource(sourceFile) != null) {
          val destinationFile = stratoWorkspace.directoryStructure.gitDirectory.resolve(file)
          destinationFile.file.createFromResourceOrFile(sourceFile)
        }
      }

      val configureRepositoryCommands = Seq(
        Seq("remote", "set-branches", "origin", "*"),
        Seq("config", "--local", "include.path", configFileName),
        Seq("config", "--local", "remote.origin.tagopt", "--no-tags"),
        Seq("config", "--local", "blame.ignoreRevsFile", Path.of(".git", blameIgnoreRevsFileName).toString)
      )

      configureRepositoryCommands.foreach { args =>
        new CommonProcess(stratoWorkspace).runGit(stratoWorkspace.directoryStructure.sourcesDirectory)(args.toSeq: _*)
      }
    } catch {
      case NonFatal(e) =>
        stratoWorkspace.log.warning(
          s"""Failed to configure git aliases. Full stack trace can be found in the log file.
             |If the message above contains a cygwin error, please reboot your machine.
             |If that does not help, please contact ${stratoWorkspace.internal.helpMailGroup}.""".stripMargin
        )
        stratoWorkspace.log.debug(e.toString)

    }
  }

  private def copyHooks(): Unit = {
    if (stratoWorkspace.git.copyHooks) {
      try {
        val hooksDestinationDir = stratoWorkspace.directoryStructure.gitHooksDirectory
        hooksDestinationDir.dir.create()
        if (!hooksDestinationDir.exists()) {
          stratoWorkspace.log.error(s"Git hooks not accessible at '$hooksDestinationDir'")
        }
        stratoWorkspace.log.info("Copying new git hooks to local disk...")
        overwrittenHookNames.foreach(overwriteHook(hooksDestinationDir))
      } catch {
        case NonFatal(exception) =>
          stratoWorkspace.log.error(s"Git hooks were not copied:\n $exception")
      }
    }
  }

  private def overwriteHook(hooksDestinationDir: Path)(hookName: String): Unit = {
    val destinationFile = hooksDestinationDir.resolve(hookName)
    destinationFile.deleteIfExists()
    val sourceFile = s"/git/hooks/$hookName"
    if (getClass.getResource(sourceFile) != null) {
      val bindings = Map(
        "@JAVA_PROJECT@" -> stratoWorkspace.javaProject,
        "@JAVA_VERSION@" -> stratoWorkspace.javaVersion
      )
      destinationFile.file.createFromResourceOrFile(sourceFile, bindings)
      destinationFile.file.makeExecutable()
    }
  }
}
