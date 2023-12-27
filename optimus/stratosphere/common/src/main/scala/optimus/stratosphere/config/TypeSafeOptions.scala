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
package optimus.stratosphere.config

import com.typesafe.config.Config
import optimus.stratosphere.common.RemoteIntellijLocation
import optimus.stratosphere.utils.MemSize
import optimus.stratosphere.utils.MemUnit
import org.fusesource.jansi.Ansi.Color

import java.nio.file.Path
import java.time.{Duration => JDuration}
import scala.annotation.implicitNotFound
import scala.collection.immutable.Seq
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

/** Type magic to return Java Duration or Scala Duration but nothing else */
@implicitNotFound("This field needs either [FiniteDuration] or [java.time.Duration] set explicitly.")
sealed trait DurationOrDuration[A]
object DurationOrDuration {
  implicit val javaDuration: DurationOrDuration[java.time.Duration] = new DurationOrDuration[java.time.Duration] {}
  implicit val scalaDuration: DurationOrDuration[FiniteDuration] = new DurationOrDuration[FiniteDuration] {}
}

final case class ConsoleColors(enabled: Boolean, error: Color, warning: Color, highlight: Color)
object ConsoleColors {
  val Disabled: ConsoleColors = ConsoleColors(enabled = false, Color.DEFAULT, Color.DEFAULT, Color.DEFAULT)
}

// IMPORTANT: Remember to add a case to TypeSafeOptionsTest every time you add an 'object' here
// All values needs to be `def`s here to allow for updating configuration in-memory.
trait TypeSafeOptions { self: StratoWorkspaceCommon =>

  def javaVersion: String = self.select("javaVersion")
  def javaShortVersion: String = self.select("javaShortVersion")
  def javaProject: String = self.select("javaProject")
  def scalaVersion: String = self.select("scalaVersion")
  def scalaHomePath: String = self.select("scalaHomePath")

  def historyMerges: Seq[Config] = self.select("history-merges")

  def stratosphereInfra: Path = self.select("stratosphereInfra")
  def stratosphereInfraOverride: Option[String] = self.select("stratosphereInfraOverride")
  def stratosphereInstallDir: Option[Path] = self.select("stratosphereInstallDir")
  def stratosphereHome: Path = self.select("stratosphereHome")
  def stratosphereSrcDir: Option[String] = self.select("stratosphereSrcDir")
  def stratosphereVersion[A: Extractor]: A = self.select("stratosphereVersion")
  def stratosphereVersionSymlink: Option[String] = self.select("stratosphereVersionSymlink")
  def stratosphereWorkspace: String = self.select("stratosphereWorkspace")
  def obtVersion: String = self.select("obt-version")

  def userName: String = self.select("userName")

  // please keep the object sorted

  object artifactoryTools {
    def availableTools: Set[String] = self
      .select[Config]("artifactory-tools.available")
      .entrySet()
      .asScala
      .map(_.getKey)
      .toSet[String]

    object available {
      def forName(name: String): Config = self.select(s"artifactory-tools.available.$name")

      object graphviz {
        def version: Option[String] = self.select("artifactory-tools.available.graphviz.version")
      }
      object git {
        def version: Option[String] = self.select("artifactory-tools.available.git.version")
      }
    }
  }

  object catchUp {
    def oldDefaultBranch: String = self.select("catch-up.old-default-branch")
    def defaultBranch: String = self.select("catch-up.default-branch") match {
      // auto-upgrade everyone
      case value if value == oldDefaultBranch => "staging"
      case other                              => other
    }
    def defaultMode: String = self.select("catch-up.default-mode")

    object remote {
      def name: String = self.select("catch-up.remote.name")
      def url: String = self.select("catch-up.remote.url")
    }
  }

  object ciMonitoring {
    def monitoredCiInstances: Seq[String] = self.select("ci-monitoring.monitored-ci-instances")
    def jobQuery: String = self.select("ci-monitoring.job-query")
  }

  object formatter {
    def consoleLength: Int = self.select("formatter.console-length")

    def excludesJava: Option[Seq[String]] = self.select("formatter.excludesJava")
    def excludes: Set[Regex] =
      self.select[Option[Seq[String]]]("formatter.excludes").getOrElse(Seq.empty).map(_.r).toSet
    def includes: Set[Regex] =
      self.select[Option[Seq[String]]]("formatter.includes").getOrElse(Seq.empty).map(_.r).toSet
  }

  object git {
    def copyHooks: Boolean = self.select("git.copy-hooks")
    def indexUpdated: Boolean = self.select("git.index-updated")
    def usedInWorkspace: Boolean = self.select("git.used-in-workspace")
    def requiredVersion: String = self.select("git.required-version")
  }

  object intellij {
    def attachAgent: Boolean = self.select("intellij.attach-agent")
    def jdk: Option[String] = self.select("intellij.jdk")
    def licenseServer: String = self.select("intellij.license-server")
    def linuxJcefSandbox: Boolean = self.select("intellij.linux-jcef-sandbox")
    def migrateSettings: Boolean = self.select("intellij.migrate-settings")
    def pythonPluginVersion: String = self.select("intellij.pythonPluginVersion")
    def scalaPluginVersion: String = self.select("intellij.scalaPluginVersion")
    def ultimate: Boolean = self.select("intellij.ultimate")
    def version: String = self.select("intellij.version")

    protected def disabledPlugins: Seq[Map[String, String]] = self.select("intellij.disabledPlugins")
    def disabledPluginsNames: Seq[String] = disabledPlugins.map(_.apply("name"))
    def disabledPluginsDirs: Seq[String] = disabledPlugins.map(_.apply("path"))

    object backup {
      def maxBackupsCount: Int = self.select("intellij.backups.max-backups-count")
      def dirsToSkip: Seq[String] = self.select("intellij.backups.dirs-to-skip")
    }

    object catchUp {
      def showQuestion: Boolean = self.select("intellij.catch-up.show-question")
    }

    object defaults {
      def disableAsyncStackTraces: Boolean = self.select("intellij.defaults.disable-async-stack-traces")
      def startupWaitTime: FiniteDuration = self.select("intellij.defaults.startup-wait-time")
      def killGhostProcessesOnStartup: Boolean = self.select("intellij.defaults.kill-ghost-processes-on-startup")
      def cycleBufferSize: Int = self.select("intellij.defaults.cycle-buffer-size")
      def editorZeroLatencyTyping: Boolean = self.select("intellij.defaults.editor-zero-latency-typing")
      def maxIntellisenseFilesize: Int = self.select("intellij.defaults.max-intellisense-filesize")
      def newVersionStartup: FiniteDuration = self.select("intellij.defaults.new-version-startup")
      def reformatOnPaste: Boolean = self.select("intellij.defaults.reformat-on-paste")
      def addImportsOnPaste: Boolean = self.select("intellij.defaults.add-imports-on-paste")
    }

    object idea64 {
      def vmoptions: Seq[String] = self.select("intellij.idea64.vmoptions")
      def extraVmoptions: Seq[String] = self.select("intellij.idea64.extraVmoptions")
    }

    object jetfire {
      def path: String = self.select("intellij.jetfire.path")
    }

    object location {
      def afs: RemoteIntellijLocation =
        new RemoteIntellijLocation(
          self.select[Config]("intellij.location.afs"),
          intellij.ultimate,
          intellij.version,
          "/")
      def artifactory: RemoteIntellijLocation = new RemoteIntellijLocation(
        self.select[Config]("intellij.location.artifactory"),
        intellij.ultimate,
        intellij.version,
        "-")
    }

    object plugins {
      def main: Seq[Map[String, String]] = self.select("intellij.plugins.main")
      def optional: Seq[Map[String, String]] = self.select("intellij.plugins.optional")
      def extension: Seq[Map[String, String]] = self.select("intellij.plugins.extension")
    }

    object telemetry {
      def enabled: Boolean = self.select("intellij.telemetry.enabled")
    }

    object test {
      object vmoptions {
        def default: Seq[String] =
          self.select[Option[Seq[String]]]("intellij.test.vmoptions.default").getOrElse(Seq.empty)
      }
    }
  }

  object internal {
    def bannerMessage: String = self.select("internal.banner-message")
    def hasVersionChanged: Boolean = self.select[Boolean]("internal.version-changed")
    def helpMailGroup: String = self.select("internal.help-mail-group")
    def msGroup: Path = self.select("internal.ms-group")
    def oldStratosphereVersion: Option[String] = self.select("internal.old-stratosphere-version")
    def stashHostname: String = self.select("internal.stash-hostname")

    object console {
      def colors: ConsoleColors = ConsoleColors(
        enabled = self.select("internal.console.use-colors"),
        error = Color.valueOf(self.select("internal.console.colors.error")),
        warning = Color.valueOf(self.select("internal.console.colors.warning")),
        highlight = Color.valueOf(self.select("internal.console.colors.highlight"))
      )
    }

    object environment {
      def proids: Seq[String] = self.select("internal.environment.proids")
    }

    object diag {
      def uploadLocation: Path = self.select("internal.diag.upload-location")
    }

    object factories {
      def restClient: Option[String] = self.select("internal.factories.rest-client")
    }

    object files {
      def shellCmdExt: String = self.select("internal.files.shell-cmd-ext")
      def execExt: String = self.select("internal.files.exec-ext")
      def stratosphere: String = self.select("internal.files.stratosphere")
      def ideaExecutable: String = self.select("internal.files.idea-executable")
      def ideaScript: String = self.select("internal.files.idea-script")
      def fsNotifierExecutable: String = self.select("internal.files.fs-notifier-executable")
      def vmOptionsProperties: String = self.select("internal.files.vm-options-properties")
    }

    object java {
      def install: Path = self.select("internal.java.install")
      def jdkPath(javaProject: String, javaVersion: String): Path =
        self.select[Path]("internal.java.base-path").resolve(javaProject).resolve(javaVersion).resolve("exec")
      def home: Path = self.select("internal.java.home")
    }

    object mail {
      def enabled: Boolean = self.select("internal.mail.enabled")
    }

    object messages {
      def afSecure(userName: String): String =
        self.select("internal.messages.af-secure").replace("$USERNAME", userName).stripMargin

      object email {
        def domain: String = self.select("internal.messages.email.domain")
        def smtpHost: String = self.select("internal.messages.email.smtp-host")
      }
    }

    object jfrog {
      def configFile: Path = self.select("internal.jfrog.config-file")
      def home: Path = self.select("internal.jfrog.home")
    }

    object markdown {
      def bitbucketProject: String = self.select("internal.markdown.bitbucket-project")
      def maxRecursionLevel: Int = self.select("internal.markdown.max-recursion-level")
      def enableGraphs: Boolean = self.select("internal.markdown.enable-graphs")
      def showLineNumbers: Boolean = self.select("internal.markdown.show-line-numbers")

      def iconsDir: Path = self.select("internal.markdown.icons-dir")

      def defaultStylesheetPath: Path = self.select("internal.markdown.default-stylesheet-path")
      def overridesStylesheetPath: Path = self.select("internal.markdown.overrides-stylesheet-path")

      def stratoReleaseNotes: Path = self.select("internal.markdown.strato-release-notes")

      object graphviz {
        def path: Option[String] = self.select("internal.markdown.graphviz.path")
      }
    }

    object obt {
      def appRunnerExecutable: String = self.select("internal.obt.app-runner-executable")
      def configDir: Path = self.select("internal.obt.config-dir")
      def enabledRunTypes: Seq[String] = self.select("internal.obt.enabled-run-types")
      def executable: String = self.select("internal.obt.executable")
      def install: Path = self.select("internal.obt.install")
      def installDir: Path = self.select("internal.obt.install-dir")
      def pythonEnabled: Boolean = self.select("internal.obt.python-enabled")
      def serverExecutable: String = self.select("internal.obt.server-executable")
      def testRunnerExecutable: String = self.select("internal.obt.test-runner-executable")
    }

    object paths {
      def afsDev: Path = self.select("internal.paths.afs-dev")
      def conemu: Path = self.select("internal.paths.conemu")
      def console2: String = self.select("internal.paths.console2")
      def execv2: String = self.select("internal.paths.execv2")
      def initEnviron: String = self.select("internal.paths.init-environ")
      def javassist(version: String): String =
        self.select("internal.paths.javassist").replace("$VERSION", version)
      def proidHomes: Seq[String] = self.select("internal.paths.proid-homes")
      def python: String = self.select("internal.paths.python")
      def windowsTerminal: String = self.select("internal.paths.windows-terminal")
    }

    object repositoryMigration {
      def stashTimeout: JDuration = self.select("internal.repository-migration.stash-timeout")
    }

    object robocopy {
      def args: String = self.select("internal.robocopy.args")
      def enabled: Boolean = self.select("internal.robocopy.enabled")
    }

    object scheduledCleanup {
      def startHour: Int = self.select("internal.scheduled-cleanup.start-hour")
      def startMinute: Int = self.select("internal.scheduled-cleanup.start-minute")
    }

    object sparse {
      def keepOpenDirs: Seq[Path] = self.select("internal.sparse.keep-open-dirs")
      def maxScopeDepth: Int = self.select("internal.sparse.max-scope-depth")
      object backup {
        def skipPatterns: Seq[String] = self.select("internal.sparse.backup.skip-patterns")
        def maxFileSizeKb: MemSize =
          MemSize.of(self.select[Int]("internal.sparse.backup.max-file-size-kb"), MemUnit.KB)
        def maxFilesCount: Int = self.select("internal.sparse.backup.max-files-count")
      }
    }

    object stratosphere {
      def infraPath: String = self.select("internal.stratosphere.infra-path")
      def scriptsDir: String = self.select("internal.stratosphere.scripts-dir")
    }

    object telemetry {
      def enabled: Boolean = self.select("internal.telemetry.enabled")
      def destination: String = self.select("internal.telemetry.destination")
      def timeout: FiniteDuration = self.select("internal.telemetry.timeout")
    }

    object tools {
      def artifactoryUrl: String = self.select("internal.tools.artifactory-url")
      def artifactoryApikeyUrl: String = self.select("internal.tools.artifactory-apikey-url")
      def artifactorySetupCmd: Seq[String] = self.select("internal.tools.artifactory-setup-cmd")
      def jfrogCmd: Seq[String] = self.select("internal.tools.jfrog-cmd")

      object versions {
        def jfrogCli: String = self.select("internal.tools.versions.jfrog-cli")
      }
    }

    object urls {
      def artifactoryBug: String = self.select("internal.urls.artifactory-bug")
      def indexerExclusions: String = self.select("internal.urls.indexer-exclusions")
      def gitVersion: String = self.select("internal.urls.git-version")
      def jenkinsLibrary: String = self.select("internal.urls.jenkins-library")
      def jenkinsLibraryBrowser: String = self.select("internal.urls.jenkins-library-browser")
      def runconfs: String = self.select("internal.urls.runconfs")
      def splunk: String = self.select("internal.urls.splunk")
      def stackoverflow: String = self.select("internal.urls.stackoverflow")
      def stratoHelpForum: String = self.select("internal.urls.strato-help-forum")
      def stratoRecentIssues: String = self.select("internal.urls.strato-recent-issues")

      object bitbucket {
        def blue: String = self.select("internal.urls.bitbucket.blue")
        def red: String = self.select("internal.urls.bitbucket.red")
      }

      object migration {
        def featureBranches: String = self.select("internal.urls.migration.feature-branches")
        def phase1: String = self.select("internal.urls.migration.phase1")
        def oldBranches: String = self.select("internal.urls.migration.old-branches")
        def oldGit: String = self.select("internal.urls.migration.old-git")
        def troubleshooting: String = self.select("internal.urls.migration.troubleshooting")
      }

      object pcHealth {
        def check: String = self.select("internal.urls.pc-health.check")
        def credentialGuard: String = self.select("internal.urls.pc-health.credential-guard")
      }
    }
  }

  object notifications {
    def enabled: Boolean = self.select("notify.enabled")
    def timeout: Duration = self.select("notify.timeout")
  }

  object obt {
    def args: Seq[String] = self.select("obt.args")
    def opts: Seq[String] = self.select("obt.opts")
    def fileHandleLimit: Int = self.select("obt.file-handle-limit")

    object docker {
      def args: Seq[String] = self.select("obt.docker.args")
      def opts: Seq[String] = self.select("obt.docker.opts")
    }

    object java {
      object home {
        def path: Option[String] = self.select("obt.java.home.override")
      }
    }

    object server {
      def args: Seq[String] = self.select("obt.server.args")
      def opts: Seq[String] = self.select("obt.server.opts")
    }
  }

  object openInBitbucket {
    def parentBranch: String = self.select("open-in-bitbucket.parent-branch")
    def remoteName: String = self.select("open-in-bitbucket.remote-name")
  }

  object prCreationTool {
    def timeout: JDuration = self.select("pr-creation-tool.timeout")
    def autoConfirm: Boolean = self.select("pr-creation-tool.auto-confirm")
    def startBuild: Boolean = self.select("pr-creation-tool.start-build")
    def forcePush: Boolean = self.select("pr-creation-tool.force-push")
    def skipPush: Boolean = self.select("pr-creation-tool.skip-push")
    def catchup: Boolean = self.select("pr-creation-tool.catchup")
    def targetBranch: String = self.select("pr-creation-tool.target-branch")
    def remote: String = self.select("pr-creation-tool.remote")
    def defaultReviewers: Seq[String] = self.select("pr-creation-tool.default-reviewers")
    def privateBuildUrl: String = self.select("pr-creation-tool.private-build-url")
    def ciType: String = self.select("pr-creation-tool.ci-type")
  }

  object python {
    object console {
      def version: String = self.select("python.console.version")
    }
  }

  object setup {
    object defaults {
      def mainBranch: String = self.select("setup.defaults.mainBranch")
      def stagingBranch: String = self.select("setup.defaults.stagingBranch")
      def repo: String = self.select("setup.defaults.repo")
      def referenceRepo: Option[String] = self.select("setup.defaults.referenceRepo")
      def usePrivateFork: Boolean = self.select("setup.defaults.use-private-fork")
      def fetchMode: String = self.select("setup.defaults.fetchMode")
      def fetchOvernight: Boolean = self.select("setup.defaults.fetch-overnight")
      def keepRefRepo: Boolean = self.select("setup.defaults.keep-ref-repo")
      def bloblessClone: Boolean = self.select("setup.defaults.blobless-clone")
      def goodCommit: Boolean = self.select("setup.defaults.good-commit")

      object cloneDepth {
        def minimum: Int = self.select("setup.defaults.clone-depth.minimum")
        def short: Int = self.select("setup.defaults.clone-depth.short")
        def average: Int = self.select("setup.defaults.clone-depth.average")
        def default: String = self.select("setup.defaults.clone-depth.default")
      }
    }

    object timeouts {
      def cloning[A: Extractor: DurationOrDuration]: A = self.select("setup.timeouts.clone")
      def configure[A: Extractor: DurationOrDuration]: A = self.select("setup.timeouts.configure")
      def fetch[A: Extractor: DurationOrDuration]: A = self.select("setup.timeouts.fetch")
      def privateFork[A: Extractor: DurationOrDuration]: A = self.select("setup.timeouts.private-fork")
    }

    def allowedDirs: Seq[String] = self.select("setup.allowed-dirs")

    def requiredSpace: MemSize = self.select("setup.required-space")
    def requiredMemory: MemSize = self.select("setup.required-memory")

    def createStartMenuItems: Boolean = self.select("setup.create-start-menu-items")
    def cloneIntellijFonts: Boolean = self.select("setup.clone-intellij-fonts")
  }

  object show {
    def logo: Boolean = self.select("show.logo")
    def startupTooltips: Boolean = self.select("show.startup-tooltips")
    def commandTime: Boolean = self.select("show.command-time")
  }

  object tools {
    object git {
      def isDefault: Boolean = self.select("tools.git.isDefault")
    }
  }

  // please keep the object sorted
}
