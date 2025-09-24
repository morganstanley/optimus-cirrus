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
import com.typesafe.config.ConfigValueFactory
import optimus.stratosphere.bootstrap.OsSpecific
import optimus.stratosphere.bootstrap.WorkspaceRoot
import optimus.stratosphere.bootstrap.config.StratosphereConfig
import optimus.stratosphere.common.CommonDirectoryStructure
import optimus.stratosphere.common.IntellijDirectoryStructure
import optimus.stratosphere.filesanddirs.PathsOpts._
import optimus.stratosphere.logger.CentralLogger
import optimus.stratosphere.logger.Logger
import org.fusesource.jansi.AnsiConsole

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

/** Strategy for setting workspace location. */
sealed abstract class WorkspaceLocation(val path: Option[Path]) {
  def reloadBaseConfig: Boolean = true
}

/** Use location from given path. */
final case class CustomWorkspace(root: Path) extends WorkspaceLocation(Some(root))

object CustomWorkspace {
  def find(in: Path): Option[CustomWorkspace] =
    Option(WorkspaceRoot.find(in)).map(CustomWorkspace.apply)
}

/** Search for workspace in current dir and up. Default. */
case object AutoDetect extends WorkspaceLocation(None)

/** Use when running outside of workspace */
case object NoWorkspace extends WorkspaceLocation(Some(null)) {
  override def reloadBaseConfig: Boolean = false
}

class StratoWorkspaceCommon protected (
    val customWorkspaceLocation: WorkspaceLocation,
    customLogger: Option[CentralLogger]
) extends TypeSafeOptions {

  private var _config: Config = _
  private var _directoryStructure: CommonDirectoryStructure = _
  private var _intellijDirectoryStructure: IntellijDirectoryStructure = _
  private var _log: CentralLogger = _
  private var _reloadCallback: () => Unit = () => {}
  private var _baseConfigs: BaseConfigs = _

  object msGroup {
    lazy val msGroupPath: Path = StratoWorkspaceCommon.this.internal.msGroup

    lazy val isAvailable: Boolean = {
      val result = msGroupPath.exists()
      if (!result)
        StratoWorkspaceCommon.this.log.warning(s"Cannot access '$msGroupPath', marking it as inaccessible.")
      result
    }
  }

  final def config: Config = loadOrThrow(_config)

  protected final def config_=(newConfig: Config): Unit = _config = newConfig

  final def baseConfigs: BaseConfigs = loadOrThrow(_baseConfigs)

  protected final def baseConfigs_=(newBaseConfigs: BaseConfigs): Unit = {
    _baseConfigs = newBaseConfigs
  }

  final def defaultConfig: Config = baseConfigs.defaultConfig

  final def localUserConfig: Config = baseConfigs.localUserConfig

  final def globalUserConfig: Config = baseConfigs.globalUserConfig

  final def directoryStructure: CommonDirectoryStructure =
    loadOrThrow(_directoryStructure)

  final def intellijDirectoryStructure: IntellijDirectoryStructure =
    loadOrThrow(_intellijDirectoryStructure)

  implicit final def log: CentralLogger =
    loadOrThrow(_log)

  final def addLogger(additionalLogger: Logger): Unit = {
    _log = _log.withLogger(additionalLogger)
  }

  final def setReloadCallback(callback: () => Unit): Unit = {
    _reloadCallback = callback
  }

  final def reload(
      workspaceLocation: WorkspaceLocation = Option(_directoryStructure)
        .map(ds => CustomWorkspace(ds.stratosphereWorkspaceDir))
        .getOrElse(customWorkspaceLocation),
      withBaseConfig: Boolean = true
  ): Unit = {
    // Cannot use logger here, we're initializing it below
    val msg = "Initializing workspace..."
    customLogger match {
      case Some(l) => l.info(msg)
      case None    => println(msg)
    }

    _config =
      workspaceLocation.path.map(StratosphereConfig.loadFromLocation).getOrElse(StratosphereConfig.loadFromCurrentDir())

    _directoryStructure = new CommonDirectoryStructure(this)

    _intellijDirectoryStructure = IntellijDirectoryStructure(this)

    _log = customLogger.getOrElse {
      val logDir =
        try {
          val logDir = directoryStructure.logsDirectory
          logDir.dir.create()
          logDir
        } catch {
          case _: Exception =>
            Paths.get(System.getProperty("java.io.tmpdir"), "strato-logs")
        }

      if (internal.console.colors.enabled) AnsiConsole.systemInstall()
      new CentralLogger(logDir, internal.console.colors)
    }

    if (withBaseConfig) _baseConfigs = BaseConfigs(directoryStructure)
    _reloadCallback()
  }

  final def isOutsideOfWorkspace: Boolean =
    directoryStructure.stratosphereWorkspaceName == StratoWorkspaceNames.noWorkspace ||
      !Files.exists(directoryStructure.sourcesDirectory)

  final def hasGitInWorkspace: Boolean = git.usedInWorkspace && directoryStructure.gitDirectory.exists()

  final def useUnpatchedGit: Boolean = OsSpecific.isWindows && git.useUpdatedFetchSettings

  final def select[A: Extractor](name: String): A = implicitly[Extractor[A]].extract(config, name)

  final def update(name: String)(value: Any): Unit = {
    config = config.withValue(name, ConfigValueFactory.fromAnyRef(value))
  }

  private def loadOrThrow[T](nullableThing: T): T = Option(nullableThing).getOrElse(notInitialized())

  private def notInitialized() =
    throw new IllegalStateException("Trying to access stratosphere workspace before it is initialised.")
}

object StratoWorkspaceNames {
  val noWorkspace = ".no-workspace"
}

class StratoWorkspace protected (
    override val customWorkspaceLocation: WorkspaceLocation = AutoDetect,
    customLogger: Option[CentralLogger] = None
) extends StratoWorkspaceCommon(customWorkspaceLocation, customLogger) {

  reload(withBaseConfig = customWorkspaceLocation.reloadBaseConfig)

  def apply(customWorkspaceLocation: WorkspaceLocation = AutoDetect): StratoWorkspace =
    StratoWorkspace(customWorkspaceLocation)

  override def toString: String = s"StratoWorkspace(customWorkspaceLocation=$customWorkspaceLocation)"
}

object StratoWorkspace {
  def apply(
      customWorkspaceLocation: WorkspaceLocation,
      customLogger: Option[CentralLogger] = None
  ): StratoWorkspace =
    new StratoWorkspace(customWorkspaceLocation, customLogger)
}
