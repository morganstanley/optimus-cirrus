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
package optimus.buildtool.compilers.venv
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.CompilationMessage.Severity
import optimus.buildtool.config.PythonConfiguration
import optimus.buildtool.config.PythonConfiguration.OverriddenCommands
import optimus.buildtool.dependencies.PythonDefinition
import optimus.buildtool.dependencies.PythonDependencyDefinition
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.OsUtils
import optimus.buildtool.utils.Sandbox
import optimus.buildtool.utils.Utils
import optimus.platform.async
import optimus.platform.entity
import optimus.platform.node
import optimus.platform.util.Log
import optimus.stratosphere.artifactory.ArtifactoryToolDownloader

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.sys.process.Process
import scala.sys.process.ProcessLogger

final case class PythonEnvironment(
    venvCache: Directory,
    uvCache: Directory,
    pipCredentialFile: Option[FileAsset],
    pypiUvCredentialFile: Option[FileAsset]
)

object ThinPyappWrapper extends Log {
  private[buildtool] val Artifact = "{artifact}"
  private[buildtool] val Sources = "{sources}"
  private[buildtool] val VenvCache = "{venvCacheDir}"
  private[buildtool] val RequirementsFile = "{requirements}"
  private[buildtool] val PthFile = "{libsPth}"

  @async def runTpa(python: PythonDefinition, cmds: Seq[String], where: Option[Directory]): Unit = {
    val cmd = cmds.mkString(" && ")
    PythonLauncher.launchWithPython(python, "ThinPyappLaunch", cmd, where)
  }

  @async def runTpa(
      python: PythonDefinition,
      cmds: Seq[String],
      where: Option[Directory],
      pythonEnvironment: PythonEnvironment): Unit = {
    val commands = PythonLauncher.prerequisites(pythonEnvironment) ++ cmds
    val cmd = commands.mkString(" && ")
    PythonLauncher.launchWithPython(python, "ThinPyappLaunch", cmd, where)
  }

  @async def createTpa(
      artifact: Path,
      pythonConfig: PythonConfiguration,
      sandbox: Sandbox,
      pythonEnvironment: PythonEnvironment,
      buildAfsMapping: Boolean): Seq[CompilationMessage] = {
    import VenvProvider._
    import VenvUtils._

    val (venv, cacheMessages) =
      ensureVenvExists(pythonConfig.overriddenCommands, pythonConfig.python, pythonEnvironment)

    val thinPyappCommand = {
      def injectVars(cmd: String): String = cmd
        .replace(Artifact, artifact.toString)
        .replace(Sources, sandbox.sourceDir.pathString)
        .replace(VenvCache, pythonEnvironment.venvCache.pathString)
        .replace(RequirementsFile, sandbox.buildDir.resolveFile("requirements.txt").pathString)
        .replace(PthFile, sandbox.buildDir.resolveFile("libs.pth").pathString)

      val cmd = if (pythonConfig.isArtifactory) {
        createRequirements(sandbox.buildDir, pythonConfig)
        val maybeBuildMapping = if (buildAfsMapping) "--build-afs-mapping" else ""
        s"thin-pyapp --cache-dir ${pythonEnvironment.uvCache} --source ${sandbox.sourceDir} -r requirements.txt $artifact $maybeBuildMapping"
      } else {
        createPthFile(sandbox.buildDir, pythonConfig)
        s"thin-pyapp --cache-dir ${pythonEnvironment.uvCache} --source ${sandbox.sourceDir} --pth-file libs.pth $artifact"
      }
      pythonConfig.overriddenCommands.thinPyappCmd
        .map(injectVars)
        .getOrElse(cmd)
    }

    val cmds = PythonLauncher.prerequisites(pythonEnvironment) ++ Seq(
      s"source ${scripts(venv.toString)}activate",
      thinPyappCommand,
      "deactivate"
    )

    PythonLauncher.launchCompilation("ThinPyappLaunch", cmds.mkString(" && "), Some(sandbox.buildDir)) {
      case out if out.contains("ERROR") =>
        CompilationMessage(None, out, Severity.Error)
    } ++ cacheMessages
  }
}

object VenvUtils extends Log {
  private def libDefinition(lib: PythonDependencyDefinition): String = s"${lib.pypiName}==${lib.version}"

  def createRequirements(sandboxBuild: Directory, pythonConfig: PythonConfiguration): Unit = {
    val requirements = sandboxBuild.resolveFile("requirements.txt")
    Files.writeString(requirements.path, pythonConfig.artifactoryDependencies.map(libDefinition).mkString("\n"))
  }

  def createPthFile(sandboxBuild: Directory, pythonConfig: PythonConfiguration): Unit = {
    val pth = sandboxBuild.resolveFile("libs.pth")
    Files.writeString(pth.path, pythonConfig.afsDependencies.map(_.path).mkString("\n"))
  }

  def scripts(venvPath: String): String =
    if (Utils.isWindows) s"$venvPath\\Scripts\\"
    else s"$venvPath/bin/"
}

@entity object VenvProvider {
  private[buildtool] val CacheVenvName = "{cacheVenvName}"
  private[buildtool] val VenvCacheDir = "{venvCacheDir}"
  private[buildtool] val ValidCacheMarker = ".completed"

  import VenvUtils._

  private def pythonVenv(venvName: String, python: PythonDefinition): String =
    Seq(
      s"python -m venv --system-site-packages $venvName",
      s"${scripts(venvName)}python -m pip --disable-pip-version-check install morganstanley-optimus-thin-pyapp==${python.thinPyapp}"
    ).mkString("&&")

  private def cacheName(python: PythonDefinition): String = python.hash

  private def cachePath(python: PythonDefinition, cacheDir: Directory): Path =
    cacheDir.resolveDir(python.hash).path

  private def cacheExists(python: PythonDefinition, pythonEnvironment: PythonEnvironment): (Boolean, Boolean) = {
    val cacheDir = pythonEnvironment.venvCache
    (cacheDir.resolveDir(python.hash).exists, cacheDir.resolveDir(python.hash).resolveFile(ValidCacheMarker).exists)
  }

  @async private def create(
      overriddenCmds: OverriddenCommands,
      python: PythonDefinition,
      pythonEnvironment: PythonEnvironment): (Path, Seq[CompilationMessage]) = {
    val venvCache = pythonEnvironment.venvCache
    if (!pythonEnvironment.venvCache.exists) Files.createDirectories(venvCache.path)
    val cacheVenvName = cacheName(python)

    def injectVars(cmd: String): String = {
      cmd
        .replace(CacheVenvName, cacheVenvName)
        .replace(VenvCacheDir, venvCache.pathString)
    }

    val cmds = Seq(
      PythonLauncher.prerequisites(pythonEnvironment) ++
        Seq(
          overriddenCmds.pythonVenvCmd
            .map(injectVars)
            .getOrElse(pythonVenv(cacheVenvName, python)))
    ).flatten.mkString(" && ")

    val result = PythonLauncher.launchWithPython(python, "Venv", cmds, Some(venvCache))

    val cacheDir = cachePath(python, venvCache)
    Files.createFile(cacheDir.resolve(ValidCacheMarker))
    (cacheDir, result)
  }

  @async private def recreate(
      overriddenCmds: OverriddenCommands,
      python: PythonDefinition,
      pythonEnvironment: PythonEnvironment): (Path, Seq[CompilationMessage]) = {
    AssetUtils.recursivelyDelete(Directory(cachePath(python, pythonEnvironment.venvCache)))
    create(overriddenCmds, python, pythonEnvironment)
  }

  /* node is used here in order to synchronize async calls,
     we have to make sure that 2 processes aren't creating the same cache in the same time */
  @node def ensureVenvExists(
      overriddenCmds: OverriddenCommands,
      forPython: PythonDefinition,
      pythonEnvironment: PythonEnvironment): (Path, Seq[CompilationMessage]) = {
    val (exists, validCache) = cacheExists(forPython, pythonEnvironment)
    if (exists) {
      if (validCache) (cachePath(forPython, pythonEnvironment.venvCache), Nil)
      else recreate(overriddenCmds, forPython, pythonEnvironment)
    } else
      create(overriddenCmds, forPython, pythonEnvironment)
  }

  import optimus.buildtool.cache.NodeCaching.reallyBigCache
  `ensureVenvExists_info`.setCustomCache(reallyBigCache)
}

object PythonLauncher extends Log {
  def prerequisites(pythonEnvironment: PythonEnvironment): Seq[String] = {
    val setEnv = if (OsUtils.isWindows) "set" else "export"

    def set(key: String, value: Option[String]): Seq[String] =
      value match {
        case Some(value) if Files.exists(Paths.get(value)) => Seq(s"$setEnv $key=$value")
        case _ =>
          log.warn(s"$key not set; Pypi Artifactory access may fail")
          Seq.empty
      }

    set(ArtifactoryToolDownloader.PipConfigFile, pythonEnvironment.pipCredentialFile.map(_.pathString)) ++
      set(ArtifactoryToolDownloader.UvConfigFile, pythonEnvironment.pypiUvCredentialFile.map(_.pathString))
  }

  @async def launchWithPython(
      python: PythonDefinition,
      prefix: String,
      cmdline: String,
      workingDir: Option[Directory] = None): Seq[CompilationMessage] = {
    python.binPath match {
      case Some(pythonBinPath) =>
        launchCompilation(prefix, cmdline, workingDir, Seq(pythonBinPath)) {
          case out if out.contains("ERROR:") =>
            CompilationMessage(None, out, Severity.Error)
        }
      case None => Seq(CompilationMessage(None, python.notFoundMessage, Severity.Error))
    }
  }

  @async def launch(
      cmdLine: String,
      workingDir: Option[Directory],
      addToPath: Seq[String] = Seq.empty): (Int, Seq[String]) = {
    val cmds =
      if (Utils.isWindows) Seq("cmd.exe", "/c", cmdLine)
      else Seq("ksh", "-c", cmdLine)

    val pb = new ProcessBuilder(cmds: _*)
    workingDir.foreach(dir => pb.directory(dir.path.toFile))

    val pathEnv = {
      val currentPath = pb.environment().getOrDefault("PATH", "")
      val separator = if (Utils.isWindows) ";" else ":"
      val toAdd = addToPath.mkString("", separator, separator)
      toAdd + currentPath
    }
    pb.environment().put("PATH", pathEnv)
    pb.environment().remove("FPATH")

    val logging = mutable.Buffer[String]()

    logging += s"Command line: ${cmds.mkString(" ")}"
    val returnCode = Process(pb) ! ProcessLogger { s =>
      logging += s
    }
    logging += s"Return code: $returnCode"

    (returnCode, logging.toIndexedSeq)
  }

  @async def launchCompilation(
      prefix: String,
      cmdLine: String,
      workingDir: Option[Directory],
      addToPath: Seq[String] = Seq.empty)(pf: PartialFunction[String, CompilationMessage]): Seq[CompilationMessage] = {
    val (returnCode, logging) = launch(cmdLine, workingDir, addToPath)
    val messages = logging.collect(pf).toBuffer
    val failed = returnCode != 0
    if (failed) {
      logging.foreach(l => log.warn(s"$prefix $l"))
      val knownError = messages.exists(_.isError)
      if (!knownError)
        messages += CompilationMessage(
          None,
          s"""Process ${Seq(cmdLine).mkString(" ")} ended up with status code: $returnCode,
             |output: ${logging.mkString("\n")}""".stripMargin,
          Severity.Error
        )
    } else {
      logging.foreach(l => log.debug(s"$prefix $l"))
    }

    messages.toIndexedSeq
  }
}
