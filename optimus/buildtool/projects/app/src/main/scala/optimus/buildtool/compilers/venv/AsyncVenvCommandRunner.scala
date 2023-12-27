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
import optimus.buildtool.dependencies.PythonDependencyDefinition
import optimus.buildtool.files.Directory
import optimus.buildtool.utils.OsUtils
import optimus.buildtool.utils.Utils
import optimus.platform.async
import optimus.platform.node
import optimus.platform.util.Log

import java.nio.file.Files
import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.sys.process.Process
import scala.sys.process.ProcessLogger

object AsyncVenvCommandRunner extends Log {

  @async private def launch(prefix: String, cmdLine: String, workingDir: Directory, addToPath: Seq[String])(
      pf: PartialFunction[String, CompilationMessage]): Seq[CompilationMessage] = {
    val cmds =
      if (Utils.isWindows) Seq("cmd.exe", "/c", cmdLine)
      else Seq("ksh", "-c", cmdLine)

    val pb = new ProcessBuilder(cmds: _*)
    pb.directory(workingDir.path.toFile)

    val pathEnv = {
      val currentPath = pb.environment().getOrDefault("PATH", "")
      val separator = if (Utils.isWindows) ";" else ":"
      val toAdd = addToPath.mkString("", separator, separator)
      toAdd + currentPath
    }
    pb.environment().put("PATH", pathEnv)

    val logging = mutable.Buffer[String]()

    logging += s"Command line: ${cmds.mkString(" ")}"
    val returnCode = Process(pb) ! ProcessLogger { s =>
      logging += s
    }
    logging += s"Return code: $returnCode"

    val messages = logging.collect(pf)
    val failed = returnCode != 0

    if (failed) {
      logging.foreach(l => log.warn(s"$prefix $l"))
      val knownError = messages.exists(_.isError)
      if (!knownError)
        messages += CompilationMessage(
          None,
          s"Process ${cmds.mkString(" ")} ended up with status code: $returnCode",
          Severity.Error)
    } else {
      logging.foreach(l => log.debug(s"$prefix $l"))
    }

    messages.toIndexedSeq
  }

  @node def createVenv(
      venvName: String,
      pythonConfig: PythonConfiguration,
      sandboxBuild: Directory,
      pipCache: Directory): Seq[CompilationMessage] = {
    if (pythonConfig.isArtifactory) createRequirements(sandboxBuild, pythonConfig)
    val tmpVenv: String = s"$venvName-tmp"

    val commands = Seq(
      Seq(s"python -m venv --copies $tmpVenv") ++
        prerequisites() ++
        (if (pythonConfig.isArtifactory) installLibs(tmpVenv, pipCache) else Seq()) ++
        venvPack(tmpVenv, venvName, pythonConfig, pipCache) ++
        rmVenv(tmpVenv)
    ).flatten.mkString(" && ")

    launch("Venv", commands, sandboxBuild, Seq(pythonConfig.python.path)) {
      case out if out.contains("ERROR:") =>
        CompilationMessage(None, out, Severity.Error)
    }
  }

  private def prerequisites(): Seq[String] =
    if (OsUtils.isWindows) Seq()
    else
      Seq(
        "export HOME=/var/tmp/$USER",
        "module load msde/artifactory-eng-tools/prod",
        "setup_user -cli pypi"
      )

  private def rmVenv(dir: String): Seq[String] =
    if (Utils.isWindows) Seq(s"rmdir /s /q $dir")
    else Seq(s"rm -r -f $dir")
  private def libDefinition(lib: PythonDependencyDefinition): String = s"${lib.name}==${lib.version}"
  private def venvPack(
      venvPath: String,
      to: String,
      pythonConfiguration: PythonConfiguration,
      pipCache: Directory): Seq[String] = Seq(
    s"${scripts(venvPath)}python -m pip install venv-pack2==${pythonConfiguration.python.venvPack} --cache-dir ${pipCache.path}",
    s"${scripts(venvPath)}venv-pack -p $venvPath -o $to"
  )
  private def scripts(venvPath: String): String =
    if (Utils.isWindows) s"$venvPath\\Scripts\\"
    else s"$venvPath/bin/"
  private def installLibs(venvPath: String, pipCache: Directory): Seq[String] =
    Seq(s"${scripts(venvPath)}python -m pip install -r requirements.txt --cache-dir ${pipCache.path}")
  private def createRequirements(sandboxBuild: Directory, pythonConfig: PythonConfiguration): Unit = {
    val requirements = sandboxBuild.resolveFile("requirements.txt")
    Files.writeString(requirements.path, pythonConfig.artifactoryDependencies.map(libDefinition).mkString("\n"))
  }
}

object VenvUtils {
  def sitePackages(venv: String, pythonConfig: PythonConfiguration): String =
    if (OsUtils.isWindows) s"$venv/Lib/site-packages"
    else s"$venv/lib/python${majorMinorPython(pythonConfig)}/site-packages"

  /* extracts major and minor python version
  "3.10.10" -> "3.10"
  "3.10.10-1 -> "3.10"
   */
  def majorMinorPython(pythonConfig: PythonConfiguration): String =
    pythonConfig.python.version
      .split("\\.")
      .take(2)
      .mkString(".")
}
