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
package optimus.stratosphere.artifactory
import optimus.stratosphere.bootstrap.OsSpecific
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.utils.CommonProcess
import org.apache.commons.io.FileUtils

import java.nio.file.Files
import java.nio.file.Path

object VenvTool {
  def allVenvTools: Seq[VenvTool] = Seq(RuffTool)
}

trait VenvTool {
  def create(stratoWorkspace: StratoWorkspaceCommon): Path = {
    if (shouldBeInstalled(stratoWorkspace))
      reinstall(stratoWorkspace)
    binary(stratoWorkspace)
  }

  def binary(stratoWorkspace: StratoWorkspaceCommon): Path
  def shouldBeInstalled(stratoWorkspace: StratoWorkspaceCommon): Boolean
  def reinstall(stratoWorkspace: StratoWorkspaceCommon): Unit
  def venvName(stratoWorkspace: StratoWorkspaceCommon): String

  protected def venvPath(stratoWorkspace: StratoWorkspaceCommon): Path =
    stratoWorkspace.directoryStructure.toolsDir
      .resolve(venvName(stratoWorkspace))
}

object RuffTool extends VenvTool {
  def reinstall(stratoWorkspace: StratoWorkspaceCommon): Unit = {
    stratoWorkspace.log.info(s"Installing ruff ${stratoWorkspace.intellij.plugins.ruff.binaryVersion}")
    val venv = venvPath(stratoWorkspace)
    if (Files.exists(venv)) FileUtils.deleteDirectory(venv.toFile)

    val command = Seq("ksh", "-c") ++ Seq(
      Seq(
        s"module load python/core/${stratoWorkspace.intellij.plugins.ruff.pythonVersion}",
        s"python -m venv ${venvPath(stratoWorkspace)}",
        s"${venvPath(stratoWorkspace)}/bin/pip install ruff==${stratoWorkspace.intellij.plugins.ruff.binaryVersion}"
      ).mkString(" && "))

    new CommonProcess(stratoWorkspace)
      .runAndWaitFor(
        command,
        env = Map(ArtifactoryToolDownloader.PipConfigFile -> stratoWorkspace.internal.pypi.configFile.toFile.toString),
        ignoreExitCode = true
      )
    if (!binary(stratoWorkspace).toFile.exists()) stratoWorkspace.log.warning(s"Installing ruff failed")
  }

  def binary(stratoWorkspace: StratoWorkspaceCommon): Path =
    venvPath(stratoWorkspace)
      .resolve("bin")
      .resolve("ruff")

  def venvName(stratoWorkspace: StratoWorkspaceCommon): String = {
    val python = stratoWorkspace.intellij.plugins.ruff.pythonVersion
    val ruff = stratoWorkspace.intellij.plugins.ruff.binaryVersion
    s"venvtools-$python-ruff-$ruff"
  }

  def shouldBeInstalled(stratoWorkspace: StratoWorkspaceCommon): Boolean =
    OsSpecific.isLinux && stratoWorkspace.intellij.plugins.ruff.enabled && !binary(stratoWorkspace).toFile.exists()
}
