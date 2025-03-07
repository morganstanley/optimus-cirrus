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
package optimus.buildtool.builders.postbuilders.installer.component

import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.PythonArtifact
import optimus.buildtool.artifacts.PythonMetadata
import optimus.buildtool.builders.postbuilders.installer.BundleFingerprintsCache
import optimus.buildtool.builders.postbuilders.installer.InstallableArtifacts
import optimus.buildtool.compilers.venv.PythonConstants
import optimus.buildtool.compilers.venv.PythonEnvironment
import optimus.buildtool.compilers.venv.PythonLauncher
import optimus.buildtool.compilers.venv.VenvProvider
import optimus.buildtool.compilers.venv.VenvUtils
import optimus.buildtool.config.PythonConfiguration.OverriddenCommands
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.trace.InstallPythonArtifact
import optimus.buildtool.trace.InstallPythonWheels
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Jars
import optimus.platform._

import java.nio.file.Files
import scala.collection.immutable.Seq

sealed trait PythonInstallerConfiguration
final object InstallTpa extends PythonInstallerConfiguration
final case class InstallTpaWithWheels(pythonEnvironment: PythonEnvironment) extends PythonInstallerConfiguration

class PythonInstaller(
    pathBuilder: InstallPathBuilder,
    bundleFingerprintsCache: BundleFingerprintsCache,
    configuration: PythonInstallerConfiguration
) extends ComponentInstaller {
  override def descriptor: String = "Python artifacts"
  private val wheelsDir: Directory = pathBuilder.wheelsDir()

  @async override def install(installable: InstallableArtifacts): Seq[FileAsset] = {
    val pythonArtifacts = installable.includedArtifacts(ArtifactType.Python)
    install(pythonArtifacts)
  }

  @async def install(artifacts: Seq[PythonArtifact]): Seq[FileAsset] = {
    configuration match {
      case InstallTpa => artifacts.apar.flatMap(artifact => installTpa(artifact))
      case InstallTpaWithWheels(pythonEnvironment) =>
        val tpa = artifacts.apar.flatMap(artifact => installTpa(artifact))
        artifacts.aseq.foreach(artifact => installArtifactWheels(artifact, pythonEnvironment))
        val wheels = Directory.findFiles(wheelsDir)
        tpa ++ wheels
    }
  }

  @async
  def installTpa(artifact: PythonArtifact): Seq[FileAsset] = {
    val target = pathBuilder.pathForTpa(artifact.scopeId)
    val fpName = s"PythonTpa(${artifact.scopeId})"

    bundleFingerprintsCache
      .bundleFingerprints(artifact.scopeId)
      .writeIfKeyChanged[FileAsset](fpName, artifact.inputsHash) {
        ObtTrace.traceTask(artifact.scopeId, InstallPythonArtifact) {
          Files.createDirectories(target.parent.path)
          AssetUtils.atomicallyCopy(artifact.file, target, replaceIfExists = true)
          Seq(target)
        }
      }
  }

  @async
  def installArtifactWheels(artifact: PythonArtifact, pythonEnvironment: PythonEnvironment): Unit =
    ObtTrace.traceTask(artifact.scopeId, InstallPythonWheels) {
      val metadata = PythonMetadata.load(artifact.file.path)
      val targetRequirements = wheelsDir.resolveFile(s"requirements-${artifact.inputsHash}")
      Files.createDirectories(targetRequirements.parent.path)

      val file = JarAsset(artifact.path)
      Jars.withJar(file) { root =>
        val requirements = root.resolveFile("requirements")
        if (requirements.exists) {
          AssetUtils.atomicallyCopy(requirements, targetRequirements)
          val (venv, _) =
            VenvProvider.ensureVenvExists(OverriddenCommands.empty, metadata.python, pythonEnvironment)

          val cmd = PythonLauncher.prerequisites(pythonEnvironment) ++
            Seq(s"source ${VenvUtils.scripts(venv.toString)}activate") ++
            Seq(PythonConstants.pip.download(wheelsDir.path, targetRequirements.path).mkString(" ")) ++
            Seq("deactivate")

          val (returnCode, output) = PythonLauncher.launch(cmd.mkString(" && "), workingDir = None)
          if (returnCode != 0)
            throw new RuntimeException(s"""
                                          |Pip download failed with exit code $returnCode.
                                          |Output:
                                          |${output.mkString("\n")}
                                          |""".stripMargin)
          Files.delete(targetRequirements.path)
        }
      }
    }
}
