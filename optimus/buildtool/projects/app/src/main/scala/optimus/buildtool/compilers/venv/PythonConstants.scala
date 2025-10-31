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

import optimus.buildtool.artifacts.JsonImplicits._
import optimus.buildtool.config.StaticConfig
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Jars

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import scala.util.Properties.isWin

object PythonConstants {
  object tpa {
    val ConfigFileName: String = "config.json"

    val username = System.getProperty("user.name")
    val configDir: String = s"/var/tmp/$username/.tpa/config"
    val uvConfigFilePath: String = s"$configDir/uv.toml"
    val pipConfigFilePath: String = s"$configDir/pip.ini"
    val UV_CONFIG_FILE: String = "UV_CONFIG_FILE"
    val PIP_CONFIG_FILE: String = "PIP_CONFIG_FILE"

    def unpackedArtifact(artifactName: String, config: TpaConfig): Path = {
      if (isWin) throw new IllegalStateException("Windows is not supported")
      else Paths.get(s"/var/tmp/$username/.tpa/artifacts/.$artifactName.tpa-${config.id}")
    }

    def unpackedArtifactSrc(artifactName: String, config: TpaConfig): Path =
      unpackedArtifact(artifactName, config).resolve("src")
    def unpackedArtifactVenv(artifactName: String, config: TpaConfig): Path =
      unpackedArtifact(artifactName, config).resolve("venv")
    def unpackedArtifactPython(artifactName: String, config: TpaConfig): Path =
      unpackedArtifactVenv(artifactName, config).resolve("bin").resolve("python")
    def unpackedArtifactScript(artifactName: String, script: String, config: TpaConfig): Path =
      unpackedArtifactSrc(artifactName, config).resolve(script)

    def unpackCmdRawArgs(
        artifact: Path,
        tpaArgs: Seq[String]
    ): String = (Seq("python", artifact.toString) ++ tpaArgs).mkString(" ")

    def unpackCmd(
        artifact: Path,
        cacheDir: Option[Path] = None,
        unpackPath: Option[Path] = None,
        existingVenv: Option[Path] = None,
        findLinks: Option[Path] = None,
        index: Boolean = true): String = {
      val cacheString = cacheDir.map(c => s"--cache-dir $c")
      val unpackString = unpackPath.map(up => s"--unpack-path $up")
      val existingVenvString = existingVenv.map(ev => s"--existing-venv $ev")
      val findLinksString = findLinks.map(fl => s"--find-links $fl")
      val indexString = if (index) None else Some("-ni")

      (Seq("python", artifact) ++ cacheString ++ unpackString ++ existingVenvString ++ findLinksString ++ indexString)
        .mkString(" ")
    }

    def runScriptCmd(
        artifactName: String,
        script: String,
        args: Seq[String],
        config: TpaConfig,
        test: Boolean = false): String = {
      val pythonExecutable = if (test) "pytest" else "python"
      (Seq(
        unpackedArtifactVenv(artifactName, config).resolve("bin").resolve(pythonExecutable),
        unpackedArtifactScript(artifactName, script, config)) ++ args)
        .mkString(" ")
    }

    final case class TpaConfig(id: String, afs_mappings_exist: Boolean)
    object TpaConfig {
      def load(tpa: Path): TpaConfig = {
        val file = JarAsset(tpa)
        Jars.withJar(file) { root =>
          val metadataJson = root.resolveFile(RelativePath(ConfigFileName)).asJson
          AssetUtils.readJson[TpaConfig](metadataJson, unzip = false)
        }
      }
    }
  }

  object pip {
    def setupCmd: String = s"${StaticConfig.string(
        "artifactorySetupUserPath")} -cli pypi -repo mspypi-local-only --config-root ${PythonConstants.tpa.configDir}"
    def setupCmdIfNeeded: Option[String] = if (
      !Files.exists(Paths.get(PythonConstants.tpa.uvConfigFilePath)) || !Files.exists(
        Paths.get(PythonConstants.tpa.pipConfigFilePath))
    ) Some(setupCmd)
    else None

    def download(
        destination: Path,
        requirements: Path,
        cacheDir: Option[Path] = None,
        requireHashes: Boolean = true): Seq[String] = {
      val cacheString = Some(cacheDir.map(c => s"--cache-dir $c").getOrElse("--no-cache-dir"))
      val destString = Some(s"--dest $destination")
      val requirementsString = Some(s"--requirement $requirements")
      val requireHashesString = if (requireHashes) Some("--require-hashes") else None
      Seq(
        "pip",
        "download",
        "--prefer-binary",
        "--no-deps") ++ cacheString ++ requireHashesString ++ requirementsString ++ destString
    }
  }
}
