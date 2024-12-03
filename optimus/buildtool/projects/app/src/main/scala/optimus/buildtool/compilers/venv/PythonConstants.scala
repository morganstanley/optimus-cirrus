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
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Jars

import java.nio.file.Path
import java.nio.file.Paths
import scala.collection.immutable.Seq

object PythonConstants {
  object tpa {
    val ConfigFileName: String = "config.json"

    def unpackedArtifact(artifactName: String, config: TpaConfig): Path = {
      val user = System.getProperty("user.name")
      Paths.get(s"/var/tmp/$user/.tpa/artifacts/.$artifactName.tpa-${config.id}")
    }

    def unpackedArtifactSrc(artifactName: String, config: TpaConfig): Path =
      unpackedArtifact(artifactName, config).resolve("src")
    def unpackedArtifactVenv(artifactName: String, config: TpaConfig): Path =
      unpackedArtifact(artifactName, config).resolve("venv")
    def unpackedArtifactPython(artifactName: String, config: TpaConfig): Path =
      unpackedArtifactVenv(artifactName, config).resolve("bin").resolve("python")
    def unpackedArtifactScript(artifactName: String, script: String, config: TpaConfig): Path =
      unpackedArtifactSrc(artifactName, config).resolve(script)
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

    def runScriptCmd(artifactName: String, script: String, args: Seq[String], config: TpaConfig): String = {
      (Seq(unpackedArtifactPython(artifactName, config), unpackedArtifactScript(artifactName, script, config)) ++ args)
        .mkString(" ")
    }

    final case class TpaConfig(id: String)
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
}
