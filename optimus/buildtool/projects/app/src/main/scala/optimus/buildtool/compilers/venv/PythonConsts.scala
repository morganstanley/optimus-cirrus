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

import java.nio.file.Path
import java.nio.file.Paths

object PythonConsts {
  object tpa {
    def unpackedArtifact(artifactName: String): Path = {
      val user = System.getProperty("user.name")
      Paths.get(s"/var/tmp/$user/.tpa/artifacts/.$artifactName.tpa")
    }

    def unpackedArtifactSrc(artifactName: String): Path = unpackedArtifact(artifactName).resolve("src")
    def unpackedArtifactVenv(artifactName: String): Path = unpackedArtifact(artifactName).resolve("venv")
    def unpackedArtifactPython(artifactName: String): Path =
      unpackedArtifactVenv(artifactName).resolve("bin").resolve("python")
    def unpackedArtifactScript(artifactName: String, script: String): Path =
      unpackedArtifactSrc(artifactName).resolve(script)

    def unpackCmd(
        artifact: Path,
        cacheDir: Option[Path] = None,
        unpackPath: Option[Path] = None,
        existingVenv: Option[Path] = None): String = {
      val cacheString = cacheDir.map(c => s"--cache-dir $c")
      val unpackString = unpackPath.map(up => s"--unpack-path $up")
      val existingVenvString = existingVenv.map(ev => s"--existing-venv $ev")

      (Seq("python", artifact) ++ cacheString ++ unpackString ++ existingVenvString).mkString(" ")
    }

    def runScriptCmd(artifactName: String, script: String, args: Seq[String]): String = {
      (Seq(unpackedArtifactPython(artifactName), unpackedArtifactScript(artifactName, script)) ++ args).mkString(" ")
    }
  }

}
