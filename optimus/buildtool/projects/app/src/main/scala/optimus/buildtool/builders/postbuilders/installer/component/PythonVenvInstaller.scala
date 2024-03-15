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
import optimus.buildtool.builders.postbuilders.installer.BundleFingerprintsCache
import optimus.buildtool.builders.postbuilders.installer.InstallableArtifacts
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.trace.InstallPython
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.OsUtils
import optimus.platform._
import optimus.stratosphere.filesanddirs.Unzip

import java.nio.file.Files
import scala.collection.immutable.Seq

class PythonVenvInstaller(
    pathBuilder: InstallPathBuilder,
    bundleFingerprintsCache: BundleFingerprintsCache
) extends ComponentInstaller {
  override def descriptor: String = "Python venv artifacts"
  @async override def install(installable: InstallableArtifacts): Seq[FileAsset] = {
    install(installable.includedArtifacts(ArtifactType.Python))
  }

  @async def install(artifacts: Seq[PythonArtifact]): Seq[FileAsset] = {
    artifacts.apar.flatMap(artifact => install(artifact))
  }

  @async def install(artifact: PythonArtifact): Seq[FileAsset] = {
    val target =
      pathBuilder
        .dirForScope(artifact.scopeId, leaf = "venvs", branch = s".exec/${OsUtils.exec}")
        .resolveDir(artifact.scopeId.module)

    val fpName = s"PythonVenv(${artifact.scopeId})"

    bundleFingerprintsCache
      .bundleFingerprints(artifact.scopeId)
      .writeIfKeyChanged[FileAsset](fpName, artifact.inputsHash) {
        ObtTrace.traceTask(artifact.scopeId, InstallPython) {
          Files.createDirectories(target.parent.path)
          Unzip.extract(artifact.file.path.toFile, target.path.toFile).map(path => FileAsset(path))
        }
      }
  }
}
