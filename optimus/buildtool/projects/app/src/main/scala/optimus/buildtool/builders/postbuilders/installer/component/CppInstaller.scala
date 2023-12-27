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

import java.nio.file.Files
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.InternalCppArtifact
import optimus.buildtool.builders.postbuilders.installer.BundleFingerprintsCache
import optimus.buildtool.builders.postbuilders.installer.InstallableArtifacts
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.trace.InstallCpp
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.Jars
import optimus.buildtool.utils.OsUtils
import optimus.buildtool.utils.Utils
import optimus.platform._

import scala.collection.immutable.Seq

class CppInstaller(
    pathBuilder: InstallPathBuilder,
    bundleFingerprintsCache: BundleFingerprintsCache
) extends ComponentInstaller {

  override def descriptor: String = "C++ artifacts"

  @async override def install(installable: InstallableArtifacts): Seq[FileAsset] =
    install(installable.includedArtifacts(ArtifactType.Cpp))

  @async def install(artifacts: Seq[InternalCppArtifact]): Seq[FileAsset] =
    artifacts.apar.flatMap { a =>
      Jars.withJar(a.file) { d =>
        val installedReleaseFile = a.release.map(d.resolveFile).flatMap(f => install(a, f))
        val installedDebugFile = a.debug.map(d.resolveFile).flatMap(f => install(a, f))
        installedReleaseFile ++ installedDebugFile
      }
    }

  @async private def install(artifact: InternalCppArtifact, file: FileAsset): Option[FileAsset] = {
    val hash = Hashing.hashFileContent(file)
    val exec = artifact.osVersion match {
      case OsUtils.WindowsVersion => OsUtils.WindowsSysName
      case OsUtils.Linux7Version  => OsUtils.Linux7SysName
      case OsUtils.Linux6Version  => OsUtils.Linux6SysName
      case x                      => throw new IllegalArgumentException(s"Unrecognized OS version: $x")
    }
    val target = pathBuilder.dirForScope(artifact.scopeId, leaf = "lib", branch = s".exec/$exec").resolveFile(file.name)
    bundleFingerprintsCache.bundleFingerprints(artifact.scopeId).writeIfChanged(target, hash) {
      ObtTrace.traceTask(artifact.scopeId, InstallCpp) {
        Files.createDirectories(target.parent.path)
        AssetUtils.atomicallyCopy(file, target, replaceIfExists = true)
        if (!Utils.isWindows) {
          Files.setPosixFilePermissions(target.path, Utils.ExecuteBits)
        }
      }
    }

  }
}
