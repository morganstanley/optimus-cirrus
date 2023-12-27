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

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.jar.JarOutputStream
import java.util.zip.ZipEntry

import optimus.buildtool.builders.postbuilders.installer.BatchInstallableArtifacts
import optimus.buildtool.builders.postbuilders.installer.BundleFingerprintsCache
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.files.JarAsset
import optimus.buildtool.trace.InstallVersionJar
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.Jars
import optimus.platform._

import scala.collection.immutable.Seq

class VersionInstaller(
    cache: BundleFingerprintsCache,
    pathBuilder: InstallPathBuilder,
    installVersion: String,
    commit: Option[String]
) extends ComponentBatchInstaller {

  override val descriptor = "version files"

  @async override def install(installable: BatchInstallableArtifacts): Seq[FileAsset] = {
    installable.metaBundles.apar.flatMap(installVersionJar).toIndexedSeq
  }

  @async private def installVersionJar(mb: MetaBundle): Option[FileAsset] = {
    val versionJar = this.versionJar(mb)

    val newHash = Hashing.hashStrings(fingerprint)
    cache.bundleFingerprints(mb).writeIfChanged(versionJar, newHash) {
      ObtTrace.traceTask(RootScopeId, InstallVersionJar(mb)) {
        writeVersionJar(mb, versionJar)
      }
    }
  }

  private[component] def versionJar(mb: MetaBundle): JarAsset = {
    val libDir = pathBuilder.libDir(mb)
    libDir.resolveJar(s"$mb.version.jar")
  }

  private[component] def writeVersionJar(mb: MetaBundle, versionJar: JarAsset): Unit = {
    Files.createDirectories(versionJar.parent.path)
    AssetUtils.atomicallyWrite(versionJar, replaceIfExists = true, localTemp = true) { tempPath =>
      val manifest = Jars.createManifest()
      val jarStream = new JarOutputStream(Files.newOutputStream(tempPath), manifest)
      try {
        jarStream.putNextEntry(new ZipEntry(s"com/ms/${mb.meta}/${mb.bundle}/version.txt"))
        jarStream.write(installVersion.getBytes(StandardCharsets.UTF_8))
        // write it, i guess, even if empty? not sure it matters.
        jarStream.putNextEntry(new ZipEntry(s"com/ms/${mb.meta}/${mb.bundle}/environmentvariables.txt"))
        commit foreach { commit =>
          jarStream.write(s"GIT_COMMIT=$commit".getBytes(StandardCharsets.UTF_8))
        }
      } finally jarStream.close()
    }
  }

  private def fingerprint: Seq[String] = installVersion +: commit.toList

}
