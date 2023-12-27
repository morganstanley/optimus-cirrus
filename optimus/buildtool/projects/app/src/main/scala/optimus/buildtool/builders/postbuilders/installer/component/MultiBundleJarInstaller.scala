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

import optimus.buildtool.builders.postbuilders.installer.BatchInstallableArtifacts
import optimus.buildtool.builders.postbuilders.installer.Installer
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.NamingConventions.scopeOutputName
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Hashing
import optimus.platform._

import scala.collection.immutable.Seq

class MultiBundleJarInstaller(installer: Installer) extends ComponentBatchInstaller {
  import installer._

  override val descriptor = "multi-bundle jars"

  @async override def install(installable: BatchInstallableArtifacts): Seq[FileAsset] = {
    installable.metaBundles.apar.flatMap(mb => installMultiBundleJars(mb, installable.allScopes)).toIndexedSeq
  }

  @async private def installMultiBundleJars(metaBundle: MetaBundle, scopeIds: Set[ScopeId]): Seq[JarAsset] = {
    // copy 'special' jars into the bundle lib directory, so they're available to runscripts
    scopeIds.apar.flatMap { scopeId =>
      val targetBundles = scopeConfigSource.scopeConfiguration(scopeId).targetBundles
      if (targetBundles.contains(metaBundle)) {
        val name = scopeOutputName(scopeId)
        val sourceJar = pathBuilder.libDir(scopeId).resolveJar(name)
        val targetDir = pathBuilder.libDir(metaBundle)
        val targetJar = targetDir.resolveJar(name)
        val newHash = Hashing.hashFileContent(sourceJar)
        bundleFingerprints(metaBundle).writeIfChanged(targetJar, newHash) {
          Files.createDirectories(targetDir.path)
          AssetUtils.copy(sourceJar, targetJar, replaceIfExists = true)
        }
      } else None
    }
  }.toIndexedSeq
}
