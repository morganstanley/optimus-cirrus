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
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.files.FileAsset
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Hashing
import optimus.platform._

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.collection.immutable.Set

class DependencyFileInstaller(installer: Installer) extends ComponentBatchInstaller {
  import installer._

  override val descriptor = "dependency files"

  @async override def install(installable: BatchInstallableArtifacts): Seq[FileAsset] =
    installDependencyFiles()

  @async private def installDependencyFiles() = {
    val allVersions: Set[String] = for {
      scopeId <- scopeConfigSource.compilationScopeIds.apar
      dep <- scopeConfigSource.scopeConfiguration(scopeId).externalRuntimeDependencies.filter(!_.isMaven)
    } yield s"${dep.group}.${dep.name} = ${dep.version}"
    val allVersionsSeq = allVersions.toIndexedSeq.sorted

    val newVersionHash = Hashing.hashStrings(allVersionsSeq)
    val versionsFile = installDir.resolveFile("versions.properties")
    val installedVersionsFile = bundleFingerprints(RootScopeId).writeIfChanged(versionsFile, newVersionHash) {
      val versionsFile = installDir.resolveFile("versions.properties")
      AssetUtils.atomicallyWrite(versionsFile, replaceIfExists = true, localTemp = true) { tempPath =>
        Files.write(tempPath, allVersionsSeq.asJava)
      }
    }

    val nativePaths: Set[String] = for {
      scopeId <- scopeConfigSource.compilationScopeIds.apar
      dep <- scopeConfigSource.scopeConfiguration(scopeId).externalNativeDependencies
      path <- dep.paths ++ dep.extraPaths.map(_.pathString)
    } yield path
    val nativePathsSeq = nativePaths.toIndexedSeq.sorted

    val newNativePathsHash = Hashing.hashStrings(nativePathsSeq)
    val nativeDepsFile = installDir.resolveFile("native-dependencies.txt")
    val installedNativeDepsFile = bundleFingerprints(RootScopeId).writeIfChanged(nativeDepsFile, newNativePathsHash) {
      AssetUtils.atomicallyWrite(nativeDepsFile, replaceIfExists = true, localTemp = true) { tempPath =>
        Files.write(tempPath, nativePathsSeq.asJava)
      }
    }

    Seq(installedVersionsFile, installedNativeDepsFile).flatten
  }

}
