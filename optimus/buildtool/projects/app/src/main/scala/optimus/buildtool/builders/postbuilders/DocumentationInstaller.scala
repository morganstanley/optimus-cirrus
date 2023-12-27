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
package optimus.buildtool.builders.postbuilders

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.builders.postbuilders.installer.BundleFingerprintsCache
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.ParentId
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.Directory.RegularFileFilter
import optimus.buildtool.files.DirectoryFactory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.files.WorkspaceSourceRoot
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.HashedContent
import optimus.platform._

import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap

class DocumentationInstaller(
    scopeConfigSource: ScopeConfigurationSource,
    workspaceSourceRoot: WorkspaceSourceRoot,
    factory: DirectoryFactory,
    docBundle: MetaBundle,
    pathBuilder: InstallPathBuilder,
    bundleFingerprintsCache: BundleFingerprintsCache
) extends PostBuilder {
  val fileFilter: PathFilter = RegularFileFilter

  @async private def sources(absDocRoot: Directory): SortedMap[FileAsset, HashedContent] = {
    val sourceFolder = factory.lookupSourceFolder(workspaceSourceRoot, absDocRoot)
    if (sourceFolder.exists) {
      sourceFolder.findSourceFiles(fileFilter).map { case (id, c) =>
        workspaceSourceRoot.resolveFile(id.workspaceSrcRootToSourceFilePath) -> c
      }
    } else SortedMap.empty
  }

  @async def copy(absDocRoots: Seq[Directory]): Unit = {
    val bundleFingerprints = bundleFingerprintsCache.bundleFingerprints(docBundle)
    val docInstallDir = pathBuilder.dirForMetaBundle(docBundle, "doc")
    absDocRoots.apar.flatMap { absDocRoot =>
      val filesAndContent = sources(absDocRoot)
      val targetDir = docInstallDir.resolveDir(workspaceSourceRoot.relativize(absDocRoot))
      val hashedContentPerFile = filesAndContent.map { case (fileAsset, content) =>
        absDocRoot.relativize(fileAsset) -> content.hash
      }
      bundleFingerprints.writeIfAnyChanged(targetDir, hashedContentPerFile) {
        AssetUtils.recursivelyDelete(targetDir)
        filesAndContent.map {
          case (file, content) => {
            val target = docInstallDir.resolveFile(workspaceSourceRoot.relativize(file))
            Files.createDirectories(target.parent.path)
            if (file.exists) {
              AssetUtils.atomicallyCopy(file, target, replaceIfExists = true)
            } else {
              AssetUtils.atomicallyWrite(target, replaceIfExists = true) { tempFile =>
                Files.write(tempFile, content.utf8ContentAsString.getBytes(StandardCharsets.UTF_8))
              }
            }
            target
          }
        }
      }
    }
  }

  @async override def postProcessScopeArtifacts(id: ScopeId, artifacts: Seq[Artifact]): Unit = {
    val absDocRoots = scopeConfigSource.scopeConfiguration(id).absDocRoots
    copy(absDocRoots)
  }

  @async override def postProcessArtifacts(
      scopes: Set[ScopeId],
      artifacts: Seq[Artifact],
      successful: Boolean): Unit = {
    if (successful) {
      val parentIds: Set[ParentId] = scopes.apar.flatMap(scopeId => scopeId.parents)
      parentIds.foreach { id =>
        val root = scopeConfigSource.root(id)
        copy(Seq(root.resolveDir("doc")))
      }
    }
  }
}
