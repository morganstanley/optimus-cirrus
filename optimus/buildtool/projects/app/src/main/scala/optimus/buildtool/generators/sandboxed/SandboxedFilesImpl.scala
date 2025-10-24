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
package optimus.buildtool.generators.sandboxed

import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceFileId
import optimus.buildtool.generators.sandboxed.SandboxedBase.TypedContent
import optimus.buildtool.utils.Sandbox
import optimus.buildtool.utils.SandboxFactory
import optimus.platform._

@entity class SandboxedFilesImpl(
    sandboxFactory: SandboxFactory,
    protected val workspaceSourceRoot: Directory,
    val generatorId: String,
    protected val internalContent: Seq[TypedContent[RelativePath, SourceFileId]],
    protected val externalContent: Seq[TypedContent[Directory, FileAsset]],
) extends SandboxedFiles
    with SandboxedBase {
  import SandboxedFiles._

  // workspaceSourceRoot to source folder
  override protected type InternalLocation = RelativePath
  override protected type ExternalLocation = Directory

  @node def isEmpty: Boolean = internalContent.forall(_.isEmpty) && externalContent.forall(_.isEmpty)

  // Note that there's no guarantee that the internalFolders exist on disk (we may be reading them
  // from git, for example), so sandbox them to ensure they exist in a known location
  @node private def internalSandbox: Sandbox =
    sandboxFactory(
      generatorId,
      internalContent.map { c =>
        RelativePath(c.fileType.rootType.name) -> c.allContent
      }
    )

  @node def internalRoot(fileType: FileType = Template): Directory =
    internalSandbox.sourceDir(RelativePath(fileType.rootType.name))

  @node def roots(fileType: FileType = Template): Seq[Directory] =
    internalSandbox.sourceDir(RelativePath(fileType.rootType.name)) +:
      externalContent.filter(_.fileType == fileType).flatMap(_.content.map(_.location))

  @node def files(fileType: FileType = Template): Seq[FileAsset] =
    internalContentByType(fileType).keySet.apar.map(k => internalSandbox.source(k).file).toSeq ++
      externalContentByType(fileType).keys
  @node def allFiles: Seq[FileAsset] =
    internalSandbox.sources.map(_._2.file) ++ externalContent.flatMap(_.content).flatMap(_.files.keys)
}

object SandboxedFilesImpl {
  import optimus.buildtool.cache.NodeCaching.reallyBigCache
  // Ensure that we don't repeatedly recreate the sandbox for a given set of sources
  internalSandbox.setCustomCache(reallyBigCache)
}
