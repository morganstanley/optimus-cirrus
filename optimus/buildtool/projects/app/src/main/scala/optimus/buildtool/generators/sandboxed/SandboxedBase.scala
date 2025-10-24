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
import optimus.buildtool.files.SourceFileId
import optimus.buildtool.generators.sandboxed.SandboxedFiles.FileType
import optimus.buildtool.generators.sandboxed.SandboxedFiles.Template
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.TypeClasses._
import optimus.platform._

import scala.collection.immutable.SortedMap

@entity trait SandboxedBase extends Sandboxed {
  import SandboxedBase._

  protected type InternalLocation
  protected type ExternalLocation

  protected val workspaceSourceRoot: Directory
  protected val internalContent: Seq[TypedContent[InternalLocation, SourceFileId]]
  protected val externalContent: Seq[TypedContent[ExternalLocation, FileAsset]]

  protected val internalContentByType: Map[FileType, SortedMap[SourceFileId, HashedContent]] =
    toTypeMap(internalContent)
  protected val externalContentByType: Map[FileType, SortedMap[FileAsset, HashedContent]] =
    toTypeMap(externalContent)

  @node def internalContent(fileType: FileType = Template): SortedMap[SourceFileId, HashedContent] =
    internalContentByType(fileType)
  @node def allInternalContent: SortedMap[SourceFileId, HashedContent] = internalContentByType.values.toSeq.merge

  @node def externalContent(fileType: FileType = Template): SortedMap[FileAsset, HashedContent] =
    externalContentByType(fileType)
  @node def allExternalContent: SortedMap[FileAsset, HashedContent] = externalContentByType.values.toSeq.merge

  // Note: There's no guarantee that the internal files exist at the paths referenced by `FileAsset` here
  // (we may be reading them from git, for example), so no attempt should be made to read those files. If
  // you need that guarantee (if you're passing the paths to an external process, for instance), then use
  // the `files` method below.
  @node def content(fileType: FileType = Template): SortedMap[FileAsset, HashedContent] =
    internalContent(fileType).map { case (id, c) =>
      workspaceSourceRoot.resolveFile(id.workspaceSrcRootToSourceFilePath) -> c
    } ++ externalContent(fileType)
}

object SandboxedBase {
  final case class TypedContent[+A, B: Ordering](
      fileType: FileType,
      content: Seq[Content[A, B]]
  ) {
    def allContent: SortedMap[B, HashedContent] = content.map(_.files).merge
    def isEmpty: Boolean = content.forall(_.files.isEmpty)
  }

  final case class Content[+A, B: Ordering](
      location: A,
      files: SortedMap[B, HashedContent]
  )

  private def toTypeMap[A, B: Ordering](
      seq: Seq[TypedContent[A, B]]
  ): Map[SandboxedFiles.FileType, SortedMap[B, HashedContent]] =
    seq
      .groupBy(_.fileType)
      .map { case (k, s) =>
        k -> s.flatMap(_.content).map(_.files).merge
      }
      .withDefaultValue(SortedMap.empty)
}
