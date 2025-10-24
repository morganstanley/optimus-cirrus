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

import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.SourceFileId
import optimus.buildtool.utils.HashedContent
import optimus.platform._

import scala.collection.immutable.SortedMap

/**
 * Base trait containing APIs that are common to both SandboxedInputs (used in setup/fingerprinting)
 * and SandboxedFiles (used in generation).
 */
@entity trait Sandboxed {
  import SandboxedFiles._

  /** All internal files (ie. files from within the repo) for a given file type, along with their content */
  @node def internalContent(fileType: FileType = Template): SortedMap[SourceFileId, HashedContent]

  /** All internal files (ie. files from within the repo), along with their content */
  @node def allInternalContent: SortedMap[SourceFileId, HashedContent]

  /** All external files (ie. files from outside the repo) for a given file type, along with their content */
  @node def externalContent(fileType: FileType = Template): SortedMap[FileAsset, HashedContent]

  /** All external files (ie. files from outside the repo), along with their content */
  @node def allExternalContent: SortedMap[FileAsset, HashedContent]

  /**
   * Current contents (both internal and external) of the sandbox for a given file type.
   *
   * Note that the `FileAsset` paths refer to their canonical location; there's no guarantee that the
   * internal files actually exist at these locations (we may be reading them from git, for example), so no
   * attempt should be made to read those files directly. If you need that guarantee (if you're passing the
   * paths to an external process, for instance), then use [[SandboxedFiles.files]], which will return file
   * path within the sandbox to which they have been written.
   */
  @node def content(fileType: FileType = Template): SortedMap[FileAsset, HashedContent]
}
