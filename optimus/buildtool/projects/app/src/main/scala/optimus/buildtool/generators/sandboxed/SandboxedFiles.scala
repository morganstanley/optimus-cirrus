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
import optimus.platform._

/**
 * A sandbox containing files needed for source generation.
 */
@entity trait SandboxedFiles extends Sandboxed {
  import SandboxedFiles._

  def generatorId: String

  /** True if no files have been captured by the sandbox */
  @node def isEmpty: Boolean

  /** Internal root location (ie. for files from within the repo) in the sandbox for a given file type */
  @node def internalRoot(fileType: FileType = Template): Directory

  /** All root locations (both internal and external) for a given file type */
  @node def roots(fileType: FileType = Template): Seq[Directory]

  /**
   * All files for a given file type, at their sandboxed location.
   *
   * Internal files (ie. those from within the repo) will have been sandboxed, so these paths are safe to
   * read directly or to pass to an external process. However, for code called from within the OBT process
   * prefer using the [[optimus.buildtool.utils.HashedContent]] from [[Sandboxed.content]] if possible,
   * to avoid unnecessary file I/O.
   */
  @node def files(fileType: FileType = Template): Seq[FileAsset]
}

object SandboxedFiles {

  trait FileType {
    def name: String = getClass.getSimpleName.stripSuffix("$")
    // by default, put each file type in a different content root
    def rootType: FileType = this
  }
  case object Template extends FileType

}
