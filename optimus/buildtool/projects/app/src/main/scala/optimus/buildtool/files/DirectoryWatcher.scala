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
package optimus.buildtool.files

import java.nio.file.Path

import optimus.buildtool.files.Directory.NoFilter
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.Directory.RegularFileFilter

/**
 * Abstraction for checking for changes in directories
 */
private[files] trait DirectoryWatcher {

  /**
   * Registers the specified path to be watched. All files and directories under rootPath which match notificationFilter
   * will be watched (note that notificationFilter doesn't have to match the parent directories in order to be watched).
   */
  def registerPath(
      rootPath: Path,
      notificationFilter: PathFilter = RegularFileFilter,
      dirFilter: PathFilter = NoFilter,
      maxDepth: Int = Int.MaxValue): Unit

  /**
   * Gets the set of registered root Paths which changed (i.e. a file or subdirectory was added, modified, or removed)
   * since the last call to this method.
   *
   * Note that this is not the set of subdirectories or files that changed - it's the root directories under which there
   * were changes.
   */
  def getModifiedPathsAndReset(): Set[Path]

  /**
   * Gets the set of paths that are currently being watched
   */
  def watchedPaths: Set[Path]

  /**
   * Number of files checked on last scan
   */
  def scannedFiles: Int

  /**
   * Releases any resources associated with this watcher
   */
  def close(): Unit
}
