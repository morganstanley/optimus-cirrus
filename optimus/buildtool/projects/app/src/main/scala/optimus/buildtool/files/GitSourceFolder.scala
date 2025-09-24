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

import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.utils.GitFile
import optimus.buildtool.utils.GitUtils
import optimus.buildtool.utils.HashedContent
import optimus.platform._
import org.eclipse.jgit.lib.ObjectId
import org.eclipse.jgit.revwalk.RevCommit

import scala.collection.immutable.SortedMap

// Note: Do not add constructor arguments to GitSourceFolder without considering cache misses due to entity
// inequality (it's for this reason that commit/commitId are not constructor arguments)
@entity class GitSourceFolder private[files] (
    val workspaceSrcRootToSourceFolderPath: RelativePath,
    val workspaceSourceRoot: Directory,
    private[files] val gitUtils: GitUtils,
    factory: SourceFolderFactory,
    treeId: ObjectId
) extends SourceFolder {

  private val dir = workspaceSourceRoot.resolveDir(workspaceSrcRootToSourceFolderPath)
  private val relSourceFolderPath = workspaceSrcRootToSourceFolderPath.pathString.stripSuffix("/")

  @node override def exists: Boolean = treeId != ObjectId.zeroId

  @node override protected[files] def _findSourceFiles(
      filter: PathFilter,
      maxDepth: Int,
      scopeArtifacts: Boolean
  ): SortedMap[SourceFileId, HashedContent] = {
    val files = gitFiles(filter, maxDepth)
    gitUtils.hash(files).map { case (f, h) =>
      val workspaceSrcRootToSourceFilePath = RelativePath(f.path)
      val id = if (scopeArtifacts) {
        val sourceFolderToSourceFilePath =
          workspaceSrcRootToSourceFolderPath.relativize(workspaceSrcRootToSourceFilePath)
        SourceFileId(workspaceSrcRootToSourceFilePath, sourceFolderToSourceFilePath)
      } else {
        SourceFileId(workspaceSrcRootToSourceFilePath, workspaceSrcRootToSourceFilePath)
      }
      id -> h
    }
  }

  @node override protected def findFilesAbove(
      path: RelativePath,
      fileFilter: PathFilter
  ): SortedMap[SourceFileId, HashedContent] = {
    path.parentOption
      .map { parent =>
        val psf = factory.lookupSourceFolder(workspaceSourceRoot, workspaceSourceRoot.resolveDir(parent))
        val files = psf._findSourceFiles(fileFilter, maxDepth = 1, scopeArtifacts = false)
        if (parent == RelativePath.empty) files
        else findFilesAbove(parent, fileFilter) ++ files
      }
      .getOrElse(SortedMap.empty)
  }

  @node private def gitFiles(
      filter: PathFilter,
      maxDepth: Int = Int.MaxValue
  ): Seq[GitFile] = gitUtils.findFilesWithId(dir, treeId, filter, maxDepth)

  override def toString: String =
    s"${getClass.getSimpleName}($relSourceFolderPath, $treeId)"
}

@entity object GitSourceFolder {
  @node def apply(
      workspaceSrcRootToSourceFolderPath: RelativePath,
      workspaceSourceRoot: Directory,
      gitUtils: GitUtils,
      factory: SourceFolderFactory,
      commit: RevCommit
  ): GitSourceFolder = {
    val tree = gitUtils.treeId(workspaceSourceRoot.resolveDir(workspaceSrcRootToSourceFolderPath), commit)
    GitSourceFolder(workspaceSrcRootToSourceFolderPath, workspaceSourceRoot, gitUtils, factory, tree)
  }
}
