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

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime

import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.PathUtils
import optimus.platform._
import org.eclipse.jgit.lib.ObjectId
import org.eclipse.jgit.lib.Repository
import org.eclipse.jgit.revwalk.RevCommit
import org.eclipse.jgit.treewalk.TreeWalk
import org.eclipse.jgit.treewalk.filter.TreeFilter
import org.eclipse.jgit.treewalk.filter.{PathFilter => GitPathFilter}

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap
import scala.collection.mutable

// Note: Do not add constructor arguments to GitSourceFolder without considering cache misses due to entity
// inequality (it's for this reason that commit/commitId are not constructor arguments)
@entity class GitSourceFolder private[files] (
    val workspaceSrcRootToSourceFolderPath: RelativePath,
    val workspaceSourceRoot: Directory,
    private[files] val repo: Repository,
    factory: SourceFolderFactory,
    treeId: ObjectId
) extends SourceFolder {
  import GitSourceFolder._

  private val relSourceFolderPath = workspaceSrcRootToSourceFolderPath.pathString.stripSuffix("/")
  private val hashing = GitHashing(workspaceSrcRootToSourceFolderPath, repo)

  @node override def exists: Boolean = treeId != ObjectId.zeroId

  @node override protected[files] def _findSourceFiles(
      filter: PathFilter,
      maxDepth: Int,
      scopeArtifacts: Boolean
  ): SortedMap[SourceFileId, HashedContent] = {
    val files = gitFiles(filter, maxDepth)
    hashing.hash(files, scopeArtifacts)
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
  ): Seq[GitFile] =
    if (exists) {
      val reader = repo.newObjectReader()
      val treeWalk = new TreeWalk(repo, reader)
      val absSourceFolderPath = workspaceSourceRoot.resolveDir(workspaceSrcRootToSourceFolderPath).pathString
      treeWalk.setFilter(new Filter(None, filter, maxDepth, absSourceFolderPath))
      treeWalk.reset(treeId)
      treeWalk.setRecursive(true)

      val files = mutable.Buffer[GitFile]()
      while (treeWalk.next()) {
        val path =
          if (workspaceSrcRootToSourceFolderPath == RelativePath.empty) treeWalk.getPathString
          else s"$relSourceFolderPath/${treeWalk.getPathString}"

        files += GitFile(path, treeWalk.getObjectId(0))
      }
      files.toIndexedSeq
    } else Nil

  override def toString: String =
    s"${getClass.getSimpleName}($relSourceFolderPath, ${PathUtils.platformIndependentString(repo.getWorkTree.toString)}, $treeId)"
}

@entity object GitSourceFolder {
  final case class GitFile(path: String, id: ObjectId)

  object GitFileAttributes extends BasicFileAttributes {
    private val EPOCH = FileTime.fromMillis(0)

    override def lastModifiedTime(): FileTime = EPOCH
    override def lastAccessTime(): FileTime = EPOCH
    override def creationTime(): FileTime = EPOCH
    override def isRegularFile: Boolean = true // git only stores files
    override def isDirectory: Boolean = false
    override def isSymbolicLink: Boolean = false
    override def isOther: Boolean = false
    override def size(): Long = 0
    override def fileKey(): AnyRef = null
  }

  class Filter(
      root: Option[GitPathFilter],
      filter: PathFilter,
      maxDepth: Int,
      absSourceFolderPath: String
  ) extends TreeFilter {
    override def include(walker: TreeWalk): Boolean = matchFilter(walker) <= 0

    override def matchFilter(walker: TreeWalk): Int = {
      if (walker.getDepth >= maxDepth) 1 // definitely no match
      else {
        val rootMatch = root.map(_.matchFilter(walker)).getOrElse(0)
        if (rootMatch == 1) 1 // definitely no match
        else if (rootMatch == 0 && filter(abs(walker.getPathString), GitFileAttributes)) 0 // definitely a match
        else if (walker.isSubtree) -1 // a subtree might match the PathFilter
        else 1 // definitely no match
      }
    }

    private def abs(pathString: String): String = s"$absSourceFolderPath/$pathString"

    override def shouldBeRecursive(): Boolean = true

    override def clone(): Filter = this
  }

  @node def apply(
      workspaceSrcRootToSourceFolderPath: RelativePath,
      workspaceSourceRoot: Directory,
      repo: Repository,
      factory: SourceFolderFactory,
      commit: RevCommit
  ): GitSourceFolder = {
    if (workspaceSrcRootToSourceFolderPath == RelativePath.empty) {
      GitSourceFolder(workspaceSrcRootToSourceFolderPath, workspaceSourceRoot, repo, factory, commit.getTree)
    } else {
      val walk = TreeWalk.forPath(repo, workspaceSrcRootToSourceFolderPath.pathString, commit.getTree)
      val tree = if (walk != null) walk.getObjectId(0) else ObjectId.zeroId
      GitSourceFolder(workspaceSrcRootToSourceFolderPath, workspaceSourceRoot, repo, factory, tree)
    }
  }
}

@entity private[files] class GitHashing(workspaceSrcRootToSourceFolderPath: RelativePath, repo: Repository) {
  import GitSourceFolder._

  // In a separate entity to prevent cache misses due to treeId changes
  @node def hash(
      files: Seq[GitFile],
      scopeArtifacts: Boolean
  ): SortedMap[SourceFileId, HashedContent] = {
    log.debug(s"Hashing ${files.size} git source files for ${workspaceSrcRootToSourceFolderPath.pathString}")
    val reader = repo.newObjectReader()
    val queue = reader.open(files.map(_.id).asJava, false)
    val pathsIter = files.map(_.path).iterator

    val contents = mutable.Buffer[(SourceFileId, HashedContent)]()
    while (queue.next()) {
      val p = pathsIter.next()
      val workspaceSrcRootToSourceFilePath = RelativePath(p)
      val id = if (scopeArtifacts) {
        val sourceFolderToSourceFilePath =
          workspaceSrcRootToSourceFolderPath.relativize(workspaceSrcRootToSourceFilePath)
        SourceFileId(workspaceSrcRootToSourceFilePath, sourceFolderToSourceFilePath)
      } else {
        SourceFileId(workspaceSrcRootToSourceFilePath, workspaceSrcRootToSourceFilePath)
      }

      val hashedContent = Hashing.hashFileInputStreamWithContent(p, () => queue.open().openStream())
      contents += ((id, hashedContent))
    }
    SortedMap(contents: _*)
  }
}
