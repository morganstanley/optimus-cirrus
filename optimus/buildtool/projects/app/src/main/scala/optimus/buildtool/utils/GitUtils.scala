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
package optimus.buildtool.utils

import java.io.InputStream
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.util.Collections
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.LocalDirectoryFactory
import optimus.buildtool.files.ReactiveDirectory
import optimus.buildtool.trace.ObtTrace
import optimus.git.diffparser.DiffParser
import optimus.platform._
import optimus.platform.util.Log
import optimus.stratosphere.bootstrap.GitProcess
import optimus.stratosphere.config.CustomWorkspace
import optimus.stratosphere.config.StratoWorkspace
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.lib.Constants
import org.eclipse.jgit.lib.ObjectId
import org.eclipse.jgit.lib.Repository
import org.eclipse.jgit.revwalk.RevCommit
import org.eclipse.jgit.revwalk.RevWalk
import org.eclipse.jgit.storage.file.FileRepositoryBuilder
import org.eclipse.jgit.treewalk.TreeWalk

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.sys.process.Process
import scala.sys.process.ProcessLogger

@entity class GitUtils(
    gitDir: Directory,
    reflog: ReactiveDirectory
) {

  val repo: Repository = new FileRepositoryBuilder().setGitDir(gitDir.path.toFile).build
  val git = new Git(repo)

  @node final def declareVersionDependence(): Unit = reflog.declareVersionDependence()

  @node def file(path: String): GitFile = {
    val repo = git.getRepository
    val head = commit(Constants.HEAD)
    val treeWalk = TreeWalk.forPath(repo, PathUtils.platformIndependentString(path), head.getTree.getId)
    val fileId = if (treeWalk != null) treeWalk.getObjectId(0) else ObjectId.zeroId
    GitFile(path, repo, fileId)
  }

  @node def commit(revStr: String): RevCommit = {
    declareVersionDependence()
    val repo = git.getRepository
    val walk = new RevWalk(repo)
    walk.parseCommit(repo.resolve(Constants.HEAD))
  }
}

object GitUtils extends Log {
  private case object HeadFilter extends PathFilter {

    override def apply(pathString: String, attrs: BasicFileAttributes): Boolean =
      attrs.isRegularFile && pathString.endsWith("HEAD")
    override def apply(path: Path, attrs: BasicFileAttributes): Boolean = attrs.isRegularFile && path.endsWith("HEAD")
  }

  @scenarioIndependent
  @node def apply(gitDir: Directory, directoryFactory: LocalDirectoryFactory): GitUtils = {
    val reflog = directoryFactory.lookupDirectory(gitDir.resolveDir("logs").path, HeadFilter, maxDepth = 1)
    GitUtils(gitDir, reflog)
  }

  @scenarioIndependent
  @node def find(
      srcDir: Directory,
      directoryFactory: LocalDirectoryFactory
  ): Option[GitUtils] = {
    val gitDir = srcDir.resolveDir(Constants.DOT_GIT)
    if (gitDir.existsUnsafe) {
      Some(GitUtils(gitDir, directoryFactory))
    } else {
      log.warn(s"No git directory found at $gitDir. Analysis will be stored without commit hash.")
      None
    }
  }
}

final case class GitFile(path: String, repo: Repository, fileId: ObjectId) {
  def withStream[A](f: InputStream => A): A = {
    val stream = repo.open(fileId).openStream()
    try f(stream)
    finally stream.close()
  }

  def exists: Boolean = fileId != ObjectId.zeroId
}

final case class NativeGitUtils(workspaceSourceRoot: Directory) extends Log {
  val gitProcess = new GitProcess(StratoWorkspace(CustomWorkspace(workspaceSourceRoot.parent.path)).config)

  def diffFiles(from: String): Set[FileAsset] = {
    val lines = execute("-C", workspaceSourceRoot.pathString, "diff", "--name-only", from)
    lines.filter(!_.startsWith("warning: ")).map(l => workspaceSourceRoot.resolveFile(l)).toSet
  }

  private[utils] def analyzeDiffOutput(output: Seq[String]): Map[FileAsset, Set[Int]] = {
    val diffs = DiffParser.parse(output.mkString("\n")).asScala
    diffs.map { diff =>
      val fileAsset = workspaceSourceRoot.resolveFile(diff.toFileName)
      val lineNumbers: Set[Int] = diff.hunks.flatMap { hunk =>
        val start = hunk.toFileRange.lineStart
        val count = hunk.toFileRange.lineCount
        start until (start + count)
      }.toSet
      fileAsset -> lineNumbers
    }.toMap
  }

  def diffLines(from: String): Map[FileAsset, Set[Int]] = {
    val diffOutput = execute("-C", workspaceSourceRoot.pathString, "diff", "-U0", from)
    analyzeDiffOutput(diffOutput)
  }

  private def execute(args: String*): Seq[String] = execute(args)

  private def execute(args: Iterable[String]): Seq[String] = {
    val output = mutable.Buffer[String]()
    val pb = gitProcess.createProcessBuilder(workspaceSourceRoot.path, Collections.emptyMap(), args.toSeq: _*)
    log.debug(s"Executing: ${pb.command().asScala.mkString(" ")}")
    val ret = Process(pb) ! ProcessLogger(msg => output += msg)
    if (ret != 0) {
      val msg = s"Git command failed:\n${output.mkString("\n")}"
      log.error(msg)
      ObtTrace.error(msg)
      throw new RuntimeException("Git command failed")
    }
    output.toIndexedSeq
  }
}
