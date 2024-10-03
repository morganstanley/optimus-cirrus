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

import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.LocalDirectoryFactory
import optimus.buildtool.files.ReactiveDirectory
import optimus.buildtool.trace.ObtTrace
import optimus.git.diffparser.DiffParser
import optimus.platform._
import optimus.platform.util.Log
import optimus.stratosphere.bootstrap.GitProcess
import optimus.stratosphere.config.StratoWorkspace
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.lib.Constants
import org.eclipse.jgit.lib.ObjectId
import org.eclipse.jgit.lib.Repository
import org.eclipse.jgit.lib.StoredConfig
import org.eclipse.jgit.revwalk.RevCommit
import org.eclipse.jgit.revwalk.RevWalk
import org.eclipse.jgit.storage.file.FileBasedConfig
import org.eclipse.jgit.storage.file.FileRepositoryBuilder
import org.eclipse.jgit.treewalk.TreeWalk
import org.eclipse.jgit.util.FS

import java.io.InputStream
import java.nio.file.Files
import java.nio.file.PathMatcher
import java.util.Collections
import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.sys.process.Process
import scala.sys.process.ProcessLogger

@entity class GitUtils(
    gitDir: Directory,
    reflogDir: ReactiveDirectory,
    configDir: ReactiveDirectory,
    sparseCheckoutDir: ReactiveDirectory
) {
  import GitUtils._

  val repo: Repository = new FileRepositoryBuilder().setGitDir(gitDir.path.toFile).build
  val git = new Git(repo)

  @node final def declareReflogVersionDependence(): Unit = reflogDir.declareVersionDependence()

  @node def file(path: String): GitFile = {
    val repo = git.getRepository
    val head = commit(Constants.HEAD)
    val treeWalk = TreeWalk.forPath(repo, PathUtils.platformIndependentString(path), head.getTree.getId)
    val fileId = if (treeWalk != null) treeWalk.getObjectId(0) else ObjectId.zeroId
    GitFile(path, repo, fileId)
  }

  @node def commit(revStr: String): RevCommit = {
    declareReflogVersionDependence()
    val repo = git.getRepository
    val walk = new RevWalk(repo)
    walk.parseCommit(repo.resolve(revStr))
  }

  @node def mergedConfig: StoredConfig = {
    // note that getConfig will automatically reload the config if the files have changed but we must declare dependence
    // so that this node is invalidated in cache
    configDir.declareVersionDependence()
    log.info("Loading git config via jgit")
    val mainConfig = git.getRepository.getConfig
    // jgit doesn't seem to handle the worktreeConfig logic internally
    if (repo.getConfig.getBoolean("extensions", "worktreeConfig", false)) {
      val configWorktreeFile = configDir.resolveFile(ConfigWorktreeFile).path.toFile
      log.info(s"Loading ${configWorktreeFile} (because extensions.worktreeConfig = true)")
      val mergedConfig = new FileBasedConfig(mainConfig, configWorktreeFile, FS.DETECTED)
      mergedConfig.load()
      mergedConfig
    } else mainConfig
  }

  @node def isSparseCheckoutEnabled: Boolean =
    mergedConfig.getBoolean("core", "sparsecheckout", false)

  @node def local(dir: Directory): Boolean = sparseMatcher.forall(m => m.local(dir)) // default to `true`

  @node private[utils] def sparseMatcher: Option[GitSparseMatcher] = {
    if (isSparseCheckoutEnabled) {
      sparseCheckoutDir.declareVersionDependence()
      val sparseFile = sparseCheckoutDir.resolveFile(SparseCheckoutFile)
      log.info(s"Loading ${sparseFile.path}")
      // we've declared version dependence, so this is safe here
      if (sparseFile.existsUnsafe) {
        val configLines = Files.readAllLines(sparseFile.path).asScala.to(Seq)
        log.info(s"Read ${configLines.size} lines from ${sparseFile.path}")
        Some(GitSparseMatcher(gitDir.parent, configLines))
      } else {
        log.warn(s"sparsecheckout was enabled in git but ${sparseFile.path} was not present")
        None
      }
    } else {
      log.info("sparsecheckout mode was not enabled in git")
      None
    }
  }
}

object GitUtils extends Log {
  private val ConfigWorktreeFile = "config.worktree"
  private val ConfigFile = "config"
  private val SparseCheckoutFile = "sparse-checkout"

  @scenarioIndependent @node private def filteredDirectory(
      dir: Directory,
      directoryFactory: LocalDirectoryFactory,
      filenames: String*) =
    directoryFactory.lookupDirectory(dir.path, Directory.fileNamePredicate(filenames: _*), maxDepth = 1)
  @scenarioIndependent
  @node def apply(gitDir: Directory, directoryFactory: LocalDirectoryFactory): GitUtils = {
    val reflogDir = filteredDirectory(gitDir.resolveDir("logs"), directoryFactory, Constants.HEAD)
    // note that ConfigFile is loaded implicitly by jgit so we must watch it too
    val configDir = filteredDirectory(gitDir, directoryFactory, ConfigFile, ConfigWorktreeFile)
    val sparseCheckoutDir = filteredDirectory(gitDir.resolveDir("info"), directoryFactory, SparseCheckoutFile)
    GitUtils(gitDir, reflogDir = reflogDir, configDir = configDir, sparseCheckoutDir = sparseCheckoutDir)
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

final case class NativeGitUtils(workspaceSourceRoot: Directory, ws: StratoWorkspace) extends Log {
  val gitProcess = new GitProcess(ws.config)

  def diffFiles(from: String): Set[FileAsset] = {
    val linesModified = execute("-C", workspaceSourceRoot.pathString, "diff", "--name-only", from)
    files(linesModified)
  }

  def untrackedFiles(): Set[FileAsset] = {
    val linesUntracked = execute("-C", workspaceSourceRoot.pathString, "ls-files", "--others", "--exclude-standard")
    files(linesUntracked)
  }

  private def files(lines: Seq[String]): Set[FileAsset] =
    lines
      .filter(!_.startsWith("warning: "))
      .map(l => workspaceSourceRoot.resolveFile(l))
      .toSet

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

@entity class GitSparseMatcher(workspaceSourceRoot: Directory, private[utils] val config: Seq[String]) {
  // Patterns is ordered by most to least-specific (ie. in the opposite order to git config;
  // this means the first one to match wins
  private val patterns: Seq[(PathMatcher, Boolean)] = {
    // Note that we don't use `pathString` here since we don't want a leading '/' to be converted to '//' on linux
    val rootStr = workspaceSourceRoot.path.toString.replace('\\', '/')
    config.map { c =>
      val (raw, include) = if (c.startsWith("!")) (c.substring(1), false) else (c, true)
      val glob = if (raw.endsWith("/")) s"$rootStr$raw**" else s"$rootStr$raw"
      val pm = workspaceSourceRoot.fileSystem.getPathMatcher(s"glob:$glob")
      (pm, include)
    }.reverse
  }

  @node def local(dir: Directory): Boolean =
    patterns
      .collectFirst {
        // matching globs are file-based by virtue of the "**" suffix, so test against a dummy file within the
        // directory
        case (pm, include) if pm.matches(dir.resolveFile("dummy").path) =>
          include
      }
      .getOrElse(true)
}
