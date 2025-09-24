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
import optimus.buildtool.files.Directory.NoFilter
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.LocalDirectoryFactory
import optimus.buildtool.files.ReactiveDirectory
import optimus.buildtool.files.RelativePath
import optimus.buildtool.trace.ObtStats
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
import org.eclipse.jgit.treewalk.filter.TreeFilter
import org.eclipse.jgit.treewalk.filter.{PathFilter => GitPathFilter}
import org.eclipse.jgit.util.FS

import java.io.InputStream
import java.nio.file.Files
import java.nio.file.PathMatcher
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime
import java.util.Collections
import scala.collection.compat._
import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.sys.process.Process
import scala.sys.process.ProcessLogger

@entity class GitUtils(
    workspaceSourceRoot: Directory,
    gitDir: Directory,
    reflogDir: ReactiveDirectory,
    configDir: ReactiveDirectory,
    sparseCheckoutDir: ReactiveDirectory
) {
  import GitUtils._

  val repo: Repository = new FileRepositoryBuilder().setGitDir(gitDir.path.toFile).build
  val git = new Git(repo)

  @node final def declareReflogVersionDependence(): Unit = reflogDir.declareVersionDependence()

  @node def file(path: FileAsset): GitFile = {
    val repo = git.getRepository
    val head = commit(Constants.HEAD)
    val pathStr = workspaceSourceRoot.relativize(path).pathString
    val treeWalk = TreeWalk.forPath(repo, pathStr, head.getTree.getId)
    val fileId = if (treeWalk != null) treeWalk.getObjectId(0) else ObjectId.zeroId
    GitFile(pathStr, repo, fileId)
  }

  @node def findFiles(dir: Directory, filter: PathFilter = NoFilter, maxDepth: Int = Int.MaxValue): Seq[GitFile] = {
    val tree = treeId(dir, commit(Constants.HEAD))
    findFilesWithId(dir, tree, filter, maxDepth)
  }

  @node private[buildtool] def findFilesWithId(
      dir: Directory,
      treeId: ObjectId,
      filter: PathFilter,
      maxDepth: Int = Int.MaxValue): Seq[GitFile] =
    if (treeId != ObjectId.zeroId) {
      val reader = repo.newObjectReader()
      val treeWalk = new TreeWalk(repo, reader)
      treeWalk.setFilter(new Filter(None, filter, maxDepth, dir))
      treeWalk.reset(treeId)
      treeWalk.setRecursive(true)

      val relDir = workspaceSourceRoot.relativize(dir)
      val relDirStr = relDir.pathString.stripSuffix("/")

      val files = mutable.Buffer[GitFile]()
      while (treeWalk.next()) {
        val path =
          if (relDir == RelativePath.empty) treeWalk.getPathString
          else s"$relDirStr/${treeWalk.getPathString}"
        files += GitFile(path, repo, treeWalk.getObjectId(0))
      }
      files.toIndexedSeq
    } else Nil

  @node def treeId(dir: Directory, commit: RevCommit): ObjectId = {
    val relPath = workspaceSourceRoot.relativize(dir)
    if (relPath == RelativePath.empty) {
      commit.getTree
    } else {
      val walk = TreeWalk.forPath(repo, relPath.pathString, commit.getTree)
      if (walk != null) walk.getObjectId(0) else ObjectId.zeroId
    }
  }

  @node def hash(files: Seq[GitFile]): SortedMap[GitFile, HashedContent] = {
    val reader = repo.newObjectReader()
    val queue = reader.open(files.map(_.fileId).asJava, false)
    val fileIter = files.iterator

    val contents = mutable.Buffer[(GitFile, HashedContent)]()
    while (queue.next()) {
      val f = fileIter.next()
      ObtTrace.addToStat(ObtStats.ReadGitSourceFiles, 1)
      ObtTrace.addToStat(ObtStats.ReadDistinctGitSourceFiles, Set(f.path))
      val hashedContent = Hashing.hashFileInputStreamWithContent(f.path, () => queue.open().openStream())
      contents += (f -> hashedContent)
    }
    SortedMap(contents.to(Seq): _*)
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
    log.debug("Loading git config via jgit")
    val mainConfig = git.getRepository.getConfig
    // jgit doesn't seem to handle the worktreeConfig logic internally
    if (repo.getConfig.getBoolean("extensions", "worktreeConfig", false)) {
      val configWorktreeFile = configDir.resolveFile(ConfigWorktreeFile).path.toFile
      log.debug(s"Loading ${configWorktreeFile} (because extensions.worktreeConfig = true)")
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
      log.debug(s"Loading ${sparseFile.path}")
      // we've declared version dependence, so this is safe here
      if (sparseFile.existsUnsafe) {
        val configLines = Files.readAllLines(sparseFile.path).asScala.to(Seq)
        log.debug(s"Read ${configLines.size} lines from ${sparseFile.path}")
        Some(GitSparseMatcher(gitDir.parent, configLines))
      } else {
        log.warn(s"sparsecheckout was enabled in git but ${sparseFile.path} was not present")
        None
      }
    } else {
      log.debug("sparsecheckout mode was not enabled in git")
      None
    }
  }
}

object GitUtils extends Log {
  private val ConfigWorktreeFile = "config.worktree"
  private val ConfigFile = "config"
  private val SparseCheckoutFile = "sparse-checkout"

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
      dir: Directory
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

    private def abs(pathString: String): String = s"${dir.pathString}/$pathString"

    override def shouldBeRecursive(): Boolean = true

    override def clone(): Filter = this
  }

  @scenarioIndependent @node private def filteredDirectory(
      dir: Directory,
      directoryFactory: LocalDirectoryFactory,
      filenames: String*) =
    directoryFactory.lookupDirectory(dir.path, Directory.fileNamePredicate(filenames: _*), maxDepth = 1)

  @scenarioIndependent @node def apply(srcDir: Directory, directoryFactory: LocalDirectoryFactory): GitUtils = {
    val gitDir = srcDir.resolveDir(Constants.DOT_GIT)
    val reflogDir = filteredDirectory(gitDir.resolveDir("logs"), directoryFactory, Constants.HEAD)
    // note that ConfigFile is loaded implicitly by jgit so we must watch it too
    val configDir = filteredDirectory(gitDir, directoryFactory, ConfigFile, ConfigWorktreeFile)
    val sparseCheckoutDir = filteredDirectory(gitDir.resolveDir("info"), directoryFactory, SparseCheckoutFile)
    GitUtils(srcDir, gitDir, reflogDir = reflogDir, configDir = configDir, sparseCheckoutDir = sparseCheckoutDir)
  }

  @scenarioIndependent
  @node def find(
      srcDir: Directory,
      directoryFactory: LocalDirectoryFactory
  ): Option[GitUtils] = {
    val gitDir = srcDir.resolveDir(Constants.DOT_GIT)
    if (gitDir.existsUnsafe) {
      Some(GitUtils(srcDir, directoryFactory))
    } else {
      log.warn(s"No git directory found at $gitDir. Analysis will be stored without commit hash.")
      None
    }
  }
}

object GitFile {
  implicit val ordering: Ordering[GitFile] = Ordering.by(_.path)
}
final case class GitFile(path: String, repo: Repository, fileId: ObjectId) {
  def withStream[A](f: InputStream => A): A = {
    val stream = repo.open(fileId).openStream()
    try f(stream)
    finally stream.close()
  }

  def exists: Boolean = fileId != ObjectId.zeroId
}

final case class GitStatus(modifiedFiles: Set[FileAsset], untrackedFiles: Set[FileAsset])

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

  // `status` uses the untracked cache, so is faster than `ls-files` (at the expense of not showing the content
  // of untracked dirs)
  def status(): GitStatus = {
    val linesStatus = execute("-C", workspaceSourceRoot.pathString, "status", "--porcelain")
    val (untracked, modifiedFileStrs) = linesStatus.partition(_.startsWith("??"))
    val (untrackedDirStrs, untrackedFileStrs) = untracked.partition(_.endsWith("/"))

    val modifiedFiles = files(modifiedFileStrs.map { s =>
      // handle renames like: `R  foo.txt -> bar.txt`
      val renameIndex = s.indexOf(" -> ")
      val index = if (renameIndex > 0) renameIndex + 4 else 3
      s.substring(index)
    })
    val untrackedFiles = files(untrackedFileStrs.map(_.substring(3)))
    val untrackedDirs = dirs(untrackedDirStrs.map(_.substring(3)))
    val untrackedDirFiles = untrackedDirs.flatMap(d => Directory.findFiles(d).toSet)

    GitStatus(modifiedFiles, untrackedFiles ++ untrackedDirFiles)
  }

  private def files(lines: Seq[String]): Set[FileAsset] =
    lines
      .filter(!_.contains(": "))
      .map(l => workspaceSourceRoot.resolveFile(l))
      .toSet

  private def dirs(lines: Seq[String]): Set[Directory] =
    lines
      .filter(!_.contains(": "))
      .map(l => workspaceSourceRoot.resolveDir(l))
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
      val msg = s"Git command failed (exit code $ret):\n${output.mkString("\n")}"
      log.error(msg)
      ObtTrace.error(msg)
      throw new RuntimeException(s"Git command failed with exit code $ret")
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
