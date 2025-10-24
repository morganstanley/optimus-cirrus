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
package optimus.stratosphere.utils

import optimus.stratosphere.bitbucket.Project
import optimus.stratosphere.bitbucket.Repository
import optimus.stratosphere.bootstrap.StratosphereException
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.utils.GitUtils.GitApi

import java.net.URI
import java.nio.file.Path
import java.nio.file.Paths
import scala.collection.compat._
import scala.io.Source
import scala.util._
import scala.util.control.NonFatal

final case class LocalBranch(trackedRemoteName: Option[String])

object RemoteUrl {
  // Regex to extract instance URL, project key, and repo name in one go
  private val UrlPartsRegex = """https?://(?:.+@)?(stash[^.]+\.ms\.com)/.*?/([^/]+)/([^/]+?)(?:\.git)?$""".r

  def apply(url: String): RemoteUrl = new RemoteUrl(url)
}

final case class RemoteUrl(url: String) {
  // Extract parts using the regex
  private val urlParts = RemoteUrl.UrlPartsRegex.findFirstMatchIn(url)

  def instanceUrl: Option[String] = urlParts.map(_.group(1))

  def projectKey: String = urlParts.map(_.group(2)).getOrElse(url.split("/").takeRight(2).head)

  def repoName: String = urlParts.map(_.group(3)).getOrElse(url.split('/').last.trim.stripSuffix(".git"))

  def repository: Repository = Repository(Project(projectKey), repoName)

  def isFork: Boolean = url.contains("~") // [...]/scm/upstream_repo/repo.git versus [...]/scm/~user_id/repo.git

  def isSameRepo(other: RemoteUrl): Boolean = {
    val uri = new URI(url)
    val otherUri = new URI(other.url)
    uri.getHost == otherUri.getHost && uri.getPath == otherUri.getPath
  }
}

final case class RemoteSpec(name: String, remoteUrl: RemoteUrl, originUrl: Option[RemoteUrl] = None) {
  def isFork: Boolean = remoteUrl.isFork
  override def toString: String = s"$name -> ${remoteUrl.url}"
}

final case class GitLog(commit: String, date: String)

object GitUtils {
  final case class Branch(value: String) extends AnyVal {
    override def toString: String = value
  }

  final case class Remote(value: String) extends AnyVal {
    override def toString: String = value
  }

  final case class RemoteAndBranch(remote: Remote, branch: Branch) {
    override def toString: String = s"$remote/$branch"
  }

  final case class BranchesForMerge(source: RemoteAndBranch, target: RemoteAndBranch)

  trait GitApi {
    def checkBranchExists(remoteAndBranch: RemoteAndBranch): Boolean

    def fetch(remoteAndBranch: RemoteAndBranch): String

    /**
     * Returns the list of commit hashes that are present in the `source` branch but not in the `target` branch.
     * This uses `git rev-list target..source` to compute the difference.
     *
     * @param branches source and target branches where 'source' is a branch to compare
     *                 (usually the feature branch). And 'target' is a branch to compare
     *                 against (usually the base branch).
     * @return Sequence of commit hashes unique to `source` compared to `target`.
     */
    def commitsUniqueToSource(branches: BranchesForMerge): Seq[String]

    /**
     * Returns the list of commit hashes that are present in the `source` branch but not in the `target` branch.
     * This uses `git rev-list target..source` to compute the difference.
     *
     * @param localSource the local branch to compare (usually the feature branch)
     * @param target the remote and branch to compare against (usually the base branch)
     * @return Sequence of commit hashes unique to `localSource` compared to `target`
     */
    def commitsUniqueToSource(localSource: Branch, target: RemoteAndBranch): Seq[String]
  }
}

/**
 * Simple wrapper for git commands. Note: Beware git- (and not only) injection.
 */
final case class GitUtils(workspace: StratoWorkspaceCommon) extends GitApi {

  private val RemoteUrlRegex = "remote\\.(.+)\\.url\\s+(.+)".r
  private val MissingIndexRegex = """^warning: no corresponding \.idx:\s+(.+\.pack)$""".r

  def diff(): String = runGit("diff")

  def diffNameOnly(commit: String, withPattern: String = ""): Seq[String] = {
    val pattern = if (withPattern.nonEmpty) Seq(withPattern) else Seq()
    val cmd = Seq("diff", "--name-only", commit) ++ pattern
    runGit(cmd: _*).getLines()
  }

  def add(what: String): String = runGit("add", what)

  def parents(commitHash: String = "HEAD"): Array[String] = {
    val result = runGit("rev-list", "--parents", "-n", "1", commitHash)
    result.split("\\s").drop(1)
  }

  private def rawCommitsUniqueToSource(source: String, target: String): Seq[String] = {
    runGit("rev-list", s"$target..$source").getLines()
  }

  def commitsUniqueToSource(branches: GitUtils.BranchesForMerge): Seq[String] = {
    rawCommitsUniqueToSource(source = branches.source.toString, target = branches.target.toString)
  }

  def commitsUniqueToSource(localSource: GitUtils.Branch, target: GitUtils.RemoteAndBranch): Seq[String] = {
    rawCommitsUniqueToSource(source = localSource.toString, target = target.toString)
  }

  def replace(source: String, target: String): String = runGit("replace", source, target)

  def undoReplace(source: String): String = runGit("replace", "-d", source)

  def allReplaceRefs(): Seq[String] = runGit("replace", "-l").getLines()

  def addRemote(name: String, location: RemoteUrl): String = runGit("remote", "add", name, location.url)

  def addRemoteNoTags(name: String, location: RemoteUrl): String = runGit("add-remote", name, location.url)

  def setRemote(name: String, location: RemoteUrl): String = runGit("remote", "set-url", name, location.url)

  def removeRemote(name: String): String = runGit("remote", "remove", name)

  def disableTags(name: String): String = runGit("config", s"remote.$name.tagopt", "--no-tags")

  def disableGc(): String = runGit("config", "gc.auto", "0")

  def collectGarbage(): String = runGit("gc")

  def updateIndex(version: Int): String = {
    require(Seq(2, 3, 4).contains(version), s"Version must be one of: (2,3,4), was $version")
    runGit("update-index", "--index-version", version.toString)
  }

  def createMissingIndices(packFilesLimit: Int): Seq[String] = {
    countObjects()
      .collect { case MissingIndexRegex(packPath) => Paths.get(packPath) }
      .take(packFilesLimit)
      .map(indexPack)
  }

  def indexPack(path: Path): String =
    runGit("index-pack", path.toString)

  def writeCommitGraph(): String = runGit("commit-graph", "write", "--reachable")

  def originRemoteUrl(): RemoteUrl = remoteUrl("origin").get

  def remoteUrl(remoteName: String): Option[RemoteUrl] =
    Try(runGit("config", s"remote.$remoteName.url")) match {
      case Success(url) => Some(RemoteUrl(url))
      // if remote does not exist process will return exit code 1
      case Failure(_: StratosphereException) => None
      case Failure(throwable)                => throw throwable
    }

  def allRemoteUrls(): Seq[RemoteUrl] = allRemotes().map(_.remoteUrl)

  def allRemoteNames(): Seq[String] = allRemotes().map(_.name)

  def allRemotes(): Seq[RemoteSpec] =
    getConfig("remote\\..+\\.url").collect { case RemoteUrlRegex(name, url) =>
      RemoteSpec(name, RemoteUrl(url))
    }.distinct

  def maybeLocalBranch(name: String): Option[LocalBranch] = {
    val localBranchExists = checkBranchExists(name)
    val result = Try(runGit("config", s"branch.$name.remote"))

    if (localBranchExists) {
      Some(LocalBranch(if (result.isSuccess) Some(result.get) else None))
    } else None
  }

  def checkBranchContainsRevision(branchName: String, revision: String): Boolean =
    listBranchesContainingRevision(revision).contains(branchName)

  def checkBranchExists(branchName: String): Boolean = revParse(branchName).isDefined

  def checkBranchExists(remoteAndBranch: GitUtils.RemoteAndBranch): Boolean =
    runGit("ls-remote", "--heads", remoteAndBranch.remote.value, remoteAndBranch.branch.value).trim.nonEmpty

  def showFileFromRevision(ref: String, filePath: String): Option[String] = {
    Try(runGit("show", s"$ref:$filePath")).map(Some(_)).getOrElse(None)
  }

  def getConfig(regex: String): Seq[String] =
    Try(runGit("config", "--get-regexp", regex)) match {
      case Success(result)                   => result.getLines()
      case Failure(_: StratosphereException) => Seq.empty // git will exit with 1 if no config is found
      case Failure(throwable)                => throw throwable
    }

  def commit(message: String): String = runGit("commit", "--message", Text.doubleQuote(message))

  def createBranch(newBranch: String, fromBranch: String, force: Boolean = false): String = {
    val forceFlag = if (force) Seq("--force") else Seq()
    val cmd = Seq("branch", newBranch, fromBranch) ++ forceFlag
    runGit(cmd: _*)
  }

  def createBranchFromCurrent(newBranch: String, force: Boolean = false): String = {
    val forceFlag = if (force) Seq("--force") else Seq()
    val cmd = Seq("branch", newBranch) ++ forceFlag
    runGit(cmd: _*)
  }

  def rebase(commit: String): String = runGit("rebase", "--autostash", commit)

  def relativeAge(commit: String): String =
    runGit("log", "-1", "--format=%ar", commit).replaceAll("\r?\n", "")

  def currentBranch(): String = runGit("rev-parse", "--abbrev-ref", "HEAD").trim

  def cherryPick(commits: Seq[String]): String = runGit("cherry-pick" +: commits: _*)

  def hasLocalChanges(): Boolean = localChanges().nonEmpty

  def localChanges(): Seq[String] = runGit("status", "--short").getLines()

  def currentCommitHash(): String = runGit("rev-list", "--max-count=1", "HEAD")

  def deleteBranch(branchName: String, evenIfNotMerged: Boolean = false): String = {
    val deletionFlag = if (evenIfNotMerged) "-D" else "--delete"
    val gitCmd = Seq("branch", deletionFlag, branchName)
    runGit(gitCmd: _*)
  }

  def createTag(tagName: String, commit: String, force: Boolean = false): String = {
    if (force) runGit("tag", "--force", tagName, commit)
    else runGit("tag", tagName, commit)
  }

  private val noGc: Seq[String] = Seq("-c", "gc.auto=0")

  def fetchAll(): String = {
    val cmd = noGc ++ Seq("fetch", "--no-tags")
    runGit(cmd: _*)
  }

  def fetch(remoteRepo: String): String = {
    val cmd = noGc ++ Seq("fetch", remoteRepo, "--no-tags")
    runGit(cmd: _*)
  }

  /**
   * @deprecated use typesafe version of fetch: `fetch(remoteAndBranch: GitUtils.RemoteAndBranch): String`
   */
  def fetch(remoteRepo: String, branch: String): String = {
    val cmd = noGc ++ Seq("fetch", remoteRepo, branch, "--no-tags")
    runGit(cmd: _*)
  }

  def fetch(remoteAndBranch: GitUtils.RemoteAndBranch): String = {
    fetch(remoteAndBranch.remote.value, remoteAndBranch.branch.value)
  }

  def fetch(remoteRepo: String, remoteBranch: String, localBranch: String, force: Boolean = false): String = {
    val forceFlag = if (force) Seq("--force") else Seq()
    val cmd = noGc ++ Seq("fetch", remoteRepo, s"$remoteBranch:$localBranch", "--no-tags", "--verbose") ++ forceFlag
    runGit(cmd: _*)
  }

  def fetchAllTags(): String = runGit((noGc ++ Seq("fetch", "--tags")): _*)

  def fetchAllBranches(remoteRepo: String): String = runGit((noGc ++ Seq("fetch", remoteRepo, "--no-tags")): _*)

  def initRepo(): String = runGit("init", ".") ++ runGit("checkout", "-b", "main")

  def isShallow: Option[Boolean] = {
    Try {
      // very old git clients won't understand the command
      val output = runGit("rev-parse", "--is-shallow-repository")
      Try(output.toBoolean).toOption
    }.toOption.flatten
  }

  def listAllBranches(): Seq[String] = retrieveBranchesNames(runGit("branch"))

  def listBranchesContainingRevision(revision: String): Seq[String] = retrieveBranchesNames(
    runGit("branch", "--contains", revision))

  def listDirectoryStructure(): Seq[String] =
    runGit("ls-tree", "-d", "-r", "HEAD", "--name-only").getLines()

  def listDirectoryStructure(gitRef: String): Seq[String] =
    runGit("ls-tree", "--name-only", gitRef).getLines()

  def lsRemoteTag(remote: String, tagName: String): Seq[String] =
    runGit("ls-remote", "--tags", remote, tagName).getLines()

  def merge(revisionOrBranch: String): String = runGit("merge", "--autostash", revisionOrBranch)

  def setUpstream(remoteName: String, remoteBranch: String, localBranch: String): String =
    runGit("branch", s"--set-upstream-to=$remoteName/$remoteBranch", localBranch)

  def status(): String = runGit("status")

  def revParse(ref: String): Option[String] = Try(runGit("rev-parse", "--verify", ref)).toOption

  def resetKeep(resetTo: String): String = runGit("reset", "--keep", resetTo)

  def switchTo(revisionOrBranch: String): String = runGit("checkout", revisionOrBranch)

  def listIgnoredFiles(
      listDirectories: Boolean = true,
      additionalGitIgnorePatterns: Seq[String] = Seq.empty): Seq[String] = {
    val excludes = additionalGitIgnorePatterns.flatMap(pattern => Seq("--exclude", pattern))
    val directory = if (listDirectories) Seq("--directory") else Seq()
    val args = Seq("ls-files", "--others", "--ignored", "--exclude-standard") ++ directory ++ excludes
    val output = runGit(args: _*)
    output.split("\r?\n|\r").filter(_.nonEmpty).to(Seq)
  }

  def workingDirectoryIsClean(): Boolean = status().contains("nothing to commit")

  private val logSeparator = "----"

  def gitLogsForPattern(pattern: String): String =
    runGit("log", "--follow", s"""--pretty=$logSeparator%n%h%n%cs""", "--name-only", "--no-merges", pattern)

  def parseGitFileLogs(gitLogs: String): Map[Path, Seq[GitLog]] = {
    val logList = gitLogs.split(logSeparator).filterNot(log => log.isEmpty || log.equals("\n"))
    logList.foldLeft(Map.empty[Path, Seq[GitLog]]) { (acc, log) =>
      try {
        val Seq(commit, date, files @ _*) = log.getLines().filterNot(line => line.isEmpty)
        val gitLog = GitLog(commit, date)
        files.foldLeft(acc) { (innerAcc, file) =>
          val filePath = Paths.get(file)
          innerAcc.updated(filePath, innerAcc.getOrElse(filePath, Nil) :+ gitLog)
        }
      } catch {
        case NonFatal(e) => throw new RuntimeException(s"Error parsing line $log", e)
      }
    }
  }

  def countObjects(): Seq[String] =
    runGit("count-objects", "--verbose").getLines()

  private[utils] def retrieveBranchesNames(listBranchesOutput: String) =
    listBranchesOutput.split("""\s+|[\s*]+""").filter(_.nonEmpty).toList

  def runGit(args: String*): String = {
    val env = Map(
      "GIT_ADVICE" -> "0", // no hints please!
      "GIT_ASK_YESNO" -> "false",
    )
    CommonProcess.in(workspace).runGit(workspace.directoryStructure.sourcesDirectory, env)(args: _*)
  }

  private implicit class GitResultOps(result: String) {
    def getLines(): Seq[String] = Source.fromString(result).getLines().to(Seq)
  }
}
