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

import optimus.stratosphere.bootstrap.StratosphereException
import optimus.stratosphere.config.StratoWorkspaceCommon

import java.nio.file.Path
import java.nio.file.Paths
import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.io.Source
import scala.util._
import scala.util.control.NonFatal

final case class LocalBranch(trackedRemoteName: Option[String])

final case class RemoteUrl(url: String) {
  def repoName: String = url.split('/').last.trim.stripSuffix(".git")
  def isFork: Boolean = url.contains("~") // [...]/scm/upstream_repo/repo.git versus [...]/scm/~user_id/repo.git
  def projectKey: String = url.split("/").takeRight(2).head
}

final case class RemoteSpec(name: String, remoteUrl: RemoteUrl, originUrl: Option[RemoteUrl] = None) {
  def isFork: Boolean = remoteUrl.isFork
  override def toString: String = s"$name -> ${remoteUrl.url}"
}

final case class GitLog(commit: String, date: String)

/**
 * Simple wrapper for git commands. Note: Beware git- (and not only) injection.
 */
final case class GitUtils(workspace: StratoWorkspaceCommon) {

  def diff(): String = runGit("diff")

  def diffNameOnly(commit: String, withPattern: String = ""): Seq[String] = {
    val pattern = if (withPattern.nonEmpty) Seq(withPattern) else Seq()
    val cmd = Seq("diff", "--name-only", commit) ++ pattern
    val res = runGit(cmd: _*)
    Source.fromString(res).getLines().to(Seq)
  }

  def add(what: String): String = runGit("add", what)

  def parents(commitHash: String = "HEAD"): Array[String] = {
    val result = runGit("rev-list", "--parents", "-n", "1", commitHash)
    result.split("\\s").drop(1)
  }

  def replace(source: String, target: String): String = runGit("replace", source, target)

  def undoReplace(source: String): String = runGit("replace", "-d", source)

  def addRemote(name: String, location: RemoteUrl): String = runGit("remote", "add", name, location.url)

  def setRemote(name: String, location: RemoteUrl): String = runGit("remote", "set-url", name, location.url)

  def removeRemote(name: String): String = runGit("remote", "remove", name)

  def disableTags(name: String): String = runGit("config", s"remote.$name.tagopt", "--no-tags")

  def disableGc(): String = runGit("config", "gc.auto", "0")

  def collectGarbage(): String = runGit("gc") + runGit("prune")

  def updateIndex(version: Int): String = {
    require(Seq(2, 3, 4).contains(version), s"Version must be one of: (2,3,4), was $version")
    runGit("update-index", "--index-version", version.toString)
  }

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

  def allRemotes(): Seq[RemoteSpec] = {
    val result = runGit("remote", "-v")
    result
      .split("\n")
      .flatMap { row =>
        row.split("\\s+") match {
          case Array(name, url)    => Some(RemoteSpec(name, RemoteUrl(url)))
          case Array(name, url, _) => Some(RemoteSpec(name, RemoteUrl(url)))
          case _                   => None
        }
      }
      .toList
      .distinct
  }

  def maybeLocalBranch(name: String): Option[LocalBranch] = {
    val localBranchExists = checkBranchExists(name)
    val result = Try(runGit("config", s"branch.$name.remote"))

    if (localBranchExists) {
      Some(LocalBranch(if (result.isSuccess) Some(result.get) else None))
    } else None
  }

  def checkBranchContainsRevision(branchName: String, revision: String): Boolean =
    listBranchesContainingRevision(revision).contains(branchName)

  def checkBranchExists(branchName: String): Boolean = Try(runGit("rev-parse", "--verify", branchName)).isSuccess

  def showFileFromRevision(ref: String, filePath: String): Option[String] = {
    Try(runGit("show", s"$ref:$filePath")).map(Some(_)).getOrElse(None)
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

  def hasLocalChanges(): Boolean = localChanges().nonEmpty

  def localChanges(): Seq[String] = Source.fromString(runGit("status", "--short")).getLines().to(Seq)

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

  def listDirectoryStructure(): Seq[String] = {
    val result = runGit("ls-tree", "-d", "-r", "HEAD", "--name-only")
    Source.fromString(result).getLines().to(Seq)
  }

  def listDirectoryStructure(gitRef: String): Seq[String] = {
    val result = runGit("ls-tree", "--name-only", gitRef)
    Source.fromString(result).getLines().to(Seq)
  }

  def lsRemoteTag(remote: String, tagName: String): Seq[String] = {
    val result = runGit("ls-remote", "--tags", remote, tagName)
    Source.fromString(result).getLines().to(Seq)
  }

  def merge(revisionOrBranch: String): String = runGit("merge", "--autostash", revisionOrBranch)

  def setUpstream(remoteName: String, remoteBranch: String, localBranch: String): String =
    runGit("branch", s"--set-upstream-to=$remoteName/$remoteBranch", localBranch)

  def status(): String = runGit("status")

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
        val Seq(commit, date, files @ _*) = Source.fromString(log).getLines().to(Seq).filterNot(line => line.isEmpty)
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

  private[utils] def retrieveBranchesNames(listBranchesOutput: String) =
    listBranchesOutput.split("""\s+|[\s*]+""").filter(_.nonEmpty).toList

  def runGit(args: String*): String = {
    val env = Map("GIT_ASK_YESNO" -> "false")
    CommonProcess.in(workspace).runGit(workspace.directoryStructure.sourcesDirectory, env)(args: _*)
  }
}
