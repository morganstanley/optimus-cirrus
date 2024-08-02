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
package optimus.stratosphere.catchup

import optimus.stratosphere.bootstrap.StratosphereAbortException
import optimus.stratosphere.bootstrap.StratosphereException
import optimus.stratosphere.bootstrap.config.migration.truncation.HistoryTruncationMigration
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.sparse.SparseUtils
import optimus.stratosphere.updater.GitUpdater
import optimus.stratosphere.utils.GitUtils
import optimus.stratosphere.utils.IntervalPrinter.timeThis

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.immutable.Seq
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

object CatchUp {
  private final case class GitReference(tagName: String, commit: String)

  val AutoConfirm: String => Boolean = _ => true

  def currentDate: String = {
    val format = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss")
    format.format(Calendar.getInstance().getTime)
  }

  val fetchBranch = "stratosphere/catchUp/target"
  private val CatchupHead = "CATCHUP_HEAD"

  private val CommitTagExtractor = """(\w+)\s+refs/tags/(.*)""".r

  private object CatchupProgress {
    val postStateInit = 0.1
    val repoStateChecked = 0.2
    val fetchFinished = 0.5
    val cacheFound = 0.6
    val rebased = 0.8
    val sparseRefreshed = 1.0
  }
}

class CatchUp(
    ws: StratoWorkspaceCommon,
    doUserConfirm: String => Boolean,
    displayProgressBar: Double => Unit,
    isCanceled: () => Boolean,
    args: CatchUpParams) {

  private val logger = ws.log

  private def gitUtil = GitUtils(ws)
  import CatchUp._
  import args._

  def run(): Unit = {
    step(CatchupProgress.postStateInit)

    if (new HistoryTruncationMigration(ws.directoryStructure.stratosphereWorkspaceDir, ws.config).isMigrationNeeded) {
      abort("Migration is required. Please run 'stratosphere migrate' first.")
    }

    val currBranch = getCurrentBranch()
    logger.info(s"Current branch: $currBranch")

    checkWorkspaceState(currBranch)
    step(CatchupProgress.repoStateChecked)

    val remoteToUse = prepareRemoteForCatchup()
    step(CatchupProgress.fetchFinished)

    val bestRef = findBestFromGitTag(remoteToUse)
    step(CatchupProgress.cacheFound)

    bestRef match {
      case Some(gitRef) =>
        catchupWithCommit(currBranch, gitRef)
        step(CatchupProgress.rebased, canCancel = false)

        SparseUtils.refresh()(ws)
        step(CatchupProgress.sparseRefreshed, canCancel = false)
      case _ =>
        abort(s"No good commit was found in $branchToUse from $remoteName.")
    }
  }

  private def checkWorkspaceState(currBranch: String): Unit = {
    logger.info("Checking for local changes...")
    val changes = gitUtil.localChanges()

    val untrackedFilePrefix = "?? "
    val (untrackedFiles, otherFiles) = changes.partition(_.startsWith(untrackedFilePrefix))
    if (untrackedFiles.nonEmpty) {
      logger.info(
        s"ignoring ${untrackedFiles.size} untracked file(s)/folder(s), which are unlikely to affect catchup: ${untrackedFiles
            .map(_.stripPrefix(untrackedFilePrefix))
            .mkString(",")}")
    }

    val isOnLocalBranch = currBranch != branchToUse
    val hasLocalChanges = otherFiles.nonEmpty

    if (isMerge && !isOnLocalBranch) {
      abort(
        s"""You are on '$branchToUse' and used '--merge' which would result in nonlinear history.
           |Please rerun catchup without '--merge' argument or contact ${ws.internal.helpMailGroup} if you are in doubt.""".stripMargin)
    }

    if (isOnLocalBranch && hasLocalChanges) {
      val mergeRebaseMsg =
        if (isMerge) s"Do you want to merge changes from $branchToUse' into your local '$currBranch' branch anyway?"
        else s"Do you want to rebase your '$currBranch' branch on top of '$branchToUse' branch anyway?"
      val questionMsg = s"You have ${otherFiles.size} uncommitted file(s). $mergeRebaseMsg"
      if (!doUserConfirm(questionMsg)) abort("Abort requested by user", abortedByUser = true)
    }
  }

  private def catchupWithCommit(currBranch: String, gitRef: GitReference): Unit = {
    val backupTag = s"BACKUP-$currBranch-$currentDate"
    val GitReference(catchupTag, commit) = gitRef
    val isOnLocalBranch = currBranch != branchToUse

    gitUtil.createTag(CatchupHead, commit, force = true)
    gitUtil.createTag(backupTag, "HEAD", force = true)

    try {
      logger.info(s"Moving $branchToUse to latest good commit: $commit...")
      if (isOnLocalBranch) gitUtil.createBranch(branchToUse, CatchupHead, force = true)
      else gitUtil.resetKeep(commit)

      val result = if (isMerge) {
        logger.info(s"Merging latest good commit: $commit to current branch...")
        gitUtil.merge(commit)
      } else if (isOnLocalBranch) {
        logger.info(s"Rebasing current branch on latest good commit: $commit...")
        gitUtil.rebase(commit)
      } else ""

      if (result.contains("Applying autostash resulted in conflicts.")) {
        throw new StratosphereException(result)
      }
    } catch {
      case NonFatal(e) =>
        abort(s"""Catchup failed, please resolve the conflicts and proceed as advised by Git:
                 |${e.getMessage}""".stripMargin)
    } finally {
      logger.info(s"""
                     |Previous state of your workspace was tagged as $backupTag.
                     |If needed you can run `git checkout $backupTag` to get it back.
                     |""".stripMargin)
    }

    logger.info(s"Successfully caught up with remote tag $catchupTag from ${gitUtil.relativeAge(commit)}")
    if (!isMerge && isOnLocalBranch)
      logger.warning(s"""Rebase mode was used, so please anticipate using the --force (-f) flag on push.
                        |For more info see https://stackoverflow.com/questions/8939977/8940299""".stripMargin)
    gitUtil.deleteBranch(fetchBranch, evenIfNotMerged = true)
  }

  private def findBestFromGitTag(remote: String): Option[GitReference] = {
    val tagName = s"CATCHUP_$branchToUse"

    logger.info(s"Checking remote hash for $tagName...")

    timeThis("fetch-git-catchup-tag", ws.log) {
      val bestRef = getRemoteTag(remote, tagName).collectFirst {
        case CommitTagExtractor(commit, tag) if tag == tagName => GitReference(tagName = tag, commit = commit)
      }
      if (bestRef.isEmpty) logger.warning(s"Remote git tag $tagName not found!")
      bestRef
    }
  }

  protected def getRemoteTag(remote: String, tagName: String): Seq[String] =
    gitUtil.lsRemoteTag(remote, tagName)

  private def prepareRemoteForCatchup(): String = {
    val catchupRemoteName = ws.catchUp.remote.name

    val catchupRemoteUrl = GitUpdater.catchupRemoteUrl(ws)
    val currentCatchupRemote = gitUtil.remoteUrl(catchupRemoteName)

    currentCatchupRemote match {
      case Some(remoteUrl) =>
        if (remoteUrl != catchupRemoteUrl) {
          Try(gitUtil.setRemote(catchupRemoteName, catchupRemoteUrl)) match {
            case Success(_) =>
              logger.info(
                s"Catchup remote '$remoteName' url updated from ${remoteUrl.url} to '${catchupRemoteUrl.url}'")
            case Failure(ex) =>
              throw new StratosphereException(
                s"Catchup remote '$remoteName' url cannot be updated. Please contact ${ws.internal.helpMailGroup}",
                ex)
          }
        }
        Try(gitUtil.disableTags(catchupRemoteName)) match {
          case Success(_) =>
          case Failure(ex) =>
            throw new StratosphereException(
              s"Failed to set disable fetching tags for '$remoteName'. Please contact ${ws.internal.helpMailGroup}.",
              ex
            )
        }
      case None =>
        Try {
          gitUtil.addRemote(catchupRemoteName, catchupRemoteUrl)
          gitUtil.disableTags(catchupRemoteName)
        } match {
          case Success(_) =>
            logger.info(s"Catchup remote '$remoteName' was added.")
          case Failure(ex) =>
            throw new StratosphereException(
              s"Catchup remote '$remoteName' does not exist. Please contact ${ws.internal.helpMailGroup}.",
              ex)
        }
    }

    logger.info(s"Resolved catchup remote to use: $remoteName")
    runFetchIfNeeded(remoteName)

    // We only set tracking once if remote was missing and was just created
    if (currentCatchupRemote.isEmpty) {
      val defaultBranch = ws.catchUp.defaultBranch
      if (Try(gitUtil.setUpstream(catchupRemoteName, defaultBranch, defaultBranch)).isSuccess) {
        logger.info(s"'$defaultBranch' branch set to track '$catchupRemoteName' remote.")
      } else {
        logger.warning(s"Unable to set '$defaultBranch' branch to track '$catchupRemoteName' remote.")
      }
    }

    remoteName
  }

  private def getCurrentBranch(): String = {
    Try(gitUtil.currentBranch()).toOption
      .filter("HEAD (no branch)" != _)
      .getOrElse(abort(s"""Your git HEAD is detached state.
                          |Please checkout $branchToUse or your feature branch and rerun the catchup.""".stripMargin))
  }

  private def runFetchIfNeeded(remote: String): Unit = {
    def runFetch() = Try(gitUtil.fetch(remote, branchToUse, fetchBranch, force = true))

    logger.info(s"Fetching $remote/$branchToUse")
    if (runFetch().isFailure) {
      logger.warning(s"Fetch failed. Retrying...")
      runFetch() match {
        case Failure(e) => throw new StratosphereException("Fetch failed, see logs for details", e)
        case _          =>
      }
    }
  }

  private def step(fraction: Double, canCancel: Boolean = true): Unit = {
    displayProgressBar(fraction)
    if (canCancel && isCanceled()) abort("Abort requested by user", abortedByUser = true)
  }

  private def abort(msg: String, abortedByUser: Boolean = false): Nothing = {
    logger.warning(s"Catchup aborted: $msg")
    throw new StratosphereAbortException(msg, abortedByUser)
  }
}
