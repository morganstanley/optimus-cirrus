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
import optimus.stratosphere.config.CustomWorkspace
import optimus.stratosphere.config.StratoWorkspace
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.filesanddirs.PathsOpts._
import optimus.stratosphere.logger.CentralLogger
import optimus.stratosphere.sparse.SparseUtils
import optimus.stratosphere.utils.GitUtils
import optimus.stratosphere.utils.IntervalPrinter.timeThis
import optimus.stratosphere.utils.RemoteUrl
import optimus.utils.ExitCode

import java.nio.file.Path
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.immutable.Seq
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

object CatchUp {
  private sealed trait CatchupRef
  private final case class CacheReference(distance: Int, commit: String, description: String) extends CatchupRef
  private final case class GitReference(tagName: String, commit: String) extends CatchupRef

  val NonInteractive: String => Boolean = _ => false

  def currentDate: String = {
    val format = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss")
    format.format(Calendar.getInstance().getTime)
  }

  val backupBranch = "stratosphere/backup/sync"
  val fetchBranch = "stratosphere/catchUp/target"
  private val CatchupHead = "CATCHUP_HEAD"

  private val ObtCacheResult = ".*Cache hit: commit=([0-9a-z]+) distance=([0-9]+).*".r
  private val CommitTagExtractor = """(\w+)\s+refs/tags/(.*)""".r
}

final case class CatchUpObtInput(args: Seq[String])
final case class CatchUpObtOutput(exitCode: Int, output: String)
final case class CatchUpObtContext(input: CatchUpObtInput, workspace: StratoWorkspaceCommon, log: CentralLogger)

class CatchUp(
    ws: StratoWorkspaceCommon,
    askIf: String => Boolean,
    displayProgressBar: Double => Unit,
    obtRunner: CatchUpObtContext => CatchUpObtOutput,
    isCanceled: () => Boolean,
    args: CatchUpParams) {

  private val logger = ws.log

  private def gitUtil = GitUtils(ws)
  import CatchUp._
  import args._

  def run(): Unit = {
    // for progress bar
    val postStateInit = 0.1
    val fetchFinished = 0.3
    val cacheFound = 0.6
    val rebased = 0.8
    val sparseRefreshed = 1.0

    val modeSpecified = dryRun || checkout || merge || rebase
    val defaultCatchupMode = ws.catchUp.defaultMode

    val modeDryRun = dryRun || (!modeSpecified && defaultCatchupMode == "dry-run")
    val modeMerge = merge || (!modeSpecified && defaultCatchupMode == "merge")
    val modeRebase = rebase || (!modeSpecified && defaultCatchupMode == "rebase")

    val changes = gitUtil.localChanges()
    step(postStateInit)

    if (new HistoryTruncationMigration(ws.directoryStructure.stratosphereWorkspaceDir).isMigrationNeeded) {
      abort("Migration is required. Please run 'stratosphere migrate' first.")
    }

    val untrackedFilePrefix = "?? "

    val (untrackedFiles, otherFiles) = changes.partition(_.startsWith(untrackedFilePrefix))

    if (untrackedFiles.nonEmpty) {
      logger.info(
        s"ignoring ${untrackedFiles.size} untracked file(s)/folder(s), which are unlikely to affect catchup: ${untrackedFiles
            .map(_.stripPrefix(untrackedFilePrefix))
            .mkString(",")}")
    }

    if (!modeDryRun && !ignoreLocalChanges && otherFiles.nonEmpty) onLocalChanges(changes)

    val currBranch = Try(gitUtil.currentBranch()).toOption.filter("HEAD (no branch)" != _)

    logger.info(s"Current branch: ${currBranch.getOrElse("No branch")}")
    val onCorrectBranch = modeDryRun || (currBranch match {
      case Some(branch) => branch == branchToUse
      case None         => true
    })

    if (!modeDryRun && !onCorrectBranch && !modeMerge && !modeRebase && !ignoreWrongBranch) onWrongBranch()
    val remoteToUse = prepareRemoteForCatchup()
    step(fetchFinished)

    val bestRef = findBestRef(remoteToUse)
    step(cacheFound)

    val backupTag = s"BACKUP-${currBranch.getOrElse("detached")}-$currentDate"

    bestRef match {
      case Some(CacheReference(distance, commit, _)) if modeDryRun =>
        gitUtil.createTag(CatchupHead, commit, force = true)
        logger.info(
          s"Found latest good commit: $commit from ${gitUtil.relativeAge(commit)} with distance: $distance, tagged as $CatchupHead.")
      case Some(CacheReference(distance, commit, description)) =>
        if (distance > maxDistance) abort(s"Latest good commit: $description is too far away to be used.")
        else {
          catchupWithCommit(commit)
          logger.info(s"Successfully caught up with Silverking cache from ${gitUtil.relativeAge(commit)}")
        }
      case Some(GitReference(tag, commit)) if modeDryRun =>
        gitUtil.createTag(CatchupHead, commit, force = true)
        logger.info(
          s"Found latest good commit from git tag $tag: $commit from ${gitUtil.relativeAge(commit)}, tagged as $CatchupHead.")
      case Some(GitReference(tag, commit)) =>
        catchupWithCommit(commit)
        logger.info(s"Successfully caught up with remote tag $tag from ${gitUtil.relativeAge(commit)}")
      case None =>
        abort(s"No good commit was found in $branchToUse from $remoteName.")
    }

    def catchupWithCommit(commit: String): Unit = {
      gitUtil.createTag(CatchupHead, commit, force = true)
      gitUtil.createTag(backupTag, "HEAD", force = true)
      gitUtil.createBranchFromCurrent(backupBranch, force = true)

      def checkout(): Unit = {
        logger.info(s"Checking out to latest good commit: $commit...")
        gitUtil.switchTo(backupBranch)
        gitUtil.createBranch(branchToUse, commit, force = true)
        gitUtil.switchTo(branchToUse)
      }

      try {
        if (modeRebase) {
          logger.info(s"Rebasing current branch on latest good commit: $commit...")
          gitUtil.rebase(commit)
        } else if (modeMerge) {
          logger.info(s"Merging latest good commit: $commit to current branch...")
          gitUtil.merge(commit)
        } else checkout()
      } catch {
        case NonFatal(e) =>
          throw new StratosphereException(
            s"Syncing failed. State of your workspace is preserved in $backupBranch branch",
            e)
      }

      if (!noFetch) gitUtil.deleteBranch(fetchBranch, evenIfNotMerged = true)
    }

    step(rebased, Some(backupTag))

    if (modeDryRun) {
      logger.info(s"skip sparse utils refresh")
    } else {
      SparseUtils.refresh()(ws)
    }
    step(sparseRefreshed, Some(backupTag), refreshSparse = true)

    logger.info(s"""
                   |Previous state of your workspace was tagged as $backupTag.
                   |If needed you can run `git checkout $backupTag` to get it back.
                   |""".stripMargin)
  }

  private def onLocalChanges(changes: Seq[String]): Unit = {
    if (disableInteractive) abort("Local changes detected. Please stash or commit them.")

    val msgSize = 20
    val changesMsg =
      if (changes.size > msgSize) changes.take(msgSize) :+ s"... and ${changes.size - msgSize} more..." else changes
    val msg = s"Detected local changes\n\t${changesMsg.mkString("\n\t")}\nDo you want stash them to proceed?"
    if (!askIf(msg)) abort("Cannot proceed without stashing local changes.")
    logger.info("Stashing local changes...")
    gitUtil.stash(includeUntracked = true)
  }

  private def onWrongBranch(): Unit = {
    if (disableInteractive) abort("Workspace is on wrong branch and neither --merge nor --rebase was provided.")
    val msg =
      s"Catchup cannot continue in current branch without --merge or --rebase. Do you want to checkout to $branchToUse branch to proceed?"
    if (!askIf(msg)) abort("Cannot proceed on feature branch without --merge or --rebase option specified.")
    logger.info(s"Changing branch to $branchToUse...")
    gitUtil.switchTo(branchToUse)
  }

  private def findBestRef(remoteToUse: String): Option[CatchupRef] = {
    val tag = s"CATCHUP_$branchToUse" // we cannot use fetchBranch for the git tag strategy!
    findBestFromGitTag(remote = remoteToUse, tagName = tag).orElse {
      logger.warning(s"Remote git tag $tag not found! Using OBT cache instead...")
      val branch = if (noFetch) branchToUse else fetchBranch
      findBestFromObtCache(branch)
    }
  }

  private def findBestFromGitTag(remote: String, tagName: String): Option[GitReference] = {
    if (CatchUpStrategy.GitCompatible.contains(args.strategy)) {
      logger.info(s"Checking remote hash for $tagName...")

      timeThis("fetch-git-catchup-tag", ws.log) {
        getRemoteTag(remote, tagName).collectFirst {
          case CommitTagExtractor(commit, tag) if tag == tagName => GitReference(tagName = tag, commit = commit)
        }
      }
    } else None
  }

  protected def getRemoteTag(remote: String, tagName: String): Seq[String] =
    gitUtil.lsRemoteTag(remote, tagName)

  private def findBestFromObtCache(branch: String): Option[CacheReference] = {
    if (CatchUpStrategy.ObtCompatible.contains(args.strategy)) {
      timeThis("obt", ws.log) {
        logger.info(s"Querying OBT cache...")

        val targetWs = createWsWithTargetConf()
        val args = Seq(
          "--printBestCachedGitCommitsForBranch",
          branch,
          "--sourceDir",
          ws.directoryStructure.sourcesDirectory.toString)

        val result = obtRunner(CatchUpObtContext(CatchUpObtInput(args), targetWs, logger))
        result.exitCode match {
          case ExitCode.Failure =>
            /**
             * Lots of things could cause this failure -- a strato diag will have the catchup logs which will be
             * essential for the investigation. Examples of possible RC:
             *   - wrong machine setup (git problems, network problems)
             *   - wrong workspace setup
             *   - OBT or SK bug
             *   - SK cluster issues
             */
            ws.log.error(
              s"Oops, something went wrong with OBT cache query. Please contact ${ws.internal.helpMailGroup} and run 'strato diag' to capture diagnostics")
            None
          case ExitCode.Success =>
            val lines = result.output.linesIterator.toSeq
            val hits = lines.collect { case ObtCacheResult(commit, distance) =>
              CacheReference(distance.toInt, commit, "")
            }
            if (hits.nonEmpty) {
              logger.info(s"OBT found ${hits.size} relevant cache hits")
              hits.headOption
            } else {

              /**
               * Examples of possible RC that could cause this issue:
               *   - continuous-staging job not running
               *   - staging branch being broken for a long time
               *   - OBT or SK bug
               */
              ws.log.error(
                s"Oops, something went wrong: failed to get any cache hits from OBT. Please contact ${ws.internal.helpMailGroup}")
              None
            }
        }

      }
    } else None
  }

  private def createWsWithTargetConf(): StratoWorkspaceCommon =
    try {
      val obtCatchupDirRoot: Path = ws.directoryStructure.obtCatchupDir
      val obtCatchupDirSrc: Path = obtCatchupDirRoot.resolve("src")
      obtCatchupDirSrc.delete() // deleting any old config file
      obtCatchupDirSrc.dir.create()
      val output = gitUtil.listDirectoryStructure(gitRef)

      output.filter(_.endsWith(".conf")).foreach { confFile =>
        gitUtil
          .showFileFromRevision(gitRef, confFile)
          .foreach(content => obtCatchupDirSrc.resolve(confFile).file.write(content))
      }
      StratoWorkspace(CustomWorkspace(obtCatchupDirRoot))
    } catch {
      case NonFatal(ex) =>
        ws.log.error(
          "Oops, something went wrong while loading the most recent stratosphere configuration, using current configuration instead",
          ex)
        ws
    }

  private def prepareRemoteForCatchup(): String = {
    val catchupRemoteName = ws.catchUp.remote.name
    val catchupRemoteUrl = RemoteUrl(ws.catchUp.remote.url)

    val catchupRemote = gitUtil.allRemotes().find(_.name == catchupRemoteName)

    catchupRemote match {
      case Some(remote) =>
        if (remote.remoteUrl != catchupRemoteUrl) {
          Try(gitUtil.setRemote(catchupRemoteName, catchupRemoteUrl)) match {
            case Success(_) =>
              logger.info(s"Catchup remote '$remoteName' url updated to '${catchupRemoteUrl.url}'")
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
    if (catchupRemote.isEmpty) {
      val defaultBranch = ws.catchUp.defaultBranch
      if (Try(gitUtil.setUpstream(catchupRemoteName, defaultBranch, defaultBranch)).isSuccess) {
        logger.info(s"'$defaultBranch' branch set to track '$catchupRemoteName' remote.")
      } else {
        logger.warning(s"Unable to set '$defaultBranch' branch to track '$catchupRemoteName' remote.")
      }
    }

    remoteName
  }

  private def runFetchIfNeeded(remote: String): Unit = {
    def runFetch() = Try(gitUtil.fetch(remote, branchToUse, fetchBranch, force = true))

    if (!noFetch) {
      logger.info(s"Fetching $remote/$branchToUse")
      if (runFetch().isFailure) {
        logger.warning(s"Fetch failed. Retrying...")
        runFetch() match {
          case Failure(e) => throw new StratosphereException("Fetch failed, see logs for details", e)
          case _          =>
        }
      }
    }
  }

  private def step(fraction: Double, backupTag: Option[String] = None, refreshSparse: Boolean = false): Unit = {
    displayProgressBar(fraction)
    if (isCanceled()) rollback(backupTag, refreshSparse)
  }

  private def abort(msg: String): Nothing = {
    logger.info(s"Catchup aborted: $msg")
    throw new StratosphereAbortException(msg)
  }

  private def rollback(resetToTag: Option[String] = None, refreshSparse: Boolean = false): Unit = {
    logger.info("Stopping catch-up!")
    resetToTag.foreach { resetToTag =>
      logger.info(s"reseting workspace to $resetToTag")
      gitUtil.resetKeep(resetToTag)
    }

    if (refreshSparse) {
      logger.info(s"refreshing workspace")
      SparseUtils.refresh()(ws)
    }

    throw new StratosphereAbortException("Catchup aborted externally")
  }

}
