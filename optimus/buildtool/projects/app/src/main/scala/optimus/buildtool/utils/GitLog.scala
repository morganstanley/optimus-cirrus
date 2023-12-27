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

import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger

import java.time.Instant
import optimus.buildtool.files.Directory
import optimus.buildtool.trace.ObtTrace
import optimus.platform._
import org.eclipse.jgit.diff.DiffEntry
import org.eclipse.jgit.lib.Constants
import org.eclipse.jgit.lib.ObjectId
import org.eclipse.jgit.revwalk.RevCommit
import org.eclipse.jgit.revwalk.RevWalk
import org.eclipse.jgit.revwalk.filter.RevFilter
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider
import org.eclipse.jgit.treewalk.AbstractTreeIterator
import org.eclipse.jgit.treewalk.CanonicalTreeParser

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.util.Try
import scala.util.control.NonFatal
import scala.util.matching.Regex

final case class Commit(hash: String, commitTime: Instant)
object Commit {
  def apply(rev: RevCommit): Commit = Commit(rev.getName, Instant.ofEpochSecond(rev.getCommitTime))
}

trait FileDiff {
  def contains(filePath: String): Boolean
  def contains(filePath: String, startLine: Int, endLine: Int): Boolean
}

class GitFileDiff(utils: NativeGitUtils, workspaceSourceRoot: Directory, baseline: String) extends FileDiff {
  private lazy val files: Set[String] = {
    val m = utils.diffFiles(baseline)
    GitLog.log.debug(s"${m.size} modified files since baseline $baseline:\n\t${m.map(_.pathString).mkString("\n\t")}")
    m.map(f => workspaceSourceRoot.relativize(f).pathString)
  }
  private lazy val lines: Map[String, Set[Int]] = {
    val m = utils.diffLines(baseline)
    GitLog.log.debug(
      s"${m.flatMap(_._2).sum} modified lines in ${m.size} files since baseline $baseline:\n\t${m.keys.map(_.pathString).mkString("\n\t")}"
    )
    m.map { case (f, ls) => (workspaceSourceRoot.relativize(f).pathString, ls) }
  }

  // Note: These should not be marked @impure since they're called from @nodes (eg. in ZincCompiler)
  override def contains(filePath: String): Boolean = files.contains(filePath)
  override def contains(filePath: String, startLine: Int, endLine: Int): Boolean =
    lines.get(filePath).exists(_.exists(l => l >= startLine && l <= endLine))
}

object EmptyFileDiff extends FileDiff {
  override def contains(filePath: String): Boolean = false
  override def contains(filePath: String, startLine: Int, endLine: Int): Boolean = false
}

object GitLog {
  private[utils] val log: Logger = getLogger(this.getClass)

  private val DefaultDescriptionFilter = ".*"

  @scenarioIndependent @node def apply(
      utils: GitUtils,
      workspaceSourceRoot: Directory,
      descriptionFilter: String = DefaultDescriptionFilter, // String here since Regexes don't have stable equality
      commitLength: Int
  ): GitLog = GitLogImpl(utils, workspaceSourceRoot, descriptionFilter.r, commitLength)

}

@entity trait GitLog {
  @node def diff(oldRef: String, newRef: String): Seq[DiffEntry]
  @node def HEAD: Option[Commit] = recentHeads.headOption
  @node def recentHeads: Seq[Commit]
  @node def recentCommits(from: String = Constants.HEAD): Seq[Commit]
  @async def baseline(from: String = Constants.HEAD): Option[Commit]
  // Diff between the baseline of `from` and current working tree
  @async @impure def modifiedFiles(from: String = Constants.HEAD): Option[FileDiff]
  @async @impure def reportTagMovingForward(tagName: String, dir: Directory): Unit
}

/**
 * Encapsulates the git API to allow for retrieving of recent git commits based on the current branch. A file watch is
 * maintained on the git reflog file to ensure that we invalidate the cache of recent commits when the reflog changes.
 */
@entity class GitLogImpl private[utils] (
    utils: GitUtils,
    workspaceSourceRoot: Directory,
    descriptionFilter: Regex,
    commitLength: Int
) extends GitLog {

  // triggering the configured credentials here
  private val credentials = new UsernamePasswordCredentialsProvider("", "")
  private val native = NativeGitUtils(workspaceSourceRoot)

  @node def diff(oldRef: String, newRef: String): Seq[DiffEntry] = {
    utils.declareVersionDependence()
    utils.git.diff
      .setOldTree(prepareTreeParser(oldRef))
      .setNewTree(prepareTreeParser(newRef))
      .call
      .iterator()
      .asScala
      .toIndexedSeq
  }

  private def prepareTreeParser(ref: String): AbstractTreeIterator = {
    val walk = new RevWalk(utils.repo)
    val commit = walk.parseCommit(utils.repo.resolve(ref))
    val tree = walk.parseTree(commit.getTree.getId)
    val treeParser = new CanonicalTreeParser
    val oldReader = utils.repo.newObjectReader
    try treeParser.reset(oldReader, tree.getId)
    finally oldReader.close()
    treeParser
  }

  @node def recentHeads: Seq[Commit] = {
    utils.declareVersionDependence()
    val (gitTime, commits) = AdvancedUtils.timed {
      val walk = new RevWalk(utils.repo)
      utils.git.reflog.call
        .iterator()
        .asScala
        .filterNot(_.getNewId == ObjectId.zeroId) // occurs when a branch is renamed, evidently
        .take(commitLength)
        .flatMap { rle =>
          Try(Commit(walk.parseCommit(rle.getNewId))).toOption // protect against commits no longer present in the repo
        }
        .toList
    }
    val msg = f"Loaded git reflog in ${gitTime / 1.0e9}%,.2fs"
    ObtTrace.info(msg)
    log.debug(msg)
    commits
  }

  @node def recentCommits(from: String): Seq[Commit] = {
    utils.declareVersionDependence()
    try {
      val (gitTime, commits) = AdvancedUtils.timed {
        if (descriptionFilter.regex == "localtest") {
          utils.git.log.call.iterator().asScala.take(commitLength).map(Commit(_)).toIndexedSeq
        } else {
          Option(utils.repo.resolve(from)) match {
            case None        => throw new IllegalStateException(s"Cannot resolve revision $from in the workspace.")
            case Some(start) =>
              // Make sure we get a whole bunch of merges
              val merges = utils.git
                .log()
                .add(start)
                .setRevFilter(RevFilter.ONLY_MERGES)
                .call
                .iterator()
                .asScala
                .filter(rev => descriptionFilter.findFirstIn(rev.getShortMessage).isDefined)
                .take(commitLength)
                .map(Commit(_))
                .toIndexedSeq
              val mergeHashes = merges.map(_.hash).toSet

              // Grab some local commits too.
              val unfiltered = utils.git
                .log()
                .add(start)
                .call
                .iterator()
                .asScala
                .take(commitLength)
                .map(Commit(_))
                .toIndexedSeq

              val others = unfiltered.filterNot(rev => mergeHashes.contains(rev.hash))

              (merges ++ others).sortBy(rev => rev.commitTime).reverse
          }
        }
      }
      val headStr = commits.headOption
        .map { c =>
          s" for ${c.hash}"
        }
        .getOrElse("")
      val msg = f"Loaded git commit history$headStr in ${gitTime / 1.0e9}%,.2fs"
      ObtTrace.info(msg)
      log.debug(msg)
      log.debug(s"Git commits: ${commits.mkString(", ")}")
      commits
    } catch {
      case NonFatal(t) =>
        log.error(s"Exception reading git log", t)
        Seq()
    }
  }

  @async def baseline(from: String): Option[Commit] = {
    // Note: Because in practice this is called before the file scan has been completed, it should not be
    // made a @node and call utils.declareVersionDependence
    try {
      val start = utils.repo.resolve(from)
      utils.git
        .log()
        .add(start)
        .setRevFilter(RevFilter.ONLY_MERGES)
        .call
        .iterator()
        .asScala
        .find(rev => descriptionFilter.findFirstIn(rev.getFullMessage).isDefined)
        .map(Commit(_))
    } catch {
      case NonFatal(t) =>
        log.error(s"Exception reading git log", t)
        None
    }
  }

  @async @impure private def baselineHash(from: String): Option[String] = {
    val base = baseline(from)
    if (base.isEmpty) {
      val msg = "No root found for feature branch"
      ObtTrace.warn(msg)
      log.info(msg)
    }
    base.map(_.hash)
  }

  // Diff between the baseline of `from` and current working tree
  @async @impure def modifiedFiles(from: String): Option[FileDiff] = baselineHash(from).map { hash =>
    new GitFileDiff(native, workspaceSourceRoot, hash)
  }

  @async @impure def reportTagMovingForward(tagName: String, dir: Directory): Unit = {
    val currentHead = HEAD.fold("UNKNOWN")(_.hash)
    val movingForward = isMovingForward(tagName)
    if (movingForward) {
      // Unfortunately, we cannot create the git tag ourselves as we don't have the permission to push the git tag
      // so we delegate this for a more powerful user id (via Jenkins)
      val taggingFile = dir.resolveFile("tag.properties")
      val properties = Map("tag.name" -> tagName, "tag.hash" -> currentHead)
      Utils.writePropertiesToFile(taggingFile, properties)
      log.info(s"${taggingFile.pathString} created: tag $tagName should be moved to $currentHead (HEAD)")
    } else log.info(s"Skipping tag request for $tagName as only forward tagging are allowed (HEAD is $currentHead)")
  }

  @async @impure private def isMovingForward(tagName: String): Boolean = {
    // git ls-remote --tags
    val currentRemoteHash: Option[String] = utils.git
      .lsRemote()
      .setCredentialsProvider(credentials)
      .setTags(true)
      .call()
      .asScala
      .collectFirst {
        case ref if ref.getName == s"refs/tags/$tagName" =>
          Option(ref.getPeeledObjectId).getOrElse(ref.getObjectId).name
      }

    currentRemoteHash.forall { remoteHash =>
      log.info(s"Tag $tagName is currently pointing to $remoteHash")
      utils.repo.hasObject(ObjectId.fromString(remoteHash))
    }
  }
}
