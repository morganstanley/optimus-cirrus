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
package optimus.buildtool.rubbish

import java.time.Instant
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.time.Duration
import optimus.buildtool.artifacts.PathedArtifact
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.files.Directory
import optimus.buildtool.trace.ObtStats
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.GitLog
import optimus.buildtool.utils.Utils
import optimus.platform.util.Log
import optimus.platform._
import optimus.utils.ErrorIgnoringFileVisitor
import optimus.utils.MiscUtils.{NumericFoldable, OrderingChain}

import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

trait RubbishTidyer {
  def storedArtifacts: StoredArtifacts
  def tidy(
      artifacts: StoredArtifacts,
      excludedPaths: Seq[Path],
      tempCutoff: Instant = Instant.now().minusSeconds(3600)
  ): TidiedRubbish
}
object RubbishTidyer extends Log {

  def tidyLogs(logDir: Path, maxAge: Duration): Unit = {
    val threshold = Instant ofEpochMilli OptimusApp.startupTime minus maxAge
    def tooOld(file: Path) = Files.getLastModifiedTime(file).toInstant isBefore threshold
    if (Files.exists(logDir)) {
      log.info(s"Removing OBT log files older than $threshold")
      Files
        .list(logDir)
        .iterator
        .asScala
        .filter(tooOld)
        .filter(Files.isRegularFile(_)) // avoid DirectoryNotEmptyException in some old workspaces
        .foreach(Files.delete)
    }
  }
}

trait RubbishTidyerBase extends RubbishTidyer with Log {
  import Utils._

  protected def buildDir: Directory
  protected def sandboxDir: Directory

  protected def fallibly[A](err: => String)(delete: => A): Option[A] = {
    try Some(delete)
    catch {
      case NonFatal(exn) =>
        log.warn(s"Could not $err; skipping: ${exn.getMessage}")
        None
    }
  }

  def tidy(
      artifacts: StoredArtifacts,
      excludedPaths: Seq[Path],
      tempCutoff: Instant = Instant.now().minusSeconds(3600)
  ): TidiedRubbish = {
    cleanTempFiles(tempCutoff)

    val rubbish = select(artifacts, excludedPaths)

    val deleted = fallibly("tidy rubbish") {
      if (rubbish.nonEmpty) {
        def rubbishString = s"Rubbish:\n\t${rubbish
            .map { r =>
              s"${buildDir.path.relativize(r.file)} (${r.lastModified}/${r.weight})"
            }
            .mkString("\n\t")}"

        log.debug(rubbishString)
        rubbish.flatMap { r =>
          fallibly(s"delete $r") {
            Files.deleteIfExists(r.file)
            r
          }
        }
      } else Nil
    }.getOrElse(Nil)

    val tidied = if (deleted.nonEmpty) {
      val tweaks = deleted.map { r =>
        PathedArtifact.registerDeletion(r.file)
      }
      val deletedSize = deleted.map(_.size).sum
      log.debug(f"Rubbish tidied, ${deleted.size} items deleted ($deletedSize%,d bytes)")
      TidiedRubbish(deleted, tweaks, deletedSize)
    } else TidiedRubbish.empty

    ObtTrace.setStat(ObtStats.RubbishFiles, tidied.artifacts.size)
    ObtTrace.setStat(ObtStats.RubbishSizeBytes, tidied.sizeBytes)
    tidied
  }

  private[rubbish] def cleanTempFiles(tempCutoff: Instant): Unit = {
    // because other OBT builds could be running concurrently, we give a grace period before removing temp files
    def isOld(attrs: BasicFileAttributes): Boolean = attrs.lastModifiedTime().toInstant.isBefore(tempCutoff)

    fallibly("delete temporary files") {
      def isOldTemp(p: Path, attrs: BasicFileAttributes): Boolean =
        p.getFileName.toString.startsWith(NamingConventions.TEMP) && isOld(attrs)

      AssetUtils.recursivelyDelete(buildDir, isOldTemp _, throwOnFail = false, retainRoot = true)
    }

    fallibly("delete sandbox files") {
      AssetUtils.recursivelyDelete(sandboxDir, (_, attrs) => isOld(attrs), throwOnFail = false, retainRoot = true)
    }
  }

  /** @param includePinned will reluctantly include pinned artifacts if needed to reach size limits */
  private[buildtool] def select(
      artifacts: StoredArtifacts,
      excludedPaths: Seq[Path],
      includePinned: Boolean = true
  ): Seq[StoredArtifact] = {
    if (artifacts.toTidyBytes > 0L) {
      val commitArtifacts = loadRecentCommitArtifacts()
      val withPin = {
        val all = artifacts
          .exclude(excludedPaths)
          .artifacts
          .map { r =>
            val weight = commitArtifacts.getOrElse(r.file, 0) // weight 0 is the default
            r.copy(weight = weight)
          }
        log.debug(s"All files by weight: \n\t${countByWeight(all).mkString("\n\t")}")
        if (includePinned) all else all.filter(_.weight == 0)
      }.sorted.reverse // from most to least rubbishness

      var taken = 0L
      val result = withPin.takeWhile { r =>
        taken < artifacts.toTidyBytes && { taken += r.size; true }
      }

      /**
       * Why don't we update the commit log after deleting entries?
       *
       * Because we don't need to. Those deleted entries will naturally migrate downward (they'll get pushed by new
       * entries) until they are evicted. It means slightly more work when reading the log and compiling the map, but it
       * makes the code here and in ArtifactRecency a lot simpler.
       */

      val (oldest, newest) = (result.minBy(_.lastModified), result.maxBy(_.lastModified))
      log.info(f"Selected ${result.size}%,d pieces of rubbish (total ${bytesToString(
          taken)}, oldest ${oldest.lastModified}, newest ${newest.lastModified})")

      log.debug(s"Rubbish files by weight: \n\t${countByWeight(result).mkString("\n\t")}")

      result
    } else {
      log.info(s"Nothing to tidy")
      Nil
    }
  }

  /**
   * Load artifacts from the git log and their rubbishness weight.
   *
   * Unpinned artifacts have weight 0 (the default). Pinned artifacts of other heads have weight 1. Pinned artifacts of
   * the current head have weight that depends on their pinning recency, equal to `EntriesPerArtifact + 1` for the most
   * recent down to 2 for the least recent ones.
   */
  @entersGraph // it asks for git metadata, but this shouldn't be called from graph anyways
  protected def loadRecentCommitArtifacts(): Map[Path, Int]

  private def countByWeight(files: Seq[StoredArtifact]): Seq[String] = {
    val byWeight =
      files.groupBy(_.weight).map { case (w, rs) => w -> rs.size }.to(Seq).sorted.reverse
    byWeight.map { case (w, size) => s"Weight $w: $size" }
  }
}

/**
 * Rubbish tidying for the filesystem cache.
 *
 * If a git log is provided, the most recent commits in the reflog (exact number determined by --gitLength) are used to
 * determine a set of artifacts that ought not be deleted, in case the user is switching between branches and will
 * therefore use them again.
 */
final class RubbishTidyerImpl(
    maxSizeBytes: Long,
    freeDiskSpaceTriggerBytes: Option[Long],
    override protected val buildDir: Directory,
    override protected val sandboxDir: Directory,
    git: Option[GitLog]
) extends RubbishTidyerBase {
  import Utils._

  def storedArtifacts: StoredArtifacts = {
    // Sufficient free space where freeDiskSpaceTriggerBytes is defined and less than the current free space
    val freeSpace = Utils.freeSpaceBytes(buildDir)
    (freeSpace, freeDiskSpaceTriggerBytes) match {
      case (Some(f), Some(t)) if f > t =>
        log.debug(s"Skipping rubbish cleanup (${bytesToString(f)} is greater than trigger of ${bytesToString(t)})")
        StoredArtifacts(Nil, 0L)
      case _ =>
        val artifacts = ArrayBuffer.empty[StoredArtifact]
        object visitor extends ErrorIgnoringFileVisitor {
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            artifacts += StoredArtifact(file, attrs)
            FileVisitResult.CONTINUE
          }
        }
        val extraInfo = freeSpace.map(f => s" (${bytesToString(f)} free disk space)").getOrElse("")
        log.info(s"Searching for potential rubbish$extraInfo...")
        if (buildDir.exists) {
          IO.using(Files.list(buildDir.path))(_.iterator.asScala.toArray)
            .filter(Files.isDirectory(_: Path))
            .filter((p: Path) => !RubbishTidyerImpl.RootDirectoriesToSkip.contains(p.getFileName.toString))
            .foreach((p: Path) => Files.walkFileTree(p, visitor))
        }

        val totalSize = artifacts.sumOf(_.size)
        val aboveMaxBuildDirBytes = totalSize - maxSizeBytes
        val toDelete = (freeSpace, freeDiskSpaceTriggerBytes) match {
          /** we want to free as much as is required to hit the trigger if possible, otherwise maxBuildDirSize */
          case (Some(f), Some(t)) if (t - f) < aboveMaxBuildDirBytes =>
            val belowTriggerBytes = t - f
            log.debug(s"Will attempt to free ${bytesToString(
                belowTriggerBytes)} to bring disk free space back to ${bytesToString(t)}")
            belowTriggerBytes
          case _ if aboveMaxBuildDirBytes > 0 =>
            log.debug(s"Will attempt to free ${bytesToString(
                aboveMaxBuildDirBytes)} to bring build dir size back to ${bytesToString(maxSizeBytes)}")
            aboveMaxBuildDirBytes
          case _ =>
            log.debug(
              s"Build dir size (${bytesToString(totalSize)}) less than max allowed size (${bytesToString(maxSizeBytes)}). No rubbish tidying necessary.")
            0
        }
        StoredArtifacts(artifacts.to(Seq), toDelete)
    }
  }

  /**
   * Load artifacts from the git log and their rubbishness weight.
   *
   * Unpinned artifacts have weight 0 (the default). Pinned artifacts of other heads have weight 1. Pinned artifacts of
   * the current head have weight that depends on their pinning recency, equal to `EntriesPerArtifact + 1` for the most
   * recent down to 2 for the least recent ones.
   */
  @entersGraph // it asks for git metadata, but this shouldn't be called from graph anyways
  override protected[rubbish] def loadRecentCommitArtifacts(): Map[Path, Int] = {

    val recentHeads = git.toList.apar.flatMap(_.recentHeads)
    log.debug(s"Recent heads: $recentHeads")
    val logs = recentHeads.distinct.map(commit => CommitLog.readLog(CommitLog.forCommit(commit.hash, buildDir)))

    logs match {
      case head :: rest =>
        // top artifact of all the previous commit, unsorted
        val other = rest
          .flatMap(_.values.flatMap(_.headOption))
          .distinct
          .map(p => p -> 0)

        val current = head.values
          .flatMap(_.take(CommitLog.EntriesPerArtifact).zipWithIndex) // take is in case EntriesPerArtifact changes
          .collect { case (p, index) =>
            p -> (CommitLog.EntriesPerArtifact - index) // from 1 (least recent) to EntriesPerArtifact (most recent)
          }

        // with the + 1 thats 1 for every pinned artifact not in current commit, 2+ for current commit.
        (other ++ current).toGroupedMap.map { case (k, v) => k -> (v.max + 1) }

      case _ => Map.empty[Path, Int]
    }
  }

  override def tidy(
      artifacts: StoredArtifacts,
      excludedPaths: Seq[Path],
      tempCutoff: Instant = Instant.now().minusSeconds(3600)
  ): TidiedRubbish = {
    notifyIfBuildSizeTooLow()

    val tidiedCommitLogs = fallibly("clean up commit-log") {
      // then delete any stale commit-log entries
      git.map(CommitLog.tidy(buildDir, _)).getOrElse(Nil)
    }.getOrElse(Nil)

    val tidiedRubbish = super.tidy(artifacts, excludedPaths, tempCutoff)
    tidiedRubbish.copy(artifacts = tidiedRubbish.artifacts ++ tidiedCommitLogs)
  }

  private val maxSizeMB = Utils.byteToMB(maxSizeBytes)
  private val minRecommendedSizeMB = 7000
  private def notifyIfBuildSizeTooLow(): Unit = {
    if (maxSizeMB < minRecommendedSizeMB) {
      val msg = "Configuration obt.max-build-dir-size is lower that the recommended minimum: " +
        s"please set it to $minRecommendedSizeMB or higher (current value: $maxSizeMB)."
      ObtTrace.warn(msg)
      log.warn(msg)
    }
  }
}

object RubbishTidyerImpl {
  val RootDirectoriesToSkip: Set[String] = Set(CommitLog.DirName, NamingConventions.Sparse, "zincCompilerInterface")
}

final case class StoredArtifact(file: Path, size: Long, lastModified: Instant, weight: Int = 0)
object StoredArtifact {

  /** Ascending order of rubbishness: pinning weight, then newest first, then smallest first. */
  implicit val ordering: Ordering[StoredArtifact] =
    Ordering
      .by(-(_: StoredArtifact).weight) // higher weight == least rubbish
      .orElseBy(-(_: StoredArtifact).lastModified.toEpochMilli)
      .orElseBy((_: StoredArtifact).size)

  def apply(file: Path, attrs: BasicFileAttributes): StoredArtifact =
    StoredArtifact(file, attrs.size, attrs.lastModifiedTime.toInstant)
}

final case class StoredArtifacts(artifacts: Seq[StoredArtifact], toTidyBytes: Long) {
  def exclude(paths: Seq[Path]): StoredArtifacts = {
    if (paths.nonEmpty) {
      val pathSet = paths.toSet
      StoredArtifacts(artifacts.filter(a => !pathSet.contains(a.file)), toTidyBytes)
    } else this
  }
}
object StoredArtifacts {
  val empty: StoredArtifacts = StoredArtifacts(Nil, 0)
}

final case class TidiedRubbish(artifacts: Seq[StoredArtifact], tweaks: Seq[Tweak], sizeBytes: Long)
object TidiedRubbish {
  val empty: TidiedRubbish = TidiedRubbish(Nil, Nil, 0)
}
