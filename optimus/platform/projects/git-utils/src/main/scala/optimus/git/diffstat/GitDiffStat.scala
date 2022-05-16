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
package optimus.git.diffstat

import optimus.git.diffparser.model.Diff
import optimus.git.diffparser.model.Hunk
import optimus.git.diffparser.model.LineType

import scala.util.matching.Regex

object GitDiffStat {
  def fromParsedDiffs(id: String, diffs: Seq[Diff]): GitDiffStat = {
    // construct bundle diffs
    val fromDiffs = BundleDiffStat.fromDiffs(diffs)
    val bundleDiffStats = fromDiffs.foldLeft(Map.empty[String, BundleDiffStat]) {
      (result: Map[String, BundleDiffStat], bd: BundleDiffStat) =>
        val cur: BundleDiffStat = result.getOrElse(bd.bundleId, BundleDiffStat(""))
        result ++ BundleDiffStat.sumIfKeysMatch(cur, bd)
    }

    // construct aggregate line stats
    val allLines: HunkStat = {
      val a = diffs
        .flatMap(_.hunks)
        .map(HunkStat(_))

      if (a.nonEmpty) a.reduce[HunkStat]((h1: HunkStat, h2: HunkStat) => h1 + h2) else HunkStat()
    }

    // construct aggregate file stats
    val allFiles =
      if (diffs.nonEmpty) diffs.map(FileStat(_)).reduce((f1, f2) => f1 + f2) else FileStat()

    GitDiffStat(
      id,
      allLines.added,
      allLines.removed,
      allFiles.added,
      allFiles.removed,
      allFiles.modified,
      bundleDiffStats.values.toSeq)
  }

}
final case class GitDiffStat(
    changeId: String,
    linesAdded: Int,
    linesRemoved: Int,
    filesAdded: Int,
    filesRemoved: Int,
    filesModified: Int,
    bundleStats: Seq[BundleDiffStat])

object BundleDiffStat {

  def fromDiffs(diffs: Seq[Diff]): Seq[BundleDiffStat] = {
    diffs.flatMap { diff =>
      if (diff.isNewFile || diff.isFileDeletion) {
        Seq(BundleDiffStat(diff, forceFromFileStatWhenModified = false))
      } else {
        Seq(
          BundleDiffStat(diff, forceFromFileStatWhenModified = false),
          BundleDiffStat(diff, forceFromFileStatWhenModified = true))
      }
    }
  }
  def apply(diff: Diff, forceFromFileStatWhenModified: Boolean): BundleDiffStat = {
    val fileName =
      if (forceFromFileStatWhenModified && !diff.isNewFile && !diff.isFileDeletion) diff.toFileName
      else diff.getFileName
    val lines: HunkStat =
      if (diff.hunks.nonEmpty)
        diff.hunks.map(HunkStat(_)).reduce((h1: HunkStat, h2: HunkStat) => h1 + h2)
      else HunkStat()

    if (diff.isNewFile)
      BundleDiffStat(bundleFrom(fileName), lines.added, lines.removed, filesAdded = 1)
    else if (diff.isFileDeletion)
      BundleDiffStat(bundleFrom(fileName), lines.added, lines.removed, filesRemoved = 1)
    else {
      // modified lines are owned by the TO file if modified and moved across bundle boundary, this is imperfect but we could also run with rename detection off
      val isSameBundle = diff.fromFileName == diff.toFileName
      if (forceFromFileStatWhenModified) {
        if (isSameBundle)
          BundleDiffStat(bundleFrom(fileName))
        else
          BundleDiffStat(bundleFrom(fileName), filesRemoved = 1)
      } else {
        if (isSameBundle)
          BundleDiffStat(bundleFrom(fileName), lines.added, lines.removed, filesModified = 1)
        else
          BundleDiffStat(bundleFrom(fileName), lines.added, lines.removed, filesAdded = 1)
      }
    }
  }
  def sumIfKeysMatch(b1: BundleDiffStat, b2: BundleDiffStat): Map[String, BundleDiffStat] = {
    if (b1.bundleId != b2.bundleId && b1.bundleId != "") Map(b1.bundleId -> b1, b2.bundleId -> b2)
    else
      Map(
        b2.bundleId -> BundleDiffStat(
          b2.bundleId,
          b1.linesAdded + b2.linesAdded,
          b1.linesRemoved + b2.linesRemoved,
          b1.filesAdded + b2.filesAdded,
          b1.filesRemoved + b2.filesRemoved,
          b1.filesModified + b2.filesModified
        ))
  }
  val bundlePattern: Regex = """^([^/]+/[^/]+)""".r
  private[diffstat] def bundleFrom(pathStr: String): String = {
    val fabFolders = Seq("auto-build-rules")
    val globalFolders = Seq("appscripts", "profiles", ".github")
    val firstLayer = pathStr.split("/").headOption
    firstLayer match {
      case Some(x) if fabFolders.contains(x)    => "auto-build-rules"
      case Some(x) if globalFolders.contains(x) => "global"
      case _                                    => bundlePattern.findFirstIn(pathStr).getOrElse("global")
    }

  }
}
final case class BundleDiffStat(
    bundleId: String,
    linesAdded: Int = 0,
    linesRemoved: Int = 0,
    filesAdded: Int = 0,
    filesRemoved: Int = 0,
    filesModified: Int = 0)

object FileStat {
  def apply(diff: Diff): FileStat = {
    if (diff.isFileDeletion)
      FileStat(removed = 1)
    else if (diff.isNewFile)
      FileStat(added = 1)
    else
      FileStat(modified = 1)
  }
}
final case class FileStat(added: Int = 0, removed: Int = 0, modified: Int = 0) {
  def +(that: FileStat): FileStat = {
    FileStat(this.added + that.added, this.removed + that.removed, this.modified + that.modified)
  }
}

object HunkStat {
  def apply(hunk: Hunk): HunkStat = {
    val removed = hunk.lines.count(_.tpe == LineType.From)
    val added = hunk.lines.count(_.tpe == LineType.To)
    HunkStat(added, removed)
  }
}
final case class HunkStat(added: Int = 0, removed: Int = 0) {
  def +(that: HunkStat): HunkStat = {
    HunkStat(this.added + that.added, this.removed + that.removed)
  }
}
