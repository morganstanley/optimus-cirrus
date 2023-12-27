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

import java.nio.file._
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.PathedArtifact
import optimus.buildtool.builders.postbuilders.PostBuilder
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.trace.MarkRecentArtifacts
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.GitLog
import optimus.platform._

import scala.jdk.CollectionConverters._
import scala.collection.immutable.HashMap
import scala.collection.immutable.Seq
import scala.collection.compat._
import scala.collection.mutable

/**
 * Provides the ability to track how recently artifacts have been used, to tune garbage collection more finely.
 */
final class ArtifactRecency(buildDir: Directory, gitLog: GitLog) extends PostBuilder {
  import CommitLog._

  @async override def postProcessArtifacts(scopes: Set[ScopeId], artifacts: Seq[Artifact], successful: Boolean): Unit =
    markRecent(artifacts)

  @async def markRecent(artifacts: Seq[Artifact]): Unit = ObtTrace.traceTask(ScopeId.RootScopeId, MarkRecentArtifacts) {
    // doing something like `artifacts.foreach(_.path.lastAccess = now)` takes > 1 second on Windows
    val newPaths = artifacts
      .collect { case p: PathedArtifact => (p.id, p) }
      .collect { case (id: InternalArtifactId, p) => (id, p.path) }

    gitLog.HEAD.foreach { HEAD =>
      val clogFile = forCommit(HEAD.hash, buildDir)
      val oldClog = readLog(clogFile)
      writeLog(clogFile) { updateLog(oldClog, newPaths) }
    }
  }
}

/**
 * Format for the commit log files, which contain data about artifacts recently used on a given commit. This is used to
 * heuristically pin artifacts which are likely to be reused because they were the last artifacts used on recently-used
 * commits.
 *
 * Note that these are stored outside of the versioned artifact directories, and therefore changes to the format must be
 * made carefully to avoid breaking older OBT versions (if a user switches between branches with two versions). (This is
 * also why we're using `Stringified` versions of everything.)
 */
private[rubbish] object CommitLog {

  final val DirName = "commit-log"

  /** The number of versions of each file to retain */
  final val EntriesPerArtifact = 5

  /** Poor attempt at documentation of what a certain class of strings represents. */
  type Stringified[A] = String
  type StringifiedArtifactId = (Stringified[ScopeId], Stringified[ArtifactType], Stringified[Option[String]])

  /** A single commit log. This is a list of recently-used paths on a single commit, indexed by artifact ID. */
  // HashMap, not Map, because Map doesn't have `merged`
  type CommitLog = HashMap[StringifiedArtifactId, List[Path]]
  object CommitLog {
    def newBuilder: mutable.Builder[(StringifiedArtifactId, List[Path]), CommitLog] =
      HashMap.newBuilder[StringifiedArtifactId, List[Path]]
  }

  // C:\<workspace path>\build_obt\commit-log\071fb5cb8fdd09fa7d87cd8d67ef1be0c8cbf0f6
  def forCommit(rev: String, buildDir: Directory): FileAsset =
    buildDir resolveDir DirName resolveFile rev

  /** Modify `log` by adding the provided `(artifactId, path)` pairs, pushing out previous paths */
  def updateLog(log: CommitLog, newPaths: Seq[(InternalArtifactId, Path)]): CommitLog = {

    /** Create a new list of distinct elements from old and new, where elements in new are pushed to the front. */
    def bubbleUp[T](oldEls: List[T], newEls: List[T]): List[T] = {
      (newEls ++ oldEls).distinct
    }

    val builder = CommitLog.newBuilder
    newPaths
      .map { case (InternalArtifactId(scope, tpe, disc), path) =>
        ((scope.properPath, tpe.name, disc.getOrElse("")), List(path))
      }
      .foreach(builder += _)
    val newLogs = builder.result()

    log.merged(newLogs) { case ((key, oldPaths), (_, newPaths)) =>
      key -> bubbleUp(oldPaths, newPaths).take(EntriesPerArtifact)
    }
  }

  def readLog(clog: FileAsset): CommitLog = {
    // I have no idea how to turn a Map into a HashMap without this builder; sorry
    def doRead() = HashMap.newBuilder ++= {
      val in = Files.newBufferedReader(clog.path)
      try
        in.lines.iterator.asScala
          .map { line =>
            line.split("\t") match {
              case Array(scopeId, tpe, disc, path) =>
                (scopeId, tpe, disc) -> clog.fileSystem.getPath(path)
              // backward compatibility
              case Array(scopeId, tpe, path) =>
                (scopeId, tpe, "") -> clog.fileSystem.getPath(path)
              case _ =>
                throw new IllegalStateException(
                  s"Unable to parse line '$line' from file ${clog.pathString}. Is the commit log file corrupted?")
            }
          }
          .toList
          .groupMap(_._1)(_._2) // mapValues fine because we immediately force the values
      finally in.close()
    }
    if (clog.exists) doRead().result() else HashMap.empty
  }

  def writeLog(clog: FileAsset)(log: CommitLog): Unit = {
    Files.createDirectories(clog.parent.path)
    val out = Files.newBufferedWriter(clog.path)
    try
      log.foreach { case ((scope, tpe, disc), paths) =>
        paths.foreach { path =>
          out.write(scope); out.write('\t')
          out.write(tpe); out.write('\t')
          out.write(disc); out.write('\t')
          out.write(path.toString); out.write('\n')
        }
      }
    finally out.close()
  }

  /** Delete all commit log files not in the reflog according to `git` */
  @entersGraph
  def tidy(buildDir: Directory, git: GitLog): Unit = {
    val recentCommits = git.recentHeads.map(_.hash).toSet
    val logDir = buildDir resolveDir DirName
    if (logDir.exists) {
      val logEntries = Files.list(logDir.path).iterator.asScala.toList
      logEntries.foreach { logEntry =>
        if (!recentCommits.contains(logEntry.getFileName.toString))
          Files.delete(logEntry)
      }
    }
  }
}
