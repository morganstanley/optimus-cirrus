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
package optimus.stratosphere.history
import com.typesafe.config.Config
import optimus.stratosphere.bootstrap.StratosphereException
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.history.HistoryStitch.failedFetchMessage
import optimus.stratosphere.history.HistoryStitch.failedReplaceMessage
import optimus.stratosphere.utils.GitUtils
import optimus.stratosphere.utils.RemoteUrl

import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

final case class Replacement(repo: String, from: String, to: String)

object Replacement {
  def fromConfig(config: Config): Replacement =
    Replacement(config.getString("repo"), config.getString("from"), config.getString("to"))
}

final case class MergeConfig(name: String, replacements: Seq[Replacement], requires: Seq[String]) {
  def remoteName(i: Int): String = s"history-$name-$i"
}
object MergeConfig {
  def fromConfig(config: Config): MergeConfig = {
    val replacements = config.getConfigList("replacements").asScalaUnsafeImmutable.map(Replacement.fromConfig)
    val requires =
      if (config.hasPath("requires"))
        config.getStringList("requires").asScala.toSeq
      else
        Seq()
    MergeConfig(config.getString("name"), replacements, requires)
  }
}

final class HistoryStitch(ws: StratoWorkspaceCommon) {

  private val git: GitUtils = GitUtils(ws)
  private val config = ws.historyMerges
    .map(MergeConfig.fromConfig)
    .map(config => config.name -> config)
    .toMap

  def undoStitch(name: String): Unit = {
    val merge = config(name)
    merge.requires.foreach(undoStitch)
    ws.log.info(s"Undoing ${merge.name}...")
    val allRemotes = git.allRemotes()
    val existingReplacements = git.allReplaceRefs()
    merge.replacements.zipWithIndex.foreach { case (replacement, i) =>
      if (existingReplacements.contains(replacement.from))
        git.undoReplace(replacement.from)
      if (allRemotes.exists(_.name == merge.remoteName(i)))
        git.removeRemote(merge.remoteName(i))
    }
  }

  def undoAll(): Unit =
    if (git.allReplaceRefs().nonEmpty) {
      mergeNames.foreach(undoStitch)
    }

  def stitch(name: String): Unit = {
    def errorMatchesPattern(patternGenerator: String => Regex, commit: String, e: StratosphereException): Boolean =
      patternGenerator(commit).findFirstIn(e.getMessage).isDefined

    val merge = config(name)
    merge.requires.foreach(stitch)
    ws.log.info(s"Applying ${merge.name}...")
    val allRemotes = git.allRemotes()
    val existingReplacements = git.allReplaceRefs()
    merge.replacements.zipWithIndex
      .filter { case (replacement, _) => !existingReplacements.contains(replacement.from) }
      .foreach { case (replacement, i) =>
        val remoteName = merge.remoteName(i)
        if (!allRemotes.exists(_.name == remoteName))
          git.addRemoteNoTags(remoteName, RemoteUrl(replacement.repo))
        else {
          git.setRemote(remoteName, RemoteUrl(replacement.repo))
          git.disableTags(remoteName)
        }
        ws.log.info("Fetching commits...")
        try {
          git.fetch(remoteName, replacement.to)
        } catch {
          // we just want to provide more specific logging in case commit does not exist on the remote
          // we still rethrow it to have more details logged (in case we need it) and to exit command
          case e: StratosphereException if errorMatchesPattern(failedFetchMessage, replacement.to, e) =>
            ws.log.error(
              s"Fetch failed. It seems that ${replacement.to} does not exists on ${replacement.repo}. " +
                s"Make sure you have provided a correct commit.")
            throw e
        }
        ws.log.info(s"Replacing ${replacement.from} with ${replacement.to}...")
        try {
          git.replace(replacement.from, replacement.to)
        } catch {
          // we just want to provide more specific logging in case commit does not exist on the remote
          // we still rethrow it to have more details logged (in case we need it) and to exit command
          case e: StratosphereException if errorMatchesPattern(failedReplaceMessage, replacement.from, e) =>
            ws.log.error(
              s"Replace failed. It seems that ${replacement.from} does not exist locally. " +
                s"Make sure you have provided a correct commit.")
            throw e
        }
      }
  }

  def mergeNames: Seq[String] = config.keys.toSeq
}

object HistoryStitch {
  private[history] def failedFetchMessage(commit: String): Regex = s"remote error: upload-pack: not our ref $commit".r
  private[history] def failedReplaceMessage(commit: String): Regex =
    s"'$commit' points to a replace(d|ment) object of type '\\((NULL|null)\\)'".r
}
