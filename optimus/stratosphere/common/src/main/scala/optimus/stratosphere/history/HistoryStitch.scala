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
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.utils.GitUtils
import optimus.stratosphere.utils.RemoteUrl

import scala.jdk.CollectionConverters._

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
    val replacements = config.getConfigList("replacements").asScala.map(Replacement.fromConfig)
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
    mergeNames.foreach(undoStitch)

  def stitch(name: String): Unit = {
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
          git.addRemote(remoteName, RemoteUrl(replacement.repo))
        else
          git.setRemote(remoteName, RemoteUrl(replacement.repo))
        ws.log.info("Fetching commits...")
        git.fetch(remoteName)
        ws.log.info(s"Replacing ${replacement.from} with ${replacement.to}...")
        git.replace(replacement.from, replacement.to)
      }
  }

  def mergeNames: Seq[String] = config.keys.toSeq
}
