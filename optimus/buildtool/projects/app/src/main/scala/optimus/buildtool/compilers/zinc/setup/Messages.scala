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
package optimus.buildtool.compilers.zinc.setup

import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.MessageArtifactType
import optimus.buildtool.files.FileAsset
import optimus.buildtool.utils.PathUtils
import xsbti.VirtualFileRef

import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.collection.immutable.SortedSet

object Messages {

  def messages(
      newMessages: Seq[CompilationMessage],
      prevMessages: Option[FileAsset],
      staleSources: Seq[VirtualFileRef]
  ): Seq[CompilationMessage] = {

    // ScalaC produces this unhelpful info
    val filteredNew: Iterator[CompilationMessage] =
      newMessages.iterator.filterNot(m => m.severity == CompilationMessage.Info && m.msg == "[sig files written]")

    val unchangedMessages: Iterator[CompilationMessage] = prevMessages
      .map { p =>
        // No need to track the `prevMessagesArtifact` - if it's deleted that doesn't imply that we
        // need to rerun this compilation (`SyncScalaCompiler.Inputs.bestAnalysis` is wrapped in `Hide`
        // for the same reason)
        val prevMessagesArtifact = MessageArtifactType.fromUnwatchedPath(p.path)
        val prevMessages = prevMessagesArtifact.messages
        val stalePaths = staleSources.map { file =>
          PathUtils.platformIndependentString(file.id)
        }
        prevMessages.iterator.collect {
          // Remove messages for files we just compiled, as well as any module-wide messages.
          case m @ CompilationMessage(Some(pos), _, _, _, _, _)
              if !stalePaths.contains(pos.filepath) && !pos.filepath.endsWith("/") =>
            m
        }
      }
      .getOrElse(Iterator.empty)

    // We accumulate into a sorted set because we want the messages to a) be sorted and b) be deduplicated.
    (filteredNew ++ unchangedMessages).to(SortedSet).to(Seq)
  }
}
