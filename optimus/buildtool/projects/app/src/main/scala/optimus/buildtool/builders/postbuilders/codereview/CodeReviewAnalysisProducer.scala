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
package optimus.buildtool.builders.postbuilders.codereview

import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.codereview.CodeReviewAnalysis
import optimus.buildtool.codereview.CodeReviewMessage
import optimus.buildtool.codereview.CodeReviewMessageType
import optimus.buildtool.utils.FileDiff

object CodeReviewAnalysisProducer {

  def fromCompilationMessages(
      msgs: Seq[CompilationMessage],
      settings: CodeReviewSettings,
      modifiedFiles: Option[FileDiff]
  ): CodeReviewAnalysis = {

    def shouldReport(msg: CompilationMessage): Boolean = msg.pos.exists { position =>
      !msg.alwaysIgnore && modifiedFiles.forall(files => files.contains(position.filepath))
    }

    val compilerMessagesPerFile: Map[String, Seq[CompilationMessage]] =
      msgs.filter(shouldReport).groupBy(_.pos.get.filepath)
    val compilerInfo: Map[String, Map[String, Seq[CodeReviewMessage]]] =
      compilerMessagesPerFile.map { case (filepath, compilerMessages) =>
        filepath -> toCodeReviewMessagesPerLine(compilerMessages)
      }
    CodeReviewAnalysis(prCommit = settings.prCommit, targetCommit = settings.targetCommit, compilerInfo)
  }

  private def toCodeReviewMessagesPerLine(
      compilerMessages: Seq[CompilationMessage]): Map[String, Seq[CodeReviewMessage]] = {
    val compilerMessagesPerLine = compilerMessages.filter(_.pos.isDefined).groupBy(_.pos.get.startLine.toString)
    compilerMessagesPerLine.map { case (line, messages) =>
      val codeReviewMessages = for {
        message <- messages
        pos <- message.pos
      } yield {
        // we need to adjust the position in the column: OBT starts from 1, code review tools from 0
        val start = pos.startColumn - 1
        val end =
          if (pos.startLine == pos.endLine)
            pos.endColumn - 2 // An extra -1 here because OBT excludes the end, but code review tools include it
          else
            Int.MaxValue // multi-line messages are unfortunately not supported in code review tools, so we just highlight till the end of the line
        CodeReviewMessage(
          start = start,
          end = start.max(end), // ensuring we always highlight at least one char
          `type` = toCodeReviewMessageType(message),
          msg = message.msg)
      }
      line -> codeReviewMessages
    }
  }

  private def toCodeReviewMessageType(msg: CompilationMessage): CodeReviewMessageType = msg.severity match {
    case CompilationMessage.Error                    => CodeReviewMessageType.Error
    case CompilationMessage.Warning                  => CodeReviewMessageType.Warn
    case CompilationMessage.Info if msg.isSuppressed => CodeReviewMessageType.Warn
    case _                                           => CodeReviewMessageType.Info
  }
}
