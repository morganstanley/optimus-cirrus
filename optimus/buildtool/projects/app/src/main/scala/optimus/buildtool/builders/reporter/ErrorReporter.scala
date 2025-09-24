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
package optimus.buildtool.builders.reporter

import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.MessagePosition
import optimus.buildtool.builders.BuildResult.CompletedBuildResult
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.utils.StackUtils
import optimus.platform._

class ErrorReporter(errorsDir: Directory) {

  @async def writeErrorReport(buildResult: CompletedBuildResult): Unit = {
    val artifactsErrors = buildResult.errorsByArtifact
    if (artifactsErrors.nonEmpty) {
      val idErrorMsgs = artifactsErrors.map { case (art, errs) =>
        (art.id.scopeId, errs)
      }
      HtmlReporter.writeErrorReport(errorsDir, idErrorMsgs)
    }
  }

  def writeErrorReport(exception: Throwable, fileName: String = HtmlReporter.DefaultName): Unit = {
    val htmlReportFile = FileAsset(errorsDir.path.resolve(fileName))
    if (!htmlReportFile.exists) { // only write exception error report when build errors not exists
      val msgPosition: MessagePosition = exception.getStackTrace.headOption match {
        case Some(h) => MessagePosition(h.getFileName, h.getLineNumber)
        case None    => MessagePosition(exception.getMessage)
      }

      val exceptionErrorMsg: Seq[(ScopeId, Seq[CompilationMessage])] =
        Seq(
          (
            ScopeId.RootScopeId,
            Seq(
              CompilationMessage(
                Some(msgPosition),
                StackUtils.multiLineStacktrace(exception),
                CompilationMessage.Error
              ))))
      HtmlReporter.writeErrorReport(errorsDir, exceptionErrorMsg)
    }
  }
}
