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
package optimus.buildtool.utils.process

import optimus.buildtool.app.OptimusBuildToolBootstrap
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.trace.AsyncCategoryTrace
import optimus.platform.util.Log

import scala.collection.compat._

class BackgroundProcessBuilder(
    logDir: Directory,
    sendCrumbs: Boolean
) extends ExternalProcessBuilder
    with Log {

  override def build(
      scopeId: ScopeId,
      appName: String,
      cmdLine: Seq[String],
      category: Option[AsyncCategoryTrace] = None,
      envVariablesToAdd: Map[String, String] = Map.empty,
      envVariablesToClean: Seq[String] = Nil,
      envVariablesToRetain: Option[Set[String]] = None,
      workingDir: Option[Directory] = None,
      separateLogs: Boolean = false,
      lastLogLines: Int = 10
  ): BackgroundProcess = {
    def logPrefix =
      if (scopeId == RootScopeId) s"${OptimusBuildToolBootstrap.generateLogFilePrefix()}.$appName"
      else s"${OptimusBuildToolBootstrap.generateLogFilePrefix()}.$scopeId.$appName"

    val logSuffix = if (separateLogs) "out.log" else "log"
    val logFile = logDir.resolveFile(s"$logPrefix.$logSuffix")

    val errFile = if (separateLogs) Some(logDir.resolveFile(s"$logPrefix.err.log")) else None

    _build(
      scopeId,
      appName,
      cmdLine,
      category,
      envVariablesToAdd,
      envVariablesToClean,
      envVariablesToRetain,
      workingDir,
      logFile,
      errFile,
      lastLogLines
    )
  }

  protected def _build(
      scopeId: ScopeId,
      appName: String,
      cmdLine: Seq[String],
      category: Option[AsyncCategoryTrace],
      envVariablesToAdd: Map[String, String],
      envVariablesToClean: Seq[String],
      envVariablesToRetain: Option[Set[String]],
      workingDir: Option[Directory],
      logFile: FileAsset,
      errFile: Option[FileAsset],
      lastLogLines: Int
  ) = new BackgroundProcess(
    scopeId,
    appName,
    category,
    cmdLine,
    envVariablesToAdd,
    envVariablesToClean,
    envVariablesToRetain,
    workingDir,
    sendCrumbs,
    logFile,
    errFile,
    lastLogLines
  )

}
