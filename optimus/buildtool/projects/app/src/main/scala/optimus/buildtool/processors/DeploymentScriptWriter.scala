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
package optimus.buildtool.processors

import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.builders.BackgroundCmdId
import optimus.buildtool.builders.BackgroundProcessBuilder
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.StaticConfig
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.trace.DeploymentScriptCommand
import optimus.buildtool.utils.OsUtils
import optimus.platform._

import java.nio.file.Files
import scala.collection.immutable.Seq
import scala.util._

final case class DeploymentResponse(response: String)
final case class DeploymentException(msg: String) extends RuntimeException(msg)

object DeploymentScriptWriter extends DeploymentScriptWriter {
  private val log = getLogger(this)
  private val id = BackgroundCmdId("DeploymentScriptWriter")
}

class DeploymentScriptWriter {
  import DeploymentScriptWriter._

  val cmdPrefix: Seq[String] = if (OsUtils.isWindows) Seq("cmd", "/c") else Seq("sh", "-c")
  val cmdSeparator: String = if (OsUtils.isWindows) "&&" else ";"

  @async protected def runCmdString(cmd: String, scopeId: ScopeId, logFile: FileAsset): Try[String] =
    asyncResult {
      BackgroundProcessBuilder(id, logFile, cmdPrefix :+ cmd, useCrumbs = false)
        .build(scopeId, DeploymentScriptCommand, lastLogLines = 100)

      Files.readString(logFile.path)
    }.toTry

  private val executable: String = StaticConfig.string("deploymentScriptExecutable")

  private def generateCommand(templateFile: FileAsset, paramsFile: FileAsset, outputDir: Directory): String =
    s"$executable generate deploy --file ${templateFile.pathString} --params-file ${paramsFile.pathString} --output-dir ${outputDir.pathString}"

  private def handleCmdResponse(response: String): Try[DeploymentResponse] = {
    if (response.contains("[ERROR]")) {
      log.error(s"Command returned an ERROR in the log; error details: $response")
      Failure(DeploymentException(response))
    } else Success(DeploymentResponse(response))
  }

  @async def generateDeploymentScripts(
      templateFile: FileAsset,
      paramsFile: FileAsset,
      outputDir: Directory,
      scopeId: ScopeId,
      logFile: FileAsset): Try[DeploymentResponse] = {
    val commands: Seq[String] =
      Seq(generateCommand(templateFile, paramsFile, outputDir))
    for {
      cmdResponse <- runCmdString(commands.mkString(cmdSeparator), scopeId, logFile)
      deploymentResponse <- handleCmdResponse(cmdResponse)
    } yield deploymentResponse
  }
}
