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

import optimus.buildtool.config.StaticConfig
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.utils.OsUtils
import optimus.platform.util.Log

import scala.sys.process._
import scala.util._

final case class DeploymentResponse(response: String)
final case class DeploymentException(msg: String) extends RuntimeException(msg)

class DeploymentScriptWriter extends Log {

  val cmdPrefix: Seq[String] = if (OsUtils.isWindows) Seq("cmd", "/k") else Seq("sh", "-c")
  val cmdSeparator: String = if (OsUtils.isWindows) "&&" else ";"

  protected def runCmdString(cmd: String): Try[String] = {
    val fullCommand = cmdPrefix :+ cmd
    log.info(s"Executing command: ${fullCommand.mkString(" ")}")
    val builder = stringSeqToProcess(fullCommand)
    Try(builder.!!)
  }

  private val executable: String = StaticConfig.string("deploymentScriptExecutable")

  private def generateCommand(templateFile: FileAsset, paramsFile: FileAsset, outputDir: Directory): String =
    s"$executable generate deploy --file ${templateFile.pathString} --params-file ${paramsFile.pathString} --output-dir ${outputDir.pathString} --verify"

  private def handleCmdResponse(response: String): Try[DeploymentResponse] = {

    if (response.contains("[ERROR]")) {
      log.error(s"Command returned a non-zero value, error details: $response")
      Failure(DeploymentException(response))
    } else Success(DeploymentResponse(response))
  }

  def generateDeploymentScripts(
      templateFile: FileAsset,
      paramsFile: FileAsset,
      outputDir: Directory): Try[DeploymentResponse] = {

    val commands: Seq[String] =
      Seq(generateCommand(templateFile, paramsFile, outputDir))

    for {
      cmdResponse <- runCmdString(commands.mkString(cmdSeparator))
      deploymentResponse <- handleCmdResponse(cmdResponse)
    } yield deploymentResponse

  }

}
