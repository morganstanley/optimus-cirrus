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
package optimus.stratosphere.utils

import optimus.stratosphere.bootstrap.OsSpecific
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.obt.BspConfig

import java.nio.file.Path
import scala.collection.immutable.Seq

trait ObtUtils {

  def obt(implicit workspace: StratoWorkspaceCommon): Path =
    scriptPath(workspace.internal.obt.executable)

  def obtTest(implicit workspace: StratoWorkspaceCommon): Path =
    scriptPath(workspace.internal.obt.testRunnerExecutable)

  def obtRun(implicit workspace: StratoWorkspaceCommon): Path =
    scriptPath(workspace.internal.obt.appRunnerExecutable)

  private def scriptPath(executable: String)(implicit workspace: StratoWorkspaceCommon): Path =
    workspace.internal.obt.install
      .resolve("bin")
      .resolve(s"$executable${OsSpecific.shellExt16}")

  def sparseArgs(implicit ws: StratoWorkspaceCommon): Seq[String] = BspConfig.sparseArgs(ws)

}

object ObtUtils extends ObtUtils
