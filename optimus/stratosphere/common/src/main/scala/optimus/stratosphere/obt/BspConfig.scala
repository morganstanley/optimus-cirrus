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
package optimus.stratosphere.obt

import optimus.stratosphere.bootstrap.GitProcess
import optimus.stratosphere.bootstrap.OsSpecific
import optimus.stratosphere.config.StratoWorkspaceCommon

import scala.collection.immutable.Seq

final case class BspConfig(
    name: String,
    argv: Seq[String],
    javaOpts: Seq[String],
    version: String,
    bspVersion: String,
    languages: Seq[String]
)

object BspConfig {

  def serverPath(stratoWorkspace: StratoWorkspaceCommon): String =
    s"${stratoWorkspace.internal.obt.install}/bin/${stratoWorkspace.internal.obt.serverExecutable}${OsSpecific.shellExt16}"

  def defaultArgs(stratoWorkspace: StratoWorkspaceCommon): Seq[String] =
    serverPath(stratoWorkspace) +: stratoWorkspace.obt.server.args

  def sparseArgs(stratoWorkspace: StratoWorkspaceCommon): Seq[String] =
    if (GitProcess.isSparseReady(stratoWorkspace.config)) Seq("--allowSparse") else Seq()

  def apply(stratoWorkspace: StratoWorkspaceCommon): BspConfig = {

    new BspConfig(
      name = "OBT",
      argv = defaultArgs(stratoWorkspace) ++ sparseArgs(stratoWorkspace),
      javaOpts = stratoWorkspace.obt.server.opts,
      version = "1.0",
      bspVersion = "2.0",
      languages = List("scala", "java")
    )
  }
}
