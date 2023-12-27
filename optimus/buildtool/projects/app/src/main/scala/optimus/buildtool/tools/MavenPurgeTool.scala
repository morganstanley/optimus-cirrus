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
package optimus.buildtool.tools

import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.app._
import optimus.buildtool.builders.BackgroundProcessBuilder
import optimus.buildtool.config.ScopeId
import optimus.platform._
import optimus.platform.dal.config.DalEnv
import org.kohsuke.args4j
import optimus.buildtool.files.Directory

import scala.collection.immutable.Seq

class MavenPurgeToolCmdLine extends OptimusBuildToolCmdLine {
  @args4j.Option(
    name = "--releasedScope", // for example optimus.interop.interop_client.main
    required = true,
    usage = "Released scope to be removed from Jfrog."
  )
  val releasedScope: String = ""

  @args4j.Option(
    name = "--versions",
    required = true,
    usage = "A comma separated list of released versions."
  )
  private val _versions: String = ""
  lazy val versions: Set[String] = _versions.split(",").toSet.filter(_.nonEmpty)

  @args4j.Option(
    name = "--restore",
    required = false,
    usage = "Restore purged artifacts.(defaults to false)"
  )
  val restore: Boolean = false
}

private[buildtool] object MavenPurgeTool
    extends OptimusApp[MavenPurgeToolCmdLine]
    with OptimusBuildToolAppBase[MavenPurgeToolCmdLine] {

  override def dalLocation = DalEnv("none")
  override protected val log: Logger = getLogger(this)

  @entersGraph override def run(): Unit = {
    import optimus.buildtool.tools.MavenTool._

    val (targetId, transitiveIds) = findScopes(cmdLine, cmdLine.releasedScope)
    val logDir = Directory(cmdLine.logDir)

    def scopeIdToTrainCmd(id: ScopeId, versions: Set[String]): Set[Seq[String]] = {
      val restoreMode = if (cmdLine.restore) "restore" else "purge"
      versions.map(
        Seq(
          artDirectory,
          s"$restoreMode-release",
          "--type",
          "maven",
          "--name",
          s"com.ms.${id.meta}.${id.bundle}:${id.module}",
          "--release",
          _)
      )
    }

    val allCmd = (transitiveIds + targetId).flatMap(scopeIdToTrainCmd(_, cmdLine.versions))
    val outputFile = BackgroundProcessBuilder.id.logFile(logDir)

    runCmds(allCmd, outputFile, targetId, "MavenPurgeTool")

    printLog(outputFile, log)
  }
}
