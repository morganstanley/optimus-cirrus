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
import optimus.buildtool.config.ObtConfig
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.StaticConfig
import optimus.platform._
import optimus.platform.dal.config.DalEnv
import org.kohsuke.args4j
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.trace.MavenApp

import scala.collection.immutable.Seq

object MavenTool {
  val artDirectory = StaticConfig.string("mavenExePath")

  // each art cmd will contain 2~4 lines msgs, set 40 to limit msgs for last 10~20 cmds.
  private val printLogLines = 40

  @node def findScopes(cmdLine: OptimusBuildToolCmdLine, scopeIdStr: String): (ScopeId, Set[ScopeId]) = {
    val obtConfig: ObtConfig = OptimusBuildToolImpl(cmdLine, NoBuildInstrumentation).obtConfig
    val targetId = obtConfig.scope(scopeIdStr)
    val transitiveIds = MigrationTrackerHelper(obtConfig.scopeDefinitions).transitiveInternalDeps(targetId)
    (targetId, transitiveIds)
  }

  @node def runCmds(allCmd: Set[Seq[String]], outputFile: FileAsset, rootId: ScopeId, name: String): Unit = {
    allCmd.apar.foreach { cmd =>
      BackgroundProcessBuilder
        .onDemand(outputFile, cmd)
        .build(rootId, MavenApp(name), printLogLines)
    }
  }

  def printLog(outputFile: FileAsset, log: Logger): Unit = {
    val msgs = BackgroundProcessBuilder.lastLogLines(outputFile, printLogLines) :+ s"Output saved in:${outputFile.path}"
    log.info(msgs.mkString("\n", "\n", ""))
  }

}

class MavenReserveToolCmdLine extends OptimusBuildToolCmdLine {
  @args4j.Option(
    name = "--reserveScope", // for example optimus.interop.interop_client.main
    required = true,
    usage = "Scope to be reserved for the maven release."
  )
  val reserveScope: String = ""

  // TODO (OPTIMUS-56553): use alternative or more generic term for eonID
  @args4j.Option(
    name = "--eonID",
    required = true,
    usage = "Suitable eonID for your scope and transitive internal dependencies."
  )
  val eonID: String = ""

  @args4j.Option(
    name = "--contract",
    required = true,
    usage = "Release contract for your scope and transitive internal dependencies."
  )
  val contract: String = ""
}

private[buildtool] object MavenReserveTool
    extends OptimusApp[MavenReserveToolCmdLine]
    with OptimusBuildToolAppBase[MavenReserveToolCmdLine] {

  override def dalLocation: DalEnv = DalEnv("none")
  override protected val log: Logger = getLogger(this)

  @entersGraph override def run(): Unit = {
    import optimus.buildtool.tools.MavenTool._

    val logDir = Directory(cmdLine.logDir)
    val (targetId, transitiveIds) = findScopes(cmdLine, cmdLine.reserveScope)

    def scopeIdToTrainCmd(id: ScopeId): Seq[String] =
      Seq(
        artDirectory,
        "create-package",
        "--type",
        "maven",
        "--name",
        s"com.ms.${id.meta}.${id.bundle}:${id.module}",
        "--eon",
        cmdLine.eonID,
        "--contract",
        cmdLine.contract
      )

    val allCmd = (transitiveIds + targetId).map(scopeIdToTrainCmd)
    val outputFile = BackgroundProcessBuilder.id.logFile(logDir)

    runCmds(allCmd, outputFile, targetId, "MavenReserveTool")

    printLog(outputFile, log)
  }
}
