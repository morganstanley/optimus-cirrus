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
package optimus.buildtool.compilers.npm

import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.builders.BackgroundCmdId
import optimus.buildtool.builders.BackgroundProcessBuilder
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.trace.NpmCommand
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.Utils
import optimus.platform._
import optimus.platform.util.Log

import scala.collection.immutable.Seq

object AsyncNpmCommandRunner extends Log {
  // we allow arbitrary cmds be used in sandbox, but we have to warn user it's dangerous.
  // for most of case we must ensure cmd destination directory should be inside of sandbox, otherwise it's non-RT.
  // and pay attention when cache hit we won't trigger web cmds again!
  private val dangerousCmds: Seq[String] =
    Seq("mkdir", "rmdir", "cp", "mv", "rm", "touch", "mkfs", "mkswap", "tar", "untar", "dd", "zip")

  @async private def runWebProcess(cmd: String, logFile: FileAsset, sandboxSrc: Directory, id: ScopeId): Unit = {
    val cmds =
      if (Utils.isWindows) Seq("cmd.exe", "/c", cmd)
      else Seq("ksh", "-c", cmd)
    BackgroundProcessBuilder(BackgroundCmdId("Web"), logFile, cmds, workingDir = Some(sandboxSrc))
      .buildWithRetry(id, NpmCommand)(
        maxRetry = 3, // retry 3 times to prevent maven server setup unstable issue
        msDelay = 0,
        lastLogLines = 100 // reasonable length to avoid too large web cmds log throw msg
      )
  }

  @node def runNpmCmd(
      id: ScopeId,
      commandTemplate: String,
      nodeVersion: String,
      pnpmVersion: String,
      npmBuildCommands: Seq[String],
      sandboxSrc: Directory,
      pnpmStoreDir: Directory,
      logFile: FileAsset): Seq[CompilationMessage] = {
    ObtTrace.info(s"[${id.toString}] Downloading npm remote artifacts from maven server")
    val userCmd = npmBuildCommands
      .map(c =>
        c.replace(
          "npx pnpm install",
          s"npm config set store-dir ${pnpmStoreDir.path} && npx pnpm@$pnpmVersion install"))
      .mkString(" && ")
    val cmd = commandTemplate
      .replace("{nodeVersion}", nodeVersion)
      .replace("{pnpmVersion}", pnpmVersion)
      .replace("{pnpmStoreDir}", pnpmStoreDir.pathString)
      .replace("{userCmd}", userCmd)
    runWebProcess(cmd, logFile, sandboxSrc, id)
    val warnCmds = dangerousCmds.collect { case dangerousCmd if cmd.contains(dangerousCmd) => s"'$dangerousCmd'" }
    if (warnCmds.isEmpty) Seq(CompilationMessage.info(cmd))
    else
      Seq(
        CompilationMessage.warning(
          s"use ${warnCmds.mkString(",")} may cause non-RT untracked I/O outside of sandbox, please make sure your destination is in sandbox"),
        CompilationMessage.info(cmd)
      )

  }
}
