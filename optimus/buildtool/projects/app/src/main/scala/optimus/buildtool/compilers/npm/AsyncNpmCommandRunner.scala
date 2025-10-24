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
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.trace.NpmCommand
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.Utils
import optimus.buildtool.utils.process.ExternalProcessBuilder
import optimus.platform._
import optimus.platform.util.Log

import scala.util.Properties

object AsyncNpmCommandRunner extends Log {
  // we allow arbitrary cmds be used in sandbox, but we have to warn user it's dangerous.
  // for most of case we must ensure cmd destination directory should be inside of sandbox, otherwise it's non-RT.
  // and pay attention when cache hit we won't trigger web cmds again!
  private val dangerousCmds: Seq[String] =
    Seq("mkdir", "rmdir", "cp", "mv", "rm", "touch", "mkfs", "mkswap", "tar", "untar", "dd", "zip")

  // the internal npm setup code on Windows/Linux needs these to be passed though. The rest of the env will be cleared
  // to improve consistency between different users / machines
  private val envVarsToRetain =
    if (Properties.isWin) Set("USERPROFILE", "SYS_LOC", "PROCESSOR_ARCHITECTURE", "PREFIX")
    else Set("JAVA_KRB5CCNAME", "KRB5CCNAME", "HOME")

  @async private def runWebProcess(
      cmd: String,
      processBuilder: ExternalProcessBuilder,
      sandboxSrc: Directory,
      id: ScopeId
  ): Unit = {
    val cmdLine =
      if (Utils.isWindows) Seq("cmd.exe", "/c", cmd)
      else Seq("ksh", "-c", cmd)
    processBuilder
      .build(
        id,
        "web",
        cmdLine,
        Some(NpmCommand),
        workingDir = Some(sandboxSrc),
        envVariablesToRetain = Some(envVarsToRetain),
        lastLogLines = 100 // reasonable length to avoid too large web cmds log throw msg
      )
      .startWithRetry(
        maxRetries = 3, // retry 3 times to prevent maven server setup unstable issue
        msDelay = 0
      )
  }

  @async def runNpmCmd(
      id: ScopeId,
      commandTemplate: String,
      nodeVersion: String,
      pnpmVersion: String,
      npmBuildCommands: Seq[String],
      processBuilder: ExternalProcessBuilder,
      sandboxSrc: Directory,
      pnpmStoreDir: Directory
  ): Seq[CompilationMessage] = {
    ObtTrace.info(s"[${id.toString}] Downloading npm remote artifacts from maven server")
    val cmd = commandTemplate
      .replace("{nodeVersion}", nodeVersion)
      .replace("{pnpmVersion}", pnpmVersion)
      .replace("{pnpmStoreDir}", pnpmStoreDir.pathString)
      .replace("{userCmd}", npmBuildCommands.mkString(" && "))
    runWebProcess(cmd, processBuilder, sandboxSrc, id)
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
