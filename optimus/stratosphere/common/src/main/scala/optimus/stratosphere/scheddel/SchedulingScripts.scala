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
package optimus.stratosphere.scheddel

import optimus.stratosphere.bootstrap.OsSpecific
import optimus.stratosphere.config.StratoWorkspace
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.filesanddirs.PathsOpts._
import optimus.stratosphere.scheddel.impl.TaskFrequency
import optimus.stratosphere.scheddel.impl.WindowsTaskScheduler
import optimus.stratosphere.utils.CommonProcess
import optimus.stratosphere.utils.EnvironmentUtils
import optimus.utils.ExitCode

import java.nio.file.Path
import java.time.ZonedDateTime
import scala.collection.immutable.Seq
import scala.util.Random
import scala.util.control.NonFatal

object SchedulingScripts extends ExitCode {

  def createStagingScript(stagingScriptPath: Path, scriptsDir: Path, autoRemoveScripts: Boolean = false): Unit = {

    val logFile = scriptsDir.resolveSibling("logs").dir.create()

    val removeFile =
      if (autoRemoveScripts) {
        """|if %errorlevel% EQU 0 (
           |  echo Deleting %script%
           |  call rm %script%
           |)""".stripMargin
      } else {
        ""
      }

    val scriptContent = s"""|@echo off
                            |pushd $scriptsDir
                            |
                            |echo Invoking scripts..
                            |for %%f in (.\\*.bat) do ( call :invoke-script %%f )
                            |
                            |goto :eof
                            |
                            |:invoke-script
                            |set script=%1
                            |echo Invoking %script%
                            |pushd $scriptsDir
                            |call %script% > $logFile%script%_log.txt 2>&1
                            |
                            |$removeFile
                            |
                            |exit /b
                            |
                            |:eof
                            |echo Invoking scripts completed
                            |popd""".stripMargin

    stagingScriptPath.file.write(scriptContent)
  }

  private def taskFileName(task: String) = s"$task-script.bat"

  private def taskScriptFileName(task: String, workspace: StratoWorkspaceCommon) = {
    val workspaceName = workspace.directoryStructure.stratosphereWorkspaceName
    s"$task-$workspaceName.bat"
  }

  def scheduleTask(
      taskName: String,
      scriptContent: Seq[String],
      scriptLocation: Path,
      workspace: StratoWorkspaceCommon,
      startHour: Int = 3,
      endHour: Int = 6): ExitCode = {

    val date = timeFromInterval(startHour, endHour)

    if (OsSpecific.isLinux) {
      workspace.log.warning(s"Scheduling tasks is available only for Windows machines")
      ExitCode.Failure
    } else {
      val script = scriptLocation.resolve(taskFileName(taskName))
      val scriptsDir = scriptLocation.resolve("scripts")

      try {
        script.getParent.dir.create()
        scriptsDir.dir.create()

        createStagingScript(script, scriptsDir, autoRemoveScripts = false)
        createScript(taskName, scriptsDir, scriptContent, workspace)

        val logFile = script.resolveSibling("log.txt")
        val command =
          CommonProcess.preamble ++ Seq(script.toAbsolutePath.toString, ">", logFile.toAbsolutePath.toString, "2>&1")

        val user = EnvironmentUtils.userName
        WindowsTaskScheduler.scheduleTask(taskName, command, date, user, TaskFrequency.WorkDay, workspace)
        ExitCode.Success
      } catch {
        case NonFatal(e) =>
          workspace.log.error(s"Scheduling task: $taskName failed due to: $e")
          ExitCode.Failure
      }
    }
  }

  def removeTask(taskName: String, scriptLocation: Path, workspace: StratoWorkspace): ExitCode = {
    val scriptFileName = taskScriptFileName(taskName, workspace)
    val scriptLocations = scriptLocation
      .resolve("scripts")
      .dir
      .listFiles()
      .find(path => path.name == scriptFileName)

    try {
      scriptLocations match {
        case Some(location) =>
          location.file.deleteIfExists()
          workspace.log.highlight(s"Task: $taskName for this workspace has been successfully disabled")
          ExitCode.Success
        case None =>
          workspace.log.warning(s"There are no scheduled tasks for this workspace!")
          ExitCode.Success
      }
    } catch {
      case NonFatal(e) =>
        workspace.log.error(s"Removing of scheduled task: $taskName failed due to: $e")
        ExitCode.Failure
    }
  }

  private def createScript(
      taskName: String,
      scriptLocation: Path,
      commands: Seq[String],
      workspace: StratoWorkspaceCommon): Unit = {
    val workspaceDir = workspace.directoryStructure.sourcesDirectory
    val scriptName = taskScriptFileName(taskName, workspace)
    val fetchScript = scriptLocation.resolve(scriptName)

    val scriptBeginning = s"""|pushd $workspaceDir
                              |cd %1
                              |""".stripMargin

    val scriptContent = commands.mkString(scriptBeginning, "\n", "\npopd")

    fetchScript.file.write(scriptContent)
  }

  private def timeFromInterval(startHour: Int, endHour: Int): ZonedDateTime = {
    ZonedDateTime
      .now()
      .withHour(Random.nextInt(endHour - startHour + 1))
      .withMinute(Random.nextInt(60))
  }
}
