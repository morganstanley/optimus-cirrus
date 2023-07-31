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

import java.nio.file.Path
import java.time.LocalDateTime

import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.filesanddirs.PathsOpts._
import optimus.stratosphere.scheddel.impl.FileRenamer
import optimus.stratosphere.scheddel.impl.TaskFrequency
import optimus.stratosphere.scheddel.impl.RenameResult
import optimus.stratosphere.scheddel.impl.ScheduledDeletionException
import optimus.stratosphere.scheddel.impl.WindowsTaskScheduler
import optimus.stratosphere.utils.DateTimeUtils._
import optimus.stratosphere.utils.EnvironmentUtils

import scala.collection.immutable.Seq
import scala.util.Failure
import scala.util.Success

object ScheduledDeletionEngine {

  /**
   * @param pathsToDelete
   *   must exist and be absolute
   */
  def renameAndScheduleForDeletion(
      stratoWorkspace: StratoWorkspaceCommon,
      pathsToDelete: Seq[Path]): Seq[RenameResult] = {

    validateDeletionPaths(pathsToDelete)
    val renameResults = pathsToDelete
      .flatMap { path =>
        val renamedOpt = FileRenamer.rename(path)
        logMsg(stratoWorkspace, renamedOpt)
        renamedOpt
      }
    val renamedPaths = renameResults.filter(_.renamed.isSuccess).map(_.renamed.get)

    if (renamedPaths.nonEmpty) {
      scheduleForDeletion(stratoWorkspace, renamedPaths)
    }

    renameResults
  }

  private def scheduleForDeletion(stratoWorkspace: StratoWorkspaceCommon, renamedPaths: Seq[Path]): Unit = {
    val fastCleanupConfigDir = stratoWorkspace.directoryStructure.fastDeleteConfigDir
    val stagingScriptPath = fastCleanupConfigDir.resolve("staging-script.bat")
    val deleteFilesScriptDir = fastCleanupConfigDir.resolve("delete-scripts")

    stagingScriptPath.getParent.dir.create()
    deleteFilesScriptDir.dir.create()

    val dateTimeString = formatDateTime(Patterns.fileDateTimeUpToMillisSuffix, LocalDateTime.now())
    SchedulingScripts.createStagingScript(
      stagingScriptPath = stagingScriptPath,
      scriptsDir = deleteFilesScriptDir,
      autoRemoveScripts = true)
    createDeleteRenamedFilesScript(
      deleteFilesScriptDir = deleteFilesScriptDir,
      paths = renamedPaths,
      dateTimeString = dateTimeString)

    scheduleDeletionTask(stratoWorkspace, stagingScriptPath)
  }

  private def logMsg(stratoWorkspace: StratoWorkspaceCommon, renamedOpt: Option[RenameResult]): Unit = {
    renamedOpt.foreach { renameResult =>
      val srcPath = renameResult.path
      renameResult.renamed match {
        case Success(renamed) => stratoWorkspace.log.info(s"Renamed `$srcPath` to `$renamed`")
        case Failure(e) => stratoWorkspace.log.error(s"""Could not rename `$srcPath` due to $e
                                                        |`$srcPath` will not be deleted""".stripMargin)
      }
    }
  }

  private def scheduleDeletionTask(stratoWorkspace: StratoWorkspaceCommon, stagingScriptPath: Path): Unit = {

    val taskName = "Stratosphere Cleanup Task"

    val logFile = stagingScriptPath.resolveSibling("log.txt")

    val taskCommand =
      Seq("cmd.exe", "/c", stagingScriptPath.toAbsolutePath.toString, ">", logFile.toAbsolutePath.toString, "2>&1")

    val startDateTime = LocalDateTime
      .now()
      .withHour(stratoWorkspace.internal.scheduledCleanup.startHour)
      .withMinute(stratoWorkspace.internal.scheduledCleanup.startMinute)

    val userName = EnvironmentUtils.userName

    WindowsTaskScheduler.scheduleTaskOnce(
      taskName = taskName,
      taskCommand = taskCommand,
      startDateTime = startDateTime,
      userName = userName,
      scheduleFrequency = TaskFrequency.Daily,
      stratoWorkspace)
  }

  private def createDeleteRenamedFilesScript(
      deleteFilesScriptDir: Path,
      paths: Seq[Path],
      dateTimeString: String): Unit = {
    val deleteScript = deleteFilesScriptDir.resolve(s"$dateTimeString-delete-script.bat")

    val deletions = paths
      .flatMap { path =>
        Seq(s"""del /s /f /q "$path"""", s"""rd /s /q "$path"""")
      }

    val filesExistChecks = paths.map { path =>
      s"""if exist "$path" (
         |    echo ERROR: Could not delete $path 1>&2
         |    exit /b 1
         |)""".stripMargin
    }

    val deleteScriptContent = (deletions ++ filesExistChecks).mkString("\n")
    deleteScript.file.write(deleteScriptContent)
  }

  private[scheddel] def validateDeletionPaths(pathsToDelete: Seq[Path]): Unit = {
    val invalidPaths = pathsToDelete.filter(path => !path.exists() || !path.isAbsolute)
    if (invalidPaths.nonEmpty) {
      val invalidPathsString = invalidPaths.mkString(", ")
      throw ScheduledDeletionException(s"Paths to delete do not exists or are not absolute: $invalidPathsString")
    }
  }

}
