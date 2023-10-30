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

import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.filesanddirs.PathsOpts._
import optimus.stratosphere.logger.Logger
import optimus.stratosphere.utils.IntellijBackupCause.IntellijBackupCause

import java.io.File
import java.nio.file.attribute.BasicFileAttributes
import java.time.ZonedDateTime

object IntellijBackupCause extends Enumeration {
  type IntellijBackupCause = Value
  val Manual, StratoUpdate, NewIJ, RegenerateIJ = Value
}

object IntellijUtils {
  def wipeOutProjectStructure(
      cause: IntellijBackupCause)(implicit stratoWorkspace: StratoWorkspaceCommon, log: Logger): Unit = {
    val ideaDir = stratoWorkspace.intellijDirectoryStructure.ideaConfigurationStore
    backupProjectStructure(cause)
    ideaDir.deleteIfExists()
  }

  def backupProjectStructure(
      cause: IntellijBackupCause)(implicit stratoWorkspace: StratoWorkspaceCommon, log: Logger): Option[String] = {
    val ideaDir = stratoWorkspace.intellijDirectoryStructure.ideaConfigurationStore
    val backupDir = stratoWorkspace.directoryStructure.backupsIntellij
    backupDir.dir.create()
    log.debug(s"$backupDir dir exists: ${backupDir.exists()}")
    log.debug(s"$ideaDir dir exists: ${ideaDir.exists()}")
    if (ideaDir.exists()) {
      val ijVersion: String = versionForBackupCreation(stratoWorkspace).getOrElse("NO_IJ")
      val stratoVersion: String = stratoWorkspace.stratosphereVersion
      val backupCreationDate: String = DateTimeUtils.formatDateTime("yyyy.MM.dd-HH.mm", ZonedDateTime.now())
      val backupName = s"${ijVersion}_${stratoVersion}_${backupCreationDate}_$cause"
      val targetBackupDir = backupDir.resolve(backupName).dir.create()
      log.debug(s"""Creating workspace backup in $targetBackupDir""")

      def excludeIgnoredDirs: (File, BasicFileAttributes) => Boolean = {
        val ignoredPluginDirs = stratoWorkspace.intellij.backup.dirsToSkip
        (file, _) => !ignoredPluginDirs.exists(_.contains(file.getName))
      }

      ideaDir.dir.copyTo(targetBackupDir, shouldDirBeCopied = excludeIgnoredDirs)
      backupDir.dir.rotate(keep = stratoWorkspace.intellij.backup.maxBackupsCount)
      Some(backupName)
    } else None
  }

  private def versionForBackupCreation(stratoWorkspace: StratoWorkspaceCommon): Option[String] = {
    val versionFile = stratoWorkspace.directoryStructure.ideVersion
    versionFile.file.content()
  }
}
