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

import optimus.stratosphere.common.IntellijConfigStore
import optimus.stratosphere.common.IntellijConfigStore.IntellijConfigStore
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.filesanddirs.PathsOpts._
import optimus.stratosphere.logger.Logger
import optimus.stratosphere.utils.IntellijBackupCause.IntellijBackupCause

import java.io.File
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

sealed trait IntellijBackupEntry extends Ordered[IntellijBackupEntry] {
  def date: LocalDateTime
  override def compare(that: IntellijBackupEntry): Int = this.date.compareTo(that.date)
}

final case class NonVerifiedIntellijBackupEntry(
    fullEntryName: String,
    date: LocalDateTime = LocalDateTime.of(2023, 1, 1, 0, 0))
    extends IntellijBackupEntry {
  override def toString: String = fullEntryName
}

final case class DetailedIntellijBackupEntry(
    intellijVersion: String,
    stratoVersion: String,
    date: LocalDateTime,
    cause: String)
    extends IntellijBackupEntry {
  override def toString: String =
    s"${intellijVersion}_${stratoVersion}_${date.format(IntellijBackupEntry.dateFormat)}_$cause"
}

object IntellijBackupEntry {
  val dateFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy.MM.dd-HH.mm")

  def apply(entryName: String): IntellijBackupEntry = {
    entryName.split('_') match {
      case Array(ijVersion, stratoVersion, dateStr, cause) => {
        val date = LocalDateTime.parse(dateStr, dateFormat)
        DetailedIntellijBackupEntry(ijVersion, stratoVersion, date, cause)
      }
      case _ => NonVerifiedIntellijBackupEntry(entryName)
    }
  }
}

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
      val backupEntry = DetailedIntellijBackupEntry(ijVersion, stratoVersion, LocalDateTime.now(), cause.toString)
      val backupName = backupEntry.toString
      val targetBackupDir = backupDir.resolve(backupName).dir.create()
      log.debug(s"""Creating workspace backup in $targetBackupDir""")

      def excludeIgnoredDirs: (File, BasicFileAttributes) => Boolean = {
        val ignoredPluginDirs = stratoWorkspace.intellij.backup.dirsToSkip
        (file, _) => !ignoredPluginDirs.exists(_.contains(file.getName))
      }

      ideaDir.dir.copyTo(targetBackupDir, shouldDirBeCopied = excludeIgnoredDirs)
      backupConfigStore(IntellijConfigStore.workspace, ijVersion, targetBackupDir)
      backupConfigStore(IntellijConfigStore.scratches, ijVersion, targetBackupDir)
      backupDir.dir.rotate(keep = stratoWorkspace.intellij.backup.maxBackupsCount)
      Some(backupName)
    } else None
  }

  private def backupConfigStore(configStoreName: IntellijConfigStore, fromVersion: String, targetBackupDir: Path)(
      implicit stratoWorkspace: StratoWorkspaceCommon): Unit = {
    val configStoreDir =
      stratoWorkspace.intellijDirectoryStructure.intellijConfigStoreSection(configStoreName, fromVersion)
    val configStoreBackupDir = targetBackupDir.resolve(configStoreName.toString).dir.create()
    configStoreDir.foreach(_.dir.copyTo(configStoreBackupDir))
  }

  private def versionForBackupCreation(stratoWorkspace: StratoWorkspaceCommon): Option[String] = {
    val versionFile = stratoWorkspace.directoryStructure.ideVersion
    versionFile.file.content()
  }
}
