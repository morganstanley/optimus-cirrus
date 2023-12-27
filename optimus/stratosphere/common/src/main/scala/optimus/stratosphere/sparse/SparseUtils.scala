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
package optimus.stratosphere.sparse

import optimus.stratosphere.bootstrap.GitProcess
import optimus.stratosphere.bootstrap.StratosphereException
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.filesanddirs.PathsOpts._
import optimus.stratosphere.updater.GitUpdater
import optimus.stratosphere.utils.GitUtils
import optimus.stratosphere.utils.MemUnit

import java.nio.file.Path
import java.nio.file.Paths
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.Seq
import scala.util.Try
import scala.util.control.NonFatal

trait SparseUtils {

  def sparseOpen(scopesOrProfiles: Seq[String], append: Boolean = false)(implicit
      stratoWorkspace: StratoWorkspaceCommon): Unit = {
    val currentConfig = SparseConfiguration.load()

    val newProfile: SparseProfile = {
      if (scopesOrProfiles.isEmpty) {
        if (currentConfig.isEnabled) stratoWorkspace.log.info("Sparse checkout is already enabled.")
        currentConfig.profile match {
          case Some(profile) =>
            stratoWorkspace.log.info("Enabling previous sparse checkout configuration.")
            profile
          case _ =>
            throw new StratosphereException(
              "Cannot reopen previous profile, please specify which profiles(s) or scope(s) you want to open.")
        }
      } else {
        partitionProfilesAndScopes(scopesOrProfiles) match {
          case (Seq(singleProfileName), Seq()) if !append =>
            SparseProfile.load(singleProfileName) match {
              case Some(profile) =>
                profile
              case None =>
                throw new StratosphereException(
                  s"Profile $singleProfileName does not exist. Please run `strato sparse show` to view all available profiles.")
            }
          case (profiles, scopes) if append =>
            SparseProfile.custom(
              (currentConfig.profile.toSet.flatMap((p: SparseProfile) => p.scopes) ++ scopes).toSet,
              (currentConfig.profile.toSet.flatMap((p: SparseProfile) => p.subProfiles) ++ profiles).toSet
            )
          case (profiles, scopes) =>
            SparseProfile.custom(scopes.toSet, profiles.toSet)
        }
      }
    }

    val sparseSetChanged = !(currentConfig.isEnabled && currentConfig.profile.contains(newProfile))

    if (sparseSetChanged) {
      updateSparseSet(newProfile)
    } else {
      stratoWorkspace.log.highlight("Skipping reapplying settings.")
    }
  }

  def partitionProfilesAndScopes(scopesOrProfiles: Seq[String]): (Seq[String], Seq[String]) = {
    val (scopes, profiles) = scopesOrProfiles.partition(_.contains("."))
    (profiles, scopes)
  }

  private def printWrongParamWarning(tpe: String)(existing: Set[String], toClose: Set[String])(implicit
      stratoWorkspace: StratoWorkspaceCommon): Unit = {
    val notOpenedOrUnknown = toClose.diff(existing)
    if (notOpenedOrUnknown.nonEmpty) {
      stratoWorkspace.log.warning(s"Cannot close unknown or not opened $tpe:")
      notOpenedOrUnknown.foreach(name => stratoWorkspace.log.warning(s" - $name"))
    }
  }

  def sparseClose(scopesOrProfiles: Seq[String])(implicit stratoWorkspace: StratoWorkspaceCommon): Unit = {
    val currentConfig = SparseConfiguration.load()
    if (scopesOrProfiles.isEmpty) {
      if (!currentConfig.isEnabled) {
        stratoWorkspace.log.info("Sparse checkout already disabled.")
      } else {
        stratoWorkspace.log.info("Disabling sparse checkout. Please wait, it can take a moment...")
        runSparseCmd("disable")
        // Needed for git version lower than 2.25, otherwise sparse set will be reopen on checkout
        runGitCmd("config", "core.sparsecheckout", "false")
        backupIgnoredFiles(Set())
        currentConfig.copy(isEnabled = false).save()
      }
    } else {
      currentConfig.profile.foreach { currentProfile =>
        val (profilesToClose, scopesToClose) = partitionProfilesAndScopes(scopesOrProfiles)
        printWrongParamWarning("profile")(currentProfile.subProfiles, profilesToClose.toSet)
        printWrongParamWarning("scope")(currentProfile.scopes, scopesToClose.toSet)
        val customProfile = SparseProfile.custom(
          currentProfile.scopes.diff(scopesToClose.toSet),
          currentProfile.subProfiles.diff(profilesToClose.toSet)
        )
        updateSparseSet(customProfile)
      }
    }
  }

  def refresh(updatedProfile: Option[SparseProfile] = None)(implicit stratoWorkspace: StratoWorkspaceCommon): Unit = {
    val config = SparseConfiguration.load()
    if (!stratoWorkspace.isOutsideOfWorkspace && config.isEnabled)
      updateSparseSet(updatedProfile.getOrElse(config.profile.orNull), forceReapply = true)
  }

  def printConfigurationInfo(printWarningIfDisabled: Boolean)(implicit stratoWorkspace: StratoWorkspaceCommon): Unit = {
    val config = SparseConfiguration.load()
    if (!stratoWorkspace.isOutsideOfWorkspace) {
      if (config.isEnabled) {
        printCurrentProfile()
      } else if (printWarningIfDisabled) {
        stratoWorkspace.log.info("Sparse checkout is disabled. To enable it please run `strato sparse open`.")
      }
    }
  }

  protected def printCurrentProfile()(implicit stratoWorkspace: StratoWorkspaceCommon): Unit = {
    val config = SparseConfiguration.load()
    config.profile.foreach { currentProfile =>
      if (currentProfile.isCustomProfile) {
        stratoWorkspace.log.info("")
        stratoWorkspace.log.info("Opened profiles:")
        currentProfile.subProfiles.toList.sorted.foreach(profile => stratoWorkspace.log.info(s"  - $profile"))
        stratoWorkspace.log.info("Opened scopes:")
        currentProfile.scopes.toList.sorted.foreach(scope => stratoWorkspace.log.info(s"  - $scope"))
      } else {
        stratoWorkspace.log.info("")
        stratoWorkspace.log.info("Opened profile:")
        stratoWorkspace.log.info(currentProfile.name)
      }
    }
  }

  private def timestampedDirName(): String = {
    val date = ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyy.MM.dd-HH.mm.ss"))
    s"sparse-$date"
  }

  private def filterPathsOutsideOfSparse(sourceDir: Path, sparseDirs: Set[Path])(
      relativePaths: Seq[String]): Seq[Path] = {

    def isInSparseProfile(path: Path) = sparseDirs.exists(sd => path.startsWith(sd))
    def isTopLevelFile(path: Path) = path.getNameCount <= 1
    def isOnRootPath(path: Path) = path.isFile && sparseDirs.exists(_.startsWith(path.getParent))

    relativePaths.map(Paths.get(_)).filterNot { path =>
      !sourceDir.resolve(path).exists() || isInSparseProfile(path) || isTopLevelFile(path) || isOnRootPath(path)
    }
  }

  private def restoreBackupFilesFromLegacyGitStash()(implicit ws: StratoWorkspaceCommon): Unit = {
    val sparseStashName = "strato-sparse-stash"

    val stashList = runGitCmd("stash", "list").split("\r?\n|\r")
    val stratoStashEntry = stashList.find(_.contains(sparseStashName))
    stratoStashEntry.foreach { line =>
      val pattern = """stash@\{\d+\}""".r
      val stashAddress = pattern.findFirstIn(line)
      ws.log.info(s"  Restoring stashed files from git")
      try {
        stashAddress.foreach(address => runGitCmd("stash", "pop", address))
      } catch {
        case NonFatal(e) =>
          // If we failed it might be good idea to rename that stash entry to not try again next time.
          // Unfortunately there is no simple way to do it without restashing.
          ws.log.info("  Restoring stashed files from git failed, skipping...")
      }
    }
  }

  def backupIgnoredFiles(keepOpenDirs: Set[Path])(implicit ws: StratoWorkspaceCommon): Unit = {
    ws.log.info("Processing auto-generated files and local runconfs...")

    val backupDir = ws.directoryStructure.backupsSparse
    val sourceDir = ws.directoryStructure.sourcesDirectory
    val currentBackup = backupDir.resolve("current")
    val archiveBackup = backupDir.resolve(timestampedDirName())

    // Step 1: restore all backup files keep in the git stash
    // This is needed to support legacy workspaces which already used previous stashing implementation
    restoreBackupFilesFromLegacyGitStash()

    // Step 2: backup all ignored files from outside sparse profile, except those defined in config to be skipped
    // No dirs to skip means we are closing the sparse checkout, so we don't need to stash anything
    if (keepOpenDirs.nonEmpty) {
      val skipBackupPatterns = ws.internal.sparse.backup.skipPatterns
      val filesCountLimit = ws.internal.sparse.backup.maxFilesCount

      val gitUtils = GitUtils(ws)
      def filterSparse = filterPathsOutsideOfSparse(sourceDir, keepOpenDirs) _
      val relativePathsToFilesToBackup = filterSparse(
        gitUtils.listIgnoredFiles(listDirectories = false, additionalGitIgnorePatterns = skipBackupPatterns))

      if (relativePathsToFilesToBackup.size > filesCountLimit) {
        throw new StratosphereException(
          s"""Sparse checkout aborted, exceeded limit of files to backup.
             |Tried to backup ${relativePathsToFilesToBackup.size} where limit is $filesCountLimit
             |That might indicate problems with your workspace, in case of doubts please contact ${ws.internal.helpMailGroup}.""".stripMargin)
      }

      relativePathsToFilesToBackup.foreach { relativePath =>
        val sourceFile = sourceDir.resolve(relativePath)
        val targetBackupFile = currentBackup.resolve(relativePath)

        val sizeLimit = ws.internal.sparse.backup.maxFileSizeKb
        if (sourceFile.toFile.length() > sizeLimit.bytes) {
          ws.log.info(s"  Skipping backup of $relativePath because file is bigger than ${sizeLimit.in(MemUnit.KB)}kB")
        } else {
          ws.log.info(s"  Backing-up ${relativePath.toString} file")
          try {
            sourceFile.file.move(targetBackupFile)
          } catch {
            case NonFatal(e) =>
              ws.log.error(s"Cannot backup file $relativePath", e)
          }
        }
      }

      val relativePathsToRemove = filterSparse(gitUtils.listIgnoredFiles())
      ws.log.info(s"  Removing ${relativePathsToRemove.size} autogenerated file(s)")
      relativePathsToRemove.foreach { relativePath =>
        sourceDir.resolve(relativePath).delete()
      }

      val backedUpFilesCount = relativePathsToFilesToBackup.map(currentBackup.resolve).count(_.exists())
      ws.log.info(s"  Backed-up $backedUpFilesCount file(s) in ${currentBackup.toString}")
    }

    // Step 3: Restore all backed-up files matching current sparse profile
    if (currentBackup.exists()) {
      val dirsToRestore =
        if (keepOpenDirs.isEmpty) Seq(currentBackup)
        else keepOpenDirs.map(currentBackup.resolve).filter(_.exists())
      val filesToRestore = dirsToRestore.flatMap(_.dir.listPathsRecursively().filter(_.isFile))

      filesToRestore.foreach { currentBackupFile =>
        val relativePath = currentBackup.relativize(currentBackupFile)
        val targetSourceFile = sourceDir.resolve(relativePath)
        val archiveBackupFile = archiveBackup.resolve(relativePath)

        if (targetSourceFile.exists()) {
          ws.log.info(s"  File ${relativePath.toString} already exists, skipping")
          currentBackupFile.move(archiveBackupFile)
        } else {
          ws.log.info(s"  Restoring ${relativePath.toString}")
          try {
            currentBackupFile.file.move(targetSourceFile)
          } catch {
            case NonFatal(e) =>
              ws.log.error(s"Cannot restore file $relativePath", e)
              currentBackupFile.move(archiveBackupFile)
          }
        }
      }

      if (filesToRestore.exists(_.exists())) {
        ws.log.info(s"  ${filesToRestore.size} files could not be restored, they were archived in $archiveBackup")
      }
    } else {
      ws.log.info("  No backups to restore")
    }
  }

  protected def updateSparseSet(newProfile: SparseProfile, forceReapply: Boolean = false)(implicit
      stratoWorkspace: StratoWorkspaceCommon): Unit = {
    val currentConfig = SparseConfiguration.load()

    stratoWorkspace.log.info("Updating set of opened scopes/profiles. Please wait, it can take a moment...")

    if (!GitProcess.isSparseReady(stratoWorkspace.config))
      stratoWorkspace.log.info("Upgrading git to version with sparse checkout support.")
    new GitUpdater(stratoWorkspace).ensureGitInstalled(useGitFromArtifactory = true)

    val dirStructure = stratoWorkspace.directoryStructure
    val gitSparseConfig = dirStructure.gitDirectory.resolve("info").resolve("sparse-checkout")
    gitSparseConfig.file.deleteIfExists()

    val dirsToOpen = newProfile.listAllRelativeDirs()

    backupIgnoredFiles(dirsToOpen)
    runSparseCmd("set" +: dirsToOpen.map(_.toString).toSeq: _*)
    if (forceReapply) runSparseCmd("reapply")

    SparseConfiguration(isEnabled = true, Some(newProfile)).save()
    printCurrentProfile()
  }

  protected def runGitCmd(args: String*)(implicit stratoWorkspace: StratoWorkspaceCommon): String = {
    GitUtils(stratoWorkspace).runGit(args: _*)
  }

  protected def runSparseCmd(args: String*)(implicit stratoWorkspace: StratoWorkspaceCommon): Boolean = {
    Try(runGitCmd("sparse-checkout" +: args: _*)).isSuccess
  }

  def checkLocalChangesForUncommittedFiles(ws: StratoWorkspaceCommon): Seq[String] = {
    ws.log.info("Checking local changes...")
    val gitUtils = GitUtils(ws)
    val localChanges = gitUtils.localChanges()
    val skipBackupPatterns: Seq[String] = ws.internal.sparse.backup.skipPatterns
    val ignoredFiles =
      gitUtils.listIgnoredFiles(listDirectories = false, additionalGitIgnorePatterns = skipBackupPatterns)
    localChanges.filterNot { file => ignoredFiles.contains(file) }
  }
}

object SparseUtils extends SparseUtils
