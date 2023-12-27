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
package optimus.buildtool.builders.postbuilders.installer.component

import optimus.buildtool.builders.postbuilders.installer.BundleFingerprints
import optimus.buildtool.config.OctalMode
import optimus.buildtool.files.Asset
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.PosixPermissionUtils
import optimus.buildtool.utils.TarUtils
import optimus.buildtool.utils.Utils.isWindows

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import scala.collection.immutable.SortedMap
import scala.jdk.CollectionConverters._

sealed trait CopyInstruction[AssetType <: Asset] {
  def taskId: String

  protected def sources: Set[AssetType]
  private lazy val sourcePaths: Set[String] = sources.map(_.pathString)

  def target: AssetType
  def mode: Option[OctalMode]

  protected def withMode(m: Option[OctalMode]): CopyInstruction[AssetType]

  def execute(bundleFingerprints: BundleFingerprints): Option[AssetType]

  def updateMode(): Unit = if (!isWindows) {
    mode.foreach { m =>
      val permissions = PosixPermissionUtils.fromMode(m)
      Files.setPosixFilePermissions(target.path, permissions)
    }
  }

  // added to the fingerprint to track the target mode, not just the source content
  protected val modeHash: String = mode
    .filter(_ => !isWindows) // no need to track modes under windows
    .map(m => s"/${m.mode}")
    .getOrElse("")

  def validateAndMerge(maybeOther: Option[CopyInstruction[AssetType]]): CopyInstruction[AssetType] = {
    require(maybeOther.forall(sameTarget), "Cannot merge instructions with different target locations")
    val merge: PartialFunction[Option[CopyInstruction[AssetType]], CopyInstruction[AssetType]] = {
      case other: Option[CopyInstruction[AssetType]] =>
        val otherMode = other.flatMap(_.mode)
        this.withMode(mode.orElse(otherMode))
    }
    detectConflicts.orElse(merge)(maybeOther)
  }

  protected def detectConflicts: PartialFunction[Option[CopyInstruction[AssetType]], CopyInstruction[AssetType]]

  protected def sourceConflict: PartialFunction[Option[CopyInstruction[AssetType]], CopyInstruction[AssetType]] = {
    case Some(instruction) if sourceConflicts(instruction) =>
      // detected two different source for the same target
      val errorMsg = Seq(
        s"CopyFile conflict detected: trying to copy different sources into the same target location.",
        describe(),
        instruction.describe()).mkString("\n")
      throw new IllegalStateException(errorMsg)
  }

  protected def permissionConflict: PartialFunction[Option[CopyInstruction[AssetType]], CopyInstruction[AssetType]] = {
    case Some(instruction) if modeConflicts(instruction) =>
      // detected two different permissions for the same target
      val errorMsg = Seq(
        s"CopyFile conflict detected: trying to copy the same source with two different permissions.",
        describe(),
        instruction.describe()).mkString("\n")
      throw new IllegalStateException(errorMsg)
  }

  private def sameTarget(other: CopyInstruction[AssetType]): Boolean =
    target.pathString == other.target.pathString

  private def sourceConflicts(other: CopyInstruction[AssetType]): Boolean =
    this.sourcePaths != other.sourcePaths

  private def modeConflicts(other: CopyInstruction[AssetType]): Boolean =
    (for { m <- mode; otherM <- other.mode } yield m != otherM).getOrElse(false)

  private def describe(): String = {
    val permissionsDescription = mode.fold("") { m =>
      val permissions = PosixPermissionUtils.fromMode(m).asScala
      s" with permissions ${permissions.mkString(", ")}"
    }
    s"CopyFile instruction at $taskId is requesting to copy ${sourcePaths.mkString(", ")} into ${target.pathString}$permissionsDescription"
  }

}

final case class FileCopyInstruction(
    taskId: String,
    source: FileAsset,
    sourceContent: HashedContent,
    target: FileAsset,
    mode: Option[OctalMode],
    tokenReplacements: Map[String, String])
    extends CopyInstruction[FileAsset] {

  protected val sources: Set[FileAsset] = Set(source)
  protected val detectConflicts: PartialFunction[Option[CopyInstruction[FileAsset]], CopyInstruction[FileAsset]] =
    sourceConflict.orElse(permissionConflict)

  protected def withMode(m: Option[OctalMode]): FileCopyInstruction = copy(mode = m)

  def execute(bundleFingerprints: BundleFingerprints): Option[FileAsset] = {
    bundleFingerprints.writeIfChanged(target, s"${sourceContent.hash}$modeHash") {
      if (!source.exists || tokenReplacements.nonEmpty) {
        // we need to process the content of the file
        val modifiedContent = tokenReplacements.foldLeft(sourceContent.utf8ContentAsString) {
          case (line, (token, replacement)) =>
            line.replaceAll(token, replacement)
        }
        Files.write(target.path, modifiedContent.getBytes(StandardCharsets.UTF_8))
        if (!isWindows && source.exists) {
          // OPTIMUS-38048: Files.copy keeps the mode but Files.read/Files.write obviously doesn't
          // Note that we can't do this if source.path doesn't exist (eg. in a sparse workspace)
          Files.setPosixFilePermissions(target.path, Files.getPosixFilePermissions(source.path))
        }
      } else {
        // no need to read the file, we just copy it
        Files.copy(source.path, target.path, StandardCopyOption.REPLACE_EXISTING)
      }
      updateMode()
    }
  }

}

final case class DirectoryCopyInstruction(taskId: String, source: Directory, target: Directory, mode: Option[OctalMode])
    extends CopyInstruction[Directory] {

  protected val sources: Set[Directory] = Set(source)
  // sourceConflict is acceptable for directories, since we will ensure their files do not conflict
  protected val detectConflicts: PartialFunction[Option[CopyInstruction[Directory]], CopyInstruction[Directory]] =
    permissionConflict

  protected def withMode(m: Option[OctalMode]): DirectoryCopyInstruction = copy(mode = m)

  // we do not track directories in fingerprints, only files
  override def execute(ignored: BundleFingerprints): Option[Directory] =
    if (!target.exists) {
      Files.createDirectories(target.path)
      updateMode()
      Some(target)
    } else None
}

final case class CompressedCopyInstruction(
    taskId: String,
    assetsDir: Directory,
    sourceContents: SortedMap[FileAsset, HashedContent],
    target: FileAsset,
    mode: Option[OctalMode],
    tokenReplacements: Map[String, String])
    extends CopyInstruction[FileAsset] {

  override protected def sources: Set[FileAsset] = sourceContents.keySet

  protected val detectConflicts: PartialFunction[Option[CopyInstruction[FileAsset]], CopyInstruction[FileAsset]] =
    sourceConflict.orElse(permissionConflict)

  protected def withMode(m: Option[OctalMode]): CompressedCopyInstruction = copy(mode = m)

  def execute(bundleFingerprints: BundleFingerprints): Option[FileAsset] = {
    val hashesPerFile = sourceContents.map { case (source, content) =>
      assetsDir.relativize(source) -> s"${content.hash}$modeHash"
    }
    bundleFingerprints.writeIfAnyChanged(target, hashesPerFile) {
      TarUtils.populateTarGz(target, assetsDir, sourceContents, tokenReplacements)
      updateMode()
    }
  }

}
