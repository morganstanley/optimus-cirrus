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
package optimus.stratosphere.filesanddirs

import optimus.stratosphere.bootstrap.OsSpecific
import optimus.stratosphere.bootstrap.StratosphereException
import optimus.stratosphere.config.AutoDetect
import optimus.stratosphere.config.StratoWorkspace
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.TrueFileFilter

import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.file._
import java.nio.file.attribute._
import java.util
import scala.jdk.CollectionConverters._
import scala.util.Random
import scala.util.control.NonFatal

class DirectoryOpts(path: Path) extends PathsOpts(path: Path) {
  import PathsOpts._

  if (!isDir) throw new StratosphereException(s"${path.toAbsolutePath} exists, but is not a directory")

  /** Removes all files that match `nameMatcher`. */
  class FileRemovingVisitor(nameMatcher: String => Boolean) extends SimpleFileVisitor[Path] {

    private lazy val log = StratoWorkspace(AutoDetect).log

    override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
      if (nameMatcher(file.toFile.getName)) {
        try Files.delete(file)
        catch {
          case io: IOException => log.error(s"Could not delete $file", io)
        }
      }
      FileVisitResult.CONTINUE
    }

    override def visitFileFailed(file: Path, exc: IOException): FileVisitResult = {
      log.error(s"Could not visit $file", exc)
      FileVisitResult.CONTINUE
    }

  }

  def create(): Path = {
    if (!exists()) {
      try {
        Files.createDirectories(path)
      } catch {
        case e: IOException =>
          throw new StratosphereException(s"Can't create: ${path.toAbsolutePath} due to $e")
      }
    }
    path
  }

  def randomDirectory(): DirectoryOpts = {
    val randomName = Seq.fill(10)(Random.nextInt(10)).mkString
    new DirectoryOpts(path.resolve(randomName))
  }

  /**
   * Creates a directory from a resource. Falls back to files if resource is not found. Requires a file index due to how
   * resources works - one cannot list them.
   */
  def createFromResourceOrFile(resourceOrFilePrefix: String, fileIndex: Seq[String]): Unit = {
    create()
    fileIndex.foreach { filePath =>
      path.resolve(filePath).file.createFromResourceOrFile(s"$resourceOrFilePrefix/$filePath")
    }
  }

  def listPathsRecursively(): Seq[Path] =
    if (exists())
      FileUtils.listFiles(path.toFile, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).asScala.map(_.toPath).toList
    else
      throw new FileNotFoundException(s"Cannot list paths under: '$path' because it does not exist.")

  private def safeListFiles() = Option(path.toFile.listFiles()).toList.flatten

  def listDirs(): Seq[Path] = safeListFiles().filter(_.isDirectory).map(_.toPath)

  def listFiles(): Seq[Path] = safeListFiles().filterNot(_.isDirectory).map(_.toPath)

  def list(): Seq[Path] = safeListFiles().map(_.toPath)

  def clean(): Unit = if (exists()) safeListFiles().foreach(FileUtils.deleteQuietly)

  def deleteFiles(nameMatcher: String => Boolean): Unit = {
    Files.walkFileTree(path, new FileRemovingVisitor(nameMatcher))
  }

  def copyToDir(destDir: Path, shouldBeCopied: (File, BasicFileAttributes) => Boolean = (_, _) => true): Path =
    copyTo(destDir.resolve(path.getFileName), shouldBeCopied)

  /** We had to implement our own copy method because Apache version does not preserve file attributes */
  def copyTo(
      target: Path,
      shouldFileBeCopied: (File, BasicFileAttributes) => Boolean = (_, _) => true,
      shouldDirBeCopied: (File, BasicFileAttributes) => Boolean = (_, _) => true
  ): Path = {
    Files
      .walkFileTree(
        path,
        util.EnumSet.of(FileVisitOption.FOLLOW_LINKS),
        Integer.MAX_VALUE,
        new FileVisitor[Path]() {

          override def preVisitDirectory(dir: Path, sourceBasic: BasicFileAttributes): FileVisitResult = {
            if (shouldDirBeCopied(dir.toFile, sourceBasic)) {
              val targetDir = Files.createDirectories(target.resolve(path.relativize(dir)))

              if (OsSpecific.isWindows) {
                val dosAttrs = Files.getFileAttributeView(dir, classOf[DosFileAttributeView])
                if (dosAttrs != null) {
                  val sourceDosAttrs = dosAttrs.readAttributes()
                  val targetDosAttrs = Files.getFileAttributeView(targetDir, classOf[DosFileAttributeView])
                  targetDosAttrs.setArchive(sourceDosAttrs.isArchive)
                  targetDosAttrs.setHidden(sourceDosAttrs.isHidden)
                  targetDosAttrs.setReadOnly(sourceDosAttrs.isReadOnly)
                  targetDosAttrs.setSystem(sourceDosAttrs.isSystem)
                }
              } else {
                val acl = Files.getFileAttributeView(dir, classOf[AclFileAttributeView])
                if (acl != null) Files.getFileAttributeView(targetDir, classOf[AclFileAttributeView]).setAcl(acl.getAcl)

                val posixAttrs = Files.getFileAttributeView(dir, classOf[PosixFileAttributeView])
                if (posixAttrs != null) {
                  val sourcePosix = posixAttrs.readAttributes()
                  val targetPosix = Files.getFileAttributeView(targetDir, classOf[PosixFileAttributeView])
                  targetPosix.setPermissions(sourcePosix.permissions())
                }

                val userAttrs = Files.getFileAttributeView(dir, classOf[UserDefinedFileAttributeView])
                if (userAttrs != null) {
                  val targetUser = Files.getFileAttributeView(targetDir, classOf[UserDefinedFileAttributeView])
                  userAttrs.list().asScala.foreach { key: String =>
                    val buffer = ByteBuffer.allocate(userAttrs.size(key))
                    userAttrs.read(key, buffer)
                    buffer.flip()
                    targetUser.write(key, buffer)
                  }
                }
              }

              FileVisitResult.CONTINUE
            } else {
              FileVisitResult.SKIP_SUBTREE
            }
          }

          override def visitFileFailed(file: Path, e: IOException): FileVisitResult = throw e

          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            if (shouldFileBeCopied(file.toFile, attrs))
              Files.copy(
                file,
                target.resolve(path.relativize(file)),
                StandardCopyOption.COPY_ATTRIBUTES,
                StandardCopyOption.REPLACE_EXISTING
              )
            FileVisitResult.CONTINUE
          }

          override def postVisitDirectory(dir: Path, e: IOException): FileVisitResult = {
            if (e != null) throw e
            FileVisitResult.CONTINUE
          }
        }
      )
  }

  /** Delete all files from this dir *but* the N last ones by creation date. */
  def rotate(keep: Int): Unit = list()
    .sortBy(_.creationTime)(Ordering[Long].reverse) // newest first
    .drop(keep)
    .foreach(_.deleteIfExists())

  def printAllFiles(logInfo: String => Unit, logError: (String, Throwable) => Unit): Unit =
    listPathsRecursively().foreach { logFile =>
      try {
        // This file can be really big on failure
        if (Files.isRegularFile(logFile))
          logInfo(s"Content of $logFile:\n${logFile.file.contentLines().mkString("\t", "\n\t", "\n")}")
      } catch {
        case NonFatal(e) =>
          logError(s"Error reading file $logFile", e)
      }
    }

  def newestFile(): Option[Path] = listFiles().sortBy(_.lastModifiedTime)(Ordering[Long]).lastOption

  def newestDir(): Option[Path] = listDirs().sortBy(_.lastModifiedTime)(Ordering[Long]).lastOption

  def moveContentTo(toDir: Path): Unit = listFiles().map(_.file).foreach(_.moveTo(toDir))

}
