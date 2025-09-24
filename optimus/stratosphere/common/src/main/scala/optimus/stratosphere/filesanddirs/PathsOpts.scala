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

import optimus.stratosphere.bootstrap.StratosphereException
import optimus.stratosphere.common.PlatformSpecific
import optimus.stratosphere.config.AutoDetect
import optimus.stratosphere.config.StratoWorkspace
import org.apache.commons.io.FileUtils

import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.WatchEvent
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime
import java.time.ZonedDateTime
import java.time.ZoneId
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

object PathsOpts {
  implicit def pathToPathsOpts(path: Path): PathsOpts = new PathsOpts(path)
}

/**
 * Base class of directory and file
 */
class PathsOpts protected (val path: Path) {
  def getFullPath: String = PlatformSpecific.fullPath(path).toString

  def exists(): Boolean = Files.exists(path)

  def existsOption(): Option[Path] = Option(path).filter(Files.exists(_))

  def delete(): Boolean = FileUtils.deleteQuietly(path.toFile)

  private def creationFileTime: FileTime = Files.readAttributes(path, classOf[BasicFileAttributes]).creationTime()

  def creationTime: Long = creationFileTime.toMillis

  def creationDateTime: ZonedDateTime = ZonedDateTime.ofInstant(creationFileTime.toInstant, ZoneId.systemDefault())

  private def lastModifiedFileTime: FileTime =
    Files.readAttributes(path, classOf[BasicFileAttributes]).lastModifiedTime()

  def lastModifiedTime: Long = lastModifiedFileTime.toMillis

  def lastModifiedDateTime: ZonedDateTime =
    ZonedDateTime.ofInstant(lastModifiedFileTime.toInstant, ZoneId.systemDefault())

  def deleteIfExists(): Unit = {
    def deleteIfNeeded() = exists() && (!delete() || exists())
    if (deleteIfNeeded() || deleteIfNeeded()) {
      throw new StratosphereException(
        s"""Cannot delete directory ($getFullPath).
           |Please turn off all applications using files in this directory and try again""".stripMargin)
    }
  }

  def rename(dstName: String): Path = Files.move(path, path.resolveSibling(dstName), StandardCopyOption.ATOMIC_MOVE)

  def moveTo(destDir: Path): Path = Files.move(path, destDir.resolve(path.getFileName), StandardCopyOption.ATOMIC_MOVE)

  def move(destFile: Path): Path = {
    new DirectoryOpts(destFile.getParent).create()
    Files.move(path, destFile, StandardCopyOption.ATOMIC_MOVE)
  }

  def moveWithRename(destDir: Path, newName: String): Path =
    Files.move(path, destDir.resolve(newName), StandardCopyOption.ATOMIC_MOVE)

  def name: String = path.getFileName.toString

  def isDir: Boolean =
    !exists() || Files.isDirectory(path) || FileUtils.isSymlink(path.toFile) || path.toString.startsWith("""\\""")

  def isFile: Boolean = !exists() || Files.isRegularFile(path)

  override def toString: String = getFullPath

  def file: PlainFileOpts = new PlainFileOpts(path)

  def dir: DirectoryOpts = new DirectoryOpts(path)

  def resolve(subPath: Seq[String]): Path = subPath.foldLeft(path) { case (path, dir) =>
    path.resolve(dir)
  }

  def setReadable(readable: Boolean, ownerOnly: Boolean = false) =
    path.toFile.setReadable( /* readable = */ readable, /* ownerOnly = */ ownerOnly)

  def watch(events: WatchEvent.Kind[_]*)(body: Seq[Path] => Unit): Thread = {
    val watchService = FileSystems.getDefault.newWatchService()
    path.getParent.register(watchService, events: _*)

    val thread = new Thread(() => {
      while (true) {
        val key = watchService.take()
        val modifiedFiles = key.pollEvents().asScalaUnsafeImmutable.map(_.asInstanceOf[WatchEvent[Path]].context())
        try {
          body(modifiedFiles)
        } catch {
          case NonFatal(e) =>
            StratoWorkspace(AutoDetect).log.error(s"Watcher error for path '${path.toString}'", e)
        }

        key.reset()
      }
    }) {
      override def interrupt(): Unit = {
        super.interrupt()
        watchService.close()
      }
    }

    thread.start()
    thread
  }
}
