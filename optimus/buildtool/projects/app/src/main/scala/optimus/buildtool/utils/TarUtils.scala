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
package optimus.buildtool.utils

import optimus.buildtool.builders.postinstallers.uploaders.ArchiveUtil.ArchiveEntry
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.utils.Utils.isWindows
import optimus.platform.util.Log
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import org.apache.commons.compress.archivers.tar.TarConstants
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import scala.annotation.tailrec
import scala.collection.immutable.SortedMap
import scala.util.Try
import scala.util.control.NonFatal

object TarUtils extends Log {

  private def readTarArchiveEntry(stream: TarArchiveInputStream, entry: TarArchiveEntry): String = {
    val contentArray = new Array[Byte](entry.getSize.toInt)
    stream.read(contentArray)
    new String(contentArray, "UTF-8")
  }

  def readFileInTarGz(tarGz: Path, relativePath: RelativePath): Option[String] = {
    @tailrec
    def scan(stream: TarArchiveInputStream): Option[String] = {
      val currentEntry = stream.getNextTarEntry
      currentEntry match {
        case null                                              => None
        case theOne if theOne.getName == relativePath.toString => Some(readTarArchiveEntry(stream, theOne))
        case _                                                 => scan(stream)
      }
    }
    openTarGz(tarGz)(stream => scan(stream))
  }

  def openTarGz[A](tarGz: Path)(f: TarArchiveInputStream => A): A = {
    val fs = Files.newInputStream(tarGz)
    val bss = new BufferedInputStream(fs)
    val gzipIn = new GzipCompressorInputStream(bss)
    val tarIn = new TarArchiveInputStream(gzipIn)
    val result = f(tarIn)
    tarIn.close()
    result
  }

  def populateTarGz(tarAsset: FileAsset, directory: Directory)(post: TarArchiveOutputStream => Unit): FileAsset = {
    populateTarGzStream(tarAsset) { taos =>
      directory.path.toFile.listFiles().toList.foreach { file =>
        addFileToTarGz(taos, file.toPath, None)
      }
      post(taos)
    }
  }

  def populateTarGz(
      tarAsset: FileAsset,
      files: Seq[ArchiveEntry],
      deleteOriginals: Boolean
  ): FileAsset = {
    populateTarGzStream(tarAsset) { taos =>
      files.foreach { f =>
        require(f.file.exists, s"File $f does not exist")
        def addFile(): Unit = addFileToTarGz(taos, f.file, f.targetPath)
        // We try to add the file twice before giving up
        Try(addFile()).getOrElse(addFile())
      }
    }
    if (deleteOriginals) files.foreach(f => Files.delete(f.file.path))
    tarAsset
  }

  def populateTarGz(
      tarAsset: FileAsset,
      assetsDir: Directory,
      sourceContents: SortedMap[FileAsset, HashedContent],
      tokenReplacements: Map[String, String]
  ): FileAsset = populateTarGzStream(tarAsset) { taos =>
    sourceContents.foreach { case (f, c) =>
      def addFile(): Unit = addFileToTarGz(taos, f, assetsDir.relativize(f), c, tokenReplacements)
      // We try to add the file twice before giving up
      Try(addFile()).getOrElse(addFile())
    }
  }

  private def populateTarGzStream(tarAsset: FileAsset)(f: TarArchiveOutputStream => Unit): FileAsset =
    AssetUtils.atomicallyWrite(tarAsset, replaceIfExists = true, localTemp = true) { tempFile =>
      val fos = Files.newOutputStream(tempFile)
      val bos = new BufferedOutputStream(fos)
      val gzos = new GzipCompressorOutputStream(bos)
      val taos = new TarArchiveOutputStream(gzos)
      try {
        taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU)
        f(taos)
      } catch {
        case NonFatal(ex) =>
          log.error(s"Unable to create tar archive $tarAsset", ex)
          throw ex
      } finally {
        taos.finish()
        taos.close()
      }
      tarAsset
    }

  def tarWriteFile(taos: TarArchiveOutputStream, where: String, content: String): Unit = {
    val bytes = content.getBytes(StandardCharsets.UTF_8)
    val tarEntry = new TarArchiveEntry(where)
    tarEntry.setSize(bytes.length)
    taos.putArchiveEntry(tarEntry)
    taos.write(bytes)
    taos.closeArchiveEntry()
  }

  def addFileToTarGz(taos: TarArchiveOutputStream, path: Path, base: Option[String] = None): Unit = {
    val file = path.toFile
    val entryName = base
      .map(parent => s"$parent${File.separatorChar}${file.getName}")
      .getOrElse(file.getName)

    if (Files.isSymbolicLink(path)) {
      val linkTarget = Files.readSymbolicLink(path)
      if (linkTarget.isAbsolute)
        throw new IllegalArgumentException(
          s"Failed on: $path, creating tar.gz file with absolute symlink is not allowed")

      val tarEntry = new TarArchiveEntry(entryName, TarConstants.LF_SYMLINK)
      tarEntry.setLinkName(linkTarget.toString)
      taos.putArchiveEntry(tarEntry)
      taos.closeArchiveEntry()
    } else if (file.isFile) {
      val tarEntry = new TarArchiveEntry(file, entryName)
      if (file.canExecute) makeExecutable(tarEntry)
      taos.putArchiveEntry(tarEntry)
      Files.copy(file.toPath, taos)
      taos.closeArchiveEntry()
    } else {
      val tarEntry = new TarArchiveEntry(file, entryName)
      taos.putArchiveEntry(tarEntry)
      taos.closeArchiveEntry()
      val children = file.listFiles
      if (children != null) for (child <- children) {
        addFileToTarGz(taos, child.toPath, Some(entryName))
      }
    }
  }

  private def makeExecutable(entry: TarArchiveEntry): Unit = {
    entry.setMode(entry.getMode | Integer.parseInt("100", 8))
  }

  private def addFileToTarGz(
      taos: TarArchiveOutputStream,
      asset: FileAsset,
      targetPath: RelativePath,
      content: HashedContent,
      tokenReplacements: Map[String, String]
  ): Unit = {
    if (!asset.exists || tokenReplacements.nonEmpty) {
      try {
        val entry = tarEntry(asset, targetPath)
        // we need to process the content of the file
        val originalContent = content.utf8ContentAsString
        val modifiedContentInBytes = tokenReplacements
          .foldLeft(originalContent) { case (line, (token, replacement)) =>
            line.replaceAll(token, replacement)
          }
          .getBytes(StandardCharsets.UTF_8)
        entry.setSize(modifiedContentInBytes.length)

        taos.putArchiveEntry(entry)
        taos.write(modifiedContentInBytes)
        taos.closeArchiveEntry()
      } catch {
        case NonFatal(ex) =>
          log.error(s"Unable to add the entry ${asset.pathString}", ex)
          throw ex
      }
    } else {
      // we just copy the file as is
      addFileToTarGz(taos, asset, targetPath)
    }
  }

  private def addFileToTarGz(
      taos: TarArchiveOutputStream,
      asset: FileAsset,
      targetPath: RelativePath
  ): Unit = {
    try {
      val entry = tarEntry(asset, targetPath)
      entry.setSize(Files.size(asset.path))
      taos.putArchiveEntry(entry)
      Files.copy(asset.path, taos)
      taos.closeArchiveEntry()
    } catch {
      case NonFatal(ex) =>
        log.error(s"Unable to add the entry ${asset.pathString}", ex)
        throw ex
    }
  }

  private def tarEntry(asset: FileAsset, targetPath: RelativePath): TarArchiveEntry = {
    val entry = new TarArchiveEntry(asset.path.toFile, targetPath.pathString)
    if (!isWindows && asset.exists) {
      // Note that `asset` might not exist if we're in a sparse workspace
      val octalMode = PosixPermissionUtils.toMode(Files.getPosixFilePermissions(asset.path))
      entry.setMode(octalMode.mode) // ensuring permissions are preserved
    }
    entry
  }
}
