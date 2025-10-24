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

import com.opencsv.CSVParserBuilder
import com.opencsv.CSVReaderBuilder
import com.typesafe.config.Config
import com.typesafe.config.ConfigRenderOptions
import optimus.stratosphere.bootstrap.StratosphereException
import optimus.stratosphere.utils.Text._
import optimus.utils.MemUnit

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.nio.file.FileAlreadyExistsException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.nio.file.WatchEvent
import scala.io.Codec
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.Random

class PlainFileOpts private[filesanddirs] (path: Path) extends PathsOpts(path: Path) {
  import PathsOpts._

  if (!isFile) throw new StratosphereException(s"${path.toAbsolutePath} exists, but not a file")

  def getFilenameNoExt: String = {
    def getBaseName(name: String): String =
      if (name.startsWith(".")) s".${getBaseName(name.drop(1))}" else name.split("\\.").dropRight(1).mkString(".")
    getBaseName(path.getFileName.toString)
  }

  def copyTo(destinationFile: Path): Path = try {
    destinationFile.getParent.dir.create()
    Files.copy(path, destinationFile, StandardCopyOption.REPLACE_EXISTING)
  } catch {
    case ex: IOException => if (!destinationFile.exists()) throw ex else destinationFile
    case ex: Exception   => throw ex
  }

  def copyToDir(destinationDir: Path): Path = copyTo(destinationDir.resolve(path.getFileName))

  def ifExistsCopyToDir(destinationDir: Path): Unit = if (exists()) copyTo(destinationDir.resolve(path.name))

  def create(): Path = {
    if (!exists()) {
      try {
        if (path.getParent != null) Files.createDirectories(path.getParent)
        Files.createFile(path)
      } catch {
        case _: FileAlreadyExistsException => // do nothing
        case e: IOException => throw new StratosphereException(s"Can't create: ${path.toAbsolutePath} due to: $e")
      }
    }
    path
  }

  def append(string: String): Unit = writeLock(append = true) { c =>
    c.write(buffer(string.getBytes(StandardCharsets.UTF_8)))
  }

  def makeExecutable(): Unit = path.toFile.setExecutable(true)

  // We don't want to synchronize between different files, so we don't want to use global object for synchronization.
  // Calling `String.intern` on a path will guarantee that strings with the same content will be the same object.
  private def fileSynchronizationObject() = path.toAbsolutePath.toString.intern()

  def readLock[A](f: => A): A = {
    create()
    val channel = FileChannel.open(path, StandardOpenOption.READ)
    fileSynchronizationObject().synchronized {
      try {
        channel.lock(0, Long.MaxValue, true)
        f
      } finally channel.close()
    }
  }

  def writeLock[A](append: Boolean = false)(f: FileChannel => A): A = {
    create()
    val writeOpt = if (append) StandardOpenOption.APPEND else StandardOpenOption.TRUNCATE_EXISTING
    val channel = FileChannel.open(path, StandardOpenOption.WRITE, writeOpt)
    fileSynchronizationObject().synchronized {
      try {
        channel.lock()
        f(channel)
      } finally channel.close()
    }
  }

  private def buffer(bytes: Array[Byte]): ByteBuffer = ByteBuffer.wrap(bytes)

  def write(content: String): Path =
    write(content, StandardCharsets.UTF_8)

  def write(content: String, charset: Charset): Path = writeLock() { c =>
    c.write(buffer(content.getBytes(charset)))
    path
  }

  def write(data: Array[Byte]): Path = writeLock() { c =>
    c.write(buffer(data))
    path
  }

  def writeConfig(config: Config): Unit =
    write(config.root.render(ConfigRenderOptions.concise.setFormatted(true)))

  def write(lines: Seq[String]): Path =
    write(lines.mkString(System.lineSeparator))

  def writeRandomPayload(sizeInKB: Int): Unit = writeLock() { c =>
    val array = Array.ofDim[Byte](MemUnit.KB.bytes.toInt)
    val writer = Files.newOutputStream(path)
    def writeRandomKB(): Unit = {
      Random.nextBytes(array)
      c.write(buffer(array))
    }
    try 0.to(sizeInKB).foreach(_ => writeRandomKB())
    finally writer.close()
  }

  /** Creates a file from a path to resource. If such resource does not exist, falls back to file. */
  def createFromResourceOrFile(path: String, replacements: Map[String, String] = Map.empty): Path = {
    val is = ResourceUtils.fromResourceOrFile(path)
    val source = Source.fromInputStream(is)(Codec.UTF8)
    try {
      write(source.getLines().toList.map(bind(_, replacements)))
    } finally {
      source.close()
    }
  }

  def withFileOutputStream[A](code: OutputStream => A): A = {
    val fos = Files.newOutputStream(path)
    try code(fos)
    finally fos.close()
  }

  def withFileInputStream[A](code: InputStream => A): A = {
    val fis = Files.newInputStream(path)
    try code(fis)
    finally fis.close()
  }

  def getContent(): String = readLock { Files.readString(path, StandardCharsets.UTF_8) }

  def getByteContent(): Array[Byte] = readLock { Files.readAllBytes(path) }

  def content(): Option[String] = if (exists()) Some(getContent()) else None

  def contentLines(): List[String] = readLock { Files.readAllLines(path, StandardCharsets.UTF_8).asScala.toList }

  def contentAsCsv(separator: Char = ','): Seq[Seq[String]] =
    new CSVReaderBuilder(Files.newBufferedReader(path))
      .withCSVParser(new CSVParserBuilder().withSeparator(separator).build())
      .build()
      .readAll()
      .asScala
      .toList
      .map(_.toList)

  def changeContent(f: String => String): Option[Path] = content().map(f(_)).map(write(_))

  def watch(events: WatchEvent.Kind[_]*)(body: => Unit): Thread = {
    super.watch(events: _*) { modifiedFiles =>
      val hasFileChanged = modifiedFiles.exists(_.name == path.name)
      if (hasFileChanged) body
    }
  }
}
