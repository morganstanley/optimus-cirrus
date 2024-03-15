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
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermission
import java.util.zip.GZIPInputStream
import java.util.zip.ZipInputStream
import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq

object Unzip {
  private val BufferSize = 8192

  def extract(fromFile: File, toDirectory: File): Seq[Path] = {
    val from = {
      val inputStream = new FileInputStream(fromFile)
      if (fromFile.getName.endsWith(".tar.gz")) new TarArchiveInputStream(new GZIPInputStream(inputStream))
      else new ZipInputStream(inputStream)
    }

    @scala.annotation.tailrec
    def nextZip(from: ZipInputStream, acc: Seq[Path] = Seq.empty[Path]): Seq[Path] = {
      val entry = from.getNextEntry
      if (entry != null) {
        val file = createFileOrDirectory(from, entry.getName, entry.isDirectory)
        from.closeEntry()

        val newAcc = if (file.isDirectory) acc else acc :+ file.toPath
        nextZip(from, newAcc)
      } else acc
    }

    @scala.annotation.tailrec
    def nextTarGz(from: TarArchiveInputStream, acc: Seq[Path] = Seq.empty[Path]): Seq[Path] = {
      val entry = from.getNextTarEntry
      if (entry != null) {
        val file = createFileOrDirectory(from, entry.getName, entry.isDirectory)
        if (OsSpecific.isLinux) {
          val fileMode = Integer.toOctalString(entry.getMode).takeRight(3)
          Files.setPosixFilePermissions(file.toPath, PosixUtils.modeToPosix(fileMode).asJava)
        }

        val newAcc = if (file.isDirectory) acc else acc :+ file.toPath
        nextTarGz(from, newAcc)
      } else acc
    }

    def createFileOrDirectory(from: InputStream, entryName: String, isDirectory: Boolean): File = {
      val target = new File(toDirectory, entryName)
      if (isDirectory)
        target.mkdirs()
      else {
        target.getParentFile.mkdirs()
        transfer(from, new FileOutputStream(target, false))
      }
      target
    }

    def runAndClose[T <: InputStream](stream: T, f: (T, Seq[Path]) => Seq[Path]): Seq[Path] =
      try f(stream, Seq.empty[Path])
      finally stream.close()

    val inputStream = new FileInputStream(fromFile)
    if (fromFile.getName.endsWith(".tar.gz")) {
      runAndClose(new TarArchiveInputStream(new GZIPInputStream(inputStream)), nextTarGz)
    } else {
      runAndClose(new ZipInputStream(inputStream), nextZip)
    }
  }

  private def transfer(in: InputStream, out: OutputStream): Unit = {
    val buffer = new Array[Byte](BufferSize)

    @scala.annotation.tailrec
    def read(): Unit = {
      val byteCount = in.read(buffer)
      if (byteCount >= 0) {
        out.write(buffer, 0, byteCount)
        read()
      }
    }
    read()
    out.close()
  }
}

object PosixUtils {
  lazy val modeToPosix: Map[String, Set[PosixFilePermission]] = {
    val allPermissions = PosixFilePermission.values()
    val allPermutations = allPermissions.toSet.subsets()
    allPermutations.map { permutation =>
      var mode = 0
      allPermissions.foreach { action =>
        mode = mode << 1
        mode += (if (permutation.contains(action)) 1 else 0)
      }
      Integer.toOctalString(mode) -> permutation
    }.toMap
  }
}
