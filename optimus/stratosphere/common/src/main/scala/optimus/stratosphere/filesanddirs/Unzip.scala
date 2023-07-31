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

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.util.zip.GZIPInputStream
import java.util.zip.ZipInputStream

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream

object Unzip {
  private val BufferSize = 8192

  def extract(fromFile: File, toDirectory: File): Unit = {
    val from = {
      val inputStream = new FileInputStream(fromFile)
      if (fromFile.getName.endsWith(".tar.gz")) new TarArchiveInputStream(new GZIPInputStream(inputStream))
      else new ZipInputStream(inputStream)
    }

    @scala.annotation.tailrec
    def nextZip(from: ZipInputStream): Unit = {
      val entry = from.getNextEntry
      if (entry != null) {
        createFileOrDirectory(from, entry.getName, entry.isDirectory)
        from.closeEntry()
        nextZip(from)
      }
    }

    @scala.annotation.tailrec
    def nextTarGz(from: TarArchiveInputStream): Unit = {
      val entry = from.getNextEntry
      if (entry != null) {
        createFileOrDirectory(from, entry.getName, entry.isDirectory)
        nextTarGz(from)
      }
    }

    def createFileOrDirectory(from: InputStream, entryName: String, isDirectory: Boolean) = {
      val target = new File(toDirectory, entryName)
      if (isDirectory)
        target.mkdirs()
      else {
        target.getParentFile.mkdirs()
        transfer(from, new FileOutputStream(target, false))
      }
    }

    def runAndClose[T <: InputStream](stream: T, f: T => Unit): Unit =
      try f(stream)
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
