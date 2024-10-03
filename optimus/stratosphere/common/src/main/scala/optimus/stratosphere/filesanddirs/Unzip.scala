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
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermission
import java.util.zip.GZIPInputStream
import java.util.zip.ZipInputStream
import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.collection.mutable.ListBuffer
import scala.util.Using

object Unzip {

  def extract(fromFile: File, toDirectory: File): Seq[Path] = {
    val extractedFiles: ListBuffer[Path] = ListBuffer.empty

    def extractZip(in: ZipInputStream): Unit = {
      var entry = in.getNextEntry
      while (entry != null) {
        createFileOrDirectory(in, entry.getName, entry.isDirectory)
        in.closeEntry()
        entry = in.getNextEntry
      }
    }

    def extractTar(in: TarArchiveInputStream): Unit = {
      var entry = in.getNextTarEntry
      while (entry != null) {
        val file = createFileOrDirectory(in, entry.getName, entry.isDirectory)
        if (OsSpecific.isLinux) {
          val fileMode = Integer.toOctalString(entry.getMode).takeRight(3)
          Files.setPosixFilePermissions(file.toPath, PosixUtils.modeToPosix(fileMode).asJava)
        }
        entry = in.getNextTarEntry
      }
    }

    def createFileOrDirectory(from: InputStream, entryName: String, isDirectory: Boolean): File = {
      val target = new File(toDirectory, entryName)
      if (isDirectory) target.mkdirs()
      else {
        target.getParentFile.mkdirs()
        Using.resource(new FileOutputStream(target, false))(from.transferTo(_))
        extractedFiles += target.toPath
      }
      target
    }

    val inputStream = new FileInputStream(fromFile)
    if (fromFile.getName.endsWith(".tar.gz")) {
      Using.resource(new TarArchiveInputStream(new GZIPInputStream(inputStream)))(extractTar)
    } else {
      Using.resource(new ZipInputStream(inputStream))(extractZip)
    }
    extractedFiles.toList
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
