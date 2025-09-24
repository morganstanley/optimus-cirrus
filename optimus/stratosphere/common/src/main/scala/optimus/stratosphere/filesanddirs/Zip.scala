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

import java.io.FileOutputStream
import java.io.IOException
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes
import java.util.zip.ZipEntry
import java.util.zip.ZipFile
import java.util.zip.ZipOutputStream
import scala.io.Codec
import scala.io.Source

object Zip {

  def pack(folder: Path, zipFilePath: Path): Unit = {
    var zos: ZipOutputStream = null
    try {
      zos = new ZipOutputStream(new FileOutputStream(zipFilePath.toFile))
      Files.walkFileTree(
        folder,
        new SimpleFileVisitor[Path]() {
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            zos.putNextEntry(new ZipEntry(folder.relativize(file).toString))
            try {
              Files.copy(file, zos)
            } catch {
              case ioe: IOException =>
                println(s"Failed to copy $file due to: $ioe")
            }
            zos.closeEntry()
            FileVisitResult.CONTINUE
          }
        }
      )
    } finally {
      if (zos != null) zos.close()
    }
  }

  def pack(files: Seq[(Path, String)], zipFilePath: Path): Unit = {
    var zos: ZipOutputStream = null
    try {
      zos = new ZipOutputStream(new FileOutputStream(zipFilePath.toFile))
      files.foreach { case (file, location) =>
        zos.putNextEntry(new ZipEntry(location))
        Files.copy(file, zos)
        zos.closeEntry()
      }
    } finally {
      if (zos != null) zos.close()
    }
  }

  implicit class ZipOps(val zipFile: ZipFile) extends AnyVal {
    def readEntry(entryName: String): Seq[String] = {
      val infoTxt = zipFile.getEntry(entryName)
      val source = Source.fromInputStream(zipFile.getInputStream(infoTxt))(Codec.UTF8)
      try source.getLines().toList
      finally source.close()
    }
  }

}
