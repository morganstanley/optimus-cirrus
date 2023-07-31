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
package optimus.utils

import com.opencsv.CSVParserBuilder
import com.opencsv.CSVReader
import com.opencsv.CSVReaderBuilder
import com.opencsv.CSVWriterBuilder
import com.opencsv.ICSVWriter
import optimus.platform.IO.usingQuietly

import java.io.BufferedReader
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.URL
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.zip.ZipFile
import scala.jdk.CollectionConverters._
import scala.util.Try

object FileUtils {

  def fileToMapTwoColumns(filename: String): Map[String, String] = {
    Try {
      val reader = getCSVReader(filename, '\t')

      usingQuietly(reader) { r =>
        r.readAll().asScala.toArray
      }.map { case Array(k, v) =>
        (k, v)
      }.toMap
    }.getOrElse(Map.empty)
  }

  def writeToFileTwoColumns(filename: String, data: Seq[Array[String]]): Unit = {
    val writer = getCSVWriter(filename, '\t')

    usingQuietly(writer) { w =>
      w.writeAll(data.asJava)
    }
  }

  def getCSVReader(filename: String, separator: Char): CSVReader = {
    new CSVReaderBuilder(new BufferedReader(new InputStreamReader(new FileInputStream(filename))))
      .withCSVParser(new CSVParserBuilder().withSeparator(separator).build)
      .build
  }

  def getCSVWriter(filename: String, separator: Char, append: Boolean = false): ICSVWriter = {
    new CSVWriterBuilder(new OutputStreamWriter(new FileOutputStream(filename, append)))
      .withSeparator(separator)
      .withEscapeChar('\\')
      .build()
  }

  // For any app/test that dynamically discovers resources within a package dynamically at runtime on the classpath
  // (e.g. docs, templates, directives, scenarios for parameterized tests)
  //
  // Use ClassLoader#getResource to locate the folder
  // Use Class#getResource and Class#getResourceAsStream to fetch the content
  //
  // It is suggested to filter by file extension and some other pattern. This function returns resources in subfolders
  // as well.
  def resourcesFromPathInJar(url: URL): Seq[String] = {
    require(Option(url).nonEmpty)
    val fileless = url.getPath.stripPrefix("file:")
    val jarFile = new ZipFile(fileless.substring(0, fileless.indexOf(jarFileSeparator)))
    try {
      val pathInJar = fileless.substring(fileless.indexOf(jarFileSeparator) + jarFileSeparator.length)
      (
        for {
          entry <- jarFile.entries().asScala
          if entry.getName.startsWith(pathInJar) && entry.getName != pathInJar // skip folder itself
          if !entry.getName.endsWith("/") // no folder
        } yield "/" + entry.getName
      ).toList

    } finally jarFile.close()
  }
  private val jarFileSeparator: String = "!/"

  /**
   * If executable and resource are in a single .jar, getting the URI and converting to a File to get the Path won't
   * work (you can only access the resource as a stream and not as a URI). See
   * https://stackoverflow.com/questions/51705451
   * @param basePath
   *   path to file under resources/ dir
   * @param fileName
   *   file name (no path) with extension
   * @param tmpPrefix
   *   prefix for tmp file created
   * @return
   *   absolute path as string
   */
  def getResourcePath(basePath: String, fileName: String, tmpPrefix: String = ""): String = {
    val name = if (basePath.isEmpty) fileName else s"$basePath/$fileName"
    val resourceStream = getClass.getClassLoader.getResourceAsStream(name)
    assert(resourceStream ne null)
    val path = Files.createTempFile(tmpPrefix, fileName)
    Files.copy(resourceStream, path, StandardCopyOption.REPLACE_EXISTING)
    path.toString
  }
}
