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

import com.google.common.io.CountingInputStream
import com.opencsv.CSVParserBuilder
import com.opencsv.CSVReader
import com.opencsv.CSVReaderBuilder
import com.opencsv.CSVWriterBuilder
import com.opencsv.ICSVParser
import com.opencsv.ICSVWriter
import optimus.logging.LoggingInfo
import optimus.platform.util.Log
import org.apache.commons.io.output.CountingOutputStream
import com.github.luben.zstd.ZstdOutputStream
import com.github.luben.zstd.ZstdInputStream
import com.github.luben.zstd.RecyclingBufferPool
import com.github.luben.zstd.Zstd

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.BufferedReader
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.io.Reader
import java.net.URL
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import java.util.zip.ZipFile
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.Using
import scala.util.control.NonFatal

object FileUtils extends Log {

  def ensureInitialized(): Unit = {}

  def getCSVReader(filename: String, separator: Char): CSVReader = {
    new CSVReaderBuilder(new BufferedReader(new InputStreamReader(new FileInputStream(filename))))
      .withCSVParser(new CSVParserBuilder().withSeparator(separator).build)
      .build
  }

  def getCSVReader(reader: Reader, separator: Char): CSVReader = {
    val csvParser = new CSVParserBuilder()
      .withSeparator(separator)
      .withQuoteChar(ICSVParser.DEFAULT_QUOTE_CHARACTER)
      .withEscapeChar(ICSVParser.DEFAULT_ESCAPE_CHARACTER)
      .withErrorLocale(Locale.getDefault())
      .withStrictQuotes(ICSVParser.DEFAULT_STRICT_QUOTES)
      .withIgnoreLeadingWhiteSpace(ICSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE)
      .withIgnoreQuotations(ICSVParser.DEFAULT_IGNORE_QUOTATIONS)
      .withFieldAsNull(ICSVParser.DEFAULT_NULL_FIELD_INDICATOR)
      .build

    new CSVReaderBuilder(reader)
      .withCSVParser(csvParser)
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
   *   path to file under resources/ dir.
   * @param fileName
   *   file name (no path) with extension
   * @param tmpPrefix
   *   prefix for tmp file created
   * @return
   *   absolute path as string
   */
  def getResourcePath(basePath: String, fileName: String, tmpPrefix: String = ""): String = {
    val name = if (basePath.isEmpty) fileName else s"$basePath/$fileName"
    var resourceStream: InputStream = null
    try {
      // For the difference between getClass.getResourceAsStream & getClass.getClassLoader.getResourceAsStream. See
      // https://stackoverflow.com/questions/14739550
      val resourceStream = getClass.getClassLoader.getResourceAsStream(name)
      assert(resourceStream ne null)
      val path = Files.createTempFile(tmpPrefix, fileName)
      Files.copy(resourceStream, path, StandardCopyOption.REPLACE_EXISTING)
      path.toString
    } finally if (resourceStream ne null) resourceStream.close()
  }

  final val diagnosticDumpDirName: String = PropertyUtils.get("optimus.diagnostic.dump.dir") orElse
    Option(System.getenv("DIAGNOSTIC_DUMP_DIR")) getOrElse System.getProperty("java.io.tmpdir")

  lazy val diagnosticDumpDir = Files.createDirectories(Paths.get(diagnosticDumpDirName))

  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-DD_HHmmss").withZone(ZoneId.systemDefault)

  def tmpPath(prefix: String): Path = {
    tmpPathWithSuffixOption(prefix, None)
  }

  def tmpPath(prefix: String, suffix: String): Path = {
    tmpPathWithSuffixOption(prefix, Some(suffix))
  }

  def tmpPathWithSuffixOption(prefix: String, suffix: Option[String]): Path = {
    val safeTime = formatter.format(Instant.now)
    val safePath = {
      val p = s"$prefix-${LoggingInfo.getHost}-${LoggingInfo.pid}-$safeTime"
      suffix match {
        case Some(s) => p + s".$s"
        case None    => p
      }
    }
    diagnosticDumpDir.resolve(safePath)
  }

  def deleteRecursively(path: Path): Unit =
    if (Files.exists(path)) try {
      log.info(s"Deleting temporary $path")
      Files
        .walk(path)
        .iterator()
        .asScala
        .toSeq
        .reverse
        .foreach(Files.deleteIfExists)
    } catch {
      case NonFatal(e) => log.warn(s"Failed to delete $path", e)
    }

  private val toDelete = mutable.HashSet.empty[Path]

  def deleteOnExit(path: Path): Path = synchronized {
    toDelete += path
    path
  }

  sys.addShutdownHook {
    val dirs = this.synchronized(toDelete.toList)
    dirs.foreach(deleteRecursively)
  }

  def gzip(
      from: Path,
      to: Path,
      removeOld: Boolean = true,
      gzipBuffer: Int = 65536,
      fileBuffer: Int = 524288): Try[(Long, Long)] = for {
    ret <- Using.Manager { use =>
      Files.deleteIfExists(to)
      val in = use(new BufferedInputStream(Files.newInputStream(from), fileBuffer))
      val counting = new CountingOutputStream(new BufferedOutputStream(Files.newOutputStream(to), fileBuffer))
      val out = use(new GZIPOutputStream(counting, gzipBuffer))
      val orig = in.transferTo(out)
      val zipped = counting.getByteCount
      (orig, zipped)
    }
    _ <- Try { if (removeOld) Files.delete(from) }
  } yield ret

  def gunzip(
      from: Path,
      to: Path,
      removeOld: Boolean = true,
      gzipBuffer: Int = 65536,
      fileBuffer: Int = 524288): Try[(Long, Long)] = for {
    ret <- Using.Manager { use =>
      Files.deleteIfExists(to)
      val count = new CountingInputStream(new BufferedInputStream(Files.newInputStream(from), fileBuffer))
      val in = use(new GZIPInputStream(count, gzipBuffer))
      val out = use(new BufferedOutputStream(Files.newOutputStream(to), fileBuffer))
      val unzipped = in.transferTo(out)
      val zipped = count.getCount
      (unzipped, zipped)
    }
    _ <- Try { if (removeOld) Files.delete(from) }

  } yield ret

  def zstdCompress(
      from: Path,
      to: Path,
      removeOld: Boolean = true,
      compressionLevel: Int = Zstd.defaultCompressionLevel(),
      fileBuffer: Int = 524288): Try[(Long, Long)] = for {
    ret <- Using.Manager { use =>
      Files.deleteIfExists(to)
      val in = use(new BufferedInputStream(Files.newInputStream(from), fileBuffer))
      val counting = new CountingOutputStream(new BufferedOutputStream(Files.newOutputStream(to), fileBuffer))
      val out = use(new ZstdOutputStream(counting, RecyclingBufferPool.INSTANCE, compressionLevel))
      val orig = in.transferTo(out)
      val compressed = counting.getByteCount
      (orig, compressed)
    }
    _ <- Try { if (removeOld) Files.delete(from) }
  } yield ret

  def zstdDecompress(from: Path, to: Path, removeOld: Boolean = true, fileBuffer: Int = 524288): Try[(Long, Long)] =
    for {
      ret <- Using.Manager { use =>
        Files.deleteIfExists(to)
        val count = new CountingInputStream(new BufferedInputStream(Files.newInputStream(from), fileBuffer))
        val in = use(new ZstdInputStream(count, RecyclingBufferPool.INSTANCE))
        val out = use(new BufferedOutputStream(Files.newOutputStream(to), fileBuffer))
        val decompressed = in.transferTo(out)
        val compressed = count.getCount
        (decompressed, compressed)
      }
      _ <- Try { if (removeOld) Files.delete(from) }
    } yield ret

}
