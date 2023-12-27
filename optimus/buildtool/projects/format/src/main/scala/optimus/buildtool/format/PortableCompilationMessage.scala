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
package optimus.buildtool.format

import java.io.BufferedInputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes
import java.util.zip.GZIPInputStream

trait PortableCompilationMessage {
  def filePath: Option[String]
  def start: Int
  def end: Int
  def severity: String
  def text: String
}

/**
 * This object encapsulates the obt compilation message artifact format, so messages can be read without the full
 * footprint of obt.
 */
object PortableCompilationMessage {

  // The classes we actually deserialize
  private final case class MessagePosition(
      filepath: String,
      startLine: Int,
      startColumn: Int,
      endLine: Int,
      endColumn: Int,
      startPoint: Int,
      endPoint: Int)

  private final case class CompilationMessage(
      pos: Option[MessagePosition],
      msg: String,
      severity: String,
      alarmId: Option[String])
      extends PortableCompilationMessage {
    def filePath: Option[String] = pos.map(_.filepath)
    def start = pos.fold(-1)(_.startPoint)
    def end = pos.fold(-1)(_.endPoint)
    private def lc = pos.fold("")(p => s" (${p.startLine}, ${p.startColumn})")
    def text = s"$severity$lc: $msg"
  }

  private final case class MessageFile(messages: Seq[CompilationMessage])

  private var messageMapTime = 0L
  private var prefixToMessagePath: Map[String, Set[Path]] = Map.empty

  // Not sure if there's anything better than this:
  private def logToStrato(s: String): Unit = println(s)

  /**
   * Read messages from a gzipped json file.
   * @param path:
   *   Path to the file
   * @return
   *   The compilation message, assuming there is no error.
   */
  def readMessageFile(path: Path): Seq[PortableCompilationMessage] = {
    var zIn: GZIPInputStream = null
    try {
      val in = Files.newInputStream(path)
      zIn = new GZIPInputStream(new BufferedInputStream(in))
      JsonSupport.readValue[MessageFile](zIn).messages
    } catch {
      case t: Throwable =>
        logToStrato(s"Failed to read from $path ${t.getStackTrace.mkString(";")}")
        Seq.empty
    } finally {
      if (zIn ne null) zIn.close()
    }
  }

  /**
   * Obtain a map of path prefixes (e.g. optimus/stratosphere/bootstrap or optimus/platform/projects/platform-test) to a
   * set of paths of actual files containing messages. This gets recreated every time the message-mapping file changes.
   */
  def getMessageFilePaths(wsPath: Path, prefix: String): Option[(Long, Set[Path])] = {
    val mappingPath = wsPath.resolve("build_obt").resolve("message-mapping.json")
    try {
      val t =
        if (!Files.exists(mappingPath)) 0L
        else
          Files.readAttributes(mappingPath, classOf[BasicFileAttributes]).lastModifiedTime().toMillis
      if (t > messageMapTime) {
        messageMapTime = t
        val data = JsonSupport.readValue[Map[String, Seq[String]]](mappingPath)
        prefixToMessagePath = data.map { case (prefix, pathStrings) =>
          prefix -> pathStrings.map(Paths.get(_)).toSet
        }
      }
    } catch {
      case t: Throwable =>
        logToStrato(s"Failed to read from $mappingPath ${t.getStackTrace.mkString(";")}")
    }
    prefixToMessagePath.get(prefix).map((messageMapTime, _))
  }
}
