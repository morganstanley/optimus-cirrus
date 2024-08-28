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
package optimus.platform.relational

import optimus.graph.NodeTask
import optimus.platform.NonForwardingPluginTagKey
import org.apache.commons.lang3.exception.ExceptionUtils
import msjava.slf4jutils.scalalog._

import java.util.regex.Pattern

object CallerDetail {
  private val log = getLogger(this)

  private val separator = Pattern.quote(System.getProperty("file.separator", "/"))
  val parser: Pattern = Pattern.compile(s"source-.*(${separator})(?<file>.*?),line-(?<line>.*?),.*")
}

class CallerDetail(val linkFormatLocation: String, val fullLocation: String) {
  def asClickableSource: String =
    try parseLocation
    catch {
      case t: Throwable =>
        CallerDetail.log.debug("Couldn't parse CallerDetail location", t)
        fullLocation
    }

  // Because group matcher with the following patter did not work : "source-(?<Name>.*),line-(?<Line>[0-9_]+),"
  private def parseLocation: String = {
    val indexOfLine = fullLocation.indexOf("line-")
    val sourceOffset = 7 // cut out "source-"
    val ref = fullLocation.substring(sourceOffset, indexOfLine - 1) // cut out "," and get full path of file
    val line = fullLocation.substring(indexOfLine)
    val endLine = line.indexOf(",")
    val lineNum = line.substring(5, endLine) // cut out "line-" and get line number
    val lastSlash = ref.lastIndexOf("/")
    val prefix = ref.substring(0, lastSlash).replace("/", ".")
    val suffix = ref.substring(lastSlash + 1) // cut out "/" and get the file name only
    prefix + ".(" + suffix + ":" + lineNum + ")" // put together path and file name in a format Intellij understands
  }
}

class ExtraCallerDetail(val node: NodeTask) {
  def generateTraces(): String = {
    val nodeTrace = node.waitersToFullMultilineNodeStack()
    val stackTrace = ExceptionUtils.getStackTrace(new Exception())
    import scala.util.Properties.{lineSeparator => NewLine}
    s"Node: $node$NewLine$NewLine" +
      s"Node Trace:$NewLine$nodeTrace$NewLine" +
      s"Stack Trace:$NewLine$stackTrace"
  }
}

object CallerDetailKey extends NonForwardingPluginTagKey[CallerDetail] {}
