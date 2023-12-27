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
package optimus.git.diffparser.model

import java.util.regex.Pattern

import scala.collection.immutable.Seq
import scala.collection.compat._
import scala.collection.mutable

final case class Range(lineStart: Int, lineCount: Int)

object LineType extends Enumeration {
  type LineType = Value
  val From, To, Neutral = Value
}

final case class Line(tpe: LineType.Value, content: String) {
  def isNeutral: Boolean = tpe == LineType.Neutral
  def isFrom: Boolean = tpe == LineType.From
  def isTo: Boolean = tpe == LineType.To
}
object Line {
  def from(content: String): Line = Line(LineType.From, content)
  def to(content: String): Line = Line(LineType.To, content)
  def neutral(content: String): Line = Line(LineType.Neutral, content)
}

final case class PartialHunk(fromFileRange: Range, toFileRange: Range, lines: mutable.Buffer[Line]) {
  def this(fromFileRange: Range, toFileRange: Range) = { // used from Java
    this(fromFileRange, toFileRange, mutable.Buffer.empty)
  }

  def addLine(line: Line): PartialHunk = {
    this.lines :+ line
    this
  }

  def toHunk(): Hunk = Hunk(fromFileRange, toFileRange, lines.to(Seq))
}
final case class Hunk(fromFileRange: Range, toFileRange: Range, lines: Seq[Line])

object Hunk {
  private val LineRangePattern: Pattern =
    Pattern.compile("^.*-([0-9]+)(?:,([0-9]+))? \\+([0-9]+)(?:,([0-9]+))?.*$")

  def fromLine(currentLine: String): PartialHunk = {
    val matcher = LineRangePattern.matcher(currentLine)
    if (matcher.matches) {
      val range1Start = matcher.group(1)
      val range1Count = if (matcher.group(2) != null) { matcher.group(2) }
      else { "1" }
      val fromRange: Range = Range(range1Start.toInt, range1Count.toInt)
      val range2Start = matcher.group(3)
      val range2Count = if (matcher.group(4) != null) { matcher.group(4) }
      else { "1" }
      val toRange: Range = Range(range2Start.toInt, range2Count.toInt)
      PartialHunk(fromRange, toRange, mutable.Buffer.empty)
    } else {
      throw new IllegalStateException(
        s"No line ranges found in the following hunk start line: '$currentLine'. Expected something like '-1,5 +3,5'.")
    }
  }

  def isHunkStart(line: String): Boolean = LineRangePattern.matcher(line).matches
}

object PartialDiff {
  private val RealPathSuffixPattern = Pattern.compile("([^/]/|src://|dst://)(.+)")
  private val HeaderFilePattern = Pattern.compile("diff --git (\\w/.+|src://.+) (\\w/.+|dst://.+)")

  def empty: PartialDiff = PartialDiff(None, None, mutable.Buffer.empty, mutable.Buffer.empty)

  /**
   * When file is deleted, path is set as '/dev/null'. Otherwise we drop the first 2 characters
   */
  def getRealPath(fileName: String): String = {
    val m = PartialDiff.RealPathSuffixPattern.matcher(fileName)
    if (fileName.equals(Diff.noFile)) {
      fileName
    } else if (m.find()) {
      m.group(2)
    } else {
      throw new RuntimeException(s"unable to find match for '$fileName'");
    }
  }

  def fileNameFromHeader(header: String, fromFile: Boolean): String = {
    val matcher = HeaderFilePattern.matcher(header)
    if (matcher.find()) {
      matcher.group(if (fromFile) 1 else 2)
    } else {
      // fall back to tokenizing spaces just to be sure
      val index = if (fromFile) 2 else 3
      header.split(" ")(index)
    }
  }
}
final case class PartialDiff(
    fromFileName: Option[String],
    toFileName: Option[String],
    headerLines: mutable.Buffer[String],
    hunks: mutable.Buffer[PartialHunk]) {

  import PartialDiff._

  def addLine(line: Line): PartialDiff = {
    hunks.last.lines += line
    this
  }

  def addHunk(hunk: PartialHunk): PartialDiff = {
    hunks += hunk
    this
  }

  def addHeaderLine(line: String): PartialDiff = {
    headerLines += line
    this
  }

  def setToFileName(name: String): PartialDiff =
    this.copy(toFileName = Some(name))

  def setFromFileName(name: String): PartialDiff =
    this.copy(fromFileName = Some(name))

  def toDiff: Diff =
    Diff(
      // in case of new / deleted / renamed files there are no +++ / --- lines
      getRealPath(fromFileName.getOrElse(fileNameFromHeader(headerLines.head, fromFile = true))),
      getRealPath(toFileName.getOrElse(fileNameFromHeader(headerLines.head, fromFile = false))),
      headerLines.to(Seq),
      hunks.map(_.toHunk()).to(Seq)
    )
}

final case class Diff(fromFileName: String, toFileName: String, headerLines: Seq[String], hunks: Seq[Hunk]) {
  require(headerLines != null || headerLines.nonEmpty, "Header must not be empty, something went wrong in parsing!")

  import Diff._

  def isNewFile: Boolean = headerLines.exists(_.startsWith(newFileMsg))
  def isRenamedFile: Boolean =
    Seq(renamedFileFromMsg, renamedFileToMsg).forall(line => headerLines.exists(_.startsWith(line)))
  def isRenamedIdenticalFile: Boolean = isRenamedFile && headerLines.contains(identicalFileContents)
  def isFileDeletion: Boolean = headerLines.exists(_.startsWith(deletedFileMsg))
  def isFileModeChange: Boolean = headerLines.exists(_.startsWith(newFileModeMsg))

  private def findMode(msg: String): String =
    headerLines.find(_.startsWith(msg)).map(_.replace(msg, "")).getOrElse(throw new RuntimeException(""))

  def getFileMode: String =
    if (isNewFile) {
      findMode(newFileMsg)
    } else if (isFileDeletion) {
      findMode(deletedFileMsg)
    } else if (isFileModeChange) {
      findMode(newFileModeMsg)
    } else {
      // example line: 'index 22e835517..e06a070ca 100644'
      headerLines
        .find(_.startsWith("index"))
        .map(_.split(" ").to(Seq))
        .fold("")(_.last)
    }

  def getFileName: String = {
    if (toFileName.equals(noFile)) fromFileName else toFileName
  }
}

object Diff {
  val noFile: String = "/dev/null"
  val newFileMsg: String = "new file mode "
  val deletedFileMsg: String = "deleted file mode "
  val newFileModeMsg: String = "new mode "
  val identicalFileContents: String = "similarity index 100%"
  val renamedFileFromMsg: String = "rename from "
  val renamedFileToMsg: String = "rename to "

}
