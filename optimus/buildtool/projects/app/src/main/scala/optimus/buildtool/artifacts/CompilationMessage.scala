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
package optimus.buildtool.artifacts

import java.util.Optional
import optimus.buildtool.utils.StackUtils
import optimus.buildtool.utils.TypeClasses._
import optimus.tools.scalacplugins.entity.reporter.OptimusNonErrorMessages

import scala.compat.java8.OptionConverters._
import scala.reflect.internal.util.NoPosition
import scala.reflect.internal.util.Position

final case class CompilationMessage(
    pos: Option[MessagePosition],
    msg: String,
    severity: CompilationMessage.Severity,
    alarmId: Option[String],
    isSuppressed: Boolean = false,
    isNew: Boolean = false
) extends Ordered[CompilationMessage] {

  def alwaysIgnore: Boolean = alarmId.contains(OptimusNonErrorMessages.INTERNAL_COMPILE_INFO.id.sn.toString)

  override def toString: String = pos match {
    case Some(p) => s"$severity $p $msg"
    case None    => s"$severity $msg"
  }

  def isWarning: Boolean = severity == CompilationMessage.Warning
  def isError: Boolean = severity == CompilationMessage.Error
  override def compare(y: CompilationMessage): Int =
    pos.compare(y.pos) elze alarmId.compare(y.alarmId) elze msg.compare(y.msg) elze
      severity.toString.compare(y.severity.toString)
}

//noinspection TypeAnnotation
object CompilationMessage {
  type Severity = optimus.buildtool.artifacts.Severity
  val Severity = optimus.buildtool.artifacts.Severity

  val Error = Severity.Error
  val Warning = Severity.Warning
  val Info = Severity.Info

  def message(msg: String, severity: Severity) = apply(MessagePosition(NoPosition), msg, severity)
  def message(t: Throwable, severity: Severity): CompilationMessage =
    message(StackUtils.multiLineStacktrace(t), severity)
  def message(msg: String, t: Throwable, severity: Severity): CompilationMessage =
    message(s"$msg: ${StackUtils.multiLineStacktrace(t)}", severity)

  def info(msg: String) = message(msg, Info)

  def error(msg: String): CompilationMessage = message(msg, Error)
  def error(t: Throwable): CompilationMessage = message(t, Error)
  def error(msg: String, t: Throwable): CompilationMessage = message(msg, t, Error)

  def warning(msg: String): CompilationMessage = message(msg, Warning)

  def apply(pos: Option[MessagePosition], msg: String, severity: Severity): CompilationMessage = {
    val alarmId = extractAlarmId(msg)
    apply(pos, msg, severity, alarmId)
  }

  // example of an Optimus alert: '[SUPPRESSED] Optimus: (12345) danger! danger!'
  private val alarmIdExtractor = """Optimus: \((\d+)\)""".r
  private def extractAlarmId(msg: String): Option[String] =
    alarmIdExtractor.findFirstMatchIn(msg).map(_.group(1))
}

// Because you can't easily reconstruct a real scala.reflect.internal.util.Position, and we don't need one anyway.
// Note that filepath is relative to the workspace root (i.e. it starts with src/).
// startPoint and endPoint are offsets from the first character in the file, and are used for message highlighting
// in IntelliJ.
final case class MessagePosition(
    filepath: String,
    startLine: Int,
    startColumn: Int,
    endLine: Int,
    endColumn: Int,
    startPoint: Int,
    endPoint: Int
) extends Ordered[MessagePosition] {
  override def toString: String = s"$filepath:[$startLine:$startColumn]:[$endLine:$endColumn]"
  override def compare(that: MessagePosition): Int =
    filepath.compare(that.filepath) elze startPoint.compare(that.startPoint) elze endPoint.compare(that.endPoint)
}

object MessagePosition {
  def apply(filepath: String): MessagePosition = MessagePosition(filepath, 0, 0, 0, 0, 0, 0)
  def apply(filepath: String, line: Int): MessagePosition = MessagePosition(filepath, line, line, 0, 0, 0, 0)
  def apply(filepath: String, line: Int, column: Int): MessagePosition =
    MessagePosition(filepath, line, line, column, column, 0, 0)
  def apply(posIn: Position): Option[MessagePosition] = {
    val pos = if (posIn eq null) NoPosition else posIn
    pos match {
      case NoPosition => None
      case p =>
        val src = p.source.file.path.replace('\\', '/')
        val start = p.focusStart
        val end = p.focusEnd
        Some(
          MessagePosition(
            src,
            startLine = start.line,
            startColumn = start.column,
            endLine = end.line,
            endColumn = end.column,
            startPoint = start.point,
            endPoint = end.point
          )
        )
    }
  }

  def apply(pos: xsbti.Position): Option[MessagePosition] =
    if (pos.sourcePath.isPresent) {
      val fname = pos.sourcePath.get.replace('\\', '/')

      // Note that Zinc returns 0-based columns and 1-based lines. The OBT standard is 1-based for both (same for plain
      // ScalaC without Zinc). To make things even more fun, BSP is 0-based for both so we convert again later.
      // Note also that we fall back to non-range positions in case start/end positions are missing
      def convertCol(o: Optional[Integer]): Int =
        o.asScala.orElse(pos.pointer.asScala).map(_ + 1).getOrElse(-1)

      def convertLine(o: Optional[Integer]): Int =
        o.asScala.orElse(pos.line.asScala).map(i => i: Int).getOrElse(-1)

      def convertPoint(o: Optional[Integer]): Int =
        o.asScala.orElse(pos.offset.asScala).map(i => i: Int).getOrElse(-1)

      Some(
        MessagePosition(
          fname,
          startLine = convertLine(pos.startLine),
          startColumn = convertCol(pos.startColumn),
          endLine = convertLine(pos.endLine),
          endColumn = convertCol(pos.endColumn),
          startPoint = convertPoint(pos.startOffset()),
          endPoint = convertPoint(pos.endOffset())
        )
      )
    } else None
}
