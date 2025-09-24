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
package optimus.stratosphere.logger

import optimus.stratosphere.config.ConsoleColors
import org.fusesource.jansi.Ansi.Color
import org.fusesource.jansi.Ansi.ansi

import java.io.ByteArrayOutputStream
import java.io.PrintStream

final case class LogLines(nonBlank: Seq[String], leadingNewlines: String, trailingNewlines: String)

object LogLines {
  def apply(message: String): LogLines = {
    val returnTrailing = -1
    val lines = message.split("\n", returnTrailing)
    val emptyLinesStart = if (message.isBlank) 0 else lines.takeWhile(_.isBlank).length
    val emptyLinesEnd = if (message.isBlank) 0 else lines.reverse.takeWhile(_.isBlank).length
    LogLines(lines.drop(emptyLinesStart).dropRight(emptyLinesEnd), "\n" * emptyLinesStart, "\n" * emptyLinesEnd)
  }
}

object Logger {
  val ErrorMarker = "ERROR: "
  val WarningMarker = "WARNING: "

  def addColorTo(text: String, color: Color)(implicit colors: ConsoleColors): String =
    if (colors.enabled) ansi().fg(color).a(text).reset().toString else text
}

abstract class Logger(colors: ConsoleColors = ConsoleColors.Disabled) {
  import optimus.stratosphere.logger.Logger._
  private implicit val _colors: ConsoleColors = colors

  def info(toLog: String): Unit
  def debug(toLog: String): Unit
  def handleAnswer(answer: String): Unit

  private def printWithIndent(toLog: String, indentMarker: String): String = {
    val indent = " " * indentMarker.length

    val lines = LogLines(toLog)

    lines.nonBlank match {
      case Seq()       => lines.leadingNewlines + indentMarker + lines.trailingNewlines
      case Seq(single) => lines.leadingNewlines + indentMarker + single + lines.trailingNewlines
      case Seq(first, rest @ _*) =>
        val content = rest.map(line => if (line.isBlank) line else indent + line).mkString("\n")
        s"""${lines.leadingNewlines}$indentMarker$first
           |$content${lines.trailingNewlines}""".stripMargin
    }
  }

  def highlight(toLog: String): Unit = info(addColorTo(toLog, colors.highlight))

  def warning(toLog: String): Unit = info(addColorTo(printWithIndent(toLog, WarningMarker), colors.warning))

  def error(toLog: String): Unit = info(addColorTo(printWithIndent(toLog, ErrorMarker), colors.error))

  private def printStackTrace(msg: String, t: Throwable, loggerLevel: String => Unit): Unit = {
    loggerLevel(msg)
    val byteArrayOutputStream: ByteArrayOutputStream = new ByteArrayOutputStream
    t.printStackTrace(new PrintStream(byteArrayOutputStream))
    loggerLevel(byteArrayOutputStream.toString)
  }

  def error(msg: String, t: Throwable): Unit = printStackTrace(msg, t, error)

  def debug(msg: String, t: Throwable): Unit = printStackTrace(msg, t, debug)

  def readLine(text: String, args: Any*): Option[String] = {
    info(text.format(args: _*))
    None
  }

  def printThrowable(t: Throwable, helpMailGroup: String): Unit =
    error(s"An unexpected error has occurred: $t. Please contact $helpMailGroup.", t)

  def withBanner: Logger = WithBannerLogger(this)
}

/**
 * Wraps the delegate Logger and prints every message in a banner:
 *
 * {{{
 *   =========
 *    Message
 *   =========
 * }}}
 *
 * If the message contains any leading/trailing whitespace those are kept outside the banner.
 */
final case class WithBannerLogger(delegate: Logger) extends Logger {
  override def info(toLog: String): Unit = printInBanner(toLog, delegate.info)
  override def debug(toLog: String): Unit = printInBanner(toLog, delegate.debug)
  override def handleAnswer(answer: String): Unit = delegate.handleAnswer(answer)

  private def printInBanner(message: String, print: String => Unit): Unit = {
    val lines = LogLines(message)
    val longestLine = message.split("\n").map(_.length).max
    val banner = "=" * (longestLine + 2)
    print(s"""${lines.leadingNewlines}$banner
             |${lines.nonBlank.map(" " + _).mkString("\n")}
             |$banner${lines.trailingNewlines}""".stripMargin)
  }
}
