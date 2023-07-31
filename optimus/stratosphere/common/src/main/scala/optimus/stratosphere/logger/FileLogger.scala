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

import java.io.Closeable
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Path
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

object FileLogger {
  private val inTest: AtomicBoolean = new AtomicBoolean(false)

  def markInTesting(): Unit = inTest.set(true)
  def canWriteToFile: Boolean = !inTest.get
}

final case class FileLogger(protected val logFile: Path) extends Logger with Closeable {
  private[this] lazy val writer = new FileWriter(logFile.toFile, /* append = */ true)

  def this(dir: Path, prefix: String) = this {
    try {
      val sdf = new SimpleDateFormat("yyyy.MM.dd-HH.mm.ss")
      val date = sdf.format(new Date())
      val logDir = dir.resolve(prefix)
      Files.createDirectories(logDir)
      logDir.resolve(s"$date.log")
    } catch {
      case _: java.lang.InternalError =>
        println("""
                  |Stratosphere cannot be run because of the bug in your ConEmu version:
                  |https://conemu.github.io/blog/2015/07/28/Build-150728.html
                  |Please install newest ConEmu version from http://myapps/ and try again.""".stripMargin)

        val failureExitCode = 1
        sys.exit(failureExitCode)
    }
  }

  override def readLine(text: String, args: Any*): Option[String] = {
    writeToFile(text.format(args: _*), lineFeed = false)
    None
  }

  override def handleAnswer(answer: String): Unit =
    info(answer)

  override def debug(toLog: String): Unit = {
    // debug is not printed to console but we want to capture on file!
    writeToFile(toLog)
  }

  override def info(toLog: String): Unit = {
    writeToFile(toLog)
  }

  private def writeToFile(toLog: Any, lineFeed: Boolean = true): Unit =
    if (FileLogger.canWriteToFile) {
      try {
        writer.append(toLog.toString)
        if (lineFeed) writer.append('\n')
        writer.flush()
      } catch {
        case NonFatal(e) =>
          println(s"WARN: Could not log message due to $e.")
      }
    }
  override def close(): Unit = writer.close()
}
