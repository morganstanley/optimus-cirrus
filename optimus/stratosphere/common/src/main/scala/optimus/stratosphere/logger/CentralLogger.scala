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

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

object CentralLogger {
  private val verboseState = new AtomicBoolean(false)

  def verbose: Boolean = verboseState.get()
  def makeVerbose(): Unit = verboseState.set(true) // One-way change
}

final case class CentralLogger(loggers: Seq[Logger]) extends Logger {
  import CentralLogger._

  def this(dir: Path, colors: ConsoleColors) =
    this(Seq(ConsoleLogger(colors), new FileLogger(dir, "stratosphere")))

  def withLogger(additionalLogger: Logger): CentralLogger =
    CentralLogger(loggers :+ additionalLogger)

  override def info(toLog: String): Unit =
    loggers.foreach(_.info(toLog))

  override def highlight(toLog: String): Unit =
    loggers.foreach(_.highlight(toLog))

  override def warning(toLog: String): Unit =
    loggers.foreach(_.warning(toLog))

  override def error(toLog: String): Unit =
    loggers.foreach(_.error(toLog))

  // When verbose is ON, we upgrade to info
  override def debug(toLog: String): Unit =
    if (verbose) info(toLog) else loggers.foreach(_.debug(toLog))

  def getProcessLogger(printOutputToScreen: Boolean): CustomProcessLogger =
    CustomProcessLogger(this, printOutputToScreen)

  override def handleAnswer(answer: String): Unit =
    loggers.foreach(_.handleAnswer(answer))

  override def readLine(text: String, args: Any*): Option[String] = {
    val answer = loggers.flatMap { strategy =>
      strategy.readLine(text, args: _*)
    }.headOption
    answer.foreach(handleAnswer)
    answer
  }
}
