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
package optimus.buildtool.compilers.zinc.reporter

import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.CompilationMessage.Severity
import optimus.buildtool.compilers.zinc.ZincLogger
import optimus.buildtool.config.ConfigurableWarnings
import optimus.buildtool.format.MischiefArgs
import optimus.buildtool.format.MischiefMessage
import sbt.internal.inc.ProblemStringFormats
import xsbti.Position
import xsbti.Problem
import xsbti.Reporter

import java.io.File
import java.util.Optional
import scala.collection.immutable.Seq
import scala.collection.mutable

class ZincReporter(val logger: ZincLogger, bspServer: Boolean) {
  def log(severity: Severity)(s: => String): Unit = severity match {
    case Severity.Error   => logger.error(s)
    case Severity.Warning => logger.warn(s)
    case Severity.Info    => if (bspServer) logger.traceMessage(s) else logger.debug(s)
  }

  private val _messages = mutable.Buffer[CompilationMessage]()
  def addMessage(msg: CompilationMessage): Unit = _messages += msg
  def messages: Seq[CompilationMessage] = _messages.toIndexedSeq

  /**
   * Configure an xsbti.Reporter that accumulates in this ZincReporter.
   */
  def configure(config: ConfigurableWarnings, mischiefArgs: Option[MischiefArgs]): Reporter = {
    val reporter = new CompilationReporter(this, new ProblemConverter(config))

    // add mischief messages if needed
    mischiefArgs.foreach(args =>
      ZincReporter
        .fromMischief(args.extraScalacMessages)
        .foreach(problem => reporter.handle(problem.obtSeverity, problem)))

    reporter
  }
}

class CompilationReporter(val parent: ZincReporter, val converter: ConvertsProblems)
    extends Reporter
    with ProblemStringFormats {

  // Internal state
  private var _hasError = false
  private var _hasWarning = false
  private val allProblems = mutable.ListBuffer.empty[Problem]

  override def reset(): Unit = {
    _hasError = false
    _hasWarning = false
    allProblems.clear()
  }

  private def problemToString(problem: Problem, severity: Severity): String = {
    val pre = s"\n${severity.toString.toLowerCase}: "
    ProblemStringFormat.showLines(problem).mkString(pre, "\n", "")
  }

  private def updateInternalState(severity: Severity): Unit = {
    severity match {
      case Severity.Error   => _hasError = true
      case Severity.Warning => _hasWarning = true
      case Severity.Info    =>
    }
  }

  /**
   * Transform a problem into a CompilationMessage, record and log it.
   */
  private[reporter] def handle(raisedAt: Severity, problem: Problem): Unit = {
    val msg = converter(raisedAt, problem)
    parent.addMessage(msg)
    parent.log(msg.severity)(problemToString(problem, msg.severity))
    updateInternalState(msg.severity)
  }

  override def log(problem: Problem): Unit = {
    allProblems += problem
    problem.severity() match {
      case xsbti.Severity.Info  => handle(Severity.Info, problem)
      case xsbti.Severity.Warn  => handle(Severity.Warning, problem)
      case xsbti.Severity.Error => handle(Severity.Error, problem)
    }
  }
  override def hasWarnings: Boolean = _hasWarning
  override def hasErrors: Boolean = _hasError
  override def printSummary(): Unit = ()
  override def problems(): Array[xsbti.Problem] = allProblems.toArray
  override def comment(pos: xsbti.Position, msg: String): Unit = ()
}

object ZincReporter {
  private sealed trait MischiefPosition extends Position {
    override def lineContent(): String = ""
    override def offset(): Optional[Integer] = Optional.empty()
    override def pointer(): Optional[Integer] = Optional.empty()
    override def pointerSpace(): Optional[String] = Optional.empty()
    override def sourceFile(): Optional[File] = Optional.empty()
  }

  private final case class WithPosition(line: Optional[Integer], filePath: String) extends MischiefPosition {
    override def sourcePath(): Optional[String] = Optional.of(filePath)
  }

  private final case object NoPos extends MischiefPosition {
    override def line(): Optional[Integer] = Optional.empty()
    override def sourcePath(): Optional[String] = Optional.empty()
  }

  private final case class MischiefProblem(position: Position, obtSeverity: Severity, message: String) extends Problem {
    override def category(): String = "mischief"
    override def severity(): xsbti.Severity = convertSeverity(obtSeverity)
  }
  private def convertSeverity(obtSeverity: Severity): xsbti.Severity = obtSeverity match {
    case Severity.Error   => xsbti.Severity.Error
    case Severity.Warning => xsbti.Severity.Warn
    case Severity.Info    => xsbti.Severity.Info
  }

  private def fromMischief(in: Seq[MischiefMessage]): Seq[MischiefProblem] = {
    in.flatMap { msg =>
      val out = Seq.newBuilder[MischiefProblem]

      val sev = CompilationMessage.Severity.safelyParse(msg.severity) match {
        case Some(sev) => sev
        case None =>
          out += MischiefProblem(
            NoPos,
            Severity.Error,
            s"Could not parse severity ${msg.severity} in mischievous message")
          Severity.Warning // for the original message that we can't parse
      }

      (msg.file, msg.line) match {
        case (None, None)             => out += MischiefProblem(NoPos, sev, msg.msg)
        case (Some(file), None)       => out += MischiefProblem(WithPosition(Optional.empty(), file), sev, msg.msg)
        case (Some(file), Some(line)) => out += MischiefProblem(WithPosition(Optional.of(line), file), sev, msg.msg)
        case (None, Some(line)) =>
          out += MischiefProblem(
            NoPos,
            Severity.Error,
            s"Got line ${line} but no corresponding file in mischievous message")
          out += MischiefProblem(NoPos, sev, msg.msg)
      }
      out.result()
    }
  }
}
