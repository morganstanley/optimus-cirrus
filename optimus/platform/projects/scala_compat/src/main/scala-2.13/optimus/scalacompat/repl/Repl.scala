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
package optimus.scalacompat.repl

import java.io.PrintWriter

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain
import scala.tools.nsc.interpreter.Results
import scala.tools.nsc.interpreter.shell._

object Repl {
  def maxPrintStringOr(default: Int): Int = throw new UnsupportedOperationException("Not supported yet on Scala 2.13")

  def run(autorun: String, promptString: String, welcome: String, expandedApplicationClasspathString: String) = {
    val settings = new Settings
    settings.usejavacp.value = true
    settings.classpath.value = expandedApplicationClasspathString
    settings.deprecation.value = true
    val shellConfig = new ShellConfig {
      override def filesToPaste: List[String] = Nil
      override def filesToLoad: List[String] = Nil
      override def batchText: String = ""
      override def batchMode: Boolean = false
      override def doCompletion: Boolean = true
      override def haveInteractiveConsole: Boolean = true
      override val promptText: String = promptString
    }
    class ConsoleLoop(settings: ShellConfig) extends ILoop(settings) {
      override protected def internalReplAutorunCode(): collection.immutable.Seq[String] = autorun :: Nil
      override def printWelcome(): Unit = {
        echo(welcome)
      }
    }
    new ConsoleLoop(shellConfig).run(settings)
  }
  def apply(
      loader: ClassLoader,
      expandedApplicationClasspathString: String,
      output: PrintWriter,
      executeFirst: String): Repl = {
    val settings = new Settings
    settings.usejavacp.value = true
    settings.embeddedDefaults(loader)
    settings.classpath.value = expandedApplicationClasspathString

    val shellConfig = new ShellConfig {
      override def filesToPaste: List[String] = Nil
      override def filesToLoad: List[String] = Nil
      override def batchText: String = ""
      override def batchMode: Boolean = false
      override def doCompletion: Boolean = true
      override def haveInteractiveConsole: Boolean = true
      override val promptText: String = promptString
    }
    val iloop = new ILoop(shellConfig)
    iloop.interpret(executeFirst)
    new Repl(iloop, iloop.Repl(shellConfig, settings, output))
  }

  sealed abstract class Result
  object Result {
    case object Success extends Result
    case object Error extends Result
    case object Incomplete extends Result
  }
  object Completion {
    val NoCandidates: CompletionCandidates = new CompletionCandidates(CompletionResult.empty)
  }
}
class CompletionCandidates private[repl] (wrapped: CompletionResult) {
  def candidates: List[String] = wrapped.candidates.map(_.declString())
  def cursor: Int = wrapped.cursor
}

class Repl private (iloop: ILoop, imain: IMain) {
  def runRepl(maxPrintString: Int, line: String): Repl.Result = {
    val lineName = "scala.repl.name.line"
    val old = System.setProperty(lineName, "$oline") // rename the default $line to avoid clash with EmbeddedREPL

    // we could set this only when the property is changed, but then we need to be careful not to initialize
    // the interpreter on startup and if someone changes it, and really it's complicated so just set it every time
    imain.reporter.asInstanceOf[ReplReporterImpl].maxPrintString = maxPrintString
    val res = imain.interpret(line)
    if (old eq null) System.clearProperty(lineName) else System.setProperty(lineName, old)
    res match {
      case Results.Success    => Repl.Result.Success
      case Results.Error      => Repl.Result.Error
      case Results.Incomplete => Repl.Result.Incomplete
    }
  }

  def complete(text: String, curPos: Int): CompletionCandidates = {
    val candidates = iloop.completion().complete(text, curPos)
    new CompletionCandidates(candidates)
  }
}
