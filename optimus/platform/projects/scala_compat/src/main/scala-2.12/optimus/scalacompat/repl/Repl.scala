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
import java.io.Writer

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain
import scala.tools.nsc.interpreter.Completion
import scala.tools.nsc.interpreter.IMain
import scala.tools.nsc.interpreter.Results
import scala.tools.nsc.interpreter.{replProps => ReplProps}
import scala.tools.nsc.{Settings => nscSettings}

object Repl {
  def maxPrintStringOr(default: Int): Int = ReplProps.maxPrintString.option.getOrElse(default)
  def run(autorun: String, promptString: String, welcome: String, expandedApplicationClasspathString: String) = {
    import scala.tools.nsc.interpreter.ILoop
    class ConsoleLoop extends ILoop {

      override def prompt = promptString
      val initialCommands = autorun

      override def createInterpreter(): Unit = {
        super.createInterpreter()
        intp.interpret(initialCommands)
      }

      override def printWelcome(): Unit = {
        echo(welcome)
      }
    }

    val cl = new ConsoleLoop()
    val settings = new Settings
    settings.usejavacp.value = true
    settings.classpath.value = expandedApplicationClasspathString
    settings.deprecation.value = true
    cl.process(settings)
  }
  def apply(
      loader: ClassLoader,
      expandedApplicationClasspathString: String,
      output: PrintWriter,
      executeFirst: String): Repl = {
    val settings = new nscSettings()
    settings.embeddedDefaults(loader)
    settings.classpath.value = expandedApplicationClasspathString
    val iMain = new IMain(settings, output)
    iMain.interpret(executeFirst)
    new Repl(iMain)
  }
  sealed abstract class Result
  object Result {
    case object Success extends Result
    case object Error extends Result
    case object Incomplete extends Result
  }
  object Completion {
    val NoCandidates: CompletionCandidates = new CompletionCandidates(
      scala.tools.nsc.interpreter.Completion.NoCandidates)
  }
}
class CompletionCandidates private[repl] (wrapped: scala.tools.nsc.interpreter.Completion.Candidates) {
  def candidates: List[String] = wrapped.candidates
  def cursor: Int = wrapped.cursor
}

class Repl private (imain: IMain) {
  def runRepl(maxPrintString: Int, line: String): Repl.Result = {
    val lineName = "scala.repl.name.line"
    val old = System.setProperty(lineName, "$oline") // rename the default $line to avoid clash with EmbeddedREPL
    // we could set this only when the property is changed, but then we need to be careful not to initialize
    // the interpreter on startup and if someone changes it, and really it's complicated so just set it every time
    imain.isettings.maxPrintString = maxPrintString
    val res = imain.interpret(line)
    if (old eq null) System.clearProperty(lineName) else System.setProperty(lineName, old)
    res match {
      case Results.Success    => Repl.Result.Success
      case Results.Error      => Repl.Result.Error
      case Results.Incomplete => Repl.Result.Incomplete
    }
  }

  private lazy val compl = {
    import scala.tools.nsc.interpreter.ILoop
    val loop = new ILoop()
    new loop.ReplCompletion(imain)
  }

  def complete(text: String, curPos: Int): CompletionCandidates = {
    new CompletionCandidates(compl.complete(text, curPos))

  }
}
