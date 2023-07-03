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

import optimus.scalacompat.repl.ScalaInterpreter.Message
import optimus.scalacompat.repl.ScalaInterpreter.ReporterState

import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer
import scala.reflect.internal.Reporter.Severity
import scala.reflect.internal.util.Position
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.Completion
import scala.tools.nsc.interpreter.ILoop
import scala.tools.nsc.interpreter.IMain
import scala.tools.nsc.interpreter.NamedParam
import scala.tools.nsc.interpreter.PresentationCompilerCompleter
import scala.tools.nsc.interpreter.ReplReporter
import scala.tools.nsc.interpreter.Results.Error
import scala.tools.nsc.interpreter.Results.Result
import scala.tools.nsc.interpreter.Results.Success
import scala.tools.nsc.interpreter.replProps
import scala.tools.nsc.interpreter.words

class ScalaInterpreter private (settings: Settings, interpreterSettings: ScalaInterpreterSettings) {
  import ScalaInterpreter.lineNameProperty

  private[repl] var _reporterState: ReporterState = null

  /**
   * For testing, allows looking at reported messages (IMain calls `reporter.reset()` after each line, so we keep
   * messages externally). See also ScalaInterpreterTestUtils for testing utilities.
   */
  def enableReporterStorage(): Unit = { _reporterState = new ReporterState() }
  def reporterState: ReporterState = _reporterState

  val iMain: IMain = new IMain(settings, interpreterSettings.out) { iMainSelf =>
    override protected def parentClassLoader: ClassLoader =
      interpreterSettings.parentClassLoader.getOrElse(super.parentClassLoader)

    override lazy val memberHandlers = new {
      val intp: iMainSelf.type = iMainSelf
    } with OptimusMemberHandlers

    override lazy val reporter: ReplReporter = new ReplReporter(iMainSelf) {
      override def doReport(pos: Position, msg: String, severity: Severity): Unit = {
        if (_reporterState != null)
          _reporterState.add(Message(pos, msg, severity))
        super.doReport(pos, msg, severity)
      }
    }
  }

  private val maxPrintStringProp = replProps.maxPrintString.option.orElse(interpreterSettings.maxPrintString)

  private lazy val completor = new PresentationCompilerCompleter(iMain)

  private def logInit(msg: String): Unit = if (interpreterSettings.echoInit) {
    interpreterSettings.out.println(msg)
    interpreterSettings.out.flush()
  }

  private[repl] def loadScript(script: String): Result = {
    if (script == null || script.isEmpty) Success
    else if (interpreterSettings.echoSeedScript) iMain.interpret(script)
    else iMain.quietRun(script)
  }

  private def init(): Unit = {
    logInit("****************** Initializing REPL *******************")
    interpreterSettings.preInit()

    if (interpreterSettings.debug) settings.debug.value = true

    logInit("* Setting classpath...")
    if (interpreterSettings.useJavaClasspath)
      settings.usejavacp.value = true
    settings.classpath.value = interpreterSettings.classpath

    settings.deprecation.value = true

    // These settings are on by default on 2.13, so enabling them on 2.12 simplifies migration.
    // Class-based wrapping is needed to prevent deadlocks in the REPL when interacting with async code e.g.
    // foo.apar.map { x => a + somethingInTheOutsideScope }
    settings.Yreplclassbased.value = true
    settings.YreplMagicImport.value = true

    interpreterSettings.entityPluginJars.foreach(jars => {
      logInit("* Loading Optimus entity compiler plugin...")
      settings.plugin.value ++= jars :: Nil
      settings.require.value ++= "entity" :: Nil
      settings.pluginOptions.value ++= "entity:silence:10571;17000;17001" :: Nil
    })

    val (scalaReplArgsProcessingSuccess, unprocessedScalaReplArgs) =
      settings.processArguments(interpreterSettings.scalaReplArgs, true)
    if (!scalaReplArgsProcessingSuccess)
      logInit("* WARNING: failed to process Scala REPL Arguments: " + unprocessedScalaReplArgs)

    interpreterSettings.scalaSettings(settings)

    // Preload seed script
    logInit("* Preloading seed script...")

    var seedString = interpreterSettings.seedScript
    if (interpreterSettings.seedFile != "" && interpreterSettings.seedFile != null) {
      try {
        val file = new File(interpreterSettings.seedFile)
        if (file.exists)
          seedString = seedString + Files.readString(file.toPath)
        else
          logInit("* WARNING: Given seed script file not exists: " + file.getAbsolutePath)
      } catch {
        case e: IOException => {
          logInit("* WARNING: Can't load seed script file: " + e.getMessage)
        }
      }
    }
    iMain.global // initialize compiler
    loadScript(seedString)
  }

  def interpret(line: String, truncate: Boolean = true): Result = {
    def intp = {
      val maxPrint = if (truncate) maxPrintStringProp else Some(0)
      val old = iMain.isettings.maxPrintString
      maxPrint.foreach(iMain.isettings.maxPrintString = _)
      val res = iMain.interpret(line)
      iMain.isettings.maxPrintString = old
      res
    }
    if (!interpreterSettings.changeLineName) intp
    else {
      val old = System.setProperty(lineNameProperty, "$oline")
      val res = intp
      if (old eq null) System.clearProperty(lineNameProperty) else System.setProperty(lineNameProperty, old)
      res
    }
  }

  def bind[T: Manifest](name: String, value: T): Result =
    bind(NamedParam(name, value))

  def bind[T: Manifest](param: NamedParam): Result = iMain.beSilentDuring {
    iMain.bind(param)
  }

  def complete(text: String, cursor: Int): Completions =
    new Completions(completor.complete(text, cursor))

  def close(): Unit = iMain.close()
}

object ScalaInterpreter {
  val lineNameProperty = "scala.repl.name.line"

  /**
   * @param parentClassLoader
   *   ClassLoader that the interpreter delegates to
   * @param changeLineName
   *   Change the default $line to avoid clash with EmbeddedREPL (used in Optimus profiler / debugger)
   */
  def apply(interpreterSettings: ScalaInterpreterSettings): ScalaInterpreter = {
    val settings = new Settings()
    val intp = new ScalaInterpreter(settings, interpreterSettings)
    intp.init()
    intp
  }

  case class Message(pos: Position, msg: String, severity: Severity)
  class ReporterState {
    val _messages: ListBuffer[Message] = ListBuffer.empty

    def add(msg: Message): Unit = _messages += msg

    def messages: List[Message] = _messages.toList

    def reset(): Unit = _messages.clear()
  }
}

class Completions private[repl] (wrapped: Completion.Candidates) {
  def cursor: Int = wrapped.cursor
  def candidates: List[String] = wrapped.candidates
}
object Completions {
  val NoCompletions = new Completions(Completion.NoCandidates)
}

class ScalaRepl private (val intp: ScalaInterpreter, replSettings: ScalaReplSettings) {
  val iLoop: ILoop = new ILoop(replSettings.in, replSettings.out) {
    override def prompt = replSettings.prompt.getOrElse(super.prompt)

    override def createInterpreter(): Unit = {
      this.intp = ScalaRepl.this.intp.iMain
    }

    override def printWelcome(): Unit =
      replSettings.welcomeMessage.fold(super.printWelcome())(echo)

    override def commands: List[LoopCommand] = customCommands ++ super.commands

    lazy val customCommands = List(
      LoopCommand.cmd("bindings", "", "(OPT) list object bindings in this session", bindingsCommand),
      LoopCommand.cmd(
        "hint",
        "<|all>",
        "(OPT) without argument print hints about appication usage one by one. With 'all' prints all messages.",
        hintCommand)
    )

    private def bindingsCommand(line: String): Result =
      bindings.foreach(b => this.intp.reporter.printMessage(b.name + ": " + b.value.toString))

    private def hintCommand(line: String): Result = {
      words(line) match {
        case "all" :: Nil => this.intp.reporter.printMessage(getHints)
        case _            => this.intp.reporter.printMessage(getHint())
      }
    }
  }

  // bindings //

  private var bindings = List[NamedParam]()

  def bind[T: Manifest](name: String, value: T): Result = intp.bind(name, value)

  def bind[T: Manifest](param: NamedParam): Result = intp.bind(param) match {
    case s @ Success =>
      bindings = param :: bindings
      s
    case r => r
  }

  // hints //

  private val hintIdx: AtomicInteger = new AtomicInteger(0)
  private def hints = replSettings.hintMessages

  def getHint(): String =
    if (hints.length > 0) hints(hintIdx.getAndSet((hintIdx.get() + 1) % hints.length))
    else "No messages available"

  def getHints: String = hints.mkString("\n")

  // rest //

  /** Run a REPL line (Scala code, or `:some` repl command) */
  def command(cmd: String): Result = {
    if (cmd.trim.startsWith(":")) try {
      iLoop.colonCommand(cmd.trim)
      Success
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Error
    }
    else intp.interpret(cmd)
  }

  def iMain = intp.iMain
  def settings = iMain.settings

  private def init(): Unit = {
    // for clarity, not strictly needed; passing `in = Some(...)` to `new ILoop` also disables JLine
    if (!replSettings.jLine) settings.Xnojline.value = true
  }

  def start(): Boolean = iLoop.process(settings)
}

object ScalaRepl {
  def apply(replSettings: ScalaReplSettings): ScalaRepl = {
    val repl = new ScalaRepl(ScalaInterpreter(replSettings), replSettings)
    repl.init()
    repl
  }

  def start(replSettings: ScalaReplSettings): Boolean = apply(replSettings).start()
}
