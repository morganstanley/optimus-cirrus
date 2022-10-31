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

import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import scala.tools.nsc.ConsoleWriter
import scala.tools.nsc.NewLinePrintWriter
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain

class ScalaInterpreterSettings {

  /** The classpath used to initialize the interpreter. */
  def classpath: String = "."

  /** Whether to set settings.usejavacp, i.e., use the runtime classpath as compiler classpath */
  def useJavaClasspath: Boolean = false

  /** ClassLoader that the interpreter delegates to */
  def parentClassLoader: Option[ClassLoader] = None

  def out: PrintWriter = new NewLinePrintWriter(new ConsoleWriter, true)

  /** The script that the REPL will preload upon startup */
  def seedScript: String = ""

  /** File containing a script that the REPL will preload upon startup */
  def seedFile: String = ""

  /** Display evaluation of the seedScript / seedFile commands */
  def echoSeedScript: Boolean = true

  /** Enable printing of REPL initialization sequence */
  def echoInit: Boolean = true

  /** Change the default $line to avoid clash with EmbeddedREPL (used in Optimus profiler / debugger) */
  def changeLineName: Boolean = false

  /** Change the length at which output is truncated */
  def maxPrintString: Option[Int] = None

  /** Additional advanced scala and compiler settings. */
  def scalaSettings(settings: Settings): Unit = ()

  /** Command-line flags processed by Settings */
  def scalaReplArgs: List[String] = Nil

  /** Compiler plugin jars, if defined the plugin is added to the compiler */
  def entityPluginJars: Option[String] = None

  /** Enable settings.debug */
  def debug: Boolean = false

  /** Function to run before Interpreter / Repl init */
  def preInit(): Unit = ()
}

/**
 * Settings for the REPL. Extend this trait with custom settings and use it to initialize an EmbeddedREPL.
 */
class ScalaReplSettings extends ScalaInterpreterSettings {

  def in: Option[BufferedReader] = if (jLine) None else Some(new BufferedReader(new InputStreamReader(System.in)))

  override def out: PrintWriter = if (jLine) new PrintWriter(Console.out, true) else new PrintWriter(System.out)

  /** Defines the prompt string */
  def prompt: Option[String] = None

  /** Defines custom welcome message or null to use default Scala welcome message */
  def welcomeMessage: Option[String] = None

  /** Defines custom hint messages that the :hint REPL command will display in round robin mode. */
  def hintMessages: Array[String] = Array.empty

  /** Allows disabling JLine and using jl.System.in/out (instead of scala.Console.in / out) */
  def jLine: Boolean = true

  /**
   * Function to run before Optimus Thread Pool is created Analogous to `OptimusApp.setup()` (useful for native library
   * initialization which depends on InheritableThreadLocal)
   */
  def preOptimusThreadInit(): Unit = ()
}
