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
package optimus.buildtool

import optimus.buildtool.app.NoBuildInstrumentation
import optimus.buildtool.app.OptimusBuildToolAppBase
import optimus.buildtool.app.OptimusBuildToolBootstrap
import optimus.buildtool.app.OptimusBuildToolCmdLine
import optimus.buildtool.app.OptimusBuildToolCmdLineT.NoneArg
import optimus.buildtool.app.OptimusBuildToolImpl
import optimus.buildtool.builders.reporter.ErrorReporter
import optimus.buildtool.files.Directory
import optimus.platform.OptimusApp.ExitHandler
import optimus.platform._
import org.fusesource.jansi.AnsiConsole

import java.io.PrintStream
import java.nio.file._
import scala.util.control.NonFatal

private[buildtool] object OptimusBuildTool
    extends OptimusApp[OptimusBuildToolCmdLine]
    with OptimusBuildToolAppBase[OptimusBuildToolCmdLine] {

  val originalStdOut: PrintStream = System.out
  val originalStdErr: PrintStream = System.err

  val DefaultArtifactVersionNumber = "1.57"

  override protected def parseCmdline(args: Array[String], exitHandler: ExitHandler): Unit = {
    super.parseCmdline(args, exitHandler)

    val logDir = if (cmdLine.bspServer) cmdLine.logDir.resolve("bsp") else cmdLine.logDir

    // Run this code as early as possible. Because we're modifying the log destination after startup,
    // we'll unfortunately end up with a (hopefully empty) logfile in the default destination (generally
    // %TEMP%/obt).
    OptimusBuildToolBootstrap.initLogging(
      Directory(logDir),
      cmdLine.debug,
      cmdLine.bspDebug,
      cmdLine.histoFreqSecs
    )

    if (cmdLine.bspServer) {
      def printStream(name: String) = {
        val fileStream = Files.newOutputStream(logDir.resolve(name))
        new PrintStream(fileStream)
      }

      System.setOut(printStream(s"${OptimusBuildToolBootstrap.logFilePrefix}-bsp-stdout.log"))
      System.setErr(printStream(s"${OptimusBuildToolBootstrap.logFilePrefix}-bsp-stderr.log"))
    }

    // this needs to happen after we've redirected stdout and stderr to a file for bsp mode, so that it's the
    // file outputs we wrap and not the default stdout/stderr
    AnsiConsole.systemInstall()
  }

  @entersGraph override def run(): Unit = {
    val errorReporter = new ErrorReporter(cmdLine.errorsDir)
    try {
      log.info(s"Starting OptimusBuildTool with args: ${args.mkString("[", ", ", "]")}")
      OptimusBuildToolBootstrap.initializeCrumbs(
        cmdLine.breadcrumbs,
        if (cmdLine.obtBenchmarkScenario != NoneArg) Some(cmdLine.obtBenchmarkScenario) else None,
        cmdLine.sendLongTermBreadcrumbs)
      val buildOk =
        if (cmdLine.scopesToBuild == Set("nobuild")) true
        else {
          val impl = OptimusBuildToolImpl(cmdLine, NoBuildInstrumentation, Some(errorReporter))
          impl.start()
        }

      obtExit(buildOk)
    } catch {
      case NonFatal(e) =>
        originalStdErr.println(s"OBT exiting due to exception: $e")
        errorReporter.writeErrorReport(e)
        throw e
    }
  }
}
