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
package optimus.stratosphere.utils

import optimus.stratosphere.bootstrap.GitProcess
import optimus.stratosphere.bootstrap.OsSpecific
import optimus.stratosphere.bootstrap.StratosphereException
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.filesanddirs.PathsOpts._
import optimus.stratosphere.logger.CustomProcessLogger
import optimus.stratosphere.logger.Logger
import optimus.stratosphere.utils.IntervalPrinter.timeThis
import optimus.utils.ExitCode

import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.util
import scala.collection.compat._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.TimeoutException
import scala.concurrent.blocking
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.sys.process.ProcessLogger
import scala.sys.process.{Process => ScalaProcess}
import scala.util.Try
import scala.util.control.NonFatal
import scala.util.matching.Regex

class CommonProcess(stratoWorkspace: StratoWorkspaceCommon) {

  /**
   * Runs the IntelliJ ide in workspace specified by sourceDir argument.
   */
  def runIntellij(sourceDir: Path): Try[Int] =
    runStratosphereCommand("ide")(sourceDir, stratoWorkspace.log.info)

  def runIntellijClient(execPath: Path): Int = {
    val cmdLine: List[String] = List(execPath.getFullPath)
    val processBuilder = addJavaToPath(new ProcessBuilder(cmdLine.asJava))
    processBuilder.start()
    ExitCode.Success
  }

  def runIntellijServer(processBuilder: ProcessBuilder, verboseLogging: Boolean = false)(implicit log: Logger): Int = {
    val joinLinkPrefix: String = "Join link"
    log.highlight("Starting in Gateway server mode")
    val processLogger = ProcessLogger(line =>
      if (verboseLogging) {
        log.info(line)
      } else if (line.startsWith(joinLinkPrefix)) {
        log.highlight(line)
        log.info("Server is running in background...")
      })
    ScalaProcess(processBuilder).run(processLogger).exitValue()
  }

  def runStratosphereCommand(
      cmd: String*)(sourceDir: Path, log: String => Unit, additionalEnv: Map[String, String] = Map.empty): Try[Int] = {
    runCommandFromStratosphereBin("stratosphere", cmd.to(Seq))(sourceDir, log, additionalEnv)
  }

  def runDetachedStratosphereCommand(args: String*): Unit = {
    val dir = stratoWorkspace.directoryStructure.sourcesDirectory.toFile
    val stratosphereInfra = stratoWorkspace.stratosphereInfra
    val exec = stratosphereInfra.resolve("bin").resolve(s"stratosphere${OsSpecific.shellExt}")

    // Launch a console so that user can see something is happening. Installing new IntelliJ may take a while.
    val pre: Array[String] = if (OsSpecific.isWindows) CommonProcess.preamble.toArray :+ "start" else Array.empty

    new ProcessBuilder((pre ++ Array(exec.toAbsolutePath.toString) ++ args): _*).directory(dir).start()
  }

  def runCommandFromStratosphereBin(appName: String, args: Seq[String])(
      sourceDir: Path,
      log: String => Unit,
      additionalEnv: Map[String, String] = Map.empty): Try[Int] = {
    val command = if (OsSpecific.isWindows) {
      CommonProcess.preamble ++ Seq(stratoWorkspace.internal.stratosphere.scriptsDir + s"\\bin\\$appName") ++ args
    } else {
      val appFullPath = stratoWorkspace.internal.stratosphere.scriptsDir + s"/bin/$appName"
      appFullPath +: args
    }

    val env = System.getenv().asScala.to(Seq) ++ Seq(
      "STRATOSPHERE_INFRA" -> stratoWorkspace.stratosphereInfra.toString,
      OsSpecific.userHomeEnvName -> OsSpecific.userHome().toString
    ) ++
      stratoWorkspace.stratosphereInfraOverride.map("STRATOSPHERE_INFRA_OVERRIDE" -> _) ++
      additionalEnv

    Try {
      timeThis(command, stratoWorkspace.log) {
        ScalaProcess(command, Some(sourceDir.toFile.getCanonicalFile), env: _*).!(ProcessLogger(log))
      }
    }
  }

  def runAndWaitForPb(
      pb: ProcessBuilder,
      label: Option[String] = None,
      timeout: Duration = Duration.Inf,
      processLogger: CustomProcessLogger = stratoWorkspace.log.getProcessLogger(printOutputToScreen = false),
      ignoreExitCode: Boolean = false): String = {

    val cmdLabel = label.getOrElse(pb.command().asScala.mkString(" "))

    timeThis(cmdLabel, stratoWorkspace.log) {
      val process = ScalaProcess(pb).run(processLogger)
      val future = Future(blocking(process.exitValue()))

      def detailedErrorMessage =
        s"""[$cmdLabel] Command failed!
           |Working dir: ${Option(pb.directory()).getOrElse(s"Defaulted to ${System.getenv("user.dir")}")}
           |Path       : ${pb.environment().asScala.find(_._1.equalsIgnoreCase("path")).map(_._2).getOrElse("")}
           |Stacktrace : ${Thread.currentThread().getStackTrace.mkString("\n\tat ", "\n\tat ", "\n")}""".stripMargin

      def exceptionMessage = s"""[$cmdLabel] Command failed!
                                |${processLogger.output}""".stripMargin

      try {
        val exitCode = Await.result(future, timeout)
        if (exitCode == 0 || ignoreExitCode) {
          processLogger.standardOutputOnly
        } else {
          stratoWorkspace.log.debug(detailedErrorMessage)
          throw new StratosphereException(exceptionMessage)
        }
      } catch {
        case e: TimeoutException =>
          stratoWorkspace.log.error(detailedErrorMessage)
          process.destroy()
          stratoWorkspace.log.error(s"[$cmdLabel] Process destroyed after timing out!")
          throw new StratosphereException(exceptionMessage, e)
      }
    }
  }

  def runAndWaitFor(
      cmds: Iterable[String],
      label: Option[String] = None,
      dir: Option[Path] = None,
      env: Map[String, String] = Map.empty,
      timeout: Duration = Duration.Inf,
      withJavaOnPath: Boolean = false,
      processLogger: CustomProcessLogger = stratoWorkspace.log.getProcessLogger(printOutputToScreen = false),
      ignoreExitCode: Boolean = false): String = {
    val pb = new ProcessBuilder(cmds.toSeq: _*)
    val updatedPb = if (withJavaOnPath) addJavaToPath(pb) else pb
    if (dir.isDefined) updatedPb.directory(dir.get.toFile)
    env.foreach {
      case (key, "")    => updatedPb.environment().remove(key)
      case (key, value) => updatedPb.environment().put(key, value)
    }
    runAndWaitForPb(updatedPb, label, timeout = timeout, processLogger = processLogger, ignoreExitCode = ignoreExitCode)
  }

  def javaBuilder(cmdLine: Seq[String], dir: File): ProcessBuilder = {
    val builder = new ProcessBuilder(cmdLine: _*)
    addJavaToPath(builder)
      .directory(dir)
  }

  def addJavaToPath(builder: ProcessBuilder): ProcessBuilder = {
    val javaHome = stratoWorkspace.internal.java.home
    val javaPath = javaHome.resolve("bin").toString

    val newBuilder = addToPath(javaPath, builder)
    newBuilder.environment().put("JAVA_HOME", javaHome.toString)
    newBuilder
  }

  def addToPath(value: String, builder: ProcessBuilder): ProcessBuilder = {
    // case-insensitive copy of 'builder.environment' to cover different variants of 'Path' variable
    val env = new util.TreeMap[String, String](String.CASE_INSENSITIVE_ORDER)
    env.putAll(builder.environment)

    Option(env.get("path")) match {
      case Some(oldPath) =>
        val newPath = value + File.pathSeparator + oldPath
        env.put("path", newPath)
      case None =>
        println(s"WARNING: Path variable not found, this may be caused by mis-configured environment.")
        env.put("PATH", value)
    }

    builder.environment().clear()
    builder.environment().putAll(env)
    builder
  }

  def isRunning(processName: String): Boolean = processList(processName).nonEmpty

  def isIntellijRunning(shared: Boolean): Boolean =
    if (shared) {
      // check any Intellij process
      isRunning(CommonProcess.intellijProcessName)
    } else {
      // check current workspace's Intellij process
      isRunning(CommonProcess.intellijProcessName) &&
      stratoWorkspace.intellijDirectoryStructure.intellijPortFile.exists()
    }

  def processList(processName: String): Iterator[ProcessHandle] = {
    ProcessHandle.allProcesses().iterator().asScala.filter { process =>
      process.info().command().orElse("").contains(processName)
    }
  }

  def runGit(srcDir: Path, env: Map[String, String] = Map.empty, ignoreExitCode: Boolean = false)(
      args: String*): String = {
    val pb = new GitProcess(stratoWorkspace.config).createProcessBuilder(srcDir, env.asJava, args: _*)
    runAndWaitForPb(pb, ignoreExitCode = ignoreExitCode)
      .split("\n")
      .filterNot(CommonProcess.isGitTraceStatement)
      .mkString("\n")
      .trim
  }

  def runFs(args: String*): String = {
    try {
      val cmd = CommonProcess.preamble ++ Seq("fs") ++ args
      runAndWaitFor(cmd)
    } catch {
      case NonFatal(_) =>
        // windows CI nodes do not have 'fs' on PATH, we need to find it manually
        val fsDir = Paths.get("""C:\Program Files\OpenAFS""").dir
        val fs = fsDir
          .listDirs()
          .map(_.resolve("bin").resolve("fs.exe"))
          .find(_.exists())
          .getOrElse(throw new IllegalStateException(s"fs.exe not found at $fsDir"))
        runAndWaitFor(CommonProcess.preamble ++ Seq(fs.toString) ++ args)
    }
  }

  def runFsWsCell(): String = runFs("wscell").trim match {
    case CommonProcess.fsCellPattern(cell) => cell
    case other                             => other
  }

  def runWhere(exec: String): Option[String] =
    Try { runAndWaitFor(CommonProcess.preamble ++ Seq("where", exec)) }.toOption
}

object CommonProcess {
  val fsCellPattern: Regex = ".*This workstation belongs to cell '(.+)\\.ms\\.com'.*".r
  private val gitTracePattern: Regex = """^\d\d:\d\d:\d\d\.[\d]+ .*[\w]+: .*$""".r
  val intellijProcessName = "idea64"

  def isGitTraceStatement(line: String): Boolean =
    gitTracePattern.findFirstIn(line).isDefined

  def in(workspace: StratoWorkspaceCommon) = new CommonProcess(workspace)

  def preamble: Seq[String] = if (OsSpecific.isWindows) Seq("cmd", "/c") else Seq.empty
}
