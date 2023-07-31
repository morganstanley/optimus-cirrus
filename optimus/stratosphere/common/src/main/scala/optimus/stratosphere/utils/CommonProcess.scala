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
import optimus.stratosphere.utils.IntervalPrinter.timeThis

import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.util
import scala.collection.compat._
import scala.collection.immutable.Seq
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

  def runStratosphereCommand(
      cmd: String*)(sourceDir: Path, log: String => Unit, additionalEnv: Map[String, String] = Map.empty): Try[Int] = {
    runCommandFromStratosphereBin("stratosphere", cmd.to(Seq))(sourceDir, log, additionalEnv)
  }

  def runDetachedStratosphereCommand(args: String*): Unit = {
    val dir = stratoWorkspace.directoryStructure.sourcesDirectory.toFile
    val env = sys.env.map { case (k, v) => s"$k=$v" }.toArray
    val stratosphereInfra = stratoWorkspace.stratosphereInfra
    val exec = stratosphereInfra.resolve("bin").resolve(s"stratosphere${OsSpecific.shellExt}")
    val cmd = Array(exec.toAbsolutePath.toString) ++ args
    Runtime.getRuntime.exec(cmd, env, dir)
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

  def runNodeScript(
      pathToScript: Path,
      runInDir: Path,
      nodeVersion: String,
      processLogger: CustomProcessLogger = stratoWorkspace.log.getProcessLogger(printOutputToScreen = false)
  ): String = {
    val args =
      if (OsSpecific.isLinux)
        Seq("ksh", "-c") ++ Seq(
          Seq(
            s". ${stratoWorkspace.internal.paths.initEnviron}",
            "export TRAIN_CI=true",
            "export WORKSPACE=/var/tmp/$$_$USER",
            s"export NODE_VERSION=$nodeVersion",
            "export NPM_CONFIG_USERCONFIG=/var/tmp/$$_$USER/.npmrc",
            "module load msde/artifactory-eng-tools/prod",
            "setup_user -cli npm",
            s"module load ossjs/node/$nodeVersion",
            s"node ${pathToScript.toString}"
          ).mkString(" && "))
      else
        CommonProcess.preamble ++ Seq(
          Seq(
            s"set NODE_VERSION=$nodeVersion",
            "module load msde/artifactory-eng-tools/prod",
            "setup_user -cli npm",
            s"module load ossjs/node/$nodeVersion",
            s"node ${pathToScript.toString}"
          ).mkString(" && "))

    runAndWaitFor(args, dir = Some(runInDir), processLogger = processLogger)
  }

  def runAndWaitForPb(
      pb: ProcessBuilder,
      label: Option[String] = None,
      timeout: Duration = Duration.Inf,
      processLogger: CustomProcessLogger = stratoWorkspace.log.getProcessLogger(printOutputToScreen = false))
      : String = {

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
        if (exitCode == 0) {
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
      processLogger: CustomProcessLogger = stratoWorkspace.log.getProcessLogger(printOutputToScreen = false))
      : String = {
    val pb = new ProcessBuilder(cmds.toSeq: _*)
    val updatedPb = if (withJavaOnPath) addJavaToPath(pb) else pb
    if (dir.isDefined) updatedPb.directory(dir.get.toFile)
    env.foreach {
      case (key, "")    => updatedPb.environment().remove(key)
      case (key, value) => updatedPb.environment().put(key, value)
    }
    runAndWaitForPb(updatedPb, label, timeout = timeout, processLogger = processLogger)
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

  def processList(processName: String): Iterator[ProcessHandle] = {
    ProcessHandle.allProcesses().iterator().asScala.filter { process =>
      process.info().command().orElse("").contains(processName)
    }
  }

  def runGit(srcDir: Path, env: Map[String, String] = Map.empty)(args: String*): String = {
    val pb = new GitProcess(srcDir.getParent).createProcessBuilder(srcDir, env.asJava, args: _*)
    runAndWaitForPb(pb)
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
}

object CommonProcess {
  val fsCellPattern: Regex = ".*This workstation belongs to cell '(.+)\\.ms\\.com'.*".r
  private val gitTracePattern: Regex = """^\d\d:\d\d:\d\d\.[\d]+ .*[\w]+: .*$""".r

  def isGitTraceStatement(line: String): Boolean =
    gitTracePattern.findFirstIn(line).isDefined

  def in(workspace: StratoWorkspaceCommon) = new CommonProcess(workspace)

  def preamble: Seq[String] = if (OsSpecific.isWindows) Seq("cmd", "/c") else Seq.empty
}
