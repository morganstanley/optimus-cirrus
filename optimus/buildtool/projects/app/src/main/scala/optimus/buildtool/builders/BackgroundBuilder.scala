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
package optimus.buildtool.builders

import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.crumbs.Properties
import optimus.buildtool.artifacts.PathingArtifact
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.trace.AsyncCategoryTrace
import optimus.buildtool.trace.CategoryTrace
import optimus.buildtool.trace.LongRunningTraceListener
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.Utils
import optimus.exceptions.RTExceptionTrait
import optimus.graph.NodePromise
import optimus.platform._
import optimus.platform.util.Log
import optimus.platform.util.ProcessEnvironment.moduleArguments

import java.io.File
import java.nio.charset.StandardCharsets
import java.time.{Instant, Duration => JDuration}
import scala.annotation.tailrec
import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal

final class BackgroundCmdException(message: String, category: AsyncCategoryTrace, cause: Throwable)
    extends Exception(s"${category.name}: $message", cause)
    with RTExceptionTrait {
  def this(message: String, category: AsyncCategoryTrace) = this(message, category, null)
}

trait BackgroundBuilder {
  @async def build(scopeId: ScopeId, category: AsyncCategoryTrace, lastLogLines: Int): Unit
}

class BackgroundProcessBuilder protected (
    id: BackgroundId,
    protected val logFile: FileAsset,
    cmds: Seq[String],
    envVariablesToAdd: Map[String, String],
    envVariablesToClean: Seq[String],
    workingDir: Option[Directory],
    useCrumbs: Boolean
) extends BackgroundBuilder
    with Log {

  @async override def build(scopeId: ScopeId, category: AsyncCategoryTrace, lastLogLines: Int): Unit =
    buildWithRetry(scopeId, category)(maxRetry = 0, msDelay = 0, lastLogLines = lastLogLines)

  @async protected def launchProcess(): Int = {
    import scala.compat.java8.FunctionConverters._
    val promise = NodePromise[Int]()

    val builder = new ProcessBuilder(cmds: _*)
      .redirectErrorStream(true)
      .redirectOutput(ProcessBuilder.Redirect.appendTo(logFile.path.toFile))

    workingDir.foreach(dir => builder.directory(dir.path.toFile))

    val env = builder.environment()
    envVariablesToAdd.foreach { case (k, v) => env.put(k, v) }
    envVariablesToClean.foreach(v => env.remove(v))

    log.debug(s"Running command: $cmds")
    log.trace(s"Env: ${env.asScala.mkString("[\n\t", "\n\t", "\n]")}")
    val process = builder.start()

    process
      .onExit()
      .handle[Boolean](asJavaBiFunction((p: Process, exception) =>
        promise.complete {
          if (exception ne null) Failure(exception)
          else Success(p.exitValue())
        }))

    asyncGet(promise.node)
  }

  @async def buildWithRetry(
      scopeId: ScopeId,
      category: AsyncCategoryTrace,
      sendCrumbs: Boolean = false,
      defaultProps: Seq[Properties.Elem[_]] = Nil)(
      maxRetry: Int,
      msDelay: Long,
      lastLogLines: Int,
      previousTryCount: Int = 0,
      showWarningsAfter: Int = 0
  ): Unit = {
    val tryCount = previousTryCount + 1
    val prettyCmd: String = cmds.mkString(" ")
    val start = Instant.now()

    def sendCrumb(exitCode: Int, start: Instant, taskLog: Option[String] = None): Unit = if (useCrumbs) {
      val logProps: Seq[Properties.Elem[_]] = taskLog match {
        case Some(value) =>
          Seq(Properties.obtTaskLog -> value)
        case None => Nil
      }
      val props: Seq[Properties.Elem[_]] = defaultProps ++ Seq(
        Properties.user -> sys.props.getOrElse("user.name", "unknown"),
        Properties.obtTaskCmd -> prettyCmd,
        Properties.obtTaskExitCode -> exitCode,
        Properties.obtTaskTryCount -> tryCount,
        Properties.obtTaskMaxTryCount -> maxRetry
      ) ++ logProps
      val state = if (exitCode == 0) "successful" else "failure"
      BackgroundProcessBuilder.sendCrumb(category, scopeId, start = start, state = state, props)
    }

    val result: NodeResult[Int] = asyncResult {
      ObtTrace.traceTask(scopeId, category) {
        val exitCode = launchProcess()
        if (sendCrumbs) sendCrumb(exitCode, start)
        exitCode
      }
    }

    result match {
      case NodeSuccess(0) => // do nothing
      case _ =>
        val (exitCode, exitCodeStr) = result match {
          case NodeSuccess(exitCode) => (exitCode, s"(exit code: $exitCode) ")
          case _                     => (1, "") // 0 -> pass, 1 -> fail
        }
        val errorMsg = s"Process failed $exitCodeStr for command: $prettyCmd"

        def showRetryWarning(exception: Option[Throwable]): Unit = {
          val warningMsg = s"Retrying $tryCount/$maxRetry - $errorMsg"
          exception match {
            case Some(t) => if (tryCount > showWarningsAfter) log.warn(warningMsg, t) else log.debug(warningMsg, t)
            case None    => if (tryCount > showWarningsAfter) log.warn(warningMsg) else log.debug(warningMsg)
          }
        }

        if (tryCount <= maxRetry) {
          result match {
            case NodeSuccess(_) => showRetryWarning(None)
            case NodeFailure(t) => showRetryWarning(Some(t))
          }
          delay(msDelay * tryCount)
          buildWithRetry(scopeId, category, sendCrumbs, defaultProps)(
            maxRetry = maxRetry,
            msDelay = msDelay,
            lastLogLines = lastLogLines,
            previousTryCount = tryCount
          )
        } else {
          val errorMsg =
            if (logFile.exists) {
              val msgs = BackgroundProcessBuilder.lastLogLines(logFile, lastLogLines) :+
                s"${id.description} failed, check its logs at ${logFile.pathString}. Command: $prettyCmd"
              msgs.mkString("\n", "\n", "")
            } else s"Process failed without logging $exitCodeStr"

          // avoid duplicated msgs, when sendCrumbs = true we already sent msg no matter it's succeed or failed
          if (!sendCrumbs) sendCrumb(exitCode, start, Some(errorMsg))

          result match {
            case NodeSuccess(_) =>
              throw new BackgroundCmdException(errorMsg, category)
            case NodeFailure(t) =>
              throw new BackgroundCmdException(errorMsg, category, t)
          }
        }
    }

  }

}

object BackgroundProcessBuilder {
  private val log = getLogger(this)
  val id: BackgroundCmdId = BackgroundCmdId("background")

  def onDemand(logFile: FileAsset, cmdLine: Seq[String], useCrumbs: Boolean): BackgroundProcessBuilder =
    apply(id, logFile, cmdLine.toIndexedSeq, useCrumbs = useCrumbs)

  def onDemand(logDir: Directory, cmdLine: String, useCrumbs: Boolean): BackgroundProcessBuilder =
    apply(id, id.logFile(logDir), cmdLine.split("\\s+").toIndexedSeq, useCrumbs = useCrumbs)

  def apply(
      id: BackgroundId,
      logFile: FileAsset,
      cmdLine: Seq[String],
      envVariablesToAdd: Map[String, String] = Map.empty,
      envVariablesToClean: Seq[String] = Nil,
      workingDir: Option[Directory] = None,
      useCrumbs: Boolean
  ): BackgroundProcessBuilder =
    new BackgroundProcessBuilder(id, logFile, cmdLine, envVariablesToAdd, envVariablesToClean, workingDir, useCrumbs)

  def lastLogLines(outputFile: FileAsset, n: Int): Seq[String] = {
    import org.apache.commons.io.input.ReversedLinesFileReader
    val rlfReader = ReversedLinesFileReader
      .builder()
      .setFile(outputFile.path.toFile)
      .setCharset(StandardCharsets.UTF_8)
      .get()
    val seqBuilder = IndexedSeq.newBuilder[String]

    @tailrec
    def readLogFile(totalLines: Int): Seq[String] = {
      val loadedLine = rlfReader.readLine()
      if (totalLines < 1 || loadedLine == null) seqBuilder.result().to(Seq)
      else {
        seqBuilder += loadedLine
        readLogFile(totalLines - 1)
      }
    }

    try {
      readLogFile(n)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Unexpected error when load log file: ${outputFile.pathString}", e)
        Seq.empty
    } finally {
      rlfReader.close()
    }
  }

  def java(
      id: BackgroundId,
      logFile: FileAsset,
      classpathArtifacts: Seq[PathingArtifact],
      javaAgentArtifacts: Seq[PathingArtifact],
      javaOpts: Seq[String],
      mainClass: String,
      mainClassArgs: Seq[String],
      useCrumbs: Boolean): BackgroundProcessBuilder = {
    val classpathOpt = Seq("-cp", classpathArtifacts.map(_.pathString).mkString(File.pathSeparator))
    val agentOpts = javaAgentArtifacts.map(a => "-javaagent:" + a.pathString)
    val cmd: Seq[String] =
      Seq(Utils.javaExecutable) ++ moduleArguments ++ javaOpts ++ agentOpts ++ classpathOpt ++ Seq(
        mainClass) ++ mainClassArgs
    apply(id, logFile, cmd, useCrumbs = useCrumbs)
  }

  private def sendCrumb(
      category: CategoryTrace,
      scopeId: ScopeId,
      start: Instant,
      state: String,
      extraProps: Seq[Properties.Elem[_]],
      end: Instant = Instant.now()): Unit =
    LongRunningTraceListener.sendCrumbForTask(
      category = category,
      scopeId = scopeId,
      start = start,
      duration = JDuration.between(start, end),
      state = state,
      customElems = Seq(Properties.obtTaskEnd -> end) ++ extraProps: _*
    )
}
