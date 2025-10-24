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
package optimus.buildtool.utils.process

import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.crumbs.Properties
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.trace.AsyncCategoryTrace
import optimus.buildtool.trace.CategoryTrace
import optimus.buildtool.trace.LongRunningTraceListener
import optimus.buildtool.trace.ObtTrace
import optimus.graph.NodePromise
import optimus.platform._

import java.time.Instant
import java.time.{Duration => JDuration}
import scala.collection.compat._
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success

class BackgroundProcess private[process] (
    scopeId: ScopeId,
    appName: String,
    category: Option[AsyncCategoryTrace],
    cmdLine: Seq[String],
    envVariablesToAdd: Map[String, String],
    envVariablesToClean: Seq[String],
    envVariablesToRetain: Option[Set[String]],
    workingDir: Option[Directory],
    sendCrumbs: Boolean,
    _logFile: FileAsset,
    val errFile: Option[FileAsset],
    lastLogLines: Int
) extends ExternalProcess {
  import BackgroundProcess.log

  private val logPrefix = if (scopeId == RootScopeId) s"[$appName]" else s"[$scopeId:$appName]"

  override def logFile: Option[FileAsset] = Some(_logFile)

  @async override def startWithRetry(maxRetries: Int, msDelay: Long, showWarningsAfter: Int = 0): Unit =
    _startWithRetry(maxRetries, msDelay, showWarningsAfter, 0)

  // noinspection NoTailRecursionAnnotation
  @async private def _startWithRetry(
      maxRetries: Int,
      msDelay: Long,
      showWarningsAfter: Int,
      previousTryCount: Int): Unit = {
    val tryCount = previousTryCount + 1
    val prettyCmd: String = cmdLine.mkString(" ")
    val start = Instant.now()

    def sendCrumb(exitCode: Int, start: Instant, taskLog: Option[String] = None): Unit = category.foreach { c =>
      val logProps: Seq[Properties.Elem[_]] = taskLog match {
        case Some(value) =>
          Seq(Properties.obtTaskLog -> value)
        case None => Nil
      }
      val props: Seq[Properties.Elem[_]] = Seq(
        Properties.user -> sys.props.getOrElse("user.name", "unknown"),
        Properties.obtTaskCmd -> prettyCmd,
        Properties.obtTaskExitCode -> exitCode,
        Properties.obtTaskTryCount -> tryCount,
        Properties.obtTaskMaxTryCount -> maxRetries
      ) ++ logProps
      val state = if (exitCode == 0) "successful" else "failure"
      BackgroundProcess.sendCrumb(c, scopeId, start = start, state = state, props)
    }

    val result: NodeResult[Int] = asyncResult {
      category match {
        case Some(c) =>
          ObtTrace.traceTask(scopeId, c) {
            val exitCode = launchProcess()
            if (sendCrumbs) sendCrumb(exitCode, start)
            exitCode
          }
        case None =>
          val exitCode = launchProcess()
          if (sendCrumbs) sendCrumb(exitCode, start)
          exitCode
      }
    }

    result match {
      case NodeSuccess(0) => // do nothing
      case _ =>
        val (exitCode, errorStr) = result match {
          case NodeSuccess(exitCode) => (exitCode, s"(exit code: $exitCode)")
          case NodeFailure(t)        => (-1, s"(exception: ${t.toString})") // 0 -> pass, !0 -> fail
        }

        if (tryCount <= maxRetries) {
          def showRetryWarning(exception: Option[Throwable]): Unit = {
            val retryMsg =
              s"$logPrefix Process failed $errorStr on try $tryCount/${maxRetries + 1} for command: $prettyCmd"
            exception match {
              case Some(t) => if (tryCount > showWarningsAfter) log.warn(retryMsg, t) else log.debug(retryMsg, t)
              case None    => if (tryCount > showWarningsAfter) log.warn(retryMsg) else log.debug(retryMsg)
            }
          }

          result match {
            case NodeSuccess(_) => showRetryWarning(None)
            case NodeFailure(t) => showRetryWarning(Some(t))
          }
          delay(msDelay * tryCount)
          _startWithRetry(
            maxRetries = maxRetries,
            msDelay = msDelay,
            previousTryCount = tryCount,
            showWarningsAfter = showWarningsAfter
          )
        } else {
          val retryStr = if (maxRetries > 0) s" after ${maxRetries + 1} tries" else ""
          val outLines = ExternalProcess.lastLogLines(_logFile, lastLogLines)
          val errLines = errFile.toSeq.flatMap(f => ExternalProcess.lastLogLines(f, lastLogLines))
          val errorMsg = errFile match {
            case Some(e) if e.existsUnsafe && errLines.nonEmpty =>
              val msgs =
                Seq(s"$logPrefix Process failed$retryStr $errorStr", s"Command: $prettyCmd", "Error log (tail):") ++
                  errLines.map(s => s"\t$s") :+
                  s"Full output log: ${_logFile.pathString}" :+
                  s"Full error log: ${e.pathString}"
              msgs.mkString("\n")
            case _ if _logFile.existsUnsafe && outLines.nonEmpty =>
              val msgs =
                Seq(s"$logPrefix Process failed$retryStr $errorStr", s"Command: $prettyCmd", "Process log (tail):") ++
                  outLines.map(s => s"\t$s") :+
                  s"Full log: ${_logFile.pathString}"
              msgs.mkString("\n")
            case _ =>
              s"$logPrefix Process failed without logging$retryStr $errorStr\nCommand: $prettyCmd"
          }

          // avoid duplicated msgs, when sendCrumbs = true we already sent msg no matter it's succeed or failed
          if (!sendCrumbs) sendCrumb(exitCode, start, Some(errorMsg))

          result match {
            case NodeSuccess(exitCode) =>
              throw new ProcessExitException(scopeId, appName, exitCode, errorMsg)
            case NodeFailure(t) =>
              throw new ProcessFailureException(scopeId, appName, errorMsg, t)
          }
        }
    }
  }

  @async protected def launchProcess(): Int = {
    import scala.compat.java8.FunctionConverters._
    val promise = NodePromise[Int]()

    val pb = new ProcessBuilder(cmdLine: _*)
    val builder = errFile match {
      case Some(err) =>
        pb
          .redirectError(ProcessBuilder.Redirect.appendTo(err.path.toFile))
          .redirectOutput(ProcessBuilder.Redirect.appendTo(_logFile.path.toFile))
      case None =>
        pb
          .redirectErrorStream(true)
          .redirectOutput(ProcessBuilder.Redirect.appendTo(_logFile.path.toFile))
    }

    workingDir.foreach(dir => builder.directory(dir.path.toFile))

    val env = builder.environment()
    envVariablesToRetain.foreach { retain =>
      env.asScala.keySet.toSeq.foreach { key => if (!retain(key)) env.remove(key) }
    }
    envVariablesToAdd.foreach { case (k, v) => env.put(k, v) }
    envVariablesToClean.foreach(v => env.remove(v))

    log.debug(s"Running command: $cmdLine")
    log.trace(s"Env: ${env.asScala.mkString("[\n\t", "\n\t", "\n]")}")
    val process = builder.start()

    process
      .onExit()
      .handle[Boolean](asJavaBiFunction((p: Process, exception) =>
        promise.complete {
          if (exception ne null) Failure(exception)
          else Success(p.exitValue())
        }))

    promise.await
  }
}

object BackgroundProcess {
  private val log = getLogger(this)

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
