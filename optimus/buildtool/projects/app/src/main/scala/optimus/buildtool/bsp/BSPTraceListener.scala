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
package optimus.buildtool.bsp

import java.time.Instant
import java.util
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import ch.epfl.scala.bsp4j.BuildClient
import ch.epfl.scala.bsp4j.BuildTargetIdentifier
import ch.epfl.scala.bsp4j.CompileReport
import ch.epfl.scala.bsp4j.CompileTask
import ch.epfl.scala.bsp4j.Diagnostic
import ch.epfl.scala.bsp4j.DiagnosticSeverity
import ch.epfl.scala.bsp4j.DidChangeBuildTarget
import ch.epfl.scala.bsp4j.LogMessageParams
import ch.epfl.scala.bsp4j.MessageType
import ch.epfl.scala.bsp4j.Position
import ch.epfl.scala.bsp4j.PublishDiagnosticsParams
import ch.epfl.scala.bsp4j.Range
import ch.epfl.scala.bsp4j.ShowMessageParams
import ch.epfl.scala.bsp4j.StatusCode
import ch.epfl.scala.bsp4j.TaskDataKind
import ch.epfl.scala.bsp4j.TaskFinishParams
import ch.epfl.scala.bsp4j.TaskId
import ch.epfl.scala.bsp4j.TaskProgressParams
import ch.epfl.scala.bsp4j.TaskStartParams
import ch.epfl.scala.bsp4j.TextDocumentIdentifier
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.MessagePosition
import optimus.buildtool.artifacts.MessagesArtifact
import optimus.buildtool.artifacts.Severity
import optimus.buildtool.bsp.BuildServerProtocolService.send
import optimus.buildtool.config.ModuleId
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.trace.CategoryTrace
import optimus.buildtool.trace.DefaultObtTraceListener
import optimus.buildtool.trace.NoOpTaskTrace
import optimus.buildtool.trace.ObtLogger
import optimus.buildtool.trace.ObtStat
import optimus.buildtool.trace.TaskTrace
import optimus.buildtool.trace.TraceFilter
import optimus.buildtool.utils.FileDiff
import optimus.utils.CollectionUtils._

import java.util.Timer
import java.util.TimerTask
import scala.jdk.CollectionConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.Seq
import scala.util.control.NonFatal
import scala.collection.compat._

object BSPSessionTraceListener {
  private val heartbeat = new Timer("BSP Heartbeat", true)
}
private[bsp] class BSPSessionTraceListener(
    val buildClient: BuildClient,
    val converter: BspScopeIdsConverter,
    val workspaceSourceRoot: Directory,
    /** None means include all categories */
    val traceFilter: Option[TraceFilter]
) extends ObtLogger {

  import BuildServerProtocolService.send

  private val sessionHeartbeat = {
    val task = new TimerTask {
      override def run(): Unit = {
        val hb = new ShowMessageParams(MessageType.LOG, "HEARTBEAT")
        hb.setTask(new TaskId("HEARTBEAT"))
        buildClient.onBuildShowMessage(hb)
      }
    }
    BSPSessionTraceListener.heartbeat.schedule(task, 60000, 60000)
    task
  }

  private var currentListener: Option[BSPTraceListener] = None

  def forBuildId(id: String): BSPTraceListener = forBuildId(Some(id))

  def forSync(): BSPTraceListener = forBuildId(None)

  def forBuildId(id: Option[String]): BSPTraceListener = {
    val previousDiagnostics = currentListener.map(_.publishedFileMessages.toMap).getOrElse(Map.empty)
    val listener = new BSPTraceListener(this, id, previousDiagnostics, workspaceSourceRoot)
    currentListener = Some(listener)
    listener
  }

  override def logMsg(msg: String, tpe: MessageType): Unit = {
    val params = new LogMessageParams(tpe, msg)
    send("onBuildLogMessage", params)
    buildClient.onBuildLogMessage(params)
  }

  def cancelHeartbeat(): Unit = sessionHeartbeat.cancel()
}

private[bsp] final case class FileKey(scopeId: ScopeId, taskCategory: CategoryTrace, filePath: String)

/** sends task traces, progress updates, log messages and compiler diagnostics over the BSP connection */
private[bsp] final class BSPTraceListener(
    sessionListener: BSPSessionTraceListener,
    buildId: Option[String],
    previousFileMessages: Map[FileKey, Seq[(MessagePosition, CompilationMessage)]],
    workspaceSourceRoot: Directory
) extends DefaultObtTraceListener {

  private var modifiedFiles: Option[FileDiff] = None

  import sessionListener._

  private val log = msjava.slf4jutils.scalalog.getLogger(this)

  // prevent taskId collisions in cases where we run the same task more than once
  private val idSuffix = new AtomicInteger(0)
  private val ids = new ConcurrentHashMap[String, TaskId]()

  private var targetMappings = Map.empty[ScopeId, BuildTargetIdentifier]
  private var targetModules = Set.empty[ModuleId]

  // used to avoid republishing all messages for targets at the end of the build which already published during the build
  private val tasksThatPublishedDiagnostics = Collections.synchronizedSet(new util.HashSet[(ScopeId, CategoryTrace)]())

  // used to avoid publishing messages for a file in this build if we published identical messages for the same file in the last build
  private[bsp] val publishedFileMessages = TrieMap[FileKey, Seq[(MessagePosition, CompilationMessage)]]()

  private def target(scopeId: ScopeId): BuildTargetIdentifier =
    targetMappings.getOrElse(scopeId, new BuildTargetIdentifier(converter.toURI(scopeId)))

  def setTargetMappings(targetMappings: Map[ScopeId, BuildTargetIdentifier]): Unit = {
    this.targetMappings = targetMappings
    this.targetModules = targetMappings.keySet.map(_.fullModule) + ScopeId.RootScopeId.fullModule
  }

  private def shouldTrace(category: CategoryTrace): Boolean = traceFilter.forall(_.include(category))

  private def shouldPublish(scope: ScopeId, category: CategoryTrace, pos: MessagePosition): Boolean = {
    traceFilter.forall { f =>
      val res = f.publish(category)
      def pathIsModified = modifiedFiles.exists(_.contains(pos.filepath))
      // We have some MessagePositions which identify a file but not a line. In that case pos.startLine == -1.
      def lineIsModified = modifiedFiles.exists(_.contains(pos.filepath, pos.startLine, pos.endLine))
      (res == TraceFilter.Include) ||
      (res == TraceFilter.InTargetModule && targetModules.contains(scope.fullModule)) ||
      (res == TraceFilter.WhenFileModified && pathIsModified) ||
      (res == TraceFilter.WhenLineModified && lineIsModified)
    }
  }

  def setModifiedFiles(files: Option[FileDiff]): Unit = modifiedFiles = files

  override def reportProgress(message: String, progress: Double = -1.0): Unit = {
    buildId.foreach { id =>
      val res = new TaskProgressParams(new TaskId(id))
      res.setMessage(message)
      res.setEventTime(System.currentTimeMillis())
      if (progress >= 0.0) {
        // n.b. this progress doesn't actually get displayed by IntelliJ yet - maybe it will one day
        res.setProgress((progress * 1000L).toLong)
        res.setTotal(1000L)
      }
      send("onBuildTaskProgress", res)
      buildClient.onBuildTaskProgress(res)
    }
  }

  override def startTask(
      scopeId: ScopeId,
      category: CategoryTrace,
      time: Instant = patch.MilliInstant.now()): TaskTrace = {
    if (shouldTrace(category)) {
      val startTimeMillis = time.toEpochMilli
      val targetIdentifier = target(scopeId)
      val taskName = task(scopeId, category)
      val taskId = newTaskId(taskName)
      buildId.foreach(id => taskId.setParents(Collections.singletonList(id)))

      val res = new TaskStartParams(taskId)
      res.setMessage(taskName)
      res.setEventTime(startTimeMillis)
      res.setDataKind(TaskDataKind.COMPILE_TASK)
      res.setData(new CompileTask(targetIdentifier))
      send("onBuildTaskStart", res)
      buildClient.onBuildTaskStart(res)

      new TaskTrace {
        override def setStat(obtStat: ObtStat, value: Long): Unit = {}
        override def addToStat(obtStat: ObtStat, value: Long): Unit = {}
        override def supportsStats: Boolean = false
        override def reportProgress(message: String, progress: Double): Unit = {
          val res = new TaskProgressParams(taskId)
          val progressStr = if (progress >= 0.0) {
            // n.b. this progress doesn't actually get displayed by IntelliJ yet - maybe it will one day
            res.setProgress((progress * 1000L).toLong)
            res.setTotal(1000L)
            f" ($progress%.1f%%)"
          } else ""
          res.setMessage(s"$taskName - $message$progressStr ")
          res.setEventTime(System.currentTimeMillis())
          res.setDataKind(TaskDataKind.COMPILE_TASK)
          res.setData(new CompileTask(targetIdentifier))
          send("onBuildTaskProgress", res)
          buildClient.onBuildTaskProgress(res)
        }

        override def publishMessages(messages: Seq[CompilationMessage]): Unit = {
          reportDiagnostics(scopeId, category, messages)
        }

        override def end(success: Boolean, errors: Int, warnings: Int, time: Instant): Unit = {
          val statusCode = if (success) StatusCode.OK else StatusCode.ERROR
          val endTimeMillis = time.toEpochMilli
          val res = new TaskFinishParams(taskId, statusCode)
          res.setMessage(taskName)
          res.setEventTime(endTimeMillis)
          res.setDataKind(TaskDataKind.COMPILE_REPORT)
          val report = new CompileReport(targetIdentifier, errors, warnings)
          buildId.foreach(id => report.setOriginId(id))
          report.setTime(endTimeMillis - startTimeMillis)
          res.setData(report)
          send("onBuildTaskFinish", res)
          buildClient.onBuildTaskFinish(res)
        }
      }
    } else NoOpTaskTrace
  }

  private def asMessageType(input: CompilationMessage.Severity): MessageType = input match {
    case CompilationMessage.Error   => MessageType.ERROR
    case CompilationMessage.Warning => MessageType.WARNING
    case CompilationMessage.Info    => MessageType.INFORMATION
  }

  private def asSeverity(input: CompilationMessage.Severity): DiagnosticSeverity = input match {
    case CompilationMessage.Error   => DiagnosticSeverity.ERROR
    case CompilationMessage.Warning => DiagnosticSeverity.WARNING
    case CompilationMessage.Info    => DiagnosticSeverity.INFORMATION
  }

  // Note that the spec for Diagnostic is inherited by BSP from the LSP spec
  // (see https://microsoft.github.io/language-server-protocol/specification#textDocument_publishDiagnostics)
  private def asDiagnostic(
      position: MessagePosition,
      msg: String,
      severity: CompilationMessage.Severity,
      taskCategory: CategoryTrace
  ): Diagnostic = {
    // lines and columns are zero-based in LSP
    val startPos = new Position(position.startLine - 1, position.startColumn - 1)
    val endPos = new Position(position.endLine - 1, position.endColumn - 1)
    val range = new Range(startPos, endPos)
    val diagnostic = new Diagnostic(range, msg)
    diagnostic.setSeverity(asSeverity(severity))
    // the LSP spec says that "source" is "a human-readable string describing the source of this diagnostic, e.g.
    // 'typescript' or 'super lint'", so the taskCategory (e.g. "java" or "scala") seems appropriate
    diagnostic.setSource(taskCategory.name)
    diagnostic
  }

  private def uri(path: String) =
    try workspaceSourceRoot.path.resolve(path).toUri.toString
    catch {
      case NonFatal(_) =>
        "file://no-file"
    }

  private def reportDiagnostics(
      scopeId: ScopeId,
      taskCategory: CategoryTrace,
      allMessages: Seq[CompilationMessage]
  ): Unit = if (!tasksThatPublishedDiagnostics.contains((scopeId, taskCategory))) {

    val taskName = task(scopeId, taskCategory)

    // collect diagnostics which have positions and directly report any without positions
    val groupedMessages = allMessages.flatMap { m =>
      val updatedMessage =
        if (m.isNew && m.pos.exists(p => modifiedFiles.exists(_.contains(p.filepath))))
          m.copy(severity = Severity.Error)
        else m
      updatedMessage.pos match {
        case Some(pos) if updatedMessage.severity == Severity.Error || shouldPublish(scopeId, taskCategory, pos) =>
          Some((pos.filepath, (pos, updatedMessage)))
        case Some(_) =>
          None // do not publish
        case None =>
          sendTaskDiagnostics(taskName, updatedMessage)
          None
      }
    }.toGroupedMap

    // convert the position messages to diagnostics by source file and publish them if they've changed since the
    // last build
    val buildTarget = target(scopeId)
    groupedMessages.foreach { case (file, positionMessages) =>
      val fileKey = FileKey(scopeId, taskCategory, file)
      // did we publish the same messages for this file in the last build?
      if (!previousFileMessages.get(fileKey).contains(positionMessages)) {
        sendFileDiagnostics(buildTarget, taskName, taskCategory, file, positionMessages.to(Seq))
      } else {
        log.trace(s"Skipping republish of matching diagnostics for $file")
      }
      publishedFileMessages.put(fileKey, positionMessages.to(Seq))
    }

    tasksThatPublishedDiagnostics.add((scopeId, taskCategory))
  }

  private def sendTaskDiagnostics(taskName: String, message: CompilationMessage): Unit = {
    val params = new ShowMessageParams(asMessageType(message.severity), message.msg)
    buildId.foreach(params.setOriginId)
    params.setTask(taskId(taskName))
    send("onBuildShowMessage", params)
    buildClient.onBuildShowMessage(params)
  }

  private def sendFileDiagnostics(
      buildTarget: BuildTargetIdentifier,
      taskName: String,
      taskCategory: CategoryTrace,
      filePath: String,
      positionMessages: Seq[(MessagePosition, CompilationMessage)]
  ): Unit = {
    val docId = new TextDocumentIdentifier(uri(filePath))
    val diagnostics = positionMessages.map { case (pos, m) =>
      asDiagnostic(pos, m.msg, m.severity, taskCategory)
    }
    val params = new PublishDiagnosticsParams(docId, buildTarget, diagnostics.asJava, true)
    // Note we're slightly violating the BSP spec here, in that we're supplying taskId
    // as the originId rather than the buildId. This is so that we can get diagnostics nested
    // within the tasks that generated them in the IntelliJ Build view.
    params.setOriginId(taskId(taskName).getId)
    send("onBuildPublishDiagnostics", params)
    buildClient.onBuildPublishDiagnostics(params)
  }

  private def newTaskId(taskName: String): TaskId = ids.compute(taskName, (_, _) => createTaskId(taskName))
  private def taskId(taskName: String): TaskId = ids.computeIfAbsent(taskName, _ => createTaskId(taskName))
  private def createTaskId(taskName: String): TaskId = new TaskId(s"$taskName-${idSuffix.getAndIncrement()}")

  def ensureDiagnosticsReported(msgArtifacts: Seq[MessagesArtifact]): Unit = {
    msgArtifacts.foreach { a =>
      reportDiagnostics(a.id.scopeId, a.taskCategory, a.messages)
    }
    // for any diagnostics published in the previous build but not this one, send empty diagnostics to ensure
    // they're removed by the client
    val removedFileMessages = previousFileMessages -- publishedFileMessages.keySet
    removedFileMessages.foreach { case (FileKey(scopeId, taskCategory, filePath), _) =>
      val buildTarget = target(scopeId)
      val taskName = task(scopeId, taskCategory)
      sendFileDiagnostics(buildTarget, taskName, taskCategory, filePath, Seq.empty)

    }
  }

  def buildTargetsChanged(): Unit = {
    // no point sending a list of changed targets, since IntelliJ doesn't read it (in 2019.2)
    val change = new DidChangeBuildTarget(Collections.emptyList())
    send("onBuildTargetDidChange", change)
    buildClient.onBuildTargetDidChange(change)
  }

  override def logMsg(msg: String, tpe: MessageType): Unit = sessionListener.logMsg(msg, tpe)

  private[this] def task(scopeId: ScopeId, taskCategory: CategoryTrace) =
    if (scopeId == RootScopeId) taskCategory.name else s"$scopeId:${taskCategory.name}"
}
