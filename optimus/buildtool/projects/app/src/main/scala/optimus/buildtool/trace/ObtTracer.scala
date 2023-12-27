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
package optimus.buildtool.trace

import java.io.PrintWriter
import java.io.StringWriter
import java.time.Instant

import ch.epfl.scala.bsp4j.MessageType
import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.crumbs.Properties
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.CompilationMessage.Error
import optimus.buildtool.artifacts.CompilationMessage.Severity
import optimus.buildtool.artifacts.CompilationMessage.Warning
import optimus.buildtool.config.ScopeId
import optimus.core.needsPlugin
import optimus.platform._
import optimus.platform.annotations.alwaysAutoAsyncArgs

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/** provides out-of-band tracing and supervision of a running OBT build */
object ObtTrace extends ObtTraceListener with ObtTracePluginApi {

  override def startBuild(): Unit = current.startBuild()
  override def reportProgress(message: String, progress: Double = -1.0): Unit = current.reportProgress(message)

  /** records the start of a task and returns a TaskTrace which can be used to record the end */
  def startTask(scopeId: ScopeId, category: CategoryTrace, time: Instant = patch.MilliInstant.now()): TaskTrace =
    current.startTask(scopeId, category, time)

  override def setStat(stat: ObtStat, value: Long): Unit = current.setStat(stat, value)
  override def setProperty[T](p: Properties.EnumeratedKey[T], v: T): Unit = current.setProperty(p, v)
  override def addToStat(stat: ObtStat, value: Long): Unit = current.addToStat(stat, value)
  override def supportsStats: Boolean = current.supportsStats

  def logMsg(msg: String, tpe: MessageType): Unit = current.logMsg(msg, tpe)

  /** may throttle the execution of fn, if a throttle is installed */
  @async @alwaysAutoAsyncArgs def throttleIfLowMem[T](id: ScopeId)(fn: => T): T = needsPlugin
  @async def throttleIfLowMem$NF[T](id: ScopeId)(fn: NodeFunction0[T]): T = current.throttleIfLowMem$NF(id)(fn)

  override def endBuild(success: Boolean): Boolean = current.endBuild(success)
}

private[buildtool] sealed trait ObtStat {
  def key: String = toString // So we can be fancy with string representation
}
private[buildtool] object ObtStats {
  case object Reruns extends ObtStat
  case object ScannedDirectories extends ObtStat
  case object ModifiedDirectories extends ObtStat
  case object RubbishFiles extends ObtStat
  case object RubbishSizeBytes extends ObtStat
  case object InvalidatedSourceFiles extends ObtStat
  case object SourceFiles extends ObtStat
  case object LinesOfCode extends ObtStat
  case object CompiledLinesOfCode extends ObtStat
  case object Cycles extends ObtStat
  case object DurationMs extends ObtStat
  case object AllocatedBytes extends ObtStat
  case object InstalledJarBytes extends ObtStat
  case object Scopes extends ObtStat
  case object PythonScopes extends ObtStat
  case object ScalacScopes extends ObtStat
  case object Compilations extends ObtStat
  case object Success extends ObtStat
  case object Errors extends ObtStat
  case object Warnings extends ObtStat
  case object Info extends ObtStat
  case object InstalledJars extends ObtStat
  case object InstalledWars extends ObtStat
  case object MavenDownloads extends ObtStat

  case object SlowTypechecks extends ObtStat
  case object SlowTypecheckDurationMs extends ObtStat

  case object FileSystemSourceFolder extends ObtStat
  case object GitSourceFolder extends ObtStat

  trait Cache {
    private val cache = this.toString
    trait CacheStat extends ObtStat {
      override def key = s"$cache$this"
    }
    case object Hit extends ObtStat with CacheStat
    case object Miss extends ObtStat with CacheStat
    case object Write extends ObtStat with CacheStat
    case object ReadBytes extends ObtStat with CacheStat
    case object WriteBytes extends ObtStat with CacheStat
    case object ExternalHit extends ObtStat with CacheStat
    case object ExternalMiss extends ObtStat with CacheStat
    case object ExternalWrite extends ObtStat with CacheStat
  }
  case object SilverKing extends Cache
  case object FilesystemStore extends Cache
  case object FilesystemConfig extends Cache
  case object SimpleStore extends Cache

  object Sync {
    sealed trait SyncStat extends ObtStat {
      override def key: String = s"Sync.$toString"
    }
    case object SourceFolders extends SyncStat
    case object Dependencies extends SyncStat
  }
}

trait ObtLogger {

  def logMsg(msg: String, tpe: MessageType): Unit

  private def printError(e: Throwable): String = {
    val s = new StringWriter()
    val w = new PrintWriter(s)
    try e.printStackTrace(w)
    finally w.close()
    new String(s.toString)
  }

  final def debug(msg: String): Unit = logMsg(msg, MessageType.LOG)

  final def info(msg: String): Unit = logMsg(msg, MessageType.INFORMATION)

  final def warn(msg: String): Unit = logMsg(msg, MessageType.WARNING)

  final def warn(msg: String, e: Throwable): Unit = logMsg(s"$msg: ${printError(e)}", MessageType.WARNING)

  final def error(msg: String): Unit = logMsg(msg, MessageType.ERROR)

  final def error(msg: String, e: Throwable): Unit = logMsg(s"$msg: ${printError(e)}", MessageType.ERROR)
}

trait ObtStatsHolder {
  def setStat(stat: ObtStat, value: Long): Unit
  def addToStat(stat: ObtStat, value: Long): Unit
  def supportsStats: Boolean
}

/** a subscriber to ObtTrace messages - must be installed using ObtTrace.withListener (or similar) */
trait ObtTraceListener extends ObtLogger with ObtStatsHolder {

  def startBuild(): Unit

  /* `progress` should be in the range 0.0 to 1.0, or -1.0 to mean unknown */
  def reportProgress(message: String, progress: Double = -1.0): Unit

  def startTask(scopeId: ScopeId, category: CategoryTrace, time: Instant): TaskTrace

  /** traces a task, recording the start and end (with success status based on whether an exception was thrown) */
  def traceTask[T](scopeId: ScopeId, category: CategoryTrace, failureSeverity: Severity = Error)(fn: => T): T =
    withTraceTask(scopeId, category, failureSeverity)(_ => fn)

  @async
  final def traceTask$NF[T](scopeId: ScopeId, category: CategoryTrace, failureSeverity: Severity = Error)(
      fn: NodeFunction0[T]
  ): T = withTraceTask$NF(scopeId, category, failureSeverity)(asNode(_ => fn()))

  def withTraceTask[T](scopeId: ScopeId, category: CategoryTrace, failureSeverity: Severity = Error)(
      fn: TaskTrace => T): T = {
    val t = startTask(scopeId, category, patch.MilliInstant.now)
    val r = Try(fn(t))
    t.completeFromTry(r, failureSeverity)
    r.get
  }

  @async
  final def withTraceTask$NF[T](scopeId: ScopeId, category: CategoryTrace, failureSeverity: Severity = Error)(
      fn: NodeFunction1[TaskTrace, T]
  ): T = {
    val t = startTask(scopeId, category, patch.MilliInstant.now)
    val n = asyncResult(EvaluationContext.cancelScope)(fn(t))
    t.completeFromTry(n.toTry, failureSeverity)
    n.value
  }

  def setProperty[T](p: Properties.EnumeratedKey[T], v: T): Unit

  @async def throttleIfLowMem$NF[T](id: ScopeId)(fn: NodeFunction0[T]): T

  /**
   * Called when the build has ended. May have side-effects.
   * @param success
   *   true iff the build succeeded (i.e. no compilation errors)
   * @return
   *   true iff the listener was satisfied with the build (e.g. no duplicate activity was detected)
   */
  def endBuild(success: Boolean): Boolean
}

/** implementation trait for installing trace listeners via plugin tags */
sealed trait ObtTracePluginApi {
  private case object ObtTracerPluginTagKey extends ForwardingPluginTagKey[ObtTraceListener]
  def current: ObtTraceListener =
    EvaluationContext.findPluginTag(ObtTracerPluginTagKey).getOrElse(LoggingObtTraceListener)
  def isConfigured: Boolean =
    EvaluationContext.findPluginTag(ObtTracerPluginTagKey).isDefined

  @entersGraph def withListener[T](tracer: ObtTraceListener)(f: => T): T =
    AdvancedUtils.givenWithPluginTag(ObtTracerPluginTagKey, tracer, Scenario.empty)(f)

  @async def withListener$NF[T](tracer: ObtTraceListener)(f: NodeFunction0[T]): T =
    AdvancedUtils.givenWithPluginTag(ObtTracerPluginTagKey, tracer, Scenario.empty)(f())

  @entersGraph def withExtraListener[T](tracer: ObtTraceListener)(f: => T): T =
    withListeners(Seq(current, tracer))(f)

  @async def withExtraListener$NF[T](tracer: ObtTraceListener)(f: NodeFunction0[T]): T =
    withListeners$NF(Seq(current, tracer))(f)

  @entersGraph def withListeners[T](tracers: Seq[ObtTraceListener])(f: => T): T =
    AdvancedUtils.givenWithPluginTag(ObtTracerPluginTagKey, MultiwayTraceListener(tracers), Scenario.empty)(f)

  @async def withListeners$NF[T](tracers: Seq[ObtTraceListener])(f: NodeFunction0[T]): T =
    AdvancedUtils.givenWithPluginTag(ObtTracerPluginTagKey, MultiwayTraceListener(tracers), Scenario.empty)(f())
}

/** sends logMsg calls to system logger */
object LoggingObtTraceListener extends DefaultObtTraceListener {
  private val log = getLogger(getClass)

  override def logMsg(msg: String, tpe: MessageType): Unit = {
    tpe match {
      case MessageType.ERROR =>
        log.error(msg)
      case MessageType.WARNING =>
        log.warn(msg)
      case MessageType.INFORMATION =>
        log.info(msg)
      case MessageType.LOG =>
        log.debug(msg)
    }
  }
}

/** default no-op implementations of all listener methods */
trait DefaultObtTraceListener extends ObtTraceListener {
  @async override def throttleIfLowMem$NF[T](id: ScopeId)(fn: NodeFunction0[T]): T = fn()

  override def startBuild(): Unit = ()
  override def reportProgress(message: String, progress: Double = -1.0): Unit = ()

  override def startTask(scopeId: ScopeId, category: CategoryTrace, time: Instant): TaskTrace =
    NoOpTaskTrace

  override def setProperty[T](p: Properties.EnumeratedKey[T], v: T): Unit = {}
  override def setStat(stat: ObtStat, value: Long): Unit = {}
  override def addToStat(stat: ObtStat, value: Long): Unit = {}
  override def supportsStats: Boolean = false

  override def logMsg(msg: String, tpe: MessageType): Unit = ()

  override def endBuild(success: Boolean): Boolean = true
}

object NoOpTaskTrace extends DefaultTaskTrace

object LoggingTaskTrace extends TaskTrace {
  private val log = getLogger(getClass)

  override def reportProgress(message: String, progress: Double): Unit =
    log.debug(s"Progress: $message")
  override def publishMessages(messages: Seq[CompilationMessage]): Unit =
    log.debug(s"Messages: $messages")
  override def end(success: Boolean, errors: Int, warnings: Int, time: Instant): Unit =
    log.debug(s"End: ${if (success) "success" else "failure"}")
  override def setStat(obtStat: ObtStat, value: Long): Unit = {}
  override def addToStat(obtStat: ObtStat, value: Long): Unit = {}
  override def supportsStats: Boolean = false
}

trait DefaultTaskTrace extends TaskTrace {
  override def reportProgress(message: String, progress: Double): Unit = ()
  override def publishMessages(messages: Seq[CompilationMessage]): Unit = ()
  override def end(success: Boolean, errors: Int, warnings: Int, time: Instant): Unit = ()
  override def setStat(obtStat: ObtStat, value: Long): Unit = {}
  override def addToStat(Stat: ObtStat, value: Long): Unit = {}
  override def supportsStats: Boolean = false
}

trait TaskTrace extends ObtStatsHolder {
  def reportProgress(
      message: String = "",
      progress: Double = -1.0 /* in the range 0.0 to 1.0, or -1.0 to mean unknown */
  ): Unit

  def publishMessages(messages: Seq[CompilationMessage]): Unit

  def end(success: Boolean, errors: Int = 0, warnings: Int = 0, time: Instant = patch.MilliInstant.now()): Unit

  def completeFromTry(r: Try[Any], failureSeverity: Severity = Error): Unit = {
    r.failed.foreach(e => publishMessages(Seq(CompilationMessage.message(e, failureSeverity))))
    val errors = if (!r.isSuccess && failureSeverity == Error) 1 else 0
    val warnings = if (!r.isSuccess && failureSeverity == Warning) 1 else 0
    end(success = errors == 0, errors = errors, warnings = warnings)
  }

  def complete(result: Try[Seq[CompilationMessage]], time: Instant = patch.MilliInstant.now()): Unit = result match {
    case Success(messages) =>
      if (messages.nonEmpty) publishMessages(messages)
      end(!messages.exists(_.isError), messages.count(_.isError), messages.count(_.isWarning), time)
    case Failure(t) =>
      publishMessages(Seq(CompilationMessage.error(t)))
      end(success = false, errors = 1, time = time)
  }
}

class AggregateTimingObtTraceListener extends DefaultObtTraceListener {
  private val categoryToTotalMillisMap: mutable.Map[CategoryTrace, Long] = mutable.Map()

  override def startTask(scopeId: ScopeId, category: CategoryTrace, time: Instant): TaskTrace = {
    val startTime = time
    new DefaultTaskTrace {
      override def end(success: Boolean, errors: Int, warnings: Int, time: Instant): Unit = {
        val duration = time.toEpochMilli - startTime.toEpochMilli
        categoryToTotalMillisMap.synchronized {
          val prev = categoryToTotalMillisMap.getOrElse(category, 0L)
          categoryToTotalMillisMap.put(category, prev + duration)
        }
      }
    }
  }

  def categoryToTotalMillis: Map[CategoryTrace, Long] =
    categoryToTotalMillisMap.synchronized(categoryToTotalMillisMap.toMap).withDefaultValue(0L)
}
