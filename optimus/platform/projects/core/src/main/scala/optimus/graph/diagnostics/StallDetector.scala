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
package optimus.graph.diagnostics

import msjava.slf4jutils.scalalog._
import optimus.breadcrumbs.Breadcrumbs
import optimus.graph.SchedulerDiagnosticUtils.ThreadSnapshot
import optimus.graph.SchedulerDiagnosticUtils._
import optimus.graph._
import optimus.graph.diagnostics.StallDetector.ExceptionLogger
import optimus.platform.util.PrettyStringBuilder
import optimus.platform.util.ThreadDumper.ThreadInfos
import org.apache.commons.io.FileUtils

import java.nio.charset.Charset
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

private[optimus] object StallDetector {

  private val maxHistoLength = 200 // lines

  val extraCheckInterval = 200 // ms

  // Regexes for extra threads to include in the thread snapshot comparison
  val permittedThreadNames: List[String] = List("main", "Test worker.*")

  val StandardReturnCode = 70
  val WithAdaptedReturnCode = 131

  val log: Logger = getLogger(getClass)

  val instance: StallDetector = {
    if (Settings.detectStalls) {
      log.info(
        s"Stall detection enabled. Creating the stall detector, interval=${Settings.detectStallInterval}s, detectStallTimeout=${Settings.detectStallTimeout}s, detectStallAdaptedTimeout=${Settings.detectStallAdaptedTimeout}s, ")
      val detector = new DefaultStallDetector(
        Settings.detectStallInterval,
        Settings.detectStallTimeout,
        Settings.detectStallAdaptedTimeout,
        Settings.detectStallAdapted,
        permittedThreadNames,
        LocalStallExceptionLogger
      )
      log.info(s"Stall detector created: $detector")
      detector.ensureStarted()
      detector
    } else {
      log.info("Stall detection disabled")
      new NoOpStallDetector
    }
  }

  def ensureInitialised(): StallDetector = StallDetector.instance

  def withStallListener[T](listener: StallDetectedError => Unit)(f: => T): T = instance.withStallListener(listener)(f)

  final class StallDetectedError(interval: Long, fullTimeout: Long, adaptedTimeout: Long, stallTime: Long)
      extends Error {
    private lazy val message: String = {
      log.info("Creating stall error message")

      val sb = new PrettyStringBuilder()
      sb.appendln("")
      sb.appendln("A stall has been detected on the graph")
      sb.appendln(s"No movement has been detected on any scheduler's work or wait queues for ${stallTime}s.")
      sb.appendln(s"StallDetectionConfig(i=$interval,fullT=$fullTimeout,adaptedT=$adaptedTimeout)")
      sb.appendln("Printing diagnostic information:")
      sb.appendDivider()

      log.info("Getting graph state")
      sb.appendln(GraphDiagnostics.getGraphState)
      sb.appendDivider()

      log.info("Getting dependency tracker state")
      sb.appendln(GraphDiagnostics.getDependencyTrackerWorkInfo(includeUTracks = false))
      sb.appendDivider()

      log.info("Dumping runtime information")
      OSInformation.dumpRuntimeInfo(sb.underlying)
      sb.appendDivider()

      if (OSInformation.isUnix) {
        log.info("Dumping process information")
        OSInformation.dumpPidInfo(sb.underlying, OSInformation.getPID, Some(maxHistoLength))
      }

      sb.toString
    }

    override def getMessage: String = message
  }

  trait ExceptionLogger {
    protected def fileDir: String = Settings.detectStallLogDir

    protected def writeExceptionToFile(fileDir: String, e: Throwable): Unit = {
      val filePath = Paths.get(fileDir).resolve(s"stallLog-${patch.MilliInstant.now.toEpochMilli}.log")
      try {
        FileUtils.write(filePath.toFile, e.getMessage, Charset.defaultCharset())
        log.info(s"Written stall log file to: $filePath")
        sendCrumb(filePath)
        // println so that this goes to Test Runner console in unit tests making it easy to see why the test failed
        println(s"Stall was detected and process will be killed. Stall log at: $filePath")
      } catch {
        case NonFatal(e) =>
          log.error(s"Encountered exception writing log file to dest: $filePath, exception: $e")
      }
    }

    private def sendCrumb(filePath: Path): Unit = {
      val crumbOffered = GraphEvents.publishStallDetectedCrumb(filePath.toString)
      log.info(s"Stall crumb was offered with result: $crumbOffered")
      if (crumbOffered) {
        // We need to ensure the crumb has been sent (if it was successfully offered) before we kill the process
        Breadcrumbs.flush()
      }
    }

    private[diagnostics] def shouldWriteFile(fileDir: String, e: Throwable): Boolean

    def logException(e: Throwable): Unit
  }

  object DistStallExceptionLogger extends ExceptionLogger {
    def logException(e: Throwable): Unit = {
      if (shouldWriteFile(fileDir, e)) writeExceptionToFile(fileDir, e)
    }

    def shouldWriteFile(fileDir: String, e: Throwable): Boolean =
      fileDir.nonEmpty && e.toString.contains("StallDetectedError")
  }

  object LocalStallExceptionLogger extends ExceptionLogger {
    def logException(e: Throwable): Unit = {
      if (shouldWriteFile(fileDir, e)) {
        writeExceptionToFile(fileDir, e)
      } else {
        log.error(e.toString)
      }
    }

    def shouldWriteFile(fileDir: String, e: Throwable): Boolean = fileDir.nonEmpty
  }
}

private[optimus] sealed trait StallDetector {
  private[diagnostics] def logAndKill(anyAdaptedTasks: Boolean, timeout: Long): Unit

  def ensureStarted(): Unit
  def detectStalls(): Unit
  def snapshotsEqual(prevSnapshot: ThreadSnapshot, curSnapshot: ThreadSnapshot): Boolean
  def checkStall(schedulerQueues: SchedulerQueues, prevSnapshot: ThreadSnapshot): (Boolean, ThreadSnapshot)
  def withStallListener[T](listener: StallDetector.StallDetectedError => Unit)(f: => T): T
}

/**
 * Used when stall detection is disabled (optimus.graph.detectStalls=false). All methods are no-ops
 */
final class NoOpStallDetector extends StallDetector {
  private[diagnostics] def logAndKill(anyAdaptedTasks: Boolean, timeout: Long): Unit = ()

  def ensureStarted(): Unit = ()
  def detectStalls(): Unit = ()
  def snapshotsEqual(prevSnapshot: ThreadSnapshot, curSnapshot: ThreadSnapshot): Boolean = false
  def checkStall(schedulerQueues: SchedulerQueues, prevSnapshot: ThreadSnapshot): (Boolean, ThreadSnapshot) =
    (false, SchedulerDiagnosticUtils.EmptyThreadSnapshot)
  def withStallListener[T](listener: StallDetector.StallDetectedError => Unit)(f: => T): T = f
  def kill(returnCode: Int): Unit = ()
}

/**
 * Graph Stall Detector
 *
 * Designed to detect deadlock situations in unit tests and test apps and record diagnostics before killing the process
 *
 * Operation: (1) Start a separate thread that periodically takes a snapshot of all of work/waits queues on the graph,
 * and any threads with names in the inclusion list (2) Compare this snapshot to the previous one to see if anything has
 * changed (3) If nothing has changed for longer than the configured timeout, and there are no adapted nodes being
 * waited on (i.e. with a plugin installed), then record diagnostics and kill the process
 *
 * intervalSecs - time (seconds) between each snapshot fullTimeoutSecs - time (seconds) after which the process will be
 * killed if no changes are detected and there are no outstanding adapted tasks being waited on adaptedTimeoutSecs -
 * time (seconds) after which the process will be killed if no changes are detected even if there are outstanding
 * adapted tasks being waited on detectStallAdapted - if true, process will be killed when a stall is detected while
 * there are outstanding tasks being waited on, else a warning will be logged threadNameInclusionlist - names of threads
 * to include in the thread snapshot for comparison exceptionLogger - custom exception logging (e.g. to log exceptions
 * to file instead of stdout)
 *
 * By default all schedulers (Scheduler.schedulers) are considered for stall analysis. This can be changed by overriding
 * the relevantSchedulers method
 *
 * There is an option to register for stall notifications via the withStallListener method. The provided listener will
 * be called before the process is killed and after any other listeners that have already been registered. Exceptions
 * thrown by listeners will be ignored, and listeners should not block since this will prevent other listeners from
 * running and prevent the process from being killed
 */
private[optimus] class DefaultStallDetector(
    intervalSecs: Int,
    fullTimeoutSecs: Int,
    adaptedTimeoutSecs: Int,
    detectStallAdapted: Boolean,
    threadNameInclusionList: collection.Seq[String],
    exceptionLogger: ExceptionLogger
) extends StallDetector {
  import StallDetector._

  private val fullTimeoutMs = TimeUnit.SECONDS.toMillis(fullTimeoutSecs)
  private val adaptedTimeoutMs = TimeUnit.SECONDS.toMillis(adaptedTimeoutSecs)

  private val started = new AtomicBoolean()
  private val executor = Executors.newSingleThreadScheduledExecutor(r => {
    val name = getClass.getSimpleName
    val thread = new Thread(r, s"Thread-$name(i=$intervalSecs,fullT=$fullTimeoutSecs,adaptedT=$adaptedTimeoutSecs)")
    thread.setDaemon(true)
    thread
  })

  final def ensureStarted(): Unit = {
    if (!started.get && started.compareAndSet(false, true)) {
      log.info(s"Starting stall detector loop")
      executor.scheduleAtFixedRate(() => detectStalls(), intervalSecs, intervalSecs, TimeUnit.SECONDS)
    }
  }

  private var lastChangedTime: Long = 0 // ms
  private var lastSnapshot: ThreadSnapshot = SchedulerDiagnosticUtils.EmptyThreadSnapshot

  def relevantSchedulers: List[Scheduler] = Scheduler.schedulers

  final def detectStalls(): Unit = {
    log.debug(s"Taking snapshot of ${relevantSchedulers.length} schedulers ")
    val schedulerQueues = relevantSchedulers.map(_.getContexts)

    val (stalled, curSnapshot) = checkStall(schedulerQueues, lastSnapshot)
    val curTime = System.currentTimeMillis()

    log.debug("Checking for any changes in the snapshot")
    if (stalled) {
      log.debug("No change detected in the snapshot")
      val timeDiff = TimeUnit.MILLISECONDS.toSeconds(curTime - lastChangedTime)
      log.debug(s"Time since last snapshot change: ${timeDiff}s")

      log.debug("Checking for adapted tasks being waited-on")
      val anyAdaptedTasks = SchedulerDiagnosticUtils.anyAdaptedTasks(schedulerQueues)

      if (!anyAdaptedTasks && timeoutExceeded(lastChangedTime, curTime, fullTimeoutMs)) {
        log.info("There were no adapted tasks being waited on and the full-stall timeout was exceeded")
        if (extraCheckStall(schedulerQueues, curSnapshot)) {
          log.debug("About to kill the process as the thread snapshot didn't change after doing two extra checks.")
          logAndKill(anyAdaptedTasks, curTime - lastChangedTime)
        } else {
          log.info(
            "There were no adapted tasks being waited on and the full-stall timeout was exceeded, " +
              "but the thread snapshot changed after doing two extra checks.")
        }
      } else if (anyAdaptedTasks && timeoutExceeded(lastChangedTime, curTime, adaptedTimeoutMs)) {
        log.debug(
          s"There were adapted tasks being waited on and the with-adapted-stall timeout was exceeded: $adaptedTimeoutSecs s. About to kill the process: ${detectStallAdapted}")
        if (detectStallAdapted) {
          logAndKill(anyAdaptedTasks, curTime - lastChangedTime)
        } else {
          log.warn(
            s"Checked for stalls after ${intervalSecs}s. " +
              s"There was a stall while adapted tasks were being waited on and the with-adapted-stall timeout was " +
              s"exceeded, but killing on adapted stalls is disabled.")
        }
      } else {
        log.info(
          s"Checked for stalls after ${intervalSecs}s. " +
            s"No stall: no movement for ${timeDiff}s and any adapted tasks? $anyAdaptedTasks")
      }
    } else {
      log.info(
        s"Checked for stalls after ${intervalSecs}s. " +
          s"No stall: movement on scheduler / listed threads")
      lastSnapshot = curSnapshot
      lastChangedTime = curTime
    }
  }

  private def extraCheckStall(schedulerQueues: SchedulerQueues, snapshot1: ThreadSnapshot): Boolean = {
    Thread.sleep(extraCheckInterval) // We want to back-off a bit before we check the thread stacks again
    val (hasStalled1, snapshot2) = checkStall(schedulerQueues, snapshot1)
    hasStalled1 && {
      Thread.sleep(extraCheckInterval) // We want to back-off a bit before we check the thread stacks again
      val (hasStalled2, _) = checkStall(schedulerQueues, snapshot2)
      hasStalled2
    }
  }

  def checkStall(schedulerQueues: SchedulerQueues, prevSnapshot: ThreadSnapshot): (Boolean, ThreadSnapshot) = {
    log.debug("Taking thread snapshots")
    // we exclude the spinning thread because that's always spinning around even when we are effectively stalled
    val curSnapshot = SchedulerDiagnosticUtils.getThreadSnapshotForQueues(
      schedulerQueues,
      threadNameInclusionList,
      excludeSpinningThread = true)
    printSnapshotDetails(curSnapshot)
    (noMovement(prevSnapshot, curSnapshot), curSnapshot)
  }

  private def printSnapshotDetails(snapshot: ThreadSnapshot): Unit = {
    log.debug(
      s"It took ${snapshot.captureTimeMs}ms to take the snapshot containing " +
        s"${snapshot.schedulerInfos.infos.size} scheduler threads and ${snapshot.otherInfos.infos.size} other threads")
    log.debug(s"Scheduler threads:")
    printThreadInfoDetails(snapshot.schedulerInfos)
    log.debug("Other threads:")
    printThreadInfoDetails(snapshot.otherInfos)
  }

  private def printThreadInfoDetails(threadInfos: ThreadInfos): Unit = {
    threadInfos.infos.foreach { case (id, t) =>
      log.debug(s"Thread ID: $id -> Thread Name: ${t.getThreadName}")
    }
  }

  final def noMovement(prevSnapshot: ThreadSnapshot, curSnapshot: ThreadSnapshot): Boolean = {
    curSnapshot.threadCount > 0 && snapshotsEqual(prevSnapshot, curSnapshot)
  }

  final def snapshotsEqual(prevSnapshot: ThreadSnapshot, curSnapshot: ThreadSnapshot): Boolean =
    curSnapshot.schedulerInfos.infos.size == prevSnapshot.schedulerInfos.infos.size &&
      prevSnapshot.threadTaskCounts == curSnapshot.threadTaskCounts && // None of the graph threads have ran tasks in the meantime
      prevSnapshot.allStringInfos == curSnapshot.allStringInfos

  private def timeoutExceeded(prevTime: Long, curTime: Long, timeout: Long): Boolean = curTime - prevTime > timeout

  private[graph] def logAndKill(anyAdaptedTasks: Boolean, stallTime: Long): Unit = {
    log.info("A stall was detected")
    log.info("Shutting down the stall detector thread")
    executor.shutdown()
    log.info("Constructing stall detected error containing process diagnostics")
    val error = new StallDetector.StallDetectedError(
      intervalSecs,
      fullTimeoutSecs,
      adaptedTimeoutSecs,
      TimeUnit.MILLISECONDS.toSeconds(stallTime))
    log.info("Finished constructing stall detected error")
    try {
      notifyListeners(error)
    } finally {
      exceptionLogger.logException(error)
      val returnCode = if (anyAdaptedTasks) WithAdaptedReturnCode else StandardReturnCode
      kill(returnCode)
    }
  }

  protected[this] def kill(returnCode: Int): Unit = InfoDumper.kill("stall", code = returnCode)

  // Listeners should not block or throw
  private val listenersLock = new Object
  @volatile private var listeners = Set.empty[StallDetectedError => Unit]

  final def withStallListener[T](listener: StallDetectedError => Unit)(f: => T): T = {
    addListener(listener)
    try f
    finally removeListener(listener)
  }

  private def addListener(f: StallDetectedError => Unit): Unit = listenersLock.synchronized {
    log.info(s"Adding new listener to current set of listeners (${listeners.size})")
    listeners += f
  }

  private def removeListener(f: StallDetectedError => Unit): Unit = listenersLock.synchronized {
    log.info(s"Removing listener from current set of listeners (${listeners.size})")
    listeners -= f
  }

  private def notifyListeners(error: StallDetectedError): Unit = {
    log.info(s"Notifying ${listeners.size} listeners of a stall")
    listeners.zipWithIndex.foreach { case (listener, i) =>
      try {
        log.info(s"Notifying listener $i")
        listener(error)
      } catch {
        case t: Throwable => log.warn(s"Swallowed exception thrown in listener $i: $t")
      }
    }
    log.info("Finished notifying listeners")
  }

  override def toString: String = {
    val name = getClass.getSimpleName
    s"$name(interval=$intervalSecs,timeout=$fullTimeoutSecs,adapted_timeout=$adaptedTimeoutSecs)"
  }
}
