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
package optimus.graph

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import one.profiler.AsyncProfiler
import one.profiler.Counter
import optimus.graph.diagnostics.ap.StackAnalysis
import optimus.graph.diagnostics.sampling.AsyncProfilerSampler
import optimus.logging.LoggingInfo
import optimus.platform.util.Log
import optimus.logging.Pid
import optimus.utils.FileUtils
import optimus.utils.PropertyUtils

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.sys.ShutdownHookThread
import scala.util.Properties
import scala.util.control.NonFatal

object AsyncProfilerIntegration extends Log {
  import RecordingStateMachine._

  private val customOffsets = new AtomicInteger(0)
  final case class CustomEventType private[AsyncProfilerIntegration] (name: String, valueType: String) {
    // someday we may allow adding these dynamically
    private[AsyncProfilerIntegration] val offset = customOffsets.getAndIncrement()
    // must be lazy, since we can't save strings until after AP is initialized
    lazy val eventNamePtr: Long = saveString(s"${name}.${valueType}")
    private[AsyncProfilerIntegration] def add(): Unit = () // profiler.addCustomEventType(offset, name, valueType)
  }

  val TestingEvent: CustomEventType = CustomEventType("Testing", "TestValue")
  val FullWaitEvent: CustomEventType = CustomEventType("FullWait", "NS")

  private def addCustomEvents(): Unit = {
    TestingEvent.add()
    FullWaitEvent.add()
  }

  private def nanoTime = System.nanoTime()

  @volatile private var profiler: AsyncProfiler = null
  @volatile private var profiling = false;
  @volatile private var samplingLoop = false
  @volatile private var _globalStackMemResetTime = nanoTime
  private var jfrTraceFile: String = null
  private var jfrTraceReady = false

  def globalStackMemResetTime: Long = _globalStackMemResetTime

  // Duplicated from optimus.platform.pickling.Registry.duringCompilation
  private val duringCompilation = Option(System.getProperty("optimus.inCompiler")).exists(_.toBoolean)

  // Don't expect this to get used except in case of disaster
  private val disableDuringCompilation =
    PropertyUtils.get("optimus.graph.obt.disable.async.profiler", false)

  // This might be used explicitly to disable in child processes and/or with multiple
  // class-loaders
  val apEnabledProperty = "optimus.graph.async.profiler"
  private var doEnable =
    !DiagnosticSettings.isWindows && !(duringCompilation && disableDuringCompilation) &&
      DiagnosticSettings.getBoolProperty(apEnabledProperty, true)

  def enabled: Boolean = profiler ne null
  def isProfiling: Boolean = profiling

  private[graph] val defaultSettings =
    ":auto=false" + // start automatically the first time continuous profiling requested
      ":onload=false" + // start automatically during static initialization
      ":defer=false" + // start sampling loop, but paused if true
      ":out=jfr" + // html or jfr
      ":event=cpu" + // comma delimited, standard async-profiler options
      ":jfrsync=false" + // or default or true
      ":interval=20" + // sampling interval in ms
      ":segment=300" + // segment length in seconds
      ":await=false" + // enable await stacks if true - must be set before app start
      ":dir=default" + // full path to output directory, or let us choose a default
      ":maxfiles=0" // keep a maximum number of jfr files in continuous mode

  private[graph] val userSettings = DiagnosticSettings.asyncProfilerSettings

  private[graph] lazy val settings: Map[String, String] = PropertyUtils.propertyMap(defaultSettings, userSettings)

  def ensureLoadedIfEnabled(): Boolean = doEnable /* && {
    if (profiler ne null) {
      true
    } else {
      AsyncProfilerIntegration.synchronized {
        if (profiler ne null)
          true;
        else {
          import scala.util.Try
          val tried = Try {
            val p = AsyncProfiler.getInstance()
            // One of the next two lines will likely throw if the native library is broken (so we'll catch
            // below and disable AP).
            p.getSamples()
            // p.saveString("test")
            if (p ne null) {
              log.info(s"asyncProfiler initialized")
              profiler = p
              // Must occur after profiler is defined!
              addCustomEvents()
              if (settings("onload").toBoolean) autoStart(true) // possibly start now, during static initialization
              true
            } else {
              log.info(s"asyncProfiler unavailable")
              doEnable = false; // disable checking forever more
              false
            }
          } recover { case t: Throwable =>
            profiler = null
            log.warn(s"asyncProfiler not initialized", t)
            doEnable = false; // disable checking forever more
            false
          }
          tried.get
        }
      }
    }
  } */

  lazy val cpuProfilingEnabled: Boolean = ensureLoadedIfEnabled() && {
    try {
      profiler.execute(s"start,event=cpu,loglevel=${AsyncProfilerSampler.apLogLevel}")
      profiler.execute(s"stop,loglevel=${AsyncProfilerSampler.apLogLevel}")
      true
    } catch {
      case e: IllegalStateException if e.getMessage.contains("access to perf events") =>
        false
      case NonFatal(e) =>
        log.error(
          s"Unexpected exception caught while checking for access to perf events.  Assuming we do not have access, but there may be additional problems.",
          e)
        false
    }
  }

  def containerSafeEvent(e0: String): String = if (e0.contains("cpu") && !cpuProfilingEnabled) {
    val e = e0.replaceAll("\\bcpu\\b", "itimer")
    log.warn(s"Changing event $e0 to $e")
    e
  } else e0

  private val commandLock = new Object
  def command(cmd: String): String = commandLock.synchronized {
    if (!ensureLoadedIfEnabled())
      "async-profiler integration not enabled"
    else {
      try {
        val ret: String = s"$cmd => ${profiler.execute(cmd)}"
        if (cmd.startsWith("start")) {
          if (DiagnosticSettings.awaitStacks) {
            _globalStackMemResetTime = nanoTime
          }
          if (profiling)
            log.error("Attempting to start profiling while still profiling")
          profiling = true
        } else if (cmd.startsWith("stop")) {
          if (!profiling)
            log.error("Attempting to stop profiling when not profiling")
          profiling = false
        }
        log.debug(s"async-profiler: $ret")
        ret
      } catch {
        case NonFatal(e) =>
          val ret = "async-profiler failed: " + e.getMessage
          log.javaLogger.error(ret) // trace will just show execute0 anyway
          ret
      }
    }
  }

  def stop(): Unit = {
    if (profiling) command("stop")
  }
  def shutdown(): Unit = stop()

  /**
   * Begin jfr sampling in concert with OG profiler tracing.
   */
  def traceStart(): Unit = synchronized {
    if ((profiler ne null) && !profiling) {
      jfrTraceFile = outputDirFromSettings().resolve(s"profile-${Pid.pidOrZero()}-${Instant.now}.jfr").toString
      addOutputFile(jfrTraceFile)
      jfrTraceReady = false
      command(s"start,event=cpu,cstack=dwarf,interval=${settings("interval")}ms,file=$jfrTraceFile")
    }
  }

  /**
   * Stop profiling when leaving profliler trace mode.
   */
  def traceStop(): Unit = synchronized {
    if ((profiler ne null) && profiling) {
      command(s"dump,file=$jfrTraceFile")
      command("stop") // sometimes necessary
      jfrTraceReady = true
    }
  }

  /**
   * Retrieve the most recently written jfr file.
   */
  def getTraceJfrFile(): Option[String] = if (jfrTraceReady) Some(jfrTraceFile) else None

  /**
   * Start continuous profiling. Returns the directory where jfr segments will be written. jfr files in this directory
   * can be concatenated in alphanumeric order.
   * @param intervalMS
   *   Sampling interval.
   * @param segmentSec
   *   Length in seconds of each jfr segment. Zero for just one segment.
   * @param event
   *   Defaults to "cpu"
   * @param jfr
   *   Defaults to true. If false, html flame graphs will be written.
   * @return
   *   the directory where segments will be written
   */

  private var samplingThread: RecordingStateMachine = null
  private var samplingShutdown: ShutdownHookThread = null

  private def samplingNotify(request: Action, await: Boolean = false): Boolean = {
    if (samplingLoop) {
      val message = samplingThread.send(request)
      if (await) message.block().nonEmpty
      else true // assume that it will work eventually or that we don't care
    } else
      false
  }

  private def outputDirFromSettings(s: Map[String, String] = settings) = {
    val p = s("dir") match {
      case "default" => FileUtils.diagnosticDumpDir.resolve(s"ap-${LoggingInfo.getHost}-${LoggingInfo.pid}")
      case dir       => Paths.get(dir)
    }
    Files.createDirectories(p)
    p
  }

  private var outputFiles = Queue.empty[Path]
  private val maxOutputFiles = settings("maxfiles").toInt
  private def addOutputFile(name: String): Unit = if (maxOutputFiles > 0) synchronized {
    outputFiles = outputFiles.enqueue(Paths.get(name)).distinct
    while (outputFiles.size > maxOutputFiles) {
      val (toDelete, newFiles) = outputFiles.dequeue
      outputFiles = newFiles
      try {
        Files.delete(toDelete)
        log.info(s"Deleted $toDelete")
      } catch {
        case NonFatal(e) =>
          log.warn(s"Unable to delete $toDelete: $e")
      }
    }
  }

  def continuousSamplingStart(
      intervalMS: Int,
      segmentSec: Int,
      event: String = "cpu",
      jfrsync: String = "false",
      jfr: Boolean = true,
      startPaused: Boolean = false,
      await: Boolean = false
  ): Option[Path] = this.synchronized {
    if (profiling || (samplingThread ne null) || Properties.isWin || !ensureLoadedIfEnabled()) None
    else {
      val dir = outputDirFromSettings(settings)
      log.info(s"async-profiler writing to directory $dir, nodeStacks=${DiagnosticSettings.awaitStacks}")
      profiling = true
      samplingLoop = true

      val eventOpts = jfrsync match {
        case "true"  => "jfrsync=default"
        case "false" => s"event=$event"
        case profile => s"jfrsync=$profile"
      }
      val conf = s"cstack=dwarf,$eventOpts,interval=${intervalMS}ms"
      samplingThread = new RecordingStateMachine(dir, segmentSec * 1000L, conf, jfr)
      samplingThread.setName("AsyncProfilerJFR")
      samplingThread.setDaemon(true) // don't prevent shutdown (but the shutdown hook below will still be called)
      val msg = samplingThread.send(if (startPaused) Pause else Start) // start sampling
      samplingThread.start()
      samplingShutdown = sys.addShutdownHook(samplingStop0(true))
      if (await) msg.block().flatMap(_ => Some(dir))
      else Some(dir) // we aren't waiting so we assume success
    }
  }

  def samplingStatus(): String =
    if (samplingThread == null) "No recording started" else samplingThread.latestMessage

  private def samplingStop0(await: Boolean): Unit = this.synchronized {
    if ((samplingThread ne null) || Properties.isWin) {
      samplingNotify(Stop, await) // await the final output
      samplingThread = null
    }
    profiling = false
    samplingLoop = false
  }

  def continuousSamplingStop(await: Boolean = false): Unit = this.synchronized {
    if (samplingShutdown ne null) {
      samplingShutdown.remove()
      samplingShutdown = null
    }
    samplingStop0(await)
  }

  def continuousSamplingPause(await: Boolean = false): Unit = this.synchronized {
    samplingNotify(Pause, await)
  }

  def continuousSamplingResume(await: Boolean = false): Unit = this.synchronized {
    samplingNotify(Start, await)
  }

  def continuousSamplingStartOrResume(settings: String): Boolean = this.synchronized {
    if (samplingLoop)
      samplingNotify(Start)
    else
      autoStart(s"defer=false;$settings").isDefined
  }

  def autoStart(force: Boolean): Option[Path] = autoStart(if (force) "auto=true;defer=false" else "")
  def autoStart(overrides: String = ""): Option[Path] = {
    val s = PropertyUtils.propertyMap(settings, overrides)
    if (!profiling && s("auto").toBoolean) {
      val out = s("out")
      if (out != "html" && out != "jfr") {
        log.error("async-profiler setting may be html or jfr")
        None
      } else {
        val ret = continuousSamplingStart(
          intervalMS = s("interval").toInt,
          segmentSec = s("segment").toInt,
          startPaused = s("defer").toBoolean,
          jfr = out == "jfr",
          event = s("event"),
          jfrsync = s("jfrsync")
        )
        ret
      }
    } else None
  }

  /**
   * Snag some async-profiler output suitable for inclusion in error logs.
   * @param durationMS
   * @param intervalMS
   * @param maxMethods
   *   Maximum number of entries in the top methods list.
   * @param maxTraces
   *   Maximum number of stack traces in the top traces list.
   * @param event
   *   Defaults to "cpu"
   * @param savePrefix
   *   If set, a profile will be written to a directory starting with this string.
   * @param saveType
   *   With the above, "jfr" or "html".
   * @return
   */
  def snag(
      durationMS: Int,
      intervalMS: Int,
      maxMethods: Int = 10,
      maxTraces: Int = 10,
      collapsed: Boolean = false,
      event: String = "cpu",
      savePrefix: Option[String] = None,
      saveType: String = "jfr"
  ): Option[Snag] = synchronized {
    if (Properties.isWin || (profiler eq null) || profiling)
      None
    else {
      val save = savePrefix.map { prefix =>
        FileUtils.tmpPath(s"$prefix-snag", saveType)
      }
      var options: String = ""
      save.foreach { file =>
        if (saveType == "jfr") options += s",jfrsync=default"
        options += s",file=$file"
      }
      try {
        val msgs = new ArrayBuffer[String]
        msgs += command(s"start,cstack=dwarf,event=$event,interval=${intervalMS}ms$options")
        Thread.sleep(durationMS)
        msgs += command("stop")
        log.info("Dumping flat")
        val hot = profiler.dumpFlat(maxMethods)
        log.info("Dumping trace")
        val traces = profiler.dumpTraces(maxTraces)
        val flame = if (collapsed) profiler.dumpCollapsed(Counter.TOTAL) else "n/a"
        msgs += (save match {
          case Some(path) =>
            log.info(s"Dumping to $path")
            command(s"dump,file=$path");
          case _ =>
            command("stop")
        })
        Some(Snag(hot, traces, flame, save, msgs.mkString(";")))
      } catch {
        case t: Throwable =>
          log.javaLogger.error(s"AsyncProfiler snagging failed.  Returning empty result: ${t.getMessage}")
          None
      }
    }
  }

  def dumpMemoized(): String = {
    if (ensureLoadedIfEnabled()) {
      profiler.execute("collapsed,memoframes,total")
    } else
      "unavailable"
  }

  def recordCustomEvent(etype: CustomEventType, valueD: Double, valueL: Long, ptr: Long): Unit = {
    // if (ensureLoadedIfEnabled())
    //   profiler.recordCustomEvent(etype.offset, valueD, valueL, ptr)
  }

  private val savedStrings = new ConcurrentHashMap[String, Long]()

  // Get pointer to string in C-land
  def saveString(name: String): Long = 0L
    // if (!ensureLoadedIfEnabled()) 0L else savedStrings.computeIfAbsent(name, profiler.saveString)

  def getMethodID(cls: Class[_], method: String, sig: String): Long = 0L
    // if (!DiagnosticSettings.awaitStacks || !ensureLoadedIfEnabled()) 0L
    // else profiler.getMethodID(cls, method, sig, false)

  def getStaticMethodID(cls: Class[_], method: String, sig: String): Long = 0L
    // if (!DiagnosticSettings.awaitStacks || !ensureLoadedIfEnabled()) 0L
    // else profiler.getMethodID(cls, method, sig, true)

  def setAwaitStackId(ids: Array[Long], signal: Long, insertionId: Long): Long = 0L
    // if (!DiagnosticSettings.awaitStacks || !ensureLoadedIfEnabled()) 0L
    // else profiler.setAwaitStackId(ids, signal, insertionId)

  def getAwaitDataAddress(): Long = 0L
    // if (!DiagnosticSettings.awaitStacks || !ensureLoadedIfEnabled()) 0L else profiler.getAwaitDataAddress()

  def getAwaitSampledSignal(): Long = 0L
    // if (!DiagnosticSettings.awaitStacks || !ensureLoadedIfEnabled()) 0L else profiler.getAwaitSampledSignal()

  def saveAwaitFrames(ids: Array[Long], nids: Int): Long = 0L
    // if (!DiagnosticSettings.awaitStacks || !ensureLoadedIfEnabled()) 0L else profiler.saveAwaitFrames(2, ids, nids)

  def externalContext(ctx: Long, shmPath: String): Unit = ()
    // if (ensureLoadedIfEnabled()) profiler.externalContext(ctx, shmPath)

  lazy val overflowMarker: Long = saveString("OverflowMarker")
  lazy val errorMarker: Long = saveString("ErrorMarker")

  final case class Snag(hot: String, traces: String, collapsed: String, path: Option[Path], msg: String)

}

/**
 * Async profiler recording state as a finite state machine.
 *
 * At any given time we have a segment, which is a current piece of the overall async-profiler recording and corresponds
 * to a JFR file. The segment can be in various states, which is encoded in its class hierarchy. There is a different
 * class hierarchy for the available state transitions, which is given by RecordingStateMachine.Message. Messages
 * are enqueued on the FSM and processed in order.
 *
 * Splitting the current state from the desired state (encoded by Messages) is important because it makes it possible to
 * gracefully handle invalid requests such, as pausing a currently stopped recording. All the transitions and states are
 * processed in a big, exhaustive match statement.
 */
private[graph] class RecordingStateMachine(
    dir: Path,
    segmentLengthMs: Long,
    conf: String,
    jfr: Boolean
) extends Thread
    with Log {
  import AsyncProfilerIntegration.command
  import RecordingStateMachine._
  private val ext = if (jfr) "jfr" else "html"

  // --- State ---
  private var segmentCount = 0 // current increasing segment count
  private var current: Segment = new Done(-1, "No recording started.") // current segment
  private val messageQueue = mutable.Queue.empty[MessageImpl] // queue of messages

  // Flag for whether this machine has been shutdown. If set to true, new messages are immediately completed and
  // dropped.
  @volatile var dead = false

  // This monitor is used to block in between updates of the current state. When recording, we block on samplingWait for
  // the whole segment length (often minutes). When a new message is added to the queue, we notify samplingWait to make
  // sure that the FSM wakes up. All state transitions synchronize samplingWait then .wait on it, to avoid dead locks.
  private val samplingWait = new Object

  // After dumping JFRs, we gzip them. We don't want to exit until all the gzips are done, so we keep track of them
  // here.
  private var gzips = List.empty[Process]
  private def gzipLog(p: Process): Unit =
    if (p.exitValue() != 0) log.error("gzip error:" + new String(p.getErrorStream.readAllBytes()))
  private def startGzip(path: String): Unit = {
    val pb = new ProcessBuilder("/usr/bin/gzip", path)
    val (notDone, done) = gzips.partition(_.isAlive)
    done.foreach(gzipLog)
    gzips = pb.start() :: notDone
  }
  private def waitGzips(): Unit = {
    gzips.foreach { p =>
      if (!p.waitFor(20, TimeUnit.SECONDS)) log.error("gzipping a JFR file is taking more than 20 seconds")
      else gzipLog(p)
    }
    gzips = List.empty[Process]
  }

  // --- State definitions ---
  // SegmentBase is the base class, and it splits into Uninit for un-initialized segments and Segment for initialized
  // segments. The segment in .current is always initialized.
  //
  // Segments have methods that change the current state. These return a new Segment, which then must be used to replace
  // .current. Attempting to alter the state of a segment twice will throw. (It would be cool to have borrow checking
  // and copy elision here!)
  sealed abstract class SegmentBase(val id: Int) {
    override def toString: String = s"${getClass}(segment=${id})"

    // No synchronization required here because the overall state machine is synchronized. This is just a simple runtime
    // check to make sure no one leaks a segment than try to alter states for it in two different places.
    private var valid = true
    protected def validSwitch(): Unit = {
      if (valid) {
        valid = false
      } else throw new IllegalStateException("Attempted to modify the state of a segment twice.")
    }
  }

  final class Uninit() extends SegmentBase(segmentCount) {
    segmentCount += 1
    def start(): Recording = {
      val file = dir.resolve(s"${Instant.now()}-$id.$ext").toString
      validSwitch()
      val msg = command(s"start,$conf,file=$file")
      val out = new Recording(segmentCount, msg)(0, file)
      out
    }
  }

  sealed abstract class Segment(id: Int, val lastMessage: String) extends SegmentBase(id) {
    def tick(): Unit = ()
  }

  // Both Paused and Recording are NotDone and can be dumped.
  sealed trait NotDone { self: Segment =>
    val file: String

    def dump(): Done = {
      validSwitch()
      val samplingMessage =
        if (jfr)
          command(s"stop,file=$file")
        else
          command(s"dump,file=$file") + "; " + command("stop")

      startGzip(file)
      new Done(id, samplingMessage)
    }
  }

  final class Paused(id: Int, msg: String)(val length: Long, val file: String) extends Segment(id, msg) with NotDone {
    def resume(): Recording = {
      validSwitch()
      val samplingMessage = command(s"resume,$conf,file=$file")
      new Recording(id, samplingMessage)(length, file)
    }
  }

  final class Recording(id: Int, msg: String)(initialLength: Long, val file: String)
      extends Segment(id, msg)
      with NotDone {
    val start: Long = System.currentTimeMillis() - initialLength
    private var t = start
    def length: Long = t - start
    def remaining: Long = segmentLengthMs - length
    override def tick(): Unit = t = System.currentTimeMillis()

    def pause(): Paused = {
      validSwitch()
      new Paused(id, command("stop"))(length, file)
    }

    /**
     * Returns Done if the current segment is finished.
     */
    def update(): Either[Done, Recording] = {
      if (length >= segmentLengthMs) Left(dump()) else Right(this)
    }
  }

  final class Done(id: Int, msg: String) extends Segment(id, msg)

  // --- Public API ---

  /**
   * Get most recent message from async profiler.
   *
   * This isn't synchronized so there is no guarantee that the message is the one you want!
   */
  def latestMessage: String = current.lastMessage

  /**
   * Request a change of state.
   *
   * The returned message will be completed when the state has been updated, or when the thread dies.
   */
  def send(m: Action): Message = {
    val msg = MessageImpl(m)
    if (dead) {
      msg.maybeFail()
      msg
    } else {
      samplingWait.synchronized {
        messageQueue.enqueue(msg)
        log.info(s"Requesting that state will $m")
        samplingWait.notifyAll()
      }
      msg
    }
  }

  // -- Implementation --
  override def run(): Unit = {
    var message: MessageImpl = null
    try {
      while (true) {
        current.tick()
        log.debug(s"Sampling wait woke up: current state: $current, messages: ${messageQueue}")

        message =
          if (messageQueue.nonEmpty) messageQueue.dequeue()
          else MessageImpl.default

        // Note: This is fairly complex, so do *not* insert an if-guard or a catch-all statement in the pattern match
        // below! We are relying on exhaustiveness checking by the compiler to make sure we didn't miss any transitions.
        (current, message.action) match {
          // Stopping when not done dumps the file and kills the thread.
          case (c: NotDone, Stop) => current = c.dump()

          // Pausing and resuming recordings
          case (c: Recording, Pause) => current = c.pause()
          case (c: Paused, Start)    => current = c.resume()
          // Some operations on paused are noop
          case (_: Paused, Continue | Pause) => ()

          // If we are recording and didn't pause or stop, we check if it is time to switch to a new segment
          case (c: Recording, Continue | Start) =>
            current = c.update() match {
              // If the segment is finished, we immediately start a new segment without waiting for gzip-ing. Note that
              // the two segments don't overlap, and the gap between them will be missing the data between the call to
              // .update above, and the call to .start() below.
              case Left(_)         => new Uninit().start()
              case Right(continue) => continue
            }

          // Starting when Done starts a new recording
          case (_: Done, Start) => current = new Uninit().start()

          // Various other actions do nothing when Done
          case (_: Done, Stop | Pause | Continue) => ()
        }

        // We then block on samplingWait for however much time is remaining in this current segment, or until woken up if
        // we aren't recording.
        if (message.action != Stop) {
          // If anyone is waiting on the message to be processed, we notify them. Note that Stop messages are notified
          // in the finally clause, after waiting for gzips.
          message.succeed(current.lastMessage)

          samplingWait.synchronized {
            current match {
              case r: Recording if r.remaining > 0 => samplingWait.wait(r.remaining)
              case _                               => samplingWait.wait()
            }
          }
        } else return
      }
    } finally {
      // Wait for gzip to run. This is done first so that someone who submits a Stop message (say, the shutdown hook)
      // won't be racing against other Stop submitters.
      waitGzips()

      // We stop accepting new messages, and wake up every awaiter to avoid deadlocks if an exception was thrown. After
      // this point we will .fail() any new send() tasks.
      dead = true
      messageQueue.dequeueAll(_ => true).foreach(_.maybeFail())
      if (message ne null) message.maybeFail() // if we had a message to complete and threw, we notify its waiters.
    }
  }
}

object RecordingStateMachine {

  // State transitions that can be requested.
  sealed trait Action
  // Default action: keep going with whatever we were doing.
  final case object Continue extends Action
  final case object Stop extends Action
  final case object Pause extends Action
  final case object Start extends Action

  /**
   * A state transition request to be fulfilled.
   */
  sealed trait Message {
    def action: Action

    /**
     * Await till the change in state has been processed. Returns the String output by asyncprofiler if the transition
     * was successful, and None if it wasn't.
     */
    def block(): Option[String]
  }

  // Save memory by making the message itself a latch, but don't expose this in the public trait.
  private final case class MessageImpl(action: Action) extends CountDownLatch(1) with Message {
    override def toString: String = s"Message@0x${hashCode().toHexString}(${action})"
    @volatile private var outcome: Option[String] = None

    // These are effectively private because only SamplingFSM sees MessageImpl
    def succeed(msg: String): Unit = {
      outcome = Some(msg)
      countDown()
    }

    // Fail the message if its not done. Doesn't do anything if succeed was already called.
    def maybeFail(): Unit = countDown()

    override def block(): Option[String] = {
      super.await()
      outcome
    }
  }
  private object MessageImpl { val default: MessageImpl = MessageImpl(Continue) }
}
