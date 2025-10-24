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

import com.sun.jna.Library
import com.sun.jna.Native

import java.io.FileOutputStream
import java.io.IOException
import optimus.utils.MiscUtils.Endoish._

import java.io.ObjectOutputStream
import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.nio.file.Path
import java.util.Base64
import com.sun.management.HotSpotDiagnosticMXBean
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.Properties.Elems
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.core.GraphDiagnosticsSource
import optimus.core.MonitoringBreadcrumbs
import optimus.core.config.StaticConfig
import optimus.graph.AsyncProfilerIntegration
import optimus.graph.DiagnosticSettings
import optimus.graph.EfficientSchedulerLogging
import optimus.graph.Exceptions
import optimus.graph.GCNative
import optimus.graph.NodeTask
import optimus.graph.Scheduler
import optimus.graph.diagnostics.sampling.ForensicSource
import optimus.logging.LoggingInfo
import optimus.platform.util.InfoDump
import optimus.platform.util.InfoDumpUtils
import optimus.platform.util.PrettyStringBuilder
import optimus.platform.util.Version
import optimus.utils.FileUtils
import msjava.slf4jutils.scalalog.Logger

import scala.util.Try
import scala.util.Using
import scala.jdk.CollectionConverters._
import optimus.utils.FileUtils.tmpPath

import java.time.Instant
import scala.util.Properties.isWin

//noinspection ScalaUnusedSymbol // ServiceLoader
class InfoDumper extends InfoDump {

  override def kill(
      prefix: String,
      msgs: List[String] = Nil,
      code: Int = -1,
      crumbSource: Crumb.Source = Crumb.RuntimeSource,
      exceptions: Array[Throwable] = Array.empty[Throwable],
      logger: Logger = log
  ): Unit =
    InfoDumper.kill(prefix, msgs, code, crumbSource, exceptions, logger)

  override def dump(
      prefix: String,
      msgs: List[String] = Nil,
      crumbSource: Crumb.Source = Crumb.RuntimeSource,
      id: ChainedID = ChainedID.root.child,
      description: String = "Information",
      heapDump: Boolean = false,
      taskDump: Boolean = false,
      noMore: Boolean = false,
      exceptions: Array[Throwable] = Array.empty,
      emergency: Boolean = false,
      doLog: Boolean = true,
      logger: Logger = log
  ): String =
    InfoDumper.dump(
      prefix,
      msgs,
      crumbSource,
      id,
      description,
      heapDump,
      taskDump,
      noMore,
      exceptions,
      emergency,
      doLog,
      logger
    )

}

object InfoDumper extends InfoDumpUtils {

  val memoryDxCommand: String = StaticConfig.string("memoryDiagCommand")
  val nodeStatsFile = "nodes.stats"
  def ensureInitialized(): Unit = {}
  FileUtils.ensureInitialized()

  @volatile private var enabled = true
  if (DiagnosticSettings.infoDumpPeriodicityHours > 0.0) {
    val t = (DiagnosticSettings.infoDumpPeriodicityHours * 3600 * 1000).toLong
    val thread = new Thread {
      override def run(): Unit = {
        while (enabled) {
          Thread.sleep(t)
          dump("periodic")
        }
      }
    }
    thread.setDaemon(true)
    thread.setName("InfoDumper")
    thread.start()
  }

  private trait Lib3 extends Library {
    def raise(sig: Int): Int
  }

  private val backupKillThread: Thread =
    if (isWin || DiagnosticSettings.infoDumpEmergencyKillSec <= 0) null
    else
      Try(Native.load("c", classOf[Lib3]))
        .map { libc =>
          // If we're still running after the timeout, this thread will dump and kill
          val emergencyDumpThread: Thread =
            new Thread {
              override def run(): Unit = {
                // If the sleep is interrupted, this thread will just exit, so we can't accidentally
                // kill the process early.
                Thread.sleep(DiagnosticSettings.infoDumpEmergencyKillSec * 1000L)
                System.err.println(s"${Instant.now} InfoDumper emergency triggered")
                dump("emergency", emergency = true)
                System.err.println(s"${Instant.now} InfoDumper killing process with SIGKILL")
                libc.raise(9) // SIGKILL
              }
            }
          emergencyDumpThread.setDaemon(true)
          emergencyDumpThread.setName("InfoDumperEmergency")

          // Last ditch effort to kill the process
          val backupKillThread: Thread =
            new Thread {
              override def run(): Unit = {
                // If the sleep is interrupted, this thread will just exit, so we can't accidentally
                // kill the process early.
                Thread.sleep(2000L * DiagnosticSettings.infoDumpEmergencyKillSec)
                libc.raise(9) // SIGKILL
              }
            }

          backupKillThread.setDaemon(true)
          backupKillThread.setName("InfoDumperKill")

          // The shutdown hook thread itself blocks exit, but the daemon threads it starts do not.
          Runtime.getRuntime.addShutdownHook(new Thread {
            override def run(): Unit = {
              // The start() will throw if the thread is already started, but we don't care.
              Try { backupKillThread.start() }
              Try { emergencyDumpThread.start() }
              // For testing only: block the shutdown to allow the emergency threads to handle it
              if (DiagnosticSettings.getBoolProperty("test.infodumper.kill", false))
                Thread.sleep(DiagnosticSettings.infoDumpEmergencyKillSec * 10000L)
            }
          })

          backupKillThread
        }
        .getOrElse(null)

  // A serializable object to be persisted for later inspection.
  @volatile private var runningTask: Option[Object] = None

  // attach extra properties to serializable object
  @volatile private var meta: Properties.Elems = Elems.Nil

  override def getMeta: Properties.Elems = meta

  // used to read off the chainedID in the hprof
  private var id: ChainedID = _

  def unsetTask(): Unit = {
    runningTask = None
    meta = Elems.Nil
    id = null
  }

  def setTask(task: Object, metaData: Elems, nodeId: ChainedID): Unit = {
    runningTask = Some(task)
    meta = metaData
    id = nodeId
  }

  /**
   * For graph-side panics, like logAndDie.
   */
  def graphPanic(prefix: String, logMsg: String, code: Int, exception: Throwable, ntsk: NodeTask): Unit = {
    MonitoringBreadcrumbs.sendGraphFatalErrorCrumb(
      logMsg + s", exiting with code ${code}",
      ntsk,
      exception,
      LoggingInfo.getLogFile,
      prefix)
    Breadcrumbs.flush()
    kill(prefix, code = code, crumbSource = GraphDiagnosticsSource, exceptions = Array(exception))
  }

  def kill(
      prefix: String,
      msgs: List[String] = Nil,
      code: Int = -1,
      crumbSource: Crumb.Source = Crumb.RuntimeSource,
      exceptions: Array[Throwable] = Array.empty[Throwable],
      logger: Logger = log
  ): Unit = {
    Try { backupKillThread.start() }
    dump(
      prefix,
      msgs = s"Exiting with code $code" :: msgs,
      crumbSource = crumbSource,
      description = "Shutdown",
      heapDump = DiagnosticSettings.heapDumpOnKill,
      taskDump = true,
      noMore = true,
      exceptions = exceptions,
      logger = logger
    )
    Breadcrumbs.flush()
    if (DiagnosticSettings.fakeOutOfMemoryErrorOnKill) {
      logger.warn("About to trigger an OutOfMemoryError")
      new Array[Byte](Int.MaxValue)
    }
    Runtime.getRuntime.exit(code)
  }

  def dump(
      prefix: String,
      msgs: List[String] = Nil,
      crumbSource: Crumb.Source = ForensicSource,
      id: ChainedID = ChainedID.root.child,
      description: String = "Information",
      heapDump: Boolean = false,
      taskDump: Boolean = false,
      noMore: Boolean = false,
      exceptions: Array[Throwable] = Array.empty,
      emergency: Boolean = false,
      doLog: Boolean = true,
      logger: Logger = log
  ): String = this.synchronized {
    if (enabled || emergency) {
      if (noMore) enabled = false
      val msg = ("InfoDumper: " + msgs.mkString(";")).applyIf(noMore)(_ + ";final")
      logger.error(msg)
      System.err.println(msg)
      Breadcrumbs.error(
        id,
        PropertiesCrumb(
          _,
          crumbSource,
          Version.properties + (Properties.description -> description) + (Properties.logMsg -> msg))) &&:
        Breadcrumbs.Flush

      val sb = new PrettyStringBuilder()
      sb.appendln(
        s"$msg: $id ${Breadcrumbs.currentRegisteredInterests} ${LoggingInfo.getHost} ${LoggingInfo.getLogFile}")
      // Snap 200ms of async-profiling before we start other logging
      val apSnag: Option[AsyncProfilerIntegration.Snag] = {
        if (DiagnosticSettings.samplingProfilerStatic) None
        else
          AsyncProfilerIntegration.snag(
            durationMS = 200,
            intervalMS = 5,
            maxMethods = 100,
            maxTraces = 10,
            savePrefix = Some(prefix),
            saveType = "ap",
            event = "cpu")
      }
      // dump full stack trace of passed in exception
      exceptions.zipWithIndex.foreach { case (e, i) =>
        sb.appendln(s"Exception passed to info dumper (${i + 1} of ${exceptions.length}):")
        Exceptions.causes(e).flatMap(Exceptions.stackTraceLines).foreach(sb.appendln)
      }
      // tcmalloc info if available
      GCNative.dumpTCMallocInfo(sb.underlying)
      // graph scheduler
      EfficientSchedulerLogging.logSchedulers(
        sb,
        Scheduler.schedulers,
        shouldPrintAllInfo = true,
        unconditionally = true)
      // OS-level
      OSInformation.dumpRuntimeInfo(sb.underlying)
      OSInformation.dumpPidInfo(sb.underlying, OSInformation.getPID, Some(200))
      System.getenv().asScala.foreach { case (k, v) =>
        sb.append(s"$k=$v")
      }
      // Append async-profiler summaries
      apSnag foreach { case AsyncProfilerIntegration.Snag(hot, trace, _, _, _) =>
        sb.appendln(hot)
        sb.appendln(trace)
      }
      var saves: List[String] = Nil
      def save(p: Path): Path = {
        saves = upload(crumbSource, id, p, deleteUncompressed = true, deleteUploadedFile = false) :: saves
        p
      }

      // Dump all this text to a temp file and upload if possible.
      writeToTmpAndLog(s"$prefix-diagnostics", crumbSource, id, sb.toString, doLog = doLog).map(save)

      // Upload jemalloc dump if available
      Option(GCNative.jemallocDump(tmpPath(s"$prefix-jemalloc", suffix = "prof"))).map(save)

      for (a <- apSnag; p <- a.path) save(_)

      if (taskDump)
        runningTask.foreach { t =>
          val path = tmpPath(s"$prefix-task", "task")
          val o = new ObjectOutputStream(new FileOutputStream(path.toFile))
          val wrote =
            try {
              o.writeObject(t)
              true
            } catch {
              case e: IOException => // including serialization exception
                dropCrumb(crumbSource, id, s"Failed to write task to $path: $e", Some(path), logger = logger)
                false
            } finally {
              o.close()
            }
          if (wrote) {
            dropCrumb(crumbSource, id, s"Wrote running task to $path", Some(path), logger = logger)
            saves = upload(crumbSource, id, path, deleteUncompressed = true, deleteUploadedFile = false) :: saves
          }
        }

      if (heapDump) {
        val path = tmpPath(s"$prefix-jmap", "hprof")
        dropCrumb(crumbSource, id, s"Dumping heap to $path", Some(path), logger = logger)
        val mxBean = ManagementFactory.newPlatformMXBeanProxy(
          ManagementFactory.getPlatformMBeanServer(),
          "com.sun.management:type=HotSpotDiagnostic",
          classOf[HotSpotDiagnosticMXBean])
        mxBean.dumpHeap(path.toString, !DiagnosticSettings.fullHeapDumpOnKill)

        if (DiagnosticSettings.runOmatEnabled) {
          if (memoryDxCommand.nonEmpty) {
            val tmp = tmpPath("memDiag")
            val csv = tmp.resolve(nodeStatsFile + ".csv")
            val command = List(memoryDxCommand, "--nodes", path.toString, "-o", tmp.toString)
            saves = execute(command, -1L).output :: saves
            saves = upload(crumbSource, id, csv, deleteUncompressed = true, deleteUploadedFile = false) :: saves
          }
        }

      }
      saves.mkString(";")
    } else "disabled"
  }

  def decodeToFile(content: String, path0: Path, tmpDir: Option[Path] = None): Try[Path] = {
    import scala.jdk.CollectionConverters._
    val path = tmpDir.fold(path0) { tmp =>
      val it = path0.iterator()
      // Don't write to the absolute path that we pulled out of splunk
      if (path0.isAbsolute) it.next()
      // Rescue bidirectional paths
      it.asScala.foldLeft(tmp) { case (acc, f) =>
        if (f.toString == "..")
          acc.getParent
        else if (f.toString == ".")
          acc
        else
          acc.resolve(f)
      }
    }
    Files.createDirectories(path.getParent)
    Using.Manager { use =>
      val decoder = Base64.getDecoder
      val bytes = decoder.decode(content)
      val out = use(Files.newOutputStream(path))
      out.write(bytes)
      path
    }
  }
}
