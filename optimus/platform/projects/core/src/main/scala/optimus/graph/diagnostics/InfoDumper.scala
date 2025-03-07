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
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Base64
import java.util.zip.GZIPOutputStream
import com.sun.management.HotSpotDiagnosticMXBean
import org.apache.commons.io
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.DiagPropertiesCrumb
import optimus.breadcrumbs.crumbs.LogPropertiesCrumb
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
import optimus.logging.LoggingInfo
import optimus.platform.util.InfoDump
import optimus.platform.util.Log
import optimus.platform.util.PrettyStringBuilder
import optimus.platform.util.Version
import optimus.utils.FileUtils
import org.apache.commons.io.IOUtils

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.Using
import scala.util.control.NonFatal
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
      exceptions: Array[Throwable] = Array.empty[Throwable]): Unit =
    InfoDumper.kill(prefix, msgs, code, crumbSource, exceptions)

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
      emergency: Boolean = false
  ): String =
    InfoDumper.dump(prefix, msgs, crumbSource, id, description, heapDump, taskDump, noMore, exceptions, emergency)

}

object InfoDumper extends Log {

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

  private def dropCrumb(crumbSource: Crumb.Source, id: ChainedID, msg: String, file: Option[Path]): Unit = {
    System.err.println(s"${Instant.now} InfoDumper id=$id " + file.fold(msg)(f => s"$msg $f"))
    Breadcrumbs.warn(
      id,
      LogPropertiesCrumb(
        _,
        crumbSource,
        (Properties.logMsg -> s"InfoDumper:$msg") ::
          file.map(Properties.file -> _.toString) ::
          Version.verboseProperties))
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
      exceptions: Array[Throwable] = Array.empty[Throwable]): Unit = {
    Try { backupKillThread.start() }
    dump(
      prefix,
      msgs = s"Exiting with code $code" :: msgs,
      crumbSource = crumbSource,
      description = "Shutdown",
      heapDump = DiagnosticSettings.fullHeapDumpOnKill,
      taskDump = true,
      noMore = true,
      exceptions = exceptions
    )
    Breadcrumbs.flush()
    if (DiagnosticSettings.fakeOutOfMemoryErrorOnKill) {
      log.warn("About to trigger an OutOfMemoryError")
      new Array[Byte](Int.MaxValue)
    }
    Runtime.getRuntime.exit(code)
  }

  def dump(
      prefix: String,
      msgs: List[String] = Nil,
      crumbSource: Crumb.Source = Crumb.RuntimeSource,
      id: ChainedID = ChainedID.root.child,
      description: String = "Information",
      heapDump: Boolean = false,
      taskDump: Boolean = false,
      noMore: Boolean = false,
      exceptions: Array[Throwable] = Array.empty,
      emergency: Boolean = false
  ): String = this.synchronized {
    if (enabled || emergency) {
      if (noMore) enabled = false
      val msg = ("InfoDumper: " + msgs.mkString(";")).applyIf(noMore)(_ + ";final")
      log.error(msg)
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
            saveType = "jfr",
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
      def save(p: Path): Unit = {
        saves = upload(crumbSource, id, p) :: saves
      }

      // Dump all this text to a temp file and upload if possible.
      val infoPath: Option[Path] =
        writeToTmpLog(s"$prefix-diagnostics", crumbSource, id, sb.toString).flatMap(zip(_).toOption)
      infoPath.foreach(save)

      // Upload jemalloc dump if available
      val jemallocPath =
        Option(GCNative.jemallocDump(tmpPath(s"$prefix-jemalloc", suffix = "prof"))).flatMap(zip(_).toOption)
      jemallocPath.foreach(save)
      for (a <- apSnag; p <- a.path; z <- zip(p)) save(_)

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
                dropCrumb(crumbSource, id, s"Failed to write task to $path: $e", Some(path))
                false
            } finally {
              o.close()
            }
          if (wrote) {
            zip(path).foreach { path =>
              dropCrumb(crumbSource, id, s"Wrote running task to $path", Some(path))
              saves = upload(crumbSource, id, path) :: saves
            }
          }
        }

      if (heapDump) {
        val path = tmpPath(s"$prefix-jmap", "hprof")
        dropCrumb(crumbSource, id, s"Dumping heap to $path", Some(path))
        val mxBean = ManagementFactory.newPlatformMXBeanProxy(
          ManagementFactory.getPlatformMBeanServer(),
          "com.sun.management:type=HotSpotDiagnostic",
          classOf[HotSpotDiagnosticMXBean])
        mxBean.dumpHeap(path.toString, true)

        if (DiagnosticSettings.runOmatEnabled) {
          if (memoryDxCommand.nonEmpty) {
            val tmp = tmpPath("memDiag")
            val csv = tmp.resolve(nodeStatsFile + ".csv")
            val command = List(memoryDxCommand, "--nodes", path.toString, "-o", tmp.toString)
            saves = OSInformation.execute(command) :: saves
            saves = upload(crumbSource, id, csv) :: saves
          }
        }

      }
      saves.mkString(";")
    } else "disabled"
  }

  def writeToTmpLog(prefix: String, crumbSource: Crumb.Source, id: ChainedID, msg: => String): Option[Path] = {
    val path: Path = tmpPath(prefix, "log")
    try {
      val m = msg
      io.FileUtils.write(path.toFile, m, Charset.defaultCharset())
      dropCrumb(crumbSource, id, s"Writing diagnostics to $path", Some(path))
      System.err.println(m) // Deliberate!
      Some(path)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Unable to write to $path: $e")
        None
    }
  }

  private def zip(p: Path): Try[Path] = zip(p, deleteOld = true)
  private def zip(p: Path, deleteOld: Boolean): Try[Path] = {
    import scala.util.Using
    val name = p.getFileName.toString
    if (name.endsWith(".gz")) Success(p)
    else {
      // go to filename's directory and add our filename + gz extension to it
      val pz =
        if (p.getNameCount > 1)
          p.getParent.resolve(p.getFileName.toString + ".gz")
        else
          Paths.get(p.getFileName.toString + ".gz")
      Using.Manager { use =>
        val in = use(Files.newInputStream(p))
        val out = use(new GZIPOutputStream((new FileOutputStream(pz.toFile))))
        in.transferTo(out)
        assert(Files.exists(pz))
        if (deleteOld)
          Try { Files.delete(p) } recover { case e: Throwable => log.warn(s"Failed to delete $p - $e") }
        pz
      }
    }
  }

  def upload(crumbSource: Crumb.Source, path: Path): String = upload(crumbSource, ChainedID.root, path)
  def upload(crumbSource: Crumb.Source, id: ChainedID, path: Path): String = {
    if (path == null) // might be called from java
      "no path to upload"
    else {
      val zipped = zip(path)
        .map { path =>
          val splunk = splunkUpload(crumbSource, id, path, Some(meta)).isSuccess
          if (splunk) s"uploaded $path as $id"
          else s"failed to upload $path as $id"
        }

      zipped match {
        case Failure(exception) => s"failed to upload with exception: $exception"
        case Success(value)     => value
      }
    }
  }

  def encode(path: Path, maxZippedFileSize: Long): Try[String] = {
    zip(path, deleteOld = false).flatMap { path =>
      if (Files.size(path) > maxZippedFileSize) {
        val msg = s"File $path exceeded $maxZippedFileSize bytes"
        log.error(msg)
        Failure(new IllegalStateException(msg))
      } else
        Using.Manager { use =>
          val input = use(Files.newInputStream(path))
          val bytes = IOUtils.toByteArray(input)
          val encoder = Base64.getEncoder
          val contents = encoder.encodeToString(bytes)
          contents
        }
    }
  }

  val MAX_FILE_SIZE = 30 * 1000 * 1000
  val MAX_CRUMB_SIZE = 35 * 1000 * 1000

  // Will zip first if not already done
  def splunkUpload(source: Crumb.Source, id: ChainedID, path: Path, meta: Option[Elems] = None): Try[String] = {
    encode(path, MAX_FILE_SIZE).flatMap { contents =>
      val file = {
        val n = path.toString
        if (n.endsWith(".gz")) n else s"${n}.gz"
      }
      if (contents.size > MAX_CRUMB_SIZE) {
        val msg = s"Encoded $file exceeds $MAX_CRUMB_SIZE bytes"
        log.error(msg)
        Failure(new IllegalArgumentException(msg))
      } else {
        val uid = id.child
        Breadcrumbs.info(
          uid,
          DiagPropertiesCrumb(
            _,
            source,
            Properties.file -> file ::
              Properties.fileContents -> contents ::
              meta :::
              Version.verboseProperties))
        dropCrumb(source, uid, s"base64 upload", Some(path))
        Breadcrumbs.flush()
        Success(file)
      }
    }
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
