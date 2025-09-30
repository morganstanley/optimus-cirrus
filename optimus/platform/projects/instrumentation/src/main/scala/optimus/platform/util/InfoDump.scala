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

package optimus.platform.util
import com.typesafe.config.ConfigFactory
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.DiagPropertiesCrumb
import optimus.breadcrumbs.crumbs.LogPropertiesCrumb
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.Properties.Elems
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.utils.FileUtils
import optimus.utils.MiscUtils.Endoish._
import org.apache.commons.io

import java.nio.file._
import java.time.Instant
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.concurrent.TimeUnit
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import optimus.utils.FileUtils.tmpPath

import java.io.Reader
import java.lang.management.ManagementFactory
import java.nio.charset.Charset
import java.util.Base64
import java.util.Objects
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import scala.jdk.CollectionConverters._
import scala.util.Using
import scala.util.control.NonFatal

object InfoDumpUtils extends InfoDumpUtils {
  final case class ExecutionResult(code: Int, output: String)
  private def nul[T] = null.asInstanceOf[T]
  private val executor = Executors.newScheduledThreadPool(1)

  private[util] def close(r: AutoCloseable): Unit = if (r != nul)
    try r.close()
    catch { case _: Exception => }

  // Drain the reader to the string builder, with optional timeout and max lines
  private[util] def drain(
      sb: StringBuilder,
      isr: => () => Reader,
      maxLines: Int = 0,
      timeoutMs: Long = 0,
      logging: Boolean = true): Int = {
    @volatile var br: BufferedReader = nul
    val thread = new Thread(new Runnable {
      override def run(): Unit = { br = new BufferedReader(isr()) }
    })
    thread.setDaemon(true)
    thread.start()
    thread.join(timeoutMs)
    if (Objects.isNull(br)) {
      if (logging) {
        sb.append("Timeout creating input stream for task!")
      }
      TIMEOUT
    } else {
      @volatile var timedout = false
      // Forcibly close the reader after the timeout
      val e: ScheduledFuture[_] =
        if (timeoutMs > 0)
          executor.schedule(
            new Runnable {
              override def run(): Unit = {
                timedout = true
                close(br)
              }
            },
            timeoutMs,
            TimeUnit.MILLISECONDS)
        else nul
      val n = if (maxLines > 0) maxLines else Int.MaxValue
      try {
        // This may get interrupted by the timeout thread
        br.lines.iterator().asScala.take(n).foreach { line =>
          sb.append(line)
          sb.append("\n")
        }
        if (e != nul) e.cancel(false)
        0
      } catch {
        case NonFatal(e) =>
          if (timedout) {
            if (logging)
              sb.append("Timed out\n")
            TIMEOUT
          } else {
            if (logging)
              sb.append(s"Exception reading stream: $e\n")
            EXCEPTION
          }
      } finally {
        close(br)
      }
    }
  }

}

trait InfoDumpUtils extends Log {
  import InfoDumpUtils._
  def getMeta: Properties.Elems = Elems.Nil
  val MAX_FILE_SIZE = 30 * 1000 * 1000
  val MAX_CRUMB_SIZE = 35 * 1000 * 1000

  def upload(crumbSource: Crumb.Source, path: Path): String =
    upload(crumbSource, path, deleteUncompressed = true, deleteUploadedFile = false)
  def upload(crumbSource: Crumb.Source, path: Path, deleteUncompressed: Boolean, deleteUploadedFile: Boolean): String =
    upload(crumbSource, ChainedID.root, path, deleteUncompressed, deleteUploadedFile)
  def upload(
      crumbSource: Crumb.Source,
      id: ChainedID,
      path: Path,
      deleteUncompressed: Boolean,
      deleteUploadedFile: Boolean,
      props: Properties.ElemOrElems*): String =
    if (path == nul) // might be called from java
      "no path to upload"
    else {
      splunkUpload(
        crumbSource,
        id,
        path,
        Some(getMeta ::: Properties.Elems(props: _*)),
        deleteUncompressed,
        deleteUploadedFile) match {
        case Failure(exception) => s"failed to upload with exception: $exception"
        case Success(value)     => value
      }
    }

  def splunkUpload(
      source: Crumb.Source,
      id: ChainedID,
      path: Path,
      meta: Option[Elems] = None,
      deleteUncompressed: Boolean = true,
      deleteUploadedFile: Boolean = false
  ): Try[String] = {
    compressAndEncode(path, MAX_FILE_SIZE, deleteUncompressed).flatMap { case (zipPath, b64) =>
      val fileName = zipPath.toString
      if (b64.size > MAX_CRUMB_SIZE) {
        val msg = s"Encoded $fileName exceeds $MAX_CRUMB_SIZE bytes"
        log.error(msg)
        Failure(new IllegalArgumentException(msg))
      } else {
        val uid = id.child
        Breadcrumbs.info(
          uid,
          DiagPropertiesCrumb(
            _,
            source,
            Properties.file -> fileName ::
              Properties.fileContents -> b64 ::
              meta :::
              Version.verboseProperties))
        dropCrumb(source, uid, s"base64 upload", Some(zipPath))
        if (deleteUploadedFile) Try(Files.delete(zipPath))
        Breadcrumbs.flush()
        Success(fileName)
      }
    }
  }

  // Returns path of compressed file and base64 encoded content
  def compressAndEncode(
      path: Path,
      maxZippedFileSize: Long,
      deleteUncompressed: Boolean = true): Try[(Path, String)] = {
    FileUtils.zstdCompress(path, deleteUncompressed).flatMap { zipped =>
      if (Files.size(zipped) > maxZippedFileSize) {
        val msg = s"File $zipped exceeded $maxZippedFileSize bytes"
        log.error(msg)
        Failure(new IllegalStateException(msg))
      } else
        Using.Manager { use =>
          val input = use(Files.newInputStream(zipped))
          val bytes = io.IOUtils.toByteArray(input)
          val encoder = Base64.getEncoder
          val contents = encoder.encodeToString(bytes)
          (zipped, contents)
        }
    }
  }

  protected def writeToTmpAndLog(
      prefix: String,
      crumbSource: Crumb.Source,
      id: ChainedID,
      msg: => String,
      deleteOnExit: Boolean = false,
      doLog: Boolean = true
  ): Option[Path] = {
    val path: Path = tmpPath(prefix, "log")
    try {
      val m = msg
      io.FileUtils.write(path.toFile, m, Charset.defaultCharset())
      dropCrumb(crumbSource, id, s"Writing diagnostics to $path", Some(path))
      if (doLog) log.warn(m)
      if (deleteOnExit)
        path.toFile.deleteOnExit()
      Some(path)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Unable to write to $path: $e")
        None
    }
  }

  protected def dropCrumb(crumbSource: Crumb.Source, id: ChainedID, msg: String, file: Option[Path]): Unit = {
    log.info(s"${Instant.now} InfoDumper id=$id " + file.fold(msg)(f => s"$msg $f"))
    Breadcrumbs.warn(
      id,
      LogPropertiesCrumb(
        _,
        crumbSource,
        (Properties.logMsg -> s"InfoDumper:$msg") ::
          file.map(Properties.file -> _.toString) ::
          Version.verboseProperties))
  }

  def expirTime(timeoutMs: Long): Long = {
    if (timeoutMs <= 0) Long.MaxValue
    else if (timeoutMs > 365 * 24 * 3600 * 1000L) timeoutMs
    else System.currentTimeMillis() + timeoutMs
  }

  def execute(sb: StringBuilder, timeoutMs: Long, maxLines: Int, logging: Boolean, cmd: String, args: Any*): Int = {
    val ExecutionResult(code, ret) = execute((cmd :: args.map(_.toString).toList), timeoutMs, maxLines, logging)
    sb.append(ret)
    code
  }

  def execute(sb: StringBuilder, cmd: String, args: Any*): Int =
    execute(sb, 30000L, 0, true, cmd, args: _*)

  def execute(cmd: String, args: Any*): ExecutionResult =
    execute((cmd :: args.map(_.toString).toList), 30000L, 0, true)

  // Execute the command
  def execute(cmd: List[String], timeoutMs: Long, maxLines: Int = 0, logging: Boolean = true): ExecutionResult = {
    val sb = new StringBuilder
    if (logging) {
      val msg = s"Executing command: ${cmd.mkString(" ")}"
      log.info(msg)
      sb.append(s"$msg\n")
    }
    val pb = new ProcessBuilder(cmd: _*)
    Try(pb.start()) match {
      case Success(p) =>
        val isr = () => new InputStreamReader(p.getInputStream)
        var code = drain(sb, isr, maxLines, timeoutMs, logging)
        if (code == TIMEOUT) {
          if (logging) {
            log.info("Timed out executing command")
            sb.append("Timed out executing command\n")
          }
        } else if (code == 0) {
          if (p.isAlive)
            p.destroyForcibly()
          else
            code = p.exitValue()
        }
        if (code != 0 && logging) {
          sb.append(s"Command exited abnormally: $code\n")
        }
        ExecutionResult(code, sb.toString)
      case Failure(e) =>
        ExecutionResult(999, s"Exception running command $e")
    }
  }

  private val sep = FileSystems.getDefault.getSeparator
  val jbin = System.getProperty("java.home") + sep + "bin" + sep
  val javaExecutable = s"${jbin}java"
  val jstack = s"${jbin}jstack"
  val jmap = s"${jbin}jmap"

  private lazy val config = ConfigFactory.parseResources("main/internal/utils.conf")

  val euStack: Option[String] = config.getString("eu-stack").split(',').find(p => Files.isExecutable(Paths.get(p)))

  val TIMEOUT = 137
  val EXCEPTION = 998

  def moduleArgs: Seq[String] = {
    val bean = ManagementFactory.getRuntimeMXBean
    val ourOpts = bean.getInputArguments.asScala.to(Seq)
    ourOpts.filter(o => o.startsWith("--add-exports") || o.startsWith("--add-opens"))
  }
}

// Service-loaded trait
trait InfoDump extends Log with InfoDumpUtils {
  def kill(
      prefix: String,
      msgs: List[String] = Nil,
      code: Int = -1,
      crumbSource: Crumb.Source = Crumb.RuntimeSource,
      exceptions: Array[Throwable] = Array.empty[Throwable]): Unit

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
      emergency: Boolean = false,
      doLog: Boolean = true
  ): String

}

object InfoDump extends Log {
  def ensureInitialized(): Unit = {}

  lazy private val dumpers = {
    val ret = ServiceLoaderUtils.all[InfoDump].applyIf(_.isEmpty)(_ :+ Default)
    log.info(s"Loaded dumpers $ret")
    ret
  }

  // A serializable object to be persisted for later inspection.
  @volatile private var runningTask: Option[Object] = None

  // attach extra properties to serializable object
  @volatile private var meta: Properties.Elems = Elems.Nil

  private object Default extends InfoDump with Log {
    def kill(
        prefix: String,
        msgs: List[String] = Nil,
        code: Int = -1,
        crumbSource: Crumb.Source = Crumb.RuntimeSource,
        exceptions: Array[Throwable] = Array.empty[Throwable]): Unit = {
      val e = new RuntimeException("Fatal error")
      val msg =
        s"Fatal error prefix=$prefix, msgs=$msgs, code=$code, source=$crumbSource, exception=${exceptions.mkString(" ,")}"
      exceptions.foreach(e.addSuppressed)
      log.warn(msg, e)
      Breadcrumbs.error(ChainedID.root, PropertiesCrumb(_, crumbSource, Properties.logMsg -> msg))
      Runtime.getRuntime.exit(code)
      throw e
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
        emergency: Boolean = false,
        doLog: Boolean = true
    ): String = {
      val msg = s"prefix=$prefix, msgs=$msgs, source=$crumbSource,  exception=${exceptions.mkString(" ,")}"
      if (doLog) log.info(msg)
      Breadcrumbs.info(ChainedID.root, PropertiesCrumb(_, crumbSource, Properties.logMsg -> msg))
      msg
    }

  }

  def kill(
      prefix: String,
      msgs: List[String] = Nil,
      code: Int = -1,
      crumbSource: Crumb.Source = Crumb.RuntimeSource,
      exceptions: Array[Throwable] = Array.empty[Throwable]): Unit =
    dumpers.foreach(_.kill(prefix, msgs, code, crumbSource, exceptions))

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
  ): String = dumpers
    .map(_.dump(prefix, msgs, crumbSource, id, description, heapDump, taskDump, noMore, exceptions, emergency))
    .mkString(";")

}
