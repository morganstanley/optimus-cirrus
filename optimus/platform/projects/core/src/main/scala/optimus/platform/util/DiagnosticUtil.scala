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

import optimus.graph.DiagnosticSettings
import optimus.graph.diagnostics.InfoDumper
import optimus.platform.EvaluationContext
import org.apache.commons.lang3.SystemUtils

import java.io._
import java.lang.management.ManagementFactory
import java.net.InetAddress
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

object DiagnosticUtil extends Log {
  val dumpToTempFile = DiagnosticSettings.getBoolProperty("optimus.writeDiagnosticFileOnPanic", false)

  def dumpDiagnosticsAndExit(header: String, exitCode: Int, exception: Throwable): Nothing = {
    val spacer = System.lineSeparator * 2
    val tempFile = if (dumpToTempFile) Some(File.createTempFile("optimus.diagnostic", ".out")) else None
    val fileLocationMessage = tempFile
      .map { f =>
        s"writing diagnostic file to: ${InetAddress.getLocalHost.getHostName}: ${f.getAbsolutePath}$spacer"
      }
      .getOrElse("")

    val diagnostics = diagnosticString(exception)
    val message = s"${header}${spacer}$fileLocationMessage${diagnostics}"

    log.error(message)

    // jenkins does not capture unit test logger output
    System.err.println(message)

    tempFile.foreach { f =>
      val pw = new PrintWriter(new FileOutputStream(f))
      try {
        pw.println(message)
        pw.flush()
      } catch {
        case NonFatal(_) =>
      } finally {
        pw.close()
      }
    }

    System.out.flush()
    System.err.flush()
    try {
      // try to make sure buffers are cleared out before exiting
      FileDescriptor.err.sync()
      FileDescriptor.out.sync()
    } catch {
      case NonFatal(_) =>
    }

    // just in case, give the process another second to clean up
    Thread.sleep(1000)

    sys.exit(exitCode)
  }

  def diagnosticString(t: Throwable): String = {
    val sb = new StringBuilder

    object Appender {
      def append(f: => String): Unit =
        try {
          sb.append(f).append(System.lineSeparator)
        } catch {
          case NonFatal(_) => // we're already in the process of failing, don't capture further errors
        }

      def append(label: String, f: => String): Unit = append(s"$label: $f")
      def newLine() = append("")
      def append(t: Throwable): Unit = append {
        val sb = new StringBuilder
        val sw = new StringWriter()
        val pw = new PrintWriter(sw)
        t.printStackTrace(pw)
        pw.flush()
        pw.close()
        sw.getBuffer.toString
      }
    }
    import Appender._

    append("A fatal error occurred, detailed diagnostic information to follow")
    newLine()

    append("EXCEPTION DETAILS")
    append(t)
    newLine()

    append("PROCESS INFORMATION")

    val mf = ManagementFactory.getRuntimeMXBean
    append("runtimeName", mf.getName)
    append("vmName", mf.getVmName)
    append("vmVendor", mf.getVmVendor)
    append("vmVersion", mf.getVmVersion)
    append("specName", mf.getSpecName)
    append("specVendor", mf.getSpecVendor)
    append("specVersion", mf.getSpecVersion)
    append("managementSpecVersion", mf.getManagementSpecVersion)
    append("classpath", mf.getClassPath)
    append("libraryPath", mf.getLibraryPath)
    append("bootClasspath", if (mf.isBootClassPathSupported) mf.getBootClassPath else "n/a")
    append("inputArguments", mf.getInputArguments.asScala.mkString(System.lineSeparator))
    newLine()

    append("PROCESS STATUS")
    append("Available processors (cores)", Runtime.getRuntime().availableProcessors().toString)
    append("Free memory (MBs)", "" + Runtime.getRuntime().freeMemory() / 1024 / 1024)
    append(
      "Maximum memory (MBs)", {
        val maxMemory = Runtime.getRuntime().maxMemory()
        if (maxMemory == Long.MaxValue) "no limit" else { "" + maxMemory / 1024 / 1024 }
      })
    append("Allocated memory (MBs)", "" + Runtime.getRuntime().totalMemory() / 1024 / 1024)
    newLine()

    append("JAVA SYSTEM PROPERTIES")
    for ((k, v) <- System.getProperties.asScala) append(k, v)
    newLine()

    append("PROCESS ENVIRONMENT")
    for ((k, v) <- System.getenv.asScala) append(k, v)
    newLine()

    append("WAITER CHAIN")
    append(EvaluationContext.currentNode.waitersToFullMultilineNodeStack(true, new PrettyStringBuilder).toString)
    newLine()

    append("THREAD STACKS")
    for ((thread, stack) <- Thread.getAllStackTraces.asScala) {
      append(s"[${thread.getName}]${if (thread.isDaemon) " daemon" else ""} ${thread.getState}")
      for (el <- stack) append(el.toString)
      newLine()
    }

    // be a bit excessive deciding where to try this
    if (!SystemUtils.IS_OS_WINDOWS) {
      val pid = getPID
      val javaHome = System.getProperty("java.home")
      executeAndAppend(sb, List("/usr/bin/free"))
      executeAndAppend(sb, List("cat", "/proc/meminfo"))
      executeAndAppend(sb, List("cat", s"/proc/$pid/status"))
      executeAndAppend(sb, List("ps", "auxfww"))
      // jmap and jstack can deadlock on Java 25 unless we buffer
      executeAndAppend(sb, List(InfoDumper.jmap, "-histo:live", pid), bufferWithFile = true)
      executeAndAppend(sb, List(InfoDumper.jstack, pid), bufferWithFile = true)
    }

    sb.toString
  }

  private def getPID: Long = ProcessHandle.current().pid()

  private def executeAndAppend(sb: StringBuilder, cmd: List[Any], bufferWithFile: Boolean = false): Unit = {
    sb.append(InfoDumper.execute(cmd.map(_.toString), bufferWithFile = bufferWithFile).output)
    sb.append(System.lineSeparator)
  }
}
