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

import java.io.BufferedReader
import java.io.InputStreamReader

import scala.util.Try

//dont use scala StringBuilder
import java.lang.StringBuilder

/**
 * OS information for diagnostic purposes. Used in StallDetector
 */
object OSInformation {
  val isWindows: Boolean = System.getProperty("os.name").toLowerCase.indexOf("win") >= 0
  val isUnix: Boolean = !isWindows

  def dumpRuntimeInfo(sb: java.lang.StringBuilder): Unit = {
    sb.append("Available processors (cores): ")
    sb.append(Runtime.getRuntime.availableProcessors())
    sb.append('\n')
    sb.append("Free memory (MBs): ")
    sb.append(Runtime.getRuntime.freeMemory() / 1024 / 1024)
    sb.append('\n')
    val maxMemory = Runtime.getRuntime.maxMemory()
    sb.append("Maximum memory (MBs): ")
    if (maxMemory == Long.MaxValue) "no limit" else sb.append(maxMemory / 1024 / 1024)
    sb.append('\n')
    sb.append("Allocated memory (MBs): ")
    sb.append(Runtime.getRuntime.totalMemory() / 1024 / 1024)
    sb.append('\n')
  }

  private val jbin = System.getProperty("java.home") + "/bin"
  private val jstack = s"$jbin/jstack"

  def dumpPidInfo(sb: java.lang.StringBuilder, pid: Long, maxHistoLength: Option[Int] = None): Unit = {
    sb.append("Collect the information about PID : " + pid + "\n")
    val commands =
      List(
        List("/usr/bin/free"),
        List("/usr/bin/cat", "/proc/meminfo"),
        List("/usr/bin/cat", s"/proc/$pid/status"),
        List("/usr/bin/ps", "auxfww"),
        List(jstack, pid.toString)
      )
    commands.map(cmd => sb.append(execute(cmd)))
    dumpHisto(sb, pid, maxHistoLength)
  }

  private def dumpHisto(sb: java.lang.StringBuilder, pid: Long, maxHistoLength: Option[Int]): Unit = {
    val histo = execute(List("jmap", "-histo:live", pid.toString))
    if (maxHistoLength.isEmpty) {
      sb.append(histo)
    } else {
      sb.append(histo.linesIterator.take(maxHistoLength.get).mkString(System.lineSeparator()))
    }
  }

  def getPID: Long = ProcessHandle.current().pid()

  // Execute the command
  private[graph] def execute(cmd: List[String]): String = {
    val sb = new StringBuilder
    sb.append("Execute the command [").append(cmd).append("]\n")
    val p = Try(Runtime.getRuntime.exec(cmd.toArray))
    if (p.isSuccess) {
      try {
        val br = new BufferedReader(new InputStreamReader(p.get.getInputStream))
        var line = br.readLine
        while (line != null) {
          sb.append(line).append('\n')
          line = br.readLine
        }

      } catch {
        case e: Exception =>
          sb.append("Got exception while executing the command - " + e.toString)
      } finally {
        p.get.destroy()
      }
    } else {
      sb.append("Got exception while executing the command - " + p.failed.getOrElse("").toString)
    }
    sb.toString
  }
}
