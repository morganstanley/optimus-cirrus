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

import optimus.platform.util.InfoDumpUtils

/**
 * OS information for diagnostic purposes. Used in StallDetector
 */
object OSInformation extends InfoDumpUtils {
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

  def dumpPidInfo(
      sb: java.lang.StringBuilder,
      pid: Long,
      maxHistoLength: Option[Int] = None,
      timeoutMs: Long = 0,
      stacks: Boolean = true
  ): Unit = {
    sb.append("Collecting information about PID : " + pid + "\n")
    val T = expirTime(timeoutMs)
    val cheapCommands =
      List(
        List("/usr/bin/free"),
        List("/usr/bin/cat", "/proc/meminfo"),
        List("/usr/bin/cat", s"/proc/$pid/status"),
        List("/usr/bin/ps", "auxfww"),
      )
    cheapCommands.foreach(cmd => sb.append(execute(cmd, T).output))

    if (stacks) {
      // jmap and jstack can deadlock (when invoked on current process) on Java 25 unless we buffer
      val buffer = pid == ProcessHandle.current().pid
      sb.append(execute(List(InfoDumpUtils.jstack, pid.toString), bufferWithFile = buffer).output)
        .append(System.lineSeparator)
      sb.append(execute(List(InfoDumpUtils.jmap, "-histo:live", pid.toString), bufferWithFile = buffer).output)
        .append(System.lineSeparator)
    }
  }

  def getPID: Long = ProcessHandle.current().pid()

}
