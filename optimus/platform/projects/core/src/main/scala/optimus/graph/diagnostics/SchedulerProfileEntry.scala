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

import java.io.CharArrayWriter

import com.opencsv.CSVWriter
import optimus.graph.diagnostics.gridprofiler.GridProfiler

import scala.jdk.CollectionConverters._

// Scheduler statistics for a single thread
final case class SchedulerProfileEntry(
    var graphTime: Long,
    var selfTime: Long,
    var cacheTimeWR: Long,
    var cacheTimeMisc: Long,
    var spinTime: Long,
    var waitTime: Long,
    var cpuTime: Long
) {
  private[diagnostics] def combine(y: SchedulerProfileEntry): SchedulerProfileEntry = copy().add(y)

  def since(y: SchedulerProfileEntry): SchedulerProfileEntry = {
    SchedulerProfileEntry(
      graphTime - y.graphTime,
      selfTime - y.selfTime,
      cacheTimeWR - y.cacheTimeWR,
      cacheTimeMisc - y.cacheTimeMisc,
      spinTime - y.spinTime,
      waitTime - y.waitTime,
      cpuTime - y.cpuTime
    )
  }

  def add(y: SchedulerProfileEntry): SchedulerProfileEntry = {
    graphTime += y.graphTime
    selfTime += y.selfTime
    cacheTimeWR += y.cacheTimeWR
    cacheTimeMisc += y.cacheTimeMisc
    spinTime += y.spinTime
    waitTime += y.waitTime
    cpuTime += y.cpuTime
    this
  }

  def userGraphTime: Long = graphTime - waitTime - spinTime
  def cacheTime: Long = cacheTimeWR + cacheTimeMisc
  def idleTime: Long = 0L
  override def toString: String = {
    " Graph: " + f"${userGraphTime * 1e-6}%.2f" +
      " CPU: " + f"${cpuTime * 1e-6}%.2f" +
      " Wait: " + f"${waitTime * 1e-6}%.2f" +
      " Spin: " + f"${spinTime * 1e-6}%.2f"
  }
}

object SchedulerProfileEntry {
  def apply(): SchedulerProfileEntry = SchedulerProfileEntry(0, 0, 0, 0, 0, 0, 0)

  private[diagnostics] def printCSV(
      agg: Map[String, Map[String, SchedulerProfileEntry]]): (String, Iterable[Map[String, String]]) = {
    // print scheduler thread stats
    val writer = new CharArrayWriter
    val csvWriter = new CSVWriter(writer)
    val header = Array(
      "Engine",
      "Thread",
      "Graph time (s)",
      "Self Time (s)",
      "Cache Time (s)",
      "Spin Time (s)",
      "Wait Time (s)",
      "Idle Time (s)",
      "CPU Time (s)")
    val data =
      for ((engine, metrics) <- agg; (thr, times) <- metrics if times.graphTime != 0)
        yield Array(
          s"$engine",
          s"$thr",
          f"${times.graphTime * 1e-9}%.2f",
          f"${times.selfTime * 1e-9}%.2f",
          f"${times.cacheTime * 1e-9}%.2f",
          f"${times.spinTime * 1e-9}%.2f",
          f"${times.waitTime * 1e-9}%.2f",
          f"${times.idleTime * 1e-9}%.2f",
          f"${times.cpuTime * 1e-9}%.2f"
        )
    if (data.nonEmpty) {
      csvWriter.writeAll((header +: data.toSeq).asJava)
    }
    (writer.toString, GridProfiler.toMaps(header, data))
  }
}

// this adds fields populated by OGTraceReader based on OGTraceCounter traces
final case class SchedulerProfileEntryForUI(
    prf: SchedulerProfileEntry,
    var gcTime: Long,
    var cMonitorTime: Long
)
