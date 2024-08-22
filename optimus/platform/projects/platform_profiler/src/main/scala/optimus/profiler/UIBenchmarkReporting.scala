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
package optimus.profiler

import optimus.graph.AuxiliaryScheduler
import optimus.graph.OGTrace
import optimus.graph.OGTraceReader
import optimus.graph.diagnostics.OGCounterView
import optimus.graph.diagnostics.messages.BlockingWaitCounter
import optimus.graph.diagnostics.messages.DALRequestCounter
import optimus.graph.diagnostics.messages.HandlerStepEventCounter
import optimus.graph.diagnostics.messages.OGEventWithComplete
import optimus.graph.diagnostics.messages.StartupEventCounter
import optimus.profiler.ui.NodeTimeLine._

import java.io.File
import scala.jdk.CollectionConverters._

// TODO (OPTIMUS-54356): add classloading time
final case class UIBenchmarkProfileInfo(
    totalWallTime: Long,
    totalGraphCPUTime: Long,
    totalStarts: Long,
    totalNonAuxBlockingTime: Long,
    totalHandlerStepTime: Long,
    totalStartupEventTime: Long,
    totalDALResults: Int)

object UIBenchmarkReporting {

  private def totalDuration[T <: OGEventWithComplete](counter: OGCounterView[T]): Long =
    counter.events.asScala.map(_.duration).sum

  def profileInfo(traceFiles: Seq[File]): UIBenchmarkProfileInfo = {
    generateProfileInfo(OGTrace.readingFromFiles(traceFiles.toArray))
  }

  // reading single file by file name
  def profileInfo(traceFileName: String): UIBenchmarkProfileInfo = {
    generateProfileInfo(OGTrace.readingFromFile(traceFileName))
  }

  def generateProfileInfo(reader: OGTraceReader): UIBenchmarkProfileInfo = {
    val counters = reader.getOGCounters
    val hotspots = reader.getHotspots.asScala
    val threads = reader.getThreads
    val (firstStartTime, lastFinishTime) = wallTimeRange(threads, counters)
    val totalWallTime = lastFinishTime - firstStartTime
    val totalGraphCPUTime = hotspots.map(p => p.selfTime + p.postCompleteAndSuspendTime).sum
    val totalStarts = hotspots.map(_.start).sum
    val blockingEvents = getCounter(counters, BlockingWaitCounter).events.asScala
    val nonAuxBlockingEvents = blockingEvents.filterNot(_.threadName.contains(AuxiliaryScheduler.name))
    val totalNonAuxBlockingTime = nonAuxBlockingEvents.map(_.duration).sum
    val totalHandlerStepTime = totalDuration(getCounter(counters, HandlerStepEventCounter))
    val totalStartupEventTime = totalDuration(getCounter(counters, StartupEventCounter))
    val totalDALResults = getCounter(counters, DALRequestCounter).events.asScala.map(_.batchSize).sum
    UIBenchmarkProfileInfo(
      totalWallTime,
      totalGraphCPUTime,
      totalStarts,
      totalNonAuxBlockingTime,
      totalHandlerStepTime,
      totalStartupEventTime,
      totalDALResults)
  }

  def stepProfileInfo(traceFileName: String, traceFileFullName: String): (String, UIBenchmarkProfileInfo) = {
    (traceFileName, profileInfo(traceFileFullName))
  }
}
