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

import scala.annotation.nowarn
import java.io.StringWriter
import java.lang.{Boolean => JBoolean}
import java.util
import scala.collection.compat._
import optimus.graph.cache.Caches
import optimus.graph.cache.CauseGraphMBean
import optimus.graph.cache.CauseProfiler
import optimus.graph.diagnostics.GraphDiagnostics
import optimus.graph.diagnostics.InfoDumper
import optimus.graph.diagnostics.PNodeInvalidate
import optimus.graph.diagnostics.PNodeTask
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.diagnostics.PerPropertyStats
import optimus.graph.diagnostics.PerTrackerStats
import optimus.graph.diagnostics.ProfilerUIControl
import optimus.graph.diagnostics.SchedulerProfileEntryForUI
import optimus.graph.diagnostics.WaitProfile
import optimus.graph.diagnostics.gridprofiler.GridProfiler
import optimus.graph.diagnostics.gridprofiler.SummaryTable
import optimus.graph.diagnostics.pgo.Profiler
import optimus.graph.outOfProcess.views.CacheView
import optimus.graph.outOfProcess.views.CacheViewHelper
import optimus.graph.outOfProcess.views.MemoryView
import optimus.graph.outOfProcess.views.MemoryViewHelper
import optimus.graph.outOfProcess.views.ScenarioStackProfileView
import optimus.graph.tracking.DependencyTrackerRoot
import optimus.platform.inputs.GraphInputConfiguration

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer

class GCInfo(var name: String, var count: Long, var time: Long) extends Serializable {
  override def toString: String = name + "[" + count + ", " + time + "ms]"
}
class CacheStats extends Serializable {
  var densities: Array[Double] = _
  var clearAllCount: Int = 0
  var totalCacheSize = 0L
  var countOfRootReachableObjectsOfInterest = 0 // Total count of objects of ClsOfInterest reached from root
  var gcInfos: Array[GCInfo] = _

  def gcTime: Long = gcInfos(0).time + gcInfos(1).time
  def gcCount: Long = gcInfos(0).count + gcInfos(1).count
  var exceptionText: String = _ // In case exception happened
}

trait GraphMBean {
  def getConsoleFlag: Boolean
  def clearCache(name: String, filter: PropertyNode[_] => Boolean): Unit
  def go(): Unit
  def profilerResetAll(): Unit
  def setFlag(flag: NodeTrace.WithChangeListener[JBoolean], value: Boolean): Unit
  def resetWaits(): Unit
  def getNativeAllocation: Long
  def getNativeWatermark: Long
  def setNativeWatermark(size: Long): Unit
  def clearAllCaches(): Unit
  def clearAllCachesWithLocal(): Unit
  def getSSRoots: Iterable[ScenarioStackProfileView]
  def resetProfileData(): Unit
  def getProfileData: String
  def getLiveReaderTaskInfo: util.ArrayList[PNodeTaskInfo]
  def collectProfile(resetAfter: Boolean, alsoClearCache: Boolean): Array[PNodeTaskInfo]
  def getCache: ArrayBuffer[CacheView]
  def profileCacheMemory(collectGCNative: Boolean): Unit
  def profileCacheMemory(precise: Boolean, clsOfInterest: String): CacheStats
  def getSchedulerProfileEntry: ArrayBuffer[(String, SchedulerProfileEntryForUI)]
  def getStallTimes: ArrayBuffer[OGSchedulerTimes.StallDetailedTime]
  def getMemory(precise: Boolean): ArrayBuffer[MemoryView]
  def saveProfile(fileName: String): Unit
  def getInvalidates: ArrayBuffer[PNodeInvalidate]
  def getTrackingScenarioInfo: (ArrayBuffer[PerPropertyStats], ArrayBuffer[PerTrackerStats])
  def getWaits: ArrayBuffer[WaitProfile]
  def showGraphConsole(): Unit
  def setTraceMode(mode: String): Unit
  def getTraceMode: String
  def getTrace: Seq[PNodeTask]
  def getSummaryTable: SummaryTable
  def gc(): Unit
  def clearCacheAndTracking(): Unit
  def resetSStackRoots(): Unit
  def getFullSchedulerState(): String
  def infoDump(message: String): String
  def asyncProfilerCommand(cmd: String): String
  def asyncProfilerStart(intervalMS: Int, sectionSec: Int): String
  def asyncProfilerStop(): Unit
  def asyncProfilerStatus(): String
}

class GraphMBeanImpl extends GraphMBean {

  private val INITIAL_STRING_BUFFER_SIZE = 2048
  override def getConsoleFlag: Boolean = DiagnosticSettings.outOfProcessAppConsole
  override def getLiveReaderTaskInfo: util.ArrayList[PNodeTaskInfo] = OGTrace.liveReader().getHotspots
  override def setFlag(flag: NodeTrace.WithChangeListener[JBoolean], value: Boolean): Unit = flag.setValue(value, false)

  override def clearCache(name: String, filter: PropertyNode[_] => Boolean): Unit =
    CacheViewHelper.clearCache(name, filter)

  override def go(): Unit = ProfilerUIControl.consoleGo()
  @nowarn("msg=10500 optimus.graph.tracking.DependencyTrackerRoot.clearCacheAndTracking")
  override def clearCacheAndTracking(): Unit = DependencyTrackerRoot.clearCacheAndTracking(CauseProfiler)
  override def resetSStackRoots(): Unit = ProfilerUIControl.resetSStackUsage()
  override def getTrace: Seq[PNodeTask] = NodeTrace.getTrace.asScalaUnsafeImmutable
  override def profilerResetAll(): Unit = Profiler.resetAll()
  override def getSummaryTable: SummaryTable = GridProfiler.getSummaryTable
  override def gc(): Unit = Runtime.getRuntime.gc()
  override def resetWaits(): Unit = NodeTrace.resetWaits()
  override def getNativeAllocation: Long = GCNative.getNativeAllocation
  override def getNativeWatermark: Long = GCNative.getNativeWatermark
  override def setNativeWatermark(size: Long): Unit = GCNative.setNativeWatermark(size)
  override def clearAllCaches(): Unit = Caches.clearAllCaches(CauseGraphMBean)
  override def clearAllCachesWithLocal(): Unit = Caches.clearAllCachesWithLocal(CauseGraphMBean)
  override def getWaits: ArrayBuffer[WaitProfile] = ProfilerUIControl.getWaits
  override def getStallTimes: ArrayBuffer[OGSchedulerTimes.StallDetailedTime] =
    OGSchedulerTimes.getStallingReasons.asScala.to(ArrayBuffer)
  override def getInvalidates: ArrayBuffer[PNodeInvalidate] = ProfilerUIControl.getInvalidates
  override def getTrackingScenarioInfo: (ArrayBuffer[PerPropertyStats], ArrayBuffer[PerTrackerStats]) = {
    val ttrackStats = DependencyTrackerRoot.ttrackStatsForAllRoots(false).values
    val perPropStats = ttrackStats.flatMap { _.perPropertyStats.values }.to(ArrayBuffer)
    val perTrackerStats = ttrackStats.flatMap { _.perTrackerStats.values }.to(ArrayBuffer)
    (perPropStats, perTrackerStats)
  }
  override def getCache: ArrayBuffer[CacheView] = CacheViewHelper.getCache
  override def getMemory(precise: Boolean): ArrayBuffer[MemoryView] = MemoryViewHelper.getMemory(precise)
  override def getSchedulerProfileEntry: ArrayBuffer[(String, SchedulerProfileEntryForUI)] =
    OGTrace.liveReader.getSchedulerProfiles.asScala.to(ArrayBuffer)
  override def getSSRoots: Iterable[ScenarioStackProfileView] = ProfilerUIControl.getSSRoots

  override def resetProfileData(): Unit = Profiler.resetAll()
  override def getProfileData: String = {
    val out = new StringWriter(INITIAL_STRING_BUFFER_SIZE)
    Profiler.writeProfileData(out)
    out.toString
  }

  override def collectProfile(resetAfter: Boolean, alsoClearCache: Boolean): Array[PNodeTaskInfo] = {
    val r = Profiler.getProfileData()
    if (resetAfter) resetProfileData()
    if (alsoClearCache) clearAllCaches()
    r.toArray
  }

  override def showGraphConsole(): Unit = ProfilerUIControl.startGraphDebugger()
  override def setTraceMode(mode: String): Unit = GraphInputConfiguration.setTraceMode(mode)
  override def getTraceMode: String = OGTrace.getTraceMode.name

  override def profileCacheMemory(collectGCNative: Boolean): Unit = {}

  override def profileCacheMemory(precise: Boolean, clsOfInterestPrefix: String): CacheStats =
    new CacheStats

  def saveProfile(fileName: String): Unit = Profiler.saveProfile(fileName)

  override def getFullSchedulerState(): String = GraphDiagnostics.getGraphState

  override def infoDump(message: String): String = InfoDumper.dump("jmx", message :: Nil)

  override def asyncProfilerCommand(cmd: String): String = AsyncProfilerIntegration.command(cmd)

  override def asyncProfilerStart(intervalMS: Int, sectionSec: Int): String = {
    AsyncProfilerIntegration
      .continuousSamplingStart(
        if (intervalMS > 0) intervalMS else 10,
        if (sectionSec > 0) sectionSec else 300,
        event = "cpu")
      .fold("Failed")(_.toString)
  }

  override def asyncProfilerStop(): Unit = AsyncProfilerIntegration.continuousSamplingStop()

  override def asyncProfilerStatus(): String = AsyncProfilerIntegration.samplingStatus()
}
