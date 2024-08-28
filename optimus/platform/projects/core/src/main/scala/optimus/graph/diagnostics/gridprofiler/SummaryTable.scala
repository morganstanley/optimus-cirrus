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
package optimus.graph.diagnostics.gridprofiler

import optimus.graph.DiagnosticSettings
import optimus.graph.OGSchedulerTimes.StallDetailedTime
import optimus.graph.Settings
import optimus.graph.diagnostics.ThreadStatUtil.ThreadStateSummary

import scala.collection.mutable.ArrayBuffer

object SummaryTable {
  private val timeCols = new ArrayBuffer[Col]()
  private val otherCols = new ArrayBuffer[Col]()

  private def statCols = timeCols ++ otherCols

  private lazy val statColsWithIndex = statCols.map(_.name.toUpperCase).zipWithIndex

  def statIndex(s: String) = {
    statColsWithIndex.find(_._1 == s.toUpperCase) match {
      case Some((_, i)) => i
      case None         => throw new IndexOutOfBoundsException(s"No such column $s")
    }
  }

  private def registerTime(col: Col): Col = {
    timeCols += col; col
  }
  private def register(col: Col): Col = {
    otherCols += col; col
  }

  def allGroups(s: SummaryTable): collection.Seq[Group] = collection.Seq[Group](
    SummaryTable.fullTableTimeCols,
    SummaryTable.otherColsGroup,
    SummaryTable.nestedWallTimes(s),
    SummaryTable.distTimes,
    SummaryTable.nestedAggregateThreadTimes,
    SummaryTable.threadStateSummary(s)
  )

  implicit def toGroup(col: Col): ColGroup = new ColGroup(col, Nil)

  def fullTableTimeCols: Group = Group("times", "Times (s)", timeCols.map(toGroup))
  def fullTableTimeColsJson(s: SummaryTable): String = fullTableTimeCols.toJson(s)
  def otherColsGroup: Group = Group("stats", "Other Metrics", otherCols.map(toGroup))
  def otherColsJson(s: SummaryTable): String = otherColsGroup.toJson(s)

  def nestedWallTimes(s: SummaryTable): Group =
    Group(
      "wall",
      "Wall Times (s)",
      totalProcTime(
        preGraphTime(
          preOptimusAppStartupTime,
          offGraphOptimusStartupTime
        ),
        clientWallTime(
          onGraphOptimusStartupTime,
          postOptimusOnGraphAppStartupTime,
          structuredStalls(s)
        ),
        graphHolesTime(
          postOptimusBetweenGraphAppStartupTime
        ),
        postGraphTime
      ) :: Nil
    )

  def distTimes: Group =
    Group(
      "dist",
      "Dist Times (s)",
      distTotal(
        remoteWallTime,
        distOverhead(
          engineStartup
        )
      ) :: Nil
    )

  // don't nest this one because we don't know how (columns are dynamic)
  def threadStateSummary(s: SummaryTable): Group =
    Group("threadsummary", "Thread CPU Time & Allocation", perThreadCol(s.threadStatesSummary, mb = true).map(toGroup))

  def nestedAggregateThreadTimes: Group =
    Group(
      "threads",
      "Aggregated Thread Times (s)",
      jvmCPUTime(
        jitTime,
        graphTime(
          internalTime,
          selfTime(
            cacheTime
          ),
          underUtilizedTime
        )
      ) :: Nil)

  def perThreadCol(threadStatesSummary: ThreadStateSummary, mb: Boolean): Iterable[Col] =
    threadStatesSummary.perThreadGroup.flatMap { case (threadSet, deltas) =>
      Col.time(s"$threadSet CPU Time", s"$threadSet CPU Time", "%12.3f", _ => deltas.totalCpuTimeNs) ::
        (if (mb && (threadSet != "jit" && threadSet != "gc"))
           Col(
             s"$threadSet Allocation (MB)",
             s"$threadSet Allocation (MB)",
             "%12d",
             _ => deltas.allocatedBytes / 1024 / 1024) :: Nil
         else Nil)
    }

  private def structuredStalls(table: SummaryTable): ColGroup = {
    val nested = perStallTypeCol(table)
    if (nested.length == 1) nested.head
    stallTime(
      nested.map(toGroup).toSeq: _*
    )
  }

  def perStallTypeCol(s: SummaryTable): collection.Seq[Col] =
    if (s.stallDetails ne null) {
      s.stallDetails.sortBy(_.pluginType.name).map { details =>
        val stall = details.pluginType.name
        val syncStackPresent = if (details.syncStackPresent) " with sync stack" else ""
        val title = s"$stall Stall"
        Col.time(s"$title$syncStackPresent", s"Stall time due to $stall$syncStackPresent", "%12.3f", _ => details.time)
      }
    } else Nil

  /** Times reported in ms need converting first because Col.time expects ns before displaying output in s */
  private val ns = 1000000
  val totalProcTime = registerTime(Col.time("Process", "Total process time", "%8.3f", _.totalProcTime * ns))
  private val preGraphTime = registerTime(
    Col.time(
      "Pre Graph",
      "Time to first graph entry, or an estimate of startup time, but will return 0 on grid engines that were already up and waiting for tasks (ie, not fresh)",
      "%8.3f",
      _.preGraphTime * ns
    ))

  /** All other times */
  private val graphHolesTime = registerTime(
    Col.time(
      "Between Graph",
      "Time between graph exits and enters (not including time to first graph enter or time after last exit)",
      "%8.3f",
      _.graphHolesTime))
  private val postGraphTime = registerTime(
    Col.time("Post Graph", "Time between last graph exit and shutdown", "%8.3f", _.postGraphTime))
  val wallTime = registerTime(
    Col.time(
      "Graph Wall",
      "Time from graph entry to graph exit on the user thread (typically equal to main thread&apos;s graph time)",
      "%8.3f",
      _.wallTime))
  private val clientWallTime = registerTime(
    Col.time(
      "Client Graph Wall",
      "Time from graph entry to graph exit on the user thread (typically equal to main thread graph time), excluding distribution",
      "%8.3f",
      _.clientWallTime
    ))
  val graphTime = registerTime(
    Col.time(
      "Graph Thread",
      "Sum of graph times of all optimus threads (that is, time they were not parked or spinning)",
      "%9.3f",
      _.graphTime))
  private val waitTime = registerTime(
    Col.time(
      "Wait",
      "Sum of the wait times of all optimus threads (wait is included in graph time)",
      " %8.3f",
      _.waitTime))
  private val stallTime = registerTime(
    Col.time("Stall", "Time the entire scheduler made no progress", "%8.3f", _.stallTime))
  val selfTime = registerTime(
    Col.time("Self", "Sum of the self times; that is times spent executing nodes", "%8.3f", _.selfTime))
  private val internalTime = registerTime(
    Col.time(
      "Internal",
      "Internal graph time (if this is negative, contact Optimus Graph Team)",
      "%8.3f",
      s => s.graphTime - s.selfTime))
  val cacheTime = registerTime(
    Col.time(
      "Cache",
      "Sum of cache times; that is times spent in cacheLookup methods (part of self time)",
      "%9.3f",
      _.cacheTime))
  private val underUtilizedTime = registerTime(
    Col.time(
      "Under-utilized",
      "Difference between theoretical maximum concurrency and actual achieved total graph self time",
      "%13.3f",
      _.underUtilizedTime,
      allowNegative = true
    ))
  val distOverhead = registerTime(
    Col.time(
      "Dist Overhead",
      "Difference in client-side and engine-side wall time of a dist task (includes serialization, waiting for engine, transmission)",
      "%12.3f",
      _.distOverhead
    ))
  val remoteWallTime = registerTime(
    Col.time("Remote Wall", "Wall time of all (successfully) disted nodes", "%8.3f", _.remoteWallTime * 1000 * 1000))
  val distTotal = registerTime(
    Col.time(
      "Dist Total",
      "Dist Overhead and Remote Wall Time",
      "%12.3f",
      s => s.distOverhead + s.remoteWallTime * 1000 * 1000))
  val engineStartup = registerTime(
    Col.time(
      "Engine startup",
      "Total engine startup (JVM process uptime) from those dist tasks that did not reuse engines",
      "%8.3f",
      s => s.engineStartupMs * 1000 * 1000))
  val optimusCPUTime = registerTime(
    Col.time("Graph CPU", "CPU time accrued by Optimus threads while they were on-graph", "%12.3f", _.optimusCPUTime))
  val jvmCPUTime = registerTime(
    Col.time(
      "JVM CPU",
      "CPU time accrued by the JVM processes executing this job and all the disted tasks)",
      "%12.3f",
      _.jvmCPUTime))

  private val preOptimusAppStartupTime = registerTime(
    Col.time("Pre-Optimus App Startup", "time until Optimus Environment init", "%8.3f", _.preOptimusStartupTime))
  private val offGraphOptimusStartupTime = registerTime(
    Col.time(
      "Off-Graph Optimus Startup",
      "time from Optimus Environment init to first graph entry",
      "%8.3f",
      _.offGraphOptimusStartupTime))
  private val onGraphOptimusStartupTime = registerTime(
    Col.time(
      "On-Graph Optimus Startup",
      "time from first graph entry to the end of optimus environment init",
      "%8.3f",
      _.onGraphOptimusStartupTime))
  private val postOptimusOnGraphAppStartupTime = registerTime(
    Col.time(
      "On-Graph App Startup",
      "time from creating Optimus Environment to launching def run/mainGui while on-graph",
      "%8.3f",
      _.postOptimusOnGraphAppStartupTime
    ))
  private val postOptimusBetweenGraphAppStartupTime = registerTime(
    Col.time(
      "Between-Graph App Startup",
      "time from creating Optimus Environment to launching def run/mainGui, while between-graph",
      "%8.3f",
      _.postOptimusBetweenGraphAppStartupTime
    ))

  /** All reported in ms so convert to ns first */
  val gcTimeStopTheWorld = registerTime(
    Col.time("Full GC", "wall time spent in stop-the-world GC", "%11.3f", _.gcTimeStopTheWorld * ns))
  val gcTimeAll = registerTime(
    Col.time("All GC", "CPU time spent by all GC (including parallel)", "%10.3f", _.gcTimeAll * ns))
  val jitTime = registerTime(
    Col.time("JIT", "time spent by JIT compiler (running in parallel, but eating CPU time)", "%8.3f", _.jitTime * ns))
  val classLoadTime = registerTime(
    Col.time("Class Load", "time spent loading classes", "%8.3f", _.clTime)
  ) // but this is nanos to start with

  /** Non-time metrics */
  private val writes = register(Col("DAL Writes", "Number of DAL write commands", "%10d", _.writes))
  private val requests = register(Col("DAL Reads", "Number of DAL read commands", "%10d", _.requests))
  private val results = register(
    Col("DAL Entities", "Number of DAL entities returned from the read commands", "%12d", _.results))
  val distTasks = register(Col("Dist Tasks", "Number of dist tasks that executed on the grid", "%10d", _.distTasks))
  private val syncStacks = register(
    Col("Sync Stacks", "Number of sync stacks that were reported", "%11d", _.syncStacks))
  val maxHeap = register(
    Col(
      "Max Heap (MB)",
      "Largest JVM heap use seen (measured in a post-GC callback)",
      "%13.3f",
      _.maxHeap / (1024.0 * 1024.0)))
  val loadAvg = register(
    Col("LoadAvg", "Average OS loadavg of all engines", "%8.3f", s => if (s.loadAvg < 0) 0.0 else s.loadAvg))
  val nodeStarts = register(Col("Node Starts", "Number of nodes started", "%10d", _.nodeStarts))
  val cacheHits = register(Col("Cache Hits", "Number of node cache hits", "%10d", _.cacheHits))
  val cacheMisses = register(Col("Cache Misses", "Number of node cache misses", "%10d", _.cacheMisses))
  val cacheEvictions = register(Col("Cache evictions", "Number of nodes evicted from cache", "%10d", _.cacheEvictions))

  private def highlightCpuLoad(load: Double): Boolean = {
    val cores = Runtime.getRuntime.availableProcessors
    val ideal = Settings.idealThreadCountToCount
    val actualCap = Math.min(ideal.toDouble / cores, 1) // ie, treat this as 100% and compare to load
    actualCap - load > 0.2 // if difference between actualCap and load you achieved is more than 20%, highlight
  }

  val jvmCPULoad = register(
    Col.percentage(
      "JVM CPU",
      "Percentage of system CPU used by the JVMs (if in red, CPU load achieved was more than 20% below maximum allowed by ideal thread count)",
      "%8.2f",
      _.jvmCPULoad,
      s => highlightCpuLoad(s.jvmCPULoad)
    ))
  val systemCPULoad = register(Col.percentage("System CPU", "How busy were the grid hosts", "%12.3f", _.systemCPULoad))

  def statTableCols(s: SummaryTable): collection.Seq[StatTableCol] = (statCols).map(_.statCol(s))

  def threadStatTableCols(s: SummaryTable): collection.Seq[StatTableCol] =
    if (s.threadStatesSummary ne null) perThreadCol(s.threadStatesSummary, mb = true).map(_.statCol(s)).toSeq else Nil
}

final case class SummaryTable(
    totalProcTime: Long = Long.MinValue,
    preGraphTime: Long = Long.MinValue,
    preOptimusStartupTime: Long = Long.MinValue,
    offGraphOptimusStartupTime: Long = Long.MinValue,
    onGraphOptimusStartupTime: Long = Long.MinValue,
    postOptimusOnGraphAppStartupTime: Long = Long.MinValue,
    postOptimusBetweenGraphAppStartupTime: Long = Long.MinValue,
    graphHolesTime: Long = Long.MinValue,
    postGraphTime: Long = Long.MinValue,
    wallTime: Long = Long.MinValue,
    clientWallTime: Long = Long.MinValue,
    graphTime: Long = Long.MinValue,
    waitTime: Long = Long.MinValue,
    stallTime: Long = Long.MinValue,
    selfTime: Long = Long.MinValue,
    cacheTime: Long = Long.MinValue,
    underUtilizedTime: Long = Long.MinValue,
    writes: Int = Int.MinValue,
    requests: Int = Int.MinValue,
    results: Int = Int.MinValue,
    distOverhead: Long = Long.MinValue,
    engineStartupMs: Long = Long.MinValue,
    remoteWallTime: Long = Long.MinValue,
    distTasks: Int = Int.MinValue,
    syncStacks: Int = Int.MinValue,
    maxHeap: Long = Long.MinValue,
    loadAvg: Double = Double.NaN,
    optimusCPUTime: Long = Long.MinValue,
    jvmCPUTime: Long = Long.MinValue,
    jvmCPULoad: Double = Double.NaN,
    systemCPULoad: Double = Double.NaN,
    gcTimeStopTheWorld: Long = Long.MinValue,
    gcTimeAll: Long = Long.MinValue,
    jitTime: Long = Long.MinValue,
    clTime: Long = Long.MinValue,
    timeBasedTriggerRatioHit: Long = Long.MinValue,
    timeBasedKillTriggerHit: Long = Long.MinValue,
    heapBasedTriggerHit: Long = Long.MinValue,
    heapBasedIncludeSITriggerHit: Long = Long.MinValue,
    heapBasedBackoffTriggerHit: Long = Long.MinValue,
    totalNumberNodesCleared: Long = Long.MinValue,
    threadStatesSummary: ThreadStateSummary = null,
    stallDetails: collection.Seq[StallDetailedTime] = null,
    nodeStarts: Long = Long.MinValue,
    cacheHits: Long = Long.MinValue,
    cacheMisses: Long = Long.MinValue,
    cacheEvictions: Long = Long.MinValue
) {
  import SummaryTable._
  override def toString: String =
    if (!DiagnosticSettings.profileSummaryJson) new StatTable(statTableCols(this) ++ threadStatTableCols(this)).toString
    else s"Times\n${fullTableTimeColsJson(this)}Other Metrics\n${otherColsJson(this)}"
}
