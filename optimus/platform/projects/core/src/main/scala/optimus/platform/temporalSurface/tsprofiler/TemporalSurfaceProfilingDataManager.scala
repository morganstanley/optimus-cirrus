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
package optimus.platform.temporalSurface.tsprofiler

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import optimus.graph.NodeTask
import optimus.graph.diagnostics.tsprofiler.TemporalSurfProfilingLevel
import optimus.platform.TemporalContext
import optimus.platform.inputs.registry.ProcessGraphInputs
import optimus.platform.temporalSurface.BranchTemporalSurface
import optimus.platform.temporalSurface.LeafTemporalSurface
import optimus.platform.temporalSurface.TemporalSurface
import optimus.platform.util.Log

import scala.jdk.CollectionConverters._

/*
  top level is a synthetic entry with no profiling data associated but it can point to multiple children
  (where each child is the root of a tweaked load context)

  TemporalSurfaceProfilingData(
  null,0,0, ... ,
  children = [
    {rootCtx1 -> TemporalSurfaceProfilingData},
    {rootCtx2 -> TemporalSurfaceProfilingData} ]
 */
object TemporalSurfaceProfilingDataManager extends Log {
  val tsProfilingFilename = "profiler_temporal_surface"

  @volatile var tsProfiling: TemporalSurfaceProfilingData = new TemporalSurfaceProfilingData()

  // used in tests and dist
  def resetTSProfiling(): Unit = tsProfiling = new TemporalSurfaceProfilingData()

  final def profilingOn(n: NodeTask): Boolean =
    ProcessGraphInputs.ProfileTSLevel.currentValueOrThrow() != TemporalSurfProfilingLevel.NONE

  private[optimus] def tsProfDataString(rootTsProf: TemporalSurfaceProfilingData): String = {
    val sb = new StringBuilder
    if (ProcessGraphInputs.ProfileTSLevel.currentValueOrThrow() != TemporalSurfProfilingLevel.NONE) {
      // print synthetic root node (root of all tweaked load contexts)
      sb.append("********** TS Profiling Data Tree ********** \n")
      sb.append(rootTsProf.prettyPrintTSProf()).append("\n")
      prettyPrintTSProfPerLevel(rootTsProf.children, 1, rootTsProf.ts, sb)
      sb.append("********** TS Profiling Data Tree END ********** \n")
    }
    sb.toString
  }

  private[temporalSurface] def shouldAggregate(ts: TemporalSurfaceProfilingData): Boolean =
    ts != null && ts.visited != initStartingValue

  private[optimus] def aggregateTSProfiling(other: TemporalSurfaceProfilingData): Unit =
    ProcessGraphInputs.ProfileTSLevel.currentValueOrThrow() match {
      case TemporalSurfProfilingLevel.BASIC =>
        if (shouldAggregate(other)) {
          if (shouldAggregate(tsProfiling))
            tsProfiling = tsProfiling.aggregate(other)
          else
            tsProfiling = other
        }
      case TemporalSurfProfilingLevel.AGGREGATE =>
        if (shouldAggregate(other)) {
          if (shouldAggregate(tsProfiling))
            tsProfiling = tsProfiling.aggregate(other, topLevelStatsOnly = true)
          else
            tsProfiling = other
        }
        // should not try to merge with null but good to have that case working too
        else tsProfiling = tsProfiling.aggregate(new TemporalSurfaceProfilingData(), topLevelStatsOnly = true)
      case _ =>
    }

  private def branchVsLeafInfo(ts: TemporalSurface): String =
    if (ts.isInstanceOf[LeafTemporalSurface]) " (LEAF) "
    else if (ts.isInstanceOf[BranchTemporalSurface]) " (BRANCH) "
    else "" // should not hit this case

  private[tsprofiler] def childrenProfOrder(
      childrenProf: ConcurrentHashMap[IdentityWrapper[TemporalSurface], TemporalSurfaceProfilingData],
      parentTS: TemporalSurface): Seq[TemporalSurface] =
    if (parentTS != null) parentTS.children
    else
      childrenProf.entrySet().asScala.toSeq.sortBy(-_.getValue.visited.get()).map(_.getKey.a)

  private def prettyPrintTSProfPerLevel(
      tsProf: ConcurrentHashMap[IdentityWrapper[TemporalSurface], TemporalSurfaceProfilingData],
      level: Int,
      parentTS: TemporalSurface,
      sb: StringBuilder): Unit = {
    // for top level synthetic node, default to what we put in the map (top level tweaked load contexts)
    // otherwise print in order of children as that is the order TS entries have been visited
    val printOrder = childrenProfOrder(tsProf, parentTS)

    printOrder.foreach(ts => {
      val correspondingTS = tsProf.getOrDefault(new IdentityWrapper(ts), new TemporalSurfaceProfilingData(ts = ts))
      sb.append(s"${"-" * level}${branchVsLeafInfo(ts)}: ${if (ts != null) ts.minimalDescription}\n")
      sb.append(correspondingTS.prettyPrintTSProf(level)).append("\n")
      if (correspondingTS.children.size == 0) sb.append("\n")
      prettyPrintTSProfPerLevel(correspondingTS.children, level + 1, ts, sb)
    })
  }

  private[optimus] def maybeUpdateProfilingTSData(
      loadContext: TemporalContext): Option[TemporalSurfaceProfilingData] = {
    // if profile temporal surface enabled (eg --profile-temporal-surface basic)
    if (ProcessGraphInputs.ProfileTSLevel.currentValueOrThrow() != TemporalSurfProfilingLevel.NONE) {
      Some(
        TemporalSurfaceProfilingDataManager.tsProfiling.children
          .computeIfAbsent(
            new IdentityWrapper[TemporalSurface](loadContext),
            _ => new TemporalSurfaceProfilingData(loadContext)))
    } else None
  }

  private[optimus] def maybeCreateChildProfilingEntry(
      childTS: TemporalSurface,
      currentTS: TemporalSurfaceProfilingData /* parent TS */ ): Option[TemporalSurfaceProfilingData] =
    if (ProcessGraphInputs.ProfileTSLevel.currentValueOrThrow() != TemporalSurfProfilingLevel.NONE)
      Some(
        currentTS.children.computeIfAbsent(
          new IdentityWrapper[TemporalSurface](childTS),
          _ => new TemporalSurfaceProfilingData(childTS)))
    else None

  def initStartingValue = new AtomicLong()
  def asAtomicL(v: Long) = new AtomicLong(v)
}
