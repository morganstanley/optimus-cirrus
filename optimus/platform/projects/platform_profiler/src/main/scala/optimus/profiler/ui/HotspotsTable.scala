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
package optimus.profiler.ui

import java.awt.Color
import java.util
import javax.swing.JMenuItem
import javax.swing.JPopupMenu
import javax.swing.JTabbedPane
import optimus.config.NodeCacheConfigs
import optimus.debugger.browser.ui.GraphBrowserAPI
import optimus.graph.NodeTaskInfo
import optimus.graph.NodeTrace
import optimus.graph.OGTraceReader
import optimus.graph.PluginType
import optimus.graph.PropertyNode
import optimus.graph.Settings
import optimus.graph.cache.Caches
import optimus.graph.cache.CauseProfiler
import optimus.graph.cache.NCPolicy
import optimus.graph.cache.NodeCache
import optimus.graph.diagnostics.DbgPreference
import optimus.graph.diagnostics.MarkTweakDependency
import optimus.graph.diagnostics.NodeName
import optimus.graph.diagnostics.PNodeTask
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.diagnostics.SelectionFlags
import optimus.profiler.DebuggerUI
import optimus.profiler.ProfilerUI
import optimus.profiler.recipes.GraphAlgos
import optimus.profiler.recipes.NodeStacks
import optimus.profiler.ui.NPTableRenderer.ValueWithTooltip
import optimus.profiler.ui.browser.GraphBrowser
import optimus.profiler.ui.common.JMenu2
import optimus.profiler.ui.common.JMenuOps
import optimus.profiler.ui.common.JPopupMenu2

import java.awt.event.MouseAdapter
import java.awt.event.MouseEvent
import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer
import javax.swing.SwingUtilities
import scala.collection.mutable

object HotspotsTable {

  val nodeCacheSectionColor: Color = Color.decode("#F7A79D")
  val distinctSectionColor: Color = Color.decode("#D0B3C9")
  val cacheTimingSectionColor: Color = Color.decode("#96C4C1")
  val nodeTimingSectionColor: Color = Color.decode("#A2C690")
  val nodeInfoSectionColor: Color = Color.decode("#D8B877")

  // Cache Hit / Miss related

  private[ui] val c_started = new TableColumnLong[PNodeTaskInfo]("Started", 60) {
    override def valueOf(row: PNodeTaskInfo): Long = row.start
    override def toolTip = "Nodes started"
    override def getHeaderColor: Color = nodeCacheSectionColor
  }
  private val c_evicted = new TableColumnLong[PNodeTaskInfo]("Evicted", 60) {
    override def valueOf(row: PNodeTaskInfo): Long = row.evicted
    override def toolTip = "Nodes evicted from cache"
    override def getHeaderColor: Color = nodeCacheSectionColor
  }
  private val c_retained = new TableColumnLong[PNodeTaskInfo]("Retained", 60) {
    override def valueOf(row: PNodeTaskInfo): Long =
      if (row.cacheHit + row.cacheMiss == 0) 0 else Math.max(row.start - row.evicted, 0)
    override def toolTip: String =
      "Derived Definition: Started - Evicted" +
        "<br>Nodes currently in cache"
    override def getHeaderColor: Color = nodeCacheSectionColor
  }
  private val c_invalidated = new TableColumnCount[PNodeTaskInfo]("Invalidated", 70) {
    override def valueOf(row: PNodeTaskInfo): Int = row.invalidated
    override def getHeaderColor: Color = nodeCacheSectionColor
  }
  private val c_overInvalidated = new TableColumnCount[PNodeTaskInfo]("Over Invalidated", 70) {
    override def valueOf(row: PNodeTaskInfo): Int = row.overInvalidated
    override def getHeaderColor: Color = nodeCacheSectionColor
  }
  private[ui] val c_cacheHit = new TableColumnLong[PNodeTaskInfo]("Cache Hit", 60) {
    override def valueOf(row: PNodeTaskInfo): Long = row.cacheHit
    override def getHeaderColor: Color = nodeCacheSectionColor
  }
  private[ui] val c_cacheMiss = new TableColumnLong[PNodeTaskInfo]("Cache Miss", 60) {
    override def valueOf(row: PNodeTaskInfo): Long = row.cacheMiss
    override def getHeaderColor: Color = nodeCacheSectionColor
  }
  private val c_cacheHitRatio = new TableColumnTime[PNodeTaskInfo]("Cache Hit Ratio", 80) {
    override def valueOf(row: PNodeTaskInfo): Double =
      if (row.cacheMiss <= 1) {
        if (row.cacheHit == 0) 0d else 1d
      } else row.cacheHit.asInstanceOf[Double] / (row.cacheHit + row.cacheMiss - 1) // There is always one miss

    override def toolTip = "Derived Definition: Cache Hit / (Cache Hit + Cache Miss)"
    override def getHeaderColor: Color = nodeCacheSectionColor
  }

  // internal xsft columns
  private val c_cacheHitTrivial = new TableColumnCount[PNodeTaskInfo]("Trivial Hit", 60) {
    override def valueOf(row: PNodeTaskInfo): Int = row.cacheHitTrivial
    override def getHeaderColor: Color = nodeCacheSectionColor
    override def toolTip: String = "(XSFT only) Count of trivial top-level hits on proxy"
  }
  private val c_cacheHitTrivialFailed = new TableColumnLong[PNodeTaskInfo]("Hit - Trivial", 60) {
    override def valueOf(row: PNodeTaskInfo): Long = row.cacheHit - row.cacheHitTrivial
    override def getHeaderColor: Color = nodeCacheSectionColor
    override def toolTip: String = "(XSFT only) Difference between cache hits and 'trivial' top-level hits on proxy"
  }

  // lookup (hash) collisions are disturbing and need to be in the graph profiler
  // hash collisions of themselves are not a big performance cost (usually)
  // but it can also mean there are problems with reuse - one NaN in the wrong place, and
  // you get no cache hits
  private val c_hashCollision = new TableColumnCount[PNodeTaskInfo]("Collisions", 60) {
    override def valueOf(row: PNodeTaskInfo): Int = row.collisionCount
    override def toolTip =
      "Hash collisions when looking up values in the node cache (when PropertyNode.equalsForCaching fails)"
    override def getHeaderColor: Color = nodeCacheSectionColor
  }

  private val c_tweakLookupTime = new TableColumnTime[PNodeTaskInfo]("Tweak Lookup Time (ms)", 60) {
    override def valueOf(row: PNodeTaskInfo): Double = row.tweakLookupTime * 1e-6
    override def toolTip = "Time spent looking up tweak of a given property"
    override def getHeaderColor: Color = nodeTimingSectionColor
  }

  private val c_tweakID = new TableColumnCount[PNodeTaskInfo]("Tweak ID", 60) {
    override def valueOf(row: PNodeTaskInfo): Int = if (row.nti == null) 0 else row.nti.tweakableID
    override def toolTip = "Internal Tweak ID"
    override def getHeaderColor: Color = nodeInfoSectionColor
    override def computeSummary(table: NPTable[PNodeTaskInfo], indexes: Seq[Int]): AnyRef = "" // Do not aggregate
  }

  private val c_tweakDependencies: TableColumn[PNodeTaskInfo] =
    new TableColumn[PNodeTaskInfo]("Tweak Dependencies", 60) {
      override def valueOf(row: PNodeTaskInfo): ValueWithTooltip[String] = {
        val flag =
          if (MarkTweakDependency.dependencyMapIsNull) SelectionFlags.NotFlagged
          else MarkTweakDependency.getTweakDependencyHighlight(row).getOrElse(SelectionFlags.NotFlagged)
        if (row.nti == null) ValueWithTooltip("", SelectionFlags.toToolTipText(flag))
        else if (row.nti.dependsOnTweakMask().allBitsAreSet())
          ValueWithTooltip("[poison]", SelectionFlags.toToolTipText(flag))
        else ValueWithTooltip(row.nti.dependsOn.asScala.mkString(", "), SelectionFlags.toToolTipText(flag))
      }
      override def toolTip = "Internal Tweak ID Dependencies"
      override def getHeaderColor: Color = nodeInfoSectionColor
    }
  private val c_process = new TableColumnString[PNodeTaskInfo]("Process", 60) {
    override def valueOf(row: PNodeTaskInfo): String = row.getProcess.displayName()
    override def toolTip = "Process name / pid"
    override def getHeaderColor: Color = nodeInfoSectionColor
  }

  // distinct counts section

  private val c_distinctScenarioStack = new TableColumnCount[PNodeTaskInfo]("Distinct SS", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.getDistinct.scenarioStacks.size
    override def toolTip: String =
      "Number of distinct scenario stacks (via hashCode) this node was evaluated under" +
        "<br>If this is high relative to Distinct Results, making the node @scenarioIndependent will have a significant benefit"
    override def getHeaderColor: Color = distinctSectionColor
  }
  private val c_distinctEntities = new TableColumnCount[PNodeTaskInfo]("Distinct Entities", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.getDistinct.entities.size
    override def toolTip: String =
      "Number of distinct entities (via hashCode) upon which this node was evaluated" +
        "<br>If this is high relative to Distinct Results, it suggests the node may not depend upon all (or most) entity constructor arguments" +
        "<br>Moving the node to an @entity object may have a significant benefit"
    override def getHeaderColor: Color = distinctSectionColor
  }
  private val c_distinctArgs = new TableColumnCount[PNodeTaskInfo]("Distinct Node Args", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.getDistinct.args.size
    override def toolTip: String =
      "Number of distinct arguments (via hashCode) this node was called with" +
        "<br>If this is high relative to Distinct Results, it suggests the node may not depend upon all the node arguments" +
        "<br>It may be beneficial to split the node into sub-nodes so that expensive parts are in @nodes which take the minimal" +
        "arguments they require, and hence will have more reuse potential"
    override def getHeaderColor: Color = distinctSectionColor
  }
  private val c_distinctResults = new TableColumnCount[PNodeTaskInfo]("Distinct Results", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.getDistinct.results.size
    override def toolTip: String =
      "Number of distinct results (via hashCode) from evaluating this node" +
        "<br>A low number of distinct results suggests this node could be optimized by:" +
        "<br>eg making it @scenarioIndependent, making it a @node on an @entity object, removing unused arguments" +
        "<br>If Started == Distinct Results, it suggests this node depends on things that vary in scenario stack, entity and arguments</br>"
    override def getHeaderColor: Color = distinctSectionColor
  }
  private val c_distinctCombined = new TableColumnCount[PNodeTaskInfo]("Distinct Combined", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.getDistinct.combined.size
    override def toolTip =
      "Number of distinct tuples of (scenario stack hash, entity hash, (node) args hash)"
    override def getHeaderColor: Color = distinctSectionColor
  }

  private val c_distinctIdScenarioStack = new TableColumnCount[PNodeTaskInfo]("Distinct Id SS", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.getDistinct.IdScenarioStacks.size
    override def toolTip: String =
      "Number of distinct identityHashCode for scenario stacks" +
        "<br>If this doesn't match Distinct SS, it suggests there are hash collisions"
    override def getHeaderColor: Color = distinctSectionColor
  }
  private val c_distinctIdEntities = new TableColumnCount[PNodeTaskInfo]("Distinct Id Entities", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.getDistinct.IdEntities.size
    override def toolTip: String =
      "Number of distinct identityHashCode for entities" +
        "<br>If this doesn't match Distinct Entities, it suggests there are hash collisions"
    override def getHeaderColor: Color = distinctSectionColor
  }
  private val c_distinctIdArgs = new TableColumnCount[PNodeTaskInfo]("Distinct Id Node Args", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.getDistinct.IdArgs.size
    override def toolTip: String =
      "Number of distinct identityHashCode for node arguments" +
        "<br>If this doesn't match Distinct Node Args, it suggests there are hash collisions"
    override def getHeaderColor: Color = distinctSectionColor
  }
  private val c_distinctIdResults = new TableColumnCount[PNodeTaskInfo]("Distinct Id Results", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.getDistinct.IdResults.size
    override def toolTip: String =
      "Number of distinct identityHashCode for results" +
        "<br>If this doesn't match Distinct Results, it shows that duplicate results are being generated" +
        "<br>This is not ideal, especially for large object structures, as it means evaluating the equals method" +
        "<br>becomes expensive as there is no 'eq' (reference) equality short-circuit check"
    override def getHeaderColor: Color = distinctSectionColor
  }
  private val c_distinctIdCombined = new TableColumnCount[PNodeTaskInfo]("Distinct Id Combined", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.getDistinct.IdCombined.size
    override def toolTip: String =
      "Number of distinct identityHashCode tuples for (scenario stack hash, entity hash, (node) args hash)" +
        "<br>This should match Started, if it doesn't it strong implies there are hash collisions"
    override def getHeaderColor: Color = distinctSectionColor
  }

  private val c_distinctScenarioStackDiff = new TableColumnCount[PNodeTaskInfo]("Distinct SS Diff", 80) {
    override def valueOf(row: PNodeTaskInfo): Int =
      row.getDistinct.IdScenarioStacks.size - row.getDistinct.scenarioStacks.size
    override def getHeaderColor: Color = distinctSectionColor
  }
  private val c_distinctEntitiesDiff = new TableColumnCount[PNodeTaskInfo]("Distinct Entities Diff", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.getDistinct.IdEntities.size - row.getDistinct.entities.size
    override def getHeaderColor: Color = distinctSectionColor
  }
  private val c_distinctArgsDiff = new TableColumnCount[PNodeTaskInfo]("Distinct Node Args Diff", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.getDistinct.IdArgs.size - row.getDistinct.args.size
    override def getHeaderColor: Color = distinctSectionColor
  }
  private val c_distinctResultsDiff = new TableColumnCount[PNodeTaskInfo]("Distinct Results Diff", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.getDistinct.IdResults.size - row.getDistinct.results.size
    override def getHeaderColor: Color = distinctSectionColor
  }
  private val c_distinctCombinedDiff = new TableColumnCount[PNodeTaskInfo]("Distinct Combined Diff", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.getDistinct.IdCombined.size - row.getDistinct.IdCombined.size
    override def getHeaderColor: Color = distinctSectionColor
  }

  private val c_debugCountScenarioStack = new TableColumnCount[PNodeTaskInfo]("Debug Count Scenario Stacks", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.getDistinct.scenarioStacks.insertCount
    override def getHeaderColor: Color = distinctSectionColor
  }
  private val c_debugCountEntities = new TableColumnCount[PNodeTaskInfo]("Debug Count Entities", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.getDistinct.entities.insertCount
    override def getHeaderColor: Color = distinctSectionColor
  }
  private val c_debugCountArgs = new TableColumnCount[PNodeTaskInfo]("Debug Count Args", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.getDistinct.args.insertCount
    override def getHeaderColor: Color = distinctSectionColor
  }
  private val c_debugCountResults = new TableColumnCount[PNodeTaskInfo]("Debug Count Results", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.getDistinct.results.insertCount
    override def getHeaderColor: Color = distinctSectionColor
  }
  private val c_debugCountCombined = new TableColumnCount[PNodeTaskInfo]("Debug Count Combined", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.getDistinct.combined.insertCount
    override def getHeaderColor: Color = distinctSectionColor
  }
  // cache timing section

  private[ui] val c_cacheTimeToolTip: String = "Total time spent looking up this node in the cache" +
    "<br>If this number is large, it is worth analysing code to determine if there is an expensive hashCode or equals function" +
    "<br>Many design decisions within the Optimus platform are predicated upon cheap/free HashMap lookups" +
    "<br>If this is not the case, there can be serious performance issues spread over many nodes" +
    "<br>The issue can be subtle, as equals methods are instant if object references match" +
    "<br>But if you end up in a situation where there are two objects instances which are equal, the cost of the equality check can be much higher" +
    "<br>@embeddable do not currently (27/11/2019) cache their hashCodes" +
    "<br>Immutable Scala collections do not cache hashCodes" +
    "<br>case classes do not cache hashCodes" +
    "<br>DAL entity equality is based on reference" +
    "<br>Heap entity equality is based on value"
  private[ui] val c_cacheTime = new TableColumnTime[PNodeTaskInfo]("Cache Time (ms)", 80) {
    override def valueOf(row: PNodeTaskInfo): Double = row.cacheTime * 1e-6
    override def toolTip: String = c_cacheTimeToolTip
    override def getHeaderColor: Color = cacheTimingSectionColor
  }
  private val c_cacheTimeAvg = new TableColumnTime[PNodeTaskInfo]("Cache Time per Node (us)", 80) {
    override def valueOf(row: PNodeTaskInfo): Double = row.avgCacheTime * 1e-3
    override def toolTip: String =
      "Derived Definition: Cache Time / (Cache Hit + Cache Miss)" +
        "<br>Average time spent looking up this node in the cache"
    override def getHeaderColor: Color = cacheTimingSectionColor
  }
  private val c_cacheReuseTime = new TableColumnTime[PNodeTaskInfo]("Node Reused Time (ms)", 80) {
    override def valueOf(row: PNodeTaskInfo): Double = row.nodeReusedTime * 1e-6
    override def toolTip: String =
      "Total time saved by using result from cache, rather than recalculating" +
        "<br>This can be inaccurate because first evaluation of a @node can trigger classloading, (lazy) val evaluation (such as one-time hashCode calculation), JIT compilation etc" +
        "<br>So benefit can be over-inflated, and vary from run to run, as node evaluation order is non-deterministic"
    override def getHeaderColor: Color = cacheTimingSectionColor
  }
  private val c_nodeUseTime = new TableColumnTime[PNodeTaskInfo]("Node Used Time (ms)", 80) {
    override def valueOf(row: PNodeTaskInfo): Double = row.nodeUsedTime * 1e-6
    override def toolTip: String =
      "Total time this node would run without any caching, ie, number of times called * (self + anc time)"
    override def getHeaderColor: Color = cacheTimingSectionColor
  }
  private[ui] val c_cacheBenefitToolTip: String = "Derived Definition: Reused Node Time - Cache Time" +
    "<br>Calculated cache benefit of this node" +
    "<br>Note warnings on Reused Node Time tooltip"
  private val c_cacheBenefit = new TableColumnTime[PNodeTaskInfo]("Cache Benefit (ms)", 80) {
    override def valueOf(row: PNodeTaskInfo): Double = row.cacheBenefit * 1e-6
    override def toolTip: String = c_cacheBenefitToolTip
    override def getHeaderColor: Color = cacheTimingSectionColor
  }
  private val c_cacheReuseTimeAmortized = new TableColumnTime[PNodeTaskInfo]("Reused Node Time (Amortized) (ms)", 80) {
    override def valueOf(row: PNodeTaskInfo): Double =
      if (row.start == 0) 0
      else
        (row.ancAndSelfTime / row.start) * row.cacheHit * 1e-6
    override def toolTip: String =
      "Derived Definition : (Self Time + ANC Time) / Started * Cache Hit" +
        "<br>Estimated time saved by using result from cache, rather than recalculating," +
        "<br>assuming all nodes of this name have equal lookup times" +
        "<br>This reduces impact of one-time costs on certain nodes" +
        "<br>If there is a big discrepancy between the Reused Node Time and the Amortized version," +
        "<br>it implies node lookup is non-uniform" +
        "<br>Some nodes may include one-off costs that inflate their self time / anc time," +
        "<br>and hence their apparent cache benefit"
    override def getHeaderColor: Color = cacheTimingSectionColor
  }
  private val c_cacheBenefitAmortized = new TableColumnTime[PNodeTaskInfo]("Cache Benefit (Amortized) (ms)", 80) {
    override def valueOf(row: PNodeTaskInfo): Double =
      if (row.start == 0) 0 else ((row.ancAndSelfTime / row.start * row.cacheHit) - row.cacheTime) * 1e-6
    override def toolTip: String =
      "Derived Definition: Reused Node Time (Amortized) - Cache Time" +
        "<br>Estimate cache benefit of this node, assuming uniform lookup times" +
        "<br>If there is a significant discrepancy between Cache Benefit and Amortized versions, it suggest node lookup is non-uniform"
    override def getHeaderColor: Color = cacheTimingSectionColor
  }

  private val c_children = new TableColumnCount[PNodeTaskInfo]("Children Lookup", 60) {
    override def valueOf(row: PNodeTaskInfo): Int = row.childNodeLookupCount
    override def toolTip = "Total number of child nodes looked up"
    override def getHeaderColor: Color = cacheTimingSectionColor
  }
  private val c_childCacheTime = new TableColumnTime[PNodeTaskInfo]("Child Cache Time (ms)", 80) {
    override def valueOf(row: PNodeTaskInfo): Double = row.childNodeLookupTime * 1e-6
    override def toolTip: String =
      "Time spent looking up child nodes in cache" +
        "<br>This is included in the Self Time of node" +
        "<br>If Self Time and Child Node Lookup Time are close, it implies the node does little work" +
        "<br>Child Node Lookup Time is the Cache Time"
    override def getHeaderColor: Color = cacheTimingSectionColor
  }

  private val c_childrenPerNode = new TableColumnTime[PNodeTaskInfo]("Children per Node", 60) {
    override def valueOf(row: PNodeTaskInfo): Double =
      if (row.start == 0) 0 else row.childNodeLookupCount.asInstanceOf[Double] / row.start
    override def toolTip = "Derived Definition: Children / Started"
    override def getHeaderColor: Color = cacheTimingSectionColor
  }
  private val c_childCacheTimePerNode = new TableColumnTime[PNodeTaskInfo]("Child Cache Time Per Node (us)", 80) {
    override def valueOf(row: PNodeTaskInfo): Double =
      if (row.start == 0) 0d else (row.childNodeLookupTime / row.start) * 1e-3
    override def toolTip: String =
      "Derived Definition: Child Cache Time / Started" +
        "<br>Average time per node spent looking up child nodes in cache"
    override def getHeaderColor: Color = cacheTimingSectionColor
  }
  // node calculation timing section

  private val c_wallTime = new TableColumnTime[PNodeTaskInfo]("Wall Time (ms)", 80) {
    override def valueOf(row: PNodeTaskInfo): Double = row.wallTime * 1e-6
    override def toolTip: String =
      "Sum of wall times of individual nodes per type" +
        "<br>Defined as the time between node started and completed." +
        "<br><br><i>Note this value might be misleading for two reasons</i><br>" +
        "<div>\u25e6 For nested nodes calls of the same type loose meaning as the time might be double counted</div>" +
        "<div>\u25e6 For waits in the middle of the node invocations this sum looses any usable meaning</div>"
    override def getHeaderColor: Color = nodeTimingSectionColor
    override def computeSummary(table: NPTable[PNodeTaskInfo], indexes: Seq[Int]): AnyRef = "" // Do not aggregate
  }
  private[ui] val c_selfTime = new TableColumnTime[PNodeTaskInfo]("Self Time (ms)", 80) {
    override def valueOf(row: PNodeTaskInfo): Double = row.selfTime * 1e-6
    override def toolTip: String =
      "Total CPU time, across multiple invocations<br><br>" +
        "<i>Note - includes: <br>" +
        "Cache lookup time of its children<br>" +
        "Tweak lookup time<br>" +
        "Some args hash time for tweakable nodes in XS scope</i><br>" +
        "See docs for more detail"
    override def getHeaderColor: Color = nodeTimingSectionColor
  }
  private val c_ancTime = new TableColumnTime[PNodeTaskInfo]("ANC Time (ms)", 80) {
    override def valueOf(row: PNodeTaskInfo): Double = row.ancSelfTime * 1e-6
    override def toolTip: String =
      "Ancillary Self Time<br>Defined as sum of self times of all non-cached child nodes<br><br>" +
        "<div style='font-family:consolas;font-size:12pt'><span style='color:blue'>def</span> g = given(tweaks) { <span style='color:green'>some_code</span> }</div>" +
        "<span style='color:green'>some_code</span> is represented by a node (<i>g_given_12</i> where 12 is the line number in the source file)" +
        "<br>ANC Time for node <i>g</i> will be the selfTime of the <i>g_given_12</i> node"
    override def getHeaderColor: Color = nodeTimingSectionColor
  }

  private val c_postCompleteAndSuspendTime = new TableColumnTime[PNodeTaskInfo]("Post Complete/Suspend Time (ms)", 80) {
    override def valueOf(row: PNodeTaskInfo): Double = row.postCompleteAndSuspendTime * 1e-6
    override def toolTip: String =
      "Time between node completing/suspending and stopping, across multiple invocations<br><br>" +
        "<i>Most of this time is taken notifying the waiters of this node.<br>" +
        "It's a graph internal time, not related to the user code in this node</i>"
    override def getHeaderColor: Color = nodeTimingSectionColor
  }

  private val c_wallTime_perStart = new TableColumnTime[PNodeTaskInfo]("Wall Time per Node (ms)", 80) {
    override def valueOf(row: PNodeTaskInfo): Double = if (row.start == 0) 0d else (row.wallTime / row.start) * 1e-6
    override def toolTip: String =
      "Derived Definition: Wall Time / Started" +
        "<br>Average of node wall time per node started"
    override def getHeaderColor: Color = nodeTimingSectionColor
  }
  private val c_selfTime_perStart = new TableColumnTime[PNodeTaskInfo]("Self Time per Node (ms)", 80) {
    override def valueOf(row: PNodeTaskInfo): Double = if (row.start == 0) 0d else (row.selfTime / row.start) * 1e-6
    override def toolTip: String =
      "Derived Definition: Self Time / Started" +
        "<br>Average of node self time per node started"
    override def getHeaderColor: Color = nodeTimingSectionColor
  }
  private val c_ancTime_perStart = new TableColumnTime[PNodeTaskInfo]("ANC Time per Node (ms)", 80) {
    override def valueOf(row: PNodeTaskInfo): Double = if (row.start == 0) 0d else (row.wallTime / row.start) * 1e-6
    override def toolTip: String =
      "Derived Definition: ANC Time / Started" +
        "<br>Average of node ANC time per node started"
    override def getHeaderColor: Color = nodeTimingSectionColor
  }
  private val c_fullTime_perStart = new TableColumnTime[PNodeTaskInfo]("Full Time per Node (ms)", 80) {
    override def valueOf(row: PNodeTaskInfo): Double =
      if (row.start == 0) 0d else (row.ancAndSelfTime / row.start) * 1e-6
    override def toolTip =
      "Derived Definition: Self Time + ANC Time / Started"
    override def getHeaderColor: Color = nodeTimingSectionColor
  }
  private val c_totalGraphTime = new TableColumnTime[PNodeTaskInfo]("Total Graph Time (ms)", 80) {
    override def valueOf(row: PNodeTaskInfo): Double =
      (row.postCompleteAndSuspendTime + row.selfTime + row.ancSelfTime) * 1e-6
    override def toolTip = "Derived Definition: Self Time + Post Complete/Suspend Time"
    override def getHeaderColor: Color = nodeTimingSectionColor
  }

  private[ui] val c_propertyNode = new TableColumnString[PNodeTaskInfo]("Property/Node", 300) {
    override def valueOf(row: PNodeTaskInfo): String = row.fullName()
    override def getHeaderColor: Color = nodeInfoSectionColor
    override def summaryType: TableColumn.SummaryType = TableColumn.SummaryType.Count
  }
  private val c_propertyShort = new TableColumnString[PNodeTaskInfo]("Property/Node Short", 200) {
    override def valueOf(row: PNodeTaskInfo): String = row.fullNamePackageShortened()
    override def toolTip = "Shortened Property/Node Name"
    override def summaryType: TableColumn.SummaryType = TableColumn.SummaryType.Count
    override def getHeaderColor: Color = nodeInfoSectionColor
  }

  // PNodeTaskInfo (ie, hotspot row) ID to list of applet names
  private[ui] val idsToApplets: mutable.Map[Int, Set[String]] = mutable.Map[Int, Set[String]]()

  private[ui] val c_applets = new TableColumnString[PNodeTaskInfo]("Applets", 300) {
    override def valueOf(row: PNodeTaskInfo): String = {
      val maybeAppletNames = idsToApplets.get(row.id)
      val maybeResult = maybeAppletNames.map(_.filterNot(_.contains("[not tracking]")).mkString(","))
      maybeResult.getOrElse("")
    }
    override def getHeaderColor: Color = nodeInfoSectionColor
    override def toolTip: String = "List of applets that ran nodes of this property type"
  }

  private val c_plugins = new TableColumnString[PNodeTaskInfo]("Scheduler Plugin", 300) {
    override def valueOf(pnti: PNodeTaskInfo): String = pnti.nti.shouldLookupPlugin().toString
    override def getHeaderColor: Color = nodeInfoSectionColor
    override def toolTip: String = " Whether Scheduler plugin may be set on this property"
  }

  private val c_minRetainedSize = new TableColumnLong[PNodeTaskInfo]("Min Retained Size", 80) {
    override def valueOf(row: PNodeTaskInfo): Long =
      if (row.jvmInfo eq null) 0 else row.jvmInfo.retainedSize - row.jvmInfo.sharedRetainedSize + row.jvmInfo.size
  }
  private val c_maxRetainedSize = new TableColumnLong[PNodeTaskInfo]("Max Retained Size", 120) {
    override def valueOf(row: PNodeTaskInfo): Long =
      if (row.jvmInfo eq null) 0 else row.jvmInfo.retainedSize + row.jvmInfo.size
  }
  private val c_cachedCount = new TableColumnCount[PNodeTaskInfo]("Cached Count", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = if (row.jvmInfo eq null) 0 else row.jvmInfo.count
  }
  private val c_reuseCycle = new TableColumnCount[PNodeTaskInfo]("Reuse Cycle", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.reuseCycle
  }
  private val c_simpleMatch = new TableColumnCount[PNodeTaskInfo]("Simple Match", 80) {
    override def valueOf(row: PNodeTaskInfo): Int = row.cacheHitTrivial
    override def toolTip = "Number of entity matches that could have been reference matches"
  }
  private val c_cacheName = new TableColumnString[PNodeTaskInfo]("Cache Description", 80) {
    override def valueOf(row: PNodeTaskInfo): String = row.cacheDescription
    override def toolTip = "Cache Policy and Cache Name (if set)"
    override def getHeaderColor: Color = nodeInfoSectionColor
  }

  val missingID: Int = -1
  val extraID: Int = -2
  private val c_missingOrExtra = new TableColumnString[PNodeTaskInfo](TableView.deltaSymbol, 10) {
    override def valueOf(row: PNodeTaskInfo, row2: PNodeTaskInfo): String =
      if (row2.id == missingID) "M" else if (row2.id == extraID) "E" else ""
    override def toolTip: String =
      "<html>M - If row was missing from the 'compare to' set (present in the original)<br>" +
        "E - If row is extra in the 'compare to' set (not present in the original)</html>"
  }

  // combine these with section colours above
  val nodeStats: Category = Category("Node Details", 1)
  val timeCategory: Category = Category("Times", 2)
  val cacheCategory: Category = Category("Cache", 3)
  val timePerStartCategory: Category = Category("Times Per Start", 4)
  val cachePerStartCategory: Category = Category("Cache Stats Per Start", 5)
  val distinctCountCategory: Category = Category("Distinct Counts", 6)
  val invalidatesCategory: Category = Category("Invalidates", 7)
  val memoryCategory: Category = Category("Memory", 8)
  val internalCategory: Category = Category("Internal", 9)

  val runColumns: ArrayBuffer[TableColumnLong[PNodeTaskInfo]] = ArrayBuffer(c_started)
  runColumns.foreach(_.setCategory(nodeStats))

  val basicTimeColumns: ArrayBuffer[TableColumnTime[PNodeTaskInfo]] =
    ArrayBuffer(c_wallTime, c_selfTime, c_ancTime, c_tweakLookupTime)
  basicTimeColumns.foreach(_.setCategory(timeCategory))

  val basicTimesPerStartColumns: ArrayBuffer[TableColumnTime[PNodeTaskInfo]] =
    ArrayBuffer(c_wallTime_perStart, c_selfTime_perStart, c_ancTime_perStart, c_fullTime_perStart)
  basicTimesPerStartColumns.foreach(_.setCategory(timePerStartCategory))

  val nodeCacheStatColumns: ArrayBuffer[TableColumn[PNodeTaskInfo]] =
    ArrayBuffer(
      c_evicted,
      c_retained,
      c_cacheHit,
      c_cacheHitTrivial,
      c_cacheHitTrivialFailed,
      c_cacheMiss,
      c_cacheHitRatio,
      c_hashCollision)
  nodeCacheStatColumns.foreach(_.setCategory(cacheCategory))

  val distinctCountColumns: ArrayBuffer[TableColumnCount[PNodeTaskInfo]] = ArrayBuffer(
    c_distinctScenarioStack,
    c_distinctIdScenarioStack,
    c_distinctScenarioStackDiff,
    c_distinctEntities,
    c_distinctIdEntities,
    c_distinctEntitiesDiff,
    c_distinctArgs,
    c_distinctIdArgs,
    c_distinctArgsDiff,
    c_distinctResults,
    c_distinctIdResults,
    c_distinctResultsDiff,
    c_distinctCombined,
    c_distinctIdCombined,
    c_distinctCombinedDiff
  )
  distinctCountColumns.foreach(_.setCategory(distinctCountCategory))

  val distinctCountDebugColumns: ArrayBuffer[TableColumnCount[PNodeTaskInfo]] = ArrayBuffer(
    c_debugCountScenarioStack,
    c_debugCountEntities,
    c_debugCountArgs,
    c_debugCountResults,
    c_debugCountCombined
  )
  distinctCountDebugColumns.foreach(_.setCategory(distinctCountCategory))

  val cacheTimeColumns: ArrayBuffer[TableColumnTime[PNodeTaskInfo]] = ArrayBuffer(c_cacheTime)
  cacheTimeColumns.foreach(_.setCategory(timeCategory))

  val cacheTimePerStartColumns: ArrayBuffer[TableColumnTime[PNodeTaskInfo]] = ArrayBuffer(c_cacheTimeAvg)
  cacheTimePerStartColumns.foreach(_.setCategory(timePerStartCategory))

  var cacheAnalysisColumns: ArrayBuffer[TableColumn[PNodeTaskInfo]] =
    ArrayBuffer(
      c_cacheReuseTime,
      c_nodeUseTime,
      c_cacheBenefit,
      c_cacheReuseTimeAmortized,
      c_cacheBenefitAmortized,
      c_children,
      c_childCacheTime
    )
  cacheAnalysisColumns.foreach(_.setCategory(cacheCategory))

  val internalTimeColumns: ArrayBuffer[TableColumnTime[PNodeTaskInfo]] =
    ArrayBuffer(c_postCompleteAndSuspendTime, c_totalGraphTime)
  internalTimeColumns.foreach(_.setCategory(timeCategory))

  val cacheAnalysisPerStartColumns: ArrayBuffer[TableColumnTime[PNodeTaskInfo]] =
    ArrayBuffer(c_childrenPerNode, c_childCacheTimePerNode)
  cacheAnalysisPerStartColumns.foreach(_.setCategory(cachePerStartCategory))

  val invalidateColumns: ArrayBuffer[TableColumnCount[PNodeTaskInfo]] = ArrayBuffer(c_invalidated, c_overInvalidated)
  invalidateColumns.foreach(_.setCategory(invalidatesCategory))

  val descriptionColumns: ArrayBuffer[TableColumn[PNodeTaskInfo]] =
    ArrayBuffer(TableColumn.flagsTableColumn, c_propertyNode, c_applets, c_plugins)
  descriptionColumns.foreach(_.setCategory(nodeStats))

  val descriptionCompactColumns: ArrayBuffer[TableColumn[PNodeTaskInfo]] =
    ArrayBuffer(TableColumn.flagsTableColumn, c_propertyShort)
  descriptionCompactColumns.foreach(_.setCategory(nodeStats))

  val memColumns: ArrayBuffer[TableColumn[PNodeTaskInfo]] =
    ArrayBuffer(c_minRetainedSize, c_maxRetainedSize, c_cachedCount)
  memColumns.foreach(_.setCategory(memoryCategory))

  val cacheDetailsColumns: ArrayBuffer[TableColumn[PNodeTaskInfo]] =
    ArrayBuffer(c_reuseCycle, c_simpleMatch, c_cacheName)
  cacheDetailsColumns.foreach(_.setCategory(cacheCategory))

  val internalColumns: ArrayBuffer[TableColumn[PNodeTaskInfo]] = ArrayBuffer(c_tweakID, c_tweakDependencies, c_process)
  internalColumns.foreach(_.setCategory(internalCategory))

  val allColumnsSorted: ArrayBuffer[TableColumn[PNodeTaskInfo]] =
    (runColumns ++ basicTimeColumns ++ basicTimesPerStartColumns ++ nodeCacheStatColumns ++
      distinctCountColumns ++ distinctCountDebugColumns ++ cacheTimeColumns ++ cacheTimePerStartColumns ++
      cacheAnalysisColumns ++ cacheAnalysisPerStartColumns ++ invalidateColumns ++ descriptionColumns ++
      internalColumns ++ descriptionCompactColumns ++ memColumns ++ cacheDetailsColumns ++ internalTimeColumns).distinct
      .sortBy(_.name)

  val viewBasic: ArrayBuffer[TableColumn[PNodeTaskInfo]] = ArrayBuffer(
    c_started,
    c_evicted,
    c_cacheHit,
    c_cacheHitTrivial,
    c_cacheMiss,
    c_cacheBenefit,
    c_selfTime,
    c_postCompleteAndSuspendTime,
    c_ancTime,
    c_tweakLookupTime,
    TableColumn.flagsTableColumn,
    c_propertyShort
  )

  val viewBasicThresholdTuning: ArrayBuffer[TableColumn[PNodeTaskInfo]] = ArrayBuffer(
    c_started,
    c_cacheHit,
    c_cacheHitTrivial,
    c_cacheMiss,
    c_cacheBenefit,
    c_selfTime,
    c_ancTime,
    c_propertyShort
  )

  val viewCompact: ArrayBuffer[TableColumn[PNodeTaskInfo]] =
    runColumns ++ nodeCacheStatColumns ++ cacheTimeColumns ++ basicTimeColumns ++ descriptionCompactColumns
  val viewCompactPerNode: ArrayBuffer[TableColumn[PNodeTaskInfo]] =
    runColumns ++ nodeCacheStatColumns ++ cacheTimePerStartColumns ++ basicTimesPerStartColumns ++ descriptionCompactColumns

  val viewGeneral: ArrayBuffer[TableColumn[PNodeTaskInfo]] =
    runColumns ++ nodeCacheStatColumns ++ cacheTimeColumns ++ basicTimeColumns ++ cacheAnalysisColumns ++ descriptionColumns

  val viewCacheContents: ArrayBuffer[TableColumn[PNodeTaskInfo]] =
    runColumns ++ nodeCacheStatColumns ++ distinctCountColumns ++ cacheTimeColumns ++ basicTimeColumns ++ descriptionColumns
  val viewMemory: ArrayBuffer[TableColumn[PNodeTaskInfo]] =
    runColumns ++ nodeCacheStatColumns ++ invalidateColumns ++ cacheDetailsColumns ++ memColumns ++ basicTimeColumns ++ descriptionColumns

  // either the nodes have the same id so are of the same type or the underlying node should have the same id (to account for XSFT proxies)
  private def analyzeFromCacheFilter(i: Int, p: PropertyNode[_]): Boolean =
    (p.getProfileId == i || p.cacheUnderlyingNode.getProfileId == i)

  private[ui] def getNodesFromCacheWithSameIDOrUnderlyingID(i: Int): util.ArrayList[PNodeTask] =
    DebuggerUI.getPropertyNodesInCaches(analyzeFromCacheFilter(i, _))
}

class HotspotsTable(reader: OGTraceReader)
    extends NPTable[PNodeTaskInfo]
    with DbgPrintSource
    with Filterable[PNodeTaskInfo] {
  import HotspotsTable._
  emptyRow = new PNodeTaskInfo(0, "", "")

  override def initialColumns: ArrayBuffer[TableColumn[PNodeTaskInfo]] = allColumnsSorted
  override def initialViewColumns: ArrayBuffer[TableColumn[PNodeTaskInfo]] = viewCompact
  private val isTweakHighlightEnabled: DbgPreference = DbgPreference("Tweak Dependency Highlights", default = false)
  private val showAllTweakables: DbgPreference = DbgPreference("Show All Tweakables", default = false)
  private[ui] val showAllTweakablesFn: Boolean => Unit = { b =>
    showAllTweakables.set(b)
    HotspotsTable.this.setList(NodeProfiler.collectProfile(b))
  }

  private def select: PNodeTaskInfo = {
    val selection = getSelection
    DebuggerUI.selectedTaskInfo = selection
    selection
  }
  // no task here, but DbgPrintSource forces us to implement this
  override def task: PNodeTask = null

  private[ui] var xsftButton: JMenuItem = _
  private[ui] var disableButton: JMenuItem = _
  dataTable.setComponentPopupMenu(initPopupMenu)
  dataTable.addMouseListener(new MouseAdapter() {
    override def mousePressed(e: MouseEvent): Unit = {
      if (SwingUtilities.isRightMouseButton(e)) {
        val r: Int = dataTable.rowAtPoint(e.getPoint)
        if (getSelection == null) {
          if (r >= 0 && r < dataTable.getRowCount) dataTable.setRowSelectionInterval(r, r)
          select
        }
      }
    }
  })
  dataTable.getSelectionModel.addListSelectionListener(_ => {
    val selection = select
    if (isTweakHighlightEnabled.get && (selection ne null)) {
      setTweakDependencyHighlight(selection)
    }
  })

  var ns_tab: JTabbedPane = _
  var ns_table: NodeStacksTable = _ // Selected Node Stacks
  var uiProfiler: UIProfiler = _ // Link up to nodes invalidates
  private var _aggregateProxies: Boolean = true

  /*
    HotspotsTable renders nodes which have been referred to after the entrypoint into the graph
    If node depends on tweakableA, but tweakabeA is not used in def running in the graph,
    tweakableA will not be shown in the HotspotsTable -> it will not be highlighted
   */
  def setTweakDependencyHighlight(selected: PNodeTaskInfo): Unit = {
    MarkTweakDependency.intialiseMap()
    MarkTweakDependency.setTweakDependencyHighlight(selected, rows)
    refresh(resetTotals = false)
  }

  private def initPopupMenu: JPopupMenu = {
    val menu = new JPopupMenu2()

    val needsNodeTrace = () => !NodeTrace.getTraceIsEmpty

    menu.addAdvCheckBoxMenu(
      "Show all tweakables",
      "Shows all tweakables even if they were not tweaked",
      showAllTweakables)(showAllTweakablesFn)

    addMenuSS("Show all nodes (of this type) from cache...") { i =>
      val nodes = getNodesFromCacheWithSameIDOrUnderlyingID(i.id)
      if (nodes.isEmpty) GraphDebuggerUI.showMessage("Nothing found in caches to match this node type")
      else DebuggerUI.browse(nodes, ignoreFilter = true)
    }

    // Single Selection Wrapper
    def addMenuSS(title: String, enabledOnlyOn: () => Boolean = null)(ntiAction: PNodeTaskInfo => Unit): JMenuItem =
      addCustomMenuSS(menu, title, enabledOnlyOn)(ntiAction)

    def addCustomMenuSS(m: JMenuOps, title: String, enabledOnlyOn: () => Boolean = null)(
        ntiAction: PNodeTaskInfo => Unit): JMenuItem = {
      val mi = m.addMenu(
        title, {
          val i = getSelection
          if (i ne null) ntiAction(i)
        })
      m.addOnPopup(mi.setEnabled((getSelection ne null) && ((enabledOnlyOn eq null) || enabledOnlyOn())))
      mi
    }

    // Multiple Selection Wrapper
    def addMenuMS(title: String, enabledOnlyOn: () => Boolean = null)(
        ntiAction: ArrayBuffer[PNodeTaskInfo] => Unit): JMenuItem = {
      val mi = menu.addMenu(
        title, {
          val lst = getSelections
          if (lst.nonEmpty) ntiAction(lst)
        })
      menu.addOnPopup(mi.setEnabled((getSelection ne null) && ((enabledOnlyOn eq null) || enabledOnlyOn())))
      mi
    }

    def addMenuForTrace(title: String)(action: util.ArrayList[PNodeTask] => Unit): Unit = {
      addMenuMS(title, needsNodeTrace) { lst =>
        val selectedIDs = lst.map(_.id).toSet
        val trace = NodeTrace.getTraceBy(n => selectedIDs.contains(n.getProfileId), false)
        if (trace.isEmpty) GraphDebuggerUI.showMessage("No matching nodes found in trace")
        else action(trace)
      }
    }

    def addMenu(subMenu: JMenu2, title: String, ntiAction: NodeTaskInfo => Unit): JMenuItem = {
      subMenu.addMenu(
        title, {
          for (pnti <- getSelections if pnti.nti ne null) {
            ntiAction(pnti.nti)
            pnti.freezeLiveInfo()
          }
          dataTable.repaint() // TODO (OPTIMUS-0000): Too much repainting here
        },
        true
      )
    }

    def cmdShowNodeStacks(i: PNodeTaskInfo, toCallee: Boolean, skipCacheable: Boolean): Unit = {
      if (ns_tab.indexOfComponent(ns_table) < 0)
        ns_tab.add("Node Stack", ns_table)
      ns_table.setMessage(null)
      ns_table.selectStackOf(i, toCallee, skipCacheable)
      ns_tab.setSelectedComponent(ns_table)
    }

    if (!reader.isLive) {
      addMenuSS("Show Parent Node Stacks...") { i =>
        cmdShowNodeStacks(i, NodeStacks.CALLER, skipCacheable = false)
      }
      addMenuSS("Show Parent Node Stacks (Skip non-cacheable...)") { i =>
        cmdShowNodeStacks(i, NodeStacks.CALLER, skipCacheable = true)
      }
      menu.addSeparator()
      addMenuSS("Show Children Node Stacks...") { i =>
        cmdShowNodeStacks(i, NodeStacks.CALLEE, skipCacheable = false)
      }
      addMenuSS("Show Children Node Stacks (Skip non-cacheable...)") { i =>
        cmdShowNodeStacks(i, NodeStacks.CALLEE, skipCacheable = true)
      }
    }

    if (reader.isLive) {

      /**
       * TODO (OPTIMUS-30206): Re-enable these once concurrency analysis tools are in place addMenuSS("Show in Node
       * Stacks...") { i => ns_table.openTo(i) ns_tab.setSelectedComponent(ns_table) }
       *
       * addMenuSS("Show in Concurrency View...") { i => ncr_table.openTo(i) ns_tab.setSelectedComponent(ncr_table) }
       */
      menu.addMenu("Print Source", printSourceWithPopUp(getSelections, dataTable))
      menu.addAdvMenu("Inspect Property Info in Console") {
        for (pnti <- getSelections) GraphConsole.instance.dump(pnti, "pnti")
      }

      addMenuSS("Show Invalidates...") { i =>
        InvalidatedNodeTable.openTo(i.nti)
      }

      addMenuForTrace("Show in Browser...") { nodes =>
        GraphDebuggerUI.addTab("Selected in Profiler", new GraphBrowser(nodes))
      }
      addMenuForTrace("Add from Trace To Selected Nodes...") { nodes =>
        GraphDebuggerUI.showSelectedNodesView(nodes.asScala)
      }

      menu.addSeparator()
      addMenuForTrace("Analyze Calculation from Trace...") { nodes =>
        val anyNodeName = nodes.get(0).nodeName().toString
        def generator() = {
          GraphBrowserAPI.generateNodeReviewForDiff(nodes).toList
        }
        val na = new NodeAnalyze(generator)
        GraphDebuggerUI.addTab(anyNodeName + " Nodes from Trace", na, na.onTabClose)
      }

      addMenuForTrace("Analyze Potential Cache Saving From Trace...") { nodes =>
        NodeProfiler.displayNodeAnalyzeFull(() => nodes.asScala.toList, "cache")
      }

      addMenuSS("Analyze Calculation from Cache...") { i =>
        val na = new NodeAnalyze(() =>
          GraphBrowserAPI.generateNodeReviewForDiff(getNodesFromCacheWithSameIDOrUnderlyingID(i.id)).toList)
        GraphDebuggerUI.addTab(i.fullName() + " Nodes from Cache", na, na.onTabClose)
      }

      menu.addMenu(
        "Analyze Potential Cache Saving...", {
          val ids = getSelections.map(_.id)
          NodeProfiler.displayNodeAnalyzeFull(
            () => DebuggerUI.getPropertyNodesInCaches(p => ids.exists(analyzeFromCacheFilter(_, p))).asScala.toList,
            "cache")
        }
      )
      menu.addSeparator()
    }

    {
      val concurrency = new JMenu2("Concurrency")
      menu.add(concurrency)

      addCustomMenuSS(concurrency, "Concurrency Report (Whole program analysis)...") { pnti =>
        val nodeName = pnti.nodeName
        val res = GraphAlgos.concurrencyTracing.minimumUnitCost(reader, nodeName)
        GraphDebuggerUI.showTimeline("Concurrency Report for " + nodeName.toString(true), res)
      }.setToolTipText(
        "Whole program analysis, so reuse between graph entry points will not count towards concurrency cost")

      addCustomMenuSS(concurrency, "Concurrency Report (Treat graph entries independently)...") { pnti =>
        val nodeName = pnti.nodeName
        val res = GraphAlgos.concurrencyTracingPerEntry.minimumUnitCost(reader, nodeName)
        GraphDebuggerUI.showTimeline("Concurrency Report for " + nodeName.toString(true), res)
      }.setToolTipText("Concurrency analysis per graph entry point")
    }

    menu.addSeparator();
    {
      val caching = new JMenu2("Caching")
      menu.add(caching)

      def add(title: String, ntiAction: NodeTaskInfo => Unit): JMenuItem = addMenu(caching, title, ntiAction)

      def enableCachePolicy(nti: NodeTaskInfo, policy: NCPolicy): Unit = {
        nti.markAsProfilerUIConfigured()
        nti.setCachePolicy(policy) // this will setCacheable too
        ProfilerUI.pref.putBoolean("ExportConfig.favorReuse", true)
        log.info(s"Updated cache policy for ${nti.name} to ${policy}")
      }

      add(
        "Remove from Cache",
        i =>
          Caches
            .allCaches(
              includeSiGlobal = true,
              includeGlobal = true,
              includeNamedCaches = true,
              includePerPropertyCaches = true,
              includeCtorGlobal = false)
            .foreach {
              _.clear(
                n => {
                  val candidate = n.cacheUnderlyingNode
                  candidate.getProfileId == i.profile || n.getProfileId == i.profile
                },
                CauseProfiler)
            }
      )
      caching.addSeparator()

      disableButton = add("Disable Cache", enableCachePolicy(_, NCPolicy.DontCache))
      add("Disable XSFT/Enable Basic", enableCachePolicy(_, NCPolicy.Basic))
      xsftButton = add("Enable XSFT", enableCachePolicy(_, NCPolicy.XSFT))
      add("Enable XS", enableCachePolicy(_, NCPolicy.XS))

      caching.addAdvCheckBoxMenu(
        "Enable Tweak Dependency Highlight",
        "Auto-highlight all Hotspots that correspond to the Tweak Dependencies of a Selected Node",
        isTweakHighlightEnabled
      ) { enable =>
        isTweakHighlightEnabled.set(enable)
        refresh(resetTotals = false)
      }
      caching.addSeparator()

      add("Cache -> default", nti => { nti.setCustomCache(null) })
      add("Cache -> Private(50)", nti => nti.setCustomCache(NodeCache.createPerPropertyCache(50)))
      add("Cache -> Private(100)", nti => nti.setCustomCache(NodeCache.createPerPropertyCache(100)))
      add("Cache -> Private(1000)", nti => nti.setCustomCache(NodeCache.createPerPropertyCache(1000)))
      (1 to 9) foreach { index =>
        val cacheName = "a" + index
        add(
          "Cache -> " + cacheName,
          nti => {
            nti.setCustomCache(NodeCacheConfigs.createNamedCache(cacheName))
            nti.markAsProfilerUIConfigured()
          }
        )
      }
    }

    menu.addSeparator();
    {
      val tracing = new JMenu2("Tracing")

      menu.add(tracing)

      def add(title: String, ntiAction: NodeTaskInfo => Unit): Unit = addMenu(tracing, title, ntiAction)

      val enableAll = tracing.addCheckBoxMenu("Enable All Individual Properties", null, checked = false) {
        NodeTrace.markAllPropertiesToBeTraced(true)
      }
      val disableAll = tracing.addCheckBoxMenu("Disable All Individual Properties", null, checked = false) {
        NodeTrace.markAllPropertiesToBeTraced(false)
      }
      menu.addOnPopup({
        enableAll.setSelected(Settings.defaultToIncludeInTrace)
        disableAll.setSelected(!Settings.defaultToIncludeInTrace)
      })
      tracing.addSeparator() // [SEE_PARTIAL_TRACE] // TODO (OPTIMUS-43938): config file to control this
      add("Enable Selected Properties -> Parents", nti => nti.setTraceSelfAndParent(true))
      add("Disable Selected Properties -> Parents", nti => nti.setTraceSelfAndParent(false))
      add("Set Profiler Ignore", nti => nti.setIgnoredInProfiler(true))
    }

    menu
  }

  override def printSource(): Unit = for (pnti <- getSelections) NodeName.printSource(pnti)

  // Not live data might as well be refreshed right away
  if (!reader.isLive) cmdRefresh()

  def cmdRefresh(): Unit = {
    val pntis = reader.getHotspots(_aggregateProxies).asScala
    setList(ArrayBuffer(pntis: _*))
  }

  def aggregateProxies: Boolean = _aggregateProxies
  def aggregateProxies_=(enable: Boolean): Unit = {
    _aggregateProxies = enable
    cmdRefresh()
  }

  override protected def diffAnnotationColumn: TableColumn[PNodeTaskInfo] = c_missingOrExtra

  override def afterUpdateList(): Unit = {
    var newCTRows = 0 // New Compare To Rows (i.e. not found in the existing set)
    var newExRows = 0 // New Existing Rows (i.e. not found in the compare to set)

    if (orgRowsC.nonEmpty) {
      rowsC = new ArrayBuffer[PNodeTaskInfo]()
      rows = rows.clone
      val hash = new util.HashMap[PNodeTaskInfo, PNodeTaskInfo]()

      // Put all 'rows to compare' but try not losing the information
      // For example in the original code 2 names might come from the different nodes (rare but possible)
      // In this new set, they will be merged into one [1]
      for (tt <- orgRowsC) {
        tt.id = extraID // Start as not found in the original set
        val prev = hash.get(tt)
        if (prev ne null) prev.combine(tt) // [1]
        else hash.put(tt, tt)
      }

      // Find the matching row from the hash of 'compare to rows' and put in the matching position
      // Also marked it as 'matched' by setting the id = 0
      // If no match exists we just add an empty PNTI with matching okgName and name
      for (tt <- rows) {
        val ttc = hash.get(tt)
        val cRow =
          if (ttc ne null) { ttc.id = 0; ttc }
          else {
            newCTRows += 1
            new PNodeTaskInfo(missingID, tt.pkgName, tt.name)
          }
        rowsC += cRow
      }

      // Whatever we didn't match in the previous loop we will add empty PNTI to existing rows
      for (tt <- orgRowsC) {
        if (tt.id == extraID) {
          newExRows += 1
          rows += new PNodeTaskInfo(0, tt.pkgName, tt.name)
          rowsC += tt
        }
      }
    } else {
      rowsC = new ArrayBuffer[PNodeTaskInfo]()
    }
  }

  override def getRowBackground(col: TableColumn[PNodeTaskInfo], row: PNodeTaskInfo): Color = {
    if (isTweakHighlightEnabled.get && MarkTweakDependency.isMapInitialised)
      MarkTweakDependency.getTweakDependencyHighlight(row) match {
        case Some(SelectionFlags.Poison)     => NPTableRenderer.darkPink
        case Some(SelectionFlags.Dependent)  => NPTableRenderer.lightPink
        case Some(SelectionFlags.Dependency) => NPTableRenderer.lightOrange
        case _                               => super.getRowBackground(col, row)
      }
    else super.getRowBackground(col, row)
  }
}
