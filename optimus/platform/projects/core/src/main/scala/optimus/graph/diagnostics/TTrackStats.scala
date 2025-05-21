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

import com.codahale.metrics.Snapshot
import optimus.graph.NodeTaskInfo
import optimus.graph.tracking.DependencyTracker
import optimus.ui.ScenarioReference

//dont use scala StringBuilder
import java.lang.StringBuilder

final case class PerTrackerStatsKey(rootID: Int, tracker: DependencyTracker)

object TTrackStats {
  def propertiesSortedByTrackingGraphSize(
      subset: Map[NodeTaskInfo, PerPropertyStats]
  ): Seq[(NodeTaskInfo, PerPropertyStats)] =
    subset.toSeq.sortWith(_._2.numNodes > _._2.numNodes)
}

final case class TTrackStats(
    perPropertyStats: Map[NodeTaskInfo, PerPropertyStats],
    perTrackerStats: Map[PerTrackerStatsKey, PerTrackerStats]) {

  def propertiesSortedByTrackingGraphSize(
      subset: Map[NodeTaskInfo, PerPropertyStats] = perPropertyStats
  ) = TTrackStats.propertiesSortedByTrackingGraphSize(subset)

  def summarisePerProperty(): String = {
    def totalNodesInTrackingGraph(subset: Map[NodeTaskInfo, PerPropertyStats] = perPropertyStats): Int =
      subset.values.map { _.numNodes }.sum

    def totalNodesTracked(subset: Map[NodeTaskInfo, PerPropertyStats] = perPropertyStats): Int =
      subset.values.map { _.numTrackedTweakables }.sum

    def sortedAsString(results: Seq[(NodeTaskInfo, PerPropertyStats)], includeIsTweaked: Boolean = false): Seq[String] =
      results.map { case (pinfo, stats) =>
        val isTweaked =
          if (includeIsTweaked)
            s"(tweaked in tracker = ${stats.wasInstanceTweaked || stats.wasPropertyTweaked}, " +
              s"tweaked anywhere = ${pinfo.wasTweakedAtLeastOnce()})"
          else ""
        s"${pinfo.fullName}: ${stats.numNodes} ${isTweaked}"
      }

    val numProperties = perPropertyStats.size

    // find properties that are tweaked in tracker
    val instTweakedInTracker = perPropertyStats.filter(entry => entry._2.wasInstanceTweaked)
    val propTweakedInTracker = perPropertyStats.filter(entry => entry._2.wasPropertyTweaked)

    val trackingGraphSize = totalNodesInTrackingGraph()
    val instTweaksTrackingGraphSize = totalNodesInTrackingGraph(instTweakedInTracker)
    val propTweaksTrackingGraphSize = totalNodesInTrackingGraph(propTweakedInTracker)

    val nodesTracked = totalNodesTracked()
    val nodesTrackedForInstTweaks = totalNodesTracked(instTweakedInTracker)
    val nodesTrackedForPropTweaks = totalNodesTracked(propTweakedInTracker)

    val sortedProperties = propertiesSortedByTrackingGraphSize()
    val sortedInstTweaks = propertiesSortedByTrackingGraphSize(instTweakedInTracker)
    val sortedPropTweaks = propertiesSortedByTrackingGraphSize(propTweakedInTracker)

    val sb = new StringBuilder
    sb.append("-------------------Per property stats-------------------\n")
    sb.append(s"${numProperties} unique properties\n")

    sb.append(
      s"Properties with biggest tracking graphs:\n${sortedAsString(sortedProperties.take(100), true).mkString("\n")}\n")
    sb.append(s"\nInstance tweaked:\n${sortedAsString(sortedInstTweaks.take(10)).mkString("\n")}\n")
    sb.append(s"\nProperty tweaked:\n${sortedAsString(sortedPropTweaks.take(10)).mkString("\n")}\n")

    sb.append(
      s"\n${instTweakedInTracker.size} properties instance tweaked " +
        s"(${Math.round((100 * instTweakedInTracker.size / numProperties).toFloat)}%)\n")

    sb.append(
      s"${propTweakedInTracker.size} properties property tweaked " +
        s"(${Math.round((100 * propTweakedInTracker.size / numProperties).toFloat)}%)\n")

    sb.append(s"${trackingGraphSize} total nodes in tracking graph\n")
    sb.append(
      s"${instTweaksTrackingGraphSize} " +
        s"total nodes in tracking graph for instance tweaked properties " +
        s"(${Math.round((100 * instTweaksTrackingGraphSize / trackingGraphSize).toFloat)}%)\n")

    sb.append(
      s"${propTweaksTrackingGraphSize} " +
        s"total nodes in tracking graph for property tweaked properties " +
        s"(${Math.round((100 * propTweaksTrackingGraphSize / trackingGraphSize).toFloat)}%)\n")

    sb.append(s"${nodesTracked} total nodes tracked\n")
    sb.append(
      s"${nodesTrackedForInstTweaks} " +
        s"total nodes tracked for instance tweaked properties " +
        s"(${Math.round((100 * nodesTrackedForInstTweaks / nodesTracked).toFloat)}%)\n")

    sb.append(
      s"${nodesTrackedForPropTweaks} " +
        s"total nodes tracked for property tweaked properties " +
        s"(${Math.round((100 * nodesTrackedForPropTweaks / nodesTracked).toFloat)}%)\n")

    sb.toString()
  }

  def summarisePerTrackerStats(): String = {
    val sb = new StringBuilder

    val numTrackers = perTrackerStats.size
    val numTweaks = perTrackerStats.values.map { _.numTweaks }.sum
    val trackingGraphSize = perTrackerStats.values.map { _.numNodesInTrackingGraph }.sum
    val nodesTracked = perTrackerStats.values.map { _.numTrackedTweakables }.sum
    val nodesUTracked = perTrackerStats.values.map { _.numObservedNodes }.sum

    sb.append("-------------------Per tracker stats-------------------\n")
    sb.append(s"${numTrackers} trackers\n")
    sb.append(s"${numTweaks} tweaks\n")
    sb.append(s"${nodesTracked} total nodes tracked\n")
    sb.append(s"${nodesUTracked} total nodes observed\n")
    sb.append(s"${trackingGraphSize} total nodes in tracking graph\n")
    sb.toString()
  }

  def summarise(): String = s"${summarisePerProperty()}\n${summarisePerTrackerStats()}"
}

final case class PerTrackerStats(
    rootID: Int,
    scenarioReference: ScenarioReference,
    numTweaks: Int, // number of tweaks
    numObservedNodes: Int, // number of observed nodes, ie, utracks
    numTrackedTweakables: Int, // number of tracked tweakables, ie, ttracks
    numNodesInTrackingGraph: Int, // nodes, not including TTrackRef.Nil
    numResets: Int // may indicate we should have pruned earlier
)

object PerPropertyStats {
  def untracked(property: NodeTaskInfo, numRecordedInvalidations: Int): PerPropertyStats =
    apply(
      property = property,
      numRecordedInvalidations = numRecordedInvalidations,
      numTrackedTweakables = 0,
      numNodes = 0,
      impact = 0,
      numInvalidatedNotCleared = 0,
      numInvalidatedAndCleared = 0,
      numNonCacheable = 0,
      maxDepth = 0,
      maxBranching = 0
    )
}

final case class PerPropertyStats(
    @transient override val property: NodeTaskInfo,
    numTrackedTweakables: Int, // number of tracked tweakables, ie, ttracks
    numNodes: Int, // nodes
    impact: Int, // if we switch off tracking for this property, how many nodes do we save?
    numRecordedInvalidations: Int, // number of recorded invalidations since last profiler reset
    numInvalidatedNotCleared: Int, // TTrackRef.Invalid nodes - may indicate we should have pruned earlier
    numInvalidatedAndCleared: Int, // where ttrackRef.get == null
    numNonCacheable: Int, // TTrackRef.Nil nodes
    maxDepth: Int, // distance from root, or size of parents
    maxBranching: Int // max branching, ie, breadth
) extends HasTweakablePropertyInfo {
  val name: String = property.fullName

  val wasEverInstanceTweaked: Boolean = property.wasTweakedByInstance
  val wasEverPropertyTweaked: Boolean = property.wasTweakedByProperty

  //  only true if those tweaks were made in DependencyTracker
  val wasInstanceTweaked: Boolean = property.wasTweakedByInstanceInTracker
  val wasPropertyTweaked: Boolean = property.wasTweakedByPropertyInTracker

  def merge(other: PerPropertyStats): PerPropertyStats = {
    require(other.property == this.property)
    PerPropertyStats(
      property = property,
      numTrackedTweakables = numTrackedTweakables + other.numTrackedTweakables,
      numNodes = numNodes + other.numNodes,
      impact = impact + other.impact,
      // max since these aren't per-tracker
      numRecordedInvalidations = Math.max(numRecordedInvalidations, other.numRecordedInvalidations),
      numInvalidatedNotCleared = numInvalidatedNotCleared + other.numInvalidatedNotCleared,
      numInvalidatedAndCleared = numInvalidatedAndCleared + other.numInvalidatedAndCleared,
      numNonCacheable = numNonCacheable + other.numNonCacheable,
      maxDepth = Math.max(maxDepth, other.maxDepth),
      maxBranching = Math.max(maxBranching, other.maxBranching)
    )
  }
}

trait HasTweakablePropertyInfo {
  def property: NodeTaskInfo
  final def isTrackedForInvalidation: Boolean = if (property ne null) property.trackForInvalidation() else false
}

// Used in testing to avoid asserting the number of Resets etc
final case class TestPerPropertyStats(
    name: String,
    numTrackedTweakables: Int,
    wasEverInstanceTweaked: Boolean,
    wasEverPropertyTweaked: Boolean,
    wasInstanceTweaked: Boolean,
    wasPropertyTweaked: Boolean,
    numNodes: Int,
    maxDepth: Int,
    maxBranching: Int,
    numRecordedInvalidations: Int,
    isTrackedForInvalidation: Boolean,
    numInvalidatedNotCleared: Int,
    numInvalidatedAndCleared: Int,
    numNonCacheable: Int,
    impactCond: Int => Boolean)

final case class StatsSummary(
    maxDepth: Int,
    breadthStats: Snapshot,
    numNodes: Int,
    nodeRefsPerTTrackStats: Snapshot,
    numInvalids: Int,
    numNils: Int,
    numCleared: Int,
    graphSize: Int) {
  private def stats(s: Snapshot): String =
    f"[1st = ${s.getValue(0.01)}%,.0f, 10th = ${s.getValue(0.1)}%,.0f, 50th = ${s.getMedian}%,.0f, " +
      f"90th = ${s.getValue(0.9)}%,.0f, 99th = ${s.getValue(0.99)}%,.0f, 99.9th = ${s.getValue(0.999)}%,.0f, " +
      f"max = ${s.getMax}%,d, mean = ${s.getMean}%,.1f]"

  override def toString: String =
    f"graphSize = $graphSize%,d, maxDepth = $maxDepth%,d, breadth = ${stats(breadthStats)}, " +
      f"numNodes = $numNodes%,d, numNodesPerTTrack = ${stats(
          nodeRefsPerTTrackStats)}, numInvalids = $numInvalids%,d, numNils = $numNils%,d, numCleared = $numCleared%,d"
}
