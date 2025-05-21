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
package optimus.graph.tracking.ttracks

import java.lang
import optimus.graph.NodeTaskInfo
import optimus.graph.NodeTrace
import optimus.graph.TweakableKey
import optimus.graph.diagnostics.PerPropertyStats
import optimus.graph.diagnostics.PerTrackerStats
import optimus.graph.diagnostics.PerTrackerStatsKey
import optimus.graph.diagnostics.StatsSummary
import optimus.graph.diagnostics.TTrackStats
import optimus.graph.tracking.DependencyTracker
import optimus.graph.tracking.DependencyTrackerRoot
import optimus.graph.tracking.NoEventCause
import optimus.graph.tracking.TraversalIdSource

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.util.Try

/**
 * API to get stats about TTracks. Used to identify nodes that are never tweaked but have a big TTrack graph - this can
 * cause long GC cycles and use a lot of memory
 */
private[tracking] final object TTrackStatsSupport {
  // Collect stats for a given property with a ttrack map (all keys in the map will refer to the same property)
  // Note - this will return un-corrected impact scores of 0 (correcting them requires another traversal)
  private def collectPerPropertyStats(
      root: DependencyTrackerRoot,
      rootId: Int,
      propertyInfo: NodeTaskInfo,
      perPropertyTtracks: Map[TweakableKey, TTrackRoot],
      numRecordedInvalidations: Int): PerPropertyStats = {
    // Since we are reporting per property, we use a different cycle per property, not per ttrack
    val stats = ttrackGraphStats(perPropertyTtracks.values.asJava, root)

    PerPropertyStats(
      property = propertyInfo,
      numTrackedTweakables = perPropertyTtracks.size,
      numRecordedInvalidations = numRecordedInvalidations,
      numNodes = stats.numNodes,
      impact = 0,
      numInvalidatedNotCleared = stats.numInvalids,
      numInvalidatedAndCleared = stats.numCleared,
      numNonCacheable = stats.numNils,
      maxDepth = stats.maxDepth,
      maxBranching = stats.breadthStats.getMax.toInt
    )
  }

  private def collectPerTrackerStats(
      root: DependencyTrackerRoot,
      rootId: Int,
      tracker: DependencyTracker): PerTrackerStats = {
    val ttracks = tracker.tweakableTracker.ttracks.asScala.toMap

    // pt == perTracker
    val ptNumTweaks = tracker.scenarioStack.expandedTweaks.size
    val ptTrackedTweakables = ttracks.size
    val ptUTrackedTweakables = tracker.allUserNodeTrackers.map(_.utracks.size).sum

    val visitor = new TTrackStatsCollector
    visitor.visit(ttracks.values.asJava, root)

    val ptNumNodesInTrackingGraph = visitor.numNodes
    val ptNumResets = visitor.numInvalids

    PerTrackerStats(
      rootId,
      tracker.scenarioReference,
      ptNumTweaks,
      ptUTrackedTweakables,
      ptTrackedTweakables,
      ptNumNodesInTrackingGraph,
      ptNumResets)
  }

  private def correctForImpact(
      root: DependencyTrackerRoot,
      perPropertyStatsMap: Map[NodeTaskInfo, PerPropertyStats],
      perPropertyTtracksMap: Map[NodeTaskInfo, Iterable[Iterable[TTrackRoot]]]): Map[NodeTaskInfo, PerPropertyStats] = {

    val updatedPerPropertyStatsMap = mutable.Map[NodeTaskInfo, PerPropertyStats]()

    val (wasTweaked, notTweaked) = perPropertyStatsMap.partition { case (_, stats) =>
      stats.numRecordedInvalidations != 0
    }

    // Lowest impact properties are all the ones which ARE tweaked (because we may not be able to switch off tracking).
    // Traverse once to mark all nodes tracked for these properties as 'visited' (using a default visitor).
    root.withFreshTraversalId { startID =>
      val graphMarker = new MarkingGraphTraverser()
      for ((tweakedKey, tweakedStats) <- wasTweaked) {
        // put tweaked properties straight into the new map since their impact scores will remain at 0
        updatedPerPropertyStatsMap.put(tweakedKey, tweakedStats)
        val perPropertyTtracksList = perPropertyTtracksMap(tweakedKey)
        for (ttrs <- perPropertyTtracksList) {
          graphMarker.visit(ttrs.asJava, startID)
        }
      }

      // Sort by increasing tracking graph size so that we traverse as much of the graph as possible when we go through
      // the 'lower impact' properties (ie, those with smaller tracking graph sizes). We can then get an estimate
      // of the cumulative benefit of switching off tracking for a given property (assuming that all properties with
      // higher impact have already been untracked)
      val propertiesByIncreasingImpact =
        TTrackStats.propertiesSortedByTrackingGraphSize(notTweaked).reverse.map { _._1 }

      for (perPropertyStatsKey <- propertiesByIncreasingImpact) {
        val visitor = new GraphTTrackCountTraverser
        val perPropertyTtracksList = perPropertyTtracksMap(perPropertyStatsKey)
        for (ttrs <- perPropertyTtracksList) {
          visitor.visit(ttrs.asJava, startID)
        }
        val impact = visitor.count
        val updatedResult = perPropertyStatsMap(perPropertyStatsKey).copy(impact = impact)
        updatedPerPropertyStatsMap.put(perPropertyStatsKey, updatedResult)
      }
      startID
    }
    updatedPerPropertyStatsMap.toMap
  }

  private[tracking] def doGetTTrackStats(root: DependencyTrackerRoot, cleanup: Boolean = false): TTrackStats = {
    val allTrackersRootedHere = DependencyTrackerRoot.childListForRoot(root)
    val thisRootID = System.identityHashCode(root)
    val perPropertyStatsMap = mutable.Map[NodeTaskInfo, PerPropertyStats]()
    // (effectively just a Map[NTI, Iterable[TTrackRoot]] but there's no point spending time flattening the values
    val perPropertyTtracksMap = mutable.Map[NodeTaskInfo, List[Iterable[TTrackRoot]]]()

    val invalidationsByProperty: Map[NodeTaskInfo, Int] = {
      val invalidates = NodeTrace.getInvalidates.asScala
      invalidates.groupBy(_.property).map { case (prop, traces) => (prop, traces.size) }
    }

    if (cleanup) {
      root.doImmediateUninterruptableForcedCleanup()
    }

    val perTrackerStatsMap = allTrackersRootedHere.map { tracker =>
      val ttracks = tracker.tweakableTracker.ttracks.asScala.toMap

      val ttracksByProperty = ttracks.groupBy(_._1.propertyInfo)
      for ((propertyInfo, perPropertyTtracks: Map[TweakableKey, TTrackRoot]) <- ttracksByProperty) {
        val numInvalidations = invalidationsByProperty.getOrElse(propertyInfo, 0)
        val perPropertyStats =
          collectPerPropertyStats(root, thisRootID, propertyInfo, perPropertyTtracks, numInvalidations)

        if (perPropertyStatsMap.contains(propertyInfo)) {
          val existingStats = perPropertyStatsMap(propertyInfo)
          perPropertyStatsMap.put(propertyInfo, existingStats.merge(perPropertyStats))
        } else perPropertyStatsMap.put(propertyInfo, perPropertyStats)

        // If this property was already in the map from another tracker under the same root, add ttracks to the list
        // and walk all of them later to collect stats for this property
        val existingTtracks = perPropertyTtracksMap.getOrElse(propertyInfo, Nil)
        val updated = perPropertyTtracks.values :: existingTtracks
        perPropertyTtracksMap.put(propertyInfo, updated)
      }

      val perTrackerStats = collectPerTrackerStats(root, thisRootID, tracker)
      val key = PerTrackerStatsKey(thisRootID, tracker)
      key -> perTrackerStats
    }.toMap

    val correctedPerPropertyStatsMap = correctForImpact(root, perPropertyStatsMap.toMap, perPropertyTtracksMap.toMap)
    val untrackedProperties = NodeTrace.untrackedProperties.asScala.collect {
      case prop if !correctedPerPropertyStatsMap.contains(prop) =>
        prop -> PerPropertyStats.untracked(prop, numRecordedInvalidations = invalidationsByProperty.getOrElse(prop, 0))
    }

    TTrackStats(correctedPerPropertyStatsMap ++ untrackedProperties, perTrackerStatsMap)
  }

  private[graph] def ttrackGraphStats(roots: lang.Iterable[TTrackRoot], idSource: TraversalIdSource): StatsSummary = {
    val stats = new TTrackStatsCollector()
    stats.visit(roots, idSource)

    val maxDepth = new GraphMaxDepthTraverser().maxDepth(roots, idSource)

    StatsSummary(
      maxDepth = maxDepth,
      breadthStats = stats.branchingStats.getSnapshot,
      numNodes = stats.numNodes,
      nodeRefsPerTTrackStats = stats.nodeRefsPerTTrackStats.getSnapshot,
      numInvalids = stats.numInvalids,
      numNils = stats.numNils,
      numCleared = stats.numInvalidatedAndCleared,
      graphSize = stats.graphSize
    )
  }
}

trait TTrackStatsActions {
  self: DependencyTracker =>

  private[tracking] def getTTrackStats(cleanup: Boolean, callback: Try[TTrackStats] => Unit): Unit =
    // running this in a batch update (even though we don't use the BatchUpdater) so that we hold the locks
    executeBatchUpdateAsync[TTrackStats](
      NoEventCause,
      _ => TTrackStatsSupport.doGetTTrackStats(self.root, cleanup),
      callback)
}
