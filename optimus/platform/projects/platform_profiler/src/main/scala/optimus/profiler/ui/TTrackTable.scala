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

import optimus.graph.diagnostics.HasTweakablePropertyInfo
import optimus.graph.diagnostics.PerPropertyStats
import optimus.graph.diagnostics.PerTrackerStats
import optimus.profiler.ui.common.JPopupMenu2

import scala.collection.mutable.ArrayBuffer

object PerTrackerStatsTable {
  val regularView = ArrayBuffer(
    new TableColumn[PerTrackerStats]("Root ID", 100) {
      override def valueOf(row: PerTrackerStats) = row.rootID
    },
    new TableColumnString[PerTrackerStats]("ScenarioReference", 200) {
      override def valueOf(row: PerTrackerStats) = row.scenarioReference.toString
    },
    new TableColumnCount[PerTrackerStats]("Tweaks", 200) {
      override def valueOf(row: PerTrackerStats) = row.numTweaks
      override def toolTip = "Number of tweaks currently applied in tracking scenario"
    },
    new TableColumnCount[PerTrackerStats]("Observed Nodes", 200) {
      override def valueOf(row: PerTrackerStats) = row.numObservedNodes
      override def toolTip = "Number of user-tracked nodes (UTracks)"
    },
    new TableColumnCount[PerTrackerStats]("Tracked Tweakables", 200) {
      override def valueOf(row: PerTrackerStats) = row.numTrackedTweakables
      override def toolTip = "Number of tracked tweakable nodes (TTrackRoots)"
    },
    new TableColumnCount[PerTrackerStats]("Tracking Graph Size (number of nodes)", 200) {
      override def valueOf(row: PerTrackerStats) = row.numNodesInTrackingGraph
      override def toolTip =
        "<html>Number of nodes in tracking graph <br><br>" +
          "<i>The total sum will be much smaller than the sum of the per-property numbers <br>" +
          "because the per-property Ttrack graphs may intersect and result in double-counted nodes</i></html>"
    }
  )
}

class PerTrackerStatsTable extends NPTable[PerTrackerStats] with Filterable[PerTrackerStats] {
  override def initialColumns: ArrayBuffer[TableColumn[PerTrackerStats]] = PerTrackerStatsTable.regularView
}

object PerPropertyStatsTable {
  val regularView = ArrayBuffer(
    new TableColumnString[PerPropertyStats]("Property", 500) {
      override def valueOf(row: PerPropertyStats) = row.name
      override def toolTip = "Package, class and field name of the tweakable property"
    },
    new TableColumnCount[PerPropertyStats]("Recorded Invalidations", 150) {
      override def valueOf(row: PerPropertyStats) = row.numRecordedInvalidations
      override def toolTip =
        "Number of recorded invalidations of this property (due to tweaks) since last Reset. Need to enable Record Invalidates."
    },
    new TableColumn[PerPropertyStats]("Tracking", 150) {
      override def valueOf(row: PerPropertyStats) = row.isTrackedForInvalidation
      override def toolTip =
        "Is this property configured to be tracked for invalidation? If not any invalidates will cause cache to be dropped"
    },
    new TableColumnCount[PerPropertyStats]("Tracked Tweakables", 200) {
      override def valueOf(row: PerPropertyStats) = row.numTrackedTweakables
      override def toolTip = "Number of tweakable nodes of this property which are currently tracked"
    },
    new TableColumn[PerPropertyStats]("Instance Tweaked (in tracking scenario)", 150) {
      override def valueOf(row: PerPropertyStats) = row.wasInstanceTweaked
    },
    new TableColumn[PerPropertyStats]("Property Tweaked (in tracking scenario)", 150) {
      override def valueOf(row: PerPropertyStats) = row.wasPropertyTweaked
    },
    new TableColumn[PerPropertyStats]("Instance Tweaked (anywhere)", 150) {
      override def valueOf(row: PerPropertyStats) = row.wasEverInstanceTweaked
    },
    new TableColumn[PerPropertyStats]("Property Tweaked (anywhere)", 150) {
      override def valueOf(row: PerPropertyStats) = row.wasEverPropertyTweaked
    },
    new TableColumnCount[PerPropertyStats]("Shared Tracking Graph Size (number of nodes)", 200) {
      override def valueOf(row: PerPropertyStats) = row.numNodes
      override def toolTip = "Number of nodes in tracking graph reachable from this property"
      override def sortByThisColumn: Boolean = true
    },
    new TableColumnCount[PerPropertyStats]("Individual Tracking Graph Size (number of nodes)", 200) {
      override def valueOf(row: PerPropertyStats) = row.impact
      override def toolTip = "Impact of switching off tracking for this node on number of nodes in tracking graph"
    }
  )

  val extendedView = ArrayBuffer(
    new TableColumnCount[PerPropertyStats]("Depth", 200) {
      override def valueOf(row: PerPropertyStats) = row.maxDepth
      override def toolTip = "Maximum depth (size of parent dependencies)"
    },
    new TableColumnCount[PerPropertyStats]("Breadth", 200) {
      override def valueOf(row: PerPropertyStats) = row.maxBranching
      override def toolTip = "Maximum branching, or breadth, from all tweaked nodes"
    },
    new TableColumnCount[PerPropertyStats]("Invalidated Nodes (to clear)", 200) {
      override def valueOf(row: PerPropertyStats) = row.numInvalidatedNotCleared
      override def toolTip =
        "Number of nodes that have been invalidated and can be garbage-collected (TTrackRef.Invalid)"
    },
    new TableColumnCount[PerPropertyStats]("Invalidated Nodes (cleared)", 200) {
      override def valueOf(row: PerPropertyStats) = row.numInvalidatedAndCleared
      override def toolTip =
        "Number of nodes that have been invalidated and garbage-collected (WeakReference to NodeTask is null)"
    },
    new TableColumnCount[PerPropertyStats]("Non-Cacheable Nodes", 200) {
      override def valueOf(row: PerPropertyStats) = row.numNonCacheable
      override def toolTip = "Number of non-cacheable nodes (TTrackRef.Nil)"
    }
  )
}

class PerPropertyStatsTable extends NPTable[PerPropertyStats] with Filterable[PerPropertyStats] {
  dataTable.setComponentPopupMenu {
    val menu = new JPopupMenu2
    TrackingMenu.addItems(menu, this)
    menu
  }
  override def initialColumns: ArrayBuffer[TableColumn[PerPropertyStats]] = PerPropertyStatsTable.regularView
}

object TrackingMenu {
  def addItems(menu: JPopupMenu2, table: NPTable[_ <: HasTweakablePropertyInfo]): Unit = {
    def cmdEnableTracking(enable: Boolean): Unit = {
      {
        table.getSelections.foreach { p =>
          if (enable) p.property.markTrackedForInvalidation()
          else p.property.markNotTrackedForInvalidation()
        }
        // user needs to refresh tracking information or application state can be incorrect (due to missing tracking
        // information for previously untracked nodes)
        UIProfiler.instance.highlightRefreshTrackingButton(true)
        table.dataTable.model.fireTableDataChanged()
      }
    }

    menu.addMenu("Enable tracking", cmdEnableTracking(true))
    menu.addMenu("Disable tracking", cmdEnableTracking(false))
  }
}
