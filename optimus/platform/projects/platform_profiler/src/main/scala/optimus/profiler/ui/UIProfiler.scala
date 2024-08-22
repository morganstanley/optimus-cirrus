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

import optimus.graph.DiagnosticSettings
import optimus.graph.DiagnosticSettings.outOfProcess
import optimus.graph.JMXConnection
import optimus.graph.cache.CauseProfiler
import optimus.graph.diagnostics.PNodeInvalidate
import optimus.graph.diagnostics.PerPropertyStats
import optimus.graph.diagnostics.PerTrackerStats
import optimus.graph.tracking.DependencyTrackerRoot
import optimus.profiler.ProfilerUI
import optimus.profiler.ui.common.JPanel2
import optimus.profiler.ui.common.JTabbedPane2
import optimus.profiler.ui.common.JToolBar2
import optimus.profiler.ui.common.ProvidesMenus
import optimus.profiler.ui.controls.SingleImageButton

import java.awt.BorderLayout
import java.awt.Color
import java.util.prefs.Preferences
import javax.swing.JButton
import javax.swing.JTabbedPane
import scala.annotation.nowarn
import scala.collection.compat._
import scala.collection.mutable.ArrayBuffer

object UIProfiler {
  val pref: Preferences = Preferences.userNodeForPackage(this.getClass)
  private var panel: UIProfiler = _

  def instance: UIProfiler = {
    if (panel eq null) NodeUI.invoke {
      if (panel eq null) panel = new UIProfiler()
    }
    panel
  }

  def getInvalidatesList: ArrayBuffer[PNodeInvalidate] =
    if (outOfProcess) JMXConnection.graph.getInvalidates else ProfilerUI.getInvalidates

  private[profiler] def getTrackingInfo: (ArrayBuffer[PerPropertyStats], ArrayBuffer[PerTrackerStats]) = {
    val ttrackStats = DependencyTrackerRoot.ttrackStatsForAllRoots(false).values
    val perPropStats = ttrackStats.flatMap { _.perPropertyStats.values }.to(ArrayBuffer)
    val perTrackerStats = ttrackStats.flatMap { _.perTrackerStats.values }.to(ArrayBuffer)

    (perPropStats, perTrackerStats)
  }
}

class UIProfiler extends JPanel2(new BorderLayout()) with ProvidesMenus {
  private val tabs: JTabbedPane = new JTabbedPane2(pref)
  private val inv_table: InvalidatedNodeTable = new InvalidatedNodeTable
  private var tracker_table: PerTrackerStatsTable = _
  private var property_table: PerPropertyStatsTable = _
  private var refreshTrackingButton: JButton = _

  init()
  private def init(): Unit = {
    val toolBar = new JToolBar2()
    val enabled = !DiagnosticSettings.offlineReview
    val btnRefresh = SingleImageButton.createRefreshButton(enabled = enabled) { cmdRefresh() }

    toolBar.add(btnRefresh)
    toolBar.addSeparator()
    refreshTrackingButton = toolBar.addButton(
      "Regenerate Tracking",
      "Clear All Caches and TTrack information, used for testing switch off/on tracking",
      enabled) {
      cmdRefreshTTracks(true)
    }
    toolBar.addSeparator()
    tabs.add("Invalidates", inv_table)
    if (enabled) {
      tracker_table = new PerTrackerStatsTable()
      property_table = new PerPropertyStatsTable()
      tabs.add("Tracking Scenarios", tracker_table)
      tabs.add("Per-Property TTracks", property_table)
    }

    add(toolBar, BorderLayout.NORTH)
    add(tabs, BorderLayout.CENTER)
  }

  def cmdRefresh(): Unit = {
    inv_table.setList(UIProfiler.getInvalidatesList)
    if (!DiagnosticSettings.offlineReview)
      cmdRefreshTTracks(false)
  }

  def highlightRefreshTrackingButton(highlight: Boolean): Unit = {
    val color = if (highlight) Color.YELLOW else getBackground
    refreshTrackingButton.setBackground(color)
  }

  @nowarn("msg=10500 optimus.graph.tracking.DependencyTrackerRoot.clearCacheAndTracking")
  private def cmdRefreshTTracks(cleanup: Boolean): Unit = {
    if (cleanup) {
      highlightRefreshTrackingButton(false)
      if (outOfProcess) JMXConnection.graph.clearCacheAndTracking()
      else DependencyTrackerRoot.clearCacheAndTracking(CauseProfiler)
    }
    val (perPropStats, perTrackerStats) =
      if (outOfProcess) JMXConnection.graph.getTrackingScenarioInfo else UIProfiler.getTrackingInfo

    property_table.setList(perPropStats)
    tracker_table.setList(perTrackerStats)
  }
}
