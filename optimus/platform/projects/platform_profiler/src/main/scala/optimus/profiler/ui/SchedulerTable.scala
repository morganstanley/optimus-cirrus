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
import optimus.graph.OGSchedulerTimes.StallDetailedTime
import optimus.graph.diagnostics.NPTreeNode
import optimus.graph.diagnostics.SchedulerProfileEntry
import optimus.graph.diagnostics.SchedulerProfileEntryForUI
import optimus.graph.diagnostics.gridprofiler.ColGroup
import optimus.graph.diagnostics.gridprofiler.SummaryTable
import optimus.profiler.ui.NPTableRenderer.ValueWithTooltip
import optimus.profiler.ui.SchedulerTable.SchedulerRowT
import optimus.profiler.ui.common.JPanel2
import optimus.profiler.ui.common.JSplitPane2
import optimus.profiler.ui.common.JTabbedPane2
import optimus.profiler.ui.common.JToolBar2
import optimus.profiler.ui.controls.SingleImageButton

import java.awt.BorderLayout
import java.awt.Color
import java.awt.Font
import javax.swing.table.TableCellRenderer
import scala.collection.mutable.ArrayBuffer

object SchedulerTable {
  type SchedulerRowT = (String, SchedulerProfileEntryForUI)

  private val regularView = ArrayBuffer(
    new TableColumnString[SchedulerRowT]("Thread") {
      override def valueOf(row: (String, SchedulerProfileEntryForUI)): String = row._1
    },
    new TableColumnTime[SchedulerRowT]("User Graph Time (ms)", 40) {
      override def valueOf(row: (String, SchedulerProfileEntryForUI)): Double = row._2.prf.userGraphTime / 1e6
    },
    new TableColumnTime[SchedulerRowT]("Self Time (ms)", 40) {
      override def valueOf(row: (String, SchedulerProfileEntryForUI)): Double = row._2.prf.selfTime / 1e6
    },
    new TableColumnTime[SchedulerRowT]("CPU Time (ms)", 40) {
      override def valueOf(row: (String, SchedulerProfileEntryForUI)): Double = row._2.prf.cpuTime / 1e6
    },
    new TableColumnTime[SchedulerRowT]("Cache Time (ms)", 40) {
      override def valueOf(row: (String, SchedulerProfileEntryForUI)): Double = row._2.prf.cacheTime / 1e6
    },
    new TableColumnTime[SchedulerRowT]("Wait Time (ms)", 40) {
      override def valueOf(row: (String, SchedulerProfileEntryForUI)): Double = row._2.prf.waitTime / 1e6
    },
    new TableColumnTime[SchedulerRowT]("Spin Time (ms)", 40) {
      override def valueOf(row: (String, SchedulerProfileEntryForUI)): Double = row._2.prf.spinTime / 1e6
    },
    new TableColumnTime[SchedulerRowT]("GC Time (ms)", 40) {
      override def valueOf(row: (String, SchedulerProfileEntryForUI)): Double = row._2.gcTime / 1e6
    },
    new TableColumnTime[SchedulerRowT]("Contended Monitor Time (ms)", 40) {
      override def valueOf(row: (String, SchedulerProfileEntryForUI)): Double = row._2.cMonitorTime / 1e6
    }
  )
}

class SchedulerTable extends NPTable[SchedulerRowT] with Filterable[SchedulerRowT] {
  emptyRow = ("", SchedulerProfileEntryForUI(SchedulerProfileEntry(), 0, 0))
  override def initialColumns: ArrayBuffer[TableColumn[SchedulerRowT]] = SchedulerTable.regularView

}

object StallTable {
  private val regularView = ArrayBuffer(
    new TableColumnString[StallDetailedTime]("Type") {
      override def valueOf(row: StallDetailedTime): String = row.pluginType.name
    },
    new TableColumnString[StallDetailedTime]("Sync Stack", width = 30) {
      override def valueOf(row: StallDetailedTime): String = if (row.syncStackPresent) "Y" else ""
    },
    new TableColumnTime[StallDetailedTime]("Stall Time (ms)", width = 40) {
      override def valueOf(row: StallDetailedTime): Double = row.time * 1e-6
    }
  )
}

class StallTable extends NPTable[StallDetailedTime] with Filterable[StallDetailedTime] {
  emptyRow = new StallDetailedTime(null, false, 0)
  override def initialColumns: ArrayBuffer[TableColumn[StallDetailedTime]] = StallTable.regularView
}

final class SummaryTableView extends NPTreeTable[NPTreeNode] {
  def setSummaryTable(table: SummaryTable): Unit = {
    val groups = SummaryTable.allGroups(table)
    setList(groups.map(g => SummaryGroupTreeNode(g.description, g.children, table)))
  }

  private val regularView = ArrayBuffer[TableColumn[NPTreeNode]](
    new TableColumnWithHighlight("Name", 200) {
      override def valueOf(row: NPTreeNode): NPTreeNode = row
      override def getCellRenderer: TableCellRenderer = NPTableRenderer.treeRenderer
    },
    new TableColumnWithHighlight("Value", 100) {
      override def valueOf(row: NPTreeNode): ValueWithTooltip[String] = row match {
        case s: SummaryTreeNode => ValueWithTooltip(s.valueString, s.tooltipText)
        case _                  => ValueWithTooltip("", null)
      }
      override def getCellRenderer: TableCellRenderer = NPTableRenderer.countRender

    }
  )

  class TableColumnWithHighlight(n: String, w: Int) extends TableColumn[NPTreeNode](n, w) {
    override def valueOf(row: NPTreeNode): Any = null
    override def getCellCustomColor(selected: Boolean, row: NPTreeNode, row2: NPTreeNode): Color = row match {
      case s: SummaryTreeNode if s.highlight => Color.pink
      case _                                 => null
    }
  }

  setView(regularView)
}

final case class SummaryGroupTreeNode(name: String, colGrps: Iterable[ColGroup], table: SummaryTable)
    extends NPTreeNode {
  open = true
  override def level: Int = 0
  override def hasChildren: Boolean = true
  override val getChildren: Iterable[NPTreeNode] = colGrps.map(SummaryTreeNode(_, table: SummaryTable, 1))
  override def title: String = name
  override def font: Font = NPTableRenderer.subHeaderFont
}

final case class SummaryTreeNode(colGrp: ColGroup, table: SummaryTable, override val level: Int) extends NPTreeNode {
  open = true
  override val hasChildren: Boolean = colGrp.children.nonEmpty
  override val getChildren: Iterable[NPTreeNode] = colGrp.children.map(SummaryTreeNode(_, table, level + 1))
  override def title: String = colGrp.col.name
  override def tooltipText: String = colGrp.col.description
  def valueString: String = colGrp.col.format.format(colGrp.col.value(table))
  def highlight: Boolean = colGrp.col.highlight(table)
}

class SchedulerView extends JPanel2(new BorderLayout()) {
  private val tabPanel = new JTabbedPane2(pref)
  private val splitter = new JSplitPane2(pref)
  private val summaryTable = new SummaryTableView
  private val schedulerTable = new SchedulerTable
  private val stallTable = new StallTable

  init()
  private def init(): Unit = {
    val toolBar = new JToolBar2()
    val btnRefresh = SingleImageButton.createRefreshButton(enabled = !DiagnosticSettings.offlineReview) { cmdRefresh() }
    toolBar.add(btnRefresh)
    toolBar.addSeparator()

    splitter.setLeftComponent(schedulerTable)
    splitter.setRightComponent(stallTable)

    tabPanel.addTab("Summary", summaryTable)
    tabPanel.addTab("Threads / Stalls", splitter)

    add(toolBar, BorderLayout.NORTH)
    add(tabPanel, BorderLayout.CENTER)
  }

  def selectThreadsStallsPanel(): Unit = tabPanel.setSelectedComponent(splitter)

  def cmdRefresh(): Unit = {
    // note: currently does not include dist as GridProfiler.getPerSchedulerProfiles but would be nice
    val (scheduler, stall, summary) = NodeProfiler.getSchedulerTab
    schedulerTable.setList(scheduler)
    stallTable.setList(stall)
    summaryTable.setSummaryTable(summary)
  }
}
