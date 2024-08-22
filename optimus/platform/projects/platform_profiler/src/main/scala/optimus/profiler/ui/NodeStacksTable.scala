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
import javax.swing.table.TableCellRenderer
import optimus.graph.OGTraceReader
import optimus.graph.diagnostics.NPTreeNode
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.profiler.extensions.NPTreeNodeExt
import optimus.profiler.recipes.NodeStacks
import optimus.profiler.recipes.PNodeTaskInfoGrp
import optimus.profiler.ui.common.JPopupMenu2

import java.awt.Component
import scala.collection.mutable.ArrayBuffer

object NodeStacksTable {
  type NSTColumns = ArrayBuffer[TableColumn[PNodeTaskInfoGrp]]

  val minView: NSTColumns = ArrayBuffer(
    new TableColumn[PNodeTaskInfoGrp]("Name", 400) {
      override def valueOf(row: PNodeTaskInfoGrp): Any = row
      override def getCellRenderer: TableCellRenderer = NPTableRenderer.treeRenderer
    },
    new TableColumnLong[PNodeTaskInfoGrp]("Total Starts", 80) {
      override def valueOf(row: PNodeTaskInfoGrp): Long = row.pnti.start
    },
    new TableColumnLong[PNodeTaskInfoGrp]("Total Cache Accesses", 80) {
      override def valueOf(row: PNodeTaskInfoGrp): Long = row.pnti.cacheAccesses
    },
    new TableColumnTime[PNodeTaskInfoGrp]("Total Cache Time", 80) {
      override def valueOf(row: PNodeTaskInfoGrp): Double = row.pnti.cacheTime * 1e-6
    }
  )

  private val compareView: NSTColumns = ArrayBuffer(
    new TableColumnLong[PNodeTaskInfoGrp]("Base Edges", 80) {
      override def toolTip: String = "Count of edges (parent to child or vice versa) into this task in base run"
      override def valueOf(row: PNodeTaskInfoGrp): Long = row.count
    },
    new TableColumnTime[PNodeTaskInfoGrp]("Base Self Time (ms)", 80) {
      override def valueOf(row: PNodeTaskInfoGrp): Double = row.selfTime * 1e-6
    },
    new TableColumnLong[PNodeTaskInfoGrp]("Compare Edges", 80) {
      override def toolTip: String = "Count of edges (parent to child or vice versa) into this task in comparison run"
      override def valueOf(row: PNodeTaskInfoGrp): Long = row.cmpGrp.count
    },
    new TableColumnLong[PNodeTaskInfoGrp]("Compare Total Starts", 80) {
      override def valueOf(row: PNodeTaskInfoGrp): Long = row.cmpGrp.pnti.start
    },
    new TableColumnLong[PNodeTaskInfoGrp]("Compare Total Cache Accesses", 80) {
      override def toolTip: String = "Total requests to cache (hits + misses) in comparison run"
      override def valueOf(row: PNodeTaskInfoGrp): Long = row.cmpGrp.pnti.cacheAccesses
    },
    new TableColumnTime[PNodeTaskInfoGrp]("Compare Total Cache Time", 80) {
      override def valueOf(row: PNodeTaskInfoGrp): Double = row.cmpGrp.pnti.cacheTime * 1e-6
    }
  )

  val fullView: NSTColumns = minView ++ compareView
}

class NodeStacksTable(reader: OGTraceReader, override protected val sortable: Boolean = false)
    extends NPTreeTable[PNodeTaskInfoGrp] {
  emptyRow = new PNodeTaskInfoGrp(new PNodeTaskInfo(-1), 0)
  setView(NodeStacksTable.fullView)
  protected val menu: JPopupMenu2 = createMenu()

  var autoRefresh: () => Unit = _

  def openTo(pnti: PNodeTaskInfo): Unit = openTo(pnti.id)

  def openTo(profileID: Int): Unit = {
    if (autoRefresh != null) {
      autoRefresh()
      autoRefresh = null
    }

    super.openTo(_.pnti.id == profileID)
    val sm = dataTable.getSelectionModel
    sm.setValueIsAdjusting(true)
    sm.clearSelection()
    var i = 0
    while (i < rows.length) {
      val pntig = rows(i)
      if (pntig.pnti.id == profileID)
        sm.addSelectionInterval(i, i)
      i += 1
    }
    sm.setValueIsAdjusting(false)
  }

  dataTable.setComponentPopupMenu(menu)

  def selectStackOf(pnti: PNodeTaskInfo, toCallee: Boolean, skipCacheable: Boolean): Unit = {
    val topGrp = NodeStacks.getAsStack(reader, pnti, toCallee, skipCacheable).trimAndSortByTitle()
    topGrp.open = true
    setList(List(topGrp))
  }

  private def createMenu(): JPopupMenu2 = {
    val menu = new JPopupMenu2
    menu.addMenu("Parent Stacks Of Selected Type") {
      val s = getSelection
      if (s ne null) selectStackOf(s.pnti, NodeStacks.CALLER, skipCacheable = false)
    }
    menu.addMenu("Parent Stacks Of Selected Type (Skip non-cacheable)") {
      val s = getSelection
      if (s ne null) selectStackOf(s.pnti, NodeStacks.CALLER, skipCacheable = true)
    }
    menu.addSeparator()
    menu.addMenu("Child Stacks Of Selected Type") {
      val s = getSelection
      if (s ne null) selectStackOf(s.pnti, NodeStacks.CALLEE, skipCacheable = false)
    }
    menu.addMenu("Child Stacks Of Selected Type (Skip non-cacheable)") {
      val s = getSelection
      if (s ne null) selectStackOf(s.pnti, NodeStacks.CALLEE, skipCacheable = true)
    }
    menu
  }
}

/** wrapper around either NodeStacksTableForTimeLineView or NodeTreeTableForTimeLineView */
private final class NodesInFlightForTimeLineView(initial: NPTable[_]) {
  private var visible: NPTable[_] = initial
  def setVisible(table: NPTable[_]): Unit = visible = table
  def getVisible: NPTable[_] = visible
}

/** Customizations for TimeLine */
class NodeStacksTableForTimeLineView(reader: OGTraceReader) extends NodeStacksTable(reader, sortable = false)

/** Customization for Compare Views */
class NodeStacksTableForComparison(reader: OGTraceReader, cmpToReader: OGTraceReader)
    extends NodeStacksTable(reader, sortable = false) {
  private val smallDiff = 0.1

  def selectParentNodeStackOf(grp: PNodeTaskInfoGrp, skipCacheable: Boolean): Unit = {
    val topGrpUnsorted = NodeStacks.getParentsAsStack(reader, grp.pnti, cmpToReader, grp.cmpGrp.pnti, skipCacheable)
    val topGrp = topGrpUnsorted.trimAndSortByDiffInEdgeCount()
    NPTreeNodeExt.openToFirst(topGrp, { g: PNodeTaskInfoGrp => g.diffInCountIsSmall(smallDiff) })
    setList(List(topGrp))
  }

  menu.addSeparator()
  menu.addMenu("Parent Stacks Of Selected Type with Compare") {
    val s = getSelection
    if (s ne null) selectParentNodeStackOf(s, skipCacheable = false)
  }

  menu.addMenu("Parent Stacks Of Selected Type (Skip non-cacheable) with Compare") {
    val s = getSelection
    if (s ne null) selectParentNodeStackOf(s, skipCacheable = true)
  }

  override def getRowBackground(col: TableColumn[PNodeTaskInfoGrp], row: PNodeTaskInfoGrp): Color =
    NPTreeNodeExt.getRowBackground(col, row)
}
