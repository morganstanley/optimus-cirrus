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
package optimus.profiler.ui.browser

import optimus.graph.DiagnosticSettings
import optimus.graph.NodeTrace
import optimus.graph.OGTrace
import optimus.graph.diagnostics.PNodeTask
import optimus.platform.util.Log
import optimus.profiler.DebuggerUI
import optimus.profiler.NodeFormatUI
import optimus.profiler.ui.GraphDebuggerUI
import optimus.profiler.ui.Icons
import optimus.profiler.ui.NodeTreeTableForGroupedBrowsing
import optimus.profiler.ui.NodeTreeView
import optimus.profiler.ui.NodeTreeViewGroup
import optimus.profiler.ui.NodeView
import optimus.profiler.ui.common.FileChooser
import optimus.profiler.ui.common.GroupingAttribute
import optimus.profiler.ui.common.GroupingAttribute.GroupingAttribute
import optimus.profiler.ui.common.JPanel2
import optimus.profiler.ui.common.JSplitPane2
import optimus.profiler.ui.common.JToolBar2
import optimus.profiler.ui.common.JUIUtils
import optimus.profiler.ui.common.NodeGroupTotals
import optimus.profiler.ui.controls.GroupByButton
import optimus.profiler.ui.controls.NodeFilterComboBox
import optimus.profiler.ui.controls.SingleImageButton
import optimus.profiler.utils.GraphDumper

import java.awt.BorderLayout
import java.awt.BorderLayout._
import java.awt.Cursor
import java.awt.event.ActionEvent
import java.util.{ArrayList => JArrayList}
import java.util.{Collection => JCollection}
import javax.swing.JButton
import javax.swing.JOptionPane
import javax.swing.JSplitPane._
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

object GraphBrowser {
  private[profiler] val log = msjava.slf4jutils.scalalog.getLogger("DEBUGGER")
  private[profiler] def resetBtnAction(): Unit = OGTrace.reset()

  val SHOW_PROVIDED_NODES = 0
  val SHOW_NODES_FROM_NODE_TRACE = 1
  val SHOW_NODES_FROM_SELECTED_IN_UI = 2
  val SHOW_NODES_FROM_CHILDREN = 3
  val SHOW_NODES_FROM_PARENTS = 4
}

/**
 * Allows for general searching of traced nodes [filter part] [grouping button] [reset/dump buttons] [node tree browser]
 * [node view aka large html dump view]
 */
class GraphBrowser(_allTasks: JCollection[PNodeTask], val showNodesFrom: Int, _ignoreFilter: Boolean = false)
    extends JPanel2
    with Log {
  // Group side controls
  private val filteringAndGroupPref = prefOf(showNodesFrom.toString)
  private val groupingTreeDetails = new NodeView
  private val groupByButton = new GroupByButton(GroupingAttribute.values.map(_.toString).toSeq, filteringAndGroupPref)
  private val groupingPane = createGroupingPane()
  private var btnReset: JButton = _
  private val btnDumpTrace = new JButton(Icons.dump)
  private val filter = new NodeFilterComboBox(filteringAndGroupPref)

  /** Right-hand side child/parent view */
  private val nodeTrees = NodeTreeBrowser(showNodesFrom)

  def this(allTasks: JCollection[PNodeTask], ignoreFilter: Boolean) =
    this(allTasks, GraphBrowser.SHOW_PROVIDED_NODES, ignoreFilter)
  def this(allTasks: JCollection[PNodeTask]) = this(allTasks, GraphBrowser.SHOW_PROVIDED_NODES)
  def this(showNodesFrom: Int) = this(null, showNodesFrom)
  def this(showNodesFrom: Int, _ignoreFilter: Boolean) = this(null, showNodesFrom, _ignoreFilter)
  def this() = this(GraphBrowser.SHOW_NODES_FROM_NODE_TRACE)

  private[ui] def nodeTreeChangeFlagOfShowInternal(): Unit = nodeTrees.changeShowInternal()

  private[ui] def allTasks: JCollection[PNodeTask] = showNodesFrom match {
    case GraphBrowser.SHOW_PROVIDED_NODES            => _allTasks
    case GraphBrowser.SHOW_NODES_FROM_NODE_TRACE     => NodeTrace.getTrace
    case GraphBrowser.SHOW_NODES_FROM_SELECTED_IN_UI => new JArrayList(DebuggerUI.selectedNodes.asJava)
    case _                                           => new JArrayList[PNodeTask]()
  }

  private def allTasksFiltered(tasks: JCollection[PNodeTask], showInternal: Boolean): JCollection[PNodeTask] = {
    val filterText = filter.getCurrentFilter
    try {
      val allNodes = NodeFormatUI.filteredNodes(tasks, filterText, showInternal)
      allNodes
    } catch {
      case _: IllegalArgumentException =>
        JOptionPane.showMessageDialog(GraphBrowser.this, "Illegal filter parameter")
        new JArrayList[PNodeTask]()
    }
  }

  private def createGroupingPane(): NodeTreeTableForGroupedBrowsing = {
    val grouping = new NodeTreeTableForGroupedBrowsing(groupingTreeDetails)
    grouping.setBrowserFilterCallback(tasks => refreshGroups(tasks.asJavaCollection))
    grouping
  }

  /** Toolbar for left-hand side grouping/filtering pane */
  private def createGroupSideToolbar(): JToolBar2 = {
    val toolBar = new JToolBar2

    // Setup callbacks
    filter.onNodeSelectedByID = groupingPane.selectNode
    filter.onFilterUpdate = _ => cmdRefresh()

    groupByButton.onSelectionChanged = () => cmdRefresh()

    val btnRefresh = SingleImageButton.createRefreshButton("Refresh") {
      if (DiagnosticSettings.outOfProcess)
        GraphDebuggerUI.showMessage("Browser doesn't support out of process yet")
      else
        cmdRefresh()
    }

    btnReset = SingleImageButton.createResetButton("Reset all recorded data") {
      if (showNodesFrom == GraphBrowser.SHOW_NODES_FROM_SELECTED_IN_UI) {
        GraphDebuggerUI.clearSelectedNodes(switchToTab = false)
      } else {
        GraphBrowser.resetBtnAction()
      }
      cmdRefresh()
    }

    btnDumpTrace.setToolTipText("Dump Graph in json format")
    btnDumpTrace.addActionListener((_: ActionEvent) => dumpTrace())

    toolBar.add(btnRefresh)
    toolBar.addSeparator()
    toolBar.add(btnReset)
    toolBar.addSeparator()
    toolBar.add(filter)
    toolBar.addSeparator()
    toolBar.add(groupByButton)
    toolBar.addSeparator()
    toolBar.add(btnDumpTrace)
    toolBar
  }

  /** Left-hand side grouping/filtering pane */
  private def createGroupPanel(): JPanel2 = {
    val splitPaneFiltered = new JSplitPane2(pref, "groupedNodes")
    splitPaneFiltered.setOrientation(VERTICAL_SPLIT)
    splitPaneFiltered.setTopComponent(groupingPane)
    splitPaneFiltered.setBottomComponent(groupingTreeDetails)

    val panel = new JPanel2(new BorderLayout())
    panel.add(createGroupSideToolbar(), BorderLayout.NORTH)
    panel.add(splitPaneFiltered, BorderLayout.CENTER)
    panel
  }

  init()

  private def init(): Unit = {
    groupingPane.dataTable.getSelectionModel.addListSelectionListener(e =>
      if (!e.getValueIsAdjusting) {
        val selections = groupingPane.getNodeTaskSelections
        if (selections.nonEmpty)
          nodeTrees.showNodes(selections)
      })

    nodeTrees.addInternalButtonListener(b => cmdRefresh(showInternal = b))

    val splitPaneAuxiliary = new JSplitPane2(pref, "browse_callees")
    splitPaneAuxiliary.setLeftComponent(createGroupPanel())
    splitPaneAuxiliary.setRightComponent(nodeTrees)

    val ntree = nodeTrees.ntree
    val ntMenu = ntree.menu
    val mi = ntMenu.addMenu("Select in Browser", "Select node and make this root") {
      filter.setSelectedItem("")
      cmdRefresh()
      groupingPane.selectNode(ntree.getSelection.task.id)
    }
    ntMenu.addOnPopup { mi.setVisible(ntree.getSelection != null) }

    splitPaneAuxiliary.setResizeWeight(1.0)

    setLayout(new BorderLayout(40, 0))
    add(splitPaneAuxiliary, CENTER)
    add(NodeView.createDetailsToolbar(pref, groupingTreeDetails, nodeTrees.nview), SOUTH)
    cmdRefresh(ignoreFilter = _ignoreFilter)
  }

  // dump node trace to file for being loaded by graph comparator later.
  protected def dumpTrace(): Unit = {
    JUIUtils.saveToFile(this, FileChooser.nodeDumpChooser) { file =>
      GraphDumper.dump(GraphBrowser.this, PNodeTask.toLive(allTasks), file)
    }
  }

  private[profiler] def dumpGraphStarted(): Unit = {
    GraphDebuggerUI.showMessage("Dumping graph, please wait...")
    setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR))
    btnReset.setEnabled(false)
    btnDumpTrace.setEnabled(false)
  }

  private[profiler] def dumpGraphSucceed(): Unit = {
    setCursor(Cursor.getDefaultCursor)
    btnReset.setEnabled(true)
    btnDumpTrace.setEnabled(true)
    GraphDebuggerUI.showMessage("Dump succeeded")
  }

  private[profiler] def dumpGraphFailed(error: String): Unit = {
    setCursor(Cursor.getDefaultCursor)
    btnReset.setEnabled(true)
    btnDumpTrace.setEnabled(true)
    GraphDebuggerUI.showMessage(s"Dump failed, please try again. Reason: $error")
  }

  def showMessage(msg: String): Unit = {
    GraphDebuggerUI.showMessage(msg)
  }

  /** ignoreFilter when a user specifically asks to see nodes of a given type in browser (or from cache) */
  def cmdRefresh(
      tasks: JCollection[PNodeTask] = allTasks,
      ignoreFilter: Boolean = false,
      showInternal: Boolean = GraphDebuggerUI.showInternal.get): Unit = {
    val doFilter = !ignoreFilter || showInternal
    val filteredTasks = if (doFilter) allTasksFiltered(tasks, showInternal) else tasks
    val tableFilteredTasks = groupingPane.filter(filteredTasks)
    val groupingPaneAlreadyWarnedAboutFilter = tableFilteredTasks.isEmpty && groupingPane.currentViewHasFilter

    // either the top filter bar or a column in the table had a filter
    val wasFiltered = !ignoreFilter && filter.getCurrentFilter.nonEmpty
    if (wasFiltered && tableFilteredTasks.isEmpty)
      if (!groupingPaneAlreadyWarnedAboutFilter) groupingPane.showMessage("No nodes found! Try removing your filter(s)")
      else {
        val tableFilter = groupingPane.getTableColumns.filter(c => (c.filter ne null) && c.filter.nonEmpty)
        groupingPane.showMessage(
          s"Did you forget to remove the table filter(s): ${tableFilter.map(_.filter).mkString(", ")} from the yellow row?")
      }
    refreshGroups(tableFilteredTasks)
  }

  def refreshGroups(filteredTasks: JCollection[PNodeTask]): Unit =
    groupingPane.setList(groupBy(filteredTasks))

  def findColumnIDByName(name: String): Int =
    groupingPane.getTableColumns.indexWhere(_.prototype.name == name)

  private def groupBy(nodes: JCollection[PNodeTask]): List[NodeTreeView] = {
    val protoTypes = groupByButton.selectedAttributes.toList.map {
      GroupingAttribute.withName(_).asInstanceOf[GroupingAttribute].groupTemplate
    }
    val root = new NodeGroupTotals
    val it = nodes.iterator
    while (it.hasNext) {
      root.insert(it.next(), protoTypes)
    }
    root.trimSimple()
    val rootView = new NodeTreeViewGroup(root, 0)
    rootView.open = true
    List(rootView)
  }

  /** For testing purpose only */
  private[optimus] def setFilterByColumnName(columnName: String, filterStr: String): Unit =
    groupingPane.viewColumns.filter(_.prototype.name == columnName)(0).filter = filterStr

  /** For testing purpose only */
  private[optimus] def filterByColumnNameWithTreeStructure(columnName: String, filterStr: String): Unit = {
    val columnIndex = groupingPane.viewColumns.filter(_.prototype.name == columnName)(0).index
    groupingPane.headerTable.setValueAt(filterStr, 0, columnIndex)
  }

  /** For testing purpose only */
  private[optimus] def setFilterInComboBox(filterStr: String): Unit = filter.setSelectedItem(filterStr)

  /** For testing purpose only */
  private[optimus] def sortRowsBy(columnName: String): Unit =
    groupingPane.setListBySorting(groupingPane.roots, increasingOrder = true, findColumnIDByName(columnName))

  /** For testing purpose only */
  private[optimus] def getRows: ArrayBuffer[NodeTreeView] = groupingPane.getRowsForTesting

  /** For testing purpose only */
  private[optimus] def getColumnValueForRow[T](columnName: String, row: NodeTreeView): T = {
    val dataTable = groupingPane.dataTable
    dataTable.getTCol(findColumnIDByName(columnName)).valueOf(row).asInstanceOf[T]
  }

  /** For testing purpose only */
  private[optimus] def groupBy(attributeName: GroupingAttribute): Unit = {
    groupByButton.selectedAttributes += attributeName.groupTemplate.name
    cmdRefresh()
  }
}
