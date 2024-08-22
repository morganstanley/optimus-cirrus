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

import optimus.graph.NodeTask
import optimus.graph.NodeTrace
import optimus.graph.diagnostics.PNodeTask
import optimus.profiler.ui.GraphDebuggerUI
import optimus.profiler.ui.NodeTreeTable
import optimus.profiler.ui.NodeTreeTable.ExpandConfig
import optimus.profiler.ui.NodeTreeTable.exceptionFilter
import optimus.profiler.ui.NodeView
import optimus.profiler.ui.browser.GraphBrowser._
import optimus.profiler.ui.common.JPanel2
import optimus.profiler.ui.common.JSplitPane2
import optimus.profiler.ui.common.JToolBar2
import optimus.profiler.ui.controls.NodeTreeFilterComboBox
import optimus.profiler.ui.controls.ProfilerTasks
import optimus.profiler.ui.controls.TwoImageButton

import java.awt.BorderLayout
import java.awt.Dimension
import java.awt.event.ActionEvent
import java.util.prefs.Preferences
import javax.swing.JButton
import javax.swing.JSplitPane
import scala.collection.mutable.ArrayBuffer

object NodeTreeBrowser {

  /** Create an independent (its own view toolbar) NodeTreeBrowser */
  def apply(root: NodeTask): NodeTreeBrowser = {
    apply(NodeTrace.accessProfile(root), showChildren = true)
  }

  /** Create an independent (its own view toolbar) NodeTreeBrowser */
  def apply(root: PNodeTask, showChildren: Boolean = true): NodeTreeBrowser = {
    val cfg = ExpandConfig(skipInternal = true, if (showChildren) NodeTreeTable.children else NodeTreeTable.parents)
    val showNodesFrom = if (showChildren) SHOW_NODES_FROM_CHILDREN else SHOW_NODES_FROM_PARENTS
    new NodeTreeBrowser(root, cfg, splitVertical = false, viewWithToolbar = true, showNodesFrom = showNodesFrom)
  }

  /** Create a part for the larger UI */
  def apply(showNodesFrom: Int): NodeTreeBrowser = {
    val cfg = ExpandConfig(skipInternal = true, NodeTreeTable.children)
    new NodeTreeBrowser(null, cfg, splitVertical = true, viewWithToolbar = false, showNodesFrom)
  }
}

/**
 * A panel that contains hierarchical node tree and the general node text viewer
 * i.e. 2 panels linked NodeTreeTable and NodeView
 */
final class NodeTreeBrowser(
    root: PNodeTask,
    expandConfig: ExpandConfig,
    splitVertical: Boolean,
    viewWithToolbar: Boolean,
    showNodesFrom: Int)
    extends JPanel2 {

  // don't share prefs between main browser and standalone browsers
  override protected[ui] val pref: Preferences = prefOf(showNodesFrom.toString)

  private val btnSFilterOutInternal = new JButton()
  private val btnExpandCollapse = TwoImageButton.createExpandCollapse(pref, "expandCollapse")

  private[browser] def changeShowInternal(): Unit = btnSFilterOutInternal.doClick()

  private var _internalButtonListener: Boolean => Unit = _
  private[ui] def addInternalButtonListener(f: Boolean => Unit): Unit = _internalButtonListener = f

  val nview = new NodeView(viewWithToolbar, pref)
  val ntree = new NodeTreeTable(nview, expandConfig) // Child/Parent view

  init(splitVertical)
  showNode(root)

  def reset(): Unit = {
    ntree.reset()
    nview.showNode(null)
  }

  def showNode(ntsk: PNodeTask): Unit = {
    ntree.showNode(ntsk)
    nview.showNode(null) // Don't show details by default
  }

  def showNodes(ntsks: ArrayBuffer[PNodeTask]): Unit = {
    ntree.showNodes(ntsks, expand = btnExpandCollapse.isChecked)
    nview.showNode(null) // Don't show details by default
  }

  private def init(splitVertical: Boolean): Unit = {
    setLayout(new BorderLayout(0, 0))

    val splitPane = new JSplitPane2(pref)

    splitPane.setOrientation(if (splitVertical) JSplitPane.VERTICAL_SPLIT else JSplitPane.HORIZONTAL_SPLIT)
    splitPane.setTopComponent(ntree)
    splitPane.setBottomComponent(nview)

    add(createTreeToolbar(), BorderLayout.NORTH)
    add(splitPane, BorderLayout.CENTER)
  }

  private def createTreeToolbar(): JToolBar2 = {
    val toolBar = new JToolBar2()

    /** default to children view */
    val profilerTasks = new ProfilerTasks(pref)
    // respect selected mode over previously selected mode from preferences if we're explicitly showing children/parents
    if (showNodesFrom == SHOW_NODES_FROM_CHILDREN || showNodesFrom == SHOW_NODES_FROM_PARENTS)
      profilerTasks.setSelectedMode(expandConfig.mode)
    profilerTasks.onSelectionChanged = mode => ntree.switchMode(mode)
    ntree.switchMode(profilerTasks.getSelectedMode)
    toolBar.add(profilerTasks)

    toolBar.addSeparator()
    btnSFilterOutInternal.addActionListener { _ =>
      val selected = GraphDebuggerUI.showInternal.get
      ntree.showInternal(selected, expand = btnExpandCollapse.isChecked)
      if (_internalButtonListener ne null) {
        _internalButtonListener(selected)
      }
    }
    btnExpandCollapse.addActionListener { _ =>
      ntree.showNodes(expand = btnExpandCollapse.isChecked)
    }

    toolBar.add(btnExpandCollapse)

    val filter = new NodeTreeFilterComboBox(pref)
    filter.setMaximumSize(new Dimension(500, 22))
    toolBar.add(filter)
    toolBar.addButton("Clear", "Clears Filter and Selection") {
      filter.setSelectedItem("")
      ntree.dataTable.clearSelection()
    }

    def findAction(path: String, useSelection: Boolean = false): Unit =
      ntree.expand(path, useSelection, compress = true)
    toolBar.addSeparator()
    toolBar.addButtonEx(
      "Find",
      "<html>Filter from Root Node(s)<br>Hold <i><b>Ctrl</b></i> to Start from current selection",
      enabled = true) { e: ActionEvent =>
      val useSelection = (e.getModifiers & ActionEvent.CTRL_MASK) != 0
      findAction(filter.getSelectedItem.toString, useSelection = useSelection)
    }
    toolBar.addSeparator()
    // trigger Find button with Enter key too
    filter.onFilterUpdate = filterPath => findAction(filterPath)

    toolBar.addButton("Show Exceptions", "Show paths to nodes that completed with exception") {
      ntree.expand(exceptionFilter, useSelection = true, compress = true)
    }
    toolBar
  }
}
