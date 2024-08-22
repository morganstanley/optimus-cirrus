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

import java.awt.BorderLayout
import java.awt.Color

import javax.swing.JSplitPane
import javax.swing.table.TableCellRenderer
import optimus.graph.diagnostics.SelectionFlags
import optimus.graph.diagnostics.NPTreeNode
import optimus.graph.diagnostics.PNodeTask
import optimus.profiler.extensions.NPTreeNodeExt
import optimus.profiler.recipes.CompareNodeDiffs
import optimus.profiler.recipes.DiffReason
import optimus.profiler.recipes.TreeComparisonResult
import optimus.profiler.recipes.TreeComparisonResult.PNodeTaskPair
import optimus.profiler.ui.common.JPanel2
import optimus.profiler.ui.common.JPopupMenu2
import optimus.profiler.ui.common.JSplitPane2
import optimus.profiler.ui.controls.JTextPaneScrollable

import scala.collection.mutable.ArrayBuffer

object NodeResultComparisonTable {
  private val allColumns = ArrayBuffer[TableColumn[NodeResultComparisonRow]](
    new TableColumn[NodeResultComparisonRow]("Task", 300) {
      override def valueOf(row: NodeResultComparisonRow): NodeResultComparisonRow = row
      override def getCellRenderer: TableCellRenderer = NPTableRenderer.treeRenderer
    },
    new TableColumn[NodeResultComparisonRow]("Other Task", 300) {
      override def valueOf(row: NodeResultComparisonRow): PNodeTask = row.task2
    },
    new TableColumn[NodeResultComparisonRow]("Result", 300) {
      override def valueOf(row: NodeResultComparisonRow): String =
        if (row.task != null) row.task.resultDisplayString else ""
    },
    new TableColumn[NodeResultComparisonRow]("Other Result", 300) {
      override def valueOf(row: NodeResultComparisonRow): String =
        if (row.task2 != null) row.task2.resultDisplayString else ""
    },
    new TableColumnString[NodeResultComparisonRow]("Diff", 300) {
      override def valueOf(row: NodeResultComparisonRow): String = row.reason.description
      override def toolTip: String = "Right click and select 'Show Diff' to see difference"
    },
    new TableColumnCount[NodeResultComparisonRow]("Seen Task", 100) {
      override def valueOf(row: NodeResultComparisonRow): Int = if (row.task != null && row.task.seenBefore) 1 else 0
      override def toolTip: String = "Task was marked as 'seen'"
    },
    new TableColumnCount[NodeResultComparisonRow]("Seen Other Task", 100) {
      override def valueOf(row: NodeResultComparisonRow): Int = if (row.task2 != null && row.task2.seenBefore) 1 else 0
      override def toolTip: String = "Other Task was marked as 'seen'"
    }
  )
}

final case class NodeResultComparisonRow(
    override val level: Int,
    pair: PNodeTaskPair,
    child: NodeResultComparisonRow,
    reason: DiffReason = DiffReason.None)
    extends NPTreeNode
    with DbgPrintSource {
  this.open = true
  override def hasChildren: Boolean = child ne null
  override def getChildren: Iterable[NPTreeNode] = List(child)

  override def task: PNodeTask = pair._1 // presumably the same task, but cope better?
  def task2: PNodeTask = pair._2
  override def title: String = task.toString
  override def printSource(): Unit = if (task ne null) task.printSource()

  if (reason ne DiffReason.None) diffFlag = SelectionFlags.Divergent
}

final class NodeResultComparisonTable(var tcr: TreeComparisonResult) extends NPTreeTable[NodeResultComparisonRow] {
  outer =>
  emptyRow = NodeResultComparisonRow(0, (PNodeTask.fake, PNodeTask.fake), null, DiffReason.None)

  setView(NodeResultComparisonTable.allColumns)

  val menu: JPopupMenu2 = NodeMenu.create(outer)
  dataTable.setComponentPopupMenu(menu)

  def setTreeComparisonResult(tcr: TreeComparisonResult): Unit = {
    this.tcr = tcr
    val root = createRootWithChildren(tcr)
    setList(List(root))
    val index = rows.length - 1
    dataTable.getSelectionModel.setSelectionInterval(index, index)
    dataTable.scrollRectToVisible(dataTable.getCellRect(index, 0, true))
  }

  private def createRootWithChildren(tcr: TreeComparisonResult): NodeResultComparisonRow = {
    def create(level: Int): NodeResultComparisonRow = {
      val next = level + 1
      if (next < tcr.path.length)
        NodeResultComparisonRow(level, tcr.path(level), create(next))
      else
        NodeResultComparisonRow(level, tcr.path(level), null, tcr.reason)
    }

    create(0)
  }

  override def getRowBackground(col: TableColumn[NodeResultComparisonRow], row: NodeResultComparisonRow): Color =
    NPTreeNodeExt.getRowBackground(col, row)
}

final class NodeTreeComparisonView(var tcr: TreeComparisonResult) extends JPanel2(new BorderLayout()) {
  private val splitPane = new JSplitPane2(pref)
  private val splitPaneTreeAndExplanation = new JSplitPane2(pref, "tree_explain")
  private val callTable = new NodeResultComparisonTable(tcr)
  private val overviewPane = new JTextPaneScrollable()
  private val diffView = new ValueTreeTable()

  init()
  updateResult(tcr)
  onSelectionChanged()

  private def init(): Unit = {
    splitPaneTreeAndExplanation.setOrientation(JSplitPane.VERTICAL_SPLIT)
    splitPaneTreeAndExplanation.setTopComponent(callTable)
    splitPaneTreeAndExplanation.setBottomComponent(overviewPane)

    splitPane.setOrientation(JSplitPane.HORIZONTAL_SPLIT)
    splitPane.setLeftComponent(splitPaneTreeAndExplanation)
    splitPane.setRightComponent(diffView)

    overviewPane.addLinkHandler(TreeComparisonResult.cmdSkipComparingArgsInNode) { skipNodeArgInComparison(tcr.childA) }
    overviewPane.addLinkHandler(TreeComparisonResult.cmdSkipComparingNode) { skipNodeComparison(tcr.childA) }

    callTable.dataTable.getSelectionModel.addListSelectionListener(e =>
      if (!e.getValueIsAdjusting) onSelectionChanged())
    callTable.menu.addSeparator()
    callTable.menu.addMenu("Skip Property in Comparison", "Skip and Recompute") {
      val sel = callTable.getSelection
      sel.task.getTask.executionInfo().setIgnoredInProfiler(true)
      val res = CompareNodeDiffs.compareTrees(callTable.tcr.branchA, callTable.tcr.branchB)
      callTable.setTreeComparisonResult(res)
    }
    callTable.menu.addMenu("Skip Node Result in Comparison", "Skip and Recompute") {
      skipNodeComparison(callTable.getSelection.task)
    }
    callTable.menu.addMenu("Skip Node Arguments in Comparison", "Skip and Recompute") {
      skipNodeArgInComparison(callTable.getSelection.task)
    }
    callTable.menu.addMenu("Compare Structure Only", "Ignore differences in Results/Args") {
      val res = CompareNodeDiffs.compareTrees(callTable.tcr.branchA, callTable.tcr.branchB, structureOnly = true)
      callTable.setTreeComparisonResult(res)
    }
    // add(createTreeToolbar(), BorderLayout.NORTH) -- for when we make the tool more interactive..
    add(splitPane, BorderLayout.CENTER)
  }

  private def updateResult(result: TreeComparisonResult): Unit = {
    tcr = result
    callTable.setTreeComparisonResult(tcr)
    overviewPane.getTextPane.setContentType("text/html")
    overviewPane.getTextPane.setText(tcr.reason.htmlDescription(tcr))
  }

  private def skipNodeArgInComparison(task: PNodeTask): Unit = {
    task.setIgnoreArgumentsInComparison()
    updateResult(CompareNodeDiffs.compareTrees(tcr.branchA, tcr.branchB))
  }

  private def skipNodeComparison(task: PNodeTask): Unit = {
    task.setIgnoreResultInComparison()
    updateResult(CompareNodeDiffs.compareTrees(tcr.branchA, tcr.branchB))
  }

  private def onSelectionChanged(): Unit = {
    val s = callTable.getSelection
    if (s ne null) {
      if (s.task.isLive) {
        val task1 = if (s.task ne null) s.task.getTask else null
        val task2 = if (s.task2 ne null) s.task2.getTask else null
        diffView.inspect(Array(task1, task2))
      } else
        diffView.inspect(Array(s.task, s.task2))
    }
  }
}
