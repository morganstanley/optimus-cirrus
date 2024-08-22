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
import optimus.debugger.browser.ui.NodeReview
import optimus.graph.diagnostics.PNodeTask
import optimus.platform.annotations.deprecating

import scala.collection.mutable.ArrayBuffer
import optimus.profiler.extensions.PNodeTaskExtension._

object NodeAnalyzeTable {
  val diffColorBg = new Color(255, 255, 215)
  val diffColorBgSelected = new Color(215, 215, 255)

  trait DiffHighlighter extends TableColumn[NodeReview] {
    override def getCellCustomColor(selected: Boolean, row: NodeReview, row2: NodeReview): Color = {
      if ((row eq row2) || valueOf(row) == valueOf(row2)) null
      else if (selected) diffColorBgSelected
      else diffColorBg
    }
  }

  private val regularView = ArrayBuffer(
    new TableColumnCount[NodeReview]("ID", 40) {
      override def valueOf(row: NodeReview): Int = row.task.id
    },
    new TableColumnTime[NodeReview]("Wall Time (ms)", 120) {
      override def valueOf(row: NodeReview): Double = row.task.wallTime() * 1e-6
    },
    new TableColumnTime[NodeReview]("Self Time (ms)", 120) {
      override def valueOf(row: NodeReview): Double = row.task.selfTime * 1e-6
    },
    new TableColumnCount[NodeReview]("SS #", 40) with DiffHighlighter {
      override def valueOf(row: NodeReview): Int = row.task.scenarioStack().trackingDepth
      override def toolTip: String = "Number of tracking scenarios in this scenario stack"
    },
    new TableColumnCount[NodeReview]("SS==", 40) with DiffHighlighter {
      override def valueOf(row: NodeReview): Int = row.commonScenarioStack
      override def toolTip: String =
        "Depth of the first common parent compared to top selected row and -1 if roots are different"
    },
    new TableColumnCount[NodeReview]("Twk +", 40) with DiffHighlighter {
      override def toolTip = "Number of used <b>extra tweaks</b> compared to top selected row"
      override def valueOf(row: NodeReview): Int = row.extraTweaks.size
    },
    new TableColumnCount[NodeReview]("Twk -", 40) with DiffHighlighter {
      override def toolTip = "Number of used <b>missing tweaks</b> compared to top selected row"
      override def valueOf(row: NodeReview): Int = row.missingTweaks.size
    },
    new TableColumnCount[NodeReview]("Twk ~", 40) with DiffHighlighter {
      override def toolTip = "Number of used <b>mismatched tweaks</b> compared to top selected row"
      override def valueOf(row: NodeReview): Int = row.mismatchedTweaks.size
    },
    new TableColumn[NodeReview]("\u2248", 40) {
      override def toolTip: String =
        "Seems like re-use should be possible (if the node is <b>not XS</b>, the scenario stack is " +
          "probably different but the node did not depend on the tweaks. If the node <b>is XS</b>, node hashes might have caused a mismatch)"
      override def valueOf(row: NodeReview): String = if (row.possibleReuse) "\u2248" else ""
      override def getCellCustomColor(selected: Boolean, row: NodeReview, row2: NodeReview): Color =
        if (!row.possibleReuse) null else Color.orange
      override def getCellRenderer: TableCellRenderer = NPTableRenderer.simpleRenderCenter
    },
    new TableColumn[NodeReview]("Entity ", 500) with DiffHighlighter {
      override def valueOf(row: NodeReview): Any = row.task.getEntity
    }
  )

  private val resultColumns = ArrayBuffer(new TableColumn[NodeReview]("Result", 500) {
    override def valueOf(row: NodeReview): Any = row.task.safeResultAsString

    override def getCellCustomColor(selected: Boolean, row: NodeReview, row2: NodeReview): Color = {
      if ((row eq row2) || row.task.resultKey == row2.task.resultKey) null
      else if (selected) diffColorBgSelected
      else diffColorBg
    }
  })

  class ArgTableColumn(val index: Int) extends TableColumn[NodeReview]("arg " + index, 500) with DiffHighlighter {
    override def valueOf(row: NodeReview): AnyRef = {
      val args = row.args
      if (args.length > index) args(index) else ""
    }
  }
}

class NodeAnalyzeTable extends NPTable[NodeReview] { outer =>
  emptyRow = new NodeReview(PNodeTask.fake)
  prototypesColumns = NodeAnalyzeTable.regularView

  private val menu = NodeMenu.create(outer)

  dataTable.setComponentPopupMenu(menu)
  dataTable.getSelectionModel.addListSelectionListener(_ => {
    val s = getSelection
    if ((emptyRow ne s) && (s ne null)) {
      recomputeDiffs(rows, s)
      dataTable.model.fireTableRowsUpdated(0, rows.length - 1)
    }
  })

  def setSource(r: ArrayBuffer[NodeReview]): Unit = {
    val nView = new ArrayBuffer[TableColumn[NodeReview]]()
    // Add columns for arguments
    if (r.length > 1) {
      val args = r(0).args
      if (args.nonEmpty) {
        var i = 0
        while (i < args.length) {
          nView += new NodeAnalyzeTable.ArgTableColumn(i)
          i += 1
        }
      }
    }
    dataTable.clearSelection()
    setRowsQuiet(r, null)
    setView(NodeAnalyzeTable.regularView ++ nView ++ NodeAnalyzeTable.resultColumns)
  }

  protected override def afterUpdateList(): Unit = {
    if (rows.nonEmpty)
      recomputeDiffs(rows, rows(0))
  }

  def recomputeDiffs(rows: ArrayBuffer[NodeReview], row: NodeReview): Unit = {
    if (row ne null) emptyRow = row
    rows.foreach(_.recomputeDiffs(row))
  }

  private[ui] def TEST_ONLY_recomputeDiffs(compareAgainst: NodeReview): Unit = recomputeDiffs(rows, compareAgainst)
}
