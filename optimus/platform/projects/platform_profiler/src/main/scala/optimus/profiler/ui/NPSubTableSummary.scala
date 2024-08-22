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
import java.awt.Component
import javax.swing.border.Border
import javax.swing.border.MatteBorder
import javax.swing.table.AbstractTableModel
import javax.swing.table.TableCellRenderer

class NPSubTableSummary[RType](table: NPTable[RType]) extends NPSubTable(table) {
  private val dtable = table.dataTable
  private val dmodel = dtable.getModel
  private val sumColor = new Color(255, 255, 242)
  private val sumBorder = new Color(249, 241, 241)
  var showTypeAsSummary = false

  private[ui] val model = new AbstractTableModel {
    override def getRowCount: Int = 1
    override def getColumnCount: Int = dmodel.getColumnCount
    override def isCellEditable(rowIndex: Int, columnIndex: Int): Boolean = false
    override def getValueAt(row: Int, column: Int): AnyRef = {
      val cols = table.getTableColumns
      val rowCount = dmodel.getRowCount
      val col = cols(column)

      if (showTypeAsSummary) {
        val v = dmodel.getValueAt(dtable.convertRowIndexToModel(dtable.getSelectedRow), column)
        if (v ne null) v.getClass.getName else ""
      } else if (rowCount <= 0) {
        ""
      } else
        col.prototype.summaryType match {
          case TableColumn.SummaryType.Count =>
            val selRowsCount = dtable.getSelectedRowCount
            "Rows: " +
              (if (selRowsCount > 1) NPTableRenderer.countFormat.format(selRowsCount) + " / " else "") +
              NPTableRenderer.countFormat.format(rowCount)
          case TableColumn.SummaryType.Total =>
            val total = col.total
            val totalSel = col.totalSelected
            if (total eq null)
              col.total = col.prototype.computeSummary(table, 0 until rowCount)

            val showSubTotals = if (totalSel eq null) {
              val srows = dtable.getSelectedRows
              col.selectedCount = srows.length
              if (srows.nonEmpty) {
                var i = 0
                while (i < srows.length) {
                  srows(i) = dtable.convertRowIndexToModel(srows(i))
                  i += 1
                }
                col.totalSelected = col.prototype.computeSummary(table, srows)
              } else
                col.totalSelected = col.total

              if (srows.length > 1) true else false // Show subTotals only when more than 1 row is selected
            } else false

            if (col.total.isInstanceOf[String]) { if (showSubTotals) col.totalSelected else col.total }
            else
              (col.selectedCount, col.total, col.totalSelected)
          case TableColumn.SummaryType.None =>
            null // looks not that safe but if you're using this mode you will be prepared
        }
    }
    override def getColumnName(column: Int) = ""
  }

  val border = new MatteBorder(0, 0, 2, 0, sumBorder)
  setModel(model)
  setTableHeader(null)
  setRowSelectionAllowed(false)
  setBackground(sumColor)
  setBorder(border)

  override def prepareRenderer(renderer: TableCellRenderer, row: Int, col: Int): Component = {
    val c = super.prepareRenderer(renderer, row, col)
    c.setBackground(null)
    c
  }
}
