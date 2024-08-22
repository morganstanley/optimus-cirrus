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
import java.awt.event.MouseEvent

import javax.swing.ListSelectionModel
import javax.swing.table.AbstractTableModel
import javax.swing.table.TableCellRenderer
import optimus.graph.diagnostics.NPTreeNode
import optimus.profiler.ui.NPTableRenderer.TTTableBaseCellRender
import optimus.profiler.ui.common.HasMessageBanners

class NPSubTableData[RType](table: NPTable[RType]) extends NPSubTable(table) with HasMessageBanners {

  private[ui] val model = new AbstractTableModel {
    override def isCellEditable(row: Int, column: Int) = false
    override def getRowCount: Int = Rows.length
    override def getColumnCount: Int = Cols.length
    override def getColumnClass(columnIndex: Int): Class[_] = {
      val rows = Rows
      if (rows.nonEmpty) {
        val v = Cols(columnIndex).prototype.valueOf(rows(0), rows(0))
        if (v.asInstanceOf[AnyRef] ne null) v.getClass else classOf[AnyRef]
      } else
        super.getColumnClass(columnIndex)
    }

    override def getValueAt(row: Int, column: Int): AnyRef = {
      val rows = Rows
      val rowsC = RowsC
      if (row >= rows.size || row < 0) ""
      else {
        val tt = rows(row)
        val tt2 = if ((rowsC.length <= row) || (rowsC(row).asInstanceOf[AnyRef] eq null)) table.emptyRow else rowsC(row)
        val col = Cols(column)
        col.prototype.valueOf(tt, tt2).asInstanceOf[AnyRef]
      }
    }
  }

  table.setBackground(Color.WHITE)
  setModel(model)
  setFont(NPTableRenderer.dataFont)

  // NPSubTableHeader contains the real header
  setTableHeader(null)

  setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION)
  getSelectionModel.addListSelectionListener(_ => {
    Cols foreach { _.totalSelected = null }
    table.sumTable.model.fireTableDataChanged()
  })

  final def prepareCustomBackground(c: Component, selected: Boolean, row: Int, col: Int): Color = {
    val tcol = getTCol(col)
    val rO = getTRow(row)
    val rC = table.emptyRow

    val cellColor = tcol.getCellCustomColor(selected, rO, rC)
    if (cellColor ne null) cellColor
    else if (selected) null
    else table.getRowBackground(tcol, rO)
  }

  override def processMouseEvent(e: MouseEvent): Unit = {
    if (e.getClickCount == 2) {
      val selection = table.getSelection
      selection match {
        case treeNode: NPTreeNode if treeNode.isCompressed =>
        case null                                          => super.processMouseEvent(e)
        case _ =>
          if (table.onRowDoubleClicked ne null)
            table.onRowDoubleClicked(selection)
      }
    }
    super.processMouseEvent(e)
  }

  override def prepareRenderer(renderer: TableCellRenderer, row: Int, col: Int): Component = {
    val c = super.prepareRenderer(renderer, row, col)
    val bg = table.getBackground
    val selected = isCellSelected(row, col)
    if (!selected)
      c.setBackground(if ((row & 1) == 1) bg else NPTableRenderer.alternateColor)

    val customBg = prepareCustomBackground(c, selected, row, col)
    if (customBg ne null)
      c.setBackground(customBg)

    if (table.getDisablingEnabled && table.isDisabledRow(convertRowIndexToView(row)))
      c.setBackground(NPTableRenderer.disabledRowColor)

    c match {
      case render: TTTableBaseCellRender => render.cellRectangle = getCellRect(row, col, false)
      case _                             =>
    }

    c
  }

}
