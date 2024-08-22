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
import java.awt.Graphics
import java.awt.Graphics2D
import java.awt.event.MouseEvent
import javax.swing.JTable
import javax.swing.border.Border
import javax.swing.border.MatteBorder
import javax.swing.event.ChangeEvent
import javax.swing.event.ListSelectionEvent
import javax.swing.event.TableColumnModelEvent
import javax.swing.event.TableColumnModelListener
import javax.swing.table.AbstractTableModel
import javax.swing.table.JTableHeader
import javax.swing.table.TableCellRenderer
import javax.swing.table.TableColumnModel

class NPSubTableHeader[RType](table: NPTable[RType]) extends NPSubTable(table) {
  private def dtable = table.dataTable
  private def dmodel = dtable.getModel
  private var filterEnabled = false
  private val filterColor = new Color(255, 255, 242)
  private val filterBorder = new Color(249, 241, 241)
  var showTypeAsSummary = false

  def setFilterRow(enable: Boolean): Unit = {
    filterEnabled = enable
    model.fireTableDataChanged()
    setPreferredScrollableViewportSize(getPreferredSize)
  }

  private[ui] val model = new AbstractTableModel {
    override def getRowCount: Int = if (filterEnabled) 1 else 0
    override def getColumnCount: Int = dmodel.getColumnCount
    override def isCellEditable(rowIndex: Int, columnIndex: Int): Boolean = {
      if (filterEnabled && rowIndex == 0) true
      else false
    }
    override def setValueAt(aValue: AnyRef, rowIndex: Int, columnIndex: Int): Unit = {
      if (filterEnabled && rowIndex == 0) {
        val col = table.getTableColumns(columnIndex)
        col.filter = if (aValue eq null) null else aValue.toString
        table.updateFilterAndRefresh()
        table.viewChanged()
      }
    }
    override def getValueAt(row: Int, column: Int): AnyRef = {
      val cols = table.getTableColumns
      val col = cols(column)
      col.filter
    }
    override def getColumnName(column: Int): String = Cols(column).prototype.name
  }

  setModel(model)
  // Support for column header tooltips
  setTableHeader(new JTableHeader(columnModel) {
    override def getToolTipText(e: MouseEvent): String = {
      val index = columnModel.getColumnIndexAtX(e.getPoint.x)
      if (index >= 0) {
        val tCol = getTCol(index)
        val toolTip = "<html>" + tCol.name + (if (tCol.toolTip ne null) "<br>" + tCol.toolTip else "") + "</html>"
        toolTip
      } else null
    }
  })
  setRowSelectionAllowed(false)
  setBackground(Color.white)
  val border = new MatteBorder(0, 0, 2, 0, filterBorder)
  setBorder(border)

  private[ui] class XColumnModelListener extends TableColumnModelListener {
    var syncMargins = true

    def syncToModel(): Unit = {
      syncToModel(table.sumTable)
      syncToModel(table.dataTable)
      table.viewChanged()
    }

    private def syncToModel(npTable: JTable): Unit = {
      val target = npTable.getColumnModel
      if (syncMargins) {
        val cm = getColumnModel
        var i = 0
        val colCount = cm.getColumnCount
        val colCountS = target.getColumnCount
        while (i < colCount && i < colCountS) {
          val cmCol = cm.getColumn(i)
          val cmsCol = target.getColumn(i)
          cmsCol.setWidth(cmCol.getWidth)
          cmsCol.setPreferredWidth(cmCol.getWidth)
          val viewColumn = table.viewColumns(cmCol.getModelIndex)
          viewColumn.index = i
          viewColumn.width = cmCol.getWidth

          i += 1
        }
      }
    }

    def columnAdded(e: TableColumnModelEvent): Unit = {}
    def columnRemoved(e: TableColumnModelEvent): Unit = {}
    def columnMoved(e: TableColumnModelEvent): Unit = {
      def doMove(table: JTable): Unit = table.getColumnModel.moveColumn(e.getFromIndex, e.getToIndex)
      doMove(table.sumTable)
      doMove(table.dataTable)

      syncToModel()
    }
    def columnMarginChanged(e: ChangeEvent): Unit = syncToModel()
    def columnSelectionChanged(e: ListSelectionEvent): Unit = {}
  }
  private[ui] val columnModelListener: XColumnModelListener = new XColumnModelListener

  getColumnModel.addColumnModelListener(columnModelListener)

  override def prepareRenderer(renderer: TableCellRenderer, row: Int, col: Int): Component = {
    val c = super.prepareRenderer(renderer, row, col)
    if (row == 0 && filterEnabled) {
      c.setBackground(filterColor)
    } else
      c.setBackground(null)
    c
  }

  override def paintComponent(g: Graphics): Unit = {
    super.paintComponent(g)
    if (!table.currentViewHasFilter) {
      val g2 = g.asInstanceOf[Graphics2D]
      val width = 25
      val text = "Double click on a cell in this line to enter a filter"
      val bounds = NPTableRenderer.diffFont.getStringBounds(text, g2.getFontRenderContext).getBounds
      g.setFont(NPTableRenderer.diffFont)
      g.setColor(Color.BLACK)
      g.drawString(text, width, bounds.height - 5)
    }
  }
}
