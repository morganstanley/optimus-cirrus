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

import javax.swing.JTable
import javax.swing.table.TableCellRenderer

import scala.collection.mutable.ArrayBuffer

class NPSubTable[RType](val table: NPTable[RType]) extends JTable {
  setRowHeight(Math.round(getRowHeight * Fonts.multiplier))

  final def Cols: ArrayBuffer[TableViewColumn[RType]] = table.getTableColumns
  final def Rows: ArrayBuffer[RType] = table.getTableRows
  final def RowsC: ArrayBuffer[RType] = table.getTableRowsC

  setAutoResizeMode(JTable.AUTO_RESIZE_OFF)
  setFillsViewportHeight(true)
  setShowGrid(false)

  def getTColIndex(column: Int): Int = getColumnModel.getColumn(column).getModelIndex
  def getTCol(column: Int): TableColumn[RType] = table.getTableColumns(getTColIndex(column)).prototype
  def getTRow(row: Int): RType = Rows(convertRowIndexToModel(row))

  override def getCellRenderer(row: Int, column: Int): TableCellRenderer = {
    val tcol = getTCol(column)
    val colCellRender = tcol.getCellRenderer
    if (colCellRender ne null) colCellRender
    else NPTableRenderer.simpleRender // Really just our custom render
  }

}
