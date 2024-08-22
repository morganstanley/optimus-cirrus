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

import java.awt.event._

import optimus.graph.diagnostics.NPTreeNode
import optimus.profiler.extensions.NPTreeNodeExt

trait TreeSelector[RType] {
  def review(i: RType): Unit
  def matched: Boolean
  def maybeLower: Boolean

  def moveDown(): TreeSelector[RType]

  def destroy(): Unit
}

abstract class NPTreeTable[RType <: NPTreeNode] extends NPTable[RType] {
  override def wantSummary = false

  // this must be false because child/parent expansion might get disarranged if we are able to sort (ie, we no longer respect the tree)
  override protected def sortable = false
  var roots: Iterable[RType] = _

  dataTable.addKeyListener(new KeyListener() {
    override def keyTyped(e: KeyEvent): Unit = {}
    override def keyReleased(e: KeyEvent): Unit = {}
    override def keyPressed(e: KeyEvent): Unit = {
      // keys act on the first node tree table cell in all currently-selected rows
      val colOpt = (0 until dataTable.getColumnCount) find (n => {
        val r = dataTable.getTCol(n).getCellRenderer
        (r ne null) && r.isInstanceOf[NPTableRenderer.TreeTableCellRenderer]
      })

      colOpt.map(col => {
        val tcol = dataTable.getTCol(col)
        val tree = tcol.getCellRenderer.asInstanceOf[NPTableRenderer.TreeTableCellRenderer]
        val ctrlDown = (e.getModifiersEx & InputEvent.CTRL_DOWN_MASK) != 0
        if (
          e.getKeyCode == KeyEvent.VK_LEFT || e.getKeyCode == KeyEvent.VK_KP_LEFT
          || e.getKeyCode == KeyEvent.VK_MINUS || e.getKeyCode == KeyEvent.VK_SUBTRACT
        ) {
          dataTable.getSelectedRows reverseMap (tree.collapse(dataTable, _, col, ctrlDown))
        } else if (
          e.getKeyCode == KeyEvent.VK_RIGHT || e.getKeyCode == KeyEvent.VK_KP_RIGHT
          || e.getKeyCode == KeyEvent.VK_EQUALS || e.getKeyCode == KeyEvent.VK_ADD
        ) {
          dataTable.getSelectedRows reverseMap (tree.expand(dataTable, _, col, ctrlDown))
        }
      })
    }
  })

  dataTable.addMouseListener(new MouseAdapter {
    override def mousePressed(e: MouseEvent): Unit = {
      val pt = e.getPoint
      val row = dataTable.rowAtPoint(pt)
      val col = dataTable.columnAtPoint(pt)
      if (row >= 0 && col >= 0) {
        val tcol = dataTable.getTCol(col)
        tcol.getCellRenderer match {
          case tree: NPTableRenderer.TreeTableCellRenderer => tree.click(dataTable, row, col, e)
          case _                                           =>
        }
      }
    }
  })

  def setList(roots: Iterable[RType]): Unit = {
    this.roots = roots
    rows = NPTreeNodeExt.expandTree(roots)
    afterUpdateList()
    refresh()
  }

  def select(pred: RType => Boolean): Unit = {
    val index = rows.indexWhere(pred)
    dataTable.getSelectionModel.setSelectionInterval(index, index)
    dataTable.scrollRectToVisible(dataTable.getCellRect(index, 0, true))
  }

  def openTo(pred: RType => Boolean): Unit =
    openTo(useSelection = false, stopOnFirstMatch = false, expandRoots = false, pred)

  def openTo(useSelection: Boolean, stopOnFirstMatch: Boolean, expandRoots: Boolean, pred: RType => Boolean): Unit = {
    val rtu = rootsToUse(useSelection)
    if (rtu ne null) {
      if (expandRoots)
        for (r <- rtu) r.open = true

      NPTreeNodeExt.openTo(rtu, stopOnFirstMatch, pred)
      updateRows()
    }
  }

  def openTo(useSelection: Boolean, pred: TreeSelector[RType], compress: Boolean): Unit = {
    val rtu = rootsToUse(useSelection)
    NPTreeNodeExt.openTo(rtu, pred, compress)
    updateRows()
  }

  private def rootsToUse(useSelection: Boolean) = {
    if (useSelection) {
      val sels = getSelections
      if (sels.isEmpty) roots
      else sels
    } else roots
  }

  private def updateRows(): Unit = {
    rows = NPTreeNodeExt.expandTree(roots)
    afterUpdateList()
    refresh()
  }

}
