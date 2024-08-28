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
package optimus.graph.diagnostics

import javax.swing.JComponent
import java.awt.Color
import java.awt.Font
import java.awt.Toolkit
import java.awt.datatransfer.StringSelection
import java.awt.event.ActionEvent
import java.awt.event.KeyEvent

import javax.swing.Icon
import javax.swing.JScrollPane
import javax.swing.JTable
import javax.swing.KeyStroke

// make entity class to extend this trait if it has customized view to show on graph debugger
trait GraphDebuggerViewable {
  def view: JComponent
}

trait GraphDebuggerValueDetailsViewable {
  def view(string: String): JComponent
}
object GraphDebuggerValueDetailsViewable {
  def table(data: Iterable[Iterable[AnyRef]], headers: Iterable[AnyRef]): JComponent =
    table(data.iterator.map { _.toArray }.toArray, headers.toArray)

  def table(data: Array[Array[AnyRef]], headers: Array[AnyRef]): JComponent = {
    val jt = new JTable(data, headers)
    jt.setBounds(30, 15, 1500, 700)
    jt.setAutoResizeMode(JTable.AUTO_RESIZE_OFF)
    jt.setColumnSelectionAllowed(true)
    jt.setRowSelectionAllowed(true)
    jt.setCellSelectionEnabled(true)
    jt.setAutoCreateRowSorter(true)
    jt.registerKeyboardAction(
      (_: ActionEvent) => {
        val str = new StringSelection({
          // we only want headers if you have multiple columns to distinguish
          val header =
            if (jt.getSelectedColumnCount <= 1) ""
            else jt.getSelectedColumns.map { jt.getColumnName }.mkString("\t") + "\n"

          val body = jt.getSelectedRows
            .map { row =>
              jt.getSelectedColumns
                .map { col =>
                  jt.getValueAt(row, col)
                }
                .mkString("\t")
            }
            .mkString("\n")
          s"$header$body"
        })
        Toolkit.getDefaultToolkit.getSystemClipboard.setContents(str, str)
      },
      KeyStroke.getKeyStroke(KeyEvent.VK_C, ActionEvent.CTRL_MASK, false),
      JComponent.WHEN_FOCUSED
    )
    new JScrollPane(jt)
  }

  def map(data: Map[AnyRef, AnyRef]): JComponent =
    table(data.iterator.map { case (k, v) => Array(k, v) }.toArray, Array[AnyRef]("key", "value"))
}

object SelectionFlags extends Enumeration {
  val NotFlagged, Irrelevant, Divergent, Outlier, Highlighted, Incomplete, Dependent, Dependency, Poison, EqualDiff,
      HashCodeDiff = Value
  type SelectionFlag = Value

  def toToolTipText(flags: SelectionFlags.Value): String = {
    flags match {
      case Irrelevant   => "<html><b>Irrelevant</b>: child of something that does not exist on other node</html>"
      case Divergent    => "<html><b>Divergent</b>: exists on other node but with different value</html>"
      case Outlier      => "<html><b>Outlier</b>: found only on this node</html>"
      case HashCodeDiff => "<html>Difference in compare hashcode</html>"
      case Poison => "<html><b>Poison</b>: captured scenario state so assuming dependency on all tweakables</html>"
      case _      => null
    }
  }
}

/**
 * General interface to generalize hierarchical info
 */
trait NPTreeNode {
  var open: Boolean = _
  var matched: Boolean = _ // Last matching matched this as a final node
  var diffFlag: SelectionFlags.Value = SelectionFlags.NotFlagged

  def level: Int
  def hasChildren: Boolean
  def getChildren: Iterable[NPTreeNode]
  def title: String = ""
  def title(jtable: JTable): String = title // Default implementation just gets a simple title
  def foreground: Color = null
  def background: Color = null
  def font: Font = null
  def tooltipText: String = null
  def icon: Icon = null

  def isCompressed = false // Compress a group of nodes into this one
  def compressClosed(): Unit = {}
  def getUncompressedChildren: Iterable[NPTreeNode] = getChildren
}
