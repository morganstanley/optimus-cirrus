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
import java.util

import javax.swing.table.TableCellRenderer
import optimus.graph.DiagnosticSettings
import optimus.graph.PTools
import optimus.graph.diagnostics.NPTreeNode
import optimus.profiler.ui.common.JPanel2
import optimus.profiler.ui.common.JToolBar2

import scala.collection.mutable.ArrayBuffer

object JVMStackEntry {
  def getHotspots(parent: JVMStackEntry, prefix: Array[Long]): Array[JVMStackEntry] = {
    val stackRoots = PTools.getHotSpots(prefix)
    val clsNames = stackRoots(0).asInstanceOf[Array[String]]
    val methodNames = stackRoots(1).asInstanceOf[Array[String]]
    val sampleCounts = stackRoots(2).asInstanceOf[Array[Int]]
    val methodIDs = stackRoots(3).asInstanceOf[Array[Long]]
    val rows = new Array[JVMStackEntry](clsNames.length)
    var i = 0
    while (i < clsNames.length) {
      rows(i) = new JVMStackEntry(parent, clsNames(i), methodNames(i), sampleCounts(i), methodIDs(i))
      i += 1
    }
    util.Arrays.sort(rows, (a: JVMStackEntry, b: JVMStackEntry) => b.count - a.count)
    rows
  }
}

class JVMStackEntry(var parent: JVMStackEntry, clsName: String, methodName: String, val count: Int, val methodID: Long)
    extends NPTreeNode {
  private lazy val children = {
    val prefix = new Array[Long](level + 1)
    var i = level
    var cur = this
    while (cur ne null) {
      prefix(i) = cur.methodID
      i -= 1
      cur = cur.parent
    }
    JVMStackEntry.getHotspots(this, prefix)
  }
  override val level: Int = if (parent eq null) 0 else parent.level + 1
  override def hasChildren: Boolean = true
  override def getChildren: Iterable[NPTreeNode] = children
  override def title: String = clsName.substring(1, clsName.length - 1).replace('/', '.') + "." + methodName
}

object JVMStacksTable {
  private val regularView = ArrayBuffer(
    new TableColumn[JVMStackEntry]("Name", width = 800) {
      override def valueOf(row: JVMStackEntry): JVMStackEntry = row
      override def getCellRenderer: TableCellRenderer = NPTableRenderer.treeRenderer
    },
    new TableColumnCount[JVMStackEntry]("Sample Count", 80) {
      override def valueOf(row: JVMStackEntry): Int = row.count
    }
  )
}

class JVMStacksTable extends NPTreeTable[JVMStackEntry] with Filterable[JVMStackEntry] {
  emptyRow = new JVMStackEntry(null, "[]", "[]", 0, 0)
  override def initialColumns: ArrayBuffer[TableColumn[JVMStackEntry]] = JVMStacksTable.regularView
}

object JVMStacksView {
  def apply(): JVMStacksView = if (PTools.samplingSupported) new JVMStacksView() else null
}

class JVMStacksView extends JPanel2(new BorderLayout()) {
  private val stacksTable = new JVMStacksTable

  init()
  private def init(): Unit = {
    val toolBar = new JToolBar2()
    toolBar.addButton("Refresh", enabled = !DiagnosticSettings.offlineReview) { cmdRefresh() }
    toolBar.addSeparator()
    toolBar.addButton("Start") { PTools.startSampling() }
    toolBar.addButton("Stop") { PTools.stopSampling() }
    add(toolBar, BorderLayout.NORTH)
    add(stacksTable, BorderLayout.CENTER)
  }

  def cmdRefresh(): Unit = stacksTable.setList(JVMStackEntry.getHotspots(null, new Array[Long](0)))
}
