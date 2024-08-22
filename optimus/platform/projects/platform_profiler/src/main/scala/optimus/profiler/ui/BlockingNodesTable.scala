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

import javax.swing.JTabbedPane
import optimus.graph.diagnostics.NodeName
import optimus.graph.diagnostics.WaitProfile
import optimus.profiler.ProfilerUI
import optimus.profiler.ui.common.JPopupMenu2

import scala.collection.mutable.ArrayBuffer

object BlockingNodesTable {
  private val regularView = ArrayBuffer[TableColumn[WaitProfile]](
    new TableColumnTime[WaitProfile]("Time Offset", 100) {
      override def valueOf(row: WaitProfile, row2: WaitProfile): Double = row.timeOffset(row2)
    },
    new TableColumnTime[WaitProfile]("Wait Time", 100) {
      override def valueOf(row: WaitProfile): Double = row.waitTimeScaled
    },
    new TableColumnString[WaitProfile]("Awaited Node", 300) {
      override def valueOf(row: WaitProfile): String = row.awaitedNode
    },
    new TableColumnString[WaitProfile]("Awaited Chain End", 300) {
      override def valueOf(row: WaitProfile): String = row.awaitedChainEnd
    },
    new TableColumn[WaitProfile]("JStack") {
      override def valueOf(row: WaitProfile): String = row.jstackPreviewString
    }
  )
}

class BlockingNodesTable extends NPTable[WaitProfile] {
  emptyRow = new WaitProfile(0, 0, null, 0, null)
  setView(BlockingNodesTable.regularView)

  var ns_table: NodeStacksTable = _
  var ns_tab: JTabbedPane = _

  dataTable.setComponentPopupMenu(initPopupMenu)

  private def initPopupMenu: JPopupMenu2 = {
    val menu = new JPopupMenu2
    menu.addMenu(
      "Select in Node Stacks...", {
        val i = getSelection
        if (i ne null) {
          ns_table.setList(ProfilerUI.getNodeStacks(NodeProfiler.getNodeStack, i.start))
          ns_table.openTo(i.endChainPropID)
          ns_table.showMessage("Selected Node Stacks")
          ns_tab.setSelectedComponent(ns_table)
        }
      },
      enable = false // TODO (OPTIMUS-55942): revisit NodeStacks
    )
    menu.addMenu(
      "Print Sources...", {
        val i = getSelection
        if (i ne null) {
          println("Source for start node and end node...")
          NodeName.printSource(i.startChainPropID)
          NodeName.printSource(i.endChainPropID)
        }
      }
    )
    menu.addSeparator()
    menu.addMenu(
      "Print JStack to StdErr", {
        val i = getSelection
        if ((i ne null) && i.jstack != null) {
          i.jstack.printStackTrace()
        }
      })
    menu
  }

  override def afterUpdateList(): Unit = {
    if (rows.nonEmpty)
      emptyRow = new WaitProfile(rows.head.start, 0, null, 0, null)
  }
}
