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

import java.awt.event.MouseAdapter
import java.awt.event.MouseEvent
import java.util.Map.{Entry => JMapEntry}

import optimus.graph.OGSchedulerLostConcurrency
import optimus.profiler.ui.LostConcurrencyTable.LostConcurrency
import optimus.profiler.ui.common.JPopupMenu2

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer

object LostConcurrencyTable {
  type LostConcurrency =
    JMapEntry[String, OGSchedulerLostConcurrency.UniqueCounts] // both sync stacks and non concurrent loops

  private val regularView = ArrayBuffer[TableColumn[LostConcurrency]](
    new TableColumnString[LostConcurrency]("Type", 100) {
      override def valueOf(row: LostConcurrency): String = row.getValue.displayString
    },
    new TableColumnCount[LostConcurrency]("Count", 100) {
      override def valueOf(row: LostConcurrency): Int = row.getValue.get()
    },
    new TableColumnString[LostConcurrency]("Plugin Type", 100) {
      override def valueOf(row: LostConcurrency): String = row.getValue.pluginType.name
    },
    new TableColumnString[LostConcurrency]("JVM Stack", 600) {
      override def valueOf(row: LostConcurrency): String = convertToHTML(row.getKey)
    }
  )
}

class LostConcurrencyTable extends NPTable[LostConcurrency] with Filterable[LostConcurrency] {
  emptyRow = null: LostConcurrency

  dataTable.setComponentPopupMenu(initPopupMenu)
  dataTable.addMouseListener(new MouseAdapter {
    override def mouseClicked(e: MouseEvent): Unit = if (e.getClickCount == 2) printSelectedStacks()
  })

  def refresh(): Unit = {
    setList(ArrayBuffer(OGSchedulerLostConcurrency.lostCCEntries().asScala: _*))
  }

  private def initPopupMenu: JPopupMenu2 = {
    val menu = new JPopupMenu2
    menu.addMenu("Print JStack to Console", printSelectedStacks())
    menu
  }

  private def printSelectedStacks(): Unit = {
    getSelections.foreach { k =>
      println(k.getValue.tpe)
      println(k.getKey)
    }
  }
  override def initialColumns: ArrayBuffer[TableColumn[LostConcurrency]] = LostConcurrencyTable.regularView
}
