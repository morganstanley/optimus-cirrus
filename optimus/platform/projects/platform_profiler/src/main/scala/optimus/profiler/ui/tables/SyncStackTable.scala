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
package optimus.profiler.ui.tables

import optimus.graph.diagnostics.messages.SyncStackCounter
import optimus.profiler.ui.Filterable
import optimus.profiler.ui.GraphDebuggerUI.setFrameToLostConcurrency
import optimus.profiler.ui.common.JPopupMenu2
import optimus.profiler.ui.NPTimelineTable
import optimus.profiler.ui.NodeTimeLine
import optimus.profiler.ui.TableColumn
import optimus.profiler.ui.TableColumnCount
import optimus.profiler.ui.TableColumnLong
import optimus.profiler.ui.TableColumnUTC
import optimus.profiler.ui.TimelineEvent
import optimus.profiler.ui.tables.SyncStackTable.truncateLines

import scala.collection.mutable.ArrayBuffer

final case class SyncStackEvent(
    id: Int,
    description: String,
    task: String,
    pluginType: String,
    startTimeNanos: Long,
    count: Int)
    extends TimelineEvent

class SyncStackTable(tline: NodeTimeLine)
    extends NPTimelineTable[SyncStackEvent](tline)
    with Filterable[SyncStackEvent] {
  emptyRow = SyncStackEvent(0, "", "", "", 0, 0)
  dataTable.setComponentPopupMenu(lostConcurrencyMenu)
  override def initialColumns: ArrayBuffer[TableColumn[SyncStackEvent]] = regularView

  private def lostConcurrencyMenu: JPopupMenu2 = {
    val menu = new JPopupMenu2
    menu.addMenu("Go to Lost Concurrency Table") {
      setFrameToLostConcurrency()
    }
    menu
  }

  override def updateData(): Unit = {
    val selectedSyncStackEvents = tline.getSelectedEvents(SyncStackCounter)
    val r = selectedSyncStackEvents.map { e =>
      SyncStackEvent(0, e.description, e.task, e.pluginType, e.time, e.count)
    }
    setList(r)
  }

  private def regularView: ArrayBuffer[TableColumn[SyncStackEvent]] = ArrayBuffer(
    new TableColumnCount[SyncStackEvent](name = "Event ID", width = 200) {
      override def valueOf(row: SyncStackEvent): Int = row.id
    },
    new TableColumn[SyncStackEvent](name = "Description", width = 200) {
      override def valueOf(row: SyncStackEvent): String = truncateLines(row.description, 10)
    },
    new TableColumn[SyncStackEvent](name = "Node Task", width = 200) {
      override def valueOf(row: SyncStackEvent): String = row.task
    },
    new TableColumn[SyncStackEvent](name = "Plugin Type", width = 200) {
      override def valueOf(row: SyncStackEvent): String = row.pluginType
    },
    new TableColumnUTC[SyncStackEvent](tline.getReader.nanoToUTC, name = "Time", width = 200) {
      override def nanos(row: SyncStackEvent): Long = row.startTimeNanos
    },
    new TableColumnLong[SyncStackEvent](name = "# Times Hit", width = 200) {
      override def valueOf(row: SyncStackEvent): Long = row.count
    }
  )
}

object SyncStackTable {
  def truncateLines(lines: String, numLines: Int): String = {
    val splitArray = lines.split("\n")
    var result = ""
    for (i <- 0 until numLines) {
      result = result + splitArray(i) + "\n"
      if ((i + 1) >= splitArray.size) {
        return result
      }
    }
    result + "..."
  }
}
