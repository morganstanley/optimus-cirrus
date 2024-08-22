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

import optimus.graph.PThreadContext
import optimus.graph.diagnostics.messages.CacheClearCounter
import optimus.profiler.ui.Filterable
import optimus.profiler.ui.TableColumn
import optimus.profiler.ui.NPTimelineTable
import optimus.profiler.ui.NodeTimeLine
import optimus.profiler.ui.TableColumnCount
import optimus.profiler.ui.TableColumnString
import optimus.profiler.ui.TableColumnUTC
import optimus.profiler.ui.TimelineEvent

import scala.collection.mutable.ArrayBuffer

final case class CacheClearEvent(
    id: Int,
    cacheName: String,
    clearDescription: String,
    startTimeNanos: Long,
    numRemoved: Long,
    numRemaining: Long,
    totalNumRemaining: Long,
    process: PThreadContext.Process)
    extends TimelineEvent

class CacheClearTable(tline: NodeTimeLine)
    extends NPTimelineTable[CacheClearEvent](tline)
    with Filterable[CacheClearEvent] {
  emptyRow = CacheClearEvent(0, "", "", 0, 0, 0, 0, PThreadContext.Process.none)
  override def initialColumns: ArrayBuffer[TableColumn[CacheClearEvent]] = regularView

  override def updateData(): Unit = {
    val selectedCacheClearEvents = tline.getSelectedEvents(CacheClearCounter)
    val r = selectedCacheClearEvents.map { e =>
      CacheClearEvent(0, e.name, e.description, e.time, e.numRemoved, e.numRemaining, e.totalNumRemaining, e.process)
    }
    setList(r)
  }

  private def regularView: ArrayBuffer[TableColumn[CacheClearEvent]] = ArrayBuffer(
    new TableColumnCount[CacheClearEvent](name = "Event ID", width = 250) {
      override def valueOf(row: CacheClearEvent): Int = row.id
    },
    new TableColumnString[CacheClearEvent](name = "Cache Name", width = 250) {
      override def valueOf(row: CacheClearEvent): String = row.cacheName
    },
    new TableColumnString[CacheClearEvent](name = "Description", width = 250) {
      override def valueOf(row: CacheClearEvent): String = row.clearDescription
    },
    new TableColumnUTC[CacheClearEvent](tline.getReader.nanoToUTC, name = "Time", width = 250) {
      override def nanos(row: CacheClearEvent): Long = row.startTimeNanos
    },
    new TableColumnCount[CacheClearEvent](name = "Entries removed", width = 250) {
      override def valueOf(row: CacheClearEvent): Int = row.numRemoved.toInt
    },
    new TableColumnCount[CacheClearEvent](name = "Entries remaining", width = 250) {
      override def valueOf(row: CacheClearEvent): Int = row.numRemaining.toInt
    },
    new TableColumnCount[CacheClearEvent](name = "Total Entries remaining (all caches)", width = 250) {
      override def valueOf(row: CacheClearEvent): Int = row.totalNumRemaining.toInt
    },
    new TableColumnString[CacheClearEvent](name = "Process", width = 250) {
      override def valueOf(row: CacheClearEvent): String = if (row.process eq null) null else row.process.displayName
    }
  )
}
