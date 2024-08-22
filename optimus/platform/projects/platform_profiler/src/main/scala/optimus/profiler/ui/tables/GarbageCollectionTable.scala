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
import optimus.graph.diagnostics.messages.GCCounter
import optimus.profiler.ui.Filterable
import optimus.profiler.ui.NPTimelineTable
import optimus.profiler.ui.NodeTimeLine
import optimus.profiler.ui.TableColumn
import optimus.profiler.ui.TableColumnCount
import optimus.profiler.ui.TableColumnLong
import optimus.profiler.ui.TableColumnString
import optimus.profiler.ui.TableColumnTime
import optimus.profiler.ui.TableColumnUTC
import optimus.profiler.ui.TimelineEvent

import scala.collection.mutable.ArrayBuffer

final case class GCEvent(
    id: Int,
    gcType: String,
    gcCause: String,
    startTimeNanos: Long,
    override val durationNanos: Long,
    memCleared: Long,
    process: PThreadContext.Process)
    extends TimelineEvent

class GarbageCollectionTable(tline: NodeTimeLine) extends NPTimelineTable[GCEvent](tline) with Filterable[GCEvent] {
  emptyRow = GCEvent(0, "", "", 0, 0, 0, PThreadContext.Process.none)
  override def initialColumns: ArrayBuffer[TableColumn[GCEvent]] = regularView

  override def updateData(): Unit = {
    val r = tline.getSelectedEvents(GCCounter).map { e =>
      GCEvent(0, e.gcTypeDescription, e.gcCause, e.startTime, e.duration, e.graphValue, e.process)
    }
    setList(r)
  }

  private def regularView: ArrayBuffer[TableColumn[GCEvent]] = ArrayBuffer(
    new TableColumnCount[GCEvent](name = "Event ID", width = 200) {
      override def valueOf(row: GCEvent): Int = row.id
    },
    new TableColumnString[GCEvent](name = "Collection Type", width = 200) {
      override def valueOf(row: GCEvent): String = row.gcType
    },
    new TableColumnString[GCEvent](name = "Cause", width = 200) {
      override def valueOf(row: GCEvent): String = row.gcCause
    },
    new TableColumnUTC[GCEvent](tline.getReader.nanoToUTC, name = "Start Time", width = 200) {
      override def nanos(row: GCEvent): Long = row.startTimeNanos
    },
    new TableColumnTime[GCEvent](name = "Duration (ms)", width = 200) {
      override def valueOf(row: GCEvent): Double = row.durationNanos * 1e-6
    },
    new TableColumnLong[GCEvent](name = "Memory Cleared (MB)", width = 220) {
      override def valueOf(row: GCEvent): Long = row.memCleared
    },
    new TableColumnString[GCEvent](name = "Process", width = 220) {
      override def valueOf(row: GCEvent): String = if (row.process eq null) null else row.process.displayName
    }
  )
}
