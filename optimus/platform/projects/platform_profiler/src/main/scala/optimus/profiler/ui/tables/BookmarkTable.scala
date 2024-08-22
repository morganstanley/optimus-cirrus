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

import java.awt.Color
import optimus.graph.diagnostics.messages.BookmarkCounter
import optimus.profiler.ui.Filterable
import optimus.profiler.ui.NPTimelineTable
import optimus.profiler.ui.NodeTimeLine
import optimus.profiler.ui.TableColumn
import optimus.profiler.ui.TableColumnString
import optimus.profiler.ui.TableColumnUTC
import optimus.profiler.ui.TimelineEvent

import scala.collection.mutable.ArrayBuffer

final case class BookmarkEvent(message: String, startTimeNanos: Long, color: Color) extends TimelineEvent

class BookmarkTable(tline: NodeTimeLine) extends NPTimelineTable[BookmarkEvent](tline) with Filterable[BookmarkEvent] {
  emptyRow = BookmarkEvent("", 0, new Color(255, 255, 255))
  override def initialColumns: ArrayBuffer[TableColumn[BookmarkEvent]] = regularView

  override def updateData(): Unit = {
    val selectedBookmarkEvents = tline.getSelectedEvents(BookmarkCounter)
    val r = selectedBookmarkEvents.map { e => BookmarkEvent(e.message, e.time, new Color(e.color)) }
    setList(r)
  }

  private def regularView: ArrayBuffer[TableColumn[BookmarkEvent]] = ArrayBuffer(
    new TableColumnString[BookmarkEvent](name = "Message", width = 250) {
      override def valueOf(row: BookmarkEvent): String = row.message
    },
    new TableColumnUTC[BookmarkEvent](tline.getReader.nanoToUTC, name = "Time", width = 250) {
      override def nanos(row: BookmarkEvent): Long = row.startTimeNanos
    },
    new TableColumnString[BookmarkEvent](name = "Color", width = 250) {
      override def valueOf(row: BookmarkEvent): String = ""
      override def getCellCustomColor(selected: Boolean, row: BookmarkEvent, row2: BookmarkEvent): Color = row.color
    }
  )
}
