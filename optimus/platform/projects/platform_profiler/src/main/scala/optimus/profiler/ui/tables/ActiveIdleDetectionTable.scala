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
import optimus.graph.OGTraceCounter
import optimus.graph.diagnostics.messages.ActiveGUICounter
import optimus.profiler.ui.Filterable
import optimus.profiler.ui.NPTableRenderer
import optimus.profiler.ui.NPTimelineTable
import optimus.profiler.ui.NodeTimeLine
import optimus.profiler.ui.TableColumn
import optimus.profiler.ui.TableColumnCount
import optimus.profiler.ui.TableColumnUTC
import optimus.profiler.ui.TimelineEvent

import scala.collection.mutable.ArrayBuffer

/**
 * This file models the data that will be provided to the profiler in regards to active and idle detections within the
 * UIs. It also models the display of said data within the table.
 */
final case class ActiveIdleDetectionEvent(
    id: Int,
    eventTimeNanos: Long,
    idlePeriodSec: Double,
    timeSinceLastChangeSec: Double,
    activePeriodSec: Double,
    eventType: OGTraceCounter.UIState)
    extends TimelineEvent {
  override def startTimeNanos: Long = eventTimeNanos
}

class ActiveIdleDetectionTable(tl: NodeTimeLine)
    extends NPTimelineTable[ActiveIdleDetectionEvent](tl)
    with Filterable[ActiveIdleDetectionEvent] {
  emptyRow = ActiveIdleDetectionEvent(0, 0, 0, 0, 0, OGTraceCounter.UIState.Idle)
  override def initialColumns: ArrayBuffer[TableColumn[ActiveIdleDetectionEvent]] = regularView

  /** Update the data by clicking the tab */
  override def updateData(): Unit = {
    val selected = tline.getSelectedEvents(ActiveGUICounter).map { e =>
      ActiveIdleDetectionEvent(0, e.time, e.activePeriodSec, e.timeSinceLastChangeSec, e.activePeriodSec, e.eventType)
    }
    setList(selected)
  }

  def regularView: ArrayBuffer[TableColumn[ActiveIdleDetectionEvent]] = ArrayBuffer(
    new TableColumnCount[ActiveIdleDetectionEvent](name = "ID", width = 200) {
      override def valueOf(row: ActiveIdleDetectionEvent): Int = row.id
    },
    new TableColumnUTC[ActiveIdleDetectionEvent](tline.getReader.nanoToUTC, name = "Event Time", width = 200) {
      override def nanos(row: ActiveIdleDetectionEvent): Long = row.eventTimeNanos
    },
    new TableColumn[ActiveIdleDetectionEvent](name = "Idle Time (seconds)", width = 200) {
      override def valueOf(row: ActiveIdleDetectionEvent): String =
        if (row.idlePeriodSec != -1) f"${row.idlePeriodSec}%.3f" else ""

      override def getCellCustomColor(
          selected: Boolean,
          row: ActiveIdleDetectionEvent,
          row2: ActiveIdleDetectionEvent): Color =
        if (row.idlePeriodSec == -1) NPTableRenderer.lightGrey else this.getHeaderColor // grey out unused cells
    },
    new TableColumn[ActiveIdleDetectionEvent](name = "Time since last change (seconds)", width = 200) {
      override def valueOf(row: ActiveIdleDetectionEvent): String =
        if (row.timeSinceLastChangeSec != -1) f"${row.timeSinceLastChangeSec}%.3f" else ""

      override def getCellCustomColor(
          selected: Boolean,
          row: ActiveIdleDetectionEvent,
          row2: ActiveIdleDetectionEvent): Color =
        if (row.timeSinceLastChangeSec == -1) NPTableRenderer.lightGrey else this.getHeaderColor
    },
    new TableColumn[ActiveIdleDetectionEvent](name = "Active Period (seconds)", width = 200) {
      override def valueOf(row: ActiveIdleDetectionEvent): String =
        if (row.activePeriodSec != -1) f"${row.activePeriodSec}%.3f" else ""

      override def getCellCustomColor(
          selected: Boolean,
          row: ActiveIdleDetectionEvent,
          row2: ActiveIdleDetectionEvent): Color =
        if (row.activePeriodSec == -1) NPTableRenderer.lightGrey else this.getHeaderColor
    },
    new TableColumn[ActiveIdleDetectionEvent](name = "Event Type", width = 200) {
      override def valueOf(row: ActiveIdleDetectionEvent): String = row.eventType.toString
    }
  )
}
