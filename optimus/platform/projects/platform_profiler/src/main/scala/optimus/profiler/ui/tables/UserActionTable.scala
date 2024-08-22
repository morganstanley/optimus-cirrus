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

import java.awt.event.MouseEvent
import optimus.graph.OGTraceCounter
import optimus.graph.diagnostics.messages.UserActionCounter
import optimus.profiler.ui.Filterable
import optimus.profiler.ui.NPTable
import optimus.profiler.ui.NodeTimeLine
import optimus.profiler.ui.TableColumn
import optimus.profiler.ui.TableColumnCount
import optimus.profiler.ui.TableColumnUTC

import java.awt.event.KeyEvent
import scala.collection.mutable.ArrayBuffer

/**
 * This file models the data that will be provided to the profiler in regards to active and idle detections within the
 * the GUI .It also models the display of said data within the table.
 *
 * Currently, if a log message is too long, it is simply cut and three dots (...) are appended at the end of the message
 * to indicate that the message was truncated.
 */
final case class UserActionEvent(id: Int, endTimeNanos: Long, description: String)

class UserActionTable(val tline: NodeTimeLine) extends NPTable[UserActionEvent] with Filterable[UserActionEvent] {
  emptyRow = UserActionEvent(0, 0, "")
  override def initialColumns: ArrayBuffer[TableColumn[UserActionEvent]] = regularView

  override def onClick(event: MouseEvent): Unit = updateSelection()

  override def onKeyReleased(e: KeyEvent): Unit = updateSelection()

  private def updateSelection(): Unit = {
    val selection = getSelection
    if (selection != null) {
      tline.setHighlightRange(selection.endTimeNanos, 0)
    }
  }

  /**
   * Update the data by clicking the tab
   */
  override def updateData(): Unit = {
    val selectedUserActionEvents = tline.getSelectedEvents(UserActionCounter)
    val r = selectedUserActionEvents.map { e => UserActionEvent(0, e.time, e.description) }
    setList(r)
  }

  private def regularView: ArrayBuffer[TableColumn[UserActionEvent]] = ArrayBuffer(
    new TableColumnCount[UserActionEvent](name = "ID", width = 150) {
      override def valueOf(row: UserActionEvent): Int = row.id
    },
    new TableColumnUTC[UserActionEvent](tline.getReader.nanoToUTC, name = "Time", width = 150) {
      override def nanos(row: UserActionEvent): Long = row.endTimeNanos
    },
    new TableColumn[UserActionEvent](name = "User Action Message", width = 400) {
      val maxLength = 200

      override def valueOf(row: UserActionEvent): String = {
        if (row.description.length > maxLength) row.description.substring(0, maxLength) + "..." else row.description
      }
    }
  )
}
