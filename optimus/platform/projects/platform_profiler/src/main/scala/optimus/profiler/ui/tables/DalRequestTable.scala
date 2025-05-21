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

import optimus.graph.NodeTrace
import optimus.graph.diagnostics.PNodeTask

import java.awt.event.MouseEvent
import java.util.concurrent.TimeUnit
import optimus.graph.diagnostics.messages.DALRequestCounter
import optimus.platform.dal.config.DALEnvs
import optimus.profiler.ui.DbgPrintSource
import optimus.profiler.ui.Filterable
import optimus.profiler.ui.GraphDebuggerUI
import optimus.profiler.ui.NPTable
import optimus.profiler.ui.NodeTimeLine
import optimus.profiler.ui.TableColumn
import optimus.profiler.ui.TableColumnCount
import optimus.profiler.ui.TableColumnString
import optimus.profiler.ui.TableColumnTime
import optimus.profiler.ui.TableColumnUTC
import optimus.profiler.ui.browser.GraphBrowser
import optimus.profiler.ui.common.JPopupMenu2

import java.util
import scala.collection.immutable.ArraySeq
import scala.collection.mutable.ArrayBuffer

final case class DalRequest(
    requestUuid: String,
    env: String,
    startTimeNanos: Long,
    durationNanos: Long,
    batchSize: Int,
    nodeIDs: Array[Int],
    locations: String,
    details: String) {
  val splunkInstance: String = if (requestUuid.isEmpty) DALEnvs.splunkInstanceQa else DALEnvs.splunkInstanceSafe(env)
}

class DalRequestsTable(val tline: NodeTimeLine)
    extends NPTable[DalRequest]
    with DbgPrintSource
    with Filterable[DalRequest] {
  emptyRow = DalRequest("", "", 0, 0, 0, null, "", "")
  override def initialColumns: ArrayBuffer[TableColumn[DalRequest]] = regularView

  dataTable.setComponentPopupMenu(initPopupMenu)

  private def initPopupMenu: JPopupMenu2 = {
    val menu = new JPopupMenu2
    val mi1 = menu.addMenu("Print Source Locations of Query", printSourceWithPopUp(getSelections, dataTable))
    menu.addOnPopup(mi1.setEnabled(getSelections.exists(_.locations.nonEmpty)))
    menu.addMenu("Show DAL Call(s) Backtrace", showDalBackTraceInBrowser(getSelections))
    menu
  }

  private def showDalBackTraceInBrowser(requests: ArraySeq[DalRequest]): Unit = {
    val selectedTaskIds = new util.HashSet[Int]()
    for (r <- requests if r.nodeIDs ne null) {
      for (id <- r.nodeIDs) selectedTaskIds.add(id)
    }
    val nodes = NodeTrace.getTraceBy(n => selectedTaskIds.contains(n.getId), false)
    GraphDebuggerUI.addTab("DAL Backtrace for Selection", new GraphBrowser(nodes))
  }

  // if provided, print query locations
  override def printSource(): Unit = getSelections.foreach { dalRequest =>
    val locations = dalRequest.locations
    if (locations.nonEmpty)
      // need the println to console, not log (used in Debugger)
      println(s"Request UUID ${dalRequest.requestUuid} at source locations:\n$locations")
  }

  /**
   * Enable single click = select cell / double click = select row. This is to allow users to copy the Splunk link
   * easily.
   */
  override def onClick(event: MouseEvent): Unit = {
    if (event.getClickCount == 2 && dataTable.getSelectedRow != -1) {
      dataTable.setCellSelectionEnabled(false)
      dataTable.setRowSelectionAllowed(true)
    } else {
      dataTable.setCellSelectionEnabled(true)
    }
    val selection = getSelection
    if (selection != null) {
      tline.setHighlightRange(selection.startTimeNanos, selection.durationNanos)
    }
  }

  override def updateData(): Unit = {
    val selectedDalRequests = tline.getSelectedEvents(DALRequestCounter)
    val r = selectedDalRequests.map { e =>
      DalRequest(e.reqID, e.env, e.time, e.duration, e.batchSize, e.nodeIDs, e.locations, e.other)
    }
    setList(r)
  }

  private def regularView: ArrayBuffer[TableColumn[DalRequest]] = ArrayBuffer(
    new TableColumnString[DalRequest](name = "Request UUID", width = 250) {
      override def valueOf(row: DalRequest): String = row.requestUuid
    },
    new TableColumnUTC[DalRequest](tline.getReader.nanoToUTC, name = "Start Time", width = 250) {
      override def nanos(row: DalRequest): Long = row.startTimeNanos
    },
    new TableColumnTime[DalRequest](name = "Client Duration (ms)", width = 250) {
      override def valueOf(row: DalRequest): Double =
        TimeUnit.NANOSECONDS.toMillis(row.durationNanos).toDouble // It's a duration, no need for adding offset
    },
    new TableColumnCount[DalRequest](name = "Batch Size", width = 250) {
      override def valueOf(row: DalRequest): Int = row.batchSize
    },
    new TableColumnString[DalRequest](name = "Details", width = 250) {
      override def valueOf(row: DalRequest): String = row.details
    },
    new TableColumnString[DalRequest](name = "Splunk Report Link", width = 250) {
      override def valueOf(row: DalRequest): String = {
        s"${row.splunkInstance}optimus_dal_request_tracer?form.id=reqId&form" +
          s".time.earliest=%40d&form.time.latest=now&form.threads=dsi&form.env=*&form.search_value=${row.requestUuid}"
      }
    }
  )
// no task here, but DbgPrintSource forces us to implement this
  override def task: PNodeTask = null
}
