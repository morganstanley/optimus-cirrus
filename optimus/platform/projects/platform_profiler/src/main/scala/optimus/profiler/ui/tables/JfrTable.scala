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

import one.jfr.JfrReader
import optimus.graph.AsyncProfilerIntegration
import optimus.profiler.ui.NPTable
import optimus.profiler.ui.NodeTimeLine
import optimus.profiler.ui.TableColumn
import optimus.profiler.utils.JFRTop

import scala.collection.mutable.ArrayBuffer

/**
 * This is the table for information provided by the Java Flight Recorder. It fills a table with the most sampled
 * methods for a given time frame.
 */
final case class JfrEvent(methodName: String, timesSampled: Long)

// refactor to use OGEventCounter
class JfrTable(val tline: NodeTimeLine) extends NPTable[JfrEvent] {
  emptyRow = JfrEvent("", 0)
  setView(JfrTable.regularView)

  /**
   * Displays the 50 most sampled functions in the JFR Table for the selected time frame.
   */
  override def updateData(): Unit = {
    val jfrEventsBuffer = ArrayBuffer[JfrEvent]()
    val numRows = 50
    AsyncProfilerIntegration.getTraceJfrFile().foreach { fname =>
      JFRTop
        .top(
          new JfrReader(fname),
          numRows,
          tline.getSelectedTimeStartA,
          tline.getSelectedTimeEndA,
          tline.getAppStartTimeAbsNanos)
        .foreach { case (methodName, timesSampled) =>
          jfrEventsBuffer += JfrEvent(methodName, timesSampled)
        }
      setList(jfrEventsBuffer.sortBy(_.timesSampled).reverse)
    }
  }
}

object JfrTable {
  val regularView: ArrayBuffer[TableColumn[JfrEvent]] = ArrayBuffer(
    new TableColumn[JfrEvent](name = "Method name", width = 600) {
      override def valueOf(row: JfrEvent): String = row.methodName
    },
    new TableColumn[JfrEvent](name = "Number of times sampled", width = 200) {
      override def valueOf(row: JfrEvent): Long = row.timesSampled
    }
  )
}
