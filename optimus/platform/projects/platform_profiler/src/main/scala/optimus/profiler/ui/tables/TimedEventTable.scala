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
import optimus.graph.diagnostics.messages.BlockingWaitCounter
import optimus.graph.diagnostics.messages.BlockingWaitEvent
import optimus.graph.diagnostics.messages.EventDescription
import optimus.graph.diagnostics.messages.HandlerStepEvent
import optimus.graph.diagnostics.messages.HandlerStepEventCounter
import optimus.graph.diagnostics.messages.OGCounter
import optimus.graph.diagnostics.messages.StartupEvent
import optimus.graph.diagnostics.messages.StartupEventCounter
import optimus.graph.diagnostics.messages.TimedEvent
import optimus.profiler.ui.DbgPrintSource
import optimus.profiler.ui.Filterable
import optimus.profiler.ui.GraphDebuggerUI
import optimus.profiler.ui.NPTable
import optimus.profiler.ui.NPTableRenderer.TimeSubRange
import optimus.profiler.ui.NPTimelineTable
import optimus.profiler.ui.NodeTimeLine
import optimus.profiler.ui.NodeTreeTable.recomputeMinMaxTime
import optimus.profiler.ui.TableColumn
import optimus.profiler.ui.TableColumnString
import optimus.profiler.ui.TableColumnTime
import optimus.profiler.ui.TableColumnTimeRange
import optimus.profiler.ui.TableColumnUTC
import optimus.profiler.ui.TimelineEvent
import optimus.profiler.ui.browser.GraphBrowser
import optimus.profiler.ui.common.JPopupMenu2

import scala.collection.mutable.ArrayBuffer

class TimedEventView(val event: EventDescription, override val startTimeNanos: Long, override val durationNanos: Long)
    extends TimelineEvent

object TimedEventView {
  def apply(event: EventDescription, startTimeNanos: Long, durationNanos: Long): TimedEventView =
    new TimedEventView(event, startTimeNanos, durationNanos)
}

/** Concrete instances just need to supply the counter */
abstract class TimedEventTable[T <: TimedEvent, V <: TimedEventView](tl: NodeTimeLine, counter: OGCounter[T])
    extends NPTimelineTable[V](tl)
    with DbgPrintSource
    with Filterable[V] {

  emptyRow = createEmptyRow
  dataTable.setComponentPopupMenu(initPopupMenu)
  override def initialColumns: ArrayBuffer[TableColumn[V]] = regularView

  protected def createEmptyRow: V

  protected def initPopupMenu: JPopupMenu2 = {
    val menu = new JPopupMenu2
    menu.addMenu("Print Source", printSourceWithPopUp(getSelections, dataTable))
    menu
  }

  override def printSource(): Unit = getSelections.foreach { e =>
    if (e.event.sourceLocation.nonEmpty)
      // need the println to console, not log (used in Debugger)
      println(s"Event ${e.event.description} at source location: ${e.event.sourceLocation}")
  }

  protected def createEventView(event: T): V

  override def updateData(): Unit = {
    val r = tline.getSelectedEvents(counter).map { e =>
      createEventView(e)
    }
    setList(r)
  }

  protected def regularView: ArrayBuffer[TableColumn[V]] = ArrayBuffer(
    new TableColumnString[V](name = "Event Type", width = 200) {
      override def valueOf(row: V): String = row.event.description
    },
    new TableColumnUTC[V](tline.getReader.nanoToUTC, name = "Start Time", width = 200) {
      override def nanos(row: V): Long = row.startTimeNanos
    },
    new TableColumnTime[V](name = "Duration (ms)", width = 200) {
      override def valueOf(row: V): Double = row.durationNanos * 1e-6
    },
    new TableColumnTimeRange[V]("Time Frame", 100) {
      override def valueOf(row: V): TimeSubRange = {
        val firstAndLastTimes = rows.map { e => (e.startTimeNanos, e.endTimeNanos) }
        val fullRange = recomputeMinMaxTime(firstAndLastTimes)
        TimeSubRange(row.startTimeNanos, row.endTimeNanos, fullRange)
      }
      override def computeSummary(table: NPTable[V], indexes: Seq[Int]): AnyRef = "" // Do not aggregate
    }
  )
}

object TimedEventTable {
  val empty: TimedEventView = TimedEventView(EventDescription.empty, 0, 0)
  def basicView(e: TimedEvent): TimedEventView = TimedEventView(e.event, e.startTime, e.duration)
}

class StartupEventTable(tl: NodeTimeLine)
    extends TimedEventTable[StartupEvent, TimedEventView](tl, StartupEventCounter) {
  override def createEmptyRow: TimedEventView = TimedEventTable.empty
  override def createEventView(e: StartupEvent): TimedEventView = TimedEventTable.basicView(e)
  // no task here, but DbgPrintSource forces us to implement this
  override def task: PNodeTask = null
}

class HandlerStepEventTable(tl: NodeTimeLine)
    extends TimedEventTable[HandlerStepEvent, TimedEventView](tl, HandlerStepEventCounter) {
  override def createEmptyRow: TimedEventView = TimedEventTable.empty
  override def createEventView(e: HandlerStepEvent): TimedEventView = TimedEventTable.basicView(e)
  // no task here, but DbgPrintSource forces us to implement this
  override def task: PNodeTask = null
}

final case class BlockingWaitEventView(
    threadName: String,
    override val event: EventDescription,
    override val startTimeNanos: Long,
    override val durationNanos: Long,
    jstack: Exception,
    awaitedTaskID: Int,
    endOfChainTaskID: Int)
    extends TimedEventView(event, startTimeNanos, durationNanos)

class BlockingWaitTable(tl: NodeTimeLine)
    extends TimedEventTable[BlockingWaitEvent, BlockingWaitEventView](tl, BlockingWaitCounter)
    with DbgPrintSource
    with Filterable[BlockingWaitEventView] {
  override def createEmptyRow: BlockingWaitEventView =
    BlockingWaitEventView("", EventDescription.empty, 0, 0, null, 0, 0)
  override def createEventView(e: BlockingWaitEvent): BlockingWaitEventView =
    BlockingWaitEventView(e.threadName, e.event, e.startTime, e.duration, e.jstack, e.awaitedTaskID, e.endOfChainTaskID)

  override def initialColumns: ArrayBuffer[TableColumn[BlockingWaitEventView]] = {
    val threadColumn = new TableColumnString[BlockingWaitEventView](name = "Thread", width = 200) {
      override def valueOf(row: BlockingWaitEventView): String = row.threadName
      override def toolTip: String = {
        "Thread blocked by awaited task (consider filtering out auxiliary threads since they don't block the UI)"
      }
    }
    regularView :+ threadColumn
  }
  override def printSource(): Unit = getSelections.foreach { e =>
    // need the println to console, not log (used in Debugger)
    if (e.jstack ne null) e.jstack.printStackTrace()
  }

  override def initPopupMenu: JPopupMenu2 = {
    val menu = new JPopupMenu2
    menu.addMenu("Print Source", printSourceWithPopUp(getSelections, dataTable))
    menu.addMenu("Show Awaited Task in Browser", find(getSelections.map(_.awaitedTaskID)))
    menu.addMenu("Show End of Wait Chain Task in Browser", find(getSelections.map(_.endOfChainTaskID)))
    menu
  }
//685195, 706240
  private def find(ids: Seq[Int]): Unit = {
    val nodes = NodeTrace.getTraceBy(n => ids.contains(n.getId), false)
    GraphDebuggerUI.addTab("Selected Tasks", new GraphBrowser(nodes))
  }
  // no task here, but DbgPrintSource forces us to implement this
  override def task: PNodeTask = null
}
