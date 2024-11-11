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

import java.awt.BasicStroke
import java.awt.Color
import java.awt.Dimension
import java.awt.Font
import java.awt.Graphics
import java.awt.Graphics2D
import java.awt.RenderingHints
import java.awt.event.InputEvent
import java.awt.event.KeyAdapter
import java.awt.event.KeyEvent
import java.awt.event.MouseEvent
import java.awt.event.MouseWheelEvent
import java.util.{ArrayList => JArrayList}
import javax.swing.JPanel
import javax.swing.event.MouseInputAdapter
import optimus.graph.OGTraceCounter
import optimus.graph.OGTraceCounter.CounterId
import optimus.graph.OGTraceReader
import optimus.graph.OGTraceReader.DistributedEvents
import optimus.graph.PThreadContext
import optimus.graph.diagnostics.DbgPreference
import optimus.graph.diagnostics.OGCounterView
import optimus.graph.diagnostics.PNodeTask
import optimus.graph.diagnostics.messages.OGCounter
import optimus.graph.diagnostics.messages.OGEvent
import optimus.graph.diagnostics.messages.OGEventWithComplete
import optimus.profiler.DebuggerUI
import optimus.profiler.ui.common.JPopupMenu2
import org.slf4j.LoggerFactory

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object NodeTimeLine {
  private val log = LoggerFactory.getLogger(NodeTimeLine.getClass)

  private val taskConnector = new BasicStroke(1, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 1.5f, Array(5f), 5f)
  private val threadStroke = new BasicStroke(5f, BasicStroke.CAP_SQUARE, BasicStroke.JOIN_MITER)
  private val counterStroke = new BasicStroke()
  private val counterTitleFont = new Font("Calibri", Font.ITALIC, Math.round(13 * Fonts.multiplier))
  private val oddRowBgColor = new Color(232, 232, 232)
  private val mouseRangeColor = new Color(0, 255, 0, 32)
  private val rangeFillColor = new Color(0, 0, 255, 32)

  // alternate colours between different tasks on the same engine
  private var taskColour = Color.blue
  private def nextColour: Color = {
    if (taskColour eq Color.blue)
      taskColour = Color.green
    else taskColour = Color.blue
    taskColour
  }

  var lastRangeStartAbsNanos = 0L
  var lastRangeEndAbsNanos = 0L
  def timeLabelToNanos(d: Double): Long = (d * 1e6).toLong + PThreadContext.dbgGlobalStart
  def nanosToTime(n: Long): Double = Math.round((n - PThreadContext.dbgGlobalStart) * 1e-4) * 1e-2
  def timeLabel(relTimeNanos: Long): String = f"${relTimeNanos * 1e-6}%1.1f ms"

  def wallTimeRange(threads: JArrayList[PThreadContext], counters: JArrayList[OGCounterView[_]]): (Long, Long) = {
    var minAbsNanos = Long.MaxValue
    var maxAbsNanos = Long.MinValue

    var i = 0
    while (i < threads.size()) {
      val ctx = threads.get(i)
      var si = 0
      while (si < ctx.spans.size()) {
        val span = ctx.spans.get(si)
        if (span.start != span.end) { // possible for nodes with fake clocks
          if (span.start < minAbsNanos) minAbsNanos = span.start
          if (span.end > maxAbsNanos) maxAbsNanos = span.end
        }
        si += 1
      }
      i += 1
    }

    i = 0
    while (i < counters.size()) {
      val ctr = counters.get(i)
      var ei = 0
      while (ei < ctr.events.size()) {
        val event = ctr.events.get(ei)
        val (start, end) = event match {
          case e: OGEventWithComplete => (e.startTime, e.endTime)
          case e: OGEvent             => (e.time, e.time)
          case _                      => (Long.MaxValue, Long.MinValue)
        }

        if (start < minAbsNanos) minAbsNanos = start
        if (end > maxAbsNanos) maxAbsNanos = end
        ei += 1
      }
      i += 1
    }

    (minAbsNanos, maxAbsNanos)
  }

  def getCounter[T <: OGEvent](counters: JArrayList[OGCounterView[_]], counterType: OGCounter[T]): OGCounterView[T] = {
    var i = 0
    while (i < counters.size()) {
      if (counters.get(i).ogtCounter eq counterType)
        return counters.get(i).asInstanceOf[OGCounterView[T]]
      i += 1
    }
    new OGCounterView[T](0, counterType)
  }
}

class NodeTimeLine(private var reader: OGTraceReader) extends JPanel {
  import NodeTimeLine._
  val timeBar = new NodeTimeBar(this)
  private val headerHeight = 0
  private val rowHeight = 20

  private val nodeHeight = 3
  private val nodeHeightMin = 40
  private val nodeStroke = new BasicStroke()

  private val counterRowHeight = 60
  private val counterHeaderHeight = 4
  private val counterTitleHOffSet = 4

  // Global zoom "speed"
  private val percentZoomBy: Double = 0.20

  // Includes counters from which we need data from to generate tables, but that are redundant to plot on the timeline
  val tableOnlyCounters: Seq[Int] = Seq(CounterId.IDLE_GUI_ID.id)

  private[ui] val menu = new JPopupMenu2

  private var threads = new JArrayList[PThreadContext]()
  private var counters = new JArrayList[OGCounterView[_]]()
  private var distributedEvents = new JArrayList[DistributedEvents]()
  private var groupNode: PNodeTask = _
  private var nodes = Array[PNodeTask]()
  private var nodeColors = Array[Color]()
  private var legendHeight = 0

  private[this] var uncheckedCounterIds = mutable.Set[Int]()

  private[this] var lastMouseX = 0

  private[this] var minAbsNanos = Long.MaxValue // Min time for ALL of the data
  private[this] var maxAbsNanos = Long.MinValue // Max time for ALL of the data
  private[this] var visibleMinAbsNanos = minAbsNanos // Visible Min Time
  private[this] var visibleMaxAbsNanos = maxAbsNanos // Visible Max Time
  private[this] var lastClickedX = 0
  private[this] var lastClickedAbsNanos = 0L
  private[this] var lastClickedMinAbsNanos = 0L
  private[this] var mouseRangeStartNanos = 0L
  private[this] var mouseRangeEndNanos = 0L
  private[this] var highlightLineAbsNanos = 0L
  private[this] var highlightRangeStartNanos = 0L
  private[this] var highlightRangeEndNanos = 0L
  private[this] var threadFilter = ""

  private class ViewOption(name: String, title: String, tooltip: String) extends DbgPreference(name, title, tooltip) {
    override protected def onValueChanged(): Unit = repaint()
  }

  val showThreadNames: DbgPreference = new ViewOption("prefShowThreadNames", "Show Thread Names", "Show thread names")
  val showOneLinePerProcess: DbgPreference =
    new ViewOption("prefShowOneLinePerProcess", "Show One Line Process", "Use when looking at distributed compute")
  val distributedEventsAsTask: DbgPreference = new ViewOption(
    "prefDEventsAsTask",
    "Distributed Event as Tasks",
    "Do not draw connecting line from the sending task")
  val linkRemoteTasksToStart: DbgPreference = new ViewOption(
    "prefLinkRemoteTasksToStart",
    "Link Remote Tasks To Start",
    "Draw connections between a task being sent from one process to the point it is received in another process"
  )
  val colorThreadsByProcess: DbgPreference = new ViewOption(
    "prefColorThreadsByProcess",
    "Colour Threads By Process",
    "Change thread background colour based on process")

  private var mouseReleaseCallback = () => ()

  init()

  def getCounter[T <: OGEvent](counterType: OGCounter[T]): OGCounterView[T] =
    NodeTimeLine.getCounter(counters, counterType)

  def selectionStartLabel: String = reader.nanoToUTCTimeOnly(Math.min(mouseRangeStartNanos, mouseRangeEndNanos))

  def getAppStartTimeAbsNanos: Long = {
    minAbsNanos
  }

  /** A = Absolute */
  def getSelectedTimeStartA: Long = {
    if (mouseRangeStartNanos == 0) visibleMinAbsNanos
    else if (mouseRangeStartNanos < mouseRangeEndNanos) mouseRangeStartNanos
    else mouseRangeEndNanos
  }

  /** A = Absolute */
  def getSelectedTimeEndA: Long = {
    if (mouseRangeEndNanos == 0) visibleMaxAbsNanos
    else if (mouseRangeStartNanos < mouseRangeEndNanos) mouseRangeEndNanos
    else mouseRangeStartNanos
  }

  def onMouseRelease(f: () => Unit): Unit = {
    mouseReleaseCallback = f
  }

  private def init(): Unit = {
    setPreferredSize(new Dimension(800, 300))
    setBackground(Color.white)
    setFocusable(true)
    precomputeValues()

    val mia: MouseInputAdapter = new MouseInputAdapter {
      override def mousePressed(e: MouseEvent): Unit = {
        if (e.getButton == MouseEvent.BUTTON1) {
          lastClickedX = e.getX
          lastClickedAbsNanos = xToAbsNanos(lastClickedX)
          lastClickedMinAbsNanos = visibleMinAbsNanos
          mouseRangeStartNanos = 0
          mouseRangeEndNanos = 0
          lastRangeStartAbsNanos = 0
          lastRangeEndAbsNanos = 0
          repaint()

          // Zero-out and clear time bar range-selected values
          timeBar.setData(0, 0, 0, 0, 0, 0)
          timeBar.repaint()
        }
        requestFocusInWindow()
      }

      override def mouseMoved(e: MouseEvent): Unit = {
        lastMouseX = e.getX
        super.mouseMoved(e)
      }

      override def mouseWheelMoved(e: MouseWheelEvent): Unit = if (e.isControlDown) {
        zoomBy(-e.getPreciseWheelRotation)
      }

      private def timeString(nanos: Long): String = {
        val utc = reader.nanoToUTC(nanos)
        val x = absNanosToX(nanos)
        s"nano=$nanos utc=$utc x=$x"
      }

      override def mouseDragged(e: MouseEvent): Unit = {
        if (e.isControlDown) {
          shiftTo(lastClickedMinAbsNanos - fracWidthToNanos(e.getX - lastClickedX))
        } else if ((e.getModifiersEx & InputEvent.BUTTON1_DOWN_MASK) != 0) {
          mouseRangeStartNanos = xToAbsNanos(lastClickedX)
          mouseRangeEndNanos = xToAbsNanos(e.getX)
          lastRangeStartAbsNanos = mouseRangeStartNanos
          lastRangeEndAbsNanos = mouseRangeEndNanos
          repaint()
        }
        GraphConsole.out(s"Range: [${timeString(mouseRangeStartNanos)}] - [${timeString(mouseRangeEndNanos)}]")
      }

      override def mouseReleased(e: MouseEvent): Unit = {
        mouseReleaseCallback()
      }
    }
    val kia = new KeyAdapter {
      override def keyPressed(e: KeyEvent): Unit = {
        if (e.isControlDown) {
          if (e.getKeyChar == '+' || e.getKeyChar == '=') zoomBy(1)
          if (e.getKeyChar == '-' || e.getKeyChar == '_') zoomBy(-1)
        }
      }
    }
    addKeyListener(kia)
    addMouseMotionListener(mia)
    addMouseListener(mia)
    addMouseWheelListener(mia)

    menu.addMenu("Zoom Out", zoomOut())
    menu.addMenu("Zoom to Range", zoomToRange())
    menu.addSeparator()
    menu.addMenu(
      "Remove data to the left", {
        reader.dropLeftOf(lastClickedAbsNanos); refresh(refreshSelectedNodes = false)
      })
    menu.addMenu(
      "Remove data to the right", {
        reader.dropRightOf(lastClickedAbsNanos); refresh(refreshSelectedNodes = false)
      })
    setComponentPopupMenu(menu)
  }

  def getSelectedEvents[T <: OGEvent](counter: OGCounter[T]): ArrayBuffer[T] = {
    val counterView = getCounter(counter)
    counterView.eventsBetween(getSelectedTimeStartA, getSelectedTimeEndA)
  }

  private def selectNodesHeight: Int = if (nodes.length > 0) Math.max(nodes.length * nodeHeight, nodeHeightMin) else 0
  private var lastThreadY: Int = headerHeight
  private var lastCounterY: Int = 0

  /** for positioning split pane divider */
  private def topOfSelectedNodes: Int = lastThreadY
  private def bottomOfSelectedNodes: Int = selectNodesHeight + topOfSelectedNodes

  private def drawSelectedNodes(g: Graphics2D, w: Int): Unit = if (nodes.length > 0) {
    var y = topOfSelectedNodes
    val hScale = getWidth.toDouble / (visibleMaxAbsNanos - visibleMinAbsNanos)

    g.setStroke(nodeStroke)
    g.setColor(Color.lightGray)
    g.drawLine(0, y, w, y)

    g.setColor(Color.darkGray)
    g.setFont(counterTitleFont)
    g.drawString("Selected Nodes", counterTitleHOffSet, bottomOfSelectedNodes - counterRowHeight / 4)

    y += 1
    g.setStroke(nodeStroke)
    var i = 0
    while (i < nodes.length) {
      val node = nodes(i)
      if (GraphDebuggerUI.showInternal.get || !node.isInternal) {
        val color = if (nodeColors != null) nodeColors(i) else Color.blue

        val start = node.firstStartTime
        val completed = node.completedTime
        g.setColor(color)
        g.drawLine(
          ((start - visibleMinAbsNanos) * hScale).toInt,
          y,
          ((completed - visibleMinAbsNanos) * hScale).toInt,
          y)
        y += nodeHeight
      }
      i += 1
    }

    if (groupNode ne null) {
      g.setColor(Color.darkGray)
      g.setStroke(NPTableRenderer.nodeTimeMarksStroke)
      val start = groupNode.firstStartTime
      val completed = groupNode.completedTime

      val x1 = ((start - visibleMinAbsNanos) * hScale).toInt
      val x2 = ((completed - visibleMinAbsNanos) * hScale).toInt

      if (x1 != x2) {
        g.drawLine(x1, topOfSelectedNodes, x1, bottomOfSelectedNodes)
        g.drawLine(x2, topOfSelectedNodes, x2, bottomOfSelectedNodes)
      } else {
        // if start and end are close enough to collapse into one, draw two shorter lines to indicate a very quick
        // group (shorter to distinguish from groups where x1 and x2 differ enough for a visible gap between them)
        val halfway = (topOfSelectedNodes + bottomOfSelectedNodes) / 2
        g.drawLine(x1, topOfSelectedNodes, x1, halfway - 10)
        g.drawLine(x1, halfway + 10, x1, bottomOfSelectedNodes)
      }
    }
  }

  private def drawSampledValueCounter(
      g: Graphics2D,
      counter: OGCounterView[OGEvent],
      bottomY: Int,
      hScale: Double,
      vScale: Double): OGEvent = {
    var eventAtLastClick: OGEvent = null
    val events = counter.events
    val count = events.size()
    val xPoints = new Array[Int](count)
    val yPoints = new Array[Int](count)
    var prevX = 0
    var i = 0
    while (i < count) {
      val event = events.get(i)
      val x = ((event.time - visibleMinAbsNanos) * hScale).toInt
      xPoints(i) = x
      yPoints(i) = bottomY - ((event.graphValue - counter.minValue) * vScale).toInt

      if (lastMouseX >= prevX && lastMouseX <= x) {
        eventAtLastClick = event
        g.drawOval(x - 2, yPoints(i) - 2, 5, 5)
      }
      prevX = x

      i += 1
    }
    g.drawPolyline(xPoints, yPoints, count)
    eventAtLastClick
  }

  private def drawBlockValueCounter(
      g: Graphics2D,
      counter: OGCounterView[OGEvent],
      bottomY: Int,
      hScale: Double,
      vScale: Double): OGEvent = {
    var eventAtLastClick: OGEvent = null
    val xPoints = new Array[Int](4)
    val yPoints = new Array[Int](4)
    val events = counter.events
    val count = events.size()
    var i = 0
    while (i < count) {
      val event = events.get(i).asInstanceOf[OGEventWithComplete]
      val x1 = ((event.startTime - visibleMinAbsNanos) * hScale).toInt
      val x2 = ((event.endTime - visibleMinAbsNanos) * hScale).toInt
      val y = bottomY - ((event.graphValue - counter.minValue) * vScale).toInt

      if (lastMouseX >= x1 && lastMouseX <= x2)
        eventAtLastClick = event

      // Draw little rectangular dome (rectangle without a bottom)
      xPoints(0) = x1
      yPoints(0) = bottomY
      xPoints(1) = x1
      yPoints(1) = y
      xPoints(2) = x2
      yPoints(2) = y
      xPoints(3) = x2
      yPoints(3) = bottomY
      g.drawPolyline(xPoints, yPoints, 4)
      i += 1
    }
    eventAtLastClick
  }

  /**
   * Creates a row in the node time line view in which to draw counters.
   * @param g
   *   graphics to draw on
   * @param width
   *   width of row
   * @param rowCount
   *   row number
   * @return
   *   coordinate on y axis where row ends
   */
  private def drawCounterGroupRow(g: Graphics2D, width: Int, rowCount: Int): Int = {
    val topY = lastThreadY + selectNodesHeight + rowCount * (counterRowHeight + counterHeaderHeight)
    val bottomY = topY + counterHeaderHeight + counterRowHeight

    g.setColor(Color.lightGray)
    g.drawLine(0, bottomY, width, bottomY)

    lastCounterY = bottomY
    bottomY
  }

  def updateChartCounters(ids: mutable.HashSet[Int]): Unit = counters.forEach(c => ids.add(c.id))

  /**
   * Regroups PCounters to be drawn on the same row into a map
   * @return
   *   map where the key is the name of the group of counters and the value is a sequence of counters to be drawn
   *   together
   */
  def getChartGroupings: ListMap[String, Seq[OGCounterView[_]]] = {
    var map: ListMap[String, Seq[OGCounterView[_]]] = ListMap()

    val insertToMap = (key: String, counter: OGCounterView[_]) => {
      if (map.contains(key)) {
        map += (key -> (map(key) :+ counter))
      } else {
        map += (key -> Seq(counter))
      }
    }

    counters.forEach(counter => {
      counter.id match {
        case CounterId.NATIVE_MEMORY_ID.id | CounterId.HEAP_MEMORY_ID.id | CounterId.CACHE_ENTRIES_ID.id =>
          insertToMap("Memory Management", counter)
        case CounterId.GC_ID.id =>
          insertToMap("Garbage Collection", counter)
        case CounterId.CACHE_CLEARS_ID.id =>
          insertToMap("Cache Clears", counter)
        case CounterId.ADAPTED_NODES_ID.id | CounterId.DAL_NODES_ID.id =>
          insertToMap("Nodes", counter)
        case CounterId.SYNC_STACKS_ID.id | CounterId.BLOCKING_ID.id =>
          insertToMap("Waits", counter)
        case CounterId.USER_ACTIONS_ID.id | CounterId.ACTIVE_GUI_ID.id | CounterId.IDLE_GUI_ID.id =>
          insertToMap("GUI Management", counter)
        case _ => insertToMap(counter.description, counter)
      }
    })
    map
  }

  def refreshThreads(search: String): Unit = {
    threadFilter = search
    repaint()
  }

  private def drawBookmarkCounter(
      g: Graphics2D,
      counter: OGCounterView[OGEvent],
      bottomY: Int,
      hScale: Double): OGEvent = {
    var eventAtLastClick: OGEvent = null
    var i = 0
    val count = counter.events.size
    while (i < count) {
      val event = counter.events.get(i)
      val timestamp = event match {
        case e: OGEventWithComplete => e.endTime
        case _                      => event.time
      }
      val x = ((timestamp - visibleMinAbsNanos) * hScale).toInt
      val y1 = bottomY
      val y2 = bottomY - counterRowHeight
      g.drawLine(x, y1, x, y2)

      if (lastMouseX >= x)
        eventAtLastClick = event

      i += 1
    }
    eventAtLastClick
  }

  def drawDiscreteValueCounter(
      g: Graphics2D,
      counter: OGCounterView[OGEvent],
      bottomY: Int,
      hScale: Double,
      vScale: Double): OGEvent = {
    var eventAtLastClick: OGEvent = null
    val count = counter.events.size
    var prevVal = 0L // Should next 3 values come from a counter descriptor?
    var prevX = 0
    var prevY = bottomY

    val xPoints = new Array[Int](count * 2)
    val yPoints = new Array[Int](count * 2)

    var i = 0
    while (i < count) {
      val event = counter.events.get(i)
      val x = ((event.time - visibleMinAbsNanos) * hScale).toInt
      val y = bottomY - ((event.graphValue - counter.minValue) * vScale).toInt
      xPoints(i * 2) = x
      yPoints(i * 2) = prevY
      xPoints(i * 2 + 1) = x
      yPoints(i * 2 + 1) = y
      if (lastMouseX >= prevX && lastMouseX <= x)
        eventAtLastClick = event

      prevVal = event.graphValue
      prevX = x
      prevY = y
      i += 1
    }
    g.drawPolyline(xPoints, yPoints, count * 2)
    eventAtLastClick
  }

  private def drawCounters(g: Graphics2D): Unit = {
    g.setStroke(counterStroke)
    g.setColor(Color.black)
    val w = getWidth
    val hScale = w.toDouble / (visibleMaxAbsNanos - visibleMinAbsNanos)

    val groupIt = getChartGroupings.iterator
    var groupCount = 0
    var lastGroupCount = -1
    var visibleGroupCount = -1
    while (groupIt.hasNext) {
      val group = groupIt.next()
      val counterSet = group._2
      val yScaler = if (counterSet.size > 1) counterRowHeight - 15 else counterRowHeight / 4
      groupCount += 1
      var setIndex = 0
      var bottomY = drawCounterGroupRow(g, w, visibleGroupCount) // last group bottom y (initialise to draw ranges)

      val counterSetIt = counterSet.iterator
      while (counterSetIt.hasNext) {
        val counter = counterSetIt.next().asInstanceOf[OGCounterView[OGEvent]]
        if (!uncheckedCounterIds.contains(counter.id) && !tableOnlyCounters.contains(counter.id)) {
          if (groupCount != lastGroupCount) {
            lastGroupCount = groupCount
            visibleGroupCount += 1
            bottomY = drawCounterGroupRow(g, w, visibleGroupCount)
          }

          val vScale = counterRowHeight.toDouble / (counter.maxValue - counter.minValue)

          g.setColor(new Color(counter.ogtCounter.defaultGraphColor))
          var eventAtLastClick: OGEvent = null

          if (counter.collectorType == OGTraceCounter.SAMPLED_VALUE)
            eventAtLastClick = drawSampledValueCounter(g, counter, bottomY, hScale, vScale)
          else if (counter.collectorType == OGTraceCounter.BLOCK_VALUE)
            eventAtLastClick = drawBlockValueCounter(g, counter, bottomY, hScale, vScale)
          else if (counter.collectorType == OGTraceCounter.BOOKMARK)
            eventAtLastClick = drawBookmarkCounter(g, counter, bottomY, hScale)
          else if (counter.collectorType == OGTraceCounter.DISCRETE_VALUE)
            eventAtLastClick = drawDiscreteValueCounter(g, counter, bottomY, hScale, vScale)

          // actually show counts for adapted nodes
          if (eventAtLastClick ne null) {
            val label = eventAtLastClick.graphLabel(counter.ogtCounter)
            g.drawString(label, counterTitleHOffSet, setIndex + bottomY - yScaler)
          } else {
            g.drawString(counter.description, counterTitleHOffSet, setIndex + bottomY - yScaler)
          }
          setIndex += counterRowHeight / counterSet.size
        }
      }
    }
  }

  def isRangeSelected: Boolean = mouseRangeStartNanos != 0L

  private def drawRange(
      g: Graphics2D,
      w: Int,
      singleLineNS: Long,
      rangeStartNS: Long,
      rangeEndNS: Long,
      rangeBorderColor: Color,
      rangeFillColor: Color): Unit = {

    // Draw single line at a specified time
    if (singleLineNS > 0L) {
      g.setColor(rangeBorderColor)
      val x = absNanosToX(singleLineNS)
      g.drawLine(x, headerHeight, x, lastCounterY)
    }

    // Draw a range encompassing a range of times
    if (rangeStartNS != 0L) {
      val selectionHeight = lastCounterY - headerHeight
      val timeToX = w.toDouble / (visibleMaxAbsNanos - visibleMinAbsNanos)
      val startX = ((-visibleMinAbsNanos + rangeStartNS) * timeToX).toInt
      val endX = ((-visibleMinAbsNanos + rangeEndNS) * timeToX).toInt
      g.setColor(rangeFillColor)
      if (startX > endX)
        g.fillRect(endX, headerHeight, startX - endX, selectionHeight)
      else
        g.fillRect(startX, headerHeight, endX - startX, selectionHeight)

      // Send time range label up to fixed time bar
      timeBar.setData(
        visibleMinAbsNanos,
        visibleMaxAbsNanos,
        mouseRangeStartNanos,
        mouseRangeEndNanos,
        highlightRangeStartNanos,
        highlightRangeEndNanos)
    }
  }

  private def drawMouseRange(g: Graphics2D, w: Int): Unit = {
    drawRange(g, w, lastClickedAbsNanos, mouseRangeStartNanos, mouseRangeEndNanos, Color.darkGray, mouseRangeColor)
  }

  // Highlight range is the highlight that shows up when a user clicks on an event in a table. It highlights where this event occurs
  // It is different from the mouse range, which shows the range the user has currently selected by clicking/dragging the mouse
  def setHighlightRange(timeNanos: Long, rangeNanos: Long): Unit = {
    if (rangeNanos.equals(0L)) {
      highlightLineAbsNanos = timeNanos
      highlightRangeStartNanos = 0L
    } else {
      highlightLineAbsNanos = timeNanos
      highlightRangeStartNanos = timeNanos
      highlightRangeEndNanos = highlightRangeStartNanos + rangeNanos
    }
    repaint()
  }

  private def drawHighlightRange(g: Graphics2D, w: Int): Unit = {
    drawRange(
      g,
      w,
      highlightLineAbsNanos,
      highlightRangeStartNanos,
      highlightRangeEndNanos,
      Color.blue,
      NodeTimeLine.rangeFillColor)
  }

  private def drawLegend(g: Graphics2D): Unit = {
    val h = getHeight
    val strokeLen = 10
    g.setFont(counterTitleFont)
    g.setStroke(threadStroke)
    val fm = g.getFontMetrics
    legendHeight = fm.getHeight
    val y = h - legendHeight / 2
    val textY = h - 2
    val textColor = Color.lightGray
    var x = strokeLen
    val labelMarginX = strokeLen
    var xEnd = x + strokeLen

    g.setColor(Color.green)
    g.drawLine(x, y, xEnd, y)
    g.setColor(textColor)
    g.drawString("Running", xEnd + labelMarginX, textY)
    x = xEnd + 2 * labelMarginX + fm.stringWidth("Running")
    xEnd = x + strokeLen

    g.setColor(Color.pink)
    g.drawLine(x, y, xEnd, y)
    g.setColor(textColor)
    g.drawString("Waiting", xEnd + labelMarginX, textY)
    x = xEnd + 2 * labelMarginX + fm.stringWidth("Waiting")
    xEnd = x + strokeLen

    g.setColor(Color.red)
    g.drawLine(x, y, xEnd, y)
    g.setColor(textColor)
    g.drawString("Sync Waiting", xEnd + labelMarginX, textY)
    x = xEnd + 2 * labelMarginX + fm.stringWidth("Sync Waiting")
    xEnd = x + strokeLen

    g.setColor(Color.cyan)
    g.drawLine(x, y, xEnd, y)
    g.setColor(textColor)
    g.drawString("Spinning", xEnd + labelMarginX, textY)
  }

  private def drawThreadHorizontalLines(g: Graphics2D, w: Int, threads: Seq[PThreadContext]): Unit = {
    g.setColor(oddRowBgColor)
    var hLineY = headerHeight
    if (colorThreadsByProcess.get && threads.nonEmpty) {
      var odd = true
      var lastCtx = threads.head.process
      var rowGroupStart = 0
      var ci = 1
      while (ci < threads.length) {
        val process = threads(ci).process
        if (lastCtx ne process) {
          if (odd)
            g.fillRect(0, headerHeight + rowHeight * rowGroupStart, w, rowHeight * (ci - rowGroupStart))
          rowGroupStart = ci
          lastCtx = process
          odd = !odd
        }
        ci += 1
      }

      if (odd && rowGroupStart != ci)
        g.fillRect(0, headerHeight + rowHeight * rowGroupStart, w, rowHeight * (ci - rowGroupStart))
    } else {
      while (hLineY < lastThreadY) {
        g.fillRect(0, hLineY, w, rowHeight)
        hLineY += 2 * rowHeight
      }
    }
  }

  private def drawVerticalLines(g: Graphics2D, w: Int, h: Int): Unit = {
    var vLineX = 0
    var vLineStep = w / 11
    while (vLineX < w) {
      g.setColor(Color.lightGray)
      g.drawLine(vLineX, headerHeight * 4 / 5, vLineX, h)
      vLineX += vLineStep
    }
  }

  private def drawProcesses(g: Graphics2D, w: Int): Int = {
    g.setFont(counterTitleFont)
    g.setStroke(threadStroke)
    var hLineY = headerHeight

    var ci = 0
    var processIndex = -1
    var process: PThreadContext.Process = null
    while (ci < threads.size()) {
      val ctx = threads.get(ci)
      if (ctx.process != process) {
        process = ctx.process
        processIndex += 1
        val topY = headerHeight + rowHeight * processIndex

        if (processIndex % 2 == 0) {
          g.setColor(oddRowBgColor)
          g.fillRect(0, hLineY, w, rowHeight)
        }
        hLineY += rowHeight

        g.setColor(Color.darkGray)
        g.drawString(process.displayName(), counterTitleHOffSet, topY + rowHeight - counterTitleHOffSet)
      }
      ci += 1
    }
    // Draw the last line to separate from counters
    g.setStroke(counterStroke)
    g.setColor(oddRowBgColor)
    g.drawLine(0, hLineY, w, hLineY)

    hLineY
  }

  private def drawThreads(g: Graphics2D, w: Int, threads: Seq[PThreadContext]): Unit = {
    g.setFont(counterTitleFont)
    g.setStroke(threadStroke)
    val hScale = w.toDouble / (visibleMaxAbsNanos - visibleMinAbsNanos)

    var ci = 0
    while (ci < threads.length) {
      val ctx = threads(ci)
      val topY = headerHeight + rowHeight * ci
      val y = topY + rowHeight / 2

      var si = 0
      while (si < ctx.spans.size()) {
        val span = ctx.spans.get(si)

        var shiftY = 0 // For better x-visibility
        val color = span.`type` match {
          case PThreadContext.SPAN_RUN       => Color.green
          case PThreadContext.SPAN_WAIT      => shiftY = 2; Color.pink
          case PThreadContext.SPAN_WAIT_SYNC => shiftY = 2; Color.red
          case PThreadContext.SPAN_SPIN      => shiftY = -2; Color.cyan
          case _                             => shiftY = 4; Color.black
        }

        val spanEnd = if (span.end != 0) span.end else visibleMaxAbsNanos
        val x = ((span.start - visibleMinAbsNanos) * hScale).toInt
        g.setColor(color)
        g.drawLine(x, y + shiftY, ((spanEnd - visibleMinAbsNanos) * hScale).toInt, y + shiftY)
        si += 1
      }

      if (showThreadNames.get && ctx.name != null) {
        g.setColor(Color.darkGray)
        val name = ctx.name
        g.drawString(name, counterTitleHOffSet, topY + rowHeight - counterTitleHOffSet)
      }

      ci += 1
    }
  }

  private def drawDistributedEvents(g: Graphics2D, w: Int): Unit = {
    val hScale = w.toDouble / (visibleMaxAbsNanos - visibleMinAbsNanos)
    val yShift = rowHeight / 2

    def drawArrow(start: Long, startCtx: PThreadContext, end: Long, endCtx: PThreadContext): Unit = {
      if (startCtx == null || endCtx == null) {
        log.warn("Skipping drawing as at least one context is null")
        return
      }
      val startID = if (showOneLinePerProcess.get) startCtx.process.nickPID - 1 else startCtx.id
      val x1 = ((start - visibleMinAbsNanos) * hScale).toInt
      val y1 = (headerHeight + rowHeight * startID).toInt

      val endID = if (showOneLinePerProcess.get) endCtx.process.nickPID - 1 else endCtx.id
      val x2 = ((end - visibleMinAbsNanos) * hScale).toInt
      val y2 = (headerHeight + rowHeight * endID).toInt

      g.drawLine(x1, y1 + yShift, x2, y2 + yShift)
    }

    def drawLocalArrow(start: Long, startCtx: PThreadContext): Unit = {
      if (startCtx == null) {
        log.warn("Skipping drawing as at least one context is null")
        return
      }
      val x1 = ((start - visibleMinAbsNanos) * hScale).toInt
      val y1 = (headerHeight + rowHeight * startCtx.id).toInt

      g.drawLine(x1, y1 + yShift, x1 - 2, y1 + yShift - 5)
      g.drawLine(x1, y1 + yShift, x1 + 2, y1 + yShift - 5)
    }

    def drawArrowsForSentAndExecutedTasks(e: DistributedEvents): Unit = {
      val endTime1 = if (linkRemoteTasksToStart.get) e.startedTime else e.receivedTime
      val endCtx1 = if (linkRemoteTasksToStart.get) e.startedThreadContext else e.receivedThreadContext

      g.setStroke(counterStroke)
      g.setColor(nextColour)
      drawArrow(e.sentTime, e.sentThreadContext, endTime1, endCtx1)

      if (e.sentTask eq e.receivedTask)
        drawLocalArrow(e.resultReceivedTime, e.resultReceivedThreadContext) // Local execution
      else
        drawArrow(e.completedTime, e.completedThreadContext, e.resultReceivedTime, e.resultReceivedThreadContext)

      g.setStroke(taskConnector)
      drawArrow(endTime1, endCtx1, e.completedTime, e.completedThreadContext)
    }

    def drawExecutedTask(e: DistributedEvents): Unit = {
      g.setStroke(threadStroke)
      g.setColor(Color.green)
      drawArrow(e.startedTime, e.startedThreadContext, e.completedTime, e.startedThreadContext)
    }

    val it = distributedEvents.iterator
    while (it.hasNext) {
      val e = it.next()
      if (distributedEventsAsTask.get)
        drawExecutedTask(e)
      else if ((e.sentTask ne null) && (e.receivedTask ne null))
        drawArrowsForSentAndExecutedTasks(e)
      else
        log.warn("Incomplete data missing task pair")
    }
  }

  override protected def paintComponent(g1: Graphics): Unit = {
    super.paintComponent(g1)
    val w = getWidth
    val h = getHeight
    val g = g1.asInstanceOf[Graphics2D]

    g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)

    if (showOneLinePerProcess.get) {
      drawVerticalLines(g, w, h)
      lastThreadY = drawProcesses(g, w)
    } else {
      val threadsToInclude = mutable.ArrayBuffer[PThreadContext]()
      val threadIt = threads.iterator
      while (threadIt.hasNext) {
        val ctx = threadIt.next()
        val includeThread = threadFilter.isEmpty || ctx.name.contains(threadFilter)
        if (includeThread) threadsToInclude += ctx
      }
      lastThreadY = headerHeight + rowHeight * threadsToInclude.length
      drawThreadHorizontalLines(g, w, threadsToInclude)
      drawVerticalLines(g, w, h)
      drawThreads(g, w, threadsToInclude)
    }

    drawDistributedEvents(g, w)

    drawSelectedNodes(g, w)
    drawCounters(g)
    drawMouseRange(g, w)
    drawHighlightRange(g, w)
    drawLegend(g)

    timeBar.setWidth(w)
    timeBar.repaint()
  }

  private[ui] def xToRelNanos(x: Int): Long =
    visibleMinAbsNanos + (x.toDouble * (visibleMaxAbsNanos - visibleMinAbsNanos) / getWidth).toLong - minAbsNanos
  private[ui] def xToAbsNanos(x: Int): Long =
    visibleMinAbsNanos + (x.toDouble * (visibleMaxAbsNanos - visibleMinAbsNanos) / getWidth).toLong
  private[ui] def fracWidthToNanos(w: Int): Long =
    (w.toDouble * (visibleMaxAbsNanos - visibleMinAbsNanos) / getWidth).toLong
  private def absNanosToX(time: Long): Int =
    (getWidth.toDouble * (time - visibleMinAbsNanos) / (visibleMaxAbsNanos - visibleMinAbsNanos)).toInt

  def zoomOut(): Unit = {
    visibleMinAbsNanos = minAbsNanos
    visibleMaxAbsNanos = maxAbsNanos
    repaint()
  }

  def zoomToRange(): Unit = {
    if (mouseRangeStartNanos == 0L && mouseRangeEndNanos == 0L) return // no selection

    visibleMinAbsNanos = mouseRangeStartNanos
    visibleMaxAbsNanos = mouseRangeEndNanos
    repaint()
  }

  // Zoom in (factor > 0) or out (factor < 0) by some amount. |factor| = 1 corresponds to the "normal" zoom in/out speed.
  //
  // If centerOn is within the view, zooming will also moved the view elastically to it.
  def zoomBy(factor: Double): Unit = {
    val min = visibleMinAbsNanos
    val max = visibleMaxAbsNanos
    val range = max - min
    val changeRange = (range * factor * percentZoomBy).toLong

    // Our zoom center is either the middle of the highlight range (if there is one) or the last click position
    val centerOn = if (mouseRangeStartNanos != 0L && mouseRangeEndNanos != 0L) {
      (mouseRangeStartNanos + mouseRangeEndNanos) / 2
    } else lastClickedAbsNanos

    // On zoom-in, we attempt to center on centerOn, but we don't want the windows to jump all over the place, so we
    // elastically move towards it. If it is within view, we displace our window so that centerOn falls in the middle,
    // but bound by the previous limits, so that we are never seeing more than we did before the zoom.
    //
    // This gives the "expected" behaviour of other graphical applications.
    val displace = if (factor > 0) {
      val center = (min + max) / 2L
      val newCenter = if (centerOn <= max && centerOn >= min) centerOn else center
      val displace = newCenter - center
      if (displace < -changeRange) -changeRange
      else if (displace > changeRange) changeRange
      else displace
    } else 0L

    visibleMinAbsNanos = Math.max(minAbsNanos, min + changeRange + displace)
    visibleMaxAbsNanos = Math.min(max - changeRange + displace, maxAbsNanos)
    repaint()
  }

  private def shiftTo(time: Long): Unit = {
    val span = visibleMaxAbsNanos - visibleMinAbsNanos
    visibleMinAbsNanos = time
    visibleMaxAbsNanos = time + span
    repaint()
  }

  private def precomputeValues(): Unit = {
    val (min, max) = wallTimeRange(threads, counters)
    minAbsNanos = min
    maxAbsNanos = max

    var i = 0
    while (i < nodes.length) {
      val profile = nodes(i)
      if ((profile ne null) && profile.firstStartTime > 0 && profile.completedTime > 0) {
        if (profile.firstStartTime < minAbsNanos) minAbsNanos = profile.firstStartTime
        if (profile.completedTime > maxAbsNanos) maxAbsNanos = profile.completedTime
      }
      i += 1
    }

    visibleMinAbsNanos = minAbsNanos
    visibleMaxAbsNanos = maxAbsNanos
    lastClickedAbsNanos = 0L
    PThreadContext.dbgGlobalStart = minAbsNanos
  }

  // Don't call getSchedulerTimes (and asViewable) every time!!!
  def getChartHeight: Int = {
    reader.getSchedulerTimes(counters, false)
    lastThreadY + counters.size() * counterRowHeight
  }

  def getReader: OGTraceReader = reader

  def setReader(reader: OGTraceReader, uncheckedCounterIds: mutable.Set[Int]): Unit = {
    this.reader = reader
    this.uncheckedCounterIds = uncheckedCounterIds
    refresh(refreshSelectedNodes = true)
  }

  def setSelectedNodes(nodes: Array[PNodeTask], nodeColors: Array[Color], groupNode: PNodeTask): Unit = {
    val repaintAll = this.nodes.length != nodes.length
    this.nodes = nodes
    this.nodeColors = nodeColors
    this.groupNode = groupNode
    if (repaintAll) refresh(refreshSelectedNodes = false)
    else this.repaint(0, lastThreadY, getWidth, selectNodesHeight)
  }

  private def refresh(refreshSelectedNodes: Boolean): Unit = {
    if (refreshSelectedNodes) {
      this.nodes = DebuggerUI.selectedNodes.toArray
      this.nodeColors = null
      this.groupNode = null
    }
    distributedEvents = reader.getDistributedEvents
    setThreadContexts(reader.getSchedulerTimes(counters, false), counters)
  }

  private def setThreadContexts(threads: JArrayList[PThreadContext], counters: JArrayList[OGCounterView[_]]): Unit = {
    this.threads = threads
    this.counters = counters
    precomputeValues()
    setPreferredSize(new Dimension(getWidth, getChartHeight))
    this.getParent.validate()
  }
}
