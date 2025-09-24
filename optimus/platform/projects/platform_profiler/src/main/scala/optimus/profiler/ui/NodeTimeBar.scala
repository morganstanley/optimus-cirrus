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

import java.awt.Color
import java.awt.Font
import java.awt.Graphics
import java.awt.Graphics2D
import java.awt.RenderingHints

import javax.swing.JPanel

class NodeTimeBar(tline: NodeTimeLine) extends JPanel {
  private[this] var _width = getWidth
  private val headerHeight = 40

  private val counterValueFont = new Font("Consolas", Font.PLAIN, Math.round(10 * Fonts.multiplier))

  private[this] var visibleMinAbsNanos = Long.MaxValue // Visible Min Time
  private[this] var visibleMaxAbsNanos = Long.MinValue // Visible Max Time
  private[this] var mouseRangeStartNanos = 0L
  private[this] var mouseRangeEndNanos = 0L
  private[this] var highlightRangeStartNanos = 0L
  private[this] var highlightRangeEndNanos = 0L

  def timeLabel(relTimeNanos: Long): String = f"${relTimeNanos * 1e-6}%1.1f ms"
  private def utcTimeLabel(absTimeNanos: Long): String = tline.getReader.nanoToUTCTimeOnly(absTimeNanos)
  def selectedTimeRangeLabel: String = timeLabel(Math.abs(mouseRangeEndNanos - mouseRangeStartNanos))
  private def highlightedTimeRangeLabel: String = timeLabel(Math.abs(highlightRangeEndNanos - highlightRangeStartNanos))

  def setData(
      _visibleMinAbsNanos: Long,
      _visibleMaxAbsNanos: Long,
      _mouseRangeStartNanos: Long,
      _mouseRangeEndNanos: Long,
      _highlightRangeStartNanos: Long,
      _highlightRangeEndNanos: Long): Unit = {
    visibleMinAbsNanos = _visibleMinAbsNanos
    visibleMaxAbsNanos = _visibleMaxAbsNanos
    mouseRangeStartNanos = _mouseRangeStartNanos
    mouseRangeEndNanos = _mouseRangeEndNanos
    highlightRangeStartNanos = _highlightRangeStartNanos
    highlightRangeEndNanos = _highlightRangeEndNanos
  }

  def setWidth(w: Int): Unit = {
    _width = w
  }

  private def drawTimeAxis(g: Graphics2D, w: Int, h: Int): Unit = {
    val metrics = g.getFontMetrics
    var vLineX = 0
    var vLineStep = w / 11
    while (vLineX < w) {
      val xLabel = utcTimeLabel(tline.xToAbsNanos(vLineX))
      val xRelLabel = timeLabel(tline.xToRelNanos(vLineX))
      val labelWidth = metrics.stringWidth(xLabel)
      g.setColor(Color.darkGray)
      g.drawString(xLabel, if (vLineX == 0) 0 else vLineX - labelWidth / 2, headerHeight - 2 * metrics.getHeight)
      g.drawString(xRelLabel, if (vLineX == 0) 0 else vLineX - labelWidth / 2, headerHeight - metrics.getHeight)

      vLineX += vLineStep
    }
  }

  private def drawMouseRangeText(g: Graphics2D, w: Int): Unit = {
    drawRangeText(g, w, mouseRangeStartNanos, Color.darkGray, selectedTimeRangeLabel)
  }

  private def drawHighlightRangeText(g: Graphics2D, w: Int): Unit = {
    drawRangeText(g, w, highlightRangeStartNanos, Color.blue, highlightedTimeRangeLabel)
  }

  private def drawRangeText(
      g: Graphics2D,
      w: Int,
      rangeStartNS: Long,
      rangeTextColor: Color,
      selectedTimeLabel: String): Unit = {

    // Write millisecond range of selected time
    if (rangeStartNS != 0L) {
      val timeToX = w.toDouble / (visibleMaxAbsNanos - visibleMinAbsNanos)
      val startX = ((-visibleMinAbsNanos + rangeStartNS) * timeToX).toInt

      g.setColor(rangeTextColor)
      g.setFont(counterValueFont)
      g.drawString(selectedTimeLabel, startX, headerHeight - 2)
    }
  }

  override protected def paintComponent(g1: Graphics): Unit = {
    super.paintComponent(g1)
    val w = _width
    val h = getHeight
    val g = g1.asInstanceOf[Graphics2D]
    g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)

    drawTimeAxis(g, w, h)
    drawMouseRangeText(g, w)
    drawHighlightRangeText(g, w)
  }
}
