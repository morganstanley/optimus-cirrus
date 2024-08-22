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
import java.awt.Component
import java.awt.Font
import java.awt.Graphics
import java.awt.Graphics2D
import java.awt.Rectangle
import java.awt.event.MouseEvent
import java.awt.font.FontRenderContext
import java.awt.font.TextAttribute
import java.text.DecimalFormat

import javax.swing.Icon
import javax.swing.ImageIcon
import javax.swing.JTable
import javax.swing.SwingConstants
import javax.swing.UIManager
import javax.swing.border.EmptyBorder
import javax.swing.table.AbstractTableModel
import javax.swing.table.DefaultTableCellRenderer
import optimus.graph.diagnostics.SelectionFlags
import optimus.graph.diagnostics.NPTreeNode
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.profiler.extensions.NPTreeNodeExt
import optimus.profiler.ui.common.JUIUtils
import javax.swing.BorderFactory

object NPTableRenderer {
  final case class TimeFullRange(start: Long, end: Long, markStart: Long, markEnd: Long)
  final case class TimeSubRange(start: Long, end: Long, fullRange: TimeFullRange)
  val emptyTimeFullRange: TimeFullRange = TimeFullRange(0L, 0L, 0L, 0L)
  val emptyTimeSubRange: TimeSubRange = TimeSubRange(0L, 0L, emptyTimeFullRange)

  private def decimalFormatTL(pattern: String): ThreadLocal[DecimalFormat] =
    new ThreadLocal[DecimalFormat] {
      override def initialValue(): DecimalFormat = { new DecimalFormat(pattern) }
    }

  val dataFont = new Font("Calibri", Font.PLAIN, Math.round(13 * Fonts.multiplier))
  val subHeaderFont = new Font("Calibri", Font.BOLD, Math.round(13 * Fonts.multiplier))
  val headerFont = new Font("Calibri", Font.PLAIN, Math.round(18 * Fonts.multiplier))
  val diffFont = new Font("Calibri", Font.ITALIC, Math.round(13 * Fonts.multiplier))
  val timeFormat: DecimalFormat = decimalFormatTL("#,###.00").get()
  val countFormat: DecimalFormat = decimalFormatTL("#,###").get()
  val defaultColor = new Color(255, 255, 255)
  val alternateColor = new Color(250, 250, 255)
  val lightPink = new Color(255, 200, 200)
  val darkPink = new Color(255, 150, 150)
  val lightOrange = new Color(252, 220, 100)
  val disabledRowColor = new Color(156, 183, 255)
  val lightGrey = new Color(230, 230, 230)
  val lightYellow = new Color(255, 254, 200)

  val simpleRender = new TTTableCellRender(dataFont, SwingConstants.LEFT)
  val simpleRenderCenter = new TTTableCellRender(dataFont, SwingConstants.CENTER)
  val timeRenderer = new TimeTableCellRender(dataFont)
  val timeDiffRender = new TimeTableCellRender(diffFont)
  val countRender = new TTTableCellRender(dataFont)
  val sizeRenderer = new TTTableCellRender(dataFont, SwingConstants.RIGHT, true)
  val countDiffRender = new TTTableCellRender(diffFont)
  val flagsRenderer = new FlagTableCellRenderer
  val treeRenderer = new TreeTableCellRenderer
  val timeRangeTableCellRender = new TimeRangeTableCellRender
  val nodeRunStroke = new BasicStroke(2)
  val nodeTimeMarksStroke = new BasicStroke(1, BasicStroke.CAP_BUTT, BasicStroke.JOIN_BEVEL, 0, Array[Float](2), 0)

  private val eicon: Icon = UIManager.getIcon("Tree.expandedIcon")
  private val cicon: Icon = UIManager.getIcon("Tree.collapsedIcon")

  private var lastFontRenderContext: FontRenderContext = _

  class TimeRangeTableCellRender extends DefaultTableCellRenderer {
    private[this] var _v: TimeSubRange = _

    override def paintComponent(_g: Graphics): Unit = {
      super.paintComponent(_g)
      val g = _g.asInstanceOf[Graphics2D]
      // we always call setValue before paintComponent, but "value" can be null when we're rendering the filter row
      // (never otherwise, since TableColumnTimeRange#valueOf always returns some value --- for now)
      if ((_v ne null) && _v.fullRange.start > 0) {
        val c = Color.blue
        val h = getHeight
        val hScale = (getWidth - 2).toDouble / (_v.fullRange.end - _v.fullRange.start)
        // Add left and right margin of 1
        g.setColor(c)
        g.setStroke(nodeRunStroke)
        val xStart = 1 + ((_v.start - _v.fullRange.start) * hScale).toInt
        val xEnd = 1 + ((_v.end - _v.fullRange.start) * hScale).toInt
        val y = h / 2
        g.drawLine(xStart, y, xEnd, y)

        if (_v.fullRange.markStart > 0) {
          g.setColor(Color.lightGray)
          g.setStroke(nodeTimeMarksStroke)
          val x = 1 + ((_v.fullRange.markStart - _v.fullRange.start) * hScale).toInt
          g.drawLine(x, 0, x, h)
        }
        if (_v.fullRange.markEnd > 0) {
          g.setColor(Color.lightGray)
          g.setStroke(nodeTimeMarksStroke)
          val x = 1 + ((_v.fullRange.markEnd - _v.fullRange.start) * hScale).toInt
          g.drawLine(x, 0, x, h)
        }
      }
    }

    override def setValue(value: Any): Unit =
      _v = value match {
        case timeRange: TimeSubRange => timeRange
        case _                       => emptyTimeSubRange
      }
  }

  class TTTableBaseCellRender extends DefaultTableCellRenderer {
    var cellRectangle: Rectangle = _

    override def paint(g: Graphics): Unit = {
      super.paint(g)
      lastFontRenderContext = g.getFontMetrics.getFontRenderContext
    }

    override def getToolTipText(e: MouseEvent): String = {
      val ttip = super.getToolTipText(e)
      if ((ttip ne null) || (lastFontRenderContext eq null) || (cellRectangle eq null)) ttip
      else {
        val text = getText
        val hasNewLine = text.indexOf('\n')
        if (hasNewLine > 0)
          "<html>" + text.replace("\n", "<br>") + "</html>"
        else {
          val sb = getFont.getStringBounds(text, lastFontRenderContext).getBounds
          val borderLeft = getBorder match {
            case eb: EmptyBorder => eb.getBorderInsets.left
            case _               => 0
          }

          if (sb.width > cellRectangle.width - borderLeft) {
            text
          } else null
        }
      }
    }
  }

  // only required if you need tooltips to vary for each cell; otherwise you can set tooltip on the column
  final case class ValueWithTooltip[V <: AnyRef](value: V, tooltip: String) {
    override def toString: String = value.toString
  }

  class TTTableCellRender(font: Font, align: Int = SwingConstants.RIGHT, negativeInRed: Boolean = false)
      extends TTTableBaseCellRender {
    private[this] var _value: AnyRef = _
    override def setValue(value: AnyRef): Unit = {
      val v = value match {
        case ValueWithTooltip(v, t) =>
          setToolTipText(t)
          v
        case v => v
      }
      _value = v
      setFont(font)
      // We create a padding for the column if align is left and right we set the padding to 8px
      setBorder(BorderFactory.createEmptyBorder(0, 8, 0, 8))
      val text = getTextOnSetValue(v)
      setHorizontalAlignment(align)
      setText(text)
    }

    def getTextOnSetValue(value: Any): String = value match {
      case d: Double =>
        if (negativeInRed && d < 0) setForeground(Color.red)
        value.toString
      case i: Int =>
        if (i == 0) "" else countFormat.format(i)
      case l: Long =>
        if (l == 0) "" else countFormat.format(value)
      case a: Array[_] =>
        value.getClass.getComponentType.getName + "[" + a.length + "]"
      case (count: Int, total: Double, subTotal: Double) =>
        if (count > 1) if (Math.abs(subTotal) < 1e-20) "" else timeFormat.format(subTotal)
        else if (Math.abs(total) < 1e-20) ""
        else timeFormat.format(total)
      case (count: Int, total: Int, subTotal: Int) =>
        if (count > 1) if (subTotal == 0) "" else countFormat.format(subTotal)
        else if (total == 0) ""
        else countFormat.format(total)
      case (count: Int, total: Long, subTotal: Long) =>
        if (count > 1) if (subTotal == 0) "" else countFormat.format(subTotal)
        else if (total == 0) ""
        else countFormat.format(total)
      case (count: Int, total, subTotal) =>
        if (count > 1) subTotal.toString else total.toString
      case s: AnyRef =>
        JUIUtils.weakCacheSlowString(s)
      case _ => ""
    }

    override def getTableCellRendererComponent(
        table: JTable,
        value: Any,
        isSelected: Boolean,
        hasFocus: Boolean,
        row: Int,
        column: Int): Component = {
      setForeground(null)
      super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column)
    }

    override def paintComponent(g: Graphics): Unit = {
      _value match {
        case (_, total: Double, subTotal: Double) =>
          val c = new Color(226, 218, 145)
          g.setColor(c)
          g.fillRect(0, 0, (getWidth * subTotal / total).asInstanceOf[Int], 3)
        case (_, total: Int, subTotal: Int) if total != 0 =>
          val c = new Color(226, 218, 145)
          g.setColor(c)
          g.fillRect(0, 0, getWidth * subTotal / total, 3)
        case (_, total: Long, subTotal: Long) if total != 0 =>
          val c = new Color(226, 218, 145)
          g.setColor(c)
          g.fillRect(0, 0, (getWidth * subTotal / total).asInstanceOf[Int], 3)
        case _ =>
      }
      super.paintComponent(g)
    }
  }

  class TimeTableCellRender(font: Font) extends TTTableCellRender(font, SwingConstants.RIGHT, true) {
    override def getTextOnSetValue(value: Any): String = value match {
      case d: Double =>
        if (d < 0) setForeground(Color.red)
        if (Math.abs(d) < 1e-6) ""
        else timeFormat.format(d)
      case _ => super.getTextOnSetValue(value)
    }
  }

  class FlagTableCellRenderer extends DefaultTableCellRenderer {
    override def getTableCellRendererComponent(
        table: JTable,
        value: Any,
        isSelected: Boolean,
        hasFocus: Boolean,
        row: Int,
        column: Int): Component = {
      val component = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column)
      value match {
        case pnti: PNodeTaskInfo =>
          setText(pnti.flagsAsString())
          setToolTipText(pnti.flagsAsStringVerbose())
        case filterText: String =>
          setText(filterText)
          setToolTipText(
            "<html>To filter properties use:<br/>" +
              "<b>=</b> tweakable<br/><b>$</b> cacheable<br/>" +
              "<b>=i</b> tweakable and instance tweaked<br/><b>$</b> cacheable<br/>" +
              "<b>=p</b> tweakable and property tweaked<br/><b>$</b> cacheable<br/>" +
              "<b>t</b> internal transparent nodes<br/><b>$</b> cacheable<br/>" +
              "<b>SI</b> scenario independent<br/>" +
              "<b>SIn</b> scenario independent nodes<br/>" +
              "<br/><b>!</b> to negate next filter and <b>;</b> to OR filters</html>")
        case _ =>
          setText("")
          setToolTipText("")
      }
      component
    }

    override def setValue(value: Any): Unit = {
      // We create a padding for the column if align is left and right we set the padding to 8px
      setBorder(BorderFactory.createEmptyBorder(0, 8, 0, 8))
      super.setValue(value)
    }
  }

  class TreeTableCellRenderer extends TTTableBaseCellRender {
    private[this] var _value: Any = _
    private[this] val horizontalSpace = 1
    private[this] val horizontalLevelSpace = 10
    private[this] val strikethroughFont = getFont.deriveFont {
      val attribs = new java.util.HashMap[TextAttribute, Object]
      attribs.put(TextAttribute.STRIKETHROUGH, TextAttribute.STRIKETHROUGH_ON)
      attribs
    }

    override def getTableCellRendererComponent(
        table: JTable,
        value: Any,
        isSelected: Boolean,
        hasFocus: Boolean,
        row: Int,
        column: Int): Component = {
      _value = value
      val component = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column)
      value match {
        case tnode: NPTreeNode =>
          val ticon = tnode.icon
          val icon = if (ticon ne null) ticon else if (tnode.open) eicon else cicon
          setBorder(
            new EmptyBorder(3, horizontalSpace * 2 + icon.getIconWidth + tnode.level * horizontalLevelSpace, 1, 0))
          if (tnode.font ne null) setFont(tnode.font)

          if (tnode.isCompressed) {
            setForeground(Color.lightGray)
          } else if (!isSelected) {
            val foreground = tnode.foreground
            if (foreground ne null) setForeground(foreground)
            else setForeground(null)
          }

          if (tnode.diffFlag == SelectionFlags.Irrelevant)
            setFont(strikethroughFont)

          if (tnode.diffFlag != SelectionFlags.NotFlagged)
            setToolTipText(SelectionFlags.toToolTipText(tnode.diffFlag))
          else {
            val t = tnode.tooltipText
            if (t ne null)
              setToolTipText(t)
          }

          val background = tnode.background
          if (background ne null) setBackground(background)
          else if (!isSelected) setBackground(null)

          setText(tnode.title(table))
        case _ => // don't overwrite already setText (e.g. filter row in NodeTreeTable name column will then be overwritten)
      }
      component
    }

    override def paintComponent(g: Graphics): Unit = {
      super.paintComponent(g)
      _value match {
        case tnode: NPTreeNode if tnode.hasChildren || tnode.isCompressed || tnode.icon != null =>
          val ticon = tnode.icon
          val icon = if (ticon ne null) ticon else if (tnode.open) eicon else cicon
          val x = horizontalSpace + tnode.level * horizontalLevelSpace
          val h = getHeight
          val y = (h - icon.getIconHeight) / 2
          if (tnode.isCompressed)
            g.drawString("...", x, y + icon.getIconHeight)
          else
            icon.paintIcon(this, g, x, y)
        case _ =>
      }
    }

    def uncompress[T <: NPTreeNode](table: NPSubTableData[T], row: Int, ctrlDown: Boolean): Unit = {
      val rows = table.Rows
      val model = table.getModel.asInstanceOf[AbstractTableModel]
      val modelRow = table.convertRowIndexToModel(row)
      val tnode = table.getTRow(row)
      val children = tnode.getChildren
      val allNodes = NPTreeNodeExt.expandTree(children, forceOpen = false)
      rows.insertAll(modelRow + 1, allNodes)
      rows.remove(modelRow)
      model.fireTableRowsUpdated(row, row)
      if (allNodes.length > 1)
        model.fireTableRowsInserted(modelRow + 1, modelRow + allNodes.length - 1)
    }

    def collapse[T <: NPTreeNode](table: NPSubTableData[T], row: Int, col: Int, ctrlDown: Boolean): Unit = {
      val tnode = table.getTRow(row)
      if (tnode.open && tnode.hasChildren) {
        val modelRow = table.convertRowIndexToModel(row)
        tnode.open = false
        val firstChild = modelRow + 1
        var i = firstChild
        val rows = table.Rows
        while (i < rows.length && rows(i).level > tnode.level) i += 1
        rows.remove(firstChild, i - firstChild)
        val model = table.getModel.asInstanceOf[AbstractTableModel]
        model.fireTableRowsDeleted(firstChild, i - 1)
        model.fireTableCellUpdated(row, col)
      }
    }

    def expand[RType <: NPTreeNode](table: NPSubTableData[RType], row: Int, col: Int, ctrlDown: Boolean): Unit = {
      val tnode = table.getTRow(row)
      if (!tnode.open && tnode.hasChildren) {
        val modelRow = table.convertRowIndexToModel(row)
        tnode.open = true
        // if ctrlDown, force open all child nodes
        val addNodes = NPTreeNodeExt.expandTree(tnode.getChildren, ctrlDown)
        table.Rows.insertAll(modelRow + 1, addNodes)
        val model = table.getModel.asInstanceOf[AbstractTableModel]
        model.fireTableRowsInserted(modelRow + 1, modelRow + addNodes.length)
        model.fireTableCellUpdated(row, col)
      }
    }

    def click[T <: NPTreeNode](table: NPSubTableData[T], row: Int, col: Int, e: MouseEvent): Unit = {
      val tnode = table.getTRow(row)
      val ctrlMask = e.isControlDown
      // Any double clicking on a compressed row will expand it
      if (tnode.isCompressed && e.getClickCount == 2) {
        uncompress(table, row, ctrlMask)
      } else {
        val ticon = tnode.icon
        val icon = if (ticon ne null) ticon else if (tnode.open) eicon else cicon
        val r = table.getCellRect(row, col, true)
        val x = r.x + horizontalSpace + tnode.level * horizontalLevelSpace
        val y = r.y + (r.height - icon.getIconHeight) / 2
        val ir = new Rectangle(x, y, icon.getIconWidth, icon.getIconHeight)

        if (ir.contains(e.getPoint) || e.getClickCount == 2) {
          if (tnode.isCompressed)
            uncompress(table, row, ctrlMask)
          else if (tnode.hasChildren) {
            if (tnode.open)
              collapse(table, row, col, ctrlMask)
            else
              expand(table, row, col, ctrlMask)
          }
        }
      }
    }
  }
}
