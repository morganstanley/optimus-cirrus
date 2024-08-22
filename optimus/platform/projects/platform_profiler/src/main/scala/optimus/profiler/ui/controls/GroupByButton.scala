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
package optimus.profiler.ui.controls

import optimus.profiler.ProfilerUI

import java.awt.Component
import java.awt.Font
import java.awt.Graphics
import java.awt.Graphics2D
import java.awt.RenderingHints
import java.awt.RenderingHints.KEY_ANTIALIASING
import java.awt.RenderingHints.VALUE_ANTIALIAS_ON
import java.awt.event.MouseEvent
import java.util.prefs.Preferences
import javax.swing.Icon
import javax.swing.JButton
import javax.swing.JMenuItem
import javax.swing.JPopupMenu
import optimus.profiler.ui.common.GroupingAttribute
import optimus.profiler.ui.common.GroupingAttribute.GroupingAttribute
import optimus.profiler.ui.common.JPopupMenu2
import optimus.profiler.ui.common.NodeGroup

import scala.collection.mutable.ArrayBuffer

object GroupByButton {
  private val title = "Group by"
  private val groupByKey = "groupByPref"
}

class GroupByButton(attributes: Seq[String], pref: Preferences) extends JButton {
  import GroupByButton._
  private val menu = new JPopupMenu2
  private val clearSeparator = new JPopupMenu.Separator()
  private val clearCommand = new JMenuItem("Clear All")
  private val checkBoxFont = new Font("Segoe UI", Font.BOLD, 12)
  val selectedAttributes: ArrayBuffer[String] = new ArrayBuffer[String]()
  var onSelectionChanged: () => Unit = _

  class SelectedIcon(attribute: String) extends Icon {
    override def getIconHeight: Int = 16
    override def getIconWidth: Int = 16
    override def paintIcon(c: Component, g: Graphics, x: Int, y: Int): Unit = {
      selectedAttributes.indexOf(attribute) match {
        case -1 => // blank icon if not selected
        case index =>
          val oldFont = g.getFont
          val g2 = g.asInstanceOf[Graphics2D]
          val iconStr = (index + 1).toString
          val sb = checkBoxFont.getStringBounds(iconStr, g2.getFontRenderContext).getBounds
          g.setFont(checkBoxFont)
          g2.setRenderingHints(new RenderingHints(KEY_ANTIALIASING, VALUE_ANTIALIAS_ON))
          g.drawString(iconStr, 15 - sb.width, sb.height - y / 2)
          g.setFont(oldFont)
      }
    }
  }

  private def getToolTip(name: String): String =
    GroupingAttribute.withName(name).asInstanceOf[GroupingAttribute].groupTemplate.toolTip

  init()
  private def init(): Unit = {

    val previouslySelected = if (ProfilerUI.prefDisabledForTesting) "" else pref.get(groupByKey, "")
    if (previouslySelected.nonEmpty) {
      val prevAttributes = previouslySelected.split(",")
      selectedAttributes ++= prevAttributes.filter(attributes.contains(_))
    }
    attributes.foreach { attr =>
      val menuItem = new JMenuItem(attr, new SelectedIcon(attr))
      menuItem.setToolTipText(getToolTip(attr))
      menuItem.addActionListener(_ => {
        if (selectedAttributes.contains(attr))
          selectedAttributes -= attr
        else
          selectedAttributes += attr
        pref.put(groupByKey, selectedAttributes.mkString(","))
        raiseSelectionChanged()
      })
      menu.add(menuItem)
    }
    menu.add(clearSeparator)
    menu.add(clearCommand)
    clearCommand.addActionListener(_ => clearSelection())
    raiseSelectionChanged()
  }

  private def clearSelection(): Unit = {
    selectedAttributes.clear()
    pref.put(groupByKey, "")
    raiseSelectionChanged()
  }

  private def raiseSelectionChanged(): Unit = {
    setText(if (selectedAttributes.isEmpty) title else title + " " + selectedAttributes.mkString(", "))
    clearCommand.setVisible(selectedAttributes.nonEmpty)
    clearSeparator.setVisible(selectedAttributes.nonEmpty)

    if (onSelectionChanged ne null)
      onSelectionChanged()
  }

  override def processMouseEvent(e: MouseEvent): Unit = {
    if (e.getID == MouseEvent.MOUSE_PRESSED && e.getButton == MouseEvent.BUTTON1) {
      menu.show(this, 0, this.getHeight)
    }
    super.processMouseEvent(e)
  }
}
