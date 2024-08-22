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

import java.awt.Color
import java.awt.Component
import java.awt.Dimension
import java.awt.event.ActionEvent
import java.util.prefs.Preferences

import javax.swing.JComboBox
import javax.swing.JList
import javax.swing.JOptionPane
import javax.swing.ListCellRenderer
import optimus.profiler.ui.Fonts
import optimus.profiler.ui.controls.NodeSelectorComboBox.ExampleString
import NodeSelectorComboBox._

object NodeSelectorComboBox {
  final class ExampleString(val str: String) {
    def length: Int = str.length
    override def toString: String = str
  }

  private val prevFilterKey = "prevFilter"
  val magicBreak = "\u00A6"
  private val maxItemCount = 20

  val examplesForBrowser = Array(
    new ExampleString("// Example queries:"),
    new ExampleString("platform // gets all nodes relevant to `platform'"),
    new ExampleString("r'.*send // gets all nodes which end in `send'"),
    new ExampleString("#5 // go to node 5"),
    new ExampleString("%5 // gets all nodes with profiler id 5"),
    new ExampleString("platform | java // gets all nodes that have anything to do with either `platform' or `java'"),
    new ExampleString("platform & java // gets all nodes that have anything to do with both `platform' and `java'"),
    new ExampleString("!platform // gets all nodes that do NOT contain `platform'"),
    new ExampleString("!r'.*send // gets all nodes which do NOT end in `send'"),
    new ExampleString("class:TemporalSource // gets all nodes with class names containing `TemporalSource'"),
    new ExampleString("class:!TemporalSource // gets all nodes with class names NOT containing `TemporalSource'"),
    new ExampleString("class:r'optimus.platform.* // gets all nodes whose class names begin with `optimus.platform'")
  )
  val toolTipForBrowser: String = "<html>Search supports:" +
    "<li>Plain text</li><li>Regular expressions (prefix: r')</li><li>PNodeTask ID (prefix: %)</li>" +
    "<br>Queries support <i>logical operators</i> & and |. " +
    "<br>Nodes can be <b>selected</b> using <i>node index selection</i> syntax (prefix: #) " +
    "followed by the index number</html>"

  val examplesForTree = Array(
    new ExampleString("//node"),
    new ExampleString("//nodeName/nodeName2")
  )
  val toolTipForTree: String =
    "<html>XPath style selector (or search for [EX] to show paths to nodes with exceptions)</html>"
}

class NodeSelectorComboBox(pref: Preferences, examples: Array[ExampleString], toolTip: String, defaultInitValue: String)
    extends JComboBox[Object] {

  var onFilterUpdate: String => Unit = _
  var onNodeSelectedByID: Int => Unit = _

  init()
  private def init(): Unit = {
    setToolTipText(toolTip)

    val initValues = pref.get(prevFilterKey, defaultInitValue).split(magicBreak)
    initValues.filter(_.nonEmpty).foreach(addItem)
    examples.foreach(addItem)
    if (initValues.isEmpty || initValues(0) == "")
      setSelectedIndex(-1)

    setRenderer(new ListCellRenderer[Object] {
      val original: ListCellRenderer[Object] = getRenderer.asInstanceOf[ListCellRenderer[Object]]
      override def getListCellRendererComponent(
          list: JList[_ <: Object],
          value: Object,
          index: Int,
          isSelected: Boolean,
          cellHasFocus: Boolean): Component = {
        val highlighted = value.toString
          .replaceAll("//.*", "<font color=green>$0</font>")
          .replaceAll("`.*'", "<i>$0</i>")
          .replaceAll("r'", "<font color=blue>$0</font>")
          .replaceAll("class:", "<font color=purple>$0</font>")
          .replaceAll("!", "<font color=red>$0</font>")
          .replaceAll("[&|]+", "<b>$0</b>")
        val doctoredVal = s"<html>$highlighted</html>"
        val c = original
          .getListCellRendererComponent(list.asInstanceOf[JList[Object]], doctoredVal, index, isSelected, cellHasFocus)
        if (value.isInstanceOf[NodeSelectorComboBox.ExampleString])
          c.setForeground(Color.gray)
        c
      }
    })

    addActionListener((e: ActionEvent) =>
      if (!e.getSource.asInstanceOf[JComboBox[Object]].isPopupVisible) {
        val filterText = if (getSelectedItem eq null) "" else getSelectedItem.toString
        storePreviousFilter(filterText)
        if (filterText.startsWith("#")) {
          val nodeId = filterText.substring(1)
          try {
            if (onNodeSelectedByID ne null)
              onNodeSelectedByID(nodeId.toInt)
          } catch {
            case _: NumberFormatException =>
              JOptionPane.showMessageDialog(this, "Invalid parameter: not an integer")
            case _: IndexOutOfBoundsException =>
              JOptionPane.showMessageDialog(this, "Non-existent node ID")
          }
        } else {
          if (onFilterUpdate ne null) onFilterUpdate(filterText)
        }
      })
    setEditable(true)
    setMaximumSize(new Dimension(Integer.MAX_VALUE, Math.round(22 * Fonts.multiplier)))
    setPreferredSize(new Dimension(100, Math.round(22 * Fonts.multiplier)))
  }

  def storePreviousFilter(newEntry: String): Unit = {
    removeItem(newEntry)
    insertItemAt(newEntry, 0)
    setSelectedIndex(0)

    var i = 0
    var storeUpdate = ""
    while (i < Math.min(maxItemCount, getItemCount)) {
      val item = getItemAt(i)
      if (item.isInstanceOf[String]) {
        storeUpdate += "" + item + magicBreak
      }
      i += 1
    }
    pref.put(prevFilterKey, storeUpdate)
  }
}

class NodeFilterComboBox(pref: Preferences)
    extends NodeSelectorComboBox(pref, examplesForBrowser, toolTipForBrowser, "") {
  def getCurrentFilter: String = if (getSelectedItem eq null) "" else getSelectedItem.toString
}

class NodeTreeFilterComboBox(pref: Preferences) extends NodeSelectorComboBox(pref, examplesForTree, toolTipForTree, "")
