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

import java.awt.event.MouseEvent
import java.util.prefs.Preferences

import javax.swing.JButton
import javax.swing.JMenuItem
import optimus.profiler.ui.NodeTreeTable
import optimus.profiler.ui.NodeTreeTable.{Mode => TreeMode}
import optimus.profiler.ui.common.JPopupMenu2

object ProfilerTasks {
  private val treeModePref = "treeModePref"
}

class ProfilerTasks(pref: Preferences) extends JButton {
  import ProfilerTasks._
  private val menu = new JPopupMenu2
  private var cur_mode: TreeMode = NodeTreeTable.children

  var onSelectionChanged: TreeMode => Unit = _

  init()

  def getSelectedMode: TreeMode = cur_mode

  private def init(): Unit = {
    val previouslySelected = pref.get(treeModePref, cur_mode.name)
    if (previouslySelected.nonEmpty) {
      val prevMode = NodeTreeTable.modes.find(m => (m ne null) && m.name == previouslySelected)
      if (prevMode.isDefined)
        cur_mode = prevMode.get
    }
    NodeTreeTable.modes.foreach { mode =>
      if (mode eq null) menu.addSeparator()
      else {
        val menuItem = new JMenuItem(mode.name, mode.icon)
        menuItem.setToolTipText(mode.toolTip)
        menuItem.addActionListener(_ => setSelectedMode(mode))
        menu.add(menuItem)
      }
    }
    raiseSelectionChanged()
  }

  def setSelectedMode(mode: TreeMode): Unit = {
    cur_mode = mode
    pref.put(treeModePref, mode.name)
    raiseSelectionChanged()
  }

  private def raiseSelectionChanged(): Unit = {
    setText(cur_mode.name)
    setIcon(cur_mode.icon)
    setToolTipText(cur_mode.toolTip)
    if (onSelectionChanged ne null)
      onSelectionChanged(cur_mode)
  }

  override def processMouseEvent(e: MouseEvent): Unit = {
    if (e.getID == MouseEvent.MOUSE_PRESSED && e.getButton == MouseEvent.BUTTON1) {
      menu.show(this, 0, this.getHeight)
    }
    super.processMouseEvent(e)
  }
}
