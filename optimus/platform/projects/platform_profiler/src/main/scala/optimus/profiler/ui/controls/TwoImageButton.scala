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

import optimus.profiler.ui.Icons

import java.awt.event.MouseEvent
import java.util.prefs.Preferences
import javax.swing.Icon
import javax.swing.ImageIcon
import javax.swing.JButton

object SingleImageButton {
  def createRefreshButton(toolTip: String = "Refresh", enabled: Boolean = true)(action: => Unit): JButton =
    createButton(toolTip, enabled, Icons.refresh, action)

  def createResetButton(toolTip: String = "Reset", enabled: Boolean = true)(action: => Unit): JButton =
    createButton(toolTip, enabled, Icons.reset, action)

  private def createButton(toolTip: String, enabled: Boolean, icon: ImageIcon, action: => Unit): JButton = {
    val btn = new JButton(icon)
    btn.setToolTipText(toolTip)
    btn.addActionListener(_ => {
      btn.setEnabled(enabled)
      action
    })
    btn
  }
}

object TwoImageButton {
  private val iconWithTooltipExpand = ImageWithToolTip(Icons.expand, "Children are expanded. Click to collapse")
  private val iconWithTooltipCollapse = ImageWithToolTip(Icons.collapse, "Children are collapsed. Click to expand")

  final case class ImageWithToolTip(image: ImageIcon, tooltip: String)

  def createExpandCollapse(pref: Preferences, name: String): TwoImageButton =
    createTwoImageButton(pref, name, iconWithTooltipExpand, iconWithTooltipCollapse)

  private def createTwoImageButton(
      pref: Preferences,
      name: String,
      imageOne: ImageWithToolTip,
      imageTwo: ImageWithToolTip) =
    new TwoImageButton(pref, name, imageOne.image, imageOne.tooltip, imageTwo.image, imageTwo.tooltip)
}

class TwoImageButton(pref: Preferences, name: String, img1: Icon, toolTip1: String, img2: Icon, toolTip2: String)
    extends JButton(img1) {
  private var _checked = pref.getBoolean(name, false)

  def isChecked: Boolean = _checked

  onStateChanged()

  private def onStateChanged(): Unit = {
    pref.putBoolean(name, _checked)
    if (_checked) {
      setIcon(img1)
      setToolTipText(toolTip1)
    } else {
      setIcon(img2)
      setToolTipText(toolTip2)
    }
  }

  override def processMouseEvent(e: MouseEvent): Unit = {
    if (e.getID == MouseEvent.MOUSE_RELEASED) {
      _checked = !_checked
      onStateChanged()
    }
    super.processMouseEvent(e)
  }
}
