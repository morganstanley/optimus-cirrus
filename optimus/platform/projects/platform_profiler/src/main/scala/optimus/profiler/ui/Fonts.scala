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

import java.awt
import java.{util => ju}

import javax.swing.BoxLayout
import javax.swing.JComponent
import javax.swing.JDialog
import javax.swing.JLabel
import javax.swing.JOptionPane
import javax.swing.JSlider
import javax.swing.UIManager
import javax.swing.plaf.FontUIResource
import optimus.profiler.ui.common.ConfigSettings
import optimus.profiler.ui.common.JPanel2

import scala.jdk.CollectionConverters._

object Fonts extends ConfigSettings {
  private val scaleFactor: Int = pref.getInt("scale", 0)
  private def saveScaleFactor(sf: Int): Unit = {
    pref.putInt("scale", sf)
  } // doesn't update immediately, because that's hard

  val multiplier: Float = math.pow(2d, scaleFactor / 20d).toFloat // 20 = 2x as big, &c.

  def initializeFontSize(): Unit = {
    if (scaleFactor != 0) {
      val defaults = UIManager.getDefaults
      for (key <- defaults.keys.asScala) {
        val value = defaults.get(key)
        value match {
          case f: awt.Font =>
            val newFont = f.deriveFont(f.getSize * multiplier)
            defaults.put(
              key,
              value match {
                case _: FontUIResource => new FontUIResource(newFont)
                case _                 => newFont
              })
          case _ =>
        }
      }
    }
  }

  def showFontSizer(owner: awt.Frame): Unit = {
    new Fonts(owner).setVisible(true)
  }
}

class Fonts(owner: awt.Frame) extends JDialog(owner, "Font Size", true) { sizer =>
  setLayout(new BoxLayout(getContentPane, BoxLayout.Y_AXIS))
  object swizzler extends JSlider(-50, 50, 0) {
    setMajorTickSpacing(20)
    setMinorTickSpacing(5)
    setPaintTicks(true)
    setSnapToTicks(true)
    setPaintLabels(true)
    setLabelTable(new ju.Hashtable[Int, JComponent] {
      def put(s: Int, l: String): Unit = {
        val label = new JLabel(l)
        label.setFont(label.getFont.deriveFont(13f))
        super.put(s, label)
      }
      put(-40, "1/4x")
      put(-20, "1/2x")
      put(0, "1x")
      put(20, "2x")
      put(40, "4x")
    })
    setValue(Fonts.scaleFactor)
  }

  object buttonsPanel extends JPanel2 {
    def updateScale(scale: Int): Unit = {
      Fonts.saveScaleFactor(scale)
      JOptionPane.showMessageDialog(sizer, "Please restart the Graph Debugger to apply the new font size.")
      sizer.setVisible(false)
    }

    addButton("Cancel") { sizer.setVisible(false) }
    addButton("Reset") { updateScale(0) }
    sizer.rootPane.setDefaultButton {
      addButton("Save") { updateScale(swizzler.getValue) }
    }
  }

  add(swizzler)
  add(buttonsPanel)
  pack()
  setLocationRelativeTo(owner)
}
