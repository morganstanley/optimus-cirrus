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
import optimus.graph.OGTrace
import optimus.graph.diagnostics.Debugger
import optimus.graph.diagnostics.pgo.Profiler
import optimus.graph.diagnostics.trace.OGEventsObserver
import optimus.graph.diagnostics.trace.OGTraceMode
import optimus.platform.inputs.GraphInputConfiguration
import optimus.profiler.DebuggerUI
import optimus.profiler.ui.GraphDebuggerUI
import optimus.profiler.ui.NodeUI
import optimus.profiler.ui.common.JMenu2
import optimus.profiler.ui.common.JPopupMenu2

import java.awt.Color
import java.awt.Component
import java.awt.FlowLayout
import java.awt.Graphics
import java.awt.Graphics2D
import java.awt.RenderingHints
import java.awt.event.MouseEvent
import javax.swing.BorderFactory
import javax.swing.Icon
import javax.swing.JButton
import javax.swing.JPanel
import javax.swing.JSeparator
import javax.swing.SwingConstants

// Some traits to avoid structural types.
trait OnTraceModeChanged {
  def onTraceModeChanged(availableMode: OGEventsObserver, newMode: OGEventsObserver): Unit
}

trait OnAdvancedCommandsChanged {
  def onAdvancedCommandsChanged(enabled: Boolean): Unit
}

object RecStopControls {
  class RecIcon(iconSize: Int, color: Color) extends Icon {
    override def paintIcon(c: Component, g1: Graphics, x: Int, y: Int): Unit = {
      val g = g1.asInstanceOf[Graphics2D]
      g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)
      g.setColor(color)
      g.fillOval(x, y + 1, iconSize, iconSize)
    }
    override def getIconWidth: Int = iconSize + 2
    override def getIconHeight: Int = iconSize + 2
  }

  class StopIcon(iconSize: Int, color: Color) extends Icon {
    override def paintIcon(c: Component, g1: Graphics, x: Int, y: Int): Unit = {
      val g = g1.asInstanceOf[Graphics2D]
      g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)
      g.setColor(color)
      val width = iconSize
      g.fillRect(x, y + 1, width, iconSize)
    }
    override def getIconWidth: Int = iconSize + 2
    override def getIconHeight: Int = iconSize + 2
  }

  def create: RecStopControls = {
    val out = new RecStopControls
    out.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 0))
    out
  }
}

// Panel holding the REC, STOP and change trace mode buttons in the bottom left of the graph debugger UI. These are all
// contained in this one widget because they share important state.
class RecStopControls private () extends JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0)) {
  // currentMode is the currently selected mode from the mode menu. During a recording this is always set to the
  // recording mode.
  private var currentMode: OGEventsObserver = OGTraceMode.hotspots

  // REC and STOP buttons
  private val recButton = new RecOrStopButton(isRec = true) {
    override protected def onClick(): Unit = {
      if (GraphDebuggerUI.resetBetweenRecordings.getState) Profiler.resetAll()
      GraphInputConfiguration.setTraceMode(currentMode)
    }
  }

  private val stopButton = new RecOrStopButton(isRec = false) {
    override protected def onClick(): Unit = GraphInputConfiguration.setTraceMode(OGTraceMode.none)
  }

  // Mode selection menu:
  private val modeButton = new JButton() with OnTraceModeChanged with OnAdvancedCommandsChanged {
    private def updateCurrentMode(mode: OGEventsObserver): Unit = {
      currentMode = mode
      setText(mode.title)
    }

    // Standard modes are in modeMenu, modes that are only visible when Show Advanced Commands is set are in advModes.
    private val modeMenu = new JPopupMenu2
    private val advModes = new JMenu2("More")

    def onAdvancedCommandsChanged(enabled: Boolean): Unit = {
      modeMenu.removeAll() // clear and repopulate
      for (mode <- OGTraceMode.modes if mode != OGTraceMode.none) {
        val addToMenu = if (mode.includeInUI()) modeMenu else advModes
        val subMenu = addToMenu.addMenu(mode.title, updateCurrentMode(mode), enable = true)
        subMenu.setToolTipText(mode.description)
      }

      if (enabled) {
        modeMenu.addSeparator()
        modeMenu.add(advModes)
      }
    }

    def onTraceModeChanged(availableMode: OGEventsObserver, newMode: OGEventsObserver): Unit = {
      // When recording, the button shows the current recording mode and selection is disabled. When
      // stopped, the button shows the most recent recording mode or the mode of the next recording.
      if (newMode eq OGTraceMode.none) {
        updateCurrentMode(availableMode)
        setEnabled(true)
        setToolTipText("Select recording mode")
      } else {
        updateCurrentMode(newMode)
        setEnabled(false)
        setToolTipText("Current recording mode")
      }
      repaint()
    }

    override def processMouseEvent(e: MouseEvent): Unit = {
      if (isEnabled && (e.getID == MouseEvent.MOUSE_PRESSED && e.getButton == MouseEvent.BUTTON1)) {
        modeMenu.show(this, 0, this.getHeight)
      }
      super.processMouseEvent(e)
    }
  }

  // Register a callback and make sure to call it with the initial state so that it starts correctly.
  private def registerAndCall[A](cb: A => Unit)(register: (A => Unit) => Unit, currentState: A): Unit = {
    register(cb)
    cb(currentState)
  }

  registerAndCall[(OGEventsObserver, OGEventsObserver)] { case (prevMode, newMode) =>
    modeButton.onTraceModeChanged(prevMode, newMode)
    recButton.onTraceModeChanged(prevMode, newMode)
    stopButton.onTraceModeChanged(prevMode, newMode)
  }(
    cb => OGTrace.registerOnTraceModeChanged((prevMode, newMode) => NodeUI.invokeLater(cb((prevMode, newMode)))),
    (OGTrace.getTraceMode, OGTrace.getTraceMode))

  registerAndCall(modeButton.onAdvancedCommandsChanged)(
    Debugger.registerAdvancedCommandChangedCallback,
    Debugger.dbgShowAdvancedCmds.get)

  // And finally setup the panel
  this.add(recButton)
  this.add(stopButton)
  this.add(new JSeparator(SwingConstants.VERTICAL))
  this.add(modeButton)
}

// The REC and STOP buttons share a fair bit of logic, so they use an abstract class with boolean flagging. This isn't
// the greatest pattern but I don't expect there to be more than two extenders.
private sealed abstract class RecOrStopButton(isRec: Boolean) extends JButton(" ") with OnTraceModeChanged {
  // Overriden because it needs to mess with state in the controls container.
  protected def onClick(): Unit

  private var painted = false

  setToolTipText(if (isRec) "Start recording" else "Stop recording")

  override def paint(g: Graphics): Unit = {
    super.paint(g)

    // This little hacky code ensures that the icon in the button is sized correctly by first waiting for the button to
    // be painted with just text, then repainting it with the icon once we know what size it is given the current font
    // size.
    //
    // There might be a better way of doing this... but I don't know it.
    if (!painted) {
      val origSize = getPreferredSize
      val iconSize = origSize.height - 12
      if (isRec) {
        // The recording button is disabled when we are recording. In that case, we make the little indicator red.
        setIcon(new RecStopControls.RecIcon(iconSize, Color.black))
        setDisabledIcon(new RecStopControls.RecIcon(iconSize, Color.red))
        setText("REC")
      } else {
        setIcon(new RecStopControls.StopIcon(iconSize, Color.black))
        setDisabledIcon(new RecStopControls.StopIcon(iconSize, Color.gray))
        setText("STOP")
      }
      painted = true
    }
  }

  def onTraceModeChanged(availableMode: OGEventsObserver, newMode: OGEventsObserver): Unit = {
    // Current mode:      REC:                                               STOP:
    //  none              enabled (can start recording)                      disabled
    //  recording         disabled (have to stop before we can start again)  enabled
    if (newMode eq OGTraceMode.none) setEnabled(isRec)
    else setEnabled(!isRec)
    repaint()
  }

  override def processMouseEvent(e: MouseEvent): Unit = {
    if (isEnabled && (e.getID == MouseEvent.MOUSE_PRESSED && e.getButton == MouseEvent.BUTTON1)) onClick()
    super.processMouseEvent(e)
  }
}
