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
import java.awt.Component
import java.awt.EventQueue
import java.awt.Graphics
import java.awt.Graphics2D
import java.awt.RenderingHints
import java.awt.event.ActionEvent
import java.awt.event.MouseAdapter
import java.awt.event.MouseEvent
import java.lang.{Boolean => JBoolean}
import java.util.concurrent.TimeUnit
import java.util.prefs.Preferences
import javax.swing.Icon
import javax.swing.JButton
import javax.swing.JCheckBox
import javax.swing.JFrame
import javax.swing.JLabel
import javax.swing.JPanel
import javax.swing.JToggleButton
import javax.swing.UIManager
import optimus.core.CoreAPI
import optimus.graph.DiagnosticSettings
import optimus.graph.JMXConnection
import optimus.graph.NodeTrace
import optimus.graph.OGSchedulerTimes
import optimus.graph.OGTrace
import optimus.graph.diagnostics.Debugger
import optimus.graph.diagnostics.SchedulerProfileEntry
import optimus.graph.diagnostics.pgo.Profiler
import optimus.graph.diagnostics.trace.OGEventsObserver
import optimus.graph.diagnostics.trace.OGTraceMode
import optimus.platform.inputs.GraphInputConfiguration
import optimus.profiler.DebuggerUI
import optimus.profiler.ui.common.JMenu2
import optimus.profiler.ui.common.JPopupMenu2

/**
 * Helper methods for graph debugger/profiler UI
 */
object NodeUI {

  def isInitialized: Boolean = lookAndFeelInitialized

  object TimelineMenu {
    val ENABLE = "Enable"
    val NODE_DETALS = "Node Details"
    val CACHE_DETAILS = "Cache Details"
    val NODE_EDGES = "Node Edges"
  }

  private[this] var lookAndFeelInitialized = false
  private[this] def initLookAndFeel(): Unit = {
    if (!lookAndFeelInitialized) {
      lookAndFeelInitialized = true
      try {
        UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName)
        UIManager.put("Table.showGrid", java.lang.Boolean.FALSE)
      } catch {
        case _: Exception =>
      }
    }
  }

  def invoke(f: => Unit): Unit = {
    if (EventQueue.isDispatchThread) f
    else
      EventQueue.invokeAndWait(() => {
        try {
          initLookAndFeel()
          f
        } catch {
          case e: Exception => e.printStackTrace()
        }
      })
  }

  def invokeLater(f: => Unit): Unit = {
    EventQueue.invokeLater(() => {
      try {
        initLookAndFeel()
        f
      } catch {
        case e: Exception => e.printStackTrace()
      }
    })
  }

  def show(panel: => JPanel, title: String = null): Unit = invoke {
    val frame = new JFrame
    frame.setContentPane(panel) // our tabs are all JPanel2's
    frame.pack()
    frame.setTitle(if (title eq null) panel.getClass.toString else title)
    frame.setVisible(true)
  }

  def createCollectPickerComponent: JToggleButton = {
    class XToggleButton extends JToggleButton("Select recording profile") {
      val menu = new JPopupMenu2
      var state = 0 // 0 -> never collected, 1 -> collecting, 2 -> prev collected, paused now....
      val iconSize = 18

      object icon extends Icon {
        override def paintIcon(c: Component, g1: Graphics, x: Int, y: Int): Unit = {
          val g = g1.asInstanceOf[Graphics2D]
          g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)
          if (state == 0 || state == 2) {
            g.setColor(Color.black)
            val width = iconSize / 3
            g.fillRect(x, y + 1, width, iconSize)
            g.fillRect(x + width + width / 2, y + 1, width, iconSize)
          } else if (state == 1) {
            g.setColor(Color.red)
            g.fillOval(x, y + 1, iconSize, iconSize)
          }
        }
        override def getIconWidth: Int = iconSize + 2
        override def getIconHeight: Int = iconSize + 2
      }

      setIcon(icon)

      val advModes = new JMenu2("More")
      for (mode <- OGTraceMode.modes if mode != OGTraceMode.none) {
        val addToMenu = if (mode.includeInUI()) menu else advModes
        val subMenu = addToMenu.addMenu(mode.title, startProfiler(mode), enable = true)
        subMenu.setToolTipText(mode.description)
      }

      Debugger.registerAdvancedCommandChangedCallback(b =>
        if (b) {
          menu.addSeparator()
          menu.add(advModes)
        } else {
          menu.remove(advModes)
          menu.remove(menu.getComponentCount - 1)
        })

      OGTrace.registerOnTraceModeChanged((prevMode, newMode) =>
        NodeUI.invokeLater(onTraceModeChanged(prevMode, newMode)))
      onTraceModeChanged(OGTrace.getTraceMode, OGTrace.getTraceMode)

      def onTraceModeChanged(availableMode: OGEventsObserver, newMode: OGEventsObserver): Unit = {
        if (newMode eq OGTraceMode.none) {
          if (state != 0) {
            setText("Recorded: " + availableMode.title)
            state = 2
          }
        } else {
          state = 1
          setText("Recording: " + newMode.title)
          setToolTipText(newMode.description)
        }
        setSelected(state == 1)
        this.repaint()
      }

      def startProfiler(mode: OGEventsObserver): Unit = {
        Profiler.resetAll()
        GraphInputConfiguration.setTraceMode(mode)
      }

      override def processMouseEvent(e: MouseEvent): Unit = {
        if (e.getID == MouseEvent.MOUSE_PRESSED && e.getButton == MouseEvent.BUTTON1) {
          if (state != 1) {
            menu.show(this, 0, this.getHeight)
            super.processMouseEvent(e)
          } else {
            GraphInputConfiguration.setTraceMode(OGTraceMode.none)
          }
        } else {
          super.processMouseEvent(e)
        }
      }
    }

    val button = new XToggleButton()
    button
  }

  /** Creates profiling scenario stack usage checkbox, that auto updates with the others */
  def createIsProfilingSSUsageComponent(pref: Preferences): JCheckBox = {
    configureCheckBox(
      "Profile Scenario Stack Usage",
      NodeTrace.profileSSUsage,
      "<html>Record all scenario stacks, you can see reuse stats in the Profiler<br><i>Medium overhead<br>Don't forget to reset</i></html>",
      pref
    )
  }

  def createIsTraceInvalidatesComponent(pref: Preferences): JCheckBox =
    configureCheckBox(
      "Record Invalidates",
      NodeTrace.traceInvalidates,
      "Trace node invalidation in tracking scenario",
      pref)

  def createTraceWaitsComponent(pref: Preferences): JCheckBox =
    configureCheckBox("Record Waits", NodeTrace.traceWaits, "Trace blocking waits (sync stacks)", pref)

  def createTraceTweaksComponent(pref: Preferences): JCheckBox = {
    val tip = s"<html>Highlight tweaks that a calculation actually depended on. " +
      s"<br>Run in traceNodes or set <i>-D${DiagnosticSettings.TRACE_TWEAKS}=true</i> to enable</html>"
    configureCheckBox("Trace Tweaks", NodeTrace.traceTweaks, tip, pref)
  }

  type NodeTraceFlag = NodeTrace.WithChangeListener[JBoolean]
  private def configureCheckBox(name: String, flag: NodeTraceFlag, tooltip: String, pref: Preferences): JCheckBox = {
    val btn = new JCheckBox(name, flag.getValue)
    btn.setEnabled(flag.available)
    if (flag.available) {
      val prev = pref.getBoolean(name, false)
      btn.setSelected(prev)
      flag.setValue(prev)
      if (DiagnosticSettings.outOfProcess) JMXConnection.graph.setFlag(flag, prev)
    }
    btn.addActionListener { _ =>
      val on = btn.isSelected
      flag.setValue(on)
      pref.put(name, on.toString)
      if (DiagnosticSettings.outOfProcess) JMXConnection.graph.setFlag(flag, on)
    }
    flag.addCallback(value => NodeUI.invokeLater(btn.setSelected(value)))
    btn.setToolTipText(tooltip)
    btn
  }

  class SchedulerTimesInfo extends JLabel {
    private var lastValues: SchedulerProfileEntry = _
    CoreAPI.optimusScheduledThreadPool.scheduleWithFixedDelay(
      () => NodeUI.invokeLater(refresh()),
      10,
      5,
      TimeUnit.SECONDS)

    addMouseListener(new MouseAdapter {
      override def mouseClicked(e: MouseEvent): Unit = {
        if (e.getClickCount == 2)
          GraphDebuggerUI.showSchedulerView()
        refresh(true)
      }
    })

    setToolTipText("Click to refresh or double click for details")

    def refresh(force: Boolean = false): Unit = {
      val st = OGSchedulerTimes.getSchedulerTimes
      if ((lastValues eq null) || st != lastValues || force) {
        val msg = "<html><b>Graph Time:</b> " + NPTableRenderer.timeFormat.format(st.userGraphTime * 1e-6) + " ms " +
          "<b>Underused Time:</b> " + NPTableRenderer.timeFormat.format(
            OGSchedulerTimes.getUnderUtilizedTime * 1e-6) + " ms " +
          "<b>Stall Time:</b> " + NPTableRenderer.timeFormat.format(
            OGSchedulerTimes.getGraphStallTime * 1e-6) + " ms " +
          "<b>Wall Time:</b> " + NPTableRenderer.timeFormat.format(
            OGSchedulerTimes.getInGraphWallTime * 1e-6) + " ms " +
          "<b>Class Load Time:</b> " + NPTableRenderer.timeFormat.format(
            OGSchedulerTimes.getClassLoadingTime * 1e-6) + " ms" +
          "</html>"
        setText(msg)
        lastValues = st
      }
    }
    refresh()
  }

  def createSchedulerTimesComponent: SchedulerTimesInfo = new SchedulerTimesInfo

  def createContinueRunningButton: JButton = {
    val btn = new JButton("Go")
    btn.addActionListener((_: ActionEvent) => DebuggerUI.go())
    DebuggerUI.OnDebuggerStopped = _ => btn.setEnabled(DebuggerUI.isDebuggerStopped)
    btn.setEnabled(DebuggerUI.isDebuggerStopped)
    btn
  }
}
