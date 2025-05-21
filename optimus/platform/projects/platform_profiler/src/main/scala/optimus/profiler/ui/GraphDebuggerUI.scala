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

import java.awt.BorderLayout
import java.awt.EventQueue
import java.awt.FlowLayout
import java.awt.Frame
import java.awt.Toolkit
import java.awt.event.ActionEvent
import java.awt.event.KeyEvent
import java.awt.event.WindowAdapter
import java.awt.event.WindowEvent
import java.util.prefs.Preferences
import javax.swing.BorderFactory
import javax.swing.JComponent
import javax.swing.JMenuBar
import javax.swing.JOptionPane
import javax.swing.JPanel
import javax.swing.JSplitPane
import javax.swing.KeyStroke
import javax.swing.WindowConstants
import javax.swing.border.EmptyBorder
import javax.swing.plaf.basic.BasicTabbedPaneUI
import optimus.graph.DiagnosticSettings
import optimus.graph.NodeTaskInfo
import optimus.graph.NodeTrace
import optimus.graph.cache.CauseProfiler
import optimus.platform.inputs.ProcessState
import optimus.platform.inputs.registry.ProcessGraphInputs
import optimus.graph.cache.UNodeCache
import optimus.graph.diagnostics.DbgPreference
import optimus.graph.diagnostics.Debugger
import optimus.graph.diagnostics.PNodeTask
import optimus.platform.AdvancedUtils
import optimus.platform.inputs.GraphInputConfiguration
import optimus.platform.util.Log
import optimus.profiler.DebuggerUI
import optimus.profiler.ProfilerUI
import optimus.profiler.recipes.concurrency.MinimumUnitCostReport
import optimus.profiler.ui.GraphDebuggerUI.clearSelectedNodes
import optimus.profiler.ui.GraphDebuggerUI.frame
import optimus.profiler.ui.GraphDebuggerUI.showMessage
import optimus.profiler.ui.GraphDebuggerUI.showInternal
import optimus.profiler.ui.browser.GraphBrowser
import optimus.profiler.ui.common.ConfigSettings
import optimus.profiler.ui.common.HasMessageBanners
import optimus.profiler.ui.common.JFrame2
import optimus.profiler.ui.common.JMenu2
import optimus.profiler.ui.common.JPanel2
import optimus.profiler.ui.common.JSplitPane2
import optimus.profiler.ui.common.JTabbedPane2
import optimus.profiler.ui.common.ProvidesMenus
import optimus.profiler.utils.NodeDiGraphWriter
import optimus.profiler.utils.ValueGraphConfig

import java.awt.Component
import javax.swing.event.ChangeEvent
import javax.swing.event.ChangeListener
import scala.collection.mutable

/** All methods should forward to the dispatch thread */
object GraphDebuggerUI extends Log with ConfigSettings {
  private lazy val frame: GraphDebuggerUI = new GraphDebuggerUI(DiagnosticSettings.offlineReview)
  private[profiler] val offlineBrowsers: mutable.Set[GraphBrowser] = mutable.Set.empty

  def clearUserPreferences(): Unit = {
    val prefs = Preferences.userNodeForPackage(getClass)
    val prefNode = {
      import java.io._
      val baos = new ByteArrayOutputStream()
      prefs.exportSubtree(baos)
      new String(baos.toByteArray).replace(System.lineSeparator(), " ")
    }
    log.warn(s"Resetting debugger preferences; old value:\n$prefNode\n")
    prefs.clear()
    prefs.flush()
  }

  private[ui] def setFrameToLostConcurrency(): Unit = {
    val profilerTab = frame.profiler
    // this is done to go into Profiler tab
    frame.tabs.setSelectedComponent(profilerTab)
    // this is done to go into the Lost Concurrency tab in Profiler
    profilerTab.tabs.setSelectedComponent(profilerTab.lc_table)
  }

  if (DiagnosticSettings.diag_lustrate) {
    val wnVersion = WhatsNew.lastVersionViewed
    clearUserPreferences()
    WhatsNew.lastVersionViewed = wnVersion
  }
  Fonts.initializeFontSize()

  def start(): Unit = NodeUI.invokeLater {
    frame.setVisible(true)
  }

  def showTimeline(title: String = null, concurrencyReport: MinimumUnitCostReport = null): Unit = {
    frame.toFront()
    frame.nodeTimeLine.cmdRefreshTimeLine()
    frame.nodeTimeLine.cmdRefreshConcurrencyReport(title, concurrencyReport)
    frame.nodeTimeLine.setVisible(true)
    frame.tabs.setSelectedComponent(frame.nodeTimeLine)
  }

  def showSchedulerView(): Unit = {
    frame.tabs.setSelectedComponent(frame.schedulerView)
    frame.schedulerView.selectThreadsStallsPanel()
    frame.schedulerView.cmdRefresh()
  }

  def clearSelectedNodes(switchToTab: Boolean = true): Unit = {
    DebuggerUI.selectedNodes.clear()
    refreshSelectedNodesViews(switchToTab)
  }

  def showSelectedNodesView(nodes: Seq[PNodeTask]): Unit = {
    DebuggerUI.selectedNodes ++= nodes
    refreshSelectedNodesViews(switchToTab = true)
  }

  private def refreshSelectedNodesViews(switchToTab: Boolean): Unit = {
    frame.nodeTimeLine.cmdRefreshTimeLine() // since selected nodes are drawn on timeline
    if (switchToTab) frame.tabs.setSelectedComponent(frame.selectedNodeBrowser)
    frame.selectedNodeBrowser.cmdRefresh()
  }

  // noinspection AccessorLikeMethodIsUnit
  def toFront(): Unit = NodeUI.invoke { frame.toFront() }

  def addTab(title: String, component: JComponent, onCloseTab: () => Unit = null): Unit =
    NodeUI.invoke(frame.tabs.addCloseableTab(title, component, onCloseTab))

  def selectedTab: Component = frame.tabs.getSelectedComponent

  def showMessage(msg: String): Unit = NodeUI.invokeLater(frame.glassPane.showMessage(msg))

  def getTitle: String = if (EventQueue.isDispatchThread) frame.getTitle else ""
  def setTitle(title: String): Unit = NodeUI.invoke(frame.setTitle(title))

  def refresh(): Unit = NodeUI.invokeLater(frame.refresh())

  def loadTrace(toCompare: Boolean): Unit = frame.profiler.loadTrace(toCompare = toCompare)

  // same preference as for the class
  private[profiler] val showInternal: DbgPreference = DbgPreference("showInternal", "Show Internal Nodes", "", pref)
}

class GraphDebuggerUI private (val offlineReview: Boolean) extends JFrame2 with ProvidesMenus {
  val glassPane: JPanel2 with HasMessageBanners = new JPanel2() with HasMessageBanners {
    override def messageChanged(newMsg: String): Unit = setVisible(newMsg ne null)
  }
  val tabs = new JTabbedPane2(pref)

  private val jtpui = new BasicTabbedPaneUI() { override def shouldRotateTabRuns(i: Int): Boolean = false }
  tabs.setUI(jtpui)
  private val profiler = NodeProfiler.instance
  private val uiprofiler = UIProfiler.instance
  private val browser = new GraphBrowser
  private val selectedNodeBrowser = new GraphBrowser(GraphBrowser.SHOW_NODES_FROM_SELECTED_IN_UI, _ignoreFilter = true)
  private val caches = new CachesView(offlineReview)
  private val nodeTimeLine = new NodeTimeLineView
  private val schedulerView = new SchedulerView
  private val jvmStacksView = JVMStacksView() // Can be null if JVM Stacks are not enabled
  private val console = GraphConsole.instance

  private val splitPane = new JSplitPane2(pref)

  init()
  tabs.addChangeListener(new ChangeListener() {
    override def stateChanged(e: ChangeEvent): Unit = {
      val tab = frame.tabs.getSelectedComponent
      if ((tab eq selectedNodeBrowser) && selectedNodeBrowser.allTasks.isEmpty)
        showMessage("No selected nodes in Selected Nodes Tab, switch to Browser Tab to see all nodes.")
    }
  })

  def refresh(): Unit = {
    browser.cmdRefresh()
    caches.cmdRefresh()
    schedulerView.cmdRefresh()
    profiler.cmdRefresh(displayMessage = false)
    uiprofiler.cmdRefresh()
  }

  private def init(): Unit = {
    updateTitle()
    setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE)
    addWindowListener(new WindowAdapter {
      override def windowClosing(e: WindowEvent): Unit = {
        console.windowClosing()
      }
      override def windowOpened(e: WindowEvent): Unit = {
        WhatsNew.show(GraphDebuggerUI.this)
      }
    })

    val contentPane = new JPanel
    contentPane.setBorder(new EmptyBorder(0, 0, 0, 0))
    contentPane.setLayout(new BorderLayout(5, 5))
    setContentPane(contentPane)

    setIconImage(Icons.insect)
    splitPane.setOrientation(JSplitPane.VERTICAL_SPLIT)
    splitPane.setOneTouchExpandable(true)
    splitPane.setBorder(new EmptyBorder(0, 0, 0, 0))
    splitPane.setDividerSize(6)
    splitPane.setTopComponent(tabs)
    splitPane.setBottomComponent(console)
    splitPane.setResizeWeight(1)
    contentPane.add(splitPane, BorderLayout.CENTER)

    if (!offlineReview) {
      val controlPanel = new JPanel2(new BorderLayout(0, 0))

      val collectPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0))
      collectPanel.setBorder(BorderFactory.createEmptyBorder(0, 5, 5, 5))
      collectPanel.add(NodeUI.createCollectPickerComponent)
      collectPanel.add(NodeUI.createIsProfilingSSUsageComponent(pref))
      collectPanel.add(NodeUI.createIsTraceInvalidatesComponent(pref))
      collectPanel.add(NodeUI.createTraceWaitsComponent(pref))
      collectPanel.add(NodeUI.createTraceTweaksComponent(pref))

      val collectAndInfoPanel = new JPanel(new BorderLayout(0, 0))
      collectAndInfoPanel.add(collectPanel, BorderLayout.WEST)

      val schedulerTimes = NodeUI.createSchedulerTimesComponent
      schedulerTimes.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5))
      collectAndInfoPanel.add(schedulerTimes, BorderLayout.EAST)

      controlPanel.add(collectAndInfoPanel, BorderLayout.CENTER)
      controlPanel.add(NodeUI.createContinueRunningButton, BorderLayout.EAST)
      contentPane.add(controlPanel, BorderLayout.SOUTH)
    }

    if (!offlineReview) {
      tabs.add("Browser", browser)
      tabs.add("Selected Nodes", selectedNodeBrowser)
      tabs.add("Profiler", profiler)
      tabs.add("UI Profiler", uiprofiler)
      tabs.add("Caches", caches)
      tabs.add("Time Line", nodeTimeLine)
      tabs.add("Scheduler", schedulerView)
    } else {
      tabs.add("Automated Tasks", new GuideView())
    }

    if (jvmStacksView ne null)
      tabs.add("JVM Hotspots", jvmStacksView)
    tabs.restoreSelectedIndex()

    glassPane.setOpaque(false)
    setGlassPane(glassPane)

    setJMenuBar {
      val menuBar = new JMenuBar

      val profilerMenu = new JMenu2("General")
      profilerMenu.setMnemonic(KeyEvent.VK_P)

      val headerTooltip =
        s"Title can also be set on the command line, e.g. -D${DiagnosticSettings.DEBUG_PROPERTY}=1;title=Hello Debugger"
      profilerMenu.addMenu("<html><i>Set</i> Title...</html>", headerTooltip, askAndSetTitle(), enable = true)
      profilerMenu.addMenu("<html><i>Set</i> Font Size...</html>", Fonts.showFontSizer(this))
      val maximize =
        DbgPreference("maximizeOnStart", "Maximize on Start", "Always show maximized on restarts", default = false)
      profilerMenu.addCheckBoxMenu(maximize)
      if (maximize.get) setExtendedState(Frame.MAXIMIZED_BOTH)
      profilerMenu.addSeparator()

      // TODO (OPTIMUS-43995): add a callback to refresh caches tab whenever someone clears cache
      add(
        profilerMenu,
        "Clear All Caches",
        AdvancedUtils.clearCache(CauseProfiler, includeSI = true, includeLocal = true))
      add(
        profilerMenu,
        "Clear All (Non SI) Caches",
        AdvancedUtils.clearCache(CauseProfiler, includeSI = false, includeLocal = true))

      profilerMenu.addAdvMenu("Disable XSFT") {
        GraphInputConfiguration.DEBUGGER_ONLY_disableXSFT()
      }

      profilerMenu.addAdvCheckBoxMenu("Show Internal Nodes", "", showInternal) { b =>
        showInternal.set(b)
        val browserToUse =
          if (!offlineReview) Set(browser)
          else GraphDebuggerUI.offlineBrowsers
        browserToUse.foreach(_.nodeTreeChangeFlagOfShowInternal())
        selectedNodeBrowser.nodeTreeChangeFlagOfShowInternal()
        nodeTimeLine.cmdRefreshTimeLine() // redraw selected nodes
        Analyze.nodeAnalyzers.foreach(_.refresh())
      }

      profilerMenu.addAdvMenu("Remove all ExpandTweaks nodes from cache") {
        UNodeCache.global.clear(x => x.executionInfo eq NodeTaskInfo.TweakExpansion, CauseProfiler)
      }

      profilerMenu.addSeparator()
      add(profilerMenu, "Generate optconf (property configuration)", profiler.cmdExportOptconfs())
      profilerMenu.addSeparator()
      add(profilerMenu, "Refresh All", refresh()).setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_F5, 0))
      add(profilerMenu, "Reset Profile", profiler.cmdResetProfile())
        .setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_R, ActionEvent.CTRL_MASK))

      add(profilerMenu, "Reset Scenario Stack Usage", ProfilerUI.resetSStackUsage())
      add(profilerMenu, "Reset Waits", ProfilerUI.resetWaits())
      add(profilerMenu, "Reset Selected Nodes", clearSelectedNodes())
      profilerMenu.addSeparator()
      add(
        profilerMenu,
        "Analyze Calculation from Cache Full...",
        profiler.cmdAnalyzeCacheFull(),
        tooltipText = Some("See cache time saving for all nodes from cache (grouped by result)")
      )
      add(
        profilerMenu,
        "Analyze Calculation from Trace Full...",
        profiler.cmdAnalyzeTraceFull(),
        tooltipText = Some("See cache time saving for all nodes from trace (grouped by result)")
      )
      profilerMenu.addSeparator()
      profilerMenu.addCheckBoxMenu("Suspend on Test Succeeded", null, Debugger.dbgBrkOnTestSucceeded)
      profilerMenu.addCheckBoxMenu("Suspend on Test Failed", null, Debugger.dbgBrkOnTestFailed)
      profilerMenu.addCheckBoxMenu("Suspend on All Tests Completed", null, Debugger.dbgBrkOnTestsCompleted)
      profilerMenu.addCheckBoxMenu("Reset Profiler Before Test", null, Debugger.dbgResetProfileBeforeTest)

      profilerMenu.addSeparator()
      profilerMenu.addCheckBoxMenu("Show Advanced Commands", null, Debugger.dbgShowAdvancedCmds)
      profilerMenu.addAdvMenu("Mark All Nodes As 'Seen'") {
        NodeTrace.markAllAsSeen()
      }

      profilerMenu.addSeparator()
      profilerMenu.addMenu("Exit", System.exit(-1)).setMnemonic('x')
      menuBar.add(profilerMenu)

      if (offlineReview) {
        val offlineMenu = new JMenu2("Offline Options")
        add(
          offlineMenu,
          "Generate NodeTask graph", {
            val graph = NodeDiGraphWriter.fullNodeGraphToString(DebuggerUI.loadedTraces.last, config = ValueGraphConfig)
            SClipboard.copy(graph, graph)
            showMessage("Copied to clipboard!")
          },
          tooltipText = Some("Only of latest trace loaded")
        )
        offlineMenu.addSeparator()
        val tooltip = "Only applies to new traces"
        offlineMenu.addCheckBoxMenu("Show start node", tooltip, Debugger.dbgOfflineShowStart)
        offlineMenu.addCheckBoxMenu("Show unattached nodes", tooltip, Debugger.dbgOfflineShowUnattached)
        offlineMenu.addCheckBoxMenu("Show speculative proxies", tooltip, Debugger.dbgOfflineShowSpecProxies)
        offlineMenu.addCheckBoxMenu("Show noncacheable nodes", tooltip, Debugger.dbgOfflineShowNonCacheable)
        menuBar.add(offlineMenu)
      }
      profiler.getMenus.foreach { menuBar.add }
      caches.getMenus.foreach { menuBar.add }
      menuBar
    }
  }

  private def askAndSetTitle(): Unit = {
    JOptionPane.showInputDialog(this, "Enter New Title", DiagnosticSettings.diag_consoleTitle) match {
      case s: String =>
        DiagnosticSettings.diag_consoleTitle = if (s == null || s.isEmpty) null else s
        updateTitle()
      case _ =>
    }
  }

  private def updateTitle(): Unit = {
    val newTitle =
      if (DiagnosticSettings.diag_consoleTitle eq null) {
        val runconfName = System.getProperty("runconf.name")
        if (runconfName ne null) runconfName else "Graph Debugger"
      } else " " + DiagnosticSettings.diag_consoleTitle
    setTitle(newTitle)
  }
}
