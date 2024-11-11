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

import optimus.graph.OGTrace
import optimus.graph.OGTraceReader
import optimus.graph.diagnostics.NodeName
import optimus.graph.diagnostics.PNodeTask
import optimus.graph.diagnostics.messages.AllOGCounters
import optimus.graph.diagnostics.trace.OGEventsObserver
import optimus.profiler.DebuggerUI
import optimus.profiler.extensions.ReaderExt
import optimus.profiler.recipes.concurrency.ConcurrencyTracing
import optimus.profiler.recipes.concurrency.ConcurrencyTracingConfig
import optimus.profiler.recipes.concurrency.MinimumUnitCostReport
import optimus.profiler.ui.GraphDebuggerUI.showMessage
import optimus.profiler.ui.NodeTimeLineView.MenuItems.CURRENT_SELECTED_TAB_KEY
import optimus.profiler.ui.NodeTimeLineView.MenuItems.NODES_IN_FLIGHT
import optimus.profiler.ui.NodeTimeLineView.MenuItems.SELECTED_COUNTERS_KEY
import optimus.profiler.ui.common.JMenu2
import optimus.profiler.ui.common.JPanel2
import optimus.profiler.ui.common.JSplitPane2
import optimus.profiler.ui.common.JTabbedPane2
import optimus.profiler.ui.common.JToolBar2
import optimus.profiler.ui.common.JUIUtils
import optimus.profiler.ui.controls.SingleImageButton
import optimus.profiler.ui.tables._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.awt.BorderLayout
import java.awt.Color
import java.awt.Dimension
import java.awt.event.ActionEvent
import java.awt.event.MouseAdapter
import java.awt.event.MouseEvent
import javax.swing.JMenuBar
import javax.swing.JMenuItem
import javax.swing.JPanel
import javax.swing.JScrollPane
import javax.swing.JSplitPane
import javax.swing.JTextField
import javax.swing.ScrollPaneConstants
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

object NodeTimeLineView {
  val log: Logger = LoggerFactory.getLogger(classOf[NodeTimeLineView])
  object MenuItems {
    val RUNNING_NODES = "Select Running Nodes"
    val CONCURRENCY_SUGGESTION = "Suggest Places to Increase Concurrency"
    val TRACE_NODES_IN_FLIGHT = "Select Nodes In Flight"
    val CURRENT_SELECTED_TAB_KEY = "current"
    val SELECTED_COUNTERS_KEY = "selectedCounterIDs"
    val NODES_IN_FLIGHT = "Nodes in Flight"
  }
}

class NodeTimeLineView(private val reader: OGTraceReader) extends JPanel2(new BorderLayout()) {
  import NodeTimeLineView._

  log.info(s"Number of registered OGCounters: ${AllOGCounters.allCounters.length}")

  private val tline = new NodeTimeLine(reader)
  private val menuSelectedCounters = new JMenu2("Select Counters")
  private val taskLabelFilterPanel: JPanel = createThreadNameFilter()
  private val scrollPane = new JScrollPane(
    tline,
    ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS,
    ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED)
  scrollPane.setWheelScrollingEnabled(true)

  private val chartWidth = 1500

  private val tabs = new JTabbedPane2(pref)
  // Contains information about the data for each tab (title, table to be displayed, IDs for counters it receives data from)
  private val nview = new NodeStacksTableForTimeLineView(reader)
  private val nodesFromTrace = new NodeTreeTableForTimeLineView()
  private val nodesInFlight = new NodesInFlightForTimeLineView(nview)
  private val dalRequests = new DalRequestsTable(tline)
  private val concurrencyReport = new ConcurrencyTable()
  private val defaultCCDisplayName = "DAL"
  private val defaultCCNodeName = NodeName.dalCall
  private val garbageCollectionEvents = new GarbageCollectionTable(tline)
  private val cacheClearEvents = new CacheClearTable(tline)
  private val syncStackEvents = new SyncStackTable(tline)
  private val blockingWaitEvents = new BlockingWaitTable(tline)
  private val userActionEvents = new UserActionTable(tline)
  private val dtqEvents = new DTQEventViewTable(tline)
  private val activeIdleEvents = new ActiveIdleDetectionTable(tline)
  private val bookmarkEvents = new BookmarkTable(tline)
  private val startupEvents = new StartupEventTable(tline)
  private val handlerStepEvents = new HandlerStepEventTable(tline)

  private val uncheckedCounterIds = readCounterIDs()

  def this() = this(OGTrace.liveReader())

  init()
  private def init(): Unit = {
    tline.menu.addSeparator()
    tline.menu.addMenu(MenuItems.TRACE_NODES_IN_FLIGHT, { nodesInFlightOrTrace() })

    tline.menu.addSeparator()
    tline.menu.addMenu(
      MenuItems.CONCURRENCY_SUGGESTION, {
        reader.printConcurrencyBranches(tline.getSelectedTimeStartA, tline.getSelectedTimeEndA)
      })

    tline.menu.addMenu(
      MenuItems.RUNNING_NODES, {
        val r = ReaderExt.selectNodes(reader, tline.getSelectedTimeStartA, tline.getSelectedTimeEndA, true)
        nview.setList(r.asScala)
        tabs.setSelectedComponent(nview)
      }
    )

    def addConcurrencyMenu(displayName: String, nodeName: => NodeName): JMenuItem = {
      tline.menu.addMenu(
        s"Concurrency Report for $displayName", {
          updateConcurrencyReport(displayName, nodeName)
        }
      )
    }

    tline.menu.addSeparator()
    addConcurrencyMenu(defaultCCDisplayName, defaultCCNodeName)
    val ccreportMenu = addConcurrencyMenu(null, DebuggerUI.selectedTaskInfo.nodeName) // name will be replaced

    tline.menu.addOnPopup {
      if (DebuggerUI.selectedTaskInfo eq null) {
        ccreportMenu.setEnabled(false)
        ccreportMenu.setText("Concurrency Report for Property Selected in Hotspots")
      } else {
        ccreportMenu.setEnabled(true)
        ccreportMenu.setText(
          s"Concurrency Report for ${DebuggerUI.selectedTaskInfo.nodeName.toString(shortened = true)}")
      }
    }

    val toolBar = new JToolBar2()
    val menuBar = new JMenuBar()
    menuSelectedCounters.setOpaque(true)
    menuBar.add(menuSelectedCounters)
    menuBar.add(createOptionsMenu())
    menuBar.add(taskLabelFilterPanel)

    def createOptionsMenu(): JMenu2 = {
      val viewMenu = new JMenu2("View")
      viewMenu.setOpaque(true)

      // consider link up task end to received
      viewMenu.addCheckBoxMenu(tline.showThreadNames)
      viewMenu.addSeparator()
      viewMenu.addCheckBoxMenu(tline.showOneLinePerProcess)
      viewMenu.addSeparator()
      viewMenu.addCheckBoxMenu(tline.distributedEventsAsTask)
      viewMenu.addSeparator()
      viewMenu.addCheckBoxMenu(tline.linkRemoteTasksToStart)
      viewMenu.addSeparator()
      viewMenu.addCheckBoxMenu(tline.colorThreadsByProcess)
      viewMenu
    }

    // Repopulates data in the currently selected timeline tab
    def updateCurrentTabData(): Unit = {
      val selectedTab = tabs.getSelectedComponent
      if (selectedTab != null) {
        val selectedTabIndex = tabs.indexOfComponent(selectedTab)
        val selectedTitle = tabs.getTitleAt(selectedTabIndex)
        pref.put(CURRENT_SELECTED_TAB_KEY, selectedTitle)
        selectedTab match {
          case npt: NPTable[_] => npt.updateData()
          case _               =>
        }
        tline.setHighlightRange(0L, 0L)
      }
    }

    def cmdRefresh(): Unit = {
      cmdRefreshTimeLine()
      updateCurrentTabData()
    }

    val btnRefresh = SingleImageButton.createRefreshButton() { cmdRefresh() }
    val btnReset = SingleImageButton.createResetButton("Reset all trace data so far") {
      OGTrace.reset()
      cmdRefresh()
    }

    toolBar.add(btnRefresh)
    toolBar.addSeparator()
    toolBar.add(btnReset)
    toolBar.addSeparator()
    toolBar.addButton("-", "Ctrl -") { tline.zoomBy(-1) }
    toolBar.addButton("All") { tline.zoomOut() }
    toolBar.addButton("Range") { tline.zoomToRange() }
    toolBar.addButton("+", "Ctrl +") { tline.zoomBy(1) }
    toolBar.addSeparator()
    toolBar.add(menuBar)

    tabs.add("Nodes in Flight", nodesInFlight.getVisible)
    tabs.add("Concurrency Report", concurrencyReport)
    tabs.add("DAL Requests", dalRequests)
    tabs.add("Garbage Collection Events", garbageCollectionEvents)
    tabs.add("Cache Clear Events", cacheClearEvents)
    tabs.add("Sync Stack Events", syncStackEvents)
    tabs.add("Blocking Waits", blockingWaitEvents)
    tabs.add("Bookmark Events", bookmarkEvents)
    tabs.add("User Actions", userActionEvents)
    tabs.add("UI blocking events", dtqEvents)
    tabs.add("Active/Idle Detection", activeIdleEvents)
    tabs.add("Startup Events", startupEvents)
    tabs.add("Handler Step Events", handlerStepEvents)

    // Update current tab data when the current selected time range changes, or when the user switches tabs
    tline.onMouseRelease(updateCurrentTabData)
    tabs.addMouseListener(new MouseAdapter {
      override def mousePressed(e: MouseEvent): Unit = updateCurrentTabData()
    })

    // We take the previous tab title and create a if statement to check if we have a instance change
    val previousTabTitle = pref.get(CURRENT_SELECTED_TAB_KEY, null)
    val previousTabIndex = if (previousTabTitle ne null) tabs.indexOfTab(previousTabTitle) else 0
    tabs.setSelectedIndex(previousTabIndex)

    // setup tline and tabs in data split pane
    val dataSplitPane = new JSplitPane2(pref)
    dataSplitPane.setOrientation(JSplitPane.VERTICAL_SPLIT)
    tline.setMinimumSize(new Dimension(chartWidth, 200))
    dataSplitPane.add(scrollPane, JSplitPane.TOP)
    dataSplitPane.add(tabs, JSplitPane.BOTTOM)

    // setup tools and time bars in tool panel
    val toolSplitPanel = new JPanel2(new BorderLayout())
    tline.timeBar.setMinimumSize(new Dimension(chartWidth, 40))
    tline.timeBar.setPreferredSize(new Dimension(chartWidth, 40))
    toolSplitPanel.add(toolBar, BorderLayout.NORTH)
    toolSplitPanel.add(tline.timeBar, BorderLayout.SOUTH)

    // add split panes into container
    add(toolSplitPanel, BorderLayout.NORTH)
    add(dataSplitPane, BorderLayout.CENTER)

    concurrencyReport.dataTable.getSelectionModel.addListSelectionListener { e =>
      if (!e.getValueIsAdjusting) onConcurrencyReportSelectionChanged()
    }
  }

  def saveCounterIDs(): Unit = {
    val asString = uncheckedCounterIds.mkString(",")
    pref.put(SELECTED_COUNTERS_KEY, asString)
  }

  def readCounterIDs(): mutable.HashSet[Int] = {
    val set = mutable.HashSet[Int]()
    try {
      val strVals = pref.get(SELECTED_COUNTERS_KEY, "").split(',')
      strVals.foreach(v => set.add(v.toInt))
    } catch { case _: Exception => }
    set
  }

  private def createThreadNameFilter(): JPanel = {
    val taskLabelFilterField = new JTextField()
    taskLabelFilterField.addActionListener((_: ActionEvent) => {
      val search = taskLabelFilterField.getText
      if (search ne null) tline.refreshThreads(search)
    })
    JUIUtils.createPanelForTextField(taskLabelFilterField, "Thread Filter")
  }

  private def generateMenuItems(): Unit = {
    menuSelectedCounters.removeAll()
    val groupings = tline.getChartGroupings

    val allCanBeChecked = uncheckedCounterIds.isEmpty
    menuSelectedCounters.addStayOpenCheckBoxMenu("All", null, checked = allCanBeChecked) { checked =>
      if (checked)
        uncheckedCounterIds.clear()
      else
        tline.updateChartCounters(uncheckedCounterIds)

      saveCounterIDs()
      cmdRefreshTimeLine()
    }

    menuSelectedCounters.addSeparator()

    val it = groupings.iterator
    while (it.hasNext) {
      val group = it.next()
      val counterIt = group._2.iterator
      while (counterIt.hasNext) {
        val counter = counterIt.next()
        val id = counter.id
        val currentlyChecked = !uncheckedCounterIds.contains(id)
        if (!tline.tableOnlyCounters.contains(counter.id)) {
          menuSelectedCounters.addStayOpenCheckBoxMenu(counter.description, null, checked = currentlyChecked) {
            checked =>
              if (checked) uncheckedCounterIds.remove(id)
              else uncheckedCounterIds.add(id)
              saveCounterIDs()
              cmdRefreshTimeLine()
          }
        }
      }
      if (it.hasNext)
        menuSelectedCounters.addSeparator()
    }
  }

  if (reader.isLive)
    OGTrace.registerOnTraceModeChanged((availableMode, _) => NodeUI.invokeLater(onTraceModeChanged(availableMode)))
  else
    cmdRefreshTimeLine()

  private def onTraceModeChanged(availableMode: OGEventsObserver): Unit = {
    val hasSomethingToDisplay = (reader.isLive && availableMode.collectsTimeLine) || reader.hasSchedulerTimes
    if (hasSomethingToDisplay) {
      updateTimelineMenu()
      updateNodeStacksColumns()
      nview.setMessage(null)
    } else
      nview.setMessage("Record details with timeline mode.")
  }

  def cmdRefreshTimeLine(): Unit = {
    tline.setReader(reader, uncheckedCounterIds)
    generateMenuItems()
    tline.updateUI()
  }

  def cmdRefreshConcurrencyReport(title: String, report: MinimumUnitCostReport): Unit = {
    concurrencyReport.setCostResults(title, report)
    tabs.setSelectedComponent(concurrencyReport)
  }

  private def onConcurrencyReportSelectionChanged(): Unit = {
    val report = concurrencyReport.costResults
    val concurrencyViewRow = concurrencyReport.getSelection
    val taskOfInterestMask = if (concurrencyViewRow eq null) 0 else concurrencyViewRow.taskOfInterestMask
    if (report ne null) {
      val nodes = new Array[PNodeTask](report.tasksOfInterest.size)
      val nodeColors = new Array[Color](nodes.length)
      var i = 0
      while (i < report.tasksOfInterest.size) {
        nodes(i) = report.tasksOfInterest(i)
        nodeColors(i) = if ((taskOfInterestMask & (1L << i)) == 0) Color.PINK else Color.BLUE
        i += 1
      }
      // selection can be null when panel refresh triggers the selectionChanged listener
      val groupNode = if (concurrencyViewRow eq null) null else concurrencyViewRow.task
      tline.setSelectedNodes(nodes, nodeColors, groupNode)
      // TODO (OPTIMUS-43368): come back to divider location
      // splitPane.setDividerLocation(Math.max(splitPane.getDividerLocation, tline.selectedNodesBase))
    }
  }

  // TODO (OPTIMUS-43368): Revisit this to check mode
  private def updateTimelineMenu(): Unit = {
    tline.menu.getMenu(MenuItems.RUNNING_NODES).foreach {
      _.setEnabled(false)
    }
    tline.menu.getMenu(MenuItems.CONCURRENCY_SUGGESTION).foreach {
      _.setEnabled(false)
    }
  }

  private def updateNodeStacksColumns(): Unit = nview.setView(NodeStacksTable.minView)

  private def updateNodesInFlight(): Unit = {
    val r = ReaderExt.selectNodes(reader, tline.getSelectedTimeStartA, tline.getSelectedTimeEndA, false)
    nview.setList(r.asScala)
    replaceCurrentNodesInFlightView(nview)
  }

  private def replaceCurrentNodesInFlightView(newView: NPTable[_]): Unit = {
    val index = tabs.indexOfComponent(nodesInFlight.getVisible)
    tabs.remove(index)
    nodesInFlight.setVisible(newView)
    tabs.add(NODES_IN_FLIGHT, newView, index)
    tabs.setSelectedComponent(newView)
  }

  private def nodesFromTraceAsTree(start: Long, end: Long): Iterable[NodeTreeView] = {
    val visitedNodes = new ArrayBuffer[PNodeTask]()
    val r = ArrayBuffer[NodeTreeView]()
    val trace = reader.getRawTasks.asScala.filter(_.isActive(start, end))

    def expandChildren(task: PNodeTask, level: Int, parentView: NodeTreeView): Unit = {
      val pnt = task
      val doShow = GraphDebuggerUI.showInternal.get || !pnt.isInternal
      if (doShow && (pnt ne null) && pnt.visitedID == 0 && pnt.isActive(start, end)) {
        pnt.visitedID = 1
        visitedNodes += task
        val view = new NodeTreeView(task, nodesFromTrace.expandCfg, level)
        view.open = true

        if (level == 0) r += view
        else parentView.addChild(view)

        val it = pnt.getCallees
        while (it.hasNext) expandChildren(it.next, level + 1, view)
      }
    }

    try {
      val it = trace.iterator
      while (it.hasNext) {
        val task = it.next()
        expandChildren(task, 0, null)
      }
    } finally {
      val it = visitedNodes.iterator
      while (it.hasNext) it.next().visitedID = 0
    }
    r
  }

  private def updateNodesFromTrace(): Unit = {
    nodesFromTrace.setList(nodesFromTraceAsTree(tline.getSelectedTimeStartA, tline.getSelectedTimeEndA))
    replaceCurrentNodesInFlightView(nodesFromTrace)
  }

  private def nodesInFlightOrTrace(): Unit = {
    // If reader collected parents/children, display those structures
    if (reader.hasEdges) updateNodesFromTrace()
    else {
      // Show message: "Record in traceNodes mode to see parent/child structures of nodes in flight"
      showMessage("Record in traceNodes mode to see parent/child structures of nodes in flight")
      updateNodesInFlight()
    }
  }

  private def selectedRange: (Long, Long) =
    if (tline.isRangeSelected) (tline.getSelectedTimeStartA, tline.getSelectedTimeEndA)
    else (Long.MinValue, Long.MaxValue)

  private def concurrencyTitle(name: String): String =
    if (!tline.isRangeSelected) s"Concurrency for $name"
    else
      s"Concurrency for $name for " + tline.timeBar.selectedTimeRangeLabel + " starting at " + tline.selectionStartLabel

  private def updateConcurrencyReport(displayName: String, nodeName: => NodeName): Unit = {
    val (start, end) = selectedRange
    val concurrencyTracing = new ConcurrencyTracing(ConcurrencyTracingConfig(minTime = start, maxTime = end))
    val res = concurrencyTracing.minimumUnitCost(reader, nodeName)
    val title = if (displayName eq null) nodeName.toString(shortened = true, includeHint = true) else displayName
    if (res eq MinimumUnitCostReport.empty)
      concurrencyReport.showMessage(s"Could not find $title calls in selected range")
    cmdRefreshConcurrencyReport(concurrencyTitle(title), res)
  }
}
