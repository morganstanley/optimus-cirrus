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

import optimus.graph.DiagnosticSettings
import optimus.graph.JMXConnection
import optimus.graph.NodeTrace
import optimus.graph.OGSchedulerTimes
import optimus.graph.OGTrace
import optimus.graph.OGTraceReader
import optimus.graph.Settings
import optimus.graph.cache.NCPolicy.registeredScopedPathAndProfileBlockID
import optimus.graph.diagnostics.PNodeTask
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.diagnostics.SchedulerProfileEntryForUI
import optimus.graph.diagnostics.WaitProfile
import optimus.graph.diagnostics.gridprofiler.GridProfiler
import optimus.graph.diagnostics.gridprofiler.GridProfilerUtils
import optimus.graph.diagnostics.gridprofiler.SummaryTable
import optimus.graph.diagnostics.pgo.AutoPGOThresholds
import optimus.graph.diagnostics.pgo.ConfigWriterSettings
import optimus.graph.diagnostics.pgo.PGOMode
import optimus.graph.diagnostics.pgo.Profiler
import optimus.graph.diagnostics.trace.OGEventsEdgesObserver
import optimus.graph.diagnostics.trace.OGEventsObserver
import optimus.graph.diagnostics.trace.OGTraceMode
import optimus.graph.outOfProcess.views.CacheView
import optimus.graph.outOfProcess.views.CacheViewHelper
import optimus.graph.outOfProcess.views.ScenarioStackProfileView
import optimus.platform.inputs.GraphInputConfiguration
import optimus.platform.util.Log
import optimus.platform_profiler.config.StaticConfig
import optimus.profiler.DebuggerUI
import optimus.profiler.MergeTraces
import optimus.profiler.PGOModuleMap
import optimus.profiler.ProfilerUI
import optimus.profiler.TraceHelper
import optimus.profiler.ui.common._
import optimus.profiler.ui.controls.SingleImageButton
import optimus.profiler.utils.NodePntiWriter

import java.awt.BorderLayout
import java.awt.Component
import java.awt.Container
import java.awt.Desktop
import java.awt.GridBagConstraints
import java.awt.GridBagLayout
import java.awt.Insets
import java.awt.event.ActionEvent
import java.awt.event.KeyEvent
import java.io._
import java.lang.Double.parseDouble
import java.net.URI
import java.util
import java.util.prefs.Preferences
import javax.swing._
import scala.collection.compat._
import scala.collection.compat.immutable.ArraySeq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

object NodeProfiler extends ConfigSettings {
  private var panel: NodeProfiler = _

  def instance: NodeProfiler = {
    if (panel eq null) NodeUI.invoke {
      if (panel eq null) panel = new NodeProfiler(OGTrace.liveReader())
    }
    panel
  }

  def loadFilePaths(): ArrayBuffer[String] = {
    val fc = FileChooser.traceFileChooser
    val filePaths: ArrayBuffer[String] = new ArrayBuffer[String]
    JUIUtils.readFromFile(null, fc, multiple = true) { file =>
      filePaths += file.getAbsolutePath
    }
    filePaths
  }

  def getSchedulerTab: (
      ArrayBuffer[(String, SchedulerProfileEntryForUI)],
      ArrayBuffer[OGSchedulerTimes.StallDetailedTime],
      SummaryTable) = {
    if (DiagnosticSettings.outOfProcess) {
      (
        JMXConnection.graph.getSchedulerProfileEntry,
        JMXConnection.graph.getStallTimes,
        JMXConnection.graph.getSummaryTable)
    } else {
      (
        OGTrace.liveReader.getSchedulerProfiles.asScala.to(ArrayBuffer),
        OGSchedulerTimes.getStallingReasons.asScala.to(ArrayBuffer),
        GridProfiler.getSummaryTable)
    }
  }

  def getNodeStack: util.ArrayList[PNodeTaskInfo] = {
    if (DiagnosticSettings.outOfProcess)
      JMXConnection.graph.getLiveReaderTaskInfo
    else
      OGTrace.liveReader().getHotspots
  }

  def refreshCacheView(): ArrayBuffer[CacheView] = CacheViewHelper.getCache

  def collectProfile(includeTweakableNotTweaked: Boolean, blockID: Int): ArrayBuffer[PNodeTaskInfo] = {
    if (DiagnosticSettings.outOfProcess)
      ArrayBuffer(JMXConnection.graph.collectProfile(resetAfter = false, alsoClearCache = false): _*)
    else
      Profiler.getProfileData(alwaysIncludeTweaked = true, includeTweakableNotTweaked, blockID)
  }

  def collectProfile: ArrayBuffer[PNodeTaskInfo] = {
    collectProfile(includeTweakableNotTweaked = false, OGTrace.BLOCK_ID_ALL)
  }

  private[profiler] def getProfilerData
      : (ArrayBuffer[PNodeTaskInfo], Iterable[ScenarioStackProfileView], ArrayBuffer[WaitProfile]) = {
    val profileData = collectProfile

    val ssRoots = if (DiagnosticSettings.outOfProcess) JMXConnection.graph.getSSRoots else ProfilerUI.getSSRoots

    val waits = if (DiagnosticSettings.outOfProcess) JMXConnection.graph.getWaits else ProfilerUI.getWaits

    (profileData, ssRoots, waits)
  }

  def displayNodeAnalyzeFull(nodes: () => List[PNodeTask], cacheOrTrace: String): Unit = {
    val na = new NodeAnalyzeEx(nodes)
    GraphDebuggerUI.addTab(s"Nodes from $cacheOrTrace grouped by result", na, na.onTabClose)
  }
}

final class NodeProfiler(reader: OGTraceReader) extends JPanel2(new BorderLayout(5, 5)) with Log with ProvidesMenus {
  val tabs = new JTabbedPane
  private val hotspotsTable = new HotspotsTable(reader)
  private val ss_table = new SSUsageTable()
  private val bl_table = new BlockingNodesTable()
  val lc_table = new LostConcurrencyTable()
  val ns_table = new NodeStacksTable(reader)
  private val gp_table = new GridProfilerTable()
  private val configPreview = new ConfigPreview()

  private var disabledRowsOnly: Boolean = _
  private var defaultSelectOptconfTuningView: Boolean = _
  private val cacheToolBar: JToolBar2 = cacheToolBarSet(hotspotsTable)

  ns_table.setMessage("Collect Trace and Click Refresh Node Stacks")

  bl_table.ns_table = ns_table
  bl_table.ns_tab = tabs

  hotspotsTable.ns_table = ns_table
  hotspotsTable.ns_tab = tabs

  setBorder(BorderFactory.createEmptyBorder(2, 2, 2, 2))

  private val toolBar = {
    val toolBar = new JToolBar2()

    val btnRefresh = SingleImageButton.createRefreshButton(enabled = reader.isLive) { cmdRefresh() }
    val btnReset = SingleImageButton.createResetButton(enabled = reader.isLive) { cmdResetProfile() }
    toolBar.add(btnRefresh)
    toolBar.addSeparator()
    toolBar.add(btnReset)
    toolBar.addSeparator()
    toolBar.addButton("Refresh Node Stacks") { cmdRefreshNodeStacks() }
    toolBar.addSeparator()
    toolBar.addButton("Refresh Applet Names") { refreshAppletNames() }
    toolBar.addSeparator()
    toolBar.addButton("Reset SSUsage", enabled = reader.isLive) { ProfilerUI.resetSStackUsage() }
    toolBar.addSeparator()
    toolBar.addButton("Reset Waits", enabled = reader.isLive) {
      NodeTrace.resetWaits()
      bl_table.setList(null)
    }
    toolBar.addSeparator()
    toolBar.addButton("Generate Optconf", "Generate optconf (property configuration)") {
      cmdExportOptconfs()
    }
    toolBar.addSeparator()
    toolBar
  }

  /*
    Select all the disabled rows and set to the Hotspotstable
   */
  private def setDisabledRowsToList(hsTable: HotspotsTable): Unit = {
    hsTable.setList(hsTable.getDisabledNodes(hsTable.getCacheDecisionMaker))
    disabledRowsOnly = true
  }

  private def updateProfilerUIPref(): Unit = {
    val cacheDecisionMaker = hotspotsTable.dataTable.table.getCacheDecisionMaker
    ProfilerUI.pref.putInt("ExportConfig.cacheConfigNeverHitThreshold", cacheDecisionMaker.neverHitThreshold)
    ProfilerUI.pref.putDouble("ExportConfig.cacheConfigBenefitThresholdMs", cacheDecisionMaker.benefitThresholdMs)
    ProfilerUI.pref.putDouble("ExportConfig.cacheConfigHitRatio", cacheDecisionMaker.hitRatio)
  }

  private def cacheToolBarLabelCreator(cacheToolBar: JToolBar2, label: JLabel, textField: JTextField) = {
    cacheToolBar.add(label)
    cacheToolBar.add(textField)
  }

  /*
    Set a toolbar for threshold setting in Threshold Tuning view
   */
  private def cacheToolBarSet(hsTable: HotspotsTable): JToolBar2 = {
    val cacheDecisionMaker = hotspotsTable.getCacheDecisionMaker
    val cacheToolBar = {
      val cacheToolBar = new JToolBar2()
      cacheToolBar.setFloatable(false)

      cacheToolBar.addButton("Disabled Rows Only / All Rows") {
        if (!disabledRowsOnly) {
          setDisabledRowsToList(hsTable)
        } else {
          cmdRefresh()
          disabledRowsOnly = false
        }
      }
      cacheToolBar.addSeparator()

      val _hitRatio = new JTextField(cacheDecisionMaker.hitRatio.toString)
      val _benefitThreshold = new JTextField(cacheDecisionMaker.benefitThresholdMs.toString)
      val _cacheMisses = new JTextField(cacheDecisionMaker.neverHitThreshold.toString)

      cacheToolBarLabelCreator(cacheToolBar, new JLabel("Cache Hit Ratio > "), _hitRatio)
      cacheToolBar.addSeparator()

      cacheToolBarLabelCreator(cacheToolBar, new JLabel("Benefit Threshold < "), _benefitThreshold)
      cacheToolBar.addSeparator()

      cacheToolBarLabelCreator(cacheToolBar, new JLabel("Cache Misses > "), _cacheMisses)
      cacheToolBar.addSeparator()

      cacheToolBar.addButton("Preview") {
        hsTable.setCacheDecisionMaker(
          AutoPGOThresholds(
            hitRatio = _hitRatio.getText().toDouble,
            benefitThresholdMs = _benefitThreshold.getText().toDouble,
            neverHitThreshold = _cacheMisses.getText().toInt))

        updateProfilerUIPref()

        if (hsTable.getTableRows.nonEmpty) {
          hsTable.setDisablingEnabled(true)
          cmdRefresh()
          if (disabledRowsOnly) setDisabledRowsToList(hsTable)
        }
      }
      cacheToolBar
    }
    cacheToolBar
  }

  tabs.add("Hotspots", hotspotsTable)
  if (reader.isLive) {
    tabs.add("Scenario Stacks Top To Bottom", ss_table)
    // TODO (OPTIMUS-55942): revisit NodeStacks (delete Blocking Waits since it's in timeline?)
    // tabs.add("Blocking/Waits", bl_table)
    // tabs.add("Node Stacks", ns_table)
    tabs.add("Lost Concurrency", lc_table)
    tabs.add("Grid Profiler", gp_table)
  }
  if (!reader.isLive && !reader.isConstructing) {
    tabs.add("Config Preview", configPreview)
  }

  add(toolBar, BorderLayout.NORTH)
  add(tabs, BorderLayout.CENTER)

  private def cmdRefreshHotspots(): Unit = hotspotsTable.cmdRefresh()

  def cmdRefreshPreviewConfig(config: String): Unit = {
    cmdRefreshHotspots()
    configPreview.cmdRefresh(config)
  }

  private def selectOptconfTuningView(): Unit = {
    defaultSelectOptconfTuningView = !defaultSelectOptconfTuningView
    if (defaultSelectOptconfTuningView) {
      add(cacheToolBar, BorderLayout.SOUTH)
      cacheToolBar.updateUI()
      hotspotsTable.setView(HotspotsTable.viewBasicThresholdTuning)
    } else {
      cacheToolBar.updateUI()
      remove(cacheToolBar)
      hotspotsTable.setDisablingEnabled(false)
      cmdRefresh()
      hotspotsTable.setView(HotspotsTable.viewBasic)
    }
  }

  override def getMenus: Seq[JMenu] = {
    val menuView = new JMenu2("Profiler")
    menuView.setMnemonic(KeyEvent.VK_V)
    add(menuView, "Save Profile...", saveProfile())
      .setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_S, ActionEvent.CTRL_MASK))
    add(menuView, "Save All Profiling Data (summary table, hotspots, optconf)...", saveAllProfilingData())
    add(menuView, "Open Profile...", loadProfile(false))
      .setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_O, ActionEvent.CTRL_MASK))
    add(menuView, "Open Profile To Compare...", loadProfile(true))
    menuView.addSeparator()
    val openOgtrace = add(menuView, "Open ogtrace...", loadTrace())
    openOgtrace.setToolTipText("Open ogtrace to be read in and populate the hotspots table")
    val openComparisonOgtrace = add(menuView, "Open ogtrace to Compare...", loadTrace(true))
    openComparisonOgtrace.setToolTipText(
      "Open ogtrace to be compared to what is currently in the hotspots table " +
        "(introduces delta columns)")
    val openOgtraceGroups = add(menuView, "Open ogtrace Groups to Compare...", { new FileChooserDialog(); () })
    openOgtraceGroups.setToolTipText(
      "Compare two (or more) ogtrace files (new comparison tab of what is missing " +
        "and extra in each will appear)")
    add(menuView, "Open Recorded ogtrace...", loadRecordedTrace())
    menuView.addSeparator()
    add(menuView, "Merge ogtraces to Preview Config...", loadAndMergeTraces())
    add(
      menuView,
      "Analyze PGO Group from regressions...",
      loadGroupAndMergeTraces(),
      tooltipText = Some("Merge ogtraces per group of modules, with specified build artifact number")
    )
    menuView.addCheckBoxMenu("Optconf Threshold Tuning", null, checked = false) {
      selectOptconfTuningView()
    }
    menuView.addSeparator()
    add(menuView, "Basic View", hotspotsTable.setView(HotspotsTable.viewBasic))
    add(menuView, "Compact View", hotspotsTable.setView(HotspotsTable.viewCompact))
    add(menuView, "Compact (Per Node) View", hotspotsTable.setView(HotspotsTable.viewCompactPerNode))
    menuView.addSeparator()
    add(menuView, "General View", hotspotsTable.setView(HotspotsTable.viewGeneral))
    add(
      menuView,
      "Generate Comparison View From Current",
      hotspotsTable.generateAndSwitchToComparisonView(regenerate = false))
    add(menuView, "Remove Delta Columns From Current", hotspotsTable.removeDeltaColumnFromView())
    menuView.addSeparator()
    add(menuView, "Cache Contents View", hotspotsTable.setView(HotspotsTable.viewCacheContents))
    add(menuView, "Memory View", hotspotsTable.setView(HotspotsTable.viewMemory))

    menuView.addSeparator()
    add(menuView, "Snap 2 Compare", cmdSnapToCompare())

    val menuHelp = new JMenu("Help")
    menuHelp.setMnemonic(KeyEvent.VK_H)
    add(menuHelp, "Recent change list...", WhatsNew.show(null))
    val uriDocs = URI.create(StaticConfig.string("debuggerDocs"))
    add(menuHelp, "Debugger Docs...", Desktop.getDesktop.browse(uriDocs))
    val uriFAQ = URI.create(StaticConfig.string("debuggerFAQ"))
    add(menuHelp, "Debugger FAQ...", Desktop.getDesktop.browse(uriFAQ))
      .setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_F1, 0))
    add(
      menuHelp,
      "Restore defaults...", {
        val confirm = JOptionPane.showConfirmDialog(
          this,
          "Restore all graph debugger options to factory settings?",
          "Restore defaults",
          JOptionPane.OK_CANCEL_OPTION)
        if (confirm == JOptionPane.OK_OPTION)
          GraphDebuggerUI.clearUserPreferences()

      }
    )

    val menuExperimental = new JMenu2("Experimental")
    configureEdgesObserverButtons(menuExperimental)

    Seq(menuView, menuHelp, menuExperimental)
  }

  def cmdRefresh(displayMessage: Boolean = true, message: String = "Hotspots/ScenarioStacks/Waits refreshed"): Unit = {
    val (profileData, ssRoots, waits) = NodeProfiler.getProfilerData
    hotspotsTable.setList(profileData)
    ss_table.setList(ssRoots)
    bl_table.setList(waits)
    lc_table.refresh()
    ss_table.refresh()

    disabledRowsOnly = false

    ns_table.autoRefresh = () => cmdRefreshNodeStacks() // Will auto update node stacks on the first use
    if (displayMessage) hotspotsTable.showMessage(message)

    gp_table.init()
  }

  private def cmdRefreshNodeStacks(): Unit = {
    ns_table.setList(ProfilerUI.getNodeStacks(NodeProfiler.getNodeStack))
    ns_table.showMessage("Node Stacks refreshed")
    hotspotsTable.showMessage("Node Stacks refreshed")
  }

  private def refreshAppletNames(): Unit = {
    NodeTrace.getTrace.asScala.foreach { pNodeTask =>
      val propertyID = pNodeTask.getTask.getProfileId
      val appletName = pNodeTask.scenarioStack.tweakableListener.dependencyTrackerRootName
      if (HotspotsTable.idsToApplets.contains(propertyID)) {
        val appletNames = HotspotsTable.idsToApplets(propertyID)
        HotspotsTable.idsToApplets.update(propertyID, appletNames + appletName)
      } else
        HotspotsTable.idsToApplets.update(propertyID, Set(appletName))
    }
    cmdRefresh(
      displayMessage = true,
      message = "Applet Names and Hotspots Refreshed"
    ) // must also refresh hotspots data
  }

  /** prints to console for now, until we add a custom dot graph viewer... */
  private def cmdGenerateGraphViz(): Unit =
    try {
      println(s"Paste the graph into ${StaticConfig.string("vizir")}")
      println(new NodePntiWriter(OGEventsEdgesObserver.asAdjacencyView(true)).digraph)
    } catch {
      case e: Exception =>
        log.error("Couldn't generate GraphViz representation!", e)
    }

  private def cmdResetGraphViz(): Unit = {
    OGEventsEdgesObserver.reset() // clear dependencies
    OGTrace.reset() // so that getCollectedPntis is clear
  }

  private def configureEdgesObserverButtons(menu: JMenu2): Unit = {
    menu.addMenu("Enable edges mode", OGTraceMode.edges.description) {
      GraphInputConfiguration.setTraceMode(OGTraceMode.edges)
    }
    menu.addMenu("Disable edges mode", toolTip = null) { GraphInputConfiguration.setTraceMode(OGTraceMode.none) }

    val genToolTip =
      "Enable 'edges' mode to trace dependencies and generate a dot representation of your call graph (for small graphs, ~4k properties)"
    val resetToolTip = "Reset traced call-graph"
    val generateBtn = menu.addMenu("Generate GraphViz", toolTip = genToolTip) { cmdGenerateGraphViz() }
    val resetBtn = menu.addMenu("Reset GraphViz Traverser", toolTip = resetToolTip) { cmdResetGraphViz() }

    OGTrace.registerOnTraceModeChanged((prevMode, newMode) =>
      NodeUI.invokeLater(onTraceModeChanged(prevMode, newMode, generateBtn, resetBtn)))
  }

  private def onTraceModeChanged(
      prevMode: OGEventsObserver,
      newMode: OGEventsObserver,
      generateBtn: JMenuItem,
      resetBtn: JMenuItem): Unit = {
    // if we were just tracing edges and now swapped to a new mode, we should still be able to click buttons
    val enableEdges = (newMode eq OGTraceMode.edges) || (prevMode eq OGTraceMode.edges)
    generateBtn.setEnabled(enableEdges)
    resetBtn.setEnabled(enableEdges)
  }

  def cmdResetProfile(): Unit = {
    if (DiagnosticSettings.outOfProcess) JMXConnection.graph.profilerResetAll() else Profiler.resetAll()
    HotspotsTable.idsToApplets.clear()
    cmdRefresh()
    hotspotsTable.showMessage("Reset (Hotspots/Trace)")
  }

  // TODO (OPTIMUS-30376): load pntis into browser too and display that tab
  private def readPntisFromFile(toCompare: Boolean = false, fc: JFileChooser)(
      getPntis: File => mutable.ArrayBuffer[PNodeTaskInfo]): Unit = {
    JUIUtils.readFromFile(this, fc) { file =>
      val rows = getPntis(file)
      if (toCompare) {
        val selectedTab = GraphDebuggerUI.selectedTab
        val baseHotspots = selectedTab match {
          case hs: NodeProfiler => hs.hotspotsTable
          case _                => hotspotsTable
        }
        baseHotspots.setCompareToList(rows)
        baseHotspots.showMessage(s"Profiler - loaded ${file.getName} to compare")
      } else {
        hotspotsTable.setList(rows)
        hotspotsTable.showMessage(s"Profiler - loaded ${file.getName}")
      }
    }
  }

  private def loadAndMergeTraces(): Unit = {
    val fc = FileChooser.traceFileChooser
    val files = ArraySeq.newBuilder[String]
    JUIUtils.readFromFile(this, fc, multiple = true) { file =>
      files += file.toString
    }

    val (config, pntis) = MergeTraces.mergeTracesConfigPreview(files.result())

    val reader: OGTraceReader = OGTrace.readingFromLocal(pntis.toArray)
    DebuggerUI.previewConfig(reader, config, "merged")
  }
  private val LN_FILER: String = StaticConfig.string("filer.ln")
  private val VI_FILER: String = StaticConfig.string("filer.vi")

  private def loadGroupAndMergeTraces(): Unit = {
    val dialog = new GroupMergeDialog()
    // TODO (OPTIMUS-65137): Initialize PgoModuleMap using pgo_modules.json in environment repo
    val (rootDir, artifactsPath, names, withDisableXSFT) = dialog.groupTracesInput()

    (rootDir, artifactsPath, names, withDisableXSFT) match {
      case (Some(rootDirVal), Some(artifactsPathVal), _, _) =>
        loadGroupAndMergeTraces(rootDirVal, artifactsPathVal, names.get, withDisableXSFT.get)
      case (null, null, null, null) => // Canceled dialog
      case (None, _, _, _) =>
        JOptionPane.showMessageDialog(
          null,
          "No resource inserted",
          "PGO Module Merge Input Error",
          JOptionPane.ERROR_MESSAGE
        );
      case (_, None, _, _) =>
        JOptionPane.showMessageDialog(
          null,
          "No build artifact inserted",
          "PGO Module Merge Input Error",
          JOptionPane.ERROR_MESSAGE
        );
      case _ =>
    }
  }

  private def loadGroupAndMergeTraces(
      rootDir: String,
      artifactsPath: String,
      names: Seq[String],
      withDisableXSFT: Seq[String]): Unit = {
    val validNames = validateGroupNames(names)
    val mergedLN = MergeTraces.mergeJobTraces(LN_FILER + rootDir, artifactsPath, validNames, withDisableXSFT)
    if (mergedLN.isEmpty) {
      val mergedVI = MergeTraces.mergeJobTraces(VI_FILER + rootDir, artifactsPath, validNames, withDisableXSFT)
      showMergedGroup(artifactsPath, mergedVI)
    } else {
      showMergedGroup(artifactsPath, mergedLN)
    }
  }

  private def showMergedGroup(artifactsPath: String, merged: Seq[(String, String, Seq[PNodeTaskInfo])]): Unit = {
    val (dataMerged, emptyDataMerged) = merged.partition(_._3.nonEmpty)

    if (dataMerged.nonEmpty) {
      val groupPreviewer = DebuggerUI.loadGroupConfigPreviewer(artifactsPath)
      dataMerged foreach { case (name, config, pntis) =>
        val reader: OGTraceReader = OGTrace.readingFromLocal(pntis.toArray)
        DebuggerUI.previewConfig(reader, groupPreviewer, config, name)
      }
    } else {
      JOptionPane.showMessageDialog(
        null,
        s"No OGTraces found any of the inserted Group Names.\n",
        "PGO Module Merge Input Error",
        JOptionPane.ERROR_MESSAGE
      )
    }

    emptyDataMerged foreach { case (name, _, _) =>
      GraphDebuggerUI.showMessage(s"No OGTraces found for Modules from group $name")
    }
  }

  private def validateGroupNames(groupNames: Seq[String]): Seq[String] = {
    val (validNames, invalidNames) = PGOModuleMap.partitionValidNames(groupNames)
    if (invalidNames.nonEmpty) {
      val invalidNamesString = invalidNames.foldRight("")(_ + ", " + _).dropRight(2)
      JOptionPane.showMessageDialog(
        null,
        "Group names " + invalidNamesString + " not found",
        "PGO Module Merge Input Error",
        JOptionPane.ERROR_MESSAGE
      )
    }

    if (validNames.nonEmpty) {
      val validNamesString = validNames.foldRight("")(_ + ", " + _).dropRight(2)
      GraphDebuggerUI.showMessage("Searching OGTraces for " + validNamesString)
    } else {
      GraphDebuggerUI.showMessage("No valid Group Names inserted - OGTraces search cancelled ")
    }
    validNames

  }

  def loadTrace(toCompare: Boolean = false): Unit = {
    val fc = FileChooser.traceFileChooser
    if (toCompare) {
      readPntisFromFile(toCompare, fc) { file =>
        val reader = TraceHelper.getReaderFromFile(file.getAbsolutePath)
        val pntis = reader.getHotspots.asScala
        ArrayBuffer(pntis: _*)
      }
    } else {
      JUIUtils.readFromFile(this, fc, multiple = true) { file =>
        DebuggerUI.loadTrace(file.getAbsolutePath, showTimeLine = true, showHotspots = true, showBrowser = true)
      }
    }
  }

  private def loadRecordedTrace(): Unit =
    DebuggerUI.loadTrace(
      OGTrace.readingFromLiveBackingStore,
      showTimeLine = true,
      showHotspots = true,
      showBrowser = true,
      "recorded")

  def loadProfile(toCompare: Boolean): Unit = {
    readPntisFromFile(toCompare, FileChooser.graphFileChooser) { file =>
      val is = new ObjectInputStream(new FileInputStream(file))
      is.readObject().asInstanceOf[mutable.ArrayBuffer[PNodeTaskInfo]]
    }
  }

  def saveProfile(): Unit = {
    JUIUtils.saveToFile(this, FileChooser.graphFileChooser) { file =>
      val is = new ObjectOutputStream(new FileOutputStream(file))
      is.writeObject(hotspotsTable.getTableRows)
      is.close()
    }
  }

  def saveAllProfilingData(): Unit = {
    JUIUtils.saveToFile(this, FileChooser.allProfileFileChooser) { file =>
      GridProfilerUtils.writeData(
        file.getName,
        autoGenerateConfig = true,
        suppliedDir = file.getParent
      )
    }
  }

  private def cmdSnapToCompare(): Unit = {
    try {
      val copy = duplicateProfilerData(Profiler.getProfileData())
      hotspotsTable.setCompareToList(copy)
    } catch {
      case ex: Exception =>
        JOptionPane.showMessageDialog(this, ex.toString, "Could not Snap Profile", JOptionPane.ERROR_MESSAGE);
    }
  }

  private def createThresholdPanel(labelThresholds: (JLabel, JTextField)*): JPanel = {
    val panel = new JPanel()
    panel.setLayout(new GridBagLayout())
    val thersholdInsets = new Insets(5, 3, 5, 3)

    for ((labelThreshold, gridY) <- labelThresholds.zipWithIndex) {
      addComponent(panel, labelThreshold._2, 0, gridY, thersholdInsets)
      addComponent(panel, labelThreshold._1, 1, gridY, thersholdInsets)
    }
    panel.setAlignmentX(Component.LEFT_ALIGNMENT)
    panel
  }

  private def addComponent(
      container: Container,
      component: Component,
      gridX: Int,
      gridY: Int,
      insets: Insets,
      gridWidth: Int = 1,
      gridHeight: Int = 1,
      anchor: Int = GridBagConstraints.LINE_START,
      fill: Int = GridBagConstraints.HORIZONTAL): Unit = {

    val gbc: GridBagConstraints =
      new GridBagConstraints(gridX, gridY, gridWidth, gridHeight, 1.0d, 1.0d, anchor, fill, insets, 0, 0)
    container.add(component, gbc)
  }

  private final class ExportConfigOptions(pref: Preferences) extends JPanel with StoreSettings {
    val autoDisableCaching =
      new JCheckBox("Auto Suggest Disable Caching for Nodes", pref.getBoolean("ExportConfig.autoDisableCaching", true))
    val includePrevDisabledCache =
      new JCheckBox("Include Previously Configured", pref.getBoolean("ExportConfig.includePrevConfigured", true))
    val generateScopedConfigs =
      new JCheckBox("Generate Config Per Scope/Applet", pref.getBoolean("ExportConfig.generateScopedConfigs", false))
    val autoTrimCaches =
      new JCheckBox("Auto Suggest Trim Caches", pref.getBoolean("ExportConfig.autoTrimCaches", false))
    val autoDisableXSFT =
      new JCheckBox("Auto Suggest Disable XSFT for Nodes", pref.getBoolean("ExportConfig.autoDisableXSFT", false))
    val copyConfigLine =
      new JCheckBox("Copy Config Argument into Clipboard", pref.getBoolean("ExportConfig.copyArgToClipBoard", true))
    val cfg: AutoPGOThresholds = AutoPGOThresholds()
    val cacheConfigNeverHitThreshold = new JTextField(
      pref.get("ExportConfig.cacheConfigNeverHitThreshold", cfg.neverHitThreshold.toString)
    )
    val cacheConfigBenefitThresholdMs = new JTextField(
      pref.get("ExportConfig.cacheConfigBenefitThresholdMs", cfg.benefitThresholdMs.toString)
    )
    val cacheConfigHitRatio = new JTextField(
      pref.get("ExportConfig.cacheConfigHitRatio", cfg.hitRatio.toString)
    )
    private val cacheConfigThresholdPanel: JPanel = createThresholdPanel(
      (new JLabel("No Hit Threshold"), cacheConfigNeverHitThreshold),
      (new JLabel("Cache Benefit Threshold (ms)"), cacheConfigBenefitThresholdMs),
      (new JLabel("Hit Ratio Threshold"), cacheConfigHitRatio)
    )

    override def store(): Unit = {
      pref.putBoolean("ExportConfig.autoDisableCaching", autoDisableCaching.isSelected)
      pref.putBoolean("ExportConfig.includePrevConfigured", includePrevDisabledCache.isSelected)
      pref.putBoolean("ExportConfig.autoTrimCaches", autoTrimCaches.isSelected)
      pref.putBoolean("ExportConfig.autoDisableXSFT", autoDisableXSFT.isSelected)
      pref.putBoolean("ExportConfig.copyArgToClipBoard", copyConfigLine.isSelected)
      pref.putInt(
        "ExportConfig.cacheConfigNeverHitThreshold",
        Integer.parseInt(cacheConfigNeverHitThreshold.getText().trim())
      )
      pref.putDouble(
        "ExportConfig.cacheConfigBenefitThresholdMs",
        parseDouble(cacheConfigBenefitThresholdMs.getText().trim())
      )
      pref.putDouble(
        "ExportConfig.cacheConfigHitRatio",
        parseDouble(cacheConfigHitRatio.getText().trim())
      )
    }

    includePrevDisabledCache.setToolTipText(
      "This includes previously loaded configuration and the nodes modified in this session")
    cacheConfigNeverHitThreshold.setToolTipText(
      "Disable caching for nodes with no hits if number of misses is greater than 'No Hit Threshold'")
    cacheConfigBenefitThresholdMs.setToolTipText(
      "To disable caching 'Cache Benefit' must be less than 'Cache Benefit Threshold'")
    cacheConfigHitRatio.setToolTipText(
      "'Hit Ratio Threshold' indicates how low the hit ratio must be for caching to actually be disabled. Calculated as hits/(hits + misses)")

    if (!Settings.perAppletProfile) {
      generateScopedConfigs.setEnabled(false)
      generateScopedConfigs.setSelected(false)
      generateScopedConfigs.setToolTipText("Enable by setting -Doptimus.graph.perAppletProfile=true")
    }

    setLayout(new BoxLayout(this, BoxLayout.Y_AXIS))
    add(autoDisableCaching)
    add(autoDisableXSFT)
    add(generateScopedConfigs)
    add(autoTrimCaches)
    add(includePrevDisabledCache)
    add(copyConfigLine)
    add(cacheConfigThresholdPanel)
  }

  def cmdExportOptconfs(): Unit = {
    val options = new ExportConfigOptions(ProfilerUI.pref)
    JUIUtils.saveToFile(this, FileChooser.optConfFileChooser(options)) { file =>
      val modes = PGOMode.fromUIOptions(options.autoDisableCaching.isSelected, options.autoDisableXSFT.isSelected)
      val includePreviouslyConfigured = options.includePrevDisabledCache.isSelected
      val generateScopedConfigs = options.generateScopedConfigs.isSelected
      val autoTrim = options.autoTrimCaches.isSelected

      val cfg = AutoPGOThresholds(
        neverHitThreshold = Integer.parseInt(options.cacheConfigNeverHitThreshold.getText.trim),
        benefitThresholdMs = parseDouble(options.cacheConfigBenefitThresholdMs.getText.trim),
        hitRatio = parseDouble(options.cacheConfigHitRatio.getText.trim)
      )

      val settings = ConfigWriterSettings(modes, cfg, includePreviouslyConfigured, autoTrim)

      var configGraphForClipboard = "--config-graph "
      var summary: String = ""

      if (reader.isLive) {
        val generatedOptconfsInfo = if (generateScopedConfigs) {
          val pntis = NodeProfiler.collectProfile
          GridProfilerUtils.writeOptconfs(pntis, settings, file.getAbsolutePath)
        } else {
          // always generate optconf based on all data, not just filtered rows in the UI
          val pntis = reader.getHotspots(true).asScala
          GridProfilerUtils.writeOptconfs(pntis, settings, file.getAbsolutePath, generateScopedOptconfs = false)
        }
        summary = generatedOptconfsInfo._1
        configGraphForClipboard += generatedOptconfsInfo._2

        if (options.copyConfigLine.isSelected) SClipboard.copy(configGraphForClipboard, configGraphForClipboard)
        hotspotsTable.showMessage(summary)
      }
    }
  }

  def cmdAnalyzeCacheFull(): Unit = {
    val nodes = DebuggerUI.getPropertyNodesInCaches(_ => true)
    NodeProfiler.displayNodeAnalyzeFull(() => nodes.asScala.toList, "cache")
  }

  def cmdAnalyzeTraceFull(): Unit = {
    val nodes = NodeTrace.getTrace.asScala.filter { task: PNodeTask =>
      !task.isInternal && task.isCacheable
    }
    NodeProfiler.displayNodeAnalyzeFull(() => nodes.toList, "trace")
  }

  private def duplicateProfilerData(rows: ArrayBuffer[PNodeTaskInfo]): ArrayBuffer[PNodeTaskInfo] = {
    val baos = new ByteArrayOutputStream
    val os = new ObjectOutputStream(baos)
    os.writeObject(rows)
    os.close()

    val bain = new ByteArrayInputStream(baos.toByteArray)
    val is = new ObjectInputStream(bain)
    is.readObject().asInstanceOf[mutable.ArrayBuffer[PNodeTaskInfo]]
  }
}
