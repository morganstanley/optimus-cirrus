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

import java.awt.event.ActionEvent
import java.awt.event.KeyEvent
import javax.swing.JMenu
import javax.swing.JMenuItem
import javax.swing.JOptionPane
import javax.swing.KeyStroke
import optimus.debug.EntityInstrumentationType.recordConstructedAt
import optimus.debug.InstrumentationConfig.instrumentAllEntities
import optimus.debugger.browser.ui.GraphBrowserAPI._
import optimus.graph.ICallSite
import optimus.graph.NodeTask
import optimus.graph.NodeTrace
import optimus.graph.PropertyNode
import optimus.graph.RecordedTweakables
import optimus.graph.cache.NCSupport
import optimus.graph.diagnostics.PNodeTask
import optimus.platform.util.html.HtmlBuilder
import optimus.platform.util.html.Indent
import optimus.profiler.DebuggerUI
import optimus.profiler.MarkObject
import optimus.profiler.NodeFormatUI
import optimus.profiler.extensions.PNodeTaskExtension._
import optimus.profiler.recipes.CompareNodeDiffs
import optimus.profiler.recipes.CompareNodeDiffs.compareTrees
import optimus.profiler.recipes.DiffReason
import optimus.profiler.ui.DbgPrintSource.printSourceWithPopUpFromSelections
import optimus.profiler.ui.browser.NodeTreeBrowser
import optimus.profiler.ui.common.JMenu2
import optimus.profiler.ui.common.JMenuOps
import optimus.profiler.ui.common.JPopupMenu2

import java.awt.event.ActionListener
import javax.swing.JButton
import javax.swing.JFrame
import javax.swing.JPanel
import javax.swing.JTextField
import scala.collection.immutable.ArraySeq

trait DbgPrintSource {
  def printSource(): Unit
  def task: PNodeTask
  final def printSourceWithPopUp(selections: ArraySeq[_], dataTable: NPSubTableData[_]): Unit = {
    printSource()
    printSourceWithPopUpFromSelections(selections, dataTable)
  }
}

object DbgPrintSource {
  def printSourceWithPopUp[RType <: DbgPrintSource](hsn: HasSelectedNode[RType], dataTable: NPSubTableData[_]): Unit = {
    val selections = hsn.getSelectionsAsRows
    selections.foreach(_.printSource())
    printSourceWithPopUpFromSelections(selections, dataTable)
  }

  protected def printSourceWithPopUpFromSelections(selections: Seq[_], dataTable: NPSubTableData[_]): Unit =
    if (selections.nonEmpty) dataTable.showMessage("See Source Location in IDE Console")
}

trait HasSelectedNode[RType <: DbgPrintSource] {
  def getSelection: PNodeTask
  def getSelections: Seq[PNodeTask] = List(getSelection)

  final def getSelectionLive: NodeTask = if (getSelection eq null) null else getSelection.getTask
  final def getSelectionsLive: Seq[NodeTask] = getSelections.filterNot(_ eq null).map(_.getTask).filterNot(_ eq null)

  def getSelectionsAsRows: Seq[RType]
  def getTop: Option[NodeTask] = None
  def refresh(): Unit = {}
}

object NodeMenu {
  def create[RType <: DbgPrintSource](hsn: HasSelectedNode[RType], table: NPTable[RType]): JPopupMenu2 =
    create(null, hsn, table)

  def create[RType <: DbgPrintSource](table: NPTable[RType]): JPopupMenu2 =
    create(null.asInstanceOf[JMenuOps => Unit], table)

  private def create[RType <: DbgPrintSource](prefixItems: JMenuOps => Unit, table: NPTable[RType]): JPopupMenu2 = {
    create(
      prefixItems,
      new HasSelectedNode[RType] {
        override def getSelection: PNodeTask = if (table.getSelection eq null) null else table.getSelection.task
        override def getSelections: Seq[PNodeTask] = table.getSelections.map(_.task)
        override def getSelectionsAsRows: Seq[RType] = table.getSelections
      },
      table
    )
  }

  private def create[RType <: DbgPrintSource](
      prefixItems: JMenuOps => Unit,
      hsn: HasSelectedNode[RType],
      table: NPTable[RType]): JPopupMenu2 = {
    class JPopupMenu2WSelection extends JPopupMenu2 {

      def disableOnCondition(mi: JMenuItem, flag: Boolean): Unit = addOnPopup(mi.setEnabled(flag))
      def disableOnNoSelection(mi: JMenuItem): Unit = addOnPopup(mi.setEnabled(hsn.getSelection != null))
      def disableOnNoLiveSelection(mi: JMenuItem): Unit = addOnPopup(mi.setEnabled(hsn.getSelectionLive != null))

      /* add menu for selection */
      def addMenuFS(title: String, toolTip: String, condition: Option[Boolean] = None)(action: => Unit): Unit = {
        val mi = addMenu(title, toolTip) { action }
        condition match {
          case Some(flag) => disableOnCondition(mi, flag)
          case None       => disableOnNoSelection(mi)
        }
      }

      /* add menu for selection */
      def addMenuFSL(title: String, toolTip: String)(action: => Unit): Unit = {
        val mi = addMenu(title, toolTip) { action }
        disableOnNoLiveSelection(mi)
      }

      /* add menu for single selection */
      def addMenuFSS(title: String, toolTip: String = null)(action: PNodeTask => Unit): JMenuItem = {
        val mi = addMenu(title, toolTip) { action(hsn.getSelection) }
        disableOnNoSelection(mi)
        mi
      }

      /* add menu For Single Selection Live*/
      def addMenuFSSLive(title: String, toolTip: String = null)(action: NodeTask => Unit): JMenuItem = {
        val mi = addMenu(title, toolTip) { action(hsn.getSelectionLive) }
        disableOnNoLiveSelection(mi)
        mi
      }
    }

    val menu = new JPopupMenu2WSelection
    if (prefixItems ne null) {
      prefixItems(menu)
      menu.addSeparator()
    }

    menu.addMenuFSS("Children...", "Browse children/callees") { sn =>
      GraphDebuggerUI.addTab(s"Children of ${sn.formatIdAndName}", NodeTreeBrowser(sn))
    }

    menu.addMenuFSS("Parents...", "Browse parents/callers") { sn =>
      GraphDebuggerUI.addTab(s"Parents of ${sn.formatIdAndName}", NodeTreeBrowser(sn, showChildren = false))
    }

    menu.addSeparator()

    {
      val mi = menu.addMenu("Mark Object...") {
        val selectedItem = hsn.getSelection

        // create a new panel to allow users to set a marker for debugger UI objects
        val frame = new JFrame(s"Marking $selectedItem ...")
        val input = new JTextField("", 30)
        val panel = new JPanel()
        val saveMarkerBtn = new JButton("Save")
        panel.add(input)
        panel.add(saveMarkerBtn)
        frame.add(panel)

        frame.setSize(300, 100)
        frame.setVisible(true)

        def recordMarkerAdded(pNodeTask: PNodeTask, marker: String): Unit = {
          MarkObject.addToMarkedObjects(pNodeTask.getTask, marker)
          frame.dispose()
        }

        saveMarkerBtn.addActionListener(new ActionListener() {
          def actionPerformed(event: ActionEvent): Unit = {
            recordMarkerAdded(selectedItem, input.getText().trim)
          }
        })

        input.addKeyListener(new java.awt.event.KeyAdapter() {
          override def keyPressed(event: KeyEvent): Unit = {
            if (event.getKeyCode == KeyEvent.VK_ENTER) {
              recordMarkerAdded(selectedItem, input.getText().trim)
            }
          }
        })
      }

      val unmarkObjectItem = menu.addMenu("Unmark Object") {
        val sn = hsn.getSelection
        MarkObject.removeFromMarkedObjects(sn.getTask)
      }

      menu.addOnPopup {
        val itemSelected = hsn.getSelection
        val isMarked = MarkObject.inMarkedObjects(itemSelected.getTask)

        if (isMarked) {
          unmarkObjectItem.setEnabled(true)
          mi.setEnabled(false)
        } else {
          unmarkObjectItem.setEnabled(false)
          mi.setEnabled(true)
        }
      }
    }

    menu.addSeparator()

    {
      val miFormatting = new JMenu2("Node Tree Title Formatting")
      miFormatting.addCheckBoxMenu("Short Package Name", "", NodeFormatUI.abbreviateName) {
        NodeFormatUI.current = NodeFormatUI.current.copy(abbreviateName = !NodeFormatUI.current.abbreviateName)
        hsn.refresh()
      }
      menu.add(miFormatting)
    }

    menu.addSeparator()

    menu.addMenuFSSLive("Copy Children Tree", "Copy children to clipboard as html/text") { sn =>
      val ids = NodeFormatUI.buildIDHash(sn)
      val hb = new HtmlBuilder

      hb.styledGroup(nodes => NodeChildrenTree(nodes: _*)) {
        DebuggerUI.foreachDescendant(NodeTrace.accessProfile(sn))((c, lvl) =>
          hb.block {
            hb ++= Indent(lvl)
            NodeFormatUI.formatLine(c.getTask, hb, ids)
          })
      }

      SClipboard.copy(hb.toString, hb.toPlaintext)
    }

    menu.addSeparator()

    def showTweaksListener(sn: NodeTask, limit2parentScenarioStacks: Boolean): Unit = {
      val tweaks = DebuggerUI.collectTweaks(sn, limit2parentScenarioStacks)

      if (tweaks.size() > 0)
        DebuggerUI.browse(PNodeTask.fromLive(tweaks), "Tweaks of " + NodeFormatUI.formatName(sn))
      else
        GraphDebuggerUI.showMessage("No tweak children of " + NodeFormatUI.formatName(sn))
    }

    menu.addAdvMenu("Tweak children...") {
      val sel = hsn.getSelectionLive
      if (sel ne null) showTweaksListener(sel, limit2parentScenarioStacks = false)
    }

    menu.addAdvMenu("Tweak children from parent contexts...") {
      val sel = hsn.getSelectionLive
      if (sel ne null) showTweaksListener(sel, limit2parentScenarioStacks = true)
    }

    {
      val mi = menu.addMenuFSSLive(
        "given() locations...",
        "Prints given() locations to console if 'Profile Scenario Stack Usage' is enabled") { sn =>
        DebuggerUI.printGivenLocations(sn)
      }
      menu.addOnPopup(mi.setEnabled(NodeTrace.profileSSUsage.getValue))
    }

    menu.addSeparator();
    {
      val inConsole = new JMenu2("In Console")
      menu.add(inConsole)
      menu.disableOnNoSelection(inConsole)
      inConsole.addMenu("Inspect node", GraphConsole.instance.dump(hsn.getSelectionLive, "node"))

      inConsole
        .addMenu(
          "Inspect node.result", {
            val sn = hsn.getSelectionLive
            if ((sn ne null) && !sn.isInternal)
              sn.actOnResult(result => GraphConsole.instance.dump(result, "val"), _ => (), _ => ())
            else GraphDebuggerUI.showMessage("Node result is not available!")
          }
        )
        .setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_N, ActionEvent.CTRL_MASK))

      inConsole.addMenu("Use node's ScenarioStack", "Console will use the ScenarioStack of this node for evaluations") {
        val sn = hsn.getSelection
        if (sn eq null) GraphDebuggerUI.showMessage("Node is not selected!")
        else
          GraphConsole.instance.setDefaultScenarioStack(sn.scenarioStack)
      }
    }

    menu.addSeparator()
    menu.addMenuFSL("Inspect node(s)...", "Browse node(s) values") {
      val title = hsn.getSelection.formatIdAndName
      val selections = hsn.getSelectionsLive
      val valueInspector = new ValueInspector(selections.toArray, selections.head.scenarioStack)
      GraphDebuggerUI.addTab(s"Value Inspector [$title]", valueInspector)
    }

    menu.addMenuFSL("Inspect node(s).result...", "Browse node(s).result values") {
      val sns = hsn.getSelectionsLive
      val filtered = sns.filter(n => !n.isInternal && n.isDoneWithResult).map(_.resultObject)
      val valueInspector = new ValueInspector(filtered.toArray, sns.head.scenarioStack)
      GraphDebuggerUI.addTab("Results", valueInspector)
    }

    {
      val mi = new JMenu("Inspect Arguments")
      menu.add(mi)
      menu.disableOnNoSelection(mi)
      menu.addOnPopup {
        val selectedNodeTask = hsn.getSelectionLive
        mi.removeAll()
        val enable = if (selectedNodeTask != null && selectedNodeTask.args.nonEmpty) {
          val title = selectedNodeTask.toSimpleName
          val args = selectedNodeTask.args
          var argc = 0
          while (argc < args.length) {
            val mia = mi.add("Arg " + argc)
            val argIndex = argc
            mia.addActionListener { _: ActionEvent =>
              val sns = hsn.getSelectionsLive
              val argsAtIndex = sns.map(_.args()(argIndex))
              GraphDebuggerUI
                .addTab(
                  s"Argument $argIndex [$title]",
                  new ValueInspector(argsAtIndex.toArray, selectedNodeTask.scenarioStack))
            }
            argc += 1
          }
          true
        } else false
        mi.setEnabled(enable)
      }
    }

    {
      val mi = menu.addMenu("Inspect Tweak Dependencies", "Browse collected tweaks/tweakables for completed nodes") {
        val sns = hsn.getSelections
        val recordedTweakables = sns.map(n => {
          if (!n.getTask.isDone) RecordedTweakables.empty
          else if (n.getTask.isXScenarioOwner) n.scenarioStack().tweakableListener.asInstanceOf[RecordedTweakables]
          else DebuggerUI.collectTweaksAsTweakTreeNode(n)
        })
        val valueInspector = new ValueInspector(recordedTweakables.toArray, sns.head.scenarioStack)
        GraphDebuggerUI.addTab("Tweak Dependencies", valueInspector)
      }

      menu.addOnPopup {
        val selectedNode = hsn.getSelectionLive
        mi.setEnabled(selectedNode ne null)
      }
    }
    {
      val title = "Compare Node Compute Graphs"
      val disabledTitle = "Compare Node Compute Graphs (select 2 nodes)"
      val mi = menu.addMenu(title, "Compare node computations to find the point where graphs diverged") {
        val sns = hsn.getSelections
        val org = sns.head
        val cmp = sns.last
        val res = compareTrees(org, cmp)
        if (res.reason eq DiffReason.None)
          GraphDebuggerUI.showMessage(s"Result of task ${org.nodeName()} is the same in both runs")
        else
          GraphDebuggerUI.addTab(s"Compare Results", new NodeTreeComparisonView(res))
      }
      menu.addOnPopup {
        val sns = hsn.getSelections
        if (sns.size == 2) {
          mi.setEnabled(true)
          mi.setText(title)
        } else {
          mi.setEnabled(false)
          mi.setText(disabledTitle)
        }
      }
    }

    menu.addSeparator()
    menu.addMenuFSSLive("Set node as A for diffing") { sn => DebuggerUI.diffSelectionA = sn }
    menu.addMenuFSSLive("Set node as B and launch diff tool...") { sn =>
      DebuggerUI.diff(DebuggerUI.diffSelectionA, sn)
    }
    menu.addMenuFS("Add node(s) to Selected Nodes list", "Add nodes to Selected Nodes list (in new tab)") {
      GraphDebuggerUI.showSelectedNodesView(hsn.getSelections)
    }
    menu.addAdvMenu("Retry XS match against A... ") {
      val nodeA = DebuggerUI.diffSelectionA.asInstanceOf[PropertyNode[_]]
      val nodeB = hsn.getSelection.getTask.asInstanceOf[PropertyNode[_]]
      val matched = NCSupport.matchXscenario(nodeA, nodeA.scenarioStack(), nodeB.scenarioStack())
      DebuggerUI.out(s"The 2 nodes match = $matched")
    }

    menu.addSeparator()
    menu.addMenuFS("Print Source", "Print source location(s) in the debugger's console") {
      DbgPrintSource.printSourceWithPopUp(hsn, table.dataTable)
    }

    menu.addMenuFS("Print Stack Trace", "Print stack trace(s) in the debugger's console") {
      hsn.getSelections.foreach(_.printStackTrace())
    }

    menu.addMenuFS(
      "Print Entity Creation Stack",
      "Print stack trace(s) of entity construction in the debugger's console" +
        "(require runconf javaArg '-Doptimus.instrument.cfg=yourWs/trace_config.sc' and recordAllEntityConstructorInvocationSites())",
      Some(instrumentAllEntities == recordConstructedAt)
    ) {
      hsn.getSelections.foreach { pnt =>
        val e = pnt.getEntity.asInstanceOf[ICallSite]
        if (e ne null)
          new Exception(s"stack trace(s) for ${pnt.nodeName()}", e.getCallSite.asInstanceOf[Exception])
            .printStackTrace()
      }
    }

    menu.addSeparator()

    {
      val mi =
        menu.addMenu("Tweak byValue and Recompute Top", "Setup arguments for whatIf function in the graph console") {
          DebuggerUI.whatIfNode = hsn.getTop.orNull
          DebuggerUI.whatIfTweakKey = hsn.getSelection.asInstanceOf[PropertyNode[_]]

          val whatIfNodeStr =
            if (DebuggerUI.whatIfNode eq null) "null"
            else DebuggerUI.whatIfNode.toPrettyName(true, false)

          val whatIfTweakKeyStr =
            if (DebuggerUI.whatIfTweakKey eq null) "null"
            else DebuggerUI.whatIfTweakKey.toPrettyName(true, false)

          GraphConsole.appendHtml(
            "<div><b>whatIfNode</b> &lt;- " + whatIfNodeStr +
              "<br><b>whatIfTweakKey</b> &lt;- " + whatIfTweakKeyStr +
              "</div><div class=in></div>")
          GraphConsole.prompt("whatIf(type new value)", 7, 21)
        }

      menu.addOnPopup {
        mi.setEnabled((hsn.getSelectionsLive, hsn.getTop) match {
          case (spn: PropertyNode[_], Some(_: PropertyNode[_])) if spn.propertyInfo.isTweakable => true
          case _                                                                                => false
        })
      }
    }

    // TODO (OPTIMUS-43990): add offline mode
    menu.addMenuFSSLive(
      "Recompute & Compare (Clear Cache)",
      "Find the point where results diverged after re-evaluation") { sn =>
      val res = CompareNodeDiffs.reevaluateAndCompareLive(sn)
      if (res.reason eq DiffReason.None)
        GraphDebuggerUI.showMessage(s"Result of task ${sn.nodeName()} is the same in both runs")
      else
        GraphDebuggerUI.addTab(s"Compare Results", new NodeTreeComparisonView(res))
    }

    // TODO (OPTIMUS-43990): refactor to use PNodeTask, and disable menu item if not in live mode
    menu.addMenuFSSLive("Recompute") { sn =>
      val newTask = sn.getProfile.invalidateAndRecomputeNode(clearCache = false)
      GraphDebuggerUI.addTab("Children of " + NodeFormatUI.formatName(newTask.getTask), NodeTreeBrowser(newTask))
    }

    menu.addMenuFSSLive("Recompute (Clear Cache)") { sn =>
      val newTask = sn.getProfile.invalidateAndRecomputeNode(clearCache = true)
      GraphDebuggerUI.addTab("Children of " + NodeFormatUI.formatName(newTask.getTask), NodeTreeBrowser(newTask))
    }

    menu.addSeparator()
    menu.addCheckBoxMenu(
      "Truncate Text",
      DebuggerUI.maxCharsInInlineArg != Integer.MAX_VALUE,
      mi => {
        if (mi.getState) {
          DebuggerUI.maxCharsInInlineArg = DebuggerUI._maxCharsInInlineArg
        } else {
          DebuggerUI.maxCharsInInlineArg = Integer.MAX_VALUE
        }
        hsn.refresh()
      }
    )

    menu.addMenu(
      "Max Length...", {
        val input = JOptionPane.showInputDialog(
          null,
          "Maximum characters to display in details:",
          "Configure Max Length",
          JOptionPane.QUESTION_MESSAGE,
          null,
          null,
          DebuggerUI._maxCharsInInlineArg)
        input match {
          case s: String =>
            try {
              val newMax = Integer.parseInt(s)
              DebuggerUI._maxCharsInInlineArg = newMax
              if (Integer.MAX_VALUE != DebuggerUI.maxCharsInInlineArg)
                DebuggerUI.maxCharsInInlineArg = DebuggerUI._maxCharsInInlineArg
              hsn.refresh()
            } catch {
              case _: Exception =>
                JOptionPane
                  .showMessageDialog(null, "Only integer is acceptable", "Invalid Input", JOptionPane.WARNING_MESSAGE)
            }
          case _ =>
        }
      }
    )
    menu
  }
}
