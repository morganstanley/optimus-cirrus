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
import java.awt.Insets
import java.awt.event.ActionListener
import java.util.prefs.Preferences

import javax.swing.JCheckBox
import javax.swing.JPanel
import javax.swing.JSeparator
import javax.swing.JTabbedPane
import javax.swing.SwingConstants
import optimus.core.SparseBitSet
import optimus.debugger.browser.ui.DebuggerViewModelHtml
import optimus.debugger.browser.ui.NodeReview
import optimus.graph.DiagnosticSettings
import optimus.graph.diagnostics.PNodeTask
import optimus.platform.util.Log
import optimus.platform.util.html._
import optimus.profiler.DebuggerUI
import optimus.profiler.ui.MaxCharUtils._
import optimus.profiler.ui.common.JToolBar2
import optimus.profiler.ui.controls.JTextPaneScrollable

object NodeView {
  def createDetailsToolbar(pref: Preferences, nviews: NodeView*): JToolBar2 = {
    val showResult = new JCheckBox("Result", true)
    showResult.setToolTipText("Node result")
    val showArgs = new JCheckBox("Args", true)
    showArgs.setToolTipText("Arguments passed to the node")
    val showWrap = new JCheckBox("Wrap text")
    showWrap.setToolTipText(
      "Select this for Result and Args that have long single-line toString, deselect for the ones where toString has line breaks to print as a table")
    val showTwkDiffs = new JCheckBox("Tweak Diffs")
    showTwkDiffs.setToolTipText("Show tweak differences in scenarios of selected nodes")
    val showXSTwks = new JCheckBox("XS Tweaks/Tweakables")
    showXSTwks.setToolTipText("Show recorded tweaks/tweakables (for cross-scenario evaluation)")
    val showResultEntityTemporalInfo = new JCheckBox("Result Temporal Info")
    val showScenarioStack = new JCheckBox("Scenarios")
    showScenarioStack.setToolTipText("Show scenarios")
    val showScenarioStackSmart = new JCheckBox("Focused Scenarios")
    showXSTwks.setToolTipText("Show recorded tweaks/tweakables (for cross-scenario evaluation)")
    showScenarioStackSmart.setToolTipText(
      "<html>Exclude common scenario stacks and unused tweaks<br>(if <i>Trace Tweaks</i> is enabled)</html>")
    showResultEntityTemporalInfo.setToolTipText("Show entity Valid Time and Transaction Time")
    val showScenarioStackEffective = new JCheckBox("Effective Scenarios")
    showScenarioStackEffective.setToolTipText(
      "<html>Show scenario stacks after <i>also-set</i> and <i>byValue</i> replacements</html>")
    val showEntity = new JCheckBox("Entity")
    val showProperties = new JCheckBox("<html><i>Properties</i></html>")
    showProperties.setToolTipText(
      "<html>Warning: while this option tries to dump all properties on the entity," +
        "<br>it's slow and can cause <b>side effects</b></html>")
    showEntity.setToolTipText("Show entity identity details")

    val preferredMaxChars = pref.getInt(MaxCharUtils.name, DetailsConfig.maxCharsMinimum)
    val maxOutputField = createMaxOutputField(preferredMaxChars)

    // format: off
    val chks = Array(showResult, showArgs, showTwkDiffs, showXSTwks,
      showScenarioStack, showScenarioStackSmart, showScenarioStackEffective,
      showEntity, showProperties, showResultEntityTemporalInfo)
    // format: on

    val actListener: ActionListener = { _ =>
      val ds = DetailsConfig(
        showResult.isSelected,
        showArgs.isSelected,
        showEntity.isSelected,
        showProperties.isSelected,
        showTwkDiffs.isSelected,
        showXSTwks.isSelected,
        showResultEntityTemporalInfo.isSelected,
        showScenarioStack.isSelected,
        showScenarioStackSmart.isSelected,
        showScenarioStackEffective.isSelected,
        showWrap.isSelected,
        readMaxCharsFrom(maxOutputField)
      )

      for (chk <- chks) pref.putBoolean(chk.getName, chk.isSelected)
      // treat this one differently because we want it coupled with max chars
      pref.putBoolean(showWrap.getName, showWrap.isSelected)
      pref.putInt(MaxCharUtils.name, ds.maxChars)
      for (nview <- nviews) nview.updateConfig(ds)
    }

    maxOutputField.addActionListener(actListener)
    val maxOutputPanel = createMaxOutputPanel(maxOutputField)

    val toolBar = new JToolBar2("Show In Details")
    toolBar.setFloatable(true)
    toolBar.setMargin(new Insets(4, 0, 0, 0))

    chks foreach { chk =>
      configureCheckBox(chk, pref, actListener)
      toolBar.add(chk)
    }
    configureCheckBox(showWrap, pref, actListener)

    actListener.actionPerformed(null) // Update config right away

    toolBar.add(new JSeparator(SwingConstants.VERTICAL), BorderLayout.LINE_END)
    toolBar.add(showWrap)
    toolBar.add(maxOutputPanel)
    toolBar
  }

  private def configureCheckBox(chk: JCheckBox, pref: Preferences, actionListener: ActionListener): Unit = {
    val name = "show " + chk.getText.toLowerCase().trim()
    chk.setName(name)
    chk.setSelected(pref.getBoolean(name, chk.isSelected))
    chk.addActionListener(actionListener)
  }
}

class NodeView(withToolbar: Boolean, pref: Preferences) extends JTabbedPane with Log {
  private val overviewPane = new JTextPaneScrollable()
  private val capturedNodeValues = new ValueTreeTable("Node")
  private val scenarioStackValues = new ValueTreeTable("Scenario")
  private val tweakDependencies = new ValueTreeTable("Tweak Dependencies")
  private var cfg = DetailsConfig.default
  private var selection = new Array[NodeReview](0)

  def this() = this(false, null)

  init(withToolbar)

  def showNode(nodeReview: NodeReview, top: NodeReview = null): Unit = {
    selection = if (nodeReview != null) {
      if (top != null)
        nodeReview.recomputeDiffs(top) // update extra/missing/mismatchedTweaks to compare to top selected
      Array(nodeReview)
    } else new Array[NodeReview](0)

    onSelectionChanged()
  }

  def showNodes(selection: Array[NodeReview], top: NodeReview): Unit = {
    this.selection = if (selection != null) {
      if (top != null) selection.foreach { s =>
        s.recomputeDiffs(top)
      } // update extra/missing/mismatchedTweaks to compare to top selected
      selection
    } else new Array[NodeReview](0)

    onSelectionChanged()
  }

  def showNodes(selection: Array[PNodeTask]): Unit = {
    this.selection = if (selection != null) selection.map(nodeReview) else new Array[NodeReview](0)
    onSelectionChanged()
  }

  private def nodeReview(node: PNodeTask) = new NodeReview(node)

  def updateConfig(cfg: DetailsConfig): Unit = {
    this.cfg = cfg
    onSelectionChanged()
  }

  def buildUpView(hb: HtmlBuilder, nodeReview: NodeReview, scenarioStackMinStackDepth: Int): Unit = {
    if (nodeReview ne null)
      DebuggerUI.underStackOfWithoutNodeTracing(nodeReview.task.scenarioStack) {
        val dvm = DebuggerViewModelHtml(nodeReview)
        dvm.nodeDetails(hb, this.selection.length > 1, scenarioStackMinStackDepth, overviewPane.getTextPane)
      }
  }

  private def init(withToolbar: Boolean): Unit = {
    setBorder(null)

    val overviewComponent =
      if (!withToolbar) overviewPane
      else {
        val panel = new JPanel
        panel.setLayout(new BorderLayout)
        panel.add(overviewPane, BorderLayout.CENTER)
        panel.add(NodeView.createDetailsToolbar(pref, this), BorderLayout.SOUTH)
        panel
      }
    add("Overview", overviewComponent)
    add("Values", capturedNodeValues)
    add("Scenario Stack", scenarioStackValues)
    add("Tweak Dependencies", tweakDependencies)
  }

  private def onSelectionChanged(): Unit = {
    val commonSSDepth = if (selection.length == 2) {
      if (cfg.tweakDiffs) {
        selection(0).recomputeDiffs(selection(1))
        selection(0).commonScenarioStack
      } else if (cfg.scenarioStackSmart)
        DebuggerUI.getCommonScenarioStackDepth(selection(0).task.scenarioStack(), selection(1).task.scenarioStack())
      else -1
    } else -1
    // +1 at the end to avoid showing any common scenario stack
    val minScenarioStackDepth = commonSSDepth + 1

    val hb = new HtmlBuilder(cfg)
    hb.styledGroup(nodes => NodeViewStyle(nodes: _*)) {
      selection.foreach { review =>
        if (DiagnosticSettings.traceTweaksEnabled) {
          val infectionSet = review.task.getTask.getTweakInfection
          hb.infectionSet = if (infectionSet eq null) SparseBitSet.empty else infectionSet
        }
        buildUpView(hb, review, minScenarioStackDepth)
      }
    }
    overviewPane.setText(hb)
    capturedNodeValues.inspect(selection.map(_.task.getTask))
    scenarioStackValues.inspect(selection.map(_.task.scenarioStack))
    tweakDependencies.inspect(selection.take(2).map(_.recordedTweakables))
  }
}
