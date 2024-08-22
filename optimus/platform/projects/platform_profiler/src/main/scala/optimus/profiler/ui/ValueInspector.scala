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
import java.awt.Component
import java.awt.Insets

import javax.swing.JCheckBox
import javax.swing.JToolBar
import optimus.core.CoreHelpers
import optimus.debugger.browser.ui.DebuggerViewModel
import optimus.graph.diagnostics.GraphDebuggerValueDetailsViewable
import optimus.platform.util.html.DetailsConfig
import optimus.platform.util.html.HtmlBuilder
import optimus.platform.util.html.HtmlInterpreters
import optimus.platform.util.html.PreFormatted
import optimus.platform.ScenarioStack
import optimus.profiler.DebuggerUI
import optimus.profiler.DebuggerUI.underStackOfWithoutNodeTracing
import optimus.profiler.ui.MaxCharUtils._
import optimus.profiler.ui.common.JPanel2
import optimus.profiler.ui.common.JSplitPane2
import optimus.profiler.ui.controls.JTextPaneScrollable

class ValueInspector(values: Array[Any], ss: ScenarioStack) extends JPanel2 {

  private[optimus] val valueTree: ValueTreeTable = new ValueTreeTable()
  private val valueViewText = new JTextPaneScrollable()

  init()
  valueTree.inspect(values)

  private def init(): Unit = {
    setLayout(new BorderLayout(20, 0))

    val showWrap = new JCheckBox("Wrap text")
    showWrap.setToolTipText(
      "Select this for objects that have long single-line toString, deselect for the ones where toString has line breaks to print as a table.")
    showWrap.addActionListener { _ =>
      wrap = showWrap.isSelected
      onSelectionChanged()
    }

    val maxOutputField = createMaxOutputField()
    maxOutputField.addActionListener { _ =>
      maxChars = readMaxCharsFrom(maxOutputField)
      onSelectionChanged()
    }
    val maxOutputPanel = createMaxOutputPanel(maxOutputField)

    val toolBar = new JToolBar()
    toolBar.setAlignmentY(Component.CENTER_ALIGNMENT)
    toolBar.setFloatable(false)
    // TODO (OPTIMUS-29498): fix stack overflow in Next Diff and reinstate the button
    add(toolBar, BorderLayout.NORTH)

    val splitPane = new JSplitPane2(pref, defDivLocation = 1000)
    splitPane.setBorder(null)

    val valuePanel = new JPanel2()
    valuePanel.setLayout(new BorderLayout(20, 0))
    valuePanel.add(valueViewText)
    val valueToolBar = new JToolBar()
    valueToolBar.setFloatable(false)
    valueToolBar.setMargin(new Insets(4, 0, 0, 0))
    valueToolBar.add(showWrap)
    valueToolBar.add(maxOutputPanel)
    valuePanel.add(valueToolBar, BorderLayout.SOUTH)

    splitPane.setTopComponent(valueTree)
    splitPane.setBottomComponent(valuePanel)

    add(splitPane, BorderLayout.CENTER)

    valueTree.dataTable.getSelectionModel.addListSelectionListener(e =>
      if (!e.getValueIsAdjusting) onSelectionChanged())
  }

  // for ValueInspector, only wrap and maxChars are relevant
  private var wrap = false
  private var maxChars = DetailsConfig.maxCharsMinimum

  private def buildUpRow(interpreter: HtmlInterpreters.Type, hb: HtmlBuilder, obj: Any): HtmlBuilder = {
    val str = underStackOfWithoutNodeTracing(ss)(CoreHelpers.safeToString(obj))
    hb.separated(2) {
      if (wrap)
        hb.noStyle("NodeResult", str)
      else
        hb ++= PreFormatted(str)

      obj match {
        case Some(viewable: GraphDebuggerValueDetailsViewable) =>
          hb.add(DebuggerViewModel.customizeViewHyperLink(ss, viewable, str))
        case viewable: GraphDebuggerValueDetailsViewable =>
          hb.add(DebuggerViewModel.customizeViewHyperLink(ss, viewable, str))
        case _ =>
      }
    }
    hb.newLine()
  }

  def onSelectionChanged(): Unit = {
    val hb = new HtmlBuilder
    hb.styledGroup(nodes => NodeViewStyle(nodes: _*)) {
      valueTree.getSelections.foreach(_.values.foreach {
        DebuggerUI.underStackOfWithoutNodeTracing(ss) {
          buildUpRow(HtmlInterpreters.prod, hb, _)
        }
      })
    }
    valueViewText.setText(hb, maxChars)
  }
}
