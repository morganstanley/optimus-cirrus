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
import java.util.prefs.Preferences
import javax.swing.BorderFactory
import javax.swing.JSplitPane
import optimus.debugger.browser.ui.NodeReview
import optimus.graph.diagnostics.PNodeTask
import optimus.profiler.ui.common.JPanel2
import optimus.profiler.ui.common.JSplitPane2
import optimus.profiler.ui.common.JToolBar2

import scala.collection.compat._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class NodeAnalyzer(pref: Preferences) extends JSplitPane2(pref) {
  private val nt_table = new NodeAnalyzeTable()
  private val details = new NodeView(true, pref)

  init()

  private def init(): Unit = {
    setOrientation(JSplitPane.VERTICAL_SPLIT)
    setTopComponent(nt_table)
    setBottomComponent(details)

    nt_table.dataTable.getSelectionModel.addListSelectionListener(e => if (!e.getValueIsAdjusting) onSelectionChanged())
  }

  private def onSelectionChanged(): Unit = { details.showNodes(nt_table.getSelections.toArray, nt_table.getSelection) }
  def setSource(allTasks: ArrayBuffer[NodeReview]): Unit = nt_table.setSource(allTasks)
}

trait Analyze[T] {
  Analyze.nodeAnalyzers.add(this)
  protected val dataGenerator: () => List[T]
  final def refresh(): Unit = {
    val nodes = dataGenerator()
    if (nodes.nonEmpty) setSource(dataGenerator())
    else GraphDebuggerUI.showMessage("Nothing found to match this node type")
  }
  protected def init(): Unit
  protected def setSource(nodes: List[T]): Unit

  private[ui] final def onTabClose(): Unit =
    Analyze.nodeAnalyzers.remove(this)
}

object Analyze {
  // we keep this list so that we can refresh these tabs when the setting for showing internal nodes changes
  val nodeAnalyzers: mutable.Set[Analyze[_]] = mutable.Set()
}

class NodeAnalyze(override protected val dataGenerator: () => List[NodeReview])
    extends JPanel2
    with Analyze[NodeReview] {
  private val nodeAnalyzer = new NodeAnalyzer(pref)
  init()

  override protected def setSource(nodes: List[NodeReview]): Unit = {
    val arrayBufferNodes = nodes.to(ArrayBuffer)
    nodeAnalyzer.setSource(arrayBufferNodes)
  }

  override protected def init(): Unit = {
    refresh()
    setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5))
    setLayout(new BorderLayout(5, 5))

    val toolBar = new JToolBar2()

    add(toolBar, BorderLayout.NORTH)
    add(nodeAnalyzer, BorderLayout.CENTER)
  }
}

class NodeAnalyzeEx(override protected val dataGenerator: () => List[PNodeTask])
    extends JPanel2(new BorderLayout())
    with Analyze[PNodeTask] {
  private val nodeAnalyzer = new NodeAnalyzer(pref)
  private val nodeGrpAnalyzer = new NodeGroupAnalyze(nodeAnalyzer)

  init()

  override protected def init(): Unit = {
    refresh()
    val contentPane = new JSplitPane2(pref, "splitOuter")
    contentPane.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5))
    contentPane.setOrientation(JSplitPane.HORIZONTAL_SPLIT)
    contentPane.setLeftComponent(nodeGrpAnalyzer)
    contentPane.setRightComponent(nodeAnalyzer)
    add(contentPane)
  }

  override protected def setSource(nodes: List[PNodeTask]): Unit = nodeGrpAnalyzer.getTable.setSource(nodes)
}
