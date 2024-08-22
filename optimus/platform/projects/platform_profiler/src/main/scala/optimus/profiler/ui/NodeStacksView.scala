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
import java.awt.Dimension
import java.util.concurrent.TimeUnit.MILLISECONDS

import javax.swing.JLabel
import javax.swing.JTextField
import javax.swing.JToolBar
import javax.swing.border.EmptyBorder
import optimus.graph.OGTraceReader
import optimus.profiler.extensions.NPTreeNodeExt
import optimus.profiler.recipes.FindJumpsInCache
import optimus.profiler.recipes.PNodeTaskInfoGrp
import optimus.profiler.ui.common.JPanel2
import optimus.profiler.ui.common.JToolBar2

class NodeStacksView(reader: OGTraceReader, cmpTo: OGTraceReader) extends JPanel2(new BorderLayout()) {
  private val toolBar = new JToolBar2("Show In Details")
  private val nodeStacksTable = new NodeStacksTableForComparison(reader, cmpTo)
  private val fldDiffPercent = new JTextField("0.3")
  private val fldMinStarts = new JTextField("100")
  private val fldMinTime = new JTextField("100")

  def this(topGrp: PNodeTaskInfoGrp) = {
    this(null, null)
    nodeStacksTable.setList(List(topGrp))
  }

  init()
  setBorder(new EmptyBorder(2, 2, 2, 2))

  def cmdRefresh(): Unit = {
    val diffPercent = fldDiffPercent.getText.toDouble
    val minStarts = fldMinStarts.getText.toInt
    val minTime = MILLISECONDS.toNanos(fldMinTime.getText.toLong)

    val grp = FindJumpsInCache.findJumpInCacheUsage(diffPercent, minStarts, minTime, reader, cmpTo)
    NPTreeNodeExt.openToFirst(grp, { g: PNodeTaskInfoGrp => Math.abs(g.diffInCount.toDouble / g.count) < diffPercent })
    nodeStacksTable.setList(List(grp))
  }

  private def addField(toolBar: JToolBar, name: String, fld: JTextField): Unit = {
    val label = new JLabel(name)
    label.setBorder(new EmptyBorder(0, 5, 0, 5)) // padding
    toolBar.add(label)
    fld.setMaximumSize(new Dimension(40, Int.MaxValue))
    toolBar.add(fld)
  }

  private def init(): Unit = {
    toolBar.addButton("Refresh") { cmdRefresh() }
    toolBar.addSeparator()

    addField(toolBar, "Percent Change:", fldDiffPercent)
    addField(toolBar, "Minimum Starts:", fldMinStarts)
    addField(toolBar, "Minimum Cache Time:", fldMinTime)

    add(toolBar, BorderLayout.NORTH)
    add(nodeStacksTable, BorderLayout.CENTER)
  }
}
