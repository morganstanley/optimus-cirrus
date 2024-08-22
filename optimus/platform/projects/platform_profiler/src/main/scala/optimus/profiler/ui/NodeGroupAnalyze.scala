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
import java.awt.event.KeyAdapter
import java.awt.event.KeyEvent
import javax.swing.JTextPane

import optimus.platform.util.Log
import optimus.profiler.DebuggerUI
import optimus.profiler.ui.common.JPanel2
import optimus.profiler.ui.common.JToolBar2

class NodeGroupAnalyze(nodeAnalyzer: NodeAnalyzer) extends JPanel2 with Log {
  private val table = new NodeGroupAnalyzeTable(nodeAnalyzer)

  init()

  private def init(): Unit = {
    setLayout(new BorderLayout(5, 5))

    add(table, BorderLayout.CENTER)
    add(createToolBar(), BorderLayout.NORTH)
  }

  private def createToolBar(): JToolBar2 = {
    val bar = new JToolBar2()

    val txtPane = new JTextPane()
    txtPane.addKeyListener(new KeyAdapter() {
      override def keyPressed(e: KeyEvent): Unit = {
        if (e.getKeyCode == KeyEvent.VK_ENTER && e.isControlDown)
          refreshWithConverter(txtPane.getText)
      }
    })
    txtPane.setToolTipText(
      "<html>Insert an expression to use as an alternative key for regrouping the result." +
        "<br>You <b>must</b> use <i>t</i> as your variable name. For example:" +
        "<br><br><i>t.toString</i>" +
        "<br><br>Entering an empty string will use the default grouping, i.e., the node result itself as a key." +
        "<br>NOTE: The result of the expression must be of type which is a subtype of <i>AnyRef</i>.</html>")

    bar.addButton("Refresh")(refreshWithConverter(txtPane.getText))
    bar.add(txtPane)
    bar
  }

  private def refreshWithConverter(expr: String): Unit = {
    try {
      if (expr.strip().nonEmpty) {
        val tpe = table.nodeResultType
        GraphConsole.instance.setResultLambda(tpe, expr)
      } else {
        // if empty string -- set the lambda to null and then the next refresh will
        // fall back to the original behavior
        DebuggerUI.currResultLambda = null
      }
      table.resetSource()
    } catch {
      case _: Exception => log.error(s"Could not refresh using $expr")
    } finally {
      // always clean up before the next refresh
      DebuggerUI.currResultLambda = null
    }
  }

  private[ui] def getTable: NodeGroupAnalyzeTable = table
}
