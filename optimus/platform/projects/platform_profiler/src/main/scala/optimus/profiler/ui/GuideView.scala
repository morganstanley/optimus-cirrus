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
import java.io.File
import optimus.graph.OGTrace
import optimus.graph.diagnostics.Debugger
import optimus.profiler.DebuggerUI._
import optimus.profiler.recipes.Recipes
import optimus.profiler.ui.common.FileChooser
import optimus.profiler.ui.common.JPanel2
import optimus.profiler.ui.common.JToolBar2
import optimus.profiler.ui.common.JUIUtils
import optimus.profiler.ui.controls.JTextPaneScrollable
import optimus.utils.MiscUtils

import javax.swing.JButton
import scala.collection.mutable.ArrayBuffer

object GuideView {
  private val cmdCompare = "cmdFindJumpInCacheUsage"

  private val startStyle = "<style type='text/css'>" +
    s"body { font-family: Arial; font-size: ${Math.round(12 * Fonts.multiplier)}pt; } " +
    "</style>"

  private val text = s"<body>If you want to compare two runs that should have roughly same number of cache " +
    s"accesses, try <a href='$cmdCompare'>finding</a> the transition point from a similar access node " +
    "to a higher access point </body>"
}

class GuideView extends JPanel2(new BorderLayout()) {
  private val overviewPane = new JTextPaneScrollable()
  private val toolBar = new JToolBar2("Show In Details")
  private var advancedMultipleProcessesButton: JButton = _

  init()
  private def init(): Unit = {
    overviewPane.addLinkHandler(GuideView.cmdCompare) {
      overviewPane.getTextPane.showMessage("See code in console")
      GraphConsole.instance.writeInput(Recipes.findJumpsInCache.code)
    }

    toolBar.addButton("Load Trace") {
      JUIUtils.readFromFile(this, FileChooser.traceFileChooser, multiple = true) { file =>
        val fileName = file.getAbsolutePath.replace('\\', '/')
        val code = MiscUtils
          .codeString {
            loadTrace("$fileName", showTimeLine = true, showHotspots = true, showBrowser = true)
            val r$count = loadedTraces.last
          }
          ._2
          .replace("$fileName", fileName)
          .replace("$count", loadedTraces.size.toString)
        GraphConsole.execute(code)
      }
    }

    def addLoadTracesButton(title: String, multipleProcessesReader: Boolean): JButton = {
      toolBar.addButton(title) {
        val files = ArrayBuffer[File]()
        JUIUtils.readFromFile(this, FileChooser.traceFileChooser, multiple = true) { file =>
          files += file
        }
        val reader =
          if (multipleProcessesReader) OGTrace.readingFromFilesAcrossMultipleProcesses(files.toArray)
          else OGTrace.readingFromFiles(files.toArray)
        loadTrace(reader, showTimeLine = true, showHotspots = true, showBrowser = true, "merged")
      }
    }

    addLoadTracesButton("Load and Merge Traces", multipleProcessesReader = false)

    toolBar.addButton("Load and Merge Last Traces") {
      val lastDir = FileChooser.lastOGTraceDir
      if (lastDir != null) {
        val files = new File(lastDir).getParentFile.listFiles(FileChooser.ogtraceFilter)
        val reader = OGTrace.readingFromFiles(files)
        loadTrace(reader, showTimeLine = true, showHotspots = true, showBrowser = true, "merged")
      }
    }

    def addMultipleProcessesButton(enabled: Boolean): Unit = {
      if (enabled) {
        if (advancedMultipleProcessesButton eq null) {
          val b = addLoadTracesButton("Load and Merge Traces From Multiple Processes", multipleProcessesReader = true)
          advancedMultipleProcessesButton = b
        }
      } else {
        if (advancedMultipleProcessesButton ne null) {
          toolBar.remove(advancedMultipleProcessesButton)
          toolBar.repaint()
          advancedMultipleProcessesButton = null
        }
      }
    }

    if (Debugger.dbgShowAdvancedCmds.get)
      addMultipleProcessesButton(enabled = true)

    Debugger.registerAdvancedCommandChangedCallback(addMultipleProcessesButton)

    overviewPane.getTextPane.setContentType("text/html")
    overviewPane.getTextPane.setText("<html>" + GuideView.startStyle + GuideView.text + "</html>")

    add(toolBar, BorderLayout.NORTH)
    add(overviewPane, BorderLayout.CENTER)
  }
}
