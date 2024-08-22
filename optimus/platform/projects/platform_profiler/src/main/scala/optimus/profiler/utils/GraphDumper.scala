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
package optimus.profiler.utils

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.io.IOException
import java.util.{ArrayList => JArrayList}

import javax.swing.JOptionPane
import javax.swing.SwingUtilities
import optimus.graph.NodeTask
import optimus.profiler.NodeDump
import optimus.profiler.ui.browser.GraphBrowser

object GraphDumper {

  /** Start dumping the graph on a different thread! */
  def dump(browser: GraphBrowser, graphDump: JArrayList[NodeTask], file: File): Unit = {
    new GraphDumpThread(browser, graphDump, file).start()
  }

  // Consider splitting into publish-consumer pattern with blocking queue so we can parallelize the producers for larger dumps.
  // This would need to include the index of the node so ordering of writes was maintained for diffing etc. - not one for now though.
  class GraphDumpThread(browser: GraphBrowser, graphDump: JArrayList[NodeTask], f: File) extends Thread {
    private[profiler] def dump(): Unit = {
      val bw = new BufferedWriter(new FileWriter(f))
      try {
        SwingUtilities.invokeLater(() => if (browser ne null) browser.dumpGraphStarted())

        bw.write("[")

        // Re-using StringBuilder and clearing instead of re-allocating each iteration
        val sb = new java.lang.StringBuilder(1024 * 1024 * 5)
        val nodeDump = new NodeDump

        // Writes are buffered up to a size, but the in-memory size of now bounded
        var i = 0
        while (i < graphDump.size) {
          val ntsk = graphDump.get(i)
          nodeDump.setNodeTask(ntsk)
          nodeDump.dumpString(sb)
          if (i < graphDump.size - 1) {
            sb.append(",")
          }
          if ((i + 1) % 10000 == 0)
            GraphBrowser.log.info(s"dumped $i/${graphDump.size} nodes...")
          bw.write(sb.toString)
          sb.setLength(0)
          i += 1 // Index increment
        }
        bw.write("]")
        SwingUtilities.invokeLater(() => if (browser ne null) browser.dumpGraphSucceed())
      } catch {
        case e: Exception =>
          SwingUtilities.invokeLater(() => if (browser ne null) browser.dumpGraphFailed(e.getMessage))
          throw e
      } finally {
        try {
          bw.close()
        } catch {
          case _: IOException =>
            if (browser ne null)
              JOptionPane.showMessageDialog(browser, "Exception happened when closing the file descriptor")
        }
      }
    }

    override def run(): Unit = {
      dump()
    }
  }

}
