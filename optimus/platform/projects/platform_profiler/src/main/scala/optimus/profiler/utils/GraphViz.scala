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

import optimus.graph.OGTrace
import optimus.graph.diagnostics.trace.OGEventsEdgesObserver
import optimus.graph.diagnostics.trace.OGTraceMode
import optimus.platform.inputs.GraphInputConfiguration
import optimus.platform.util.Log
import optimus.platform_profiler.config.StaticConfig

/** Helper for visualising code snippets */
object GraphViz extends Log {
  def tracingGraph(f: => Unit): Unit = {
    try {
      GraphInputConfiguration.setTraceMode(OGTraceMode.edges)
      f
      val digraph = new NodePntiWriter(OGEventsEdgesObserver.asAdjacencyView(true)).digraph
      log.info(s"Paste the graph into ${StaticConfig.string("vizir")}")
      log.info(s"\n$digraph")
    } catch {
      case e: Exception => log.error("Couldn't generate GraphViz representation!", e)
    } finally {
      OGEventsEdgesObserver.reset()
      GraphInputConfiguration.setTraceMode(OGTraceMode.none)
    }
  }
}
