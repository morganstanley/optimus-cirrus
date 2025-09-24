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
package optimus.graph.diagnostics.gridprofiler

import optimus.graph.diagnostics.trace.OGTraceMode

/**
 * Grid profiler levels. These get progressively more invasive: each level includes the previous.
 *
 * NONE: record environment (JVM-level) metrics. Adds code to start/end of distribution ENVIRON: same as NONE, but will
 * dump CSV/shutdown hook summary of the environment metrics LIGHT: record DAL interactions, Scheduler state changes
 * (run/wait/idle): records sync stacks, stalls, etc. HOTSPOTSLIGHT: like hotspots but lower overhead (no PNodeTask
 * allocation) ~15% CPU, ~25% memory. Does not support profiled block scopes. HOTSPOTS: turns on OGTrace for minimum
 * trace profiling needed to to build per-property hotspots TRACEEVENTS: turns on OGTrace for full trace profilings
 */

object Level extends Enumeration {
  type Level = Value
  val NONE, EDGES, ENVIRON, LIGHT, RECORDING, RECORDINGWITHHASHES, AUTO_OPTIMIZER, TIMELINELIGHT, HOTSPOTSLIGHT,
      RECORDINGLIGHT, DISTRIBUTEDTASKS, HOTSPOTS, HOTSPOTSTIMELINE, HASHCOUNTS, TRACENODES = Value

  values.foreach(k => require(OGTraceMode.find(k.toString) != null))
}
