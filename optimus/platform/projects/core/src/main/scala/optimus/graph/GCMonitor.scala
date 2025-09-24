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
package optimus.graph
import optimus.graph.DiagnosticSettings.getBoolProperty
import optimus.graph.gcmonitor.CumulativeGCStats
import optimus.graph.gcmonitor.HeapUsageMb
import optimus.graph.tracking.TrackingGraphCleanupTrigger

trait GCMonitor {
  def snapAndResetStats(consumer: String): CumulativeGCStats
  def snapStats(consumer: String): CumulativeGCStats
  def forceGC(): Unit
  def monitor(): Unit
  def kill(msg: String): Unit
}

object GCMonitor {
  val useLegacyGCMonitor = System.getProperty("optimus.gc.useLegacyGCMonitor", "true").toBoolean

  lazy val instance: GCMonitor = {
    if (useLegacyGCMonitor) LegacyGCMonitor.instance
    else throw new RuntimeException("Only LegacyGCMonitor is currently supported")
  }

  def startIfEnabled(): Unit = {
    if (!getBoolProperty("optimus.gc.disable", false))
      instance.monitor()
  }

  private[graph] final case class Cleanup(
      heap: HeapUsageMb,
      removed: Int,
      remaining: Int,
      msgs: List[String] = Nil,
      kill: Boolean = false)
}

private[graph] object GCMonitorTrigger extends TrackingGraphCleanupTrigger
