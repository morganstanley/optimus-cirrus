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

import java.lang.management.ManagementFactory

import optimus.graph.cache.Caches
import optimus.graph.diagnostics.messages.CacheEntriesCounter
import optimus.graph.diagnostics.messages.HeapMemoryCounter
import optimus.graph.diagnostics.messages.NativeMemoryCounter
import optimus.platform.util.Log

private[optimus] object OGTraceIsolation extends Log {

  private val sleepMs = DiagnosticSettings.getIntProperty("optimus.graph.timeline.period.ms", 1000)

  @volatile private var ogTracing: Boolean = false
  private[graph] def setOGTracing(x: Boolean): Unit = {
    ensureInitialized()
    ogTracing = x
    if (x) thread.interrupt()
  }
  def isTracing: Boolean = ogTracing

  private val thread = new Thread() {
    override def run(): Unit = {
      while (true) {
        try {
          if (OGTraceIsolation.isTracing) {
            NativeMemoryCounter.report(GCNative.getNativeAllocation)
            HeapMemoryCounter.report(ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed)
            CacheEntriesCounter.report(Caches.approxNumCacheEntries)
            Thread.sleep(sleepMs)
          } else
            Thread.sleep(60000)
        } catch {
          case _: InterruptedException =>
            if (ogTracing)
              log.info(s"Restarting timeline update thread, tracing=$ogTracing")
        }
      }
    }
  }

  private var initialized = false
  private def ensureInitialized(): Unit = {
    val doInitialize = this.synchronized {
      if (initialized) false
      else {
        initialized = true
        true
      }
    }
    if (doInitialize) {
      log.info(s"Starting timeline update thread, tracing=$ogTracing")
      thread.setName("TimelineUpdateThread")
      thread.setDaemon(true)
      thread.start()
    }
  }

}
