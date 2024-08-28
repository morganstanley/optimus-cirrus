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
package optimus.graph.outOfProcess.views

import optimus.graph.DiagnosticSettings
import optimus.graph.JMXConnection
import optimus.graph.PropertyNode
import optimus.graph.cache.CacheFilter
import optimus.graph.cache.Caches
import optimus.graph.cache.CauseGraphMBean
import optimus.graph.diagnostics.EvictionReason

import scala.collection.mutable.ArrayBuffer

final case class CacheView(
    name: String,
    maxSize: Long,
    currentSize: Long,
    profMaxSizeUsed: Long,
    profLockContentionTime: Long,
    profLockNumAttempts: Int,
    profCacheBatchSize: Int,
    profCacheBatchSizePadding: Int,
    evictionOverflow: Int,
    evictionTotal: Int,
    evictionUserClear: Int,
    evictionGCMonitor: Int,
    evictionInvalidEntry: Int
) {
  import CacheViewHelper._

  def clear(filter: PropertyNode[_] => Boolean): Unit =
    if (DiagnosticSettings.outOfProcess) JMXConnection.graph.clearCache(name, filter)
    else clearCache(name, filter)
}

object CacheViewHelper {
  def getCache: ArrayBuffer[CacheView] = {
    if (DiagnosticSettings.outOfProcess) {
      JMXConnection.graph.getCache
    } else {
      import scala.collection.compat._
      Caches
        .allCaches()
        .map(cache => {
          val snap = cache.getCountersSnapshot
          CacheView(
            cache.getName,
            cache.getMaxSize,
            cache.getSize,
            cache.profMaxSizeUsed,
            snap.lockContentionTime,
            snap.updateLockAttempts.toInt,
            cache.profCacheBatchSize,
            cache.profCacheBatchSizePadding,
            snap.numEvictions(EvictionReason.overflow).toInt,
            snap.numEvictionsByCause.values.sum.toInt,
            snap.numEvictions(EvictionReason.explicitUserCleared).toInt,
            snap.numEvictions(EvictionReason.memoryPressure).toInt,
            snap.numEvictions(EvictionReason.invalidEntry).toInt
          )
        })
        .to(ArrayBuffer)
    }
  }

  private[graph] def clearCache(name: String, filter: PropertyNode[_] => Boolean): Unit = {
    for (cache <- Caches.allCaches()) {
      if (cache.getName.equals(name)) {
        cache.clear(CacheFilter.predicate(filter), CauseGraphMBean)
      }
    }
  }
}
