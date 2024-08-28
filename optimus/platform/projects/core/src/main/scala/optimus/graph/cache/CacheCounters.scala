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
package optimus.graph.cache
import optimus.graph.diagnostics.EvictionReason

import java.util
import java.util.concurrent.atomic.AtomicLong

/**
 * counters for cache stats
 * These are sampled by other threads (at present for EngineExecuted crumb at start/end of task execution)
 * These counters always increase in size
 *
 * Moving some counters out of BaseUNodeCache to separate concerns
 *
 * Adopting terminology from OpenTelemetry
 * https://opentelemetry.io/docs/specs/otel/metrics/supplementary-guidelines/
 * All these are monotonically increasing counters, except for indicative cache size, which is an up-down counter
 */
class CacheCounters {
  // collected in nanoseconds (displayed in profiler in milliseconds)
  private val lockContentionTime = new AtomicLong(0)

  private val updateLockAttempts = new AtomicLong(0)

  private val numEvictionsByCause = {
    val map = new util.EnumMap[EvictionReason, AtomicLong](classOf[EvictionReason])
    for (reason <- EvictionReason.values) { map.put(reason, new AtomicLong(0)) }
    map
  }

  private val nodesRemovedCounter = new AtomicLong()

  def addLockContentionTime(time: Long): Unit = {
    lockContentionTime.getAndAdd(time)
  }

  def incrementUpdateLockAttempt(): Unit = {
    updateLockAttempts.getAndIncrement()
  }

  def addNumEvictionsByCause(reason: EvictionReason, count: Long): Unit = {
    numEvictionsByCause.get(reason).getAndAdd(count)
  }

  def nodesRemoved(removed: Long): Unit = {
    nodesRemovedCounter.getAndAdd(removed)
  }

  // direct access not encouraged, best to use snapshot
  def getUpdateLockAttempts(): Long = {
    updateLockAttempts.get()
  }

  def getNumEvictionsByCause(reason: EvictionReason): Long = {
    numEvictionsByCause.get(reason).get()
  }

  def getLockContentionTime(): Long = {
    lockContentionTime.get()
  }

  def snap(indicativeCacheSize: Long, insertCount: Long, cacheSlots: Long): CacheCountersSnapshot = {
    CacheCountersSnapshot(
      lockContentionTime.get(),
      updateLockAttempts.get(),
      EvictionReason.values.map(k => k -> numEvictionsByCause.get(k).get).toMap,
      nodesRemovedCounter.get(),
      indicativeCacheSize, // cache entries
      insertCount,
      cacheSlots
    )
  }
}

final case class CacheCountersSnapshot(
    lockContentionTime: Long,
    updateLockAttempts: Long,
    numEvictionsByCause: Map[EvictionReason, Long],
    nodesRemoved: Long,
    indicativeCacheSize: Long,
    insertCount: Long,
    cacheSlots: Long) {

  def numEvictions(reason: EvictionReason): Long = numEvictionsByCause(reason)
}

sealed trait CacheGroup {
  val shortName: String
}

object CacheGroup {
  val All: Seq[CacheGroup] = Seq(CacheGroupGlobal, CacheGroupSiGlobal, CacheGroupCtorGlobal, CacheGroupOthers)
}

case object CacheGroupGlobal extends CacheGroup {
  val shortName = "Global"
}
case object CacheGroupSiGlobal extends CacheGroup {
  val shortName = "SI"

}
case object CacheGroupCtorGlobal extends CacheGroup {
  val shortName = "Ctor"
}
case object CacheGroupOthers extends CacheGroup {
  val shortName = "Others"
}

object CacheCountersSnapshot {
  val Empty = CacheCountersSnapshot(0L, 0L, EvictionReason.values.map(_ -> 0L).toMap, 0L, 0, 0, 0)

  // be careful to differentiate between absolute and relative (delta)
  def diff(start: CacheCountersSnapshot, end: CacheCountersSnapshot): CacheCountersSnapshot = {
    CacheCountersSnapshot(
      end.lockContentionTime - start.lockContentionTime,
      end.updateLockAttempts - start.updateLockAttempts,
      EvictionReason.values.map(k => k -> (end.numEvictionsByCause(k) - start.numEvictionsByCause(k))).toMap,
      end.nodesRemoved - start.nodesRemoved,
      end.indicativeCacheSize - start.indicativeCacheSize,
      end.insertCount - start.insertCount,
      end.cacheSlots - start.cacheSlots
    )
  }

  def sum(snap1: CacheCountersSnapshot, snap2: CacheCountersSnapshot): CacheCountersSnapshot = {
    CacheCountersSnapshot(
      snap1.lockContentionTime + snap2.lockContentionTime,
      snap1.updateLockAttempts + snap2.updateLockAttempts,
      EvictionReason.values.map(k => k -> (snap1.numEvictionsByCause(k) + snap2.numEvictionsByCause(k))).toMap,
      snap1.nodesRemoved + snap2.nodesRemoved,
      snap1.indicativeCacheSize + snap2.indicativeCacheSize,
      snap1.insertCount + snap2.insertCount,
      snap1.cacheSlots + snap2.cacheSlots
    )
  }

}
