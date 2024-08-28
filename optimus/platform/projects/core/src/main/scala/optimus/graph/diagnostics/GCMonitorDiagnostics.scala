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
package optimus.graph.diagnostics

import scala.collection.concurrent
import scala.jdk.CollectionConverters._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

trait CounterBasedDiagnostics {
  val map: concurrent.Map[String, AtomicLong] = new ConcurrentHashMap[String, AtomicLong]().asScala

  def incrementCounter(toIncrement: String, time: Long = 1): Unit = {
    map.putIfAbsent(toIncrement, new AtomicLong(time)) match {
      case Some(oldValue) =>
        oldValue.addAndGet(time)
      case None => ()
    }
  }

  def getCounter(key: String): Long = {
    map.get(key).fold(0L) { _.get() }
  }
}

object GCMonitorDiagnostics extends CounterBasedDiagnostics {
  val timeBasedTriggerRatioHit = "timeBasedTriggerRatioHit"
  val timeBasedKillTriggerHit = "timeBasedKillTriggerHit"
  val heapBasedTriggerHit = "heapBasedTriggerHit"
  val heapBasedIncludeSITriggerHit = "heapBasedIncludeSITriggerHit"
  val heapBasedBackoffTriggerHit = "heapBasedBackoffTriggerHit"
  val totalNumberNodesCleared = "totalNumberNodesCleared"
}

object GCMonitorStats {
  val empty = GCMonitorStats(0, 0, 0, 0, 0, 0)

  def diff(start: GCMonitorStats, end: GCMonitorStats): GCMonitorStats = {
    GCMonitorStats(
      end.timeBasedTriggerRatioHit - start.timeBasedTriggerRatioHit,
      end.timeBasedKillTriggerHit - start.timeBasedKillTriggerHit,
      end.heapBasedTriggerHit - start.heapBasedTriggerHit,
      end.heapBasedIncludeSITriggerHit - start.heapBasedIncludeSITriggerHit,
      end.heapBasedBackoffTriggerHit - start.heapBasedBackoffTriggerHit,
      end.totalNumberNodesCleared - start.totalNumberNodesCleared
    )
  }

  def snap(): GCMonitorStats = GCMonitorStats(
    GCMonitorDiagnostics.getCounter(GCMonitorDiagnostics.timeBasedTriggerRatioHit),
    GCMonitorDiagnostics.getCounter(GCMonitorDiagnostics.timeBasedKillTriggerHit),
    GCMonitorDiagnostics.getCounter(GCMonitorDiagnostics.heapBasedTriggerHit),
    GCMonitorDiagnostics.getCounter(GCMonitorDiagnostics.heapBasedIncludeSITriggerHit),
    GCMonitorDiagnostics.getCounter(GCMonitorDiagnostics.heapBasedBackoffTriggerHit),
    GCMonitorDiagnostics.getCounter(GCMonitorDiagnostics.totalNumberNodesCleared)
  )
}

final case class GCMonitorStats(
    timeBasedTriggerRatioHit: Long, // GC Monitor Time Strategy : number of trigger ratio hit
    timeBasedKillTriggerHit: Long, // GC Monitor Time Strategy : number of kill trigger ratio hit
    heapBasedTriggerHit: Long, // GC Monitor Heap Strategy : number of hit
    heapBasedIncludeSITriggerHit: Long, // GC Monitor Heap Strategy : number of hit SI Included
    heapBasedBackoffTriggerHit: Long, // GC Monitor Heap Strategy Backoff
    totalNumberNodesCleared: Long // Total number of node cleared by GCMonitor
) {
  val cacheClearCount =
    timeBasedTriggerRatioHit + heapBasedTriggerHit + heapBasedIncludeSITriggerHit + heapBasedBackoffTriggerHit

  def combine(y: GCMonitorStats): GCMonitorStats = GCMonitorStats(
    timeBasedTriggerRatioHit + y.timeBasedTriggerRatioHit,
    timeBasedKillTriggerHit + y.timeBasedKillTriggerHit,
    heapBasedTriggerHit + y.heapBasedTriggerHit,
    heapBasedIncludeSITriggerHit + y.heapBasedIncludeSITriggerHit,
    heapBasedBackoffTriggerHit + y.heapBasedBackoffTriggerHit,
    totalNumberNodesCleared + y.totalNumberNodesCleared
  )
}

object GCStats {
  val empty = GCStats(0L, 0L, 0L, GCMonitorStats.empty, GCNativeStats.empty)
}

final case class GCStats(
    gcTimeStopTheWorld: Long, // stop the world GC time
    gcTimeAll: Long, // all GC time (including parallel etc, this contributes to CPU time)
    gcCount: Long, // total number of collections
    gcMonitorStats: GCMonitorStats,
    gcNativeStats: GCNativeStats
) {
  def combine(y: GCStats): GCStats = GCStats(
    gcTimeStopTheWorld + y.gcTimeStopTheWorld,
    gcTimeAll + y.gcTimeAll,
    gcCount + y.gcCount,
    gcMonitorStats.combine(y.gcMonitorStats),
    gcNativeStats.add(y.gcNativeStats)
  )
}

object GCNativeStats extends CounterBasedDiagnostics {
  // jumping through some hoops for java based ease of use
  object Keys {
    val heapChange = "gcnative.heapChange"
    val totalInvocations = "gcnative.totalInvocations"
    val totalInvocationsGC = "gcnative.totalInvocations.gc"
    val totalInvocationsMainCache = "gcnative.totalInvocations.mainCache"
    val totalInvocationsGlobalCache = "gcnative.totalInvocations.globalCache"
    val totalInvocationsPrivateCache = "gcnative.totalInvocations.privateCache"
  }
  def keys = Keys
  import Keys._

  val empty = GCNativeStats(0, 0, 0, 0, 0, 0)
  def snap() = GCNativeStats(
    heapChange = getCounter(heapChange),
    totalInvocations = getCounter(totalInvocations),
    totalInvocationsGC = getCounter(totalInvocationsGC),
    totalInvocationsMainCache = getCounter(totalInvocationsMainCache),
    totalInvocationsGlobalCache = getCounter(totalInvocationsGlobalCache),
    totalInvocationsPrivateCache = getCounter(totalInvocationsPrivateCache)
  )
  def diff(start: GCNativeStats, end: GCNativeStats) = end subtract start

}
final case class GCNativeStats(
    heapChange: Long,
    totalInvocations: Long,
    totalInvocationsGC: Long,
    totalInvocationsMainCache: Long,
    totalInvocationsGlobalCache: Long,
    totalInvocationsPrivateCache: Long
) {
  def add(that: GCNativeStats): GCNativeStats = combine(that, { _ + _ })
  def subtract(that: GCNativeStats): GCNativeStats = combine(that, { _ - _ })

  private def combine(that: GCNativeStats, combOp: (Long, Long) => Long): GCNativeStats = {
    GCNativeStats(
      heapChange = combOp(this.heapChange, that.heapChange),
      totalInvocations = combOp(this.totalInvocations, that.totalInvocations),
      totalInvocationsGC = combOp(this.totalInvocationsGC, that.totalInvocationsGC),
      totalInvocationsMainCache = combOp(this.totalInvocationsMainCache, that.totalInvocationsMainCache),
      totalInvocationsGlobalCache = combOp(this.totalInvocationsGlobalCache, that.totalInvocationsGlobalCache),
      totalInvocationsPrivateCache = combOp(this.totalInvocationsPrivateCache, that.totalInvocationsPrivateCache)
    )
  }
}
