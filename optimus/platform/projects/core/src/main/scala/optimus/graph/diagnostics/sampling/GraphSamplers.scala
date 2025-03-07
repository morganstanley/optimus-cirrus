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
package optimus.graph.diagnostics.sampling
import optimus.core.Collections
import optimus.graph.EnumCounter
import optimus.graph.GCMonitor
import optimus.graph.GCNative
import optimus.graph.OGLocalTables
import optimus.graph.OGSchedulerTimes
import optimus.graph.RemovableLocalTables
import optimus.graph.diagnostics.EvictionReason
import optimus.graph.diagnostics.SchedulerProfileEntry
import optimus.graph.diagnostics.messages.AccumulatedValue
import optimus.graph.diagnostics.messages.Accumulating
import optimus.graph.diagnostics.messages.AllOGCounters
import optimus.breadcrumbs.crumbs.Properties._
import optimus.graph.diagnostics.sampling.Cardinality.LogLogCounter
import optimus.graph.tracking.monitoring.QueueStats
import optimus.graph.tracking.DependencyTrackerRoot

import scala.jdk.CollectionConverters._
import optimus.scalacompat.collection._

import scala.collection.mutable.ArrayBuffer

//noinspection ScalaUnusedSymbol // ServiceLoader
class GraphSamplers extends SamplerProvider {
  import SamplingProfiler._
  def provide(sp: SamplingProfiler): Seq[SamplerTrait[_, _]] = {
    val util = new Util(sp)
    import util._
    val ss = ArrayBuffer.empty[SamplerTrait[_, _]]

    ss += Diff(_ => OGSchedulerTimes.getGraphStallTime(0) / NANOSPERMILLI, profStallTime)

    // graph, cache, self-time during epoch
    ss += new Sampler(
      sp,
      "hotspots",
      snapper = _ => OGLocalTables.getSchedulerTimes,
      process = (prev: Option[SchedulerProfileEntry], sp: SchedulerProfileEntry) => {
        prev.fold(SchedulerProfileEntry())(sp.since(_))
      },
      publish = (sp: SchedulerProfileEntry) =>
        Elems(
          profGraphTime -> sp.graphTime / NANOSPERMILLI,
          profSelfTime -> sp.selfTime / NANOSPERMILLI,
          profCacheTime -> sp.cacheTime / NANOSPERMILLI,
          profGraphSpinTime -> sp.spinTime / NANOSPERMILLI,
          profGraphWaitTime -> sp.waitTime / NANOSPERMILLI,
          profGraphIdleTime -> sp.idleTime / NANOSPERMILLI,
          profGraphCpuTime -> sp.cpuTime / NANOSPERMILLI
        )
    )

    ss += new Sampler(
      sp,
      "evictions",
      snapper = _ => {
        var snaps: List[EnumCounter[EvictionReason]] = Nil
        OGLocalTables.forAllRemovables { (table: RemovableLocalTables) =>
          val ec = table.evictionCounter
          if (ec.total() > 0)
            snaps ::= ec.snap()
        }
        snaps
      },
      process = (prev: Option[List[EnumCounter[EvictionReason]]], curr: List[EnumCounter[EvictionReason]]) => {
        val diffed = new EnumCounter(classOf[EvictionReason])
        for (c <- curr) diffed.add(c)
        for (ps <- prev; p <- ps) diffed.subtract(p)
        diffed
      },
      publish = (counts: EnumCounter[EvictionReason]) => {
        val m = counts.toMap.asScala.collect {
          case (k, v) if v > 0 =>
            k.name() -> v.toLong
        }.toMap
        Elems(profEvictions -> m)
      }
    )

    import optimus.graph.PluginType
    final case class PluginData(
        // Counts that strictly increase over the life of the process and can be diffed around intervals,
        // which in turn can be summed in dashboards.
        cumulativeCounts: Map[String, Map[String, Long]],
        // Snapshots cannot sensibly be summed over multiple intervals
        snapShots: Map[String, Map[String, Long]],
        fullWaitTimes: Map[String, Long],
        overflows: Map[String, Long]
    )

    // Number of nodes started during period, and number of nodes currently waiting
    ss += new Sampler[PluginType.PluginTracker, PluginData](
      sp,
      "plugin tracker",
      snapper = (_: Boolean) => PluginType.snapAggregatePluginCountsIntoGlobalBuffer(),
      process = { (prevOpt: Option[PluginType.PluginTracker], curr: PluginType.PluginTracker) =>
        prevOpt.fold {
          PluginData(curr.cumulativeCounts, curr.snapCounts, curr.fullWaitTimes.toMapMillis, curr.overflow.toMap)
        } { prev =>
          val diff = curr.diffCounters(prev)
          if (diff.activity > 0)
            sp.recordActivity()
          PluginData(diff.cumulativeCounts, curr.snapCounts, diff.fullWaitTimes.toMapMillis, diff.overflow.toMap)
        }
      },
      publish = { pc =>
        Elems(
          pluginCounts -> pc.cumulativeCounts,
          pluginSnaps -> pc.snapShots,
          pluginFullWaitTimes -> pc.fullWaitTimes,
          pluginNodeOverflows -> pc.overflows)
      }
    )

    if (sp.propertyUtils.get("optimus.sampling.cardinalities", false))
      ss += new Sampler(
        sp,
        "cardinalities",
        snapper = _ => {
          val counters = new Cardinality.Counters()
          OGLocalTables.forAllRemovables((table: RemovableLocalTables) => counters.add(table.getCardinalities))
          counters
        },
        process = (_: Option[Cardinality.Counters], curr: Cardinality.Counters) => curr.countEstimateMap,
        publish = (cardinalitiesMap: Map[Cardinality.Category, LogLogCounter]) => {
          val m = cardinalitiesMap.mapValuesNow(_.estimate).map { case (k, v) =>
            k.name -> Math.round(v).toInt
          }

          // In json estimators will look like, e.g., "vref" : { "0" : 37, "1" : .... }, which will make it a lot easier to compute maximum in splunk
          val estimators: Map[String, Map[String, Int]] = cardinalitiesMap.map { case (k, v) =>
            k.name -> v.estimators.zipWithIndex.map { case (v, i) =>
              i.toString -> v
            }.toMap
          }

          val raw: Map[String, Long] = cardinalitiesMap.map { case (k, v) =>
            k.name -> v.withDuplicates
          }

          val alphaNumerators: Map[String, Double] = cardinalitiesMap.map { case (k, v) =>
            k.name -> v.numerator
          }

          Elems(cardEstimated -> m, cardRaw -> raw, cardNumerator -> alphaNumerators, cardEstimators -> estimators)
        }
      )

    ss += new Sampler[PluginType.Counter, Map[String, Long]](
      sp,
      "stall times",
      snapper = (_: Boolean) => OGSchedulerTimes.snapCumulativePluginStallTimes(),
      process = (prevOption: Option[PluginType.Counter], c: PluginType.Counter) =>
        prevOption.fold[Map[String, Long]](Map.empty) { prev =>
          val stall = c.snap().getSafe
          stall.accumulate(prev, -1)
          stall.toMap.mapValuesNow(_ / NANOSPERMILLI)
        },
      publish = (m: Map[String, Long]) => {
        val s = m.map { case (ptype, time) =>
          Map("p" -> ptype, "t" -> time.toString())
        }.toSeq
        Elems(pluginStallTimes -> m)
      }
    )

    // State of DependencyTracker queues
    {
      type Cumul = Map[String, QueueStats]
      type Snap = Map[String, QueueStats.Snap]
      ss += new Sampler(
        sp,
        "dep tracker",
        snapper = _ => DependencyTrackerRoot.snap(),
        process = (previousSnap: Option[Cumul], newSnap: Cumul) => {
          val prev = previousSnap.getOrElse(Map.empty)
          val builder = Map.newBuilder[String, QueueStats.Snap]
          val keys = newSnap.keySet ++ prev.keySet
          for (k <- keys) {
            (prev.get(k), newSnap.get(k)) match {
              case (prev, Some(current)) =>
                builder += k -> QueueStats.diff(prev, current)

              case (Some(prev), None) =>
                builder += k -> QueueStats.gced(prev)

              case (None, None) =>
            }
          }

          builder.result()
        },
        publish = (snap: Snap) =>
          if (snap.isEmpty) Elems.Nil
          else
            Elems(
              profDepTrackerTaskAdded -> snap.mapValuesNow(_.added),
              profDepTrackerTaskProcessed -> snap.mapValuesNow(_.removed),
              profDepTrackerTaskQueued -> snap.mapValuesNow(_.currentSize)
            )
      )
    }

    // GC costs, according to GCMonitor
    ss += new SamplingProfiler.Sampler[GCMonitor.CumulativeGCStats, GCMonitor.CumulativeGCStats](
      sp,
      "GCMonitor cumulative",
      snapper = _ => GCMonitor.instance.snapAndResetStatsForSamplingProfiler(),
      process = (_, c) => c,
      publish = c => c.elems
    )

    // Publish diffs for all known accumulating counters
    val accumulatingCounters: Array[Accumulating[_, _ <: AccumulatedValue]] = AllOGCounters.allCounters.collect {
      case c: Accumulating[_, _] => c
    }
    ss += new Sampler[Seq[AccumulatedValue], Seq[AccumulatedValue]](
      sp,
      "accumulating counters",
      snapper = (_: Boolean) => accumulatingCounters.map(_.snap),
      process = (prevOption: Option[Seq[AccumulatedValue]], curr: Seq[AccumulatedValue]) =>
        prevOption.fold[Seq[AccumulatedValue]](Seq.empty) { prev: Seq[AccumulatedValue] =>
          Collections.map2(curr, prev) { case (c, p) =>
            c.diff(p)
          }
        },
      publish = (diffs: Seq[AccumulatedValue]) => Elems(diffs.map(_.elems).toSeq: _*)
    ) // toSeq to make 2.13 happy

    def untilGCN[T](after: => T, default: T) = if (GCNative.isLoaded) after else default

    ss += Snap(_ => untilGCN(GCNative.getNativeAllocationMB, 0), gcNativeAlloc)
    ss += Snap(_ => untilGCN(GCNative.managedSizeMB(), 0), gcNativeManagedAlloc)

    if (GCNative.usingJemalloc())
      ss += new Sampler[GCNative.JemallocSizes, Map[String, Int]](
        sp,
        "jemalloc stats",
        snapper = _ => GCNative.jemallocSizes(),
        process = (_, c) => c.toMapMB.asScala.toMap.mapValuesNow(_.toInt),
        publish = c => Elems(gcNativeStats -> c)
      )

    ss += Diff(_ => OGSchedulerTimes.getPreOptimusStartupTime / NANOSPERMILLI, profPreOptimusStartup)
    ss
  }

}
