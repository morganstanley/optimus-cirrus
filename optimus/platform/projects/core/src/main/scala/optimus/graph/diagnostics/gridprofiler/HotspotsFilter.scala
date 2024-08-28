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

import optimus.graph.diagnostics.PNodeTaskInfo
import scala.collection.{mutable => m}

// applies default filtering for hotspots
// note: ratio is 0.95 when a filter is specified as "95%"
final case class HotspotFilter(filter: Filter.Value, ftype: FilterType.Value, ratio: Double)

object Filter extends Enumeration {
  type Filter = Value
  val STARTS, EVICTIONS, INVALIDATES, CACHEHITS, CACHEMISSES, CACHETIME, CACHEBENEFIT, AVGCACHETIME, WALLTIME, SELFTIME,
      ANCSELTIME, SELF_PLUS_ANCSELTIME = Value
}

object FilterType extends Enumeration {
  type FilterType = Value
  val NONE, TOTAL, FIRST = Value
}

object HotspotFilter {

  private def filterToAccessor(f: Filter.Value): PNodeTaskInfo => Double = f match {
    case Filter.STARTS               => _.start.toDouble
    case Filter.EVICTIONS            => _.evicted.toDouble
    case Filter.INVALIDATES          => _.invalidated
    case Filter.CACHEHITS            => _.cacheHit.toDouble
    case Filter.CACHEMISSES          => _.cacheMiss.toDouble
    case Filter.CACHETIME            => _.cacheTime.toDouble
    case Filter.CACHEBENEFIT         => _.cacheBenefit.toDouble
    case Filter.AVGCACHETIME         => _.avgCacheTime.toDouble
    case Filter.WALLTIME             => _.wallTime.toDouble
    case Filter.SELFTIME             => _.selfTime.toDouble
    case Filter.ANCSELTIME           => _.ancSelfTime.toDouble
    case Filter.SELF_PLUS_ANCSELTIME => pnti => (pnti.selfTime + pnti.ancSelfTime).toDouble
  }

  // The meaning of the two supported FilterTypes:
  //
  // Filter type #1: X% of Total
  // e.g. 90% of total Started
  // given a table of starts {10, 10, 5, 3, 2, 1, 0, 0, 0, 0}, sum is 31, 90% of that is 28,
  // so we're keeping the nodes whose start times are {10, 10, 5, 3}
  //
  // Filter type #2: first X% rows in descending order
  // e.g. keeping first 25% rows in descending Start order:
  // given a table of starts {10, 10, 5, 3, 2, 1, 0, 0, 0, 0}, there are 10 node types, first 25% are the first 3
  // so we're keeping the nodes whose start times are {10, 10, 5}
  //

  private[optimus] def filterHotspots(
      hs: m.ArrayBuffer[PNodeTaskInfo],
      f: HotspotFilter
  ): m.ArrayBuffer[PNodeTaskInfo] = {
    val totals: PNodeTaskInfo = hs.reduceOption(_.combine(_)).getOrElse(new PNodeTaskInfo(0))
    f.ftype match {
      case FilterType.NONE => hs
      case FilterType.FIRST =>
        val sorted = hs.sortBy(filterToAccessor(f.filter)).reverse
        val filtered = sorted.take(Math.round(hs.size * f.ratio).toInt)
        filtered
      case FilterType.TOTAL =>
        val sorted = hs.sortBy(filterToAccessor(f.filter)).reverse
        // the limit, e.g. 28 in the example above
        val limit = filterToAccessor(f.filter)(totals) * f.ratio
        // walk in descending order, accumulating the metric
        // we will take pntis that are encountered before the accumulating sum crosses the limit
        // in the example above, scanLeft gives 0, 10, 20, 25, 28, 30, 31, 31, 31, 31, 31 (except lazy)
        // then indexWhere returns 4
        // and so we selet the first 4 hotspots: 10, 10, 5, 3
        val cutOff = sorted.iterator.scanLeft(0.0) { _ + filterToAccessor(f.filter)(_) }.indexWhere(_ >= limit)
        val filtered = sorted.take(cutOff)
        filtered
    }
  }
}
