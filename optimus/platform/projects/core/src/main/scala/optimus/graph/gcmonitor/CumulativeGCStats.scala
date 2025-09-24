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
package optimus.graph.gcmonitor

import com.sun.management.GarbageCollectionNotificationInfo
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.Properties.Elem
import optimus.breadcrumbs.crumbs.Properties.Elems
import optimus.breadcrumbs.crumbs.PropertiesCrumb

// n.b. these are accumulated during the sampling period but then reset after every publication (so not cumulative
// over all time)
final case class CumulativeGCStats(
    nMinor: Int,
    nMajor: Int,
    duration: Long,
    durationMajor: Long,
    durationMinor: Long,
    minBefore: Double,
    maxBefore: Double,
    minAfter: Double,
    maxAfter: Double) {
  def combine(gc: CumulativeGCStats): CumulativeGCStats = {
    CumulativeGCStats(
      nMinor = nMinor + gc.nMinor,
      nMajor = nMajor + gc.nMajor,
      duration = duration + gc.duration,
      durationMajor = durationMajor + gc.durationMajor,
      durationMinor = durationMinor + gc.durationMinor,
      minBefore = Math.min(minBefore, gc.minBefore),
      maxBefore = Math.max(maxBefore, gc.maxBefore),
      minAfter = Math.min(minAfter, gc.minAfter),
      maxAfter = Math.max(maxAfter, gc.maxAfter)
    )
  }

  def elems: Elems = {
    val always = Elems(
      Properties.gcDuration -> duration,
      Properties.gcNumMajor -> nMajor,
      Properties.gcNumMinor -> nMinor,
    )

    // don't report min/max if we are empty (because they are 0 and MaxValue which is not useful to see)
    if (this == CumulativeGCStats.empty) always
    else
      always ::: Elems(
        Properties.gcMaxUsedHeapAfterGC -> maxAfter,
        Properties.gcMinUsedHeapAfterGC -> minAfter,
        Properties.gcMaxUsedHeapBeforeGC -> maxBefore,
        Properties.gcMinUsedHeapBeforeGC -> minBefore
      )
  }

  def publish(): Unit = {
    Breadcrumbs(
      ChainedID.root,
      PropertiesCrumb(
        _,
        Crumb.GCSource,
        (elems + Elem(Properties.description, "stats")).m: _*
      )
    )
  }
}

object CumulativeGCStats {
  val empty: CumulativeGCStats = new CumulativeGCStats(0, 0, 0L, 0, 0, Double.MaxValue, 0.0, Double.MaxValue, 0.0)
  def apply(info: GarbageCollectionNotificationInfo): CumulativeGCStats = {
    val isMajor = info.getGcAction.contains("major")
    val gcInfo = info.getGcInfo
    val usages = GCHeapUsagesMb(info)
    CumulativeGCStats(
      nMinor = if (isMajor) 0 else 1,
      nMajor = if (isMajor) 1 else 0,
      duration = gcInfo.getDuration,
      durationMajor = if (isMajor) gcInfo.getDuration else 0,
      durationMinor = if (isMajor) 0 else gcInfo.getDuration,
      maxBefore = usages.before.total,
      minBefore = usages.before.total,
      maxAfter = usages.after.total,
      minAfter = usages.after.total
    )
  }
}
