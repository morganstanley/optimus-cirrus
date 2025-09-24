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
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.graph.GCMonitor.Cleanup
import optimus.logging.LoggingInfo
import optimus.platform.util.Version
import optimus.scalacompat.collection._

import scala.annotation.varargs
import scala.jdk.CollectionConverters._

object GCCrumbs {
  def sendCrumb(
      description: String,
      info: GarbageCollectionNotificationInfo,
      includeSI: Boolean,
      gcUsages: GCHeapUsagesMb,
      ratio: Double,
      triggerRatio: Double,
      maxHeapMB: Double,
      cleanupsTriggered: Long,
      sadMinorsSinceMajor: Long,
      cleanup: Option[Cleanup] = None,
      msgs: List[String] = Nil,
      kill: Boolean = false): Unit = {
    import gcUsages._
    val msg = (msgs ::: cleanup.fold[List[String]](Nil)(_.msgs)).mkString(";")
    val desc = if (kill || cleanup.exists(_.kill)) description + ".kill" else description

    val props: Properties.Elems = Version.verboseProperties ++ Seq(
      Properties.host -> LoggingInfo.getHost,
      Properties.description -> desc,
      Properties.gcCause -> info.getGcCause,
      Properties.gcUsedHeapBeforeGC -> before.total,
      Properties.gcUsedHeapAfterGC -> after.total,
      Properties.gcUsedOldGenBeforeGC -> before.oldGen,
      Properties.gcUsedOldGenAfterGC -> after.oldGen,
      Properties.gcUsedHeapAfterCleanup -> cleanup.fold(-1.0)(_.heap.total),
      Properties.gcUsedOldGenAfterCleanup -> cleanup.fold(-1.0)(_.heap.oldGen),
      Properties.gcRatio -> ratio,
      Properties.triggerRatio -> triggerRatio,
      Properties.gcMaxOldGen -> after.maxOldGen,
      Properties.gcMaxHeap -> maxHeapMB,
      Properties.gcFinalizerCountAfter -> cleanup.fold(-1)(_.heap.finalizerCount),
      Properties.gcPools -> prettyPools(info),
      Properties.includeSI -> includeSI,
      Properties.gcAction -> info.getGcAction,
      Properties.gcName -> info.getGcName,
      Properties.gcDuration -> info.getGcInfo.getDuration,
      Properties.gcCacheRemoved -> cleanup.fold(-1)(_.removed),
      Properties.gcCacheRemaining -> cleanup.fold(-1)(_.remaining),
      Properties.gcCleanupsFired -> cleanupsTriggered,
      Properties.gcMinorsSinceMajor -> sadMinorsSinceMajor,
      Properties.logMsg -> msg
    )

    Breadcrumbs(
      ChainedID.root,
      PropertiesCrumb(
        _,
        Crumb.GCSource,
        props
      )
    )
  }

  private def prettyPools(info: GarbageCollectionNotificationInfo) = {
    val m1 = info.getGcInfo.getMemoryUsageBeforeGc.asScala
    val m2 = info.getGcInfo.getMemoryUsageAfterGc.asScala
    (m1.mapValuesNow(_.toString).toMap, m2.mapValuesNow(_.toString).toMap)
  }

  private val crumbBase =
    Properties.Elems(Properties.host -> LoggingInfo.getHost) ++ LoggingInfo.gsfControllerId.map(gsfControllerId =>
      Properties.Elem(Properties.gsfControllerId, gsfControllerId))
  @varargs
  def gcCrumb(elems: Properties.Elem[_]*): Unit = {
    if (Breadcrumbs.collecting) {
      Breadcrumbs.send(PropertiesCrumb(ChainedID.root, Crumb.GCSource, crumbBase ++ elems))
    }
  }
}
