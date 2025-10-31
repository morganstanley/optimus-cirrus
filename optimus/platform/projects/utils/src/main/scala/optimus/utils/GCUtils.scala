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
package optimus.utils

import java.lang.management.ManagementFactory
import java.lang.management.MemoryPoolMXBean
import java.lang.management.MemoryType
import java.lang.management.MemoryUsage
import scala.jdk.CollectionConverters._
import com.sun.management.GarbageCollectionNotificationInfo
import javax.management.Notification
import javax.management.NotificationEmitter
import javax.management.NotificationListener
import msjava.slf4jutils.scalalog.getLogger
import javax.management.openmbean.CompositeData
import scala.collection.{mutable => m}

abstract class GCUtils {
  private val log = getLogger(this.getClass)
  private val listeners: m.Map[String, NotificationListener] = m.Map.empty
  private val gcPublishDuration =
    System.getProperty("optimus.dsi.gc.publish.duration.threshold.ms", "5000").toInt

  // Start Monitoring
  def start(): Unit = {
    ManagementFactory.getGarbageCollectorMXBeans.asScala
      .collect { case n: NotificationEmitter => n }
      .foreach(n =>
        n.addNotificationListener(listeners.getOrElseUpdate(n.getName, createListener(n.getName)), null, null))
    log.info(s"Started GC events publisher")
  }

  // stop monitoring
  def stop(): Unit = {
    ManagementFactory.getGarbageCollectorMXBeans.asScala
      .collect { case n: NotificationEmitter => n }
      .foreach(n => n.removeNotificationListener(listeners(n.getName), null, null))
  }

  protected def setCustomTracing(): Unit
  protected def sendCrumbs(props: Map[String, String]): Unit

  def createListener(x: String): NotificationListener = new NotificationListener {
    override def handleNotification(notification: Notification, handback: AnyRef): Unit = {
      notification match {
        case n if n.getType == GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION =>
          setCustomTracing()
          val info = GarbageCollectionNotificationInfo.from(notification.getUserData.asInstanceOf[CompositeData])
          val action = info.getGcAction
          val major = action.contains("major")
          val duration = info.getGcInfo.getDuration
          log.info(
            s"GC caused by ${info.getGcCause} finished with action message $action reported duration is $duration ms. The threshold is $gcPublishDuration ms")
          if (major || duration > gcPublishDuration) {
            val props = getGCCrumb(info)
            sendCrumbs(props)
          }
        case _ => // ignore other notifications
      }
    }
  }
  // Heap usage from beans.
  case class HeapUsageMb(total: Double, oldGen: Double, maxOldGen: Double)
  object HeapUsageMb {

    private def toMb[A](as: Iterable[A], toUsage: A => MemoryUsage, get: MemoryUsage => Long): Double =
      as.map(toUsage(_)).map(get(_)).sum.toDouble / (1024.0 * 1024.0)

    private def mapToMb(mu: Map[String, MemoryUsage], ends: String, get: MemoryUsage => Long): Double =
      toMb[MemoryUsage](mu.filter(_._1.endsWith(ends)).values, identity, get)

    private def beansToMb(pools: collection.Seq[MemoryPoolMXBean], ends: String, get: MemoryUsage => Long): Double =
      toMb[MemoryPoolMXBean](pools.filter(_.getName.endsWith(ends)), _.getUsage, get)

    def apply(): HeapUsageMb = {
      val pools = ManagementFactory.getMemoryPoolMXBeans.asScala.filter(b => b.isValid && b.getType == MemoryType.HEAP)
      HeapUsageMb(
        total = beansToMb(pools, "", _.getUsed),
        oldGen = beansToMb(pools, "Old Gen", _.getUsed),
        maxOldGen = beansToMb(pools, "Old Gen", _.getMax)
      )
    }
    def apply(usageMap: Map[String, MemoryUsage], finalizers: Option[Int]): HeapUsageMb =
      HeapUsageMb(
        total = mapToMb(usageMap, "", _.getUsed),
        oldGen = mapToMb(usageMap, "Old Gen", _.getUsed),
        maxOldGen = mapToMb(usageMap, "Old Gen", _.getMax)
      )
  }

  // Heap usage before and after GC, from notification info
  private case class GCHeapUsagesMb(before: HeapUsageMb, after: HeapUsageMb)

  private def getGCHeapUsages(
      info: GarbageCollectionNotificationInfo,
      finalizers: Option[Int] = None): GCHeapUsagesMb = {
    GCHeapUsagesMb(
      before = HeapUsageMb(info.getGcInfo.getMemoryUsageBeforeGc.asScala.toMap, None),
      after = HeapUsageMb(info.getGcInfo.getMemoryUsageAfterGc.asScala.toMap, finalizers)
    )
  }

  protected def getGCCrumb(info: GarbageCollectionNotificationInfo): Map[String, String] = {
    val usage = getGCHeapUsages(info = info)
    Map(
      "gcName" -> info.getGcName,
      "gcCause" -> info.getGcCause,
      "gcAction" -> info.getGcAction,
      "gcDuration" -> s"${info.getGcInfo.getDuration} ms",
      "gcUsedHeapBeforeGC" -> s"${usage.before.total} MB",
      "gcUsedHeapAfterGC" -> s"${usage.after.total} MB",
      "gcUsedOldGenBeforeGC" -> s"${usage.before.oldGen} MB",
      "gcUsedOldGenAfterGC" -> s"${usage.after.oldGen} MB"
    )
  }
}
