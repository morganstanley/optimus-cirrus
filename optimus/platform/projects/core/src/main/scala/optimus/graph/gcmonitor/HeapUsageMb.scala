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
import optimus.scalacompat.collection._
import optimus.utils.SystemFinalization

import java.lang.management.ManagementFactory
import java.lang.management.MemoryPoolMXBean
import java.lang.management.MemoryType
import java.lang.management.MemoryUsage
import scala.jdk.CollectionConverters._

private[graph] final case class HeapUsageMb(
    total: Double,
    maxTotal: Double,
    oldGen: Double,
    maxOldGen: Double,
    finalizerCount: Int)
private[graph] object HeapUsageMb {

  private def toMb[A](as: Iterable[A], toUsage: A => MemoryUsage, get: MemoryUsage => Long): Double =
    as.map(toUsage(_)).map(get(_)).sum.toDouble / (1024.0 * 1024.0)

  private def mapToMb(mu: Map[String, MemoryUsage], contains: String, get: MemoryUsage => Long): Double =
    toMb[MemoryUsage](mu.filter(_._1.contains(contains)).values, identity, get)

  private def beansToMb(pools: collection.Seq[MemoryPoolMXBean], contains: String, get: MemoryUsage => Long): Double =
    toMb[MemoryPoolMXBean](pools.filter(_.getName.contains(contains)), _.getUsage, get)

  private val memoryBeans = ManagementFactory.getMemoryPoolMXBeans.asScala
  private val pools = memoryBeans.filter(b => b.isValid && b.getType == MemoryType.HEAP)
  private val poolNames = pools.map(_.getName).toSet

  /** gets current heap usage from MX bean */
  def apply(): HeapUsageMb = {
    HeapUsageMb(
      total = beansToMb(pools, "", _.getUsed),
      maxTotal = beansToMb(pools, "", _.getMax),
      oldGen = beansToMb(pools, "Old Gen", _.getUsed),
      maxOldGen = beansToMb(pools, "Old Gen", _.getMax),
      finalizerCount = SystemFinalization.getObjectPendingFinalizationCount
    )
  }
  def apply(usageMap: Map[String, MemoryUsage], finalizers: Option[Int]): HeapUsageMb = {
    // exclude non-heap pools
    val poolUsage = usageMap.filterKeysNow(poolNames)
    HeapUsageMb(
      total = mapToMb(poolUsage, "", _.getUsed),
      maxTotal = mapToMb(poolUsage, "", _.getMax),
      oldGen = mapToMb(poolUsage, "Old Gen", _.getUsed),
      maxOldGen = mapToMb(poolUsage, "Old Gen", _.getMax),
      finalizerCount = finalizers.getOrElse(-1)
    )
  }
}

/** Heap usage before and after GC */
private[optimus] final case class GCHeapUsagesMb(before: HeapUsageMb, after: HeapUsageMb)

private[graph] object GCHeapUsagesMb {

  /** gets before and after heap usage from GC notification */
  def apply(info: GarbageCollectionNotificationInfo, finalizers: Option[Int] = None): GCHeapUsagesMb = {
    GCHeapUsagesMb(
      before = HeapUsageMb(info.getGcInfo.getMemoryUsageBeforeGc.asScala.toMap, None),
      after = HeapUsageMb(info.getGcInfo.getMemoryUsageAfterGc.asScala.toMap, finalizers)
    )
  }
}
