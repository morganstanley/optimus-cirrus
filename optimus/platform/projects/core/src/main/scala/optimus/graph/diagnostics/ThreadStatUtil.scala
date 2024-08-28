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

import java.lang.management.ManagementFactory
import java.text.DecimalFormat
import com.sun.management.ThreadMXBean
import msjava.slf4jutils.scalalog.getLogger
import optimus.graph.cache.CacheFilter
import optimus.graph.cache.CacheGroup
import optimus.graph.cache.Caches
import optimus.graph.cache.CauseTestCase
import optimus.platform.util.ThreadDumper
import optimus.platform.AdvancedUtils
import sun.management.ManagementFactoryHelper

object ThreadStatUtil {

  val formatter = new DecimalFormat("0,000")

  val nsInSeconds = 1000000000
  val nsInMillis = 1000000
  val bytesInMb = 1024 * 1024

  def threadSummaryTable(summaries: List[ThreadStateSummary]): Unit = {
    log.info("---------------+---------------+---------------+---------------+---------------+---------------+")
    log.info("   GC CPU(ms)    JIT CPU (ms)     og CPU (ms)    og Alloc (Mb)   other CPU (ms)  other All (Mb) ")
    log.info("---------------+---------------+---------------+---------------+---------------+---------------+")
    summaries.foreach { summary =>
      val gcCPUSec = summary.perThreadGroup("gc").totalCpuTimeNs / nsInMillis
      val jitCPUSec = summary.perThreadGroup("jit").totalCpuTimeNs / nsInMillis
      val ogWorkerCpu = summary.perThreadGroup("og-worker").totalCpuTimeNs / nsInMillis
      val ogWorkerMbs = summary.perThreadGroup("og-worker").allocatedBytes / bytesInMb
      val otherCpu = summary.perThreadGroup("other").totalCpuTimeNs / nsInMillis
      val otherMbs = summary.perThreadGroup("other").allocatedBytes / bytesInMb

      log.info(f"$gcCPUSec%,15d  $jitCPUSec%,15d $ogWorkerCpu%,15d $ogWorkerMbs%,15d $otherCpu%,15d $otherMbs%,15d")
    }
    log.info("---------------+---------------+---------------+---------------+---------------+---------------+")

    def averageB10(summaries: List[ThreadStateSummary], fn: ThreadStateSummary => Long, divisor: Long): Long = {
      val values = summaries.map(fn)
      val best10 = values.sorted.take(10)
      if (best10.nonEmpty) best10.sum / best10.size / divisor else 0
    }

    val ogCpuB10 = averageB10(summaries, _.perThreadGroup("og-worker").totalCpuTimeNs, nsInMillis)
    val ogMemB10 = averageB10(summaries, _.perThreadGroup("og-worker").allocatedBytes, bytesInMb)

    log.info("Summary:")
    log.info(f"   OG CPU(10):  ${ogCpuB10}%,15dms")
    log.info(f"   OG Mem(10):  ${ogMemB10}%,15dMb")

  }

  def clearCaches(): Unit = {
    // clear all ScenarioDependent
    AdvancedUtils.clearCache(CauseTestCase, includeSI = false, includeLocal = true)
    // clear all ScenarioIndependent except if it looks DAL related
    Caches.clearCaches(
      CauseTestCase,
      includeCtorGlobal = true,
      includeSiGlobal = true,
      includeGlobal = false,
      includeNamedCaches = false,
      includePerPropertyCaches = false,
      filter = CacheFilter(
        { n =>
          val name = n.getClass.getName
          !name.contains("optimus.platform.dal")
        },
        "ExcludeDAL")
    )

  }

  private val log = getLogger[ThreadStatUtil.type]

  private val threadsBean = ManagementFactory.getThreadMXBean.asInstanceOf[ThreadMXBean]
  private val hotspotThreadingBean = ManagementFactoryHelper.getHotspotThreadMBean

  threadsBean.setThreadAllocatedMemoryEnabled(true)
  threadsBean.setThreadCpuTimeEnabled(true)

  final case class CacheStateSummary(perCacheGroup: Map[CacheGroup, CacheStats]) {
    def combine(that: CacheStateSummary): CacheStateSummary = {
      CacheStateSummary(
        CacheGroup.All
          .map(k =>
            k -> {
              val a = this.perCacheGroup(k)
              val b = that.perCacheGroup(k)
              CacheStats(
                a.cacheSlots + b.cacheSlots,
                a.cacheEntries + b.cacheEntries,
                a.cacheEntriesDelta + b.cacheEntriesDelta,
                a.cacheInserts + b.cacheInserts,
                a.cacheRemoveBulk + b.cacheRemoveBulk,
                a.cacheRemoveLru + b.cacheRemoveLru
              )
            })
          .toMap
      )
    }
  }

  final case class CacheStats(
      cacheSlots: Long,
      cacheEntries: Long,
      cacheEntriesDelta: Long,
      cacheInserts: Long,
      cacheRemoveBulk: Long,
      cacheRemoveLru: Long)

  object CacheStateSummary {
    def empty: CacheStateSummary = CacheStateSummary(Map.empty)
  }

  final case class ThreadStateSummary(perThreadGroup: Map[String, ThreadStatDelta]) {
    def displayTable: String = {
      "\n" +
        "   ThreadGroup     CPU(s)      Allocated(mb)\n" +
        "-----------------+----------+---------------\n" +
        perThreadGroup.toList
          .map { case (threadSet, deltas) =>
            f"$threadSet%10s ${deltas.totalCpuTimeNs / 1000000000}%,10d ${deltas.allocatedBytes / 1024 / 1024}%,10d"
          }
          .mkString("\n")
    }

    def combine(other: ThreadStateSummary): ThreadStateSummary = {
      val all = perThreadGroup.values ++ other.perThreadGroup.values

      val combined = all
        .groupBy(v => threadNameToSetName(v.threadName))
        .map { case (key, values) =>
          key -> values.tail.fold(values.head)(_.combine(_))
        }

      ThreadStateSummary(combined)
    }
  }

  object ThreadStateSummary {
    def empty: ThreadStateSummary = ThreadStateSummary(Map.empty)
  }

  final case class ThreadStateSnapshot(
      userSnapshots: Map[Long, ThreadStatRaw],
      systemSnapshots: Map[String, ThreadStatRaw])

  final case class ThreadStatRaw(
      threadName: String,
      threadId: Long,
      totalCpuTimeNs: Long,
      userCpuTimeNs: Long,
      allocatedBytes: Long)

  final case class ThreadStatDelta(
      threadName: String,
      threadId: Long,
      totalCpuTimeNs: Long,
      userCpuTimeNs: Long,
      allocatedBytes: Long) {

    def combine(other: ThreadStatDelta): ThreadStatDelta = {
      ThreadStatDelta(
        threadName,
        threadId,
        totalCpuTimeNs + other.totalCpuTimeNs,
        userCpuTimeNs + other.userCpuTimeNs,
        allocatedBytes + other.allocatedBytes)
    }
  }

  def threadsSnapshot(): ThreadStateSnapshot = {
    val activeThreads = ThreadDumper.allThreads
    val activeThreadIds = activeThreads.map(_.threadId)

    val userTimes = threadsBean.getThreadUserTime(activeThreadIds)
    val cpuTimes = threadsBean.getThreadCpuTime(activeThreadIds)
    val allocations = threadsBean.getThreadAllocatedBytes(activeThreadIds)

    val userThreadStats: Map[Long, ThreadStatRaw] = activeThreads.zipWithIndex.map { case (thread, idx) =>
      thread.threadId -> ThreadStatRaw(thread.getName, thread.threadId, cpuTimes(idx), userTimes(idx), allocations(idx))
    }.toMap

    import scala.jdk.CollectionConverters._
    // n.b. hotspotThreadingBean and hotspotGCBean return the same threads including GC and compilation
    val systemThreadsRaw = hotspotThreadingBean.getInternalThreadCpuTimes.asScala
    val systemThreads: Map[String, ThreadStatRaw] = systemThreadsRaw.map { case (name, cpuTime) =>
      name -> ThreadStatRaw(name, -1, cpuTime, 0, 0)
    }.toMap

    ThreadStateSnapshot(userThreadStats, systemThreads)
  }

  private def userThreadDeltas(
      userStart: Map[Long, ThreadStatRaw],
      userEnd: Map[Long, ThreadStatRaw]): collection.Seq[ThreadStatDelta] = {
    val allUserKeys = userStart.keySet ++ userEnd.keySet

    val userDeltas: collection.Seq[ThreadStatDelta] = allUserKeys.toList.flatMap { threadId =>
      (userStart.get(threadId), userEnd.get(threadId)) match {
        case (Some(startD), Some(endD)) =>
          if (startD.threadName != endD.threadName)
            log.warn(s"threadDeltas: Thread $threadId changed name from ${startD.threadName} to ${endD.threadName}")
          Some(
            ThreadStatDelta(
              endD.threadName,
              threadId,
              endD.totalCpuTimeNs - startD.totalCpuTimeNs,
              endD.userCpuTimeNs - startD.userCpuTimeNs,
              endD.allocatedBytes - startD.allocatedBytes))
        case (Some(startD), None) =>
          log.warn(s"threadDeltas: Thread $threadId '${startD.threadName}' terminated, no stats available")
          None
        case (None, Some(endD)) =>
          log.debug(s"New Thread detected $threadId '${endD.threadName}'")
          Some(
            ThreadStatDelta(
              endD.threadName,
              endD.threadId,
              endD.totalCpuTimeNs,
              endD.userCpuTimeNs,
              endD.allocatedBytes))
        case (None, None) =>
          // unreachable state, we are looking up the combined keysets
          None
      }
    }

    userDeltas
  }

  private def systemThreadDeltas(
      start: Map[String, ThreadStatRaw],
      end: Map[String, ThreadStatRaw]): collection.Seq[ThreadStatDelta] = {

    val allKeys = start.keySet ++ end.keySet
    val systemDeltas: collection.Seq[ThreadStatDelta] = allKeys.toList.flatMap { threadName =>
      (start.get(threadName), end.get(threadName)) match {
        case (Some(startD), Some(endD)) =>
          List(
            ThreadStatDelta(
              endD.threadName,
              -1,
              endD.totalCpuTimeNs - startD.totalCpuTimeNs,
              endD.userCpuTimeNs - startD.userCpuTimeNs,
              endD.allocatedBytes - startD.allocatedBytes))
        case (Some(startD), None) =>
          log.warn(s"threadDeltas: Thread $threadName terminated, no stats available")
          None
        case (None, Some(endD)) =>
          log.debug(s"New Thread detected $threadName ")
          ThreadStatDelta(endD.threadName, -1, endD.totalCpuTimeNs, endD.userCpuTimeNs, endD.allocatedBytes)
          None
        case (None, None) =>
          // unreachable state, we are looking up the combined keysets
          None
      }
    }

    systemDeltas
  }

  /** Given two thread snapshots compute a summary of thread activity */
  def threadsSummary(startSnapshot: ThreadStateSnapshot, endSnapshot: ThreadStateSnapshot): ThreadStateSummary = {

    val userDeltasOrig = userThreadDeltas(startSnapshot.userSnapshots, endSnapshot.userSnapshots)

    // filter out JFR related threads
    val userDeltas = userDeltasOrig.filterNot(_.threadName.contains("JFR "))
    val systemDeltas = systemThreadDeltas(startSnapshot.systemSnapshots, endSnapshot.systemSnapshots)

    val combined = (userDeltas ++ systemDeltas)
      .groupBy(v => threadNameToSetName(v.threadName))
      .map { case (k, values) =>
        k -> values.tail.fold(values.head)(_.combine(_))
      }

    ThreadStateSummary(combined)
  }

  private val ThreadSet_OG_Worker = "og-worker"
  private val ThreadSet_Other = "other"
  private val ThreadSet_GC = "gc"
  private val ThreadSet_JIT = "jit"

  /**
   * Map individual thread names into well known groups.
   * @param name
   *   The thread name
   * @return
   *   The thread group name which the given thread name belongs to.
   */
  private def threadNameToSetName(name: String): String = {
    if (name.contains("og.worker") || name.equals("main") || name.equals("main-EventThread")) ThreadSet_OG_Worker
    else if (name.contains("GC")) ThreadSet_GC
    else if (name.startsWith("C2 CompilerThread") || name.startsWith("C1 CompilerThread")) ThreadSet_JIT
    else ThreadSet_Other
  }
}
