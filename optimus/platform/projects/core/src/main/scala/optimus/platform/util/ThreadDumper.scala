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
package optimus.platform.util

import java.io.Writer
import java.lang.management.ThreadInfo
import java.util.Date
import msjava.slf4jutils.scalalog.getLogger
import optimus.graph.OGScheduler
import optimus.graph.Scheduler.SchedulerSnapshot
import optimus.graph.SchedulerDiagnosticUtils.getThreadIds
import optimus.platform.stats.AppStatsControl

import scala.annotation.tailrec

/**
 * This object dumps the thread state in a format compatible with IntelliJ to ingest and inspect.
 */
object ThreadDumper {
  private val log = getLogger(this)
  private val bean = AppStatsControl.threadBean
  private val lineSeparator = System.getProperty("line.separator")

  private val MonitorPattern = """(.*)@(\p{XDigit}*)""".r

  object ThreadInfos {
    def apply(infos: => Map[Long, ThreadInfo]): ThreadInfos = {
      val start = System.currentTimeMillis()
      val data = infos
      val duration = System.currentTimeMillis() - start
      new ThreadInfos(data, duration)
    }

    val empty = new ThreadInfos(Map.empty, 0)
  }
  class ThreadInfos private (val infos: Map[Long, ThreadInfo], val captureTimeMs: Long = 0) {
    def forThreadId(threadId: Long): Option[ThreadInfo] = infos.get(threadId)
  }

  bean.setThreadContentionMonitoringEnabled(bean.isThreadContentionMonitoringSupported)

  def dumpThreadsToLog(summary: String, details: collection.Seq[String] = Nil): collection.Seq[ThreadInfo] = {
    val (dump, threadInfos) = dumpThreads(summary, details)
    log.warn(dump)
    threadInfos
  }
  def dumpThreadsToConsole(summary: String, details: collection.Seq[String] = Nil): collection.Seq[ThreadInfo] = {
    val (dump, threadInfos) = dumpThreads(summary, details)
    println(dump)
    threadInfos
  }
  def dumpThreadsToWriter(
      w: Writer,
      summary: String,
      details: collection.Seq[String] = Nil): collection.Seq[ThreadInfo] = {
    val (dump, threadInfos) = dumpThreads(summary, details)
    w.append(dump)
    w.flush()
    threadInfos
  }

  private def describeThread(threadO: Option[Thread]): String =
    threadO
      .map { thread =>
        s"""#${thread.threadId}${if (thread.isDaemon) " daemon" else ""} prio=${thread.getPriority} tid=NA nid=NA"""
      }
      .getOrElse("")

  private def describeMonitor(message: String, monitor: String, tabulated: Boolean = true): String = {
    val (className, identityHashCode) = monitor match {
      case MonitorPattern(name, code) => (name, code)
      case _                          => (monitor.getClass.getName, "0")
    }
    s"""${if (tabulated) "\t- "
      else "  "}$message <0x${"0" * (16 - identityHashCode.length)}$identityHashCode> (a $className)"""
  }

  def dumpThreadById(sb: PrettyStringBuilder, allThreadInfos: ThreadInfos, threadId: Long): Unit = {
    allThreadInfos.forThreadId(threadId) match {
      case None             => sb.appendln(s"can't find info for thread id $threadId")
      case Some(threadInfo) => dumpThreadInfo(threadInfo, None, sb)
    }
  }

  // synchronisation is required on access to the ThreadMXBean to avoid seg-faults on concurrent access
  def getAllThreadInfos(): ThreadInfos = ThreadDumper.synchronized {
    ThreadInfos(bean.dumpAllThreads(true, true).map { info => info.getThreadId -> info }.toMap)
  }

  // synchronisation is required on access to the ThreadMXBean to avoid seg-faults on concurrent access
  def getThreadInfos(
      ids: Array[Long],
      includeLocks: Boolean = true,
      excludeSpinningThread: Boolean = false): ThreadInfos = ThreadDumper.synchronized {
    ThreadInfos(
      bean
        .getThreadInfo(ids, includeLocks, includeLocks)
        .filter(_ ne null)
        .filterNot(s => excludeSpinningThread && isSpinningThread(s))
        .map { info => info.getThreadId -> info }
        .toMap)
  }

  // some callers (e.g. StallDetector) may prefer not to see the spinning thread (in OGScheduler#spin) because it is
  // continuously moving around
  private def isSpinningThread(i: ThreadInfo): Boolean =
    i.getStackTrace.exists(s => s.getMethodName == "spin" && s.getClassName == classOf[OGScheduler].getName)

  def getThreadInfosForQueues(
      schedulerQueues: List[SchedulerSnapshot],
      excludeSpinningThread: Boolean = false): ThreadInfos =
    getThreadInfos(getThreadIds(schedulerQueues.map(_.contexts)), excludeSpinningThread = excludeSpinningThread)

  def getThreadIdsByName(names: collection.Seq[String]): Array[Long] = {
    allThreads.filter(thread => names.exists(thread.getName.matches)).map(_.threadId)
  }

  def allThreads: Array[Thread] = {
    @tailrec def rootThreadGroup(t: ThreadGroup): ThreadGroup = {
      val parent = t.getParent
      if (parent eq null) t else rootThreadGroup(parent)
    }

    @tailrec def readAllThreads(root: ThreadGroup, attemptedSize: Int): Array[Thread] = {
      val res = new Array[Thread](attemptedSize)
      val count = root.enumerate(res, true)
      if (count == attemptedSize) readAllThreads(root, attemptedSize * 2) else res.take(count)
    }

    // ThreadGroup can be null if the thread has been terminated
    val currentThreadGroup = Thread.currentThread().getThreadGroup
    if (currentThreadGroup eq null) {
      // If null, then fall back to original (potentially less efficient) method of getting the threadIds
      val threads = Thread.getAllStackTraces.keySet()
      threads.toArray(new Array[Thread](threads.size()))
    } else {
      // we could ask the Thread mx bean for the count of threads, but that would be additional overhead
      // and probably not needed
      readAllThreads(rootThreadGroup(currentThreadGroup), 1000)
    }
  }

  def dumpThreads(summary: String, details: collection.Seq[String] = Nil): (String, collection.Seq[ThreadInfo]) = {
    val result = new PrettyStringBuilder
    val threadInfos = bean.dumpAllThreads(true, true)
    val allThreadsMap = allThreads.map(t => t.threadId -> t).toMap
    val threadInfoMap = threadInfos.map(i => i.getThreadId -> i).toMap

    // Dump header
    if (summary.nonEmpty) {
      result.append(s"${lineSeparator}Thread Dump Summary: $summary$lineSeparator")
    }
    details foreach { msg =>
      result.append(s"Thread Dump Details: $msg$lineSeparator")
    }
    result.append(s"Full thread dump at ${new Date}:$lineSeparator")

    // Report deadlocks first
    if (bean.findDeadlockedThreads() ne null) {
      result.append(s"${lineSeparator}Found one Java-level deadlock:$lineSeparator")
      result.append(s"${"=" * 29}$lineSeparator$lineSeparator")
      bean.findDeadlockedThreads().flatMap(threadInfoMap.get).foreach { deadlockedThread =>
        result.append(s""""${deadlockedThread.getThreadName}"$lineSeparator""")
        if (deadlockedThread.getLockName ne null) {
          result.appendln(describeMonitor("waiting to lock", deadlockedThread.getLockName, tabulated = false))
        }
        for (monitor <- deadlockedThread.getLockedMonitors) {
          result.append(describeMonitor("locked", monitor.toString, tabulated = false))
        }
        if (deadlockedThread.getLockOwnerName ne null) {
          // Goes hand-in-hand with deadlockedThread.getLockName ne null
          result.appendln(s"""  which is held by "${deadlockedThread.getLockOwnerName}"$lineSeparator""")
        }
      }
    }

    // Thread dump, youngest to oldest
    threadInfos.sortBy(i => i.getThreadId).reverse.foreach { tInfo =>
      val thread: Option[Thread] = allThreadsMap.get(tInfo.getThreadId)
      result.endln()
      dumpThreadInfo(tInfo, thread, result)
    }
    result.append(s"${lineSeparator}End of Thread Dump$lineSeparator")
    (result.toString(), threadInfos.toSeq)
  }

  def dumpThreadInfo(tInfo: ThreadInfo, thread: Option[Thread], result: PrettyStringBuilder): Unit = {
    val stackTrace = tInfo.getStackTrace

    // Thread header style mixing jStack and IntelliJ dumps
    result.append(s""""${tInfo.getThreadName}" ${describeThread(thread)}""")
    result.appendln(s""" ${tInfo.getThreadState.name().toLowerCase}""")
    result.appendln(s"""  java.lang.Thread.State: ${tInfo.getThreadState}""")

    // Stack trace
    for (index <- 0 until stackTrace.length) {
      result.appendln(s"\tat ${stackTrace(index)}")
      if (index == 0 && (tInfo.getLockName ne null)) {
        val message = (stackTrace(index).getClassName, stackTrace(index).getMethodName) match {
          case (_, methodName) if methodName.startsWith("park")            => "parking to wait for"
          case (clazzName, "wait") if clazzName == classOf[Object].getName => "waiting on"
          case _                                                           => "waiting to lock"
        }
        result.appendln(describeMonitor(message, tInfo.getLockName))
      }
      for (monitor <- tInfo.getLockedMonitors if monitor.getLockedStackDepth == index) {
        result.appendln(describeMonitor("locked", monitor.toString))
      }
    }

    // jStack style synchronizer reporting
    result.endln()
    result.appendln(s"   Locked ownable synchronizers:")
    if (tInfo.getLockedSynchronizers.length > 0) {
      tInfo.getLockedSynchronizers.foreach { synchronizer =>
        result.appendln(f"\t- <0x${synchronizer.getIdentityHashCode}%016x> (a ${synchronizer.getClassName})")
      }
    } else {
      result.appendln(s"\t- None")
    }
  }
}
