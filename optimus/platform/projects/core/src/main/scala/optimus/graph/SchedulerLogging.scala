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
package optimus.graph

import optimus.graph.OGScheduler.Context
import optimus.graph.Scheduler.ContextSnapshot
import optimus.graph.Scheduler.SchedulerSnapshot

import java.lang.management.ThreadInfo
import optimus.graph.SchedulerLogging.LoggingStats
import optimus.platform.util.PrettyStringBuilder
import optimus.platform.util.ThreadDumper
import optimus.platform.util.ThreadDumper.ThreadInfos

import scala.collection.mutable

object SchedulerLogging {
  object LoggingStats {
    val empty: LoggingStats = LoggingStats(ThreadInfos.empty, 0)
  }

  final case class LoggingStats(threadInfos: ThreadInfos, numSchedulers: Int) {
    def print(sb: PrettyStringBuilder): Unit = {
      if (threadInfos.infos.nonEmpty)
        sb.appendln(
          s"It took ${threadInfos.captureTimeMs} ms in total to dump the threads for $numSchedulers " +
            s"schedulers, with ${threadInfos.infos.size} threads overall")
    }
  }
}

trait SchedulerLogging {
  protected def logSchedulers(
      sb: PrettyStringBuilder,
      schedulers: List[Scheduler],
      shouldPrintAllInfo: Boolean): LoggingStats

  def logAllSchedulers(sb: PrettyStringBuilder, shouldPrintAllInfo: Boolean): LoggingStats =
    logSchedulers(sb, Scheduler.schedulers, shouldPrintAllInfo)

  def logCurrentScheduler(
      sb: PrettyStringBuilder,
      currentScheduler: Scheduler,
      shouldPrintAllInfo: Boolean): LoggingStats =
    logSchedulers(sb, List(currentScheduler), shouldPrintAllInfo)
}

object EfficientSchedulerLogging extends SchedulerLogging {

  def logSchedulers(sb: PrettyStringBuilder, schedulers: List[Scheduler], shouldPrintAllInfo: Boolean): LoggingStats =
    logSchedulers(sb, schedulers, shouldPrintAllInfo, unconditionally = false)
  def logSchedulers(
      sb: PrettyStringBuilder,
      schedulers: List[Scheduler],
      shouldPrintAllInfo: Boolean,
      unconditionally: Boolean): LoggingStats = {
    val schedulerQueues = Scheduler.snapshots(schedulers)
    // take into account what optimus.stallPrintStacktraces sets as well
    val printAllState = shouldPrintAllInfo && (unconditionally || Settings.stallPrintStacktraces)
    if (printAllState) sb.append("Will print all graph state.\n")
    else
      sb.append(
        "Will print minimal stall info because we are waiting for tasks adapted by plugins so a stall is expected.\n")
    if (printAllState) {
      // will also print thread dumps
      val threadInfos = ThreadDumper.getThreadInfosForQueues(schedulerQueues)
      schedulerQueues.foreach {
        case SchedulerSnapshot(scheduler, ContextSnapshot(waitQueue, workQueue, externallyBlockedQueue)) =>
          scheduler.getAllWaitStatus(sb, waitQueue, workQueue, externallyBlockedQueue, threadInfos)
      }
      LoggingStats(threadInfos, schedulers.size)
    } else {
      schedulerQueues.foreach(snapshot => snapshot.scheduler.getAllWaitStatus(sb, snapshot.contexts.waiting))
      LoggingStats.empty
    }
  }
}

object SchedulerDiagnosticUtils {
  type CtxQueue = Array[OGScheduler.Context]
  type SchedulerQueues = Iterable[ContextSnapshot]

  val EmptyThreadSnapshot: ThreadSnapshot = ThreadSnapshot(0, ThreadInfos.empty, ThreadInfos.empty, Map.empty)
  final case class ThreadSnapshot(
      threadCount: Int,
      schedulerInfos: ThreadInfos,
      otherInfos: ThreadInfos,
      threadTaskCounts: Map[Long, Int]) {
    lazy val allStringInfos: Map[Long, String] = allInfos.map { case (k, v) => (k, v.toString) }

    def captureTimeMs: Long = schedulerInfos.captureTimeMs + otherInfos.captureTimeMs
    def allInfos: Map[Long, ThreadInfo] = schedulerInfos.infos ++ otherInfos.infos
  }

  def getThreadSnapshot(
      schedulers: List[Scheduler],
      threadNames: collection.Seq[String] = collection.Seq.empty): ThreadSnapshot =
    getThreadSnapshotForQueues(schedulers.map(_.getContexts), threadNames)

  def getThreadSnapshotForQueues(
      queues: SchedulerQueues,
      threadNames: collection.Seq[String],
      excludeSpinningThread: Boolean = false): ThreadSnapshot = {
    val threadIDs = getThreadIds(queues)
    val schedulerThreadInfos =
      ThreadDumper.getThreadInfos(threadIDs, includeLocks = false, excludeSpinningThread = excludeSpinningThread)
    val otherThreadInfos =
      ThreadDumper.getThreadInfos(
        ThreadDumper.getThreadIdsByName(threadNames),
        includeLocks = false,
        excludeSpinningThread = excludeSpinningThread)
    ThreadSnapshot(threadIDs.length, schedulerThreadInfos, otherThreadInfos, getCtxTaskCounts(queues))
  }

  def getThreadSnapshotWithAdapted(
      schedulers: List[Scheduler],
      threadNames: collection.Seq[String] = collection.Seq.empty
  ): (ThreadSnapshot, Boolean) = {
    val queues = schedulers.map(_.getContexts)
    (getThreadSnapshotForQueues(queues, threadNames), anyAdaptedTasks(queues))
  }

  def anyAdaptedTasks(waitWorkQueues: SchedulerQueues): Boolean = {
    waitWorkQueues.exists { case ContextSnapshot(waitQueue, _, _) =>
      waitQueue.exists(ctx => {
        val endOfChainInfo = ctx.awaitedTaskInfoEndOfChain
        val adaptedExistsInWaitChain =
          if (ctx.awaitedTask != null)
            endOfWaitChainIsAdapted(ctx)
          else false
        endOfChainInfo.hasPlugin || adaptedExistsInWaitChain
      })
    }
  }

  private def endOfWaitChainIsAdapted(ctx: OGScheduler.Context): Boolean = {
    val forwardChain = ctx.awaitedTask.visitForwardChain()
    val endOfChain = forwardChain.get(forwardChain.size - 1)
    endOfChain.isAdapted
  }

  def getThreadIds(waitWorkQueues: Iterable[ContextSnapshot]): Array[Long] = {
    val threadIds = mutable.HashSet[Long]()

    // thread can be null if OGLogicalThread was created but didn't run yet (Context.thread is set in OGLogicalThread.run)
    def addID(ctx: Context): Unit =
      if (ctx.thread ne null)
        threadIds.add(ctx.thread.threadId)

    waitWorkQueues.foreach { case ContextSnapshot(waitQueue, workQueue, externallyBlockedQueue) =>
      waitQueue.foreach(addID)
      workQueue.foreach(addID)
      externallyBlockedQueue.foreach(addID)
    }
    threadIds.toArray
  }

  def getCtxTaskCounts(waitersAndWorkers: SchedulerQueues): Map[Long, Int] = {
    waitersAndWorkers.flatMap { case ContextSnapshot(waiters, workers, blocked) =>
      (waiters ++ workers ++ blocked).map { ctx =>
        // ctx is added to workers queue before calling run (which sets the schedulerContext field) so can be null
        if (ctx.thread != null)
          ctx.thread.threadId -> Option(ctx.schedulerContext).map(_.getTaskCounter).getOrElse(0)
        else
          0L -> 0
      }
    }.toMap
  }
}
