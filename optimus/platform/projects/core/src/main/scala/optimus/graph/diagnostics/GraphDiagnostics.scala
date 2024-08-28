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

import java.io.Writer
import optimus.graph.DiagnosticSettings
import optimus.graph.EfficientSchedulerLogging
import optimus.graph.NodeTask
import optimus.graph.OGScheduler
import optimus.graph.Scheduler
import optimus.graph.Settings
import optimus.graph.tracking.DependencyTrackerRoot
import optimus.platform.annotations.deprecating
import optimus.platform.util.Log
import optimus.platform.util.PrettyStringBuilder
import optimus.platform.util.ThreadDumper
import optimus.utils.misc.Color

/**
 * Receives signals from OGScheduler, failed unit tests, and failed Selenium tests and triggers additional diagnostic
 * output.
 */
object GraphDiagnostics extends Log {

  def graphStall(scheduler: OGScheduler, printAllInfo: Boolean = true, awaitedTask: NodeTask): Unit = {
    if (DiagnosticSettings.diag_showConsole)
      ProfilerUIControl.bookmark("Stall!", Color.red)
    try {
      log.info(getGraphState(Option(scheduler), printAllInfo, awaitedTask))
      warnIfHoldingLocks()
    } catch {
      case t: Throwable =>
        log.error("Got exception during stall generation", t)
    }
  }

  def getGraphState: String = getGraphState(None, printAllInfo = true, null)

  class GraphStallLogTesting {
    def report(msg: String): Unit = {}
    def shouldReportImmediately(workThreadCount: Int): Boolean = workThreadCount <= 1
    def await(): Unit = {}
  }
  private var TEST_ONLY_GraphStallLogTesting: GraphStallLogTesting = _

  @deprecating("For tests only! Don't ever use this")
  def withCapturedGraphStallLog[T](tester: GraphStallLogTesting)(code: => T): Unit = {
    try {
      TEST_ONLY_GraphStallLogTesting = tester
      code
      tester.await()
    } finally {
      TEST_ONLY_GraphStallLogTesting = null
    }
  }

  /**
   * Skip waiting for graph stall print interval to reach its timeout to avoid slowing down tests. Right now this
   * triggers graph stall logging if the callback is set and there are on stalled callbacks, but we could add other
   * scheduler info (like hasWork) to this check
   */
  def reportImmediately(workThreadCount: Int): Boolean =
    (TEST_ONLY_GraphStallLogTesting ne null) && TEST_ONLY_GraphStallLogTesting.shouldReportImmediately(workThreadCount)

  private def reportToListeners(dump: String): Unit =
    if (TEST_ONLY_GraphStallLogTesting ne null)
      TEST_ONLY_GraphStallLogTesting.report(dump)

  def getGraphState(
      maybeStallingScheduler: Option[OGScheduler] = None,
      printAllInfo: Boolean,
      awaitedTask: NodeTask): String = {
    val sb = new PrettyStringBuilder
    sb.showNodeState = true
    // If setting disabled and not a test caller then only log current scheduler, else log all schedulers
    val loggingStats = maybeStallingScheduler match {
      case Some(scheduler) =>
        sb.appendln(s"Scheduler ${scheduler.getSchedulerId} is stalling...")
        val stats =
          if (!Settings.reportAllSchedulerStatus)
            EfficientSchedulerLogging.logCurrentScheduler(sb, scheduler, printAllInfo)
          else EfficientSchedulerLogging.logAllSchedulers(sb, printAllInfo)
        stats
      case None =>
        sb.appendln("Printing application graph state...")
        EfficientSchedulerLogging.logAllSchedulers(sb, printAllInfo)
    }
    loggingStats.print(sb)

    val dump = sb.toString
    reportToListeners(dump)
    dump
  }

  def unitTestStall(): Unit = {
    log.info(getGraphState)
    log.info(getDependencyTrackerWorkInfo(includeUTracks = true))
    warnIfHoldingLocks()
  }

  /**
   * We already print monitor information if threads are holding locks, but log an extra warning separate to the thread
   * stacks. This logging won't include stack traces because they've already been dumped. Note: We don't want to append
   * this to the StringBuilder because we want a log.warn, not info
   */
  def warnIfHoldingLocks(): Unit = {
    val threadInfos = ThreadDumper.getThreadInfosForQueues(Scheduler.snapshots())
    val holdingLocks = threadInfos.infos.filter(_._2.getLockName ne null)
    if (holdingLocks.nonEmpty) {
      val msg = "There are threads holding locks! If these are graph threads they might be responsible for stalls. " +
        "To notify the scheduler that this is not 'real' work so it can run callbacks when stalling, " +
        "use EvaluationContext.current.doBlockingAction "
      log.warn(msg)
      holdingLocks.foreach { case (id, tInfo) =>
        log.warn(s"Thread ${tInfo.getThreadName} [id $id] holding lock ${tInfo.getLockName}")
      }
    }
  }

  def unitTestStall(w: Writer): Unit = {
    w.append(getGraphState)
    w.append(getDependencyTrackerWorkInfo(includeUTracks = true))
    w.flush()
  }

  def seleniumTestFail(
      source: AnyRef,
      maybeJstack: Option[String],
      graphState: => String = getGraphState,
      depTrackerInfo: => String = getDependencyTrackerWorkInfo(includeUTracks = true)): Unit = {
    log.info(s"Selenium test fail reported by: ${source.getClass}")
    maybeJstack.foreach(jstack => {
      log.info("Printing application jstack")
      log.info(jstack)
    })
    log.info(graphState)
    log.info(depTrackerInfo)
  }

  def getDependencyTrackerWorkInfo(includeUTracks: Boolean): String = {
    val sb = new PrettyStringBuilder()
    sb.appendln("Printing DependencyTracker work info")
    sb.appendln(DependencyTrackerRoot.getAllWorkInfo(includeUTracks))
    sb.toString
  }

  def dependencyTrackerStall(): Unit = {
    log.info("Dependency Tracker is stalling")
    log.info(getGraphState)
    // might be too much logging in real apps to include full UTrack info
    log.info(getDependencyTrackerWorkInfo(includeUTracks = false))
  }
}
