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
package optimus.buildtool.trace

import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.buildtool.config.NamingConventions.ConfigPrefix
import optimus.buildtool.config.ScopeId
import optimus.buildtool.trace
import optimus.buildtool.trace.LongRunningTraceListener.sendCrumbForTask
import optimus.buildtool.utils.Utils
import optimus.graph.EfficientSchedulerLogging
import optimus.platform.util.PrettyStringBuilder

import java.time.{Instant, Duration => JDuration}
import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object LongRunningTraceListener {
  object Config {
    private val prefix = s"$ConfigPrefix.longRunning"
    val thresholdMillis: Long =
      sys.props.get(s"$prefix.thresholdMillis").map(_.toLong).getOrElse(10.minutes.toMillis)
    val scanIntervalMillis: Long =
      sys.props.get(s"$prefix.scanIntervalMillis").map(_.toLong).getOrElse(1.minutes.toMillis)
  }

  private val log = getLogger(this)

  def sendCrumbForTask(
      category: CategoryTrace,
      scopeId: ScopeId,
      start: Instant,
      duration: JDuration,
      state: String,
      customElems: Properties.Elem[_]*): Unit = {
    val elems = Seq(
      Properties.obtCategory -> category.name,
      Properties.obtScope -> scopeId.toString,
      Properties.obtTaskStart -> start,
      Properties.obtTaskDuration -> duration,
      Properties.obtTaskState -> state
    )
    Breadcrumbs.info(ChainedID.root, PropertiesCrumb(_, ObtCrumbSource, elems ++ customElems: _*))
  }
}

class LongRunningTraceListener(
    useCrumbs: Boolean,
    thresholdMillis: Long = LongRunningTraceListener.Config.thresholdMillis,
    scanIntervalMillis: Long = LongRunningTraceListener.Config.scanIntervalMillis
) extends DefaultObtTraceListener {
  import LongRunningTraceListener.log

  protected final class RunningTask(val scopeId: ScopeId, val category: CategoryTrace, val startTimeMillis: Long)
      extends DefaultTaskTrace {
    override def end(success: Boolean, errors: Int, warnings: Int, time: Instant): Unit =
      runningTasks.remove(this)
  }

  private val runningTasks = ConcurrentHashMap.newKeySet[RunningTask]()
  private val timer = new Timer("LongRunningTaskScanner", /*isDaemon*/ true)

  override def startTask(scopeId: ScopeId, category: CategoryTrace, time: Instant): TaskTrace =
    if (category != trace.Build) {
      val task = new RunningTask(scopeId, category, time.toEpochMilli)
      runningTasks.add(task)
      task
    } else super.startTask(scopeId, category, time)

  override def endBuild(success: Boolean): Boolean = {
    runningTasks.clear()
    true
  }

  private def startScanner(): Unit = {
    val task = new TimerTask {
      private var counter: Int = 0
      override def run(): Unit = {
        // print on the first run with long-running tasks, and on every 5 runs after that
        val hadLongRunningTasks = scan(counter % 5 == 0)
        if (hadLongRunningTasks) counter += 1
        else counter = 0 // reset counter if no long-running tasks were detected
      }
    }
    timer.schedule(task, scanIntervalMillis, scanIntervalMillis)
  }

  protected def scan(printStack: Boolean): Boolean = {
    val now = System.currentTimeMillis
    val cutoff = now - thresholdMillis
    val longTasks = runningTasks.asScala.filter(_.startTimeMillis < cutoff)
    longTasks.foreach(warn(_, now))
    val hadLongRunningTasks = longTasks.nonEmpty
    if (hadLongRunningTasks && printStack) logSchedulerState()
    hadLongRunningTasks
  }

  protected def warn(t: RunningTask, now: Long): Unit = {
    log.warn(
      s"Task ${t.scopeId}:${t.category.name} has been running for ${Utils.durationString(now - t.startTimeMillis)}")

    if (useCrumbs) {
      val startInstant = Instant.ofEpochMilli(t.startTimeMillis)
      val nowInstant = Instant.ofEpochMilli(now)
      sendCrumbForTask(
        category = t.category,
        scopeId = t.scopeId,
        start = startInstant,
        duration = JDuration.between(startInstant, nowInstant),
        state = "stalling"
      )
    }
  }

  protected def logSchedulerState(): Unit = {
    val sb = new PrettyStringBuilder
    EfficientSchedulerLogging.logAllSchedulers(sb, shouldPrintAllInfo = true)
    log.debug(sb.toString)
  }

  def close(): Unit = timer.cancel()

  startScanner()
}
