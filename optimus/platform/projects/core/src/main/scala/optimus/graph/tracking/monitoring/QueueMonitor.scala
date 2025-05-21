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

package optimus.graph.tracking.monitoring

import optimus.graph.OGTrace.HashSep
import optimus.graph.Settings
import optimus.graph.tracking.DependencyTrackerAction
import optimus.graph.tracking.DependencyTrackerQueue

/**
 * Queue monitor allows application to hook into the state of dependency tracker queues so that they can log or warn
 * when latencies are too high.
 */
trait QueueMonitor {

  /**
   * When an action is enqueued and the length of the queue is higher than this value, queueSizeWarning() is called.
   */
  def queueSizeThreshold: Long
  def queueSizeWarning(size: Long): Unit

  /**
   * When an action is started and it has been on the queue for longer than this value, queueLatencyWarning() is called.
   */
  def queueLatencyThresholdNanos: Long
  def queueLatencyWarning(latencyNs: Long, action: ActionSummary): Unit

  /**
   * When an action is completed and it took longer than this value, completionLatencyWarning() is called.
   */
  def completionLatencyThresholdNanos: Long
  def completionLatencyWarning(latencyNs: Long, isApproximate: Boolean, action: ActionSummary): Unit
}

final class DefaultQueueMonitor(queue: DependencyTrackerQueue) extends QueueMonitor {
  import QueueMonitor.log

  private var nextQueueSizeThreshold: Long = Settings.dtqQueueSizeWarning
  private var nextLatencyWarningNs: Long = Settings.dtqLatencyWarningNanos

  private def perhapsStateOfQueues: String = {
    if (Settings.dtqPrintStateOfQueuesOnWarnings) s"\n\n${queue.summary.formatted}"
    else ""
  }

  override def queueSizeThreshold: Long = nextQueueSizeThreshold
  override def queueSizeWarning(size: Long): Unit = {
    nextQueueSizeThreshold = (nextQueueSizeThreshold * Settings.dtqQueueSizeWarningGrowthFactor).toLong
    log.warn(s"Slow code? The queue on $queue is long. Actions enqueued now will need " +
      s"to wait until ${size} actions are processed before they can be processed themselves. Will warn again if the " +
      f"queue size exceeds ${nextQueueSizeThreshold}. If you keep seeing this message, your application " +
      s"may be enqueuing actions (e.g. due to reactive or UI events) faster than it can process them." +
      perhapsStateOfQueues)
  }

  override def queueLatencyThresholdNanos: Long = nextLatencyWarningNs
  override def queueLatencyWarning(latencyNs: Long, action: ActionSummary): Unit = {
    if (!action.isLowPriority) {
      nextLatencyWarningNs = (nextLatencyWarningNs * Settings.dtqLatencyWarningGrowthFactor).toLong
      log.warn(
        s"Slow code? Action $action with cause ${action.cause} (from ${action.rootCause}) was enqueued on $queue " +
          f"${latencyNs / 1.0e9}%.1fs ago but is only starting now. Will warn again if the queue latency exceeds " +
          f"${nextLatencyWarningNs / 1.0e9}%.1fs. If you keep seeing this message, your application may be enqueuing " +
          s"actions (e.g. due to reactive or UI events) faster than it can process them." + perhapsStateOfQueues)
    }
  }

  override def completionLatencyThresholdNanos: Long = Settings.dtqTaskTimeLatencyWarningNanos
  override def completionLatencyWarning(latencyNs: Long, isApproximate: Boolean, action: ActionSummary): Unit = {
    val approxMsg =
      if (isApproximate)
        "(this calculation is really being done using the time that the action was scheduled to run not when it actually" +
          " run so in reality the action may have taken slightly shorter to run. In order to get more accurate results" +
          " please run with traceNodes enabled)"
      else ""
    log.warn(
      s"Task taking a long time? Action $action with cause ${action.cause} (from ${action.rootCause}) was started " +
        s"on $queue " + f"${latencyNs / 1.0e9}%.1fs ago " +
        s"${approxMsg} but is only finishing now." + perhapsStateOfQueues)
  }
}

/**
 * QueueMonitor is used to monitor the state of dependency tracker queues.
 */
object QueueMonitor {
  private[optimus] val log = msjava.slf4jutils.scalalog.getLogger(classOf[QueueMonitor])

  def register(queue: DependencyTrackerQueue): QueueMonitor = new DefaultQueueMonitor(queue)
}

// A few classes that exist mainly to decouple the monitoring from the actual actions that are not thread safe to
// access. QueueStats represents "cumulative" statistics (+ currentSize). It is turned into a snap form for
// SamplingProfiler in diff().
final case class QueueStats(cumulAdded: Long, cumulProcessed: Long, currentSize: Int)
object QueueStats {
  final case class Snap private (added: Long, processed: Long, currentSize: Int)
  object Snap { val zero: Snap = Snap(0, 0, 0) }

  def diff(prev: Option[QueueStats], newSnap: QueueStats): Snap = {
    val p = prev.getOrElse(zero)
    Snap(
      added = newSnap.cumulAdded - p.cumulAdded,
      processed = newSnap.cumulProcessed - p.cumulProcessed,
      currentSize = newSnap.currentSize)
  }

  def gced(prev: QueueStats): Snap = {
    // If the tracker has been gced, we create a new snap that assumes all previous task were processed and no
    // new tasks were added.
    Snap(0, prev.cumulAdded - prev.cumulProcessed, 0)
  }

  val zero: QueueStats = QueueStats(0, 0, 0)
}

final case class QueueActionSummary(
    root: String,
    tracker: String,
    stats: QueueStats,
    workQueue: Seq[ActionSummary],
    lowPriorityQueue: Seq[ActionSummary]
) {
  def formatted: String = {
    def formatQueue(name: String, q: Seq[ActionSummary]) =
      if (q.isEmpty) s"  $name is empty."
      else {
        s"""
           | $name(size=${q.size}) (applet ${root}}
           |${q.map(a => "    " + a.formatted).mkString("\n")}
           |""".stripMargin
      }

    s"""|Queue summary for ${tracker}
        |  cumulative statistics:
        |    actions added:     ${stats.cumulAdded}
        |    actions processed: ${stats.cumulProcessed}
        |
        |${formatQueue("Work queue", workQueue)}
        |
        |${formatQueue("Low priority queue", lowPriorityQueue)}
        |""".stripMargin
  }
}

object ActionSummary {
  def apply(a: DependencyTrackerAction[_]): ActionSummary = ActionSummary(
    a.getClass.getSimpleName + HashSep + System.identityHashCode(a).toHexString,
    a.cause.cause,
    a.cause.root.cause,
    a.isLowPriority)
}

final case class ActionSummary private (action: String, cause: String, rootCause: String, isLowPriority: Boolean) {
  private def chars(s: String, size: Int): String = {
    s.length match {
      case n if n <= size => s.padTo(size, ' ')
      case n if n > size  => s.substring(0, size - 3) + "..."
    }
  }
  def formatted = s"${chars(action, 20)}  ${chars(rootCause, 40)} -> ${chars(cause, 20)}"
}
