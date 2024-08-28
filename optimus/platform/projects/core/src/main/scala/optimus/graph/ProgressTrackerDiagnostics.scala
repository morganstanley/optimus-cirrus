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

import optimus.platform.NodeFunction0
import optimus.platform.util.Log

import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import scala.collection.mutable

private[graph] class ProgressTrackerMessageReporter extends Log {
  def report(msg: String): Unit = log.warn(msg)
}

private[graph] object ProgressTrackerDiagnostics {
  private var reporter: ProgressTrackerMessageReporter = new ProgressTrackerMessageReporter
  private val trackers = mutable.WeakHashMap[ProgressTracker, ProgressTrackerStallInfo]()
  private val intervalSecs: Int = Settings.stalledProgressIntervalSecs
  private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r => {
    new Thread(r, s"Thread-${this.getClass}(i=$intervalSecs)")
  })

  executor.scheduleAtFixedRate(() => reportStalls(), intervalSecs, intervalSecs, TimeUnit.SECONDS)

  private def reportStalls(): Unit = {
    val currTimeNanos = OGTrace.nanoTime()
    val stalledTrackers = trackers.synchronized {
      trackers.values.filter(_.timeDiffSecs(currTimeNanos) >= intervalSecs)
    }

    if (stalledTrackers.nonEmpty) {
      val message = stalledTrackers.map(_.infoMsg(currTimeNanos)).mkString("\n")
      report(s"Printing stalled progress trackers:\n$message")
    }
  }

  def removeTracker(tracker: ProgressTracker): Unit = trackers.synchronized {
    trackers.remove(tracker)
  }

  def updateTrackers(tracker: ProgressTracker, trackerStallInfo: ProgressTrackerStallInfo): Unit =
    trackers.synchronized {
      trackers.update(tracker, trackerStallInfo)
    }

  def report(msg: String): Unit = reporter.report(msg)

  def reportForTesting[T](reporter: ProgressTrackerMessageReporter)(code: NodeFunction0[T]): Unit = {
    trackers.synchronized {
      trackers.clear() // get rid of all existing stalled trackers
    }

    try {
      this.reporter = reporter
      code()
    } finally {
      this.reporter = null
    }
  }
}

final case class ProgressTrackerStallInfo(trackerName: String, progress: Double, lastUpdateTimeNanos: Long) {
  def timeDiffSecs(currTimeNanos: Long): Long =
    TimeUnit.NANOSECONDS.toSeconds(currTimeNanos - lastUpdateTimeNanos)

  def infoMsg(currTimeNanos: Long): String =
    s"Tracker $trackerName has been stalled for ${timeDiffSecs(currTimeNanos)} seconds with progress of $progress."
}
