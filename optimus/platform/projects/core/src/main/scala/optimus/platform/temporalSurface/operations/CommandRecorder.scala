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
package optimus.platform.temporalSurface.operations

import optimus.graph.Settings
import java.util.concurrent.atomic.AtomicLong

import optimus.graph.NodeTask
import optimus.platform.EvaluationQueue
import optimus.graph.NodeAwaiter
import scala.annotation.tailrec

object CommandRecorder {
  def apply: CommandRecorder = {
    if (Settings.traceTemporalSurfaceCommands) new StatsCommandRecorder(null, true) else NoCommandRecorder
  }
}

/**
 * debuggings and support of temporal surface data access
 */
sealed trait CommandRecorder {

  /**
   * call to dal
   */
  def recordDalAccess(n: NodeTask, eq: EvaluationQueue): Unit

  /**
   * create a child recorder. Used to preserve the hierarchy of recorders, e.g. by temporal surface
   * @param start
   *   the start as now
   */
  def childRecorder(start: Boolean): CommandRecorder

  /**
   * mark the us as complete. Note this may be called several times as different access paths traverse
   */
  def complete: Unit

  /**
   * does this counter have any useful data
   */
  def used: Boolean

  /**
   * the recorded start time
   */
  def startTime: Long
}

object NoCommandRecorder extends CommandRecorder {
  override def recordDalAccess(n: NodeTask, eq: EvaluationQueue): Unit = {}
  override def childRecorder(start: Boolean) = this
  override def complete: Unit = {}
  override def used = false
  override def startTime = 0L
}
final class StatsCommandRecorder(val parent: StatsCommandRecorder, _startNow: Boolean)
    extends NodeAwaiter
    with CommandRecorder {
  private val _startTime = new AtomicLong
  if (_startNow) _startTime.set(System.nanoTime())

  override def startTime = _startTime.get()

  private val root: StatsCommandRecorder = if (parent == null) this else parent.root

  private val _end = new AtomicLong

  private val _dalOperationsInProgress = new AtomicLong
  private val _totalDalCommands = new AtomicLong

  private val _totalDalBatches = new AtomicLong

  private val _dalStartTime = new AtomicLong
  private val _totalElapsedDalTimeNs = new AtomicLong

  private val _completionTime = new AtomicLong

  override def used =
    _totalDalCommands.get == 0L && _completionTime.get == 0L

  override def complete: Unit = {
    // currently child node may complete multiple times as the key ( the temporal surface) may be revisited
    {
      _end.set(System.nanoTime)
    }
  }

  override def recordDalAccess(n: NodeTask, eq: EvaluationQueue): Unit = {
    root.synchronized {
      n.continueWith(this, eq)
      val lazyTime = recordLocalAccess(n, eq, -1L)
      if (parent ne null) parent.recordChildDalAccess(n, eq, lazyTime)
      ensureStarted(lazyTime)
    }
  }
  private def ensureStarted(lazyTime: Long): Long = {
    val time = if (lazyTime < 0L) System.nanoTime else lazyTime
    // we dont really care if there is a race here - we always return the current value
    _startTime.compareAndSet(0L, time)

    _startTime.get
  }
  @tailrec private def recordChildDalAccess(n: NodeTask, eq: EvaluationQueue, time: Long): Unit = {
    val lazyTime = recordLocalAccess(n, eq, time)
    if (parent ne null) parent.recordChildDalAccess(n, eq, lazyTime)
  }
  private def recordLocalAccess(n: NodeTask, eq: EvaluationQueue, time: Long) = {
    var lazyTime = ensureStarted(time)
    if (_dalOperationsInProgress.get == 0L) {
      if (lazyTime < 0L) lazyTime = System.nanoTime
      _dalStartTime.set(lazyTime)
      _totalDalBatches.incrementAndGet()
    }
    _totalDalCommands.incrementAndGet()
    _dalOperationsInProgress.incrementAndGet()
    lazyTime
  }
  override def onChildCompleted(eq: EvaluationQueue, n: NodeTask) = root.synchronized {
    val time = System.nanoTime
    onDalComplete(eq, n, time)
    if (parent ne null) parent.onChildDalComplete(eq, n, time)
  }
  @tailrec private def onChildDalComplete(eq: EvaluationQueue, n: NodeTask, time: Long): Unit = {
    onDalComplete(eq, n, time)
    if (parent ne null) parent.onChildDalComplete(eq, n, time)
  }
  private def onDalComplete(eq: EvaluationQueue, n: NodeTask, time: Long): Unit = {
    if (_dalOperationsInProgress.get == 1L) {
      _totalElapsedDalTimeNs.addAndGet(time - _dalStartTime.get)
      _end.set(time) // may be complete
    }
    _dalOperationsInProgress.decrementAndGet()

  }

  override def toString = {
    s"[ DAL commands ${_totalDalCommands} in ${_totalDalBatches} batches took ${_totalElapsedDalTimeNs}ns (OS = ${_dalOperationsInProgress})] wall clock ${if (_end.get() == 0L) "**NA**"
      else _end.get() - startTime}ns]"
  }
  override def childRecorder(start: Boolean) = new StatsCommandRecorder(this, start)

}
