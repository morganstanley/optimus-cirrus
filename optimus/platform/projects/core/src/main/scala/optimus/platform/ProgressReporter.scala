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
package optimus.platform

import optimus.graph.OGSchedulerContext
import optimus.graph.Node
import optimus.graph.NodeAwaiter
import optimus.graph.NodeTask
import optimus.graph.Warning
import optimus.platform.PluginHelpers.toNode
import optimus.platform.annotations.nodeLift
import optimus.platform.annotations.nodeLiftByName
import optimus.platform.annotations.nodeSync
import optimus.platform.annotations.nodeSyncLift
import optimus.platform.util.Log

class ProgressMarker
object DefaultMarker extends ProgressMarker
case object TrackProgress extends ProgressMarker

//noinspection ScalaStyle
/**
 * This allows progress tracking for completing Optimus nodes.
 *
 * OnUpdateFromChild is called whenever a child node calls updateParent() e.g. when the child node computation finishes.
 *
 * Typical use-case might involve a top level node book calculation, which subsequently splits into trade evaluations,
 * each trade reporting its completeness via updateParent() method.
 *
 * NOTE: In case of distribution, progress reporter that is serialised to the remote side, MUST call updateParent() if
 * it wants to pass any notifications to the client side! It is especially important when onUpdateFromChild() is called!
 * Remote progress reporter cannot simply update its state, as it will not be reflected on the client side, but it must
 * call updateParent() in order to ensure update is propagated back to client side.
 */
class ProgressReporter(@transient private var _parent: ProgressReporter) extends Serializable {
  def this() = this(null)
  def parent: ProgressReporter = _parent

  /** This api is to be used by dist & batchers only */
  def setParent(newParent: ProgressReporter): Unit = {
    _parent = newParent
  }

  /**
   * Only when shouldReport is true will the progress updates be reported. If some progress updates are too verbose and
   * not to be processed by a listener, they can be skipped to reduce the traffic between grid engines.
   */
  val shouldReport: Any => Boolean = { _ => true }

  final protected def updateParent(update: Any): Unit = {
    if (parent ne null)
      parent.onUpdateFromChild(update)
  }
  @nodeSync
  @nodeSyncLift
  final def track[T](@nodeLift @nodeLiftByName f: => T): T = track$withNode(toNode(f _))
  final def track$queued[T](f: Node[T]): Node[T] = {
    EvaluationContext.verifyOffGraph(ignoreNonCaching = true)
    val ec = OGSchedulerContext.current()
    f.attach(ec.scenarioStack.withProgressReporter(this))
    ec.enqueueDirect(f)
    f
  }

  final def track$withNode[T](f: Node[T]): T = track$queued(f).get

  // User implemented (app infra level)
  def onUpdateFromChild(update: Any): Unit = {}
  def reportItemCompleted(marker: ProgressMarker): Unit = {}
  def reportItemCompletedWithFailure(marker: ProgressMarker, e: Throwable): Unit = reportItemCompleted(marker)

  def reportItemStarted(marker: ProgressMarker, weight: Double): ProgressReporter = this

  /** Returning null means we don't want to track updates on a given loop */
  def reportCollectionStarted(marker: ProgressMarker, c: AnyRef): CollectionProgressReporter = null

  def shutdown(): Unit = {
    // do nothing
  }
}

object UnspecifiedProgressReporter extends ProgressReporter(null)

trait CollectionProgressReporter {
  def reportIterationStarted(marker: ProgressMarker, ci: Any): ProgressReporter
}

object Progress extends Log {
  private final class ProgressCompleter(marker: ProgressMarker) extends NodeAwaiter {
    override def onChildCompleted(q: EvaluationQueue, n: NodeTask): Unit = {
      EvaluationContext.asIfCalledFrom(n, q) {
        onProgressCompleted(marker, n)
      }
    }
  }

  private[optimus] def onProgressCompleted(marker: ProgressMarker, n: NodeTask): Unit = {
    val nEx = n.exception
    try {
      EvaluationContext.given(n.scenarioStack.siRoot) {
        if (nEx eq null)
          n.scenarioStack.progressReporter.reportItemCompleted(marker)
        else
          n.scenarioStack.progressReporter.reportItemCompletedWithFailure(marker, nEx)
      }
    } catch { // under no circumstances may we throw in oCC!
      case prEx: Throwable =>
        log.error(s"Error calling reportItemCompleted${if (nEx ne null) "WithFailure" else ""} for $n", prEx)
        n.attach(Warning(prEx))
    }
  }

  @nodeSync
  @nodeSyncLift
  def track[T](progressMarker: ProgressMarker = DefaultMarker, enableTracking: Boolean = true, weight: Double = 1.0)(
      @nodeLift @nodeLiftByName f: => T): T =
    track$withNode(progressMarker, enableTracking, weight)(toNode(f _))
  def track$withNode[T](progressMarker: ProgressMarker, enableTracking: Boolean, weight: Double)(f: Node[T]): T =
    reportItemStarted(progressMarker, enableTracking, weight, f).get
  def track$queued[T](progressMarker: ProgressMarker, enableTracking: Boolean, weight: Double)(f: Node[T]): Node[T] =
    reportItemStarted(progressMarker, enableTracking, weight, f).enqueueAttached

  private def reportItemStarted[T](
      marker: ProgressMarker,
      enableTracking: Boolean,
      weight: Double,
      f: Node[T]): Node[T] = {
    val css = EvaluationContext.scenarioStack
    f.attach(css)

    if (enableTracking) {
      val pr = css.progressReporter

      if (pr ne null) {
        val newpr = pr.reportItemStarted(marker, weight)
        if (newpr ne pr) {
          if (!f.tryAddToWaiterList(new ProgressCompleter(marker)))
            newpr.reportItemCompleted(marker) // This handles the trivial case when a constant passed in
          f.replace(css.withProgressReporter(newpr))
        }
      }
    }

    f
  }
}
