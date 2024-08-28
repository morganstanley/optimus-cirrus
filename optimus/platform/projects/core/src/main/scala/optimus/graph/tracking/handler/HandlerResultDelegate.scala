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
package optimus.graph.tracking.handler

import optimus.graph.tracking.DependencyTrackerBatchUpdater
import optimus.graph.tracking.EventCause
import optimus.ui.DependencyTrackerUpdatingGesture
import optimus.ui.HandlerResult

import scala.collection.mutable.ListBuffer

trait HandlerResultDelegate {
  def validate(updater: DependencyTrackerBatchUpdater, result: HandlerResult): Unit = ()
  def change(updater: DependencyTrackerBatchUpdater, result: HandlerResult, eventCause: Option[EventCause] = None): Unit
}

object HeadlessHandlerResultDelegate extends RealUiDelegate {
  override def change(
      updater: DependencyTrackerBatchUpdater,
      result: HandlerResult,
      eventCause: Option[EventCause]): Unit = {
    result.gestures.foreach {
      case g: DependencyTrackerUpdatingGesture => g.doGesture(updater)
      case _                                   =>
    }
    updater.multiUpdater.addTweakMapImmediate(result.tweaks, result.scenarioToTweakLambdas)
  }
}

// provide a separation between real and buffered delegates
/**
 * A delegate that will interpret actions and perform the appropriate validation and action
 *
 * Note - a RealUiDelegate cannot be replaced with another HandlerResultDelegate
 */
private[optimus] trait RealUiDelegate extends HandlerResultDelegate

/**
 * A delegate that just buffers the actions. This is used with the HTML UI is it is initialising. After initialisation
 * the UI will replace this with a RealUiDelegate, and replay the buffered actions, if any have occured
 */
private[optimus] class BufferedUiDelegate extends HandlerResultDelegate {
  private lazy val log = msjava.slf4jutils.scalalog.getLogger(this.getClass)

  private val pending = ListBuffer.empty[HandlerResult]
  override def validate(updater: DependencyTrackerBatchUpdater, result: HandlerResult): Unit = ()

  override def change(
      updater: DependencyTrackerBatchUpdater,
      result: HandlerResult,
      eventCause: Option[EventCause]): Unit = {
    updater.multiUpdater.addTweakMapImmediate(result.tweaks, result.scenarioToTweakLambdas)
    val uiResults = result.copy(tweaks = Map.empty, scenarioToTweakLambdas = Map.empty)

    pending.synchronized {
      pending += uiResults
      log.warn(s"there are now ${pending.size} actions enqueued but the UI is not started yet")
    }
  }
  def pendingActions: List[HandlerResult] = pending.synchronized {
    pending.toList
  }
}
