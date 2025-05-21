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
package optimus.graph.tracking

import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.graph.Settings
import optimus.graph.tracking.DependencyTrackerRoot.log

trait CallbackSupport {
  @volatile private[tracking] var nodeInvalidated = List.empty[InvalidationCallbackBatchFactory[_]]
  @volatile private[tracking] var nodeCompleted = List.empty[CompletionCallback[_]]
  @volatile private[tracking] var scenarioDisposal: List[(DisposalInformation, EventCause) => Unit] = Nil

  private val registrationLock = new Object
  private def withRegistrationLock[T](fn: => T): T = registrationLock.synchronized(fn)

  def registerNodeCompleted[M >: Null <: TrackingMemo](
      scope: TrackingScope[M],
      callback: (TrackedNode[_], Boolean, M) => Unit): Unit =
    withRegistrationLock(nodeCompleted ::= new CompletionCallback(scope, callback))

  def registerScenarioDisposal(callback: (DisposalInformation, EventCause) => Unit): Unit =
    withRegistrationLock(scenarioDisposal ::= callback)
  def unregisterScenarioDisposal(callback: (DisposalInformation, EventCause) => Unit): Unit =
    withRegistrationLock { scenarioDisposal = scenarioDisposal.filterNot(_ == callback); () }

  // Batch APIs
  def registerNodesInvalidated[M >: Null <: TrackingMemo](
      scope: TrackingScope[M],
      callback: (List[(TrackedNode[_], M)], EventCause) => Unit): Unit =
    withRegistrationLock(nodeInvalidated ::= new InvalidationCallbackBatchFactory[M](scope, callback))

  private def callback[CB](callbacks: List[(CB, EventCause) => Unit], data: CB, cause: EventCause): Unit = {
    for (cb <- callbacks)
      try cb(data, cause)
      catch {
        case ex: Exception => log.warn("ignored exception from DependencyTracker callback", ex)
      }
  }

  private[tracking] def notifyScenarioDisposal(disposal: DisposalInformation, cause: EventCause): Unit =
    cause.counted { callback(scenarioDisposal, disposal, cause) }

  def reportChange: Boolean

  private[tracking] def notifyNodeCompleted[T](current: TrackedNode[T], previous: TrackedNode[T]): Unit = {
    val change =
      if (!reportChange) true
      else
        !((previous ne null) && previous.isDoneWithResult && current.isDoneWithResult && (current.result == previous.result))

    nodeCompleted.foreach { cb =>
      if (cb.scope eq current.scope) cb.invokeCallback(current, change)
    }
  }

  // We don't really want to ever use this - we want to use the DependencyTrackerBatchUpdaterRoot's invalidationBatcher
  // instead so that we get batching. This one is used in cases where we're doing invalidations outside of a batch
  // update, which ideally we would never do.
  private[tracking] def unbatchedTrackedNodeInvalidationObserver(
      cause: EventCause = NoEventCause): TrackedNodeInvalidationObserver =
    (tn: TrackedNode[_]) => {
      if ((tn ne null) && (!tn.isDone)) {
        nodeInvalidated.foreach { f =>
          val batch = f()
          batch.addCallback(tn)
          batch.complete(cause)
        }
      }
    }
}

private[tracking] object CallbackBase {
  val log: Logger = getLogger(this.getClass)
}

private[tracking] sealed abstract class CallbackBase[M >: Null <: TrackingMemo](val scope: TrackingScope[M]) {
  protected def ignored(e: Throwable): Unit = { CallbackBase.log.warn("Ignored failure in callback", e) }
}

private[tracking] final class CompletionCallback[M >: Null <: TrackingMemo](
    scope: TrackingScope[M],
    val callback: (TrackedNode[_], Boolean, M) => Unit)
    extends CallbackBase(scope) {
  def invokeCallback(tn: TrackedNode[_], changed: Boolean): Unit = {
    // memo could be null if the node was untracked while it was completing (in which case fine to drop)
    val cause = tn.cause
    val memo = tn.getMemo.asInstanceOf[M]
    if (memo ne null) try {
      if (scope.notifyWithoutEventCause || !cause.isEmpty) {
        callback(tn, changed, memo)
        if (Settings.trackingScenarioLoggingEnabled)
          DependencyTrackerLogging.callback("node completed", cause, scope, tn :: Nil)
      } else {
        NoEventCause.throwOrLogException(
          s"Notifications cannot be sent for tracked nodes without an EventCause in scope $scope",
          debug = true)
      }
    } catch {
      case e: Exception => ignored(e)
    }
  }
}

private[tracking] class InvalidationCallbackBatchFactory[M >: Null <: TrackingMemo](
    val scope: TrackingScope[M],
    val callback: (List[(TrackedNode[_], M)], EventCause) => Unit) {
  def apply(): InvalidationCallbackBatch[M] =
    new InvalidationCallbackBatch[M](scope, callback)
}

private[tracking] class InvalidationCallbackBatch[M >: Null <: TrackingMemo](
    callbackScope: TrackingScope[M],
    val callback: (List[(TrackedNode[_], M)], EventCause) => Unit)
    extends CallbackBase(callbackScope) {
  import scala.collection.mutable
  private val pending = mutable.Map.empty[DependencyTracker, mutable.Map[TrackedNode[_], M]]
  def addCallback(tn: TrackedNode[_]): Unit = if (tn.scope eq callbackScope) {
    // memo could be null if the node was untracked while it was completing (in which case fine to drop)
    val memo = tn.getMemo.asInstanceOf[M]
    if (memo ne null)
      pending.getOrElseUpdate(tn.dependencyTracker, mutable.Map.empty[TrackedNode[_], M]).update(tn, memo)
  }
  def complete(cause: EventCause): Unit = {
    val pendingList: List[(TrackedNode[_], M)] = pending.iterator.flatMap {
      case (ts, _) if ts.isDisposed => Nil
      case (_, v)                   => v.toList
    }.toList

    if (Settings.trackingScenarioLoggingEnabled)
      DependencyTrackerLogging.callback("node invalidated", cause, scope, pendingList.map(_._1))
    try {
      cause.counted { callback(pendingList, cause) }
    } catch {
      case e: Exception => ignored(e)
    } finally {
      pending.clear()
    }
  }
  def copyNodes(): Map[DependencyTracker, Map[TrackedNode[_], M]] = {
    pending map { case (k, v) => (k -> v.toMap) } toMap
  }
  def removeMatching(toRemove: TrackedNode[_] => Boolean): Unit = {
    pending foreach {
      // need to calculate and remove, not remove as you go for concurrent access
      case (k, v) => v --= (v.keySet.filter(toRemove))
    }
  }
}

trait TrackedNodeInvalidationObserver {
  def notifyNodeInvalidated(tn: TrackedNode[_]): Unit
}

private[tracking] class InvalidationCallbackBatcher(callbacks: CallbackSupport, cause: EventCause)
    extends TrackedNodeInvalidationObserver {
  private var callbackBatches: Seq[InvalidationCallbackBatch[_]] = _

  override def notifyNodeInvalidated(tn: TrackedNode[_]): Unit = if ((tn ne null) && (!tn.isDone)) {
    if (callbackBatches eq null) callbackBatches = callbacks.nodeInvalidated.map(_())
    callbackBatches.foreach(_.addCallback(tn))
  }

  def complete(): Unit =
    if (callbackBatches ne null) callbackBatches.foreach { _.complete(cause) }

  def currentBatches(scope: TrackingScope[_]): Seq[InvalidationCallbackBatch[_]] =
    if (callbackBatches eq null) Nil else callbackBatches.filter(_.scope == scope)
}

object NoOpTrackedNodeInvalidationObserver extends TrackedNodeInvalidationObserver {
  override def notifyNodeInvalidated(tn: TrackedNode[_]): Unit = ()
}
