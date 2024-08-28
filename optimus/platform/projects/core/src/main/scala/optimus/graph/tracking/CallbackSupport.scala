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
  private[tracking] var nodeInvalidated = List.empty[InvalidationCallbackFactory[_, _]]
  private[tracking] var nodeWillBeInvalidated = List.empty[InvalidationCallbackFactory[_, _]]
  private[tracking] var nodeCompleted = List.empty[CompletionCallbackFactory[_, _]]
  private[tracking] var scenarioDisposal: List[(DisposalInformation, EventCause) => Unit] = Nil

  private val registrationLock = new Object
  private def withRegistrationLock[T](fn: => T): T = registrationLock.synchronized(fn)

  private def removeDeregistered[M >: Null <: TrackingMemo, T <: InvalidationCallbackFactory[_, _]](
      current: List[T],
      scope: TrackingScope[M],
      callback: Any): List[T] = {
    current filterNot { c: T @unchecked =>
      (c.scope == scope) && (c.callback == callback)
    }
  }

  private def removeBooleanDeregistered[M >: Null <: TrackingMemo, T <: CompletionCallbackFactory[_, _]](
      current: List[T],
      scope: TrackingScope[M],
      callback: Any): List[T] = {
    current dropWhile { c: T @unchecked =>
      (c.scope == scope) && (c.callback == callback)
    }
  }

  def registerNodeInvalidated[M >: Null <: TrackingMemo](
      scope: TrackingScope[M],
      callback: (TrackedNode[_], M) => Unit): Unit =
    withRegistrationLock(nodeInvalidated ::= new InvalidationCallbackNonBatched("node invalidated", scope, callback))
  def unregisterNodeInvalidated[M >: Null <: TrackingMemo](
      scope: TrackingScope[M],
      callback: (TrackedNode[_], M) => Unit): Unit = {
    withRegistrationLock { nodeInvalidated = removeDeregistered(nodeInvalidated, scope, callback); () }
  }

  def registerNodeWillBeInvalidated[M >: Null <: TrackingMemo](
      scope: TrackingScope[M],
      callback: (TrackedNode[_], M) => Unit): Unit =
    withRegistrationLock(
      nodeWillBeInvalidated ::= new InvalidationCallbackNonBatched("node will be invalidated", scope, callback))
  def unregisterNodeWillBeInvalidated[M >: Null <: TrackingMemo](
      scope: TrackingScope[M],
      callback: (TrackedNode[_], M) => Unit): Unit = {
    withRegistrationLock { nodeWillBeInvalidated = removeDeregistered(nodeWillBeInvalidated, scope, callback); () }
  }

  def registerNodeCompleted[M >: Null <: TrackingMemo](
      scope: TrackingScope[M],
      callback: (TrackedNode[_], Boolean, M) => Unit): Unit =
    withRegistrationLock(nodeCompleted ::= new CompletionCallbackNonBatched("node completed", scope, callback))
  def unregisterNodeCompleted[M >: Null <: TrackingMemo](
      scope: TrackingScope[M],
      callback: (TrackedNode[_], Boolean, M) => Unit): Unit = {
    withRegistrationLock { nodeCompleted = removeBooleanDeregistered(nodeCompleted, scope, callback); () }
  }

  def registerScenarioDisposal(callback: (DisposalInformation, EventCause) => Unit): Unit =
    withRegistrationLock(scenarioDisposal ::= callback)
  def unregisterScenarioDisposal(callback: (DisposalInformation, EventCause) => Unit): Unit =
    withRegistrationLock { scenarioDisposal = scenarioDisposal.dropWhile(_ == callback); () }

  // Batch APIs
  def registerNodeInvalidatedBatch[M >: Null <: TrackingMemo](
      scope: TrackingScope[M],
      callback: (List[(TrackedNode[_], M)], EventCause) => Unit): Unit =
    withRegistrationLock(
      nodeInvalidated ::= new InvalidationCallbackBatchedFactory[M]("node invalidated", scope, callback))
  def unregisterNodeInvalidatedBatch[M >: Null <: TrackingMemo](
      scope: TrackingScope[M],
      callback: List[(TrackedNode[_], M)] => Unit): Unit = {
    withRegistrationLock { nodeInvalidated = removeDeregistered(nodeInvalidated, scope, callback); () }
  }

  def registerNodeCompletedBatch[M >: Null <: TrackingMemo](
      scope: TrackingScope[M],
      callback: (List[(TrackedNode[_], Boolean, M)], EventCause) => Unit): Unit =
    withRegistrationLock(nodeCompleted ::= new CompletionCallbackBatchedFactory[M]("node completed", scope, callback))
  def unregisterNodeCompletedBatch[M >: Null <: TrackingMemo](
      scope: TrackingScope[M],
      callback: (List[(TrackedNode[_], Boolean, M)]) => Unit): Unit = {
    withRegistrationLock { nodeCompleted = removeBooleanDeregistered(nodeCompleted, scope, callback); () }
  }

  private def callback[CB](callbacks: List[(CB, EventCause) => Unit], data: CB, cause: EventCause): Unit = {
    for (cb <- callbacks)
      try cb(data, cause)
      catch {
        case ex: Exception => log.warn("ignored exception from DependencyTracker callback", ex)
      }
  }

  private[tracking] def notifyScenarioDisposal(disposal: DisposalInformation, cause: EventCause): Unit =
    cause.counted { callback(scenarioDisposal, disposal, cause) }
}

private[tracking] object CallbackBase {
  val log: Logger = getLogger(this.getClass)
}

private[tracking] sealed abstract class CallbackBase[M >: Null <: TrackingMemo](
    val callbackType: String,
    val scope: TrackingScope[M]) {
  protected def ignored(e: Throwable): Unit = { CallbackBase.log.warn("Ignored failure in callback", e) }

  // only send a notification for scopes that allow notifications without EventCause set on the memo, or, for those that
  // don't, only send the notification if the memo has a valid EventCause
  protected def notifyCallback(scope: TrackingScope[_], memo: TrackingMemo)(fn: => Unit): Unit = {
    if (scope.notifyWithoutEventCause || memo.hasCause) {
      memo.cause.counted { fn }
    } else
      memo.cause.throwOrLogException(
        s"Notifications cannot be sent for tracked nodes without an EventCause in scope $scope",
        debug = true)
  }
}

private[tracking] sealed abstract class Callback[M >: Null <: TrackingMemo](
    callbackType: String,
    scope: TrackingScope[M])
    extends CallbackBase(callbackType, scope) {

  def copyNodes(): Map[DependencyTracker, Map[TrackedNode[_], M]]
  def removeMatching(toRemove: TrackedNode[_] => Boolean): Unit
  def addCallback(tn: TrackedNode[_]): Unit
}

private[tracking] sealed trait BatchedCallback[M >: Null <: TrackingMemo] {
  val scope: TrackingScope[M]
  def complete(cause: EventCause): Unit
}

private[tracking] sealed abstract class CompletionCallback[M >: Null <: TrackingMemo](
    callbackType: String,
    scope: TrackingScope[M])
    extends CallbackBase(callbackType, scope) {
  def addCallback(tn: TrackedNode[_], changed: Boolean): Unit
}
private[tracking] trait InvalidationCallbackFactory[M >: Null <: TrackingMemo, CB] {
  def apply(owner: CallbackBatchOwner): Callback[M]
  val callbackType: String
  val scope: TrackingScope[M]
  val callback: CB
}

private[tracking] class InvalidationCallbackNonBatched[M >: Null <: TrackingMemo](
    callbackType: String,
    scope: TrackingScope[M],
    val callback: (TrackedNode[_], M) => Unit)
    extends Callback(callbackType, scope)
    with InvalidationCallbackFactory[M, (TrackedNode[_], M) => Unit] {
  override def apply(owner: CallbackBatchOwner): Callback[M] = this
  override def addCallback(tn: TrackedNode[_]): Unit = {
    tn.getMemoOption(scope) foreach { memo =>
      try {
        notifyCallback(scope, memo) { callback(tn, memo) }
        if (Settings.trackingScenarioLoggingEnabled)
          DependencyTrackerLogging.callback(callbackType, memo.cause, scope, tn :: Nil)
      } catch {
        case e: Exception => ignored(e)
      }
    }
  }
  // no batch so nothing to remove
  override def removeMatching(toRemove: TrackedNode[_] => Boolean): Unit = ()
  override def copyNodes(): Map[DependencyTracker, Map[TrackedNode[_], M]] = Map.empty
}

private[tracking] trait CompletionCallbackFactory[M >: Null <: TrackingMemo, CB] {
  def apply(owner: CallbackBatchOwner): CompletionCallback[M]
  val scope: TrackingScope[M]
  val callback: CB
}

private[tracking] class CompletionCallbackNonBatched[M >: Null <: TrackingMemo](
    callbackType: String,
    scope: TrackingScope[M],
    val callback: (TrackedNode[_], Boolean, M) => Unit)
    extends CompletionCallback(callbackType, scope)
    with CompletionCallbackFactory[M, (TrackedNode[_], Boolean, M) => Unit] {
  override def apply(owner: CallbackBatchOwner): CompletionCallback[M] = this
  override def addCallback(tn: TrackedNode[_], changed: Boolean): Unit = {
    tn.getMemoOption(scope) foreach { memo =>
      try {
        notifyCallback(scope, memo) { callback(tn, changed, memo) }
        if (Settings.trackingScenarioLoggingEnabled)
          DependencyTrackerLogging.callback(callbackType, memo.cause, scope, tn :: Nil)
      } catch {
        case e: Exception => ignored(e)
      }
    }
  }
}

private[tracking] class InvalidationCallbackBatchedFactory[M >: Null <: TrackingMemo](
    val callbackType: String,
    val scope: TrackingScope[M],
    val callback: (List[(TrackedNode[_], M)], EventCause) => Unit)
    extends InvalidationCallbackFactory[M, (List[(TrackedNode[_], M)], EventCause) => Unit] {
  override def apply(owner: CallbackBatchOwner): Callback[M] =
    new InvalidationCallbackBatched[M](callbackType, scope, callback, owner)
}

private[tracking] class CompletionCallbackBatchedFactory[M >: Null <: TrackingMemo](
    val callbackType: String,
    val scope: TrackingScope[M],
    val callback: (List[(TrackedNode[_], Boolean, M)], EventCause) => Unit)
    extends CompletionCallbackFactory[M, (List[(TrackedNode[_], Boolean, M)], EventCause) => Unit] {
  override def apply(owner: CallbackBatchOwner): CompletionCallback[M] =
    new CompletionCallbackBatched[M](callbackType, scope, callback, owner)
}

private[tracking] sealed trait CallbackBatch {
  protected val trackerRoot: DependencyTrackerRoot
  protected val owner: CallbackBatchOwner
  protected[tracking] val cause: EventCause

  private[graph] def nodeInvalidated = trackerRoot.root.nodeInvalidated.map { _.apply(owner) }
  private[graph] def nodeWillBeInvalidated = trackerRoot.root.nodeWillBeInvalidated.map { _.apply(owner) }
  private[graph] def nodeCompleted = trackerRoot.root.nodeCompleted.map { _.apply(owner) }

  /**
   * Called when a batcher needs a completion callback, but only once for each batch Must be called after the batcher
   * has stored the information locally, as it may execute the batch immediately
   * @param batcher
   */
  def addBatcher(batcher: BatchedCallback[_]): Unit
  def complete(): Unit
}

private[tracking] class CallbackBatcher(
    val cause: EventCause,
    protected val trackerRoot: DependencyTrackerRoot,
    protected val owner: CallbackBatchOwner)
    extends CallbackBatch {
  // cache these so that we only ever create one batch of each type
  private[graph] override lazy val nodeInvalidated = super.nodeInvalidated
  private[graph] override lazy val nodeWillBeInvalidated = super.nodeWillBeInvalidated
  private[graph] override lazy val nodeCompleted = super.nodeCompleted

  private var pending = List.empty[BatchedCallback[_]]
  override def addBatcher(batcher: BatchedCallback[_]): Unit = pending = batcher :: pending
  override def complete(): Unit = if (!pending.isEmpty) {
    pending foreach { _.complete(cause) }
    pending = Nil
  }
}

private[tracking] class CallbackNotInBatch(
    override val cause: EventCause,
    protected val trackerRoot: DependencyTrackerRoot,
    protected val owner: CallbackBatchOwner)
    extends CallbackBatch {
  override def addBatcher(batcher: BatchedCallback[_]): Unit = {
    batcher.complete(NoEventCause)
  }
  override def complete(): Unit = ()
}

private[tracking] class InvalidationCallbackBatched[M >: Null <: TrackingMemo](
    callbackType: String,
    callbackScope: TrackingScope[M],
    val callback: (List[(TrackedNode[_], M)], EventCause) => Unit,
    owner: CallbackBatchOwner)
    extends Callback(callbackType, callbackScope)
    with BatchedCallback[M] {
  import scala.collection.mutable
  private val pending = mutable.Map.empty[DependencyTracker, mutable.Map[TrackedNode[_], M]]
  override def addCallback(tn: TrackedNode[_]): Unit = {
    tn.getMemoOption(scope) foreach { memo =>
      val first = pending.isEmpty
      pending.getOrElseUpdate(tn.dependencyTracker, mutable.Map.empty[TrackedNode[_], M]).update(tn, memo)
      if (first) owner.addBatcher(this)
    }
  }
  def complete(cause: EventCause): Unit = {
    val pendingList: List[(TrackedNode[_], M)] = pending.iterator.flatMap {
      case (ts, _) if ts.isDisposed => Nil
      case (_, v)                   => v.toList
    }.toList

    if (Settings.trackingScenarioLoggingEnabled)
      DependencyTrackerLogging.callback(callbackType, cause, scope, pendingList.map(_._1))
    try {
      cause.counted { callback(pendingList, cause) }
    } catch {
      case e: Exception => ignored(e)
    } finally {
      pending.clear()
    }
  }
  override def copyNodes(): Map[DependencyTracker, Map[TrackedNode[_], M]] = {
    pending map { case (k, v) => (k -> v.toMap) } toMap
  }
  override def removeMatching(toRemove: TrackedNode[_] => Boolean): Unit = {
    pending foreach {
      // need to calculate and remove, not remove as you go for concurrent access
      case (k, v) => v --= (v.keySet.filter(toRemove))
    }
  }
}

private[tracking] class CompletionCallbackBatched[M >: Null <: TrackingMemo](
    callbackType: String,
    scope: TrackingScope[M],
    val callback: (List[(TrackedNode[_], Boolean, M)], EventCause) => Unit,
    owner: CallbackBatchOwner)
    extends CompletionCallback(callbackType, scope)
    with BatchedCallback[M] {
  private[tracking] var pending = List.empty[(TrackedNode[_], Boolean, M)]
  override def addCallback(tn: TrackedNode[_], changed: Boolean): Unit = {
    tn.getMemoOption(scope) foreach { memo =>
      val first = pending eq Nil
      pending = (tn, changed, memo) :: pending
      if (first) owner.addBatcher(this)
    }
  }
  def complete(cause: EventCause): Unit = {
    if (Settings.trackingScenarioLoggingEnabled)
      DependencyTrackerLogging.callback(callbackType, cause, scope, pending.map(_._1))
    try {
      pending.groupBy(_._3.cause) foreach { case (groupCause, nodes) =>
        if (scope.notifyWithoutEventCause || !groupCause.isEmpty) {
          groupCause.counted {
            callback(nodes, groupCause)
          } // long term, this should be handled better because callbacks may modify the nodes
        } else
          groupCause.throwOrLogException(
            s"Notifications cannot be sent for tracked nodes without an EventCause in scope $scope",
            debug = true)
      }
    } catch {
      case e: Exception => ignored(e)
    } finally {
      pending = Nil
    }
  }
}

private[tracking] trait CallbackBatchOwner {
  protected def trackerRoot: DependencyTrackerRoot

  private[this] val notInBatch = new CallbackNotInBatch(NoEventCause, trackerRoot, this)
  private[this] var currentBatch: CallbackBatch = notInBatch

  private[tracking] def currentBatcher: CallbackBatch = currentBatch
  private[tracking] def resetCurrentBatcher(): Unit = currentBatch = notInBatch
  private[tracking] def setCurrentBatcher(batcher: CallbackBatch): Unit = {
    if (batcher == notInBatch) log.warn(s"Trying to reset batcher to notInBatch")
    currentBatch = batcher
  }

  def addBatcher(batcher: BatchedCallback[_]): Unit = currentBatch.addBatcher(batcher)

  def startBatch(cause: EventCause): Unit = {
    if (currentBatch eq notInBatch) {
      currentBatch = new CallbackBatcher(cause, trackerRoot, this)
    } else {
      log.error("unexpected overlapping batches", new Exception("for stacktrace"))
    }
  }
  def endBatch: Unit = {
    currentBatch.complete()
    currentBatch = notInBatch
  }

  def currentNodeInvalidationBatches: List[Callback[_]] = currentBatch.nodeInvalidated

  def notifyNodeWillBeInvalidated(tn: TrackedNode[_]): Unit = {
    if ((tn ne null) && tn.isDone) {
      for (cb <- currentBatch.nodeWillBeInvalidated)
        cb.addCallback(tn)
    }
  }

  def notifyNodeInvalidated(tn: TrackedNode[_]): Unit = {
    if ((tn ne null) && (!tn.isDone)) {
      for (cb <- currentBatch.nodeInvalidated)
        cb.addCallback(tn)
    }
  }

  def notifyNodeCompletedForSingleScope(tn: TrackedNode[_], scope: TrackingScope[_]): Unit = {
    if (trackerRoot.nodeCompleted ne null) {
      for (cb <- currentBatch.nodeCompleted if cb.scope == scope)
        cb.addCallback(tn, true)
    }
  }

  def notifyNodeCompleted[T](current: TrackedNode[T], previous: TrackedNode[T]): Unit = {
    if (trackerRoot.nodeCompleted ne null) {
      val change =
        if (!trackerRoot.reportChange) true
        else {
          !(
            (previous != null) &&
              previous.isDoneWithResult &&
              current.isDoneWithResult &&
              (current.result == previous.result)
          )
        }

      for (cb <- currentBatch.nodeCompleted)
        cb.addCallback(current, change)
    }
  }
}
