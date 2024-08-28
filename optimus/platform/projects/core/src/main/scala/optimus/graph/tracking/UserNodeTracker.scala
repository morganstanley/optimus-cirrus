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

import optimus.debug.RTVerifierCategory
import optimus.graph._
import optimus.graph.diagnostics.rtverifier.RTVerifierReporter
import optimus.graph.tracking.DependencyTracker.log
import optimus.logging.LoggingInfo
import optimus.platform.util.PrettyStringBuilder
import optimus.platform._
import optimus.ui.ScenarioReference

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.util.Try

object OverInvalidationDetection extends ThreadLocal[EventCause] {

  def checkOverInvalidationInUTrack(utrack: TrackedNode[_], memos: Iterable[TrackingMemo]): Unit = {
    val invalidateCause = get()
    if (invalidateCause ne null) {
      val invalidateRootCause = invalidateCause.root
      val it = memos.iterator
      while (it.hasNext) {
        val lastEvalCause = it.next().cause
        if (invalidateRootCause eq lastEvalCause.root) {
          val msg =
            s">>>>> Overly invalidating node in the same handler: $utrack. This could cause potential performance issue. Enable TraceNodes in graph debugger to check the node path."
          val eventStack =
            s"Previous evaluation cause stack:\n${lastEvalCause.causeStack}\n\nCurrent invalidation cause stack:\n${invalidateCause.causeStack}"
          RTVerifierReporter.reportAppletViolation(
            category = RTVerifierCategory.UTRACK_OVER_INVALIDATION,
            key = msg,
            details = eventStack,
            appletId = appletId(utrack.dependencyTracker.root))
          if (DiagnosticSettings.debugAssist) {
            log.warn(msg)
            log.warn(eventStack)
          }
        }
      }
    }
  }

  def checkOverInvalidationInDebugTrack(
      cause: EventCause,
      debugTrackId: Int,
      dependencyTrackerRoot: DependencyTrackerRoot): Unit = {
    val invalidateCause = get()
    if ((invalidateCause ne null) && (invalidateCause.root eq cause.root)) {
      val msg =
        s">>>>> A Node is snapped and then tweaked in the same handler, this might cause unexpected result. Enable TraceNodes in graph debugger and search for [uiTrackDebug] with ID $debugTrackId to check the node path."
      val eventStack =
        s"Previous evaluation cause stack:\n${cause.causeStack}\n\nCurrent invalidation cause stack:\n${invalidateCause.causeStack}"
      RTVerifierReporter.reportAppletViolation(
        category = RTVerifierCategory.UI_INVALIDATION_AFTER_EVAL,
        key = msg,
        details = eventStack,
        appletId = appletId(dependencyTrackerRoot))
      log.warn(msg)
      if (DiagnosticSettings.debugAssist) log.warn(eventStack)
    }
  }

  private def appletId(dependencyTrackerRoot: DependencyTrackerRoot): String = {
    val appletName = Option(dependencyTrackerRoot.appletName).filter(_.nonEmpty)
    val tokens = LoggingInfo.appName ++ appletName
    if (tokens.isEmpty) "UNKNOWN" else tokens.mkString("-")
  }
}

/**
 * Tracks user-specified nodes within a DependencyTracker and notifies the user about invalidation and recomputation
 * (here "user" means a user of DependencyTracker, e.g. UI or Reactive). The user can attach arbitrary memos to the
 * tracked nodes.
 */
private[graph] final class UserNodeTracker(tracker: DependencyTracker) {

  /**
   * A map from PropertyNodes to UTracks for those nodes. UTracks can be read from multiple eval threads, but we don't
   * have to synchronize on them, because the whole operation is sequentialized.
   */
  private[tracking] var utracks: java.util.Map[NodeKey[_], UTrack[_]] = new ConcurrentHashMap()

  // Snapshot all current tracked nodes
  private[tracking] def flat: Seq[(NodeKey[_], TrackedNode[_])] = {
    val builder = Seq.newBuilder[(NodeKey[_], TrackedNode[_])]
    utracks.forEach((k, utrack) => { builder += k -> utrack.anyCurrent })
    builder.result()
  }

  private[optimus] def getUTrack[T](key: NodeKey[T]): UTrack[T] = utracks.get(key).asInstanceOf[UTrack[T]]

  private[tracking] def disposeUserNodeTracker(): Unit = utracks = null

  private[tracking] def invalidateAll(): Unit = utracks.values.stream().forEach(_.invalidate())

  /**
   * Update user tracking information for a given NodeKey and TrackingScope.
   *
   * @param key
   *   The NodeKey for which to update the tracking information.
   * @param scope
   *   The TrackingScope for which to update the tracking information.
   * @param memoUpdater
   *   The update function.
   * @tparam M
   *   The memo type.
   * @tparam T
   *   The node type.
   * @return
   *   the TrackedNode, if the memoUpdater tracked the node (returned non null)
   */
  private[tracking] def userTrackUpdate[T, M >: Null <: TrackingMemo](
      key: NodeKey[T],
      scope: TrackingScope[M],
      memoUpdater: M => M): TrackedNode[T] = {
    if (Settings.trackingScenarioLoggingEnabled) DependencyTrackerLogging.logTrack(tracker.name, key, scope)
    if (tracker.isDisposed) {
      log.error(s"DependencyTracker ${tracker.name} disposed! track() called on $key")
      null // Really should throw!!!
    } else {
      val utrack = utracks
        .compute(
          key,
          (_, utrk) => {
            if (utrk eq null) {
              val newMemo = memoUpdater(null)
              if (newMemo ne null) new UTrack(Map(scope -> newMemo), key) else null
            } else {
              utrk.updateMemo(scope, memoUpdater)
            }
          }
        )
        .asInstanceOf[UTrack[T]]

      if (utrack ne null) utrack.anyCurrent else null
    }
  }

  /**
   * User tracking information that is attached to nodes.
   *
   * @param memos
   *   Memos for this node, indexed by the TrackingScope in which they apply.
   * @tparam T
   *   The result type of the node.
   */
  private[tracking] final class UTrack[T] private[UserNodeTracker] (
      private[this] var memos: Map[TrackingScope[_], TrackingMemo],
      inputKey: NodeKey[T]) { utrack =>

    // Implementation details:
    //
    // UTrack keeps track of a UTrackNode, which is attached to the stuff we are tracking, and locked behind a RW lock.
    // When invalidating, we swap out the current UTrackNode for a new one, and let the old one get GC-ed. The old
    // UTrackNode can still be invalidated or completed, but it won't communicate with the registered callbacks.

    private val template = inputKey.tidyKey.asInstanceOf[PropertyNode[T]]

    // both _previous and _current are only updated under the write lock
    private var _previous: UTrackNode = _
    private var _current = new UTrackNode
    private val lock = new ReentrantReadWriteLock()

    /* Returns the current tracked node without locking */
    def anyCurrent: TrackedNode[T] = _current

    /* Run a function that reads the current UTrack, with a read lock. */
    def read[A](f: UTrackNode => A): A = {
      lock.readLock().lock()
      try f(_current)
      finally lock.readLock().unlock()
    }

    /* Compute f if the target matches current with a read lock. */
    def doIf[A](target: UTrackNode)(f: UTrackNode => Unit): Unit = read { c => if (c eq target) f(_previous) }

    /* Swap out the current UTrackNode for a new one if it matches target. */
    def swapIf(target: UTrackNode)(run: => UTrackNode): Unit = {
      lock.writeLock().lock()
      try
        if (target eq _current) {
          _previous = _current
          _current = run
        }
      finally lock.writeLock().unlock()
    }

    /* Invalidate child UTrackNode */
    private[UserNodeTracker] def invalidate(): Unit = {
      lock.writeLock().lock()
      try _current.invalidateCache()
      finally lock.writeLock().unlock()
    }

    /**
     * Get the memo for this UTrack in a given TrackingScope. An Option is returned, which is None if no memo is
     * present.
     *
     * @param scope
     *   The scope in which to get the memo.
     * @tparam M
     *   The memo type.
     * @return
     *   The memo in the given scope, or None.
     */
    private[tracking] def getMemoOption[M >: Null <: TrackingMemo](scope: TrackingScope[M]): Option[M] =
      synchronized(memos.get(scope)).asInstanceOf[Option[M]]

    /**
     * Apply an update function to the memo for this UTrack in a given TrackingScope.
     *
     * @param scope
     *   The scope for which to update the memo.
     * @param memosUpdater
     *   Function to apply to the memo for this UTrack.
     * @tparam M
     *   The memo type.
     * @return
     *   Whether the utrack can be removed from the DependencyTracker
     */
    private[UserNodeTracker] def updateMemo[M >: Null <: TrackingMemo](
        scope: TrackingScope[M],
        memosUpdater: M => M): UTrack[T] = synchronized {
      val curMemo = getMemoOption(scope).orNull
      val newMemo = memosUpdater(curMemo)
      if (Settings.trackingScenarioLoggingEnabled)
        DependencyTrackerLogging.logTrackUpdateMemo(tracker.name, anyCurrent, scope, curMemo, newMemo)
      if (newMemo eq null) {
        memos -= scope
        if (memos.isEmpty) {
          null
        } else this
      } else {
        if (newMemo ne curMemo) { memos = memos.updated(scope, newMemo) }
        this
      }
    }

    private[tracking] def updateCause(cause: EventCause): Unit = synchronized {
      // we never return a UTrack where a memo in a given scope has a cause with a different scope
      memos.get(cause.scope).foreach { memo => memo.setCause(cause) }
    }

    private[tracking] def registerRunningNode(node: Node[T]): TrackedNode[T] = {
      read { current =>
        node.continueWithIfEverRuns(current, null)
        OGTrace.observer.manualStart(current)
        current
      }
    }

    /**
     * This is a proxy object around a PropertyNode, which holds user information in the form of TrackingMemos
     * associated with this Node for a given TrackingScope.
     */
    private final class UTrackNode extends ProxyPropertyNode[T] with TrackedNode[T] {
      // Make a full key
      srcNodeTemplate = utrack.template
      // This is needed since when setup ttracks, see NodeTask.combineInfo -> _scenarioStack.isTrackingOrRecordingTweakUsage()
      attach(tracker.scenarioStack)

      /**
       * The DependencyTracker tracking this Node.
       *
       * @return
       *   The DependencyTracker tracking this Node.
       */
      private[optimus] override def dependencyTracker: DependencyTracker = tracker
      override def key: NodeKey[T] = tidyKey

      /**
       * Get the memo for this UTrack in a given TrackingScope. An Option is returned, which is None if no memo is
       * present.
       *
       * @param scope
       *   The scope in which to get the memo.
       * @tparam M
       *   The memo type.
       * @return
       *   The memo in the given scope, or None.
       */
      private[tracking] def getMemoOption[M >: Null <: TrackingMemo](scope: TrackingScope[M]): Option[M] =
        utrack.getMemoOption(scope)

      /**
       * Get proxied PropertyNode.
       *
       * @return
       *   The proxied PropertyNode.
       */
      override private[tracking] def underlying: PropertyNode[T] = srcNodeTemplate

      override def executionInfo: NodeTaskInfo = NodeTaskInfo.UITrack

      override def effectivelyCacheable = true

      override def run(ec: OGSchedulerContext): Unit =
        throw new GraphInInvalidState("UTrack should never be run directly")

      override def onChildCompleted(eq: EvaluationQueue, ntsk: NodeTask): Unit = {
        val doNotify = {
          if (isDone) false
          else {
            // We might be receiving callbacks from multiple nodes if registerRunningNodes was called
            // multiple times, so we need to synchronize the node to avoid completing twice
            this.synchronized {
              if (isDone) false
              else {
                val preempted = ntsk.exception match {
                  case _: CancelPreemptedInFlightLowPriorityNodes => true
                  case e: ScopeWasCancelledException => e.getCause.isInstanceOf[CancelPreemptedInFlightLowPriorityNodes]
                  case _                             => false
                }
                !preempted && {
                  try { this.completeFromNode(ntsk.asInstanceOf[Node[T]], eq) }
                  catch {
                    case e: GraphException => completeWithException(e, eq)
                  }
                  true
                }
              }
            }
          }
        }

        // we have completed the node, do callbacks if we are the current node
        if (doNotify) doIf(this) { previous =>
          EvaluationContext.asIfCalledFrom(this, eq) { tracker.queue.notifyNodeCompleted(this, previous) }
        }
        else if (Settings.schedulerAssertsExt) {
          throw new GraphInInvalidState("Got two completion callbacks on the same UTrackNode")
        }
      }

      override def invalidateCache(): Unit = swapIf(this) {
        if (isDone) {
          OverInvalidationDetection.checkOverInvalidationInUTrack(this, memos.values)
          // invalidate template
          if (template.isStable) {
            // We still need to reset _xinfo
            template.unsafeResetInfo()
          } else if (template.isDone) {
            template.invalidateCache()
          }
        } else if (Settings.schedulerAssertsExt && (scenarioStack ne ScenarioStack.constant)) {
          // It possible for the user to un-track this UTTrack (will call cleanReset)
          // but it will still be hanging off TTracks. In that case we will get notification to invalidate afterwards
          // This assertion effectively verifies that cleanReset was called
          throw new GraphInInvalidState("We should only get invalidates on completed nodes")
        }

        // add callback for new UTrackNode
        val newNode = new UTrackNode
        tracker.queue.notifyNodeInvalidated(newNode)
        newNode
      }

      // TrackedNode impl - only used in Excel
      override def evaluateAsync(callback: Try[T] => Unit, cause: EventCause): Unit = {
        tracker.evaluateNodeKeyAsync(srcNodeTemplate, callback, cause)
      }
    }
  }

  private[tracking] def dumpUTrackDebugInfo(sb: PrettyStringBuilder): Unit = {
    import scala.jdk.CollectionConverters._
    sb.append(s"UTrack state for ${tracker.scenarioReference}")
    sb.startBlock()
    utracks.asScala.foreach { case (key, utrack) =>
      sb.appendln(s"$key ===> ${utrack.anyCurrent.visitForwardChain().toArray.mkString(" -> ")}")
    }
    sb.endBlock()
  }
}

/**
 * Subtrait of Nodes which contain additional information and are tracked by a UserNodeTracker. The only implementation
 * is UTrackNode above.
 */
sealed trait TrackedNode[T] extends Node[T] with TweakableKey {
  import scala.util.Failure
  import scala.util.Success
  import scala.util.Try
  def evaluateAsync(callback: Try[T] => Unit, cause: EventCause): Unit

  def scenarioReference: ScenarioReference = dependencyTracker.scenarioReference

  def key: NodeKey[T]

  private[optimus] def resultAsTry: Try[T] = {
    assert(isDone)
    if (isDoneWithResult) Success(result)
    else Failure(exception)
  }

  /**
   * The DependencyTracker that tracks this Node.
   *
   * @return
   *   The DependencyTracker that tracks this Node.
   */
  private[optimus] def dependencyTracker: DependencyTracker
  private[tracking] def underlying: PropertyNode[T]
  private[tracking] def getMemoOption[M >: Null <: TrackingMemo](scope: TrackingScope[M]): Option[M]
}

/**
 * Trait of memo objects attached to TrackingNodes in a TrackingScope.
 */
private[optimus] abstract class TrackingMemo {
  // TODO (OPTIMUS-23190): remove this default when memo and notification model changes
  private[this] var eventCause: EventCause = NoEventCause
  def cause: EventCause = eventCause
  private[tracking] def hasCause: Boolean = eventCause != NoEventCause
  private[tracking] def setCause(cause: EventCause): Unit = this.eventCause = cause
}

private[optimus] final case class EmptyMemo(name: String = "EmptyMemo") extends TrackingMemo

private[optimus] object NullScope extends TrackingScope[TrackingMemo] {
  override def defaultMemo(node: NodeKey[_], ts: DependencyTracker): TrackingMemo = EmptyMemo()
}

/**
 * Defines a scope for the tracking. For a given scope there will be at most one callback memo registered per tracked
 * node in a scenario.
 *
 * @tparam M
 *   The type of the memo returns in the callbacks.
 */
private[optimus] trait TrackingScope[M >: Null <: TrackingMemo] {

  /**
   * Get the TrackingMemo for a given Node.
   *
   * @param node
   *   The node for which to get the TrackingMemo.
   * @param ts
   *   associated tracking scenario
   * @return
   *   The TrackingMemo for the node.
   */
  def defaultMemo(node: NodeKey[_], ts: DependencyTracker): M

  def notifyWithoutEventCause: Boolean = true // change to false when every tracking scope supports EventCause
}

/**
 * A TrackingScope that contains no memos for any node.
 *
 * @param id
 *   The name of this tracking scope
 * @tparam M
 *   The type of the memo returns in the callbacks.
 */
private[optimus] abstract class SimpleTrackingScope[M >: Null <: TrackingMemo](id: String) extends TrackingScope[M] {
  override def toString: String = s"SimpleTrackingScope@${Integer.toHexString(System.identityHashCode(this))} - ($id)"
}
