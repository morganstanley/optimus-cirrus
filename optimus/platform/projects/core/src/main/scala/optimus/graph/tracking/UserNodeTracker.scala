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

object OverInvalidationDetection extends ThreadLocal[EventCause] {

  def checkOverInvalidationInUTrack(utrack: TrackedNode[_], memo: TrackingMemo): Unit = {
    val invalidateCause = get()
    if (invalidateCause ne null) {
      val invalidateRootCause = invalidateCause.root
      val lastEvalCause = utrack.cause
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
private[graph] final class UserNodeTracker[M >: Null <: TrackingMemo](
    tracker: DependencyTracker,
    val scope: TrackingScope[M]) {
  userNodeTracker =>

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

  private[tracking] def invalidateAll(observer: TrackedNodeInvalidationObserver): Unit =
    utracks.values.stream().forEach(_.invalidate(observer))

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
  private[tracking] def userTrackUpdate[T](key: NodeKey[T], memoUpdater: M => M): TrackedNode[T] = {
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
              if (newMemo ne null) new UTrack(newMemo, key) else null
            } else {
              utrk.updateMemo(memoUpdater)
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
      @volatile private[this] var memo: M,
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
    private[UserNodeTracker] def invalidate(observer: TrackedNodeInvalidationObserver): Unit = {
      lock.writeLock().lock()
      try _current.invalidateCache(observer)
      finally lock.writeLock().unlock()
    }

    private[tracking] def getMemo: M = memo

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
    private[UserNodeTracker] def updateMemo(memosUpdater: M => M): UTrack[T] = synchronized {
      val prevMemo = memo
      memo = memosUpdater(memo)
      if (Settings.trackingScenarioLoggingEnabled)
        DependencyTrackerLogging.logTrackUpdateMemo(tracker.name, anyCurrent, scope, prevMemo, memo)
      if (memo eq null) null else this
    }

    private[tracking] def registerRunningNode(node: Node[T], cause: EventCause): TrackedNode[T] = {
      read { current =>
        current.setCause(cause)
        node.continueWithIfEverRuns(current, null)
        OGTrace.observer.manualStart(current)
        current
      }
    }

    /**
     * This is a proxy object around a PropertyNode, which holds user information in the form of a TrackingMemo
     * associated with this Node within this scope and dependency tracker.
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
      private[optimus] override def scope: TrackingScope[_] = userNodeTracker.scope

      private[optimus] def getMemo: M = utrack.getMemo

      private var tokens: List[EventCause.Token] = Nil
      @volatile private var _cause: EventCause = NoEventCause
      private[optimus] override def cause: EventCause = _cause
      private[optimus] def setCause(cause: EventCause): Unit = synchronized {
        // we will only call the completion notification once, so we only pass a single (arbitrary) event cause there,
        // but we do want to keep all requesting EventCauses active, so we take a token from all of them and only
        // release after the callback is complete
        _cause = cause
        tokens ::= cause.createAndTrackToken()
      }

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

      override def onChildCompleted(eq: EvaluationQueue, ntsk: NodeTask): Unit = try {
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
          EvaluationContext.asIfCalledFrom(this, eq) {
            if (memo ne null) tracker.root.notifyNodeCompleted(this, previous)
          }
        }
        else if (Settings.schedulerAssertsExt) {
          throw new GraphInInvalidState("Got two completion callbacks on the same UTrackNode")
        }
      } finally
        synchronized {
          var failure: Exception = null
          // Release all tokens, even if one or more throws. Collect all failures.
          tokens.foreach { t =>
            try t.release()
            catch {
              case f: Exception =>
                if (failure eq null) failure = f
                else failure.addSuppressed(f)
            }
          }
          tokens = Nil
          if (failure ne null) throw failure
        }

      override def invalidateCache(observer: TrackedNodeInvalidationObserver): Unit = swapIf(this) {
        if (isDone) {
          if (memo ne null) OverInvalidationDetection.checkOverInvalidationInUTrack(this, memo)
          // invalidate template
          if (template.isStable) {
            // We still need to reset _xinfo
            template.unsafeResetInfo()
          } else if (template.isDone) {
            template.invalidateCache(observer)
          }
        } else if (Settings.schedulerAssertsExt && (scenarioStack ne ScenarioStack.constant)) {
          // It possible for the user to un-track this UTTrack (will call cleanReset)
          // but it will still be hanging off TTracks. In that case we will get notification to invalidate afterwards
          // This assertion effectively verifies that cleanReset was called
          throw new GraphInInvalidState("We should only get invalidates on completed nodes")
        }

        // add callback for new UTrackNode
        val newNode = new UTrackNode
        if (memo ne null) observer.notifyNodeInvalidated(newNode)
        newNode
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
  private[tracking] def getMemo: TrackingMemo // can be null if the node has been untracked
  private[optimus] def scope: TrackingScope[_]
  private[optimus] def cause: EventCause
}

/**
 * Trait of memo objects attached to TrackingNodes in a TrackingScope.
 */
private[optimus] trait TrackingMemo

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
