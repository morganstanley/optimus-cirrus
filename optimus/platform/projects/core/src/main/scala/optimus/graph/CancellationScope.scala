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

import java.util.UUID
import com.github.benmanes.caffeine.cache.Caffeine
import msjava.base.util.uuid.MSUuid
import msjava.slf4jutils.scalalog._
import optimus.platform.ScenarioStack

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

/**
 * Representation of a grouping of nodes which can all be cancelled. Cancellation scopes are associated with one or more
 * scenario stacks, and any node executing in that scenario stack will be subject to cancellation by the scope.
 *
 * Note that nodes that have already started executing cannot be cancelled. Cancellation scopes work internally as an
 * event/listener style API, maintaining a set of listeners to be signalled upon cancellation.
 *
 * Cancellation scopes can also have child scopes, which will receive any cancellation events from their parent (unless
 * the child scope is dropped).
 *
 * @see
 *   CoreAPI.withCancellation
 */
sealed abstract class CancellationScope(val _cancelOnException: Boolean) extends Serializable {
  @volatile protected[graph] var cause: Throwable = _
  @transient protected var causeToAbortNodeWith: Throwable = _
  @transient final protected var uuid: UUID = _
  @transient private[optimus] val sequenceId: Long = CancellationScope.sequenceIdCounter.getAndIncrement()

  final def isCancelled: Boolean = cause != null
  final def cancellationCause: Throwable = cause

  /**
   * When a CS is cancelled manually every task is completed with OptimusCancellationException, otherwise if an
   * exception causes cancellation (because cancelOnException is true) then we use ScopeWasCancelledException to
   * complete unfinished tasks
   */
  final def cancellationCauseToAbortNodeWith(scenarioStack: ScenarioStack, ntsk: NodeTask = null): Throwable = {
    if (DiagnosticSettings.includeCancelledNodeInException)
      new ScopeWasCancelledException(scenarioStack, cause, ntsk)
    else
      this.synchronized {
        if (causeToAbortNodeWith eq null) {
          if (cause.isInstanceOf[OptimusCancellationException]) causeToAbortNodeWith = cause
          else causeToAbortNodeWith = new ScopeWasCancelledException(scenarioStack, cause, null)
        }
        causeToAbortNodeWith
      }
  }

  /** Lazily allocated uuid, don't need extra work of lazy val */
  protected def initializedUUID(): UUID = this.synchronized {
    if (uuid eq null) uuid = MSUuid.generateJavaUUID()
    uuid
  }

  override def toString = s"${this.getClass.getName}(${if (uuid != null) uuid else hashCode()}, $isCancelled)"

  /**
   * Signal a cancellation on this scope. The nodes will be cancelled with an OptimusCancellationException. Note that
   * this will not cancel already running nodes. Note that only the first cancellation will have an effect.
   *
   * @param msg
   *   The cancellation message.
   */
  final def cancel(msg: String): Unit = cancelInternal(new OptimusCancellationException(msg))

  /**
   * Signal a cancellation on this scope. The nodes will be cancelled with an OptimusCancellationException with a
   * default message.
   *
   * Note that this will not cancel already running nodes.
   *
   * Note that only the first cancellation will have an effect.
   */
  final def cancel(): Unit = cancel(s"Cancelled at ${System.currentTimeMillis()}")

  /**
   * Like  .cancel methods, but completes nodes with an ElusiveOptimusCancellationException, which cannot be trapped
   * with asyncResult and nodeResult.
   */
  final def cancelElusively(msg: String): Unit = cancelInternal(new ElusiveOptimusCancellationException(msg))

  /**
   * Signal a cancellation on this scope. The nodes will be cancelled with an OptimusCancellationException that wraps
   * the cause.
   *
   * Note that this will not cancel already running nodes.
   *
   * Note that only the first cancellation will have an effect.
   *
   * @param cause
   *   The cause of cancellation.
   */
  final def cancel(cause: Throwable): Unit = cancelInternal(new OptimusCancellationException(cause))

  private[graph] def cancelInternal(cause: Throwable): Unit

  /** The goal is for this method to ALWAYS return true! */
  final def cancelOnException: Boolean = _cancelOnException

  /**
   * Add a cancellation listener to this scope. This will fire when cancel is called.
   *
   * @param l
   *   The listener to add.
   */
  def addListener(l: CancellationScope => Unit): Unit

  /**
   * Remove a cancellation listener to this scope.
   *
   * @param l
   *   The listener to remove.
   */
  def removeListener(l: CancellationScope => Unit): Unit

  /**
   * Create a child scope which catches a given set of exceptions and signals cancellation for them, and hides another
   * set.
   */
  def childScope(): CancellationScope

  /**
   * Returns a CancellationScope parented to both this and other (which would cancel if this or other is cancelled)
   */
  private[optimus] def union(other: CancellationScope): CancellationScope

  private[optimus] def isSameOrTransitiveChildOf(other: CancellationScope): Boolean
}

object CancellationScope {
  private[graph] val log: Logger = getLogger(this.getClass)

  private val internCache = Caffeine.newBuilder().weakValues().build[UUID, CancellationScope]

  private[graph] def fromUUID(id: UUID, cause: Throwable): CancellationScope = {
    internCache.get(id, _ => new NestedCancellationScope(id, cause))
  }

  private val sequenceIdCounter = new AtomicLong()

  def newScope(): CancellationScope = new NestedCancellationScope(parent1 = NullCancellationScope, parent2 = null)
  def newScopeWithCancellationOption(enable: Boolean) =
    new NestedCancellationScope(enable, parent1 = NullCancellationScope, parent2 = null)
}

/**
 * Wrapper exception used to signal cancellation. This will be thrown by any node that gets cancelled.
 *
 * @param msg
 *   The cancellation message.
 * @param cause
 *   The wrapped cause, if one exists.
 */
class OptimusCancellationException private[optimus] (msg: String, cause: Throwable) extends Exception(msg, cause) {
  def this(msg: String) = this(msg, null)
  def this(cause: Throwable) = this("Cancelled via CancellationScope", cause)
  def wrapped = new OptimusCancellationException(msg, this)
}

class ElusiveOptimusCancellationException(msg: String, cause: Throwable)
    extends OptimusCancellationException(msg, cause)
    with InvisibleToNodeResult {
  def this(msg: String) = this(msg, null)
  override def wrapped: OptimusCancellationException = new ElusiveOptimusCancellationException(msg, cause)
}

/** When scheduler cancels nodes because CS was cancelled, this exception is used */
private[optimus] class ScopeWasCancelledException(
    scenarioStack: ScenarioStack,
    cause: Throwable,
    // for debugging only - N.B. this is null unless DiagnosticSettings.includeCancelledNodeInException is enabled
    private val cancelledNodeTask: NodeTask)
    extends GraphException("CS cancelled", cause)
    with FlowControlException {
  override def getMessage: String = s"CS cancelled. [ScenarioStack: $scenarioStack, CacheId: ${scenarioStack._cacheID}]"
}

/**
 * This is the implementation of CancellationScope that is used everywhere. This keeps a list of listeners and child
 * scopes and propagates cancellations to them.
 */
sealed private[graph] class NestedCancellationScope(
    cancelOnException: Boolean = Settings.cancelOnException,
    // It's important that these are strong (non-weak) references because the child needs to keep its ancestors alive
    // so that they can notify it about cancellations. Node that parent -> child references are weak, so without the
    // strong child -> parent link, we could have nodes running in a grandchild, the child could be GCd, and then
    // the parent would no longer have a link to the grandchild to cancel it.
    @transient parent1: CancellationScope /* not null */,
    @transient parent2: CancellationScope /* null unless this is a union */ )
    extends CancellationScope(cancelOnException) {
  @transient private var listeners: java.util.HashSet[CancellationScope => Unit] = _
  @transient private var children: mutable.WeakHashMap[CancellationScope, Object] = _ // Should be WeakList...

  /** For serialization only */
  def this(uuid: UUID, cause: Throwable) = {
    this(parent1 = NullCancellationScope, parent2 = null)
    this.cause = cause
    this.uuid = uuid
  }

  // noinspection ScalaUnusedSymbol
  private def writeReplace(): AnyRef = new NestedCSMoniker(cause, initializedUUID())

  /** Cancels the scope if it wasn't cancelled before */
  private[graph] override def cancelInternal(ex: Throwable): Unit = if (!isCancelled) {
    var kids: Iterable[CancellationScope] = null
    var ears: java.util.HashSet[CancellationScope => Unit] = null
    this.synchronized {
      if (!isCancelled) { // Double check under lock to avoid races
        cause = ex
        ears = listeners
        listeners = null
        kids = if (children ne null) children.keys else null
        children = null
      }
    }

    if (ears ne null) {
      val it = ears.iterator()
      while (it.hasNext) {
        val cb = it.next()
        try { cb(this) }
        catch {
          case t: Throwable => CancellationScope.log.error("CancellationScope cb threw an exception", t)
        }
      }
    }
    if (kids ne null) {
      val it = kids.iterator
      while (it.hasNext) {
        it.next().cancelInternal(cause)
      }
    }
  }

  override def childScope(): CancellationScope = childScopeWithAdditionalParent(null)

  private def childScopeWithAdditionalParent(other: NestedCancellationScope): CancellationScope = {
    val cs = new NestedCancellationScope(_cancelOnException, this, other)

    // Add child to both parents. If either returns true they were already cancelled, so just cancel immediately
    if (tryToAddChild(cs))
      cs.cancelInternal(cause)
    else if ((other ne null) && other.tryToAddChild(cs))
      cs.cancelInternal(other.cause)

    cs
  }

  // if this is cancelled, returns true, else adds child and returns false
  private def tryToAddChild(child: CancellationScope): Boolean = this.synchronized {
    if (isCancelled) true
    else {
      if (children eq null) children = new mutable.WeakHashMap[CancellationScope, Object]()
      children.update(child, null)
      false
    }
  }

  private[optimus] def union(other: CancellationScope): CancellationScope = {
    // fastest optimization: every CS is already a child of NullCancellationScope
    if (other eq NullCancellationScope) this
    else {
      // slower optimization: if other is already a child of this (or vice versa) then it already meets the contract
      // of being parented by this and other
      if (other.isSameOrTransitiveChildOf(this)) other
      else if (this.isSameOrTransitiveChildOf(other)) this
      // slowest case: create new child parented to both this and other
      else childScopeWithAdditionalParent(other.asInstanceOf[NestedCancellationScope])
    }
  }

  override def addListener(listener: CancellationScope => Unit): Unit = {
    var wasAlreadyCancelled = false
    this.synchronized {
      if (isCancelled)
        wasAlreadyCancelled = true
      else {
        if (listeners eq null) listeners = new java.util.HashSet[CancellationScope => Unit]()
        listeners.add(listener)
      }
    }
    if (wasAlreadyCancelled) {
      try { listener(this) }
      catch {
        case t: Throwable => CancellationScope.log.error("CancellationScope cb threw an exception", t)
      }
    }
  }

  override def removeListener(l: CancellationScope => Unit): Unit = {
    this.synchronized {
      if (listeners ne null) {
        listeners.remove(l)
        if (listeners.size() == 0)
          listeners = null
      }
    }
  }

  override private[optimus] def isSameOrTransitiveChildOf(other: CancellationScope): Boolean =
    (this eq other) || parent1.isSameOrTransitiveChildOf(other) ||
      ((parent2 ne null) && parent2.isSameOrTransitiveChildOf(other))
}

/**
 * Default scope for new ScenarioStacks. This ignores all requests.
 */
object NullCancellationScope extends CancellationScope(Settings.cancelOnException) {
  private val moniker: AnyRef = new NullCSMoniker
  // noinspection ScalaUnusedSymbol
  private def writeReplace(): AnyRef = moniker

  private[optimus] override def cancelInternal(t: Throwable): Unit = {}
  override def addListener(l: CancellationScope => Unit): Unit = {}
  override def removeListener(l: CancellationScope => Unit): Unit = {}
  override def childScope(): CancellationScope = new NestedCancellationScope(parent1 = this, parent2 = null)
  private[optimus] override def union(other: CancellationScope): CancellationScope = other
  override private[optimus] def isSameOrTransitiveChildOf(other: CancellationScope): Boolean = this eq other
}

/* It's a class and not an object to save a few bytes during serialization */
private class NullCSMoniker extends Serializable { def readResolve(): AnyRef = NullCancellationScope }

private class NestedCSMoniker(val cause: Throwable, val id: UUID) extends Serializable {
  def readResolve(): AnyRef = CancellationScope.fromUUID(id, cause)
}
