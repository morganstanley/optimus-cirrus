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
package optimus.platform.pickling

import optimus.graph._
import optimus.graph.cache.DelayedCSProxyNode
import optimus.platform.EvaluationContext
import optimus.platform.EvaluationQueue
import optimus.platform.ScenarioStack
import optimus.platform.dal.LinkResolutionException
import optimus.platform.dal.SourcedLinkResolutionException
import optimus.platform.storable.Entity

import java.util
import scala.util.control.NonFatal

/**
 * Represents a field of any entity which is lazily loaded/unpickled (because it points to some other entity, either
 * directly or transitively via collections, embeddables etc.).
 *
 * Initially we are created with the pickled representation of the value and the associated unpickler. When we are
 * resolved via withScenarioStack, we will return an UnpickleWrapper to actually do the unpickling (and associated DAL
 * loading). The UnpickleWrapper runs in the SI root of the SS that the LPR was created with but using the SI params
 * from the requester's SS (so that tracking information, cancellation scope etc. is correct).
 *
 * Subsequent lookups from the same CancellationScope prior to completion of unpickling will return the same
 * UnpickleWrapper, and lookups from other CancellationScopes will return UnpickleCSProxies instead (so that there is
 * only one actual UnpickleWrapper in flight at any time).
 *
 * Once the UnpickleWrapper completes, if it has a "usable" result (i.e. a value or an RT exception), any pending
 * UnpickleCSProxies will complete and the LazyPickledReference will also be completed and will thereafter behave as a "stable"
 * node which always returns itself in response to withScenarioStack. This is an important optimization because it means
 * that usage of a fully resolved LPR is essentially free - it behaves the like a normal field. Also the pickled
 * representation and the unpickler will be released to save memory.
 *
 * If the UnpickleWrapper completes with a non-RT exception, any UnpickleCSProxies in different cancellation scopes will create a
 * single fresh UnpickleWrapper to try again. The LazyPickledReference itself won't be completed, so any subsequent calls
 * to withScenarioStack will create further proxies.
 *
 * Note that the LazyPickledReference itself is never enqueued, never runs, is never waited on, and never explicitly
 * waits on any node (although the UnpickleWrapper does call it back on completion)
 */
final class LazyPickledReference[A <: AnyRef](
    // pickled and unpickler are null after a usable result is resolved
    private[optimus] var pickled: Any,
    private[this] var unpickler: Unpickler[A],
    override val entity: Entity,
    override val propertyInfo: NodeTaskInfo)
    extends PropertyNode[A]
    with MaybePickledReference[A] {
  outer =>
  // (null except while an attempt to unpickle is running)
  @transient private[this] var unpickleWrapper: UnpickleWrapper = _
  // (null before there were any attempts to resolve, and null after a usable result is resolved)
  @transient private[this] var unpickleByCs: util.Map[CancellationScope, MaybePickledReference[A]] =
    _

  // Make sure to store the environment we were created under. This is important since we could in theory be constructed
  // under one environment and then run under another, and we must load any child entities under the DAL environment we
  // that our parent entity was loaded with (i.e. the ss at the time of our construction)
  attach(EvaluationContext.scenarioStack.siRoot)

  /**
   * Returns a node which will resolve the underlying unpickled value in the caller's CancellationScope.
   * That node could be this, or an UnpickleWrapper, or an UnpickleCSProxy (see class comment)
   */
  override def prepareForExecutionIn(ss: ScenarioStack): MaybePickledReference[A] = {
    // huge optimization: we only complete the LPR if the result is RT, and in that case the requester's
    // CancellationScope doesn't matter so just return this directly
    if (isDone) this
    else
      synchronized {
        // recheck under lock (we mustn't even attempt to create an UnpickleWrapper if we are done because our
        // pickled state will have been cleared)
        if (isDone) this
        else {
          // take the SI params (including CancelScope, ChainedID, profiler block ID, NodeInputs including GridProfiler)
          // from the requesting scenario stack so that we handle errors and attribution correctly, but use the
          // RuntimeEnvironment from the SI stack that the LPR was created with (see the attach above)
          val mergedSS = scenarioStack().withSIParamsFrom(ss)

          // Note that entries are retained until the LPR is completed with an RT result so that we don't keep
          // retrying to resolve in the same CancellationScope (i.e. we follow normal CS semantics)
          if (unpickleByCs eq null) unpickleByCs = new util.HashMap()
          unpickleByCs.computeIfAbsent(
            ss.cancelScope,
            { _ =>
              val unpickleWrapper = getOrCreateUnpickleWrapper(mergedSS)
              // optimization: if unpickleWrapper is already in cs (which happens when we created
              // it rather than getting an existing one), we can use it directly without a proxy
              if (unpickleWrapper.scenarioStack.cancelScope eq ss.cancelScope) unpickleWrapper
              else new UnpickleCSProxy(mergedSS, unpickleWrapper)
            }
          )
        }
      }
  }

  private def getOrCreateUnpickleWrapper(ss: ScenarioStack): MaybePickledReference[A] = synchronized {
    // we only ever complete the LPR with an RT result (usable from any CS), so there's no need to create a new proxy
    if (isDone) this
    else {
      if ((unpickleWrapper eq null) || unpickleWrapper.isDoneWithExceptionToHide)
        unpickleWrapper = new UnpickleWrapper(ss)
      unpickleWrapper
    }
  }

  private def releaseState(): Unit = synchronized {
    pickled = null
    unpickler = null
    unpickleByCs = null
    unpickleWrapper = null
  }

  /**
   * Calls the unpickler and enriches the result. Notifies the LazyPickledReference (as well as normal waiters, i.e.
   * UnpickleCSProxy nodes or user nodes) on completion.
   */
  private class UnpickleWrapper(ss: ScenarioStack) extends ProxyPropertyNode[A] with MaybePickledReference[A] {
    srcNodeTemplate = outer
    // unlike normal proxies we don't delegate to the underlying node (i.e. the outer LPR) we need to track tweakable
    // usage ourself
    doInitTrackingValue()
    attach(ss)

    override def run(ec: OGSchedulerContext): Unit = {
      assert(pickled != null)
      assert(!outer.entity.dal$isTemporary)
      val p = new PickledMapWrapper(
        // unpickler only uses this for grabbing inline SerializedEntities - it doesn't need the parent entity's props
        properties = null,
        temporalContext = outer.entity.dal$loadContext,
        inlineEntitiesByRef = outer.entity.dal$inlineEntities
      )

      val child =
        try unpickler.unpickle$queued(pickled, p)
        catch {
          // unpickle$queued may sometimes fail directly because of synchronous unpickle implementations, and want to
          // treat that the same as when a node returned by unpickle$queued fails so that we get our special handling
          case NonFatal(ex) => new AlreadyFailedNode(ex)
        }
      child.continueWith(this, ec)
    }

    override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
      combineInfo(child, eq)
      if (child.isDoneWithException) {
        val enrichedException = child.exception match {
          case e: LinkResolutionException =>
            new SourcedLinkResolutionException(e.ref, e.temporalContext, outer.entity, outer.propertyInfo)
          case e => e
        }
        completeWithException(enrichedException, eq)
      } else {
        completeWithResult(child.asInstanceOf[Node[A]].result, eq)
      }

      // if we have a "usable" result (i.e. a value or an RT exception) we can complete the outer LPR since this result
      // would be valid for any requester to use
      if (isDoneWithUsableResult) {
        outer.completeFromNode(this, eq)
        outer.releaseState()
      }
    }

    override private[optimus] def pickled: Any = outer.pickled

    // override to *not* return proxyInfo because we're not really a proxy - we're the real executing LPR.
    // This also allows us to distinguish ourselves from UnpickleCSProxy (which does return proxyInfo)
    override def executionInfo: NodeTaskInfo = NodeTaskInfo.LazyPickledReference
  }

  /**
   * A proxy to wait on a UnpickleWrapper from a different CancellationScope
   */
  private class UnpickleCSProxy(ss: ScenarioStack, candidate: PropertyNode[A])
      extends DelayedCSProxyNode[A](outer, candidate)
      with MaybePickledReference[A] {
    srcNodeTemplate = outer
    replace(ss)

    override def nextPossibleHit(eq: EvaluationQueue): PropertyNode[A] =
      outer.getOrCreateUnpickleWrapper(ss)

    // note that srcNodeTemplate is just the outer - it doesn't have the right CS for matching (but this does)
    override protected def keyForMatching: PropertyNode[A] = this

    override private[optimus] def pickled: Any = outer.pickled
  }

  // it cannot be cloned because a lazy reference is evaluated once when first requested
  override def isClonable: Boolean = false
  override def reset(): Unit = throw new GraphInInvalidState("LazyPickledReference doesn't support reset")

  // The default implementation of tidyKey looks this node up in the ScenarioStack.constant, but that will cause
  // us to create and cache a child node, wasting memory. Since we're already in the siRoot stack (so not holding on
  // to some other scenario stack) it's good enough to just return this.
  override def tidyKey: PropertyNode[A] = this

  // override our executionInfo so that it is marked as cacheable, since although our propertyInfo is not cacheable the
  // scheduler needs to treat us as cacheable because we can be waited on by multiple waiters and therefore we are
  // a risk for sync-stack dependency inversion deadlocks
  override def executionInfo: NodeTaskInfo = NodeTaskInfo.LazyPickledReference

  private[pickling] def pendingNodeCount_FOR_TEST_ONLY =
    Option(unpickleByCs).map(_.size).getOrElse(0)
}

/**
 * LazyPickledReference#withScenarioStack returns various classes, but they all implement this trait. This
 * is useful for re-pickling the reference without resolving it first and other special cases
 */
private[optimus] sealed trait MaybePickledReference[A <: AnyRef] extends PropertyNode[A] {

  /** pickled representation if we're unresolved (or null if we've already been resolved) */
  private[optimus] def pickled: Any
}
