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
package optimus.graph.cache;

import static optimus.graph.OGTrace.OfferValueSuffix;
import static optimus.graph.OGTrace.ProcessOnChildCompletedSuffix;
import static optimus.graph.OGTrace.observer;
import static optimus.graph.cache.NCSupport.isDelayedProxy;
import static optimus.graph.cache.NCSupport.isDirectlyReusableWRTCancelScopeAndEnv;
import static optimus.graph.cache.NCSupport.isNotUsableWRTCancelScopeAndEnv;
import java.util.ArrayList;
import optimus.core.TPDMask;
import optimus.graph.AlreadyCompletedPropertyNode;
import optimus.graph.CacheTopic$;
import optimus.graph.DiagnosticSettings;
import optimus.graph.GraphInInvalidState;
import optimus.graph.NodeTask;
import optimus.graph.NodeTaskInfo;
import optimus.graph.OGLocalTables;
import optimus.graph.OGSchedulerContext;
import optimus.graph.OGTrace;
import optimus.graph.PropertyNode;
import optimus.graph.ProxyPropertyNode;
import optimus.graph.RecordedTweakables;
import optimus.graph.Settings;
import optimus.graph.TweakNode;
import optimus.platform.EvaluationQueue;
import optimus.platform.RecordingScenarioStack;
import optimus.platform.ScenarioStack;

/**
 * DelayedPutIfAbsentPropertyProxyNode created when cache can't answer definitively at the time of
 * the request Note: 1. Always has identity of the requesting scenarioStack 2. Always matches
 * cancellation scope 3. Never used as a key in lookup
 *
 * <p>[PROXY_TIME_ATTRIBUTION] - Proxy nodes behave as a wrapper around a cache lookup - So self
 * time T of the proxy node is made up of the time taken to do this lookup - Since this happens in
 * onChildCompleted, T is added to postCompleteAndSuspend time of owner (cacheUnderlyingNode) and so
 * we count it as 'graph overhead time' rather than 'user blame time' for the owner node - To
 * calculate ANC time we need to add the ancPlusSelfTime of the owner node to the proxy - Normally
 * this happens when we take a dependency on from a node to another in completeFromNode ->
 * combineInfo - HOWEVER, for proxies, we don't add ancPlusSelfTime of the owner to the proxy
 * because the owner IS cacheable - So we have specific callouts to lookupEndProxy to account for
 * this
 *
 * <p>Note: see XS proxy below for notes on computing cache benefit time - this will be a different
 * calculation based on the proxy type
 */
abstract class DelayedProxyNode<T> extends ProxyPropertyNode<T> {
  protected PropertyNode<T> hit; // Possible hit

  DelayedProxyNode(PropertyNode<T> key, PropertyNode<T> hit) {
    this.hit = hit;
    srcNodeTemplate_$eq(key);
    attach(key.scenarioStack());
  }

  @Override
  public PropertyNode<T> cacheUnderlyingNode() {
    return srcNodeTemplate();
  }

  @Override
  public String name_suffix() {
    return "~$~";
  }

  @Override
  public void run(OGSchedulerContext ec) {
    if (hit == null) hit = nextPossibleHit(ec);
    if (hit.isDone()) onChildCompleted(ec, hit);
    else {
      ec.enqueue(hit);
      hit.continueWith(this, ec);
    }
  }

  protected PropertyNode<T> keyForMatching() {
    return srcNodeTemplate();
  }

  protected void processOnChildCompleted(EvaluationQueue eq, NodeTask child) {
    if (child != hit) throw new GraphInInvalidState();

    PropertyNode<T> key = keyForMatching();
    // hit was the key we executed to get the result and it's always a match, OR
    // hit was another proxy we're waiting on, so we can avoid doing matchedXS (only CS is
    // different), OR
    // hit was some node we waited for to see if we can re-use it and we'll check if can!
    boolean waitingOnProxy = isDelayedProxy(hit);
    if (key == child
        || isDirectlyReusableWRTCancelScopeAndEnv(key, hit)
            && (waitingOnProxy || matchedXS(key, hit, eq))) {
      if (waitingOnProxy) {
        // Only CS is different and we don't need to store N keys (srcNodeTemplates) for N proxies
        // It's not just an optimization: for XS srcNodeTemplate contains computed
        // RecordedTweakables
        // Instead of copying them, just share the key (srcNodeTemplate) between proxies
        srcNodeTemplate_$eq(((ProxyPropertyNode<T>) (hit)).srcNodeTemplate());
      } else if (scenarioStack().isTrackingIndividualTweakUsage()) {
        // [SEE_TRK_REPORT] XS Owner nodes are always evaluated in non-tracking scenarios,
        // this call allows TrackingScenario to 'draw' ttracks to this proxy.
        // Note: when we get the value from another proxy ttracks are already pointing to it and the
        // call to completeFromNode below will draw an edge from one proxy to another (this)
        try {
          key.scenarioStack().tweakableListener().reportTweakableUseBy(this);
        } catch (Throwable e) {
          // reportTweakableUseBy can throw (see ScenarioStack.combineTweakData)
          // we're in an onChildCompleted callback, so unless we completeWithException here,
          // scheduler ends up in logAndDie
          NCSupport.log.error("onChildCompleted threw exception!", e);
          completeWithException(e, eq);
          return;
        }
      }

      // [SEE_XS_TWEAK_INFECTION] - we set infection here based on KEY's dependencies, and NOT in
      // combineInfo
      if (DiagnosticSettings.traceTweaksEnabled
          && observer.traceTweaks()
          && hit.isXScenarioOwner()) {
        setTweakInfection(
            srcNodeTemplate()
                .scenarioStack()
                .tweakableListener()
                .recordedTweakables()
                .getTweakInfection());
      }

      OGTrace.lookupEndProxy(eq, hit, this, hit != key, true, ProcessOnChildCompletedSuffix);
      completeFromNode(hit, eq); // lookupEndProxy BEFORE completeFromNode
      hit = null; // cleanup
    } else {
      hit = nextPossibleHit(eq);
      eq.enqueue(this, hit);
      hit.continueWith(this, eq);
    }
  }

  protected PropertyNode<T> nextPossibleHit(EvaluationQueue eq) {
    return srcNodeTemplate();
  }

  boolean matchedXS(PropertyNode<T> key, PropertyNode<T> hit, EvaluationQueue eq) {
    return true;
  }
}

/** If proxy guarantees not to call into graph */
abstract class DelayedProxyNodeNoReEnqueue<T> extends DelayedProxyNode<T> {
  DelayedProxyNodeNoReEnqueue(PropertyNode<T> key, PropertyNode<T> hit) {
    super(key, hit);
  }

  @Override
  public void onChildCompleted(EvaluationQueue eq, NodeTask child) {
    if (DiagnosticSettings.proxyChainStackLimit > -1) {
      if (!eq.delayOnChildCompleted(this, child))
        try {
          processOnChildCompleted(eq, child);
        } finally {
          eq.afterOnChildCompleted();
        }
    } else processOnChildCompleted(eq, child);
  }
}

/**
 * DelayedXSProxyNode created whenever XS node is requested. This effectively causes a two step
 * resolve: 1. find proxy in the right scenario stack 2. if 1 fails .... find and match xs node
 *
 * <p>This has the following benefits: Multiple requests for the same node from the same
 * ScenarioStack will always find this proxy first and often avoid the expensive
 * NCSupport.matchXscenario Under TrackingScenario multiple edges (ttracks) leading to this proxy
 * and only single edge out (ttrack reduction)
 *
 * <p>Note: this class contains a reference to a UNodeCache to be able to make putIfAbsent requests,
 * passing it in explicitly would allow separating caches for proxies vs xs nodes
 *
 * <p>To calculate cache benefit time OVERALL (accounting for the proxy AND the underlying owner
 * node) we have to add the nodeReusedTime of the owner to the nodeReusedTime of the proxy.
 *
 * <p>Why is this correct? Remember: nodeReusedTime = nodeUsedTime - ancAndSelfTime, where
 * nodeUsedTime = total ancAndSelfTime across all invocations (assuming no caching)
 *
 * <p>Total nodeReusedTime = nodeUsedTime[prx] - ancAndSelfTime[owner] + nodeUsedTime[owner] -
 * ancAndSelfTime[prx] (rearranging) = nodeUsedTime[prx] - ancAndSelfTime[prx] + nodeUsedTime[owner]
 * - ancAndSelfTime[owner] = nodeReusedTime[prx] + nodeReusedTime[owner]
 */
@SuppressWarnings("FieldMayBeFinal") // hot path, don't want the cost of a final on constructor
// here
final class DelayedXSProxyNode<T> extends DelayedProxyNode<T> {
  private NodeCCache cache;

  @Override
  public void run(OGSchedulerContext ec) {
    if (hit == null) hit = nextPossibleHit(ec);
    if (hit.isDone()) processOnChildCompleted(ec, hit);
    else {
      ec.enqueue(hit);
      hit.continueWith(this, ec);
    }
  }

  /**
   * It's currently possible to run into a fake cycle with a mix of when clauses. See
   * XSDesignNotes.md
   */
  @Override
  public boolean tryToRecoverFromCycle(TPDMask mask, EvaluationQueue eq) {
    // hit == null if this proxy is run() -> nextPossibleHit() (likely another thread)
    if (hit != null && hit != srcNodeTemplate() && hit.isXScenarioOwner()) {
      hit = srcNodeTemplate();
      eq.enqueue(this, hit);
      hit.continueWith(this, eq);
      return true;
    }
    return false;
  }

  DelayedXSProxyNode(NodeCacheBase cache, PropertyNode<T> key) {
    super(key, null);
    this.cache = scenarioStack().cacheSelector().cacheForXSOwner(key, cache);

    boolean tweakNode = key instanceof TweakNode<?>;
    // Note that TweakNodes may use XS proxies even when getFavorReuse is not enabled (see
    // [XS_BY_NAME_TWEAKS])
    if (Settings.schedulerAsserts
        && (!(key.propertyInfo().getFavorReuseIsXS() || tweakNode) || !key.isNew()))
      throw new GraphInInvalidState();

    NodeTask trackingProxy = scenarioStack().tweakableListener().trackingProxy();
    if (trackingProxy == null) trackingProxy = this;

    // Setup XS 'owner' that will also contain collected tweaks/tweakables on completion
    key.markAsXScenarioOwner();
    key.replace(RecordingScenarioStack.withNewListener(scenarioStack(), trackingProxy));
    // TweakNode is excluded here because it manually reports via onTweakUsedBy {@link
    // TweakNode#reportTweakableUseBy}
    if (key.isTrackingValue() && !tweakNode && !key.isDoneWithCancellation())
      key.scenarioStack().tweakableListener().onTweakableNodeUsedBy(key, this);

    /*
    1. scenarioStack is non-tracking... No point to report anything
    2. scenarioStack is trk(ing)...     super#onChildCompleted(EvaluationQueue, NodeTask)  [SEE_TRK_REPORT]
    3. scenarioSTack is rec(ording) ... NodeTask#combineInfo(NodeTask, EvaluationQueue)    [SEE_REC_REPORT]
    */
    if (scenarioStack().isRecordingTweakUsage()) markAsTrackingValue();
  }

  @Override
  public void cancelInsteadOfRun(EvaluationQueue eq) {
    hit = null; // Clean up some memory
    srcNodeTemplate().replace(scenarioStack().withRecorded(RecordedTweakables.empty()));
    super.cancelInsteadOfRun(eq);
  }

  @Override
  protected void onChildCompleted(EvaluationQueue eq, NodeTask child) {
    if (child != hit)
      return; // Cycle broken (tryToRecoverFromCycle) and child is now an outdated information.
    super.onChildCompleted(eq, child);
  }

  private void assertMatchIsInCorrectState(PropertyNode<T> key, PropertyNode<T> hit) {
    // These invariants always hold by the time this method is called:
    // - hit is done
    // - hit is different from the key
    // - key isn't done
    // - hit has recorded tweakables
    if (!((hit.isDone())
        && (hit != key)
        && (!key.isDone())
        && (hit.scenarioStack().tweakableListener().recordedTweakables() != null)))
      throw new GraphInInvalidState("Invalid XS hit " + hit + " for key " + key);
  }

  @Override
  boolean matchedXS(PropertyNode<T> key, PropertyNode<T> hit, EvaluationQueue eq) {
    ScenarioStack kss = key.scenarioStack();

    if (Settings.schedulerAsserts) assertMatchIsInCorrectState(key, hit);

    // We update tweakable listener to the end snapshot of tweakables when we complete
    // matchXscenario
    // See NCSupport#matchXscenario(kss, css)
    if (kss.tweakableListener() instanceof RecordedTweakables) return true;

    return NCSupport.matchXscenario(key, kss, hit.scenarioStack());
  }

  @Override
  protected PropertyNode<T> nextPossibleHit(EvaluationQueue eq) {
    // Returns one of the following:
    // 1a Some hit node completed and fully matched. RecordingStack of srcNodeTemplate() will
    // contain dependencies
    // 1b Some hit node NOT completed but possibly will be a full match
    // 2 Original srcNodeTemplate (now 'published')

    /*
     * lookupStart/EndPartial won't increment/decrement cache lookup depth on this ctx here since the call to
     * putIfAbsentCore might call back into graph (if our tweak resolution involves evaluating a when clause, for
     * example). By keeping this lookup 'flat' here we avoid warnings about recursive cache lookup.
     * Note - if the inner evaluation on graph itself causes a recursive cache lookup, we will get a warning
     * (since that is user code)
     */
    var startTime = observer.lookupStart();
    PropertyNode<T> r = cache.putIfAbsent(NCPolicy.XSOwner, srcNodeTemplate(), eq);
    var lCtx = OGLocalTables.getOrAcquire(eq);
    observer.lookupEndPartial(lCtx, startTime, this, srcNodeTemplate(), true);
    lCtx.release();
    return r;
  }

  @Override
  public void reportTweakableUseBy(NodeTask node) {
    // Consider: Not to report in the cases were early up-reports were already done....
    // Something like if (node.scenarioStack().isRecordingTweakUsage() && hit != srcNodeTemplate())
    srcNodeTemplate().scenarioStack().tweakableListener().reportTweakableUseBy(node);
  }
}

@SuppressWarnings({
  "FieldMayBeFinal", // hot path, don't want the cost of a final on constructor here
  "unchecked" // Mostly around IPS
})
class DelayedXSFTProxyNode<T> extends DelayedProxyNodeNoReEnqueue<T> {
  enum State {
    ERROR,
    EVALUATE,
    WAIT,
    TAKE_VALUE
  }

  // [SEE_XSFT_SPECULATIVE_PROXY]
  private static final PropertyNode<?> speculativelyCompletedMarker =
      new AlreadyCompletedPropertyNode<>(0, null, NodeTaskInfo.Marker);

  // @formatter:off
  //
  //           ss1
  //      ss1.1   ss1.2
  //  ss1.1.1
  //
  // Consider:
  // ss1.1 get around first to claim the right to run the underlying node, but first it will try ss1
  // (may create it)
  // meanwhile ss1 is serving as synchronization point and ss1.2 will wait for ss1 to report the
  // value
  // ss1.1 gets the value....  will report (up) to ss1 (have to verify the value is not affected
  // from ss1.1 to ss1)
  // ss1 gets the value... will report (down) to ss1.2 (have to verify the value is not affected
  // from ss1.2 to ss1)
  //
  // @formatter:on
  private static class InProgressState<T> {
    ArrayList<DelayedXSFTProxyNode<T>> promisedToDown;
    DelayedXSFTProxyNode<T> promisedToUp;
    DelayedXSFTProxyNode<T> promisedBy; // aka node that promised some value
    boolean mustHaveValue;
    /** MT safe to set to true (checked in the deadlock case) */
    boolean doSelfEvaluate;
    /**
     * MT safe to set to the latest as the mask is always just incrementing The last mask observed
     * (allows for recovery detection and optimizations)
     */
    TPDMask dependsOnMask;
  }

  private static final InProgressState<Object> COMPLETED = new InProgressState<>();
  private static final InProgressState<Object> EMPTY = new InProgressState<>();

  /**
   * Short, within JVM stack, temporary value and most certainly not a singleton like other policies
   * Note: this is why we can hold onto EvaluationQueue
   */
  @SuppressWarnings("FieldMayBeFinal") // don't want the cost of a final on constructor here
  private static class CompletedProxyPolicy extends NCPolicy {
    private PropertyNode<?> nodeWithValue;
    private EvaluationQueue eq;
    private long computedInCacheID;

    CompletedProxyPolicy(
        PropertyNode<?> nodeWithValue, long computedInCacheID, EvaluationQueue eq) {
      super(true, false, 0);
      this.nodeWithValue = nodeWithValue;
      this.computedInCacheID = computedInCacheID;
      this.eq = eq;
    }

    @Override
    <T> DelayedProxyNode<T> createProxy(
        NodeCacheBase cache, PropertyNode<T> key, PropertyNode<T> found) {
      PropertyNode<T> typedValue = (PropertyNode<T>) nodeWithValue;
      DelayedXSFTProxyNode<T> proxy;
      if (observer.collectsAccurateCacheStats())
        proxy = new PDelayedXSFTProxyNode<>(cache, typedValue, computedInCacheID);
      else proxy = new DelayedXSFTProxyNode<>(cache, typedValue, null);

      proxy.hit = (PropertyNode<T>) speculativelyCompletedMarker;
      OGTrace.lookupEndProxy(eq, nodeWithValue, proxy, false, false, "");
      proxy.completeFromNode(typedValue, eq); // lookupEndProxy BEFORE completeFromNode
      proxy.ips = (InProgressState<T>) COMPLETED;
      return proxy;
    }
  }

  protected void setComputeInCacheID(long computedInCacheID) {}

  private NodeCacheBase cache;

  InProgressState<T> ips; // State exists only while resolving values to save space in cache

  /**
   * Either this proxy started running already (in which case someone needed it) or it was enqueued
   * (either by another proxy that needed to run it, or by a real parent node). Note that there
   * might be a race here, but it's benign: if there is a task at the end of some chain but it
   * doesn't have waiters when we call this method, then that just means that someone else ran us
   * and completed the waiter. See XSFTDesignNotes.md.
   */
  @Override
  public boolean valueWasRequested() {
    return !isNew() || hasWaiters(); // [SEE_XSFT_REQUESTED]
  }

  @Override
  protected boolean breakProxyChains(EvaluationQueue eq) {
    NodeTaskInfo info = propertyInfo();
    info.updateDependsOnMask(TPDMask.poison);

    DelayedXSFTProxyNode<T> promisedToUp = null;
    ArrayList<DelayedXSFTProxyNode<T>> promisedToDown = null;
    boolean doSelfEvaluate;
    synchronized (this) {
      if (ips == COMPLETED) return false; // done (maybe through cycle recovery)
      // no new dependency information
      if (info.dependsOnTweakMask() == ips.dependsOnMask) return false;

      doSelfEvaluate = ips.doSelfEvaluate;
      if (doSelfEvaluate) {
        promisedToUp = ips.promisedToUp;
        promisedToDown = ips.promisedToDown;
        // we're going to break chains so reset state [SEE_PARTIAL_CLEANUP]
        ips.promisedToUp = null;
        ips.promisedToDown = null;
      }
    }

    boolean didBreakChain = false;
    if (doSelfEvaluate) {
      if (promisedToUp != null) {
        promisedToUp.offerValue(null, true, false, eq);
        didBreakChain = true;
      }

      if (promisedToDown != null) {
        for (var downProxy : promisedToDown) {
          downProxy.offerValue(null, false, false, eq);
          didBreakChain = true;
        }
      }
    } else {
      offerValue(null, false, false, eq);
      didBreakChain = true;
    }
    return didBreakChain;
  }

  @Override
  public boolean tryToRecoverFromCycle(TPDMask mask, EvaluationQueue eq) {
    NodeTaskInfo info = propertyInfo();
    info.updateDependsOnMask(mask); // Try to update the mask first

    synchronized (this) {
      if (ips == COMPLETED) return false;
      if (ips.doSelfEvaluate) return false; // We are directly waiting for the original/source node
      if (info.dependsOnTweakMask() == ips.dependsOnMask) return false;
    }
    offerValue(null, false, false, eq); /* restart the dance */
    return true;
  }

  DelayedXSFTProxyNode(NodeCacheBase cache, PropertyNode<T> key, PropertyNode<T> possibleHit) {
    super(key, possibleHit);
    this.cache = cache;
  }

  /** Does this belong to ScenarioStack? */
  static ScenarioStack minimumScenario(TPDMask propertyMask, ScenarioStack ss) {
    // We can not re-use across different tweakableListeners because we don't retain any tweakable
    // dependency details and will not be able to properly re-report them...
    // Find the minimum scenario stack we can use
    while (!ss._cacheID().mayContainTweak(propertyMask) && ss.isTransparentForCaching()) {
      ss = ss.parent();
    }
    return ss;
  }

  /**
   * canUseValue doesn't need to be in a synchronized block because it just reads the mask, and
   * masks are additive
   */
  private boolean canUseValue(PropertyNode<T> value, PropertyNode<T> key, boolean toParent) {
    var ips = this.ips;
    TPDMask safeMask = ips == null ? null : ips.dependsOnMask;
    return canUseValue(value, key, toParent, safeMask);
  }

  /** toParent aka valueIsBelow */
  static <T> boolean canUseValue(
      PropertyNode<T> value, PropertyNode<T> key, boolean toParent, TPDMask safeMask) {
    OGTrace.consumeTimeInMs(OGTrace.TESTABLE_FIXED_DELAY_XSFT, CacheTopic$.MODULE$);
    if (isNotUsableWRTCancelScopeAndEnv(key, value)) return false;
    // Original calculations of minimum scenario stack
    // would not change since the mask hasn't changed
    if (safeMask == key.propertyInfo().dependsOnTweakMask()) return true;

    ScenarioStack requestingSS = key.scenarioStack();
    ScenarioStack from, to;
    if (toParent) {
      from = value.scenarioStack();
      to = requestingSS;
    } else {
      from = requestingSS;
      to = value.scenarioStack();
    }
    ScenarioStack ss = from;
    while (ss._cacheID() != to._cacheID() && !ss.isRoot()) {
      if (value.tweakPropertyDependenciesIntersect(ss._cacheID().tweakMask())) return false;
      ss = ss.parent();
    }
    return true;
  }

  /** Called by a proxy with a higher depth that needs a value (hence willingToEval) */
  private State promiseOrAwait(DelayedXSFTProxyNode<T> willingToEval) {
    State state;
    synchronized (this) {
      // This is just an optimization: "|| srcNodeTemplate().isDone()"
      // Right after the underlying node will be completing the owner proxy will be complete
      if (ips == COMPLETED || srcNodeTemplate().isDone()) state = State.TAKE_VALUE;
      else {
        if (ips == null) ips = new InProgressState<>();
        if (ips.promisedBy == null) {
          state = State.EVALUATE;
          setWaitingOn(willingToEval);
          ips.promisedBy = willingToEval;
        } else {
          state = State.WAIT; // Someone already promised value to this proxy
          if (ips.promisedToDown == null) ips.promisedToDown = new ArrayList<>();
          ips.promisedToDown.add(willingToEval);
        }
      }
    }
    return state;
  }

  /**
   * Called on the proxy that is being offered value
   *
   * <p>offerValue() is racy:
   *
   * <ul>
   *   <li>from cancelInsteadOfRun() only occurs when no previous calls to runByWaitingOrEvaluating
   *       (i.e. it's never racing with ips.xxx = setters)
   *   <li>from runByWaitingOrEvaluating() without mutating any ips.xxx
   *   <li>form breakProxyChains() only on a full stall
   *   <li>from tryToRecoverFromCycle() the chain in question is not progressing
   *   <li>from notifyAll() of another proxy or from onChildCompleted() also one a time
   * </ul>
   *
   * <p>Notes:
   *
   * <ul>
   *   <li>valueOwner == true only from onChildCompleted and child == srcNode and ips.doSelfEvaluate
   *       == true
   *   <li>if valueOwner == true we always accept the result even it's cancelled
   *   <li>All inter-proxy interactions and completions are not managed the usual way such as
   *       completeFromNode. That's all for the "regular" users of the proxy Internally everything
   *       is managed in state transitions inside of synchronized (this) {}
   * </ul>
   */
  private void offerValue(
      PropertyNode<T> nodeWithValue, boolean toParent, boolean valueOwner, EvaluationQueue eq) {
    // offerValue is like NodeTask.notifyWaiters and needs to reset waitingOn....
    setWaitingOn(null);
    var canTake =
        valueOwner || (nodeWithValue != null && canUseValue(nodeWithValue, this, toParent));

    InProgressState<T> prev_ips; // Retain some values for after synchronized(this)
    synchronized (this) {
      // in the case where we had a cycle or cancellation, this proxy is already done
      if (ips == COMPLETED) return;

      this.hit = null;
      prev_ips = ips;
      // ips can become null on re-tries
      if (prev_ips == null) prev_ips = (InProgressState<T>) EMPTY;

      if (canTake) {
        ips = (InProgressState<T>) COMPLETED;
        if (!valueOwner) {
          setComputeInCacheID(nodeWithValue.scenarioStack()._cacheID().id());
          srcNodeTemplate_$eq(nodeWithValue); // Sets the result
          if (toParent) nodeWithValue.replace(scenarioStack()); // Reduce memory
        } else if (!prev_ips.mustHaveValue) {
          hit = (PropertyNode<T>) speculativelyCompletedMarker;
        }
      } else if (isCancelled()) {
        ips = (InProgressState<T>) COMPLETED; // Complete by aborting
      } else {
        ips = null; // Will have to retry ... Clean up intermediate state [SEE_PARTIAL_CLEANUP]
      }
    }

    if (canTake) {
      var cacheHit = !valueOwner && prev_ips.mustHaveValue;
      OGTrace.lookupEndProxy(eq, nodeWithValue, this, cacheHit, valueOwner, OfferValueSuffix);
      completeFromNode(nodeWithValue, eq); // lookupEndProxy BEFORE completeFromNode
    } else if (ips == COMPLETED) {
      super.cancelInsteadOfRun(eq);
    } else {
      // Couldn't take the value, entire state is reset (e.g. this.ips = null)
      // Maybe value is useful for other proxies
      if (nodeWithValue != null && toParent) speculativeStore(nodeWithValue, eq);
      // But still need the value! Re-start the dance...
      if (prev_ips.mustHaveValue) runByWaitingOrEvaluating(eq);
    }

    PropertyNode<T> nodeToOffer = canTake ? nodeWithValue : null;
    notifyAll(prev_ips, nodeToOffer, eq);
  }

  private void speculativeStore(PropertyNode<T> nodeWithValue, EvaluationQueue eq) {
    // Assumptions changed, and we can't use the value, but after retries some proxies CAN find and
    // use this value... We aggressively insert this value hoping it will be found after retries...
    var computedInSS = nodeWithValue.scenarioStack();
    var minScenario = minimumScenario(propertyInfo().dependsOnTweakMask(), computedInSS);
    if (minScenario._cacheID() != computedInSS._cacheID()) { // [SEE_SS_COMPARE_FOR_CACHE]
      // Note: don't adjust blockID this is completely speculative insertion
      var computedInCacheID = computedInSS._cacheID().id();
      nodeWithValue.replace(minScenario);
      findOrInsertUpProxy(
          nodeWithValue, new CompletedProxyPolicy(nodeWithValue, computedInCacheID, eq), eq);
    }
  }

  private void notifyAll(InProgressState<T> ps, PropertyNode<T> nodeToOffer, EvaluationQueue eq) {
    if (ps.promisedToUp != null) ps.promisedToUp.offerValue(nodeToOffer, true, false, eq);

    if (ps.promisedToDown != null)
      for (DelayedXSFTProxyNode<T> downProxy : ps.promisedToDown)
        downProxy.offerValue(nodeToOffer, false, false, eq);
  }

  private PropertyNode<T> findOrInsertUpProxy(
      PropertyNode<T> key, NCPolicy policy, EvaluationQueue ec) {
    var startTime = observer.lookupStart();
    PropertyNode<T> r = cache.putIfAbsent(policy, key, ec);

    // [SEE_RACEY_SPECULATIVE_INSERTION]
    OGLocalTables lCtx = OGLocalTables.getOrAcquire(ec);
    observer.lookupEndPartial(lCtx, startTime, key, r, false); // don't count this as a real hit
    lCtx.release();
    return r;
  }

  private void runByWaitingOrEvaluating(EvaluationQueue ec) {
    InProgressState<T> cips;
    synchronized (this) {
      if (ips == COMPLETED) {
        if (hit == speculativelyCompletedMarker)
          observer.lookupAdjustCacheHit(ec, srcNodeTemplate());
        return; // We are done, or being done
      }
      if (ips == null) ips = new InProgressState<>();
      ips.mustHaveValue = true;
      if (ips.promisedBy == null) {
        ips.promisedBy = this;
        cips = ips;
      } else {
        return; // Was already promised a value
      }
    }

    // NOTES on hit:
    // 1. null - no suggestions...
    // 2. proxy found that is only different in CS
    // 3. upProxy found by alternative lookup (note the cacheID won't match ours, see the code in
    // alternativeLookup)
    if (hit != null && hit.scenarioStack()._cacheID() == scenarioStack()._cacheID()) {
      cips.doSelfEvaluate = true;
      PropertyNode<T> proxyWithDiffCS = hit; // [SEE_PROXY_CHAINING]
      hit = null;
      ec.enqueue(this, proxyWithDiffCS);
      proxyWithDiffCS.continueWith(this, ec);
    } else {
      boolean selfEvaluate = false;
      cips.dependsOnMask = propertyInfo().dependsOnTweakMask();
      ScenarioStack minSS = minimumScenario(cips.dependsOnMask, scenarioStack());
      // MUST compare cacheID, not the SS itself, because cacheIDs are compared in caches
      // e.g. multiple SI ss with different plugin tags [SEE_SS_COMPARE_FOR_CACHE]
      if (minSS._cacheID() == scenarioStack()._cacheID()) selfEvaluate = true;
      else {
        DelayedXSFTProxyNode<T> upProxy = null;
        // Alternative lookup inserted a proxy and passed it in 'hit' to avoid second lookup
        if (hit != null) {
          if (hit.scenarioStack()._cacheID() == minSS._cacheID()) // [SEE_SS_COMPARE_FOR_CACHE]
            // Check assumptions didn't change [SEE_HIT_IS_PROXY]
            upProxy = (DelayedXSFTProxyNode<T>) hit;
          hit = null; // cleanup early and effectively 'mark' suggested hit as unusable
        }
        if (upProxy == null) {
          // Consider: minSS.cancelScope != this.scenarioStack.cancelScope and the value can't be
          // reused because either minSS.cs is canceled OR cs.hiddenOn(value.exception) == true.
          // So findOrInsertUpProxy should return a new proxy/new scenario stack/correct CS
          var minSSWithCorrectCS = minSS.withCancellationScopeAndBlockID(scenarioStack());
          var newKey = srcNodeTemplate().prepareForExecutionIn(minSSWithCorrectCS);
          upProxy = (DelayedXSFTProxyNode<T>) findOrInsertUpProxy(newKey, NCPolicy.XSFTInner, ec);
        }

        State state = upProxy.promiseOrAwait(this);
        if (state == State.TAKE_VALUE) {
          var nodeToOffer = upProxy.srcNodeTemplate();
          offerValue(nodeToOffer.isDone() ? nodeToOffer : null, false, false, ec);
          // return ... no code in function should evaluate afterwards
        } else if (state == State.EVALUATE) {
          cips.promisedToUp = upProxy;
          selfEvaluate = true;
        } else setWaitingOn(upProxy); // state == State.WAIT
      }

      if (selfEvaluate) {
        cips.doSelfEvaluate = true;
        PropertyNode<T> useValueOf = srcNodeTemplate();
        ec.enqueue(this, useValueOf); // [SEE_XSFT_GLOBAL_SCHEDULER_ENQUEUE]
        useValueOf.continueWith(this, ec);
        // return ... no code in function should evaluate afterwards
      }
    }
  }

  @Override
  protected void processOnChildCompleted(EvaluationQueue eq, NodeTask child) {
    if (child == srcNodeTemplate()) offerValue(srcNodeTemplate(), true, true, eq); // value owner
    else
      // noinspection unchecked (the only 'child' possible is the proxy with a different CS)
      offerValue(((DelayedProxyNode<T>) child).srcNodeTemplate(), false, false, eq);
  }

  @Override
  public void run(OGSchedulerContext ec) {
    runByWaitingOrEvaluating(ec);
  }

  @Override
  public void cancelInsteadOfRun(EvaluationQueue ec) {
    // Will cancel is race to complete will be lost
    offerValue(null, false, false, ec);
  }
}

final class PDelayedXSFTProxyNode<T> extends DelayedXSFTProxyNode<T> {
  private long computedInCacheID;

  PDelayedXSFTProxyNode(NodeCacheBase cache, PropertyNode<T> key, PropertyNode<T> possibleHit) {
    super(cache, key, possibleHit);
  }

  PDelayedXSFTProxyNode(NodeCacheBase cache, PropertyNode<T> key, long computedInCacheID) {
    super(cache, key, null);
    this.computedInCacheID = computedInCacheID;
  }

  @Override
  protected void setComputeInCacheID(long computedInCacheID) {
    this.computedInCacheID = computedInCacheID;
  }

  @Override
  public long computedInCacheID() {
    return computedInCacheID != 0 ? computedInCacheID : super.computedInCacheID();
  }
}
