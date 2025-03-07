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

import static optimus.graph.cache.DelayedXSFTProxyNode.canUseValue;
import static optimus.graph.cache.NCSupport.isDirectlyReusableWRTCancelScope;
import static optimus.graph.cache.NCSupport.matchXscenario;
import java.io.Serial;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import optimus.core.TPDMask;
import optimus.entity.EntityInfo;
import optimus.graph.GraphInInvalidState;
import optimus.graph.NodeTask;
import optimus.graph.NodeTaskInfo;
import optimus.graph.OGScheduler;
import optimus.graph.OGTrace;
import optimus.graph.PropertyInfo;
import optimus.graph.PropertyNode;
import optimus.graph.Settings;
import optimus.graph.TweakableListener$;
import optimus.platform.EvaluationQueue;
import optimus.platform.ScenarioStack;
import optimus.platform.storable.Entity;
import optimus.platform.storable.EntityImpl;
import scala.collection.Iterator;

@SuppressWarnings("StaticInitializerReferencesSubClass")
public abstract class NCPolicy implements Serializable {
  public static final String DEFAULT_POLICY_NAME = "Default";
  public static final String SCENARIO_INDEPENDENT_POLICY_NAME = "Scenario Independent";

  public static final NCPolicy Basic = new BasicPolicy();
  public static final NCPolicy SI = new SIPolicy();
  private static final NCPolicy RuntimeEnv = new RuntimeEnvPolicy();

  public static final NCPolicy XS = new XSPolicy();
  static final NCPolicy XSOwner = new XSOwnerPolicy();

  static final NCPolicy XSFTInner = new XSFTInnerPolicy();
  public static final NCPolicy XSFT = new XSFTPolicy();

  public static final NCPolicy DontCache = new DontCachePolicy();

  private static final HashMap<String, Integer> scopePathToIDs = new HashMap<>();

  final boolean alwaysNeedsProxy;
  final boolean acceptAnyUsableCS;
  /** True for DontCache policy only */
  public final boolean noCache;

  public static void resetCaches() {
    synchronized (scopePathToIDs) {
      scopePathToIDs.clear();
    }
    NCPolicyComposite.policyCache.clear();
  }

  /** Returns user filterable name for anything but default policies */
  public String policyName() {
    return null;
  }

  /* Returns name to use in optconf file*/
  public String optconfName() {
    return toString();
  }

  /**
   * Under some conditions (e.g. tracking scenario), might need to 'downgrade' policy (e.g. from
   * XSFT to Default)
   */
  public NCPolicy switchPolicy(PropertyNode<?> key) {
    return this;
  }

  /** Check if the policy is the same as the given policy or it's composite and contains it */
  public boolean contains(NCPolicy policy) {
    return this == policy;
  }

  static class LookupResult<T> {
    static LookupResult<Object> empty = new LookupResult<>();
    public boolean done;
    public PropertyNode<T> result;
  }

  public static NCPolicy forInfo(NodeTaskInfo nti, NCPolicy requestedPolicy) {
    if (nti.isScenarioIndependent()) return NCPolicy.SI;
    if (nti.isGivenRuntimeEnv()) return NCPolicy.RuntimeEnv;
    // this means we avoid setting any other policy on non-cacheable nodes (e.g. @node vals)
    // without having to check isCacheable before calling setCachePolicy (see uses of forInfo)
    if (!nti.getCacheable()) return NCPolicy.DontCache;
    if (nti.isInternal() || (nti.snapFlags() & NodeTaskInfo.NOTNODE) != 0 || nti.isRecursive())
      return NCPolicy.Basic;
    if (nti.isGroupCached()) return NCPolicy.Basic;

    return requestedPolicy;
  }

  /** Helper method in switch on/off policies on entire entities */
  public static void setAllCachePoliciesTo(EntityInfo info, NCPolicy policy) {
    Iterator<PropertyInfo<?>> it = info.properties().iterator();
    while (it.hasNext()) {
      PropertyInfo<?> prop = it.next();
      if (prop.getCacheable()) prop.setCachePolicy(policy);
    }
  }

  /**
   * for recreating serialized NCPolicies (note that not all policies are serializable by default)
   */
  public static NCPolicy forName(String name) {
    return switch (name) {
      case "BasicPolicy" -> Basic;
      case "SIPolicy" -> SI;
      case "RuntimeEnvPolicy" -> RuntimeEnv;
      case "XSPolicy" -> XS;
      case "XSFTPolicy" -> XSFT;
      case "DontCachePolicy" -> DontCache;
      default -> throw new GraphInInvalidState(
          "Shouldn't be attempting to get any other policy from name");
    };
  }

  public NCPolicy combineWith(String currentTargetPath, NCPolicy policy, String targetPath) {
    // currentTargetPath empty means `this` is the default
    if (currentTargetPath.isEmpty()) return composite(this, policy, targetPath);
    else return composite(null, this, currentTargetPath).combineWith("IGNORED", policy, targetPath);
  }

  public static int scopeMaskFromPath(String scopedPath) {
    if (scopedPath == null || scopedPath.isEmpty()) return 0;
    return 1 << scopeIDFromPath(scopedPath);
  }

  public static int profileBlockIDFromPath(String scopedPath) {
    return scopeIDFromPath(scopedPath) + OGTrace.BLOCK_ID_UNSCOPED;
  }

  public static int scopeIDFromPath(String scopedPath) {
    if (scopedPath == null || scopedPath.isEmpty()) return 0;
    int scopeID;
    synchronized (scopePathToIDs) {
      scopeID = scopePathToIDs.computeIfAbsent(scopedPath, path -> scopePathToIDs.size());
    }
    // TODO(OPTIMUS-72345): path based mask could overflow
    if (scopeID > 30) OGScheduler.log.error("Too many scoped paths: " + scopeID);
    return scopeID;
  }

  public static Map<String, Integer> registeredScopedPathAndProfileBlockID() {
    var r = new HashMap<String, Integer>();
    synchronized (scopePathToIDs) {
      for (var kv : scopePathToIDs.entrySet()) {
        r.put(kv.getKey(), kv.getValue() + OGTrace.BLOCK_ID_UNSCOPED);
      }
    }
    return r;
  }

  /** Returns new policy with path-independent policy set if one wasn't provide before */
  public NCPolicy withPathIndependentIfNotSet(NCPolicy scopeIndependent) {
    return this; // Since default is pathIndependent, it's always already set
  }

  /** Every policy other than composite is always pathIndependent */
  public NCPolicy pathIndependent() {
    return this;
  }

  public static NCPolicy composite(NCPolicy defaultPolicy, NCPolicy policy, String targetPath) {
    return new NCPolicyComposite.CompositeN(defaultPolicy, policy, scopeMaskFromPath(targetPath));
  }

  /** Really for testing only */
  public static NCPolicy composite(
      NCPolicy defaultPolicy, NCPolicy policy1, int mask1, NCPolicy policy2, int mask2) {
    return new NCPolicyComposite.CompositeN(
        defaultPolicy,
        new NCPolicyComposite.PolicyMask[] {
          new NCPolicyComposite.PolicyMask(policy1, mask1),
          new NCPolicyComposite.PolicyMask(policy2, mask2)
        });
  }

  NCPolicy() {
    noCache = false;
    alwaysNeedsProxy = false;
    acceptAnyUsableCS = false;
  }

  NCPolicy(boolean alwaysNeedsProxy, boolean acceptAnyUsableCS, long associatedFlags) {
    this.noCache = (associatedFlags & NodeTaskInfo.DONT_CACHE) != 0;
    this.alwaysNeedsProxy = alwaysNeedsProxy;
    this.acceptAnyUsableCS = acceptAnyUsableCS;
  }

  @Override
  // do not override because serialisation depends on this implementation in forName()
  public final String toString() {
    return getClass().getSimpleName();
  }

  @Serial
  Object writeReplace() {
    throw new GraphInInvalidState("NCPolicy in general should not be serialized");
  }

  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  <T> boolean matchesScenario(PropertyNode<T> cValue, PropertyNode<T> key) {
    return key.scenarioStack()._cacheID() == cValue.scenarioStack()._cacheID();
  }

  <T> int hashOf(PropertyNode<T> key) {
    return NCSupport.chash(key.hashCodeForCaching(), key.scenarioStack());
  }

  final <T> DelayedProxyNode<T> updateProxy(
      NodeCacheBase cache,
      PropertyNode<T> key,
      PropertyNode<T> found,
      DelayedProxyNode<T> prevProxy) {
    if (prevProxy == null) return createProxy(cache, key, found);
    prevProxy.hit = found; // 'found' can be null here, but that's ok [SEE_PROXY_CHAINING]
    return prevProxy;
  }

  <T> DelayedProxyNode<T> createProxy(
      NodeCacheBase cache, PropertyNode<T> key, PropertyNode<T> found) {
    return new DelayedCSProxyNode<>(key, found);
  }

  <T> boolean tryLocalCache(PropertyNode<T> key) {
    return key.propertyInfo().tryLocalCache();
  }

  <T> PropertyNode<T> localMatch(PropertyNode<T> key) {
    Entity e = key.entity();
    Object localCache = EntityImpl.getLocalCache(e);
    if (localCache != null) {
      @SuppressWarnings("unchecked")
      PropertyNode<T> last = (PropertyNode<T>) localCache;
      if (last.entity() == e
          && last.args() == NodeTask.argsEmpty
          && // local cache is limited to no args for now
          (last.scenarioStack().cancelScope() == key.scenarioStack().cancelScope())
          && // CS still has to match
          last.scenarioStack()._cacheID() == key.scenarioStack()._cacheID()) {
        return last;
      }
    }
    return null;
  }

  public boolean appliesToScenarioIndependent() {
    return false;
  }

  public boolean appliesToRecursive() {
    return false;
  }

  <T> LookupResult<T> alternativeLookup(
      NodeCacheBase cache, PropertyNode<T> key, EvaluationQueue eq) {
    //noinspection unchecked
    return (LookupResult<T>) LookupResult.empty;
  }

  private static class NCPolicyMoniker implements Serializable {
    String name;

    NCPolicyMoniker(String name) {
      this.name = name;
    }

    @Serial
    Object readResolve() {
      return NCPolicy.forName(name);
    }
  }

  private static class BasicPolicy extends NCPolicy {
    private final Object moniker = new NCPolicyMoniker(toString());

    @Serial
    @Override
    Object writeReplace() {
      return moniker;
    }

    @Override
    public String optconfName() {
      return "default";
    }

    @Override
    public boolean appliesToRecursive() {
      return true;
    }
  }

  private static class SIPolicy extends NCPolicy {
    private final Object moniker = new NCPolicyMoniker(toString());

    @Serial
    @Override
    Object writeReplace() {
      return moniker;
    }

    SIPolicy() {
      super(false, false, NodeTaskInfo.SCENARIOINDEPENDENT);
    }

    @Override
    public boolean appliesToScenarioIndependent() {
      return true;
    }
  }

  private static class RuntimeEnvPolicy extends NCPolicy {
    private final Object moniker = new NCPolicyMoniker(toString());

    @Serial
    @Override
    Object writeReplace() {
      return moniker;
    }

    RuntimeEnvPolicy() {
      super(false, false, NodeTaskInfo.GIVEN_RUNTIME_ENV);
    }
  }

  private static class DontCachePolicy extends NCPolicy {
    private final Object moniker = new NCPolicyMoniker(toString());

    @Serial
    @Override
    Object writeReplace() {
      return moniker;
    }

    DontCachePolicy() {
      super(false, false, NodeTaskInfo.DONT_CACHE);
    }

    @Override
    public boolean appliesToScenarioIndependent() {
      return true; // allow disabling cache on SI nodes
    }

    @Override
    public boolean appliesToRecursive() {
      return true; // allow disabling cache on @recursive nodes
    }

    /** match existing name in optconf */
    @Override
    public String optconfName() {
      return "dontCache";
    }
  }

  private static class XSFTPolicy extends NCPolicy {
    private final Object moniker = new NCPolicyMoniker(toString());

    @Serial
    @Override
    Object writeReplace() {
      return moniker;
    }

    XSFTPolicy() {
      super(true, false, 0);
    }

    @Override
    public String policyName() {
      return "XSFT";
    }

    @Override
    <T> DelayedProxyNode<T> createProxy(
        NodeCacheBase cache, PropertyNode<T> key, PropertyNode<T> found) {
      if (OGTrace.observer.collectsAccurateCacheStats())
        return new PDelayedXSFTProxyNode<>(cache, key, found);
      else return new DelayedXSFTProxyNode<>(cache, key, found);
    }

    // match existing name in optconf
    @Override
    public String optconfName() {
      return policyName();
    }

    @Override
    public NCPolicy switchPolicy(PropertyNode<?> key) {
      return key.scenarioStack().isTrackingIndividualTweakUsage() ? NCPolicy.Basic : this;
    }

    /*
         ss0   <- 1. inserted proxy 2. found proxy it's not done 3. found proxy it's done and we can use 4. done and can't use
      ssN      <- requesting
    */
    @Override
    <T> LookupResult<T> alternativeLookup(
        NodeCacheBase cache, PropertyNode<T> key, EvaluationQueue eq) {
      ScenarioStack ss = key.scenarioStack();
      TPDMask dependsOnTweakMask = key.propertyInfo().dependsOnTweakMask();
      // [SEE_XSFT_SPECULATIVE_PROXY]
      ScenarioStack minSS = DelayedXSFTProxyNode.minimumScenario(dependsOnTweakMask, ss);
      if (minSS._cacheID() == ss._cacheID()) // [SEE_SS_COMPARE_FOR_CACHE]
        //noinspection unchecked
        return (LookupResult<T>) LookupResult.empty;
      else {
        PropertyNode<T> newKey = key.prepareForExecutionIn(minSS);
        // [SEE_COUNT_TIME_FROM_ALTERNATIVE_LOOKUP]
        // we're already in a lookup so don't add OGTrace callouts here
        PropertyNode<T> upProxy = cache.putIfAbsent(NCPolicy.XSFTInner, newKey, eq);
        LookupResult<T> r = new LookupResult<>();
        if (upProxy instanceof DelayedXSFTProxyNode<T> xsftProxy) {
          r.result = upProxy;
          var srcNode = xsftProxy.srcNodeTemplate(); // [SEE_CAST_ALT_LOOKUP]
          if (srcNode.isDone() && canUseValue(srcNode, key, false, dependsOnTweakMask))
            r.done = true;
        }
        return r;
      }
    }

    <T> boolean matchesScenario(PropertyNode<T> cValue, PropertyNode<T> key) {
      return super.matchesScenario(cValue, key) && cValue instanceof DelayedXSFTProxyNode<?>;
    }
  }

  /** avoids recursive alternativeLookup calls (when already in an xsft alternativeLookup call) * */
  private static class XSFTInnerPolicy extends XSFTPolicy {
    @Override
    <T> LookupResult<T> alternativeLookup(
        NodeCacheBase cache, PropertyNode<T> key, EvaluationQueue eq) {
      //noinspection unchecked
      return (LookupResult<T>) LookupResult.empty;
    }

    @Override
    <T> DelayedProxyNode<T> createProxy(
        NodeCacheBase cache, PropertyNode<T> key, PropertyNode<T> found) {
      if (OGTrace.observer.collectsAccurateCacheStats())
        return new PDelayedXSFTProxyNode<>(cache, key, null);
      else return new DelayedXSFTProxyNode<>(cache, key, null);
    }
  }

  private static class XSPolicy extends NCPolicy {
    private final Object moniker = new NCPolicyMoniker(toString());

    @Serial
    @Override
    Object writeReplace() {
      return moniker;
    }

    @Override
    public String optconfName() {
      return policyName();
    }

    XSPolicy() {
      super(true, false, NodeTaskInfo.FAVOR_REUSE);
    }

    @Override
    public String policyName() {
      return "XS";
    }

    @Override
    <T> DelayedProxyNode<T> createProxy(
        NodeCacheBase cache, PropertyNode<T> key, PropertyNode<T> found) {
      return new DelayedXSProxyNode<>(cache, key);
    }

    @Override
    <T> boolean matchesScenario(PropertyNode<T> cValue, PropertyNode<T> key) {
      return super.matchesScenario(cValue, key) && cValue instanceof DelayedXSProxyNode<T>;
    }
  }

  /** Policy to match original (aka owner) nodes */
  private static class XSOwnerPolicy extends NCPolicy {
    // We never put up a proxy to resolve CancellationScope for xs owners
    // because the DelayedPutIfAbsentPropertyXSProxyNode handles matches of CancellationScope
    // if we are ready to insert XSOwner we definitively already tried a match with a different
    // CancellationScope
    XSOwnerPolicy() {
      super(false, true, 0);
    }

    @Override
    <T> int hashOf(PropertyNode<T> key) {
      return NCSupport.chash(
          key.hashCodeForCaching(), key.scenarioStack().ssShared().scenarioStack());
    }

    @Override
    <T> DelayedProxyNode<T> createProxy(
        NodeCacheBase cache, PropertyNode<T> key, PropertyNode<T> found) {
      throw new GraphInInvalidState();
    }

    @Override
    <T> boolean matchesScenario(PropertyNode<T> cValue, PropertyNode<T> key) {
      if (cValue.isXScenarioOwner()) {
        ScenarioStack k_ss = key.scenarioStack();
        ScenarioStack v_ss = cValue.scenarioStack();
        if (cValue.isDone()) {
          return isDirectlyReusableWRTCancelScope(key, cValue) && matchXscenario(key, k_ss, v_ss);
        } else if (Settings.delayResolveXSCache) {
          // Quick check of current recorded tweakables to see if this node is not a match
          var partialMatch = NCSupport.partialMatchXS(cValue, k_ss, v_ss);
          // Do not match on anything that has a tweakable listener in our own tweakable
          // listener parent chain otherwise we would have a cycle.
          // Note the order of testing could make a perf difference. Needs more testing/research
          return partialMatch
              && !TweakableListener$.MODULE$.intersectsAndRecordParent(k_ss, v_ss, key);
        }
      }
      return false;
    }
  }
}
