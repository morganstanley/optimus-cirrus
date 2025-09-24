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

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import optimus.graph.CacheTopic$;
import optimus.graph.CancellationScope;
import optimus.graph.DiagnosticSettings;
import optimus.graph.NodeTask;
import optimus.graph.NodeTaskInfo;
import optimus.graph.OGTrace;
import optimus.graph.PropertyNode;
import optimus.graph.ProxyPropertyNode;
import optimus.graph.RecordedTweakables;
import optimus.graph.Settings;
import optimus.graph.TweakTreeNode;
import optimus.platform.NodeHash;
import optimus.platform.ScenarioStack;
import optimus.platform.Tweak;
import optimus.platform.Tweak$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NCSupport {
  public static class JVMInfo implements Serializable {
    @Serial private static final long serialVersionUID = 1L;
    long retainedSize;
    public JVMClassInfo[] clsInfos;
  }

  public static class JVMClassInfo implements Serializable {
    @Serial private static final long serialVersionUID = 1L;
    public transient Class<?> cls;
    // classes whose instances are retained in the node cache
    // and which are reachable from a cached instance of this
    // class (only non-empty for cached nodes)
    public transient Class<?>[] retainedClasses;
    // classes whose instances are retained in the node cache and which are reachable
    // from a cached instance of this class as well as from a cached instance of some
    // other class (only non-empty for cached nodes)
    public transient Class<?>[] sharedRetainedClasses;

    public transient int[] retainedClassSizes; // count,size, count,size, count,size, ...
    public transient int[] sharedClassSizes; // count,size, count,size, count,size, ...
    public int count;
    public long size;
    public long retainedSize;
    public long sharedRetainedSize;
    public String finalizer;
    public Reachability reachability;
    public String name;
  }

  protected static final Logger log = LoggerFactory.getLogger(NCSupport.class);

  /**
   * Computes the hash of the nodeTask with either given scenarioStack or the
   * scenarioIndependentScenarioStack
   */
  static int chash(int keyHash, ScenarioStack ss) {
    return chash(keyHash * 41 ^ ss._cacheID().hashCode());
  }

  /**
   * From CHM Applies a supplemental hash function to a given hashCode, which defends against poor
   * quality hash functions. This is critical because we use power-of-two length hash tables, that
   * otherwise encounter collisions for hashCodes that do not differ in lower or upper bits.
   */
  private static int chash(int h) {
    // Spread bits to regularize both segment and index locations,
    // using variant of single-word Wang/Jenkins hash.
    h += (h << 15) ^ 0xffffcd7d;
    h ^= (h >>> 10);
    h += (h << 3);
    h ^= (h >>> 6);
    h += (h << 2) + (h << 14);
    return h ^ (h >>> 16);
  }

  static int roundUpToPowerOf2(int minSize, int requestedSize) {
    int size = minSize;
    while (size < requestedSize) size <<= 1;
    return size;
  }

  /**
   * -agentpath:path_to_heapexp.[dll|so]
   *
   * @param caches initial set of top level caches (null for every cache instance)
   * @param precise false: instance/total size (fast), true: retained/shared size (slow)
   * @param typeOfInterest prefix for the type of interest
   * @param profile output array of arrays packaged results
   */
  static native long getCacheJVMSize(
      Object[] caches, boolean precise, String typeOfInterest, /* [out] */ Object[] profile);

  public static native void tagNativeNodes(Object[] caches, int seconds);

  /**
   * Computes the total JVM size of data held by this cache. Sub-objects held by roots other than
   * those that reach to this cache will be excluded
   */
  public static JVMInfo getJVMSize(Object[] caches) {
    return getProfile(caches, false, "~");
  }

  // from heapexp.cpp's ClassDetails
  private static final int MATCHED_FILTER = 1;
  private static final int NODE_TYPE = 2;
  private static final int HAS_FINALIZER = 4;
  private static final int CACHE_VALUE_REF = 8;
  private static final int CACHE_KEY_HOLDER = 16;
  private static final int CACHE_RETAINED = 32;
  private static final int CACHE_SHARED = 64;

  public static JVMInfo getProfile(Object[] caches, boolean detailed, String typeOfInterestPrefix) {
    Object[] jvmp = new Object[8];
    JVMInfo r = new JVMInfo();
    r.retainedSize = getCacheJVMSize(caches, detailed, typeOfInterestPrefix, jvmp);

    // Unpack all data
    Class<?>[] classes = (Class<?>[]) jvmp[0];
    if (classes == null) classes = new Class<?>[0];
    int[] counts = (int[]) (jvmp[1]);
    long[] sizes = (long[]) (jvmp[2]);
    Class<?>[][] retainedClasses = (Class<?>[][]) jvmp[3];
    Class<?>[][] sRetainedClasses = (Class<?>[][]) jvmp[4];
    int[] nodeTypes = (int[]) (jvmp[5]);
    int[][] retainedClassSizes = (int[][]) jvmp[6];
    int[][] sharedClassSizes = (int[][]) jvmp[7];
    r.clsInfos = new JVMClassInfo[classes.length];
    for (int i = 0; i < classes.length; i++) {
      JVMClassInfo cls = new JVMClassInfo();
      cls.cls = classes[i];
      cls.name = classes[i].getName();
      cls.count = counts[i];
      cls.size = sizes[i * 3];
      cls.retainedSize = sizes[i * 3 + 1];
      cls.sharedRetainedSize = sizes[i * 3 + 2];
      cls.retainedClasses = retainedClasses[i];
      cls.sharedRetainedClasses = sRetainedClasses[i];
      cls.retainedClassSizes = retainedClassSizes[i];
      cls.sharedClassSizes = sharedClassSizes[i];
      cls.reachability = Reachability.Value;

      if ((nodeTypes[i] & NODE_TYPE) != 0) {
        cls.reachability = Reachability.Cached;
      } else if ((nodeTypes[i] & CACHE_RETAINED) != 0) {
        cls.reachability = Reachability.Unique;
      }

      if ((nodeTypes[i] & CACHE_SHARED) != 0) {
        if ((nodeTypes[i] & NODE_TYPE) != 0) cls.reachability = Reachability.CachedShared;
        else cls.reachability = Reachability.Shared;
      }

      if ((nodeTypes[i] & HAS_FINALIZER) != 0) cls.finalizer = "yes";

      r.clsInfos[i] = cls;
    }
    return r;
  }

  /**
   * Update Profiler stats
   *
   * @param typeOfInterestPrefix Lpackage/type format
   * @return number of objects reached from root of typeOfInterestPrefix
   */
  public static JVMInfo updateProfile(
      Object[] caches, boolean detailed, String typeOfInterestPrefix) {
    JVMInfo r = getProfile(caches, detailed, typeOfInterestPrefix);

    HashMap<Class<?>, JVMClassInfo> hash = new HashMap<>();
    for (JVMClassInfo clsInfo : r.clsInfos) hash.put(clsInfo.cls, clsInfo);

    OGTrace.memory(hash);
    return r;
  }

  /**
   * Returns cachedValue if the value is usable directly from cachedValue key if the value needs to
   * be recomputed null if cannot answer the question at this point (obviously transient)
   */
  static PropertyNode<?> isUsableWRTCancelScopeAndEnv(
      PropertyNode<?> key, PropertyNode<?> cachedValue) {
    ScenarioStack keySS = key.scenarioStack();
    ScenarioStack cachedSS = cachedValue.scenarioStack();

    // super-fast path: always usable if CancellationScope and RuntimeEnvironment match exactly
    if (keySS.cancelScopeAndEnvId() == cachedSS.cancelScopeAndEnvId()) return cachedValue;

    // if compatAllowCrossRuntimeCacheHits is NOT enabled, the SSCacheID already takes the runtime
    // environment into account so we can safely ignore it here (that's the old behavior).
    // see [[CROSS_RUNTIME_MATCHING]]
    // n.b. we don't track access to the environment here - we're not using it, only comparing it
    boolean sameEnv =
        !Settings.compatAllowCrossRuntimeCacheHits || keySS.envId() == cachedSS.envId();

    // if complete with usable result (i.e. not non-RT exception), CS doesn't matter at all
    if (cachedValue.isDoneWithUsableResult()) {
      // if same env or didn't access env then can reuse, otherwise cannot
      if (sameEnv || !cachedValue.accessedRuntimeEnv()) return cachedValue;
      else return key;
    }

    boolean sameCS = keySS.cancellationScopeId() == cachedSS.cancellationScopeId();

    // not complete, or complete but with non-RT exception, so must check CS
    if (sameCS) {
      if (sameEnv) return cachedValue; // exact match for CS and env so always safe to re-use

      // same CS but different env, so if we're done we can re-use if and only if the env wasn't
      // actually accessed
      if (cachedValue.isDone()) {
        if (cachedValue.accessedRuntimeEnv()) return key;
        else return cachedValue;
      }
    }

    // different CS
    if (cachedSS.cancelScope().isCancelled() || cachedValue.isDoneWithExceptionToHide())
      return key; // The cached value is cancelled and can't be reused (ever)

    // different CS and/or env, and the node isn't complete/cancelled yet
    return null; // Can't answer the question for now
  }

  /**
   * Returns true if definitely can't use the value (false if we can use value or cannot determine
   * yet
   */
  static <T> boolean isNotUsableWRTCancelScopeAndEnv(
      PropertyNode<T> key, PropertyNode<T> cachedValue) {
    return isUsableWRTCancelScopeAndEnv(key, cachedValue) == key;
  }

  /** Returns true when can re-use directly */
  static boolean isDirectlyReusableWRTCancelScopeAndEnv(
      PropertyNode<?> key, PropertyNode<?> cachedValue) {
    return isUsableWRTCancelScopeAndEnv(key, cachedValue) == cachedValue;
  }

  /** Return true for delayed proxies */
  public static boolean isDelayedProxy(NodeTask ntsk) {
    return ntsk instanceof DelayedProxyNode;
  }

  public static boolean isProxy(NodeTask ntsk) {
    return ntsk instanceof ProxyPropertyNode<?>;
  }

  /** Return true for CancellationScope proxies (used in tests to filter CS proxies in cache) */
  public static boolean isDelayedCSProxy(NodeTask ntsk) {
    return ntsk instanceof DelayedCSProxyNode;
  }

  /* Filter out delayed proxies */
  public static ArrayList<NodeTask> removeProxies(ArrayList<NodeTask> path) {
    ArrayList<NodeTask> r = new ArrayList<>();
    NodeTask lastInserted = null;
    for (NodeTask tsk : path) {
      NodeTask underlying = tsk.cacheUnderlyingNode();
      if (underlying != lastInserted) {
        lastInserted = underlying;
        r.add(underlying);
      }
    }
    return r;
  }

  /**
   * If debugging, set dbgNameToTriggerOn in IJ debugger and add breakpoint in
   * NCSupport#setBreakPointHere() below.
   */
  @SuppressWarnings("unused") // See the comment above (this can be used for debugging session
  private static String dbgNameToTriggerOn;

  // make sure that we only hit breakpoints in this thread
  private static final ThreadLocal<Boolean> dbgTraceEnabled = new ThreadLocal<>();

  private static <T> void enableOnPropertyNode(PropertyNode<T> value) {
    if (value.propertyInfo().name().equals(dbgNameToTriggerOn)) {
      dbgTraceEnabled.set(Boolean.TRUE);
    } else dbgTraceEnabled.set(Boolean.FALSE); // to make sure it's not null in other threads
  }

  private static void disableOnPropertyNode() {
    dbgTraceEnabled.set(Boolean.FALSE);
  }

  private static void setBreakPointHere() {
    if (dbgTraceEnabled.get()) {
      System.err.println("Reached breakpoint");
    }
  }

  /**
   * Helper methods for debugging cross-scenario cache misses - each of these report a different
   * reason for the miss, but we only need to set the breakpoint in the method above then walk up
   * the stack frame
   */
  private static void reportMissingTweak() {
    setBreakPointHere();
  }

  private static void reportMismatchedTweak() {
    setBreakPointHere();
  }

  private static void reportSubMatchFailed() {
    setBreakPointHere();
  }

  private static void reportEvaluateInNotSupported() {
    setBreakPointHere();
  }

  private static void reportPossiblyHaveExtraTweak() {
    setBreakPointHere();
  }

  private static void reportHaveExtraTweak() {
    setBreakPointHere();
  }

  private static void reportMismatchedRoot() {
    setBreakPointHere();
  }

  /**
   * Used to quickly filter out non-matching running XS node
   *
   * @param kss (key scenario stack) RecordingScenarioStack of the requested node
   * @param css (cached scenario stack) RecordingScenarioStack of a running cached node
   * @param dbgPropertyNode the cached node we might have a potential match on (used only for
   *     diagnostics if trace is enabled or -Doptimus.profile.xsreporting is explicitly set to true)
   * @return true if the scenario stack might be able to be re-used, false if it definitely cannot
   *     be re-used
   */
  static boolean partialMatchXS(
      PropertyNode<?> dbgPropertyNode, ScenarioStack kss, ScenarioStack css) {
    if (DiagnosticSettings.enableXSReporting) enableOnPropertyNode(dbgPropertyNode);
    try {
      // Different environments can have completely different results, that's encapsulated in root
      if (kss.ssShared()._cacheID() != css.ssShared()._cacheID()) {
        if (DiagnosticSettings.enableXSReporting) reportMismatchedRoot();
        return false;
      }

      // Take a snapshot of current reported tweakables to match against
      RecordedTweakables rt = css.tweakableListener().recordedTweakables();

      return matchXscenario(kss, rt) != null; // Verifies all tweaks match
    } finally {
      if (DiagnosticSettings.enableXSReporting) disableOnPropertyNode();
    }
  }

  /**
   * matchXscenario Matches xs node (aka 'X' aka STATE_XSCENARIO_OWNER) against a given scenario
   * stack (RecordingScenarioStack) On success modifies key's scenarioStack with the remapped result
   * of the match
   *
   * <p>Note: This function doesn't need cached RecordingScenarioStack, just the recorded tweaks on
   * it, It looks to match tweak structure only and doesn't mutate that scenario stack. But does
   * need key (aka query) ScenarioStack because it might need to resolve tweaks (e.g. calling when
   * clauses) And it will mutate it (assigning remapped tweak tree node to its listener) on success
   *
   * <p>As part of the match matched tweak's scenariostack trackingDepth is being recorded. This is
   * to allow quick filtering-out of inner ScenarioStack's tweaks, that the outer ones almost always
   * don't care for
   *
   * <p>In order to match Recoreded Tweakables (here by Tweakables we mean
   * Tweaks/Tweakables/TweakHashes...) We need to match it as a tree. e.g. b := a a := 4 a: = 3 It's
   * not enough to just lookup tweak for a, as it appears multiple times and b is sensitive to a
   * that was in the scenarioStack below where tweak for b was found.
   *
   * @param kss (key scenario stack) RecordingScenarioStack of the requested node
   * @param css (cached scenario stack) RecordingScenarioStack of a completed cached node
   * @param key the cached node we might have a potential match on (used only for diagnostics if
   *     trace is enabled or -Doptimus.profile.xsreporting is explicitly set to true)
   * @return true if scenario stacks can be re-used AND kss tracking info will be filled in at this
   *     point! false if it's not a match! NOTE: there are a LOT optimizations possible!!!!
   */
  public static boolean matchXscenario(PropertyNode<?> key, ScenarioStack kss, ScenarioStack css) {
    if (DiagnosticSettings.enableXSReporting) enableOnPropertyNode(key);

    OGTrace.consumeTimeInMs(OGTrace.TESTABLE_FIXED_DELAY_XS, CacheTopic$.MODULE$);
    try {
      // Different environments can have completely different results, that's encapsulated in root
      if (kss.ssShared()._cacheID() != css.ssShared()._cacheID()) {
        if (DiagnosticSettings.enableXSReporting) reportMismatchedRoot();
        return false;
      }

      // We are going to need to look at the tweaks in kss. If kss is cancelled, it might be
      // because we are currently modifying its MutableSSCacheID in a dependency tracker
      // invalidation. If that's the case, calling matchXScenario(infoTarget, kss, rt) below can
      // produce a ConcurrentModificationException or potentially wrong results.
      //
      // We don't need to check more than once because of
      // DependencyTrackerQueue.evaluationBarrier().
      // If we are checking either we are in a currently running cancelled node (in which case we
      // will mutate only after waitForCancelledNodesToStop, and thus our node is done) or in a
      // NodeTask.complete() call after the barrier, in which case the scope has already been
      // cancelled.
      //
      // css is fine though because we only look at recordedTweakables, which is not mutable.
      if (kss.cancelScope().isCancelled()) return false;

      RecordedTweakables rt = css.tweakableListener().recordedTweakables();
      // Verifies all tweakables but returns (re)mappedTweaks
      TweakTreeNode[] mappedTweaks = matchXscenario(kss, rt);
      if (mappedTweaks == null) return false;

      // Besides (re)mappedTweaks, other tweakables and tweak hashes remain unchanged). See
      // [XS_BY_NAME_TWEAKS]
      // [SEE_XS_COMPLETED] cannot complete if not reusable w.r.t. CS
      key.replace(key.scenarioStack().withRecorded(rt.withTweaked(mappedTweaks)));
      return true;
    } finally {
      if (DiagnosticSettings.enableXSReporting) disableOnPropertyNode();
    }
  }

  /**
   * Verifies that given key scenario stack would produce the same tweak results All Tweakables
   * (Tweaks/Tweakables/TweakableHashes) are validated
   *
   * @return returns null (as opposed to even empty TweakTreeNode[0] to signify that the match has
   *     failed
   */
  private static TweakTreeNode[] matchXscenario(ScenarioStack kss, RecordedTweakables tcss) {
    TweakTreeNode[] matchedTweaks = matchTweaks(kss, tcss);
    if (matchedTweaks == null) return null;

    if (mayHaveTweaksForHashes(kss, tcss.tweakableHashes())) return null;

    if (hasTweaksForTweakables(kss, tcss.tweakable())) return null;

    return matchedTweaks;
  }

  /**
   * Verifies that given key scenario stack would produce the same tweak results Only Tweaks are
   * matched
   *
   * @return returns null (as opposed to even empty TweakTreeNode[0] to signify that the match has
   *     failed
   */
  private static TweakTreeNode[] matchTweaks(
      ScenarioStack kss, RecordedTweakables cachedTweakables) {
    TweakTreeNode[] cachedTweaks = cachedTweakables.tweaked();
    if (cachedTweaks.length == 0) return cachedTweaks;

    TweakTreeNode[] mappedTweaks = new TweakTreeNode[cachedTweaks.length];
    int mappedCount = 0;

    // Check that all previously tweaked properties are still tweaked and the tweaks match
    for (TweakTreeNode ttn : cachedTweaks) {
      PropertyNode<?> key = ttn.key();
      var ktwk_ss = kss.getTweakAndScenarioStackNoWhenClauseTracking(key);
      if (ktwk_ss == null) {
        NodeTaskInfo info = key.propertyInfo();
        log.error(
            "Unexpected, Optimus Graph Team (info = "
                + info.toString()
                + " key = "
                + key.toKeyString()
                + ")");
        Thread.dumpStack();
        return null;
      }
      Tweak ktwk = ktwk_ss.tweak();
      if (ktwk == null) { // [2] Was tweaked, but it's not now!
        if (DiagnosticSettings.enableXSReporting) reportMissingTweak();
        return null;
      }

      Tweak ctwk = ttn.tweak();
      if (ktwk != ctwk && !ctwk.equals(ktwk)) { // We don't have matching tweaks
        if (DiagnosticSettings.enableXSReporting) reportMismatchedTweak();
        return null;
      }

      ScenarioStack ktwkss = ktwk_ss.foundInScenarioStack();
      TweakTreeNode mapped;
      // If this tweak depended on other tweakables, (e.g. Tweak.byName(a := b), or even
      // Tweak.byName(a := a + 1)), then
      // we need to check that those dependencies match when seen from the key scenario stack where
      // we found the tweak,
      // This may not be at the same depth as the tweak was present in the cached scenarios stack,
      // so we'll remap
      // the depth. See [XS_BY_NAME_TWEAKS]
      if (ttn.nested() != null) {
        if (ctwk.evaluateIn() != Tweak$.MODULE$.evaluateInParentOfGiven()) {
          if (DiagnosticSettings.enableXSReporting) reportEvaluateInNotSupported();
          return null; // Currently only evaluateInParentOfGiven is supported
        }
        TweakTreeNode[] subMatch = matchXscenario(ktwkss.parent(), ttn.nested());
        if (subMatch == null) {
          if (DiagnosticSettings.enableXSReporting) reportSubMatchFailed();
          return null;
        }
        mapped = ttn.withDepth(ktwk, ktwkss.trackingDepth(), subMatch);
      } else mapped = ttn.withDepth(ktwk, ktwkss.trackingDepth());

      mappedTweaks[mappedCount++] = mapped;
    }
    return mappedTweaks;
  }

  private static boolean mayHaveTweaksForHashes(ScenarioStack kss, NodeHash[] hashes) {
    if (hashes != null) {
      for (NodeHash hash : hashes) {
        // if we have a tweak now for something we didn't have before we don't match
        // Note: missing on a possible match where default computed value would be the same as the
        // new tweak
        // Note: missing on a possible match where property tweak would not have matched the target
        if (hash.propertyInfo().wasTweakedAtLeastOnce() && kss.mayDependOnTweak(hash)) {
          if (DiagnosticSettings.enableXSReporting) reportPossiblyHaveExtraTweak();
          return true;
        }
      }
    }
    return false;
  }

  private static boolean hasTweaksForTweakables(ScenarioStack kss, PropertyNode<?>[] tweakable) {
    for (PropertyNode<?> key : tweakable) {
      if (kss.getTweakNoWhenClauseTracking(key) != null) {
        // [1] We have a tweak now for something we didn't have before
        if (DiagnosticSettings.enableXSReporting) reportHaveExtraTweak();
        return true;
      }
    }
    return false;
  }
}
