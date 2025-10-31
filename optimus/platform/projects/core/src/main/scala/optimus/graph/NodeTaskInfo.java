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
package optimus.graph;

import static optimus.graph.OGTrace.CachedSuffix;
import static optimus.graph.OGTrace.XsSuffix;
import static optimus.graph.Settings.defaultCacheByNameTweaks;
import static optimus.graph.Settings.maxCustomFrames;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import optimus.breadcrumbs.Breadcrumbs;
import optimus.breadcrumbs.ChainedID$;
import optimus.breadcrumbs.crumbs.Crumb;
import optimus.breadcrumbs.crumbs.Properties;
import optimus.breadcrumbs.crumbs.PropertiesCrumb;
import optimus.config.CacheConfig;
import optimus.config.CachePolicyConfig;
import optimus.config.ConstructorConfig;
import optimus.config.NodeCacheInfo;
import optimus.config.NodeConfig;
import optimus.config.PropertyConfig;
import optimus.core.CoreUtils;
import optimus.core.MonitoringBreadcrumbs$;
import optimus.core.TPDMask;
import optimus.entity.OptimusInfo;
import optimus.graph.cache.NCPolicy;
import optimus.graph.cache.NodeCCache;
import optimus.graph.cache.PerPropertyCache;
import optimus.graph.diagnostics.NodeName;
import optimus.graph.loom.CallSiteHolder;
import optimus.platform.ScenarioStack;
import optimus.platform.inputs.registry.ProcessGraphInputs;
import optimus.platform.util.PrettyStringBuilder;
import optimus.platform.util.Version$;
import scala.Option;

@SuppressWarnings(
    "StaticInitializerReferencesSubClass") // SequenceNodeTaskInfo is and needs to be super simple
public class NodeTaskInfo {
  // BEGIN WARNING: The next set of flags have to match the ones in
  // optimus.tools.scalacplugins.entity.PropertyFlags!!!!
  public static final long NOTNODE = 1; // PropertyInfo for non-@node stored properties!
  public static final long TWEAKABLE = 1L << 1;
  private static final long STORED = 1L << 2;
  public static final long INDEXED = 1L << 3;
  public static final long KEY = 1L << 4;
  public static final long SCENARIOINDEPENDENT = 1L << 5; // @scenarioIndependent or @siRhs
  public static final long GIVEN_RUNTIME_ENV = 1L << 6; // Lower stack to si + initialTime
  private static final long TWEAKHANDLER = 1L << 7; // Tweak handler is present (aka also-sets)
  public static final long DONT_CACHE = 1L << 8;
  private static final long IS_ABSTRACT = 1L << 9;
  private static final long IS_JOB = 1L << 10;
  private static final long IS_RECURSIVE = 1L << 11;
  // END WARNING

  // Runtime updated flags
  public static final long WAS_INSTANCE_TWEAKED = 1L << 12;
  public static final long WAS_PROPERTY_TWEAKED = 1L << 13;
  public static final long WAS_TWEAKED = WAS_INSTANCE_TWEAKED | WAS_PROPERTY_TWEAKED;

  // only set if tweaked in DependencyTracker
  public static final long WAS_INSTANCE_TWEAKED_TRACKER = 1L << 14;
  private static final long WAS_PROPERTY_TWEAKED_TRACKER = 1L << 15;
  private static final long WAS_TWEAKED_TRACKER =
      WAS_INSTANCE_TWEAKED_TRACKER | WAS_PROPERTY_TWEAKED_TRACKER;

  /** Track tweakable by hash, assuming false positive are rare or none */
  public static final long TRACK_TWEAKABLES_BY_HASH = 1L << 16;

  // Cache control flags (see also SCENARIOINDEPENDENT and DONT_CACHE)
  // Could be XS or XSFT or... Mostly for UI filtering only
  public static final long FAVOR_REUSE = 1L << 17;
  private static final long GROUP_CACHED = 1L << 18;
  private static final long LOCAL_CACHE = 1L << 19;
  public static final long HOLDS_NATIVE = 1L << 20; // Will be released from GCNative
  public static final long SINGLE_THREADED = 1L << 21; // Switch to a single queue
  public static final long SHOULD_LOOKUP_PLUGIN = 1L << 22; // Plugin installed (needs lookup)

  // if true, then HOLDS_NATIVE (and possibly more in future) was set by heap explorer
  private static final long HEAP_EXPLORED = 1L << 23;

  // When set, asserts that this node can only be awaited once.
  public static final long AT_MOST_ONE_WAITER = 1L << 24;

  // if true, DependencyTracker doesn't track this tweak key
  public static final long DONT_TRACK_FOR_INVALIDATION = 1L << 25;

  public static final long PROFILER_UI_CONFIGURED = 1L << 26; // nodes configured via profiler UI

  // Externally-Configured is now a bitmap
  public static final long EXTERNALLY_CONFIGURED_CUSTOM_CACHE = 1L << 27;
  public static final long EXTERNALLY_CONFIGURED_POLICY = 1L << 28;
  public static final long EXTERNALLY_CONFIGURED =
      EXTERNALLY_CONFIGURED_CUSTOM_CACHE | EXTERNALLY_CONFIGURED_POLICY;

  public static final long ATTRIBUTABLE_USER_CODE = 1L << 29;
  public static final long TRACE_SELF_AND_PARENT = 1L << 30; // [SEE_PARTIAL_TRACE]

  /*
    Next set of flag are to help profiler identify some nodes
    They will probably be moved out to some other profiler specific flags
  */
  public static final String NAME_MODIFIER_PROXY = "~$";
  public static final String NAME_MODIFIER_TWEAK = ":=";
  public static final String NAME_MODIFIER_BOOTSTRAP = "-bootstrap";
  public static final long PROFILER_PROXY = 1L << 31; // Proxy
  // Mostly helper nodes, all have custom executionInfo
  public static final long PROFILER_INTERNAL = 1L << 32;
  public static final long PROFILER_IGNORE = 1L << 33; // unstable non-RT nodes

  // While profiling this flag will set if at least once at least one child was adapted (i.e. real
  // async was used)
  public static final long ASYNC_NEEDED = 1L << 34;

  /** See comments on usage in markPropertyTweaked */
  private static final long WAS_MARK_PTWEAKED_CALLED = 1L << 35;

  private static final long EXCLUDE_FROM_RTV_NODE_RERUNNER = 1L << 36;

  private static final long DISABLE_REMOVE_REDUNDANT = 1L << 37;

  @SuppressWarnings("FieldMayBeFinal") // Set atomically via Unsafe
  private volatile long _flags;

  private final String _name;
  private SchedulerPlugin _plugin; // Scheduler plug-in can register here
  private PluginType _reportingPluginType;

  volatile NodeClsIDSupport.FieldMap fieldMap;

  /**
   * Actually tweaked properties counter, with 1 reserved for callers of internalSetTweakMaskForDAL
   */
  private static final AtomicInteger _tweakableCounter = new AtomicInteger(1);

  /** Unique id of a property, used to locate profile info */
  public int profile;

  @Override
  public int hashCode() {
    return profile;
  }

  /* Non unique ID of each tweakable property */
  private int tweakableID;
  // essentially a bloom filter to optimise tweak lookup (for apps with hundreds of tweakables and
  // only a handful of actual tweaks)
  private volatile TPDMask tweakMask;
  private volatile TPDMask dependsOnTweakMask = TPDMask.empty;

  // This is filled in during EntityInfo ctor
  public OptimusInfo entityInfo;

  // Used in NodeMetaFactory to switch caching on and off on the fly
  public CallSiteHolder callSiteHolder;

  private enum ConfiguredBy {
    OPTCONF,
    API,
    UI
  }

  // configuration that was in place before live optconf was applied via NodeCacheConfigs.apply
  // this could be a load-time optconf from -Doptimus.profile (externally_configured_* = true)
  // or a result of API calls such as node.setCacheable(false) (externally_configured_* = false)
  // when optconf is undone via NodeCacheConfigs.reset, config reverts to this.
  static class PropertyConfigBackup {
    // for each config option (policy, ccache), the possibilities are:
    // 1. not configured (primordial state)
    // 2. configured via API to be false/null
    // 3. configured via API to be true/value
    // 4. configured via optconf to be false/null
    // 5. configured via optconf to be true/value
    NCPolicy policy;
    boolean externallyConfiguredPolicy;
    NodeCCache cache;
    boolean externallyConfiguredCustomCache;

    // to control 'who' is able to revert to previous state
    final ConfiguredBy configuredBy;

    PropertyConfigBackup(NCPolicy policy, NodeCCache cache, ConfiguredBy configuredBy) {
      this.policy = policy;
      this.cache = cache;
      this.configuredBy = configuredBy;
    }
  }

  private PropertyConfigBackup configBackup;

  private static final VarHandle flags_h;

  static {
    try {
      flags_h = MethodHandles.lookup().findVarHandle(NodeTaskInfo.class, "_flags", long.class);
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  private void updateFlags(long flags, boolean enable) {
    if (enable) {
      updateFlags(flags, 0);
    } else {
      updateFlags(0, flags);
    }
  }

  /** Returns true if the flags were updated */
  private void updateFlags(long setFlags, long clearFlags) {
    long prevFlags, newFlags;
    do {
      prevFlags = _flags;
      newFlags = (prevFlags | setFlags) & ~clearFlags;
      if (prevFlags == newFlags) {
        return; // Optimize for setting the flag once (i.e. expect no change)
      }
    } while (!flags_h.compareAndSet(this, prevFlags, newFlags));
  }

  /**
   * NodeTaskInfo are assumed to be singleton (e.g. don't do this: executionInfo = new
   * NodeTaskInfo())
   */
  public NodeTaskInfo(String name, long flags) {
    this(name, flags, true);
  }

  protected NodeTaskInfo(String name, long flags, boolean registerWithTrace) {
    _flags =
        flags
            | TRACK_TWEAKABLES_BY_HASH
            | (Settings.defaultToIncludeInTrace ? TRACE_SELF_AND_PARENT : 0)
            | (DiagnosticSettings.rtvNodeRerunnerExcludeByDefault
                ? EXCLUDE_FROM_RTV_NODE_RERUNNER
                : 0);
    _name = name;
    profile = NodeTrace.register(this, registerWithTrace);
    NCPolicy policy =
        NCPolicy.forInfo(
            this,
            ProcessGraphInputs.EnableXSFT().currentValueOrThrow() ? NCPolicy.XSFT : NCPolicy.Basic);

    _cachePolicy = NCPolicy.forInfo(this, policy);
    if (isJob()) {
      setPlugin(new JobPlugin());
    }
  }

  /**
   * NodeTaskInfos are assumed to be singleton e.g. don't do this: def executionInfo = new
   * NodeTaskInfo() at a minimum it creates a leak DAL batcher plugins used to be not well-behaved
   * in this regard. They are trying to pass custom scheduler plugin per instance. In order to
   * accommodate this behaviour we have code that registers the basic NodeTaskInfo and here we'll
   * clone it with setting a plugin on a copy which is not registered.
   *
   * <p>IMPORTANT: This is probably NOT the API you are looking for. Only name, flags and profile
   * will be copied!
   */
  public NodeTaskInfo copyWithPlugin(SchedulerPlugin plugin) {
    var nti = new NodeTaskInfo(_name, _flags, false);
    nti.profile = profile;
    nti.setPlugin(plugin);
    return nti;
  }

  static void configureCachePolicy(NodeTaskInfo info, NCPolicy filterPolicy, NCPolicy newPolicy) {
    // either we don't care what policy was before (ie, filterPolicy = null), or we want to be
    // careful and check
    if (info != null && (filterPolicy == null || info.cachePolicy() == filterPolicy))
      info.setDefaultCachePolicy(newPolicy);
  }

  /**
   * Specifically used for APIs that bulk-enable new 'default' policy. Behaves as if setting through
   * optconf
   */
  private void setDefaultCachePolicy(NCPolicy requestedPolicy) {
    /* for consistency with other APIs that set cache config (policies set through optconf 'win' over API) */
    if (warnIfExternallyConfigured(EXTERNALLY_CONFIGURED_POLICY, "setDefaultCachePolicy")) {
      return;
    }

    // use forInfo to avoid setting non-default policies on internal NTIs (see one other use case in
    // constructor) */
    NCPolicy policy = NCPolicy.forInfo(this, requestedPolicy);

    // setPolicy checks policy != _cachePolicy anyway, but don't even markConfigured or capture
    // backup if no changes
    boolean setPolicy = policy != _cachePolicy && setPolicyExternal(policy, ConfiguredBy.API);
    if (setPolicy) {
      if (Settings.schedulerAsserts && policy != requestedPolicy) {
        OGScheduler.log.debug(
            "Requested policy was {} but actually set {}", requestedPolicy, policy);
      }
    }
  }

  public final String rawName() {
    return _name;
  }

  public String name() {
    return _name;
  }

  public String modifier() {
    return "";
  }

  public final String fullName() {
    return nodeName().toString();
  }

  public final String fullNamePackageShortened() {
    return nodeName().toString(true);
  }

  /**
   * return Node Name which should match NodeName.fromNodeCls(nodeOfThisProperty.getClass) in the
   * simplest case
   */
  public final NodeName nodeName() {
    Class<?> cls = runtimeClass();
    if (cls == null) {
      return new NodeName("", _name, modifier()); // Rare case like [marker]
    }

    String pkgName = cls.getPackage().getName();
    String fullName = cls.getName();
    // The reason for not calling cls.getSimpleName() and instead doing substring is because the
    // part of outer
    // class completely disappears
    return new NodeName(
        pkgName, fullName.substring(pkgName.length() + 1) + "." + _name, modifier());
  }

  public final SchedulerPlugin getPlugin(ScenarioStack scenarioStack) {
    SchedulerPlugin scopedPlugin = scenarioStack.siParams().resolvePlugin(this);
    return scopedPlugin != null ? scopedPlugin : _plugin;
  }

  /** You are <b>NOT</b> allowed to use this method. Only existing usage is allowed. */
  public final SchedulerPlugin unsafeGetPlugin_LIKELY_TO_BE_WRONG() {
    // the only reason why this function exists is as a temporary mechanism
    // to move from the legacy NTI-only plugins to scoped plugins.
    return getPlugin(ScenarioStack.constant());
  }

  public final SchedulerPlugin getPlugin_TEST_ONLY() {
    return unsafeGetPlugin_LIKELY_TO_BE_WRONG();
  }

  public final PluginType getPluginType(ScenarioStack scenarioStack) {
    SchedulerPlugin plugin = getPlugin(scenarioStack);
    return plugin == null ? PluginType.None() : plugin.pluginType();
  }

  // Possibly overridden by fake plugins
  public PluginType reportingPluginType(ScenarioStack scenarioStack) {
    return _reportingPluginType == null ? getPluginType(scenarioStack) : _reportingPluginType;
  }

  public void setPlugin(SchedulerPlugin plugin) {
    if (this == Default) {
      throw new GraphException("Cannot set plugin on a default info!");
    }

    if (isJob() && _plugin != null) {
      // isJob implies that _plugin points to a JobPlugin
      ((JobPlugin) _plugin).setSubPlugin(plugin);
      return;
    }

    if (plugin != null) {
      _plugin = plugin;
      markAsShouldLookupPlugin(); // Turn on AFTER _plugin set
    } else {
      // we do not reset the flag because it might have been set by a scoped plugin
      // on scenario stack, so it is never safe to remove
      _plugin = null;
    }

    if (callSiteHolder != null) callSiteHolder.refreshTarget();
  }

  public void clearPlugin_TEST_ONLY() {
    _plugin = null;
    updateFlags(SHOULD_LOOKUP_PLUGIN, false);
  }

  protected /*[graph]*/ NodeTaskInfo setReportingPluginType(PluginType pt) {
    _reportingPluginType = pt;
    return this;
  }

  /** For visualization use */
  public final long snapFlags() {
    return _flags;
  }

  public final boolean shouldLookupPlugin() {
    return (_flags & SHOULD_LOOKUP_PLUGIN) != 0;
  }

  public final boolean isStored() {
    return (_flags & STORED) != 0;
  }

  final boolean hasNoPlugAndNotST() {
    return (_flags & (SHOULD_LOOKUP_PLUGIN | SINGLE_THREADED)) == 0;
  }

  final boolean atMostOneWaiter() {
    return (_flags & AT_MOST_ONE_WAITER) != 0;
  }

  public final boolean canRemoveRedundant() {
    return (_flags & DISABLE_REMOVE_REDUNDANT) == 0;
  }

  public final void disableRemoveRedundant() {
    updateFlags(DISABLE_REMOVE_REDUNDANT, true);
  }

  public int syncID;

  private NCPolicy _cachePolicy;
  private NodeCCache _ccache;
  public int localCacheSlot;
  public int reuseCycle;
  public long reuseStats;

  public final NCPolicy cachePolicy() {
    return _cachePolicy;
  }

  private void setCacheNameIfDefault() {
    //noinspection StringEquality: intentional we only reset the name if it has never been set
    if (_ccache != null && _ccache.getName() == PerPropertyCache.defaultName()) {
      _ccache.setName(fullName());
    }
  }

  public final NodeCCache customCache() {
    setCacheNameIfDefault();
    return _ccache;
  }

  public final void setCustomCache(NodeCCache ccache) {
    requireNonAbstract(true);
    if (!warnIfExternallyConfigured(EXTERNALLY_CONFIGURED_CUSTOM_CACHE, "setCustomCache")) {
      setCache(ccache);
    }
  }

  /** sets flag only */
  public void markAsShouldLookupPlugin() {
    updateFlags(SHOULD_LOOKUP_PLUGIN, true);
  }

  private void markAsExternallyConfigured(long flag) {
    checkExternallyConfiguredFlag(flag);
    updateFlags(flag, 0);
  }

  // Cross scenario policies (XS, XSFT) require at least 2 slots for proxies otherwise we have no
  // hope of cache reuse
  private void configureSizeForCrossScenario(NodeCCache newCache) {
    if (newCache != null && _cachePolicy.policyName() != null && newCache.getMaxSize() < 2) {
      var msg = "Can't set custom cache of size < 2 on a cross-scenario node! Updating size to 2";
      NodeCacheInfo.log().javaLogger().warn(msg);
      newCache.setMaxSize(2);
    }
  }

  private void setCache(NodeCCache newCache) {
    if (_ccache != newCache) { // do not trash the cache if it is not being changed
      if (_ccache != null) {
        _ccache.recordNotInUse();
      }

      configureSizeForCrossScenario(newCache);

      _ccache = newCache;
      setCacheNameIfDefault();

      if (_ccache != null) {
        newCache.recordInUse();
      }
    }
  }

  void undoExternalCacheConfig() {
    undoExternalCacheConfig(ConfiguredBy.OPTCONF);
  }

  void TEST_ONLY_clearBackupConfiguration() {
    configBackup = null;
  }

  /**
   * optconf parameter should match that supplied when initialising backup, so disableXSFT can't
   * undo optconf-config
   */
  private void undoExternalCacheConfig(ConfiguredBy optconf) {
    if (configBackup == null || configBackup.configuredBy != optconf) {
      return;
    }
    if (isExternallyConfigured(EXTERNALLY_CONFIGURED_CUSTOM_CACHE)) {
      if (!configBackup.externallyConfiguredCustomCache) {
        clearExternallyConfigured(EXTERNALLY_CONFIGURED_CUSTOM_CACHE);
      }
      setCache(configBackup.cache);
    }
    if (isExternallyConfigured(EXTERNALLY_CONFIGURED_POLICY)) {
      if (!configBackup.externallyConfiguredPolicy) {
        clearExternallyConfigured(EXTERNALLY_CONFIGURED_POLICY);
      }
      setPolicy(configBackup.policy);
    }
  }

  /** Very rare case where we want to ensure a mask */
  public final void internalSetTweakMaskForDAL() {
    tweakableID = 1;
    tweakMask = TPDMask.fromIndex(1);
  }

  private static int addAndCheckIfOverflowingTweakIDs() {
    int limit = 64 * DiagnosticSettings.tweakUsageQWords;
    int count = _tweakableCounter.incrementAndGet();
    if (count == limit + 1
        || (count > limit && (count - limit) % DiagnosticSettings.tweakOverflowAmountToLog == 0))
      OGScheduler.log.info(
          "Tweakable IDs are overflowing, this can in some cases lead to sub-optimal cache reuse for under XSFT. If you see this is happening try enabling cross scenario caching for those nodes.");
    return count;
  }

  /**
   * Sets tweakMask on the property if it was not set before as follows:
   *
   * <ol>
   *   <li>has no parents: <br>
   *       sets tweakMask on itself by finding next tweakableID
   *   <li>no parent has already assigned tweakMask (i.e. was never tweaked) <br>
   *       sets tweakMask on itself by finding next tweakableID <br>
   *       copy this value into all the parents
   *   <li>all parents already share the mask and id (most likely because it's a single inheritance)
   *       <br>
   *       copy this value into self and other parents that didn't have it set before
   *   <li>multiple parents have different ids and masks<br>
   *       tweakableID = MAX_VALUE (basically a marker to signify that multiple bits are turned on
   *       in the mask <br>
   *       mask is union of all the previously set masks <br>
   *       copy this value into self and other parents that didn't have it set before <br>
   *       <br>
   *       There is room to optimize this use case if we consider each hierarchy recursively.
   *       Consider:
   *       <pre>
   *    trait B1 { x }
   *    trait B2 { x }
   *    trait B3 { x }
   *    class A extends B1, B2, B3
   *    B1.x := 3   // B1.x gets a new id lets say 2
   *    B2.x := 3   // B2.x gets a new id lets say 3
   *    A.x := 4    // A gets id == MAX_VALUE and mask of 2 and 3
   *                // B3 gets id == MAX_VALUE and mask of 2 and 3
   *                // but could just get a copy lets say of 2
   *    </pre>
   * </ol>
   *
   * Corollary:
   * <li>Mask once set never changes. Critical as once observed could be recorded in places like
   *     SSCacheID and node masks.
   * <li>Class hierarchies try to share one ID, so that SSCacheID.mayContainTweak can quickly
   *     determine whether there is a tweak for 'this' key and all of it parents
   * <li>Also we are saving the bits! In theory we can lose re-use but this is only in badly
   *     designed interface Consider from the example above:
   *
   *     <pre>
   *  class A2 extends B1 { def calc = x }
   *  a2.calc
   *  given(B2.x := 5) a2.calc // calc not re-used by XSFT because of mask sharing
   *  </pre>
   *
   *     If those B1.x and B2.x are not related they should be not have been "merged" by A and
   *     overridden by a single 'x' and if they are related there should have been a common base
   *     trait
   */
  public final void ensureTweakMaskIsSet() {
    if (tweakMask != null) return;

    synchronized (NodeTaskInfo.class) {
      if (tweakMask != null) return;
      if (_matchOn == null) {
        tweakableID = addAndCheckIfOverflowingTweakIDs();
        tweakMask = TPDMask.fromIndex(tweakableID);
      } else {
        var singleTweakID = -1;
        var newMask = new TPDMask();
        for (var parentInfo : _matchOn) {
          if (parentInfo.tweakMask != null) {
            newMask.merge(parentInfo.tweakMask);
            if (singleTweakID == -1) singleTweakID = parentInfo.tweakableID;
            else if (singleTweakID != parentInfo.tweakableID) singleTweakID = -2;
          }
        }
        if (singleTweakID == -1) {
          // No parent had any masks on them
          tweakableID = addAndCheckIfOverflowingTweakIDs();
          tweakMask = TPDMask.fromIndex(tweakableID);
        } else if (singleTweakID > 0) {
          // Only one parent had an ID assigned
          tweakableID = singleTweakID;
          tweakMask = newMask;
        } else {
          // Multiple parents had distinct ids assigned (possible in the upside down triangle case)
          tweakableID = Integer.MAX_VALUE; // It's meaningless
          tweakMask = newMask;
        }

        for (var parentInfo : _matchOn)
          if (parentInfo.tweakMask == null) {
            parentInfo.tweakableID = tweakableID;
            parentInfo.tweakMask = tweakMask;
          }
      }
    }
  }

  /** Not ideal that we expose these methods, might be better to put tests in separateJVMs */
  public final void TEST_ONLY_setAssumeDependsOn(int[] propertyIndexes) {
    if (!isScenarioIndependent()) {
      dependsOnTweakMask = TPDMask.fromArray(TPDMask.indexListToMask(propertyIndexes));
      if (dependsOnTweakMask.empty()) dependsOnTweakMask = TPDMask.empty;
      _cachePolicy = NCPolicy.XSFT;
    }
  }

  public final void TEST_ONLY_clearDependencies() {
    dependsOnTweakMask = TPDMask.empty;
  }

  /* Roughly the same as setTweakMask, but without complaining that we're resetting IDs */
  public final void TEST_ONLY_setTweakableID(int id) {
    tweakableID = id;
    tweakMask = id == 0 ? null : TPDMask.fromIndex(id);
    reserveTweakID(id); // [SEE_RESERVE_TWEAK_ID]
  }

  public static void TEST_ONLY_resetTweakableCounter() {
    _tweakableCounter.set(1);
  }

  private void setDependsOnMask(String mask) {
    if (!isScenarioIndependent()) {
      dependsOnTweakMask = TPDMask.stringDecoded(mask);
    }
  }

  // TODO (OPTIMUS-47353) this is over complicated - refactor to drop all the Options
  private NodeCCache extractCacheFromConfig(CachePolicyConfig config) {
    Option<CacheConfig> cacheConfig = config.cache();
    if (cacheConfig.isEmpty()) return null;

    return cacheConfig.get().getOrInstantiateCache();
  }

  // because getOrElse(null) causes NPE on None.. TODO (OPTIMUS-47353): refactor not to use Options
  private <T> T getConfig(Option<T> maybeConfig) {
    return maybeConfig.isDefined() ? maybeConfig.get() : null;
  }

  // optconf application entry point, only for constructor nodes!
  public void setExternalConfig(ConstructorConfig config) {
    NCPolicy policy = config.cachePolicy();
    if (policy != null) {
      setPolicyExternal(policy);
    } else {
      // this is ok in the else branch because the only valid constructor policy through optconf is
      // DontCache, in which
      // case don't set a custom cache (validation of ConstructorConfig creation will fail)
      NodeCCache cache = extractCacheFromConfig(config);
      if (cache != null) {
        setCustomCacheExternal(cache);
      }
    }
  }

  // main optconf application entry point
  public void setExternalConfig(NodeConfig nodeConfig) {
    requireNonAbstract(false);

    CacheConfig cacheConfig = getConfig(nodeConfig.cacheConfig());
    if (cacheConfig != null) {
      setCustomCacheExternal(cacheConfig.getOrInstantiateCache());
    }

    PropertyConfig config = getConfig(nodeConfig.config());
    if (config != null) {
      setConfig(config);
    }

    PropertyConfig tweakConfig = getConfig(nodeConfig.tweakConfig());
    if (tweakConfig != null) {
      tweakInfo().setConfig(tweakConfig);
    }
  }

  // private, only called from setExternalConfig, caller verifies non-abstract
  private void setConfig(PropertyConfig config) {
    NCPolicy policy = config.cachePolicy();
    if (policy != null) {
      var resolvedPolicy =
          config.scopePath().isDefined() && policy.pathIndependent() == policy
              ? NCPolicy.composite(null, policy, config.scopePath().get())
              : policy;
      setPolicyExternal(resolvedPolicy);
    }

    NodeCCache cache = extractCacheFromConfig(config);
    if (cache != null) {
      setCustomCacheExternal(cache);
    }

    if (config.localSlot().isDefined()) {
      localCacheSlot = config.localSlot().get();
    }

    if (config.gcnative().isDefined()) {
      setHoldsNativeMemory(config.gcnative().get());
    }

    if (config.syncId().isDefined()) {
      setSingleThreaded();
      syncID = config.syncId().get();
    }

    // TODO (OPTIMUS-40508): revisit dependencies from optconf
    if (config.dependsOnMask().isDefined()) {
      setDependsOnMask(
          config.dependsOnMask().get()); // consider merging if nti already collected dependencies
      setPolicyExternal(NCPolicy.XSFT);
    }

    if (config.trackForInvalidation().isDefined()) {
      if (config.trackForInvalidation().get()) {
        markTrackedForInvalidation();
      } else {
        markNotTrackedForInvalidation();
      }
    }
  }

  /**
   * Returns true for nodes that adapter spawns after adapting some other node. NOT used for
   * runtime, just for presentation.
   */
  public boolean isAdapter() {
    return false;
  }

  /**
   * Linearized match list of tweaks (based on inheritance) <br>
   * Used in tweak matcher to find super classes that might have had tweaks applied to them <br>
   * _matchOn[0] is the most immediate parent <br>
   * _matchOn.last toward the most base
   */
  protected NodeTaskInfo[] _matchOn;
  // Need to maintain this list to mark as tweaked all the sub-classes
  private NodeTaskInfo[] _subclasses;

  // Compiler plugin generates the call to setter
  public final NodeTaskInfo[] matchOn() {
    return _matchOn;
  }

  public final NodeTaskInfo[] matchOnChildren() {
    return _subclasses;
  }

  void registerSubclass(NodeTaskInfo subClsTaskInfo) {
    // Multiple threads could be initializing entities and updating the same parent
    NodeTaskInfo[] moc = _subclasses;
    if (!CoreUtils.contains(
        moc, subClsTaskInfo)) { // Consider improving code gen to avoid this case
      NodeTaskInfo[] newMoc =
          (moc == null) ? new NodeTaskInfo[1] : Arrays.copyOf(moc, moc.length + 1);
      newMoc[newMoc.length - 1] = subClsTaskInfo;
      _subclasses = newMoc;
    }
  }

  /**
   * If true, when the property is not-tweaked it is scenario-independent for all invocations.
   *
   * <p>To be scenario-independent, for all invocations (when not directly tweaked (NOT CURRENTLY
   * SUPPORTED)), the function body must: - NOT depend on anything that come from the scenario - NOT
   * call any functions that can access the scenario (including DAL operations that use the
   * Scenario's temporal policy). - NOT depend on any other tweakable properties.
   */
  public final boolean isScenarioIndependent() {
    return isScenarioIndependent(_flags);
  }

  public static boolean isScenarioIndependent(long flags) {
    return (flags & SCENARIOINDEPENDENT) != 0;
  }

  public final boolean isGivenRuntimeEnv() {
    return isGivenRuntimeEnv(_flags);
  }

  public static boolean isGivenRuntimeEnv(long flags) {
    return (flags & GIVEN_RUNTIME_ENV) != 0;
  }

  public final boolean tryLocalCache() {
    return (_flags & LOCAL_CACHE) != 0;
  }

  public final boolean isGroupCached() {
    return (_flags & GROUP_CACHED) != 0;
  }

  public final void markGroupCached() {
    updateFlags(GROUP_CACHED, 0);
    // Currently grouped caching is not supported for XS/XSFT
    _cachePolicy = NCPolicy.forInfo(this, _cachePolicy);
  }

  /** For testing */
  public final void unmarkGroupCached() {
    updateFlags(0, GROUP_CACHED);
  }

  public final void markAsyncNeeded() {
    updateFlags(ASYNC_NEEDED, 0);
  }

  public final boolean isAbstract() {
    return (_flags & IS_ABSTRACT) != 0;
  }

  public final boolean getCacheable() {
    return getCacheable(_flags);
  }

  public static boolean getCacheable(long flags) {
    return (flags & DONT_CACHE) == 0;
  }

  public final boolean getCacheableNode() {
    return (_flags & (DONT_CACHE | NOTNODE)) == 0;
  }

  public final boolean isJob() {
    return (_flags & IS_JOB) != 0;
  }

  /**
   * Note: adjustScenarioStack changes scenario stack and this is what allows to reuse
   * hashOf/matches directly
   */
  public final ScenarioStack adjustScenarioStack(PropertyNode<?> key, ScenarioStack requestingSS) {
    if (isScenarioIndependent()) return requestingSS.asSiStack(key);
    if (isGivenRuntimeEnv()) {
      if (requestingSS.isSelfOrAncestorScenarioIndependent()) {
        if (Settings.throwOnInvalidSIToRuntimeEnvUsage)
          throw new GraphException(
              "Cannot call @givenRuntimeEnv node from @scenarioIndependent node");
        else MonitoringBreadcrumbs$.MODULE$.sendEvilCallFromSINodeToGivenRuntimeEnvNode();
      }

      // @givenRuntimeEnv runs a node in the "initial runtime scenario stack" + our requesting
      // cancellation scope. We do it this way to ensure that a non RT exception in a @gRE @node
      // called inside asyncResult {} doesn't pollute every other invocation of the same node
      // forever.
      return requestingSS
          .initialRuntimeScenarioStack()
          .withCancellationScopeAndCache(
              requestingSS.cancelScope(), requestingSS.cacheSelector().withoutScope());
    }
    return requestingSS;
  }

  /** PUBLIC CACHE CONFIG APIS */
  public final void setCacheable(boolean enable) {
    checkAndSetPolicy("setCacheable", enable ? NCPolicy.Basic : NCPolicy.DontCache);
  }

  public final void setFavorReuse(boolean enable) {
    checkAndSetPolicy("setFavorReuse", enable ? NCPolicy.XS : NCPolicy.Basic);
  }

  public final void setCachePolicy(NCPolicy policy) {
    checkAndSetPolicy("setCachePolicy", policy);
  }

  private void preExternalConfig(ConfiguredBy configuredBy) {
    requireNonAbstract(false); // don't enforce non-abstract classes rule if setting through optconf
    if (configBackup == null) {
      configBackup = new PropertyConfigBackup(_cachePolicy, _ccache, configuredBy);
    }
  }

  /** SET THROUGH OPTCONF */
  private void setPolicyExternal(NCPolicy policy) {
    setPolicyExternal(policy, ConfiguredBy.OPTCONF);
  }

  private boolean setPolicyExternal(NCPolicy policy, ConfiguredBy configuredBy) {
    preExternalConfig(configuredBy);
    configBackup.externallyConfiguredPolicy = isExternallyConfigured(EXTERNALLY_CONFIGURED_POLICY);
    if (configuredBy == ConfiguredBy.OPTCONF)
      markAsExternallyConfigured(EXTERNALLY_CONFIGURED_POLICY);
    return setPolicy(policy);
  }

  private void setCustomCacheExternal(NodeCCache customCache) {
    preExternalConfig(ConfiguredBy.OPTCONF);
    configBackup.externallyConfiguredCustomCache =
        isExternallyConfigured(EXTERNALLY_CONFIGURED_CUSTOM_CACHE);
    markAsExternallyConfigured(EXTERNALLY_CONFIGURED_CUSTOM_CACHE);
    setCache(customCache);
  }

  // return true if policy was actually set
  private boolean setPolicy(NCPolicy policy) {
    if (policy == _cachePolicy) {
      return false;
    }
    if ((isScenarioIndependent() && !policy.appliesToScenarioIndependent())
        || (!isScenarioIndependent() && policy == NCPolicy.SI)
        || (isRecursive() && !policy.appliesToRecursive())) {
      // Too verbose otherwise
      if (Settings.schedulerAsserts) OGScheduler.log.warn("Incompatible NCPolicy, ignoring!");
      return false;
    }

    // set new flag, drop the old one
    var containsXS = policy.contains(NCPolicy.XS);
    var setFlags = (policy.noCache ? DONT_CACHE : 0) | (containsXS ? FAVOR_REUSE : 0);
    var clearFlags = (policy.noCache ? 0 : DONT_CACHE) | (containsXS ? 0 : FAVOR_REUSE);
    updateFlags(setFlags, clearFlags);
    // set policy must be AFTER update flags
    // for composite policies if path independent is not set, retain the old value
    _cachePolicy = policy.withPathIndependentIfNotSet(_cachePolicy.pathIndependent());
    return true;
  }

  private void checkAndSetPolicy(String entryPoint, NCPolicy policy) {
    requireNonAbstract(true);
    // Tweaks are already XSFT by design. Minimum SS is THE given block. Wrapping into proxy is a
    // waste
    if (NAME_MODIFIER_TWEAK.equals(this.modifier()) && policy == NCPolicy.XSFT)
      policy = NCPolicy.Basic;

    if (!warnIfExternallyConfigured(EXTERNALLY_CONFIGURED_POLICY, entryPoint)) {
      setPolicy(policy);
    }

    if (callSiteHolder != null) callSiteHolder.refreshTarget();
  }

  /**
   * throwException will be false if the user is editing cache properties through an optconf file.
   * We currently do not crash even if the user configures properties of an entity or node that does
   * not exist through optconf, so in this case we just log a warning.
   */
  private void requireNonAbstract(boolean throwException) {
    if (isAbstract()) {
      String msg = "Cannot set cache configuration on abstract method: " + this;
      NodeCacheInfo.log().javaLogger().error(msg);

      if (throwException) { // don't throw it yet, though; just crumb (TODO (OPTIMUS-46038):
        // reinstate this as exception)
        var exn = new IllegalArgumentException();
        if (Settings.schedulerAsserts) {
          throw exn;
        }
        Properties.Elems elems =
            Properties.Elems$.MODULE$.apply(
                Properties.type().apply("requireNonAbstract"),
                Properties.name().apply(this.toString()),
                Properties.logMsg().apply(msg),
                Properties.stackTrace().apply(Exceptions.minimizeTrace(exn, 5, 2)),
                Version$.MODULE$.verboseProperties());
        Breadcrumbs.send(
            PropertiesCrumb.apply(ChainedID$.MODULE$.root(), Crumb.rtSource(), elems.m()));
      }
    }
  }

  /** optconf configuration overrides APIs */
  private boolean warnIfExternallyConfigured(long flag, String api) {
    boolean externallyConfigured = isExternallyConfigured(flag);
    if (externallyConfigured) {
      NodeCacheInfo.log()
          .javaLogger()
          .warn("{} for {} ignored because optconf is in effect", api, this);
    }
    return externallyConfigured;
  }

  /**
   * exactly the same as the APIs they wrap, but this exposes them with an uglier name for testing
   */
  public void TEST_ONLY_setPolicyExternal(NCPolicy policy) {
    setPolicyExternal(policy);
  }

  public void TEST_ONLY_undoExternalCacheConfig() {
    undoExternalCacheConfig();
  }

  /** This is an internal API, that is going to be removed. DO NOT USE IT */
  public final NodeTaskInfo ensureNotCacheable() {
    setCacheable(false);
    return this;
  }

  public final void setTrackTweakableByHash(boolean enable) {
    updateFlags(TRACK_TWEAKABLES_BY_HASH, enable);
  }

  public final boolean trackTweakableByHash() {
    return (_flags & TRACK_TWEAKABLES_BY_HASH) != 0;
  }

  // TODO (OPTIMUS-43938): integrate with file to configure how nodes are displayed in debugger
  public final void setIgnoredInProfiler(boolean enable) {
    updateFlags(PROFILER_IGNORE, enable);
  }

  public boolean isIgnoredInProfiler() {
    return (_flags & NodeTaskInfo.PROFILER_IGNORE) == NodeTaskInfo.PROFILER_IGNORE;
  }

  // ensure flag is zero or one of EXTERNALLY_CONFIGRED_* flags or a bitmap of them
  private void checkExternallyConfiguredFlag(long flag) {
    long all = EXTERNALLY_CONFIGURED_CUSTOM_CACHE | EXTERNALLY_CONFIGURED_POLICY;
    if ((flag | all) != all) {
      throw new GraphException("unexpected flag " + flag + " passed in EXTERNALLY_CONFIGURED API");
    }
  }

  @SuppressWarnings("unused") // Note: called externally from heapexp
  private boolean isExternallyConfigured() {
    return isExternallyConfigured(EXTERNALLY_CONFIGURED_CUSTOM_CACHE)
        | isExternallyConfigured(EXTERNALLY_CONFIGURED_POLICY);
  }

  public final boolean hasExternallyConfiguredCustomCache() {
    return isExternallyConfigured(EXTERNALLY_CONFIGURED_CUSTOM_CACHE);
  }

  public final boolean hasExternallyConfiguredPolicy() {
    return isExternallyConfigured(EXTERNALLY_CONFIGURED_POLICY);
  }

  public boolean isExternallyConfigured(long flag) {
    checkExternallyConfiguredFlag(flag);
    return (_flags & flag) != 0;
  }

  private void clearExternallyConfigured(long flag) {
    checkExternallyConfiguredFlag(flag);
    updateFlags(0, flag);
  }

  public void clearExternallyConfigured() {
    clearExternallyConfigured(EXTERNALLY_CONFIGURED);
  }

  /** This is for graph debugger use only, if you use it in your code you will be cursed */
  public final void markAsProfilerUIConfigured() {
    // why clear this? to allow api overrides?
    updateFlags(PROFILER_UI_CONFIGURED, EXTERNALLY_CONFIGURED);
  }

  public final void clearProfilerUIConfigured() {
    updateFlags(0, PROFILER_UI_CONFIGURED);
  }

  public final boolean getHoldsNativeMemory() {
    return (_flags & HOLDS_NATIVE) != 0;
  }

  private void setHoldsNativeMemory(boolean enable) {
    updateFlags(HOLDS_NATIVE, enable);
  }

  public final void markHeapExplored() {
    updateFlags(HEAP_EXPLORED, 0);
  }

  public final boolean isHeapExplored() {
    return (_flags & HEAP_EXPLORED) != 0;
  }

  public final boolean getSingleThreaded() {
    return (_flags & SINGLE_THREADED) != 0;
  }

  private void setSingleThreaded() {
    updateFlags(SINGLE_THREADED, true);
  }

  public final boolean getFavorReuseIsXS() {
    return _cachePolicy.contains(NCPolicy.XS);
  }

  /** The following set of flags support tweaking and optimization around tweaking */
  // Is this property tweakable per-instance?
  public final boolean isTweakable() {
    return (_flags & TWEAKABLE) != 0;
  }

  public final boolean wasTweakedAtLeastOnce() {
    return (_flags & WAS_TWEAKED) != 0;
  }

  public final boolean wasTweakedByInstance() {
    return (_flags & WAS_INSTANCE_TWEAKED) != 0;
  }

  public final boolean wasTweakedByProperty() {
    return (_flags & WAS_PROPERTY_TWEAKED) != 0;
  }

  public final boolean wasTweakedAtLeastOnceInTracker() {
    return (_flags & WAS_TWEAKED_TRACKER) != 0;
  }

  public final boolean wasTweakedByInstanceInTracker() {
    return (_flags & WAS_INSTANCE_TWEAKED_TRACKER) != 0;
  }

  public final boolean wasTweakedByPropertyInTracker() {
    return (_flags & WAS_PROPERTY_TWEAKED_TRACKER) != 0;
  }

  public final void markInstanceTweakedInTracker() {
    updateFlags(WAS_INSTANCE_TWEAKED_TRACKER, 0);
  }

  public final void markPropertyTweakedInTracker() {
    updateFlags(WAS_PROPERTY_TWEAKED_TRACKER, 0);
  }

  public final boolean isExcludedFromRTVNodeRerunner() {
    return (_flags & EXCLUDE_FROM_RTV_NODE_RERUNNER) != 0;
  }

  public final void setExcludeFromRTVNodeRerunner(boolean exclude) {
    if (exclude) updateFlags(EXCLUDE_FROM_RTV_NODE_RERUNNER, 0);
    else updateFlags(0, EXCLUDE_FROM_RTV_NODE_RERUNNER);
  }

  /**
   * Reserves tweak ID (currently) by bumping the counter to at least that value It's possible to
   * just reserve that ID or another strategy would be to reserve that bit Note: It's to load config
   * after some properties are already tweaked, tweak filters are probabilistic Note: really should
   * be private[graph]
   */
  public static void reserveTweakID(int id) {
    while (true) {
      int count = _tweakableCounter.get();
      if (count >= id) {
        break;
      }
      _tweakableCounter.compareAndSet(count, id);
    }
  }

  /**
   * Used in optconf remapping: if optconf specifies twkId for a property that has not yet been
   * registered in this JVM, then that twkId might be smaller than our current _tweakableCounter
   * value. If that's the case we might end up sharing twkId with another tweakable, which could
   * reduce reuse.
   *
   * <p>This returns either the next value, or the id itself (which is OK since we will call
   * reserveTweakID after remapping IDs to make sure counter is bumped to minimum required value)
   */
  public static int nextTweakID(int id) {
    int count = _tweakableCounter.get();
    if (count >= id) return addAndCheckIfOverflowingTweakIDs();
    else return id;
  }

  public final TPDMask tweakMask() {
    return tweakMask;
  }

  public final int tweakableID() {
    return tweakableID;
  }

  public final TPDMask dependsOnTweakMask() {
    return dependsOnTweakMask;
  }

  // check whether actual dependencies are a subset of assumed dependencies
  final void updateDependsOnMask(NodeTask nodeTask) {
    if (getCacheable() && !nodeTask.isTweakPropertyDependencySubsetOf(dependsOnTweakMask)) {
      synchronized (this) {
        // We specifically allocate a new instance to allow easy detection of change
        TPDMask newMask = new TPDMask();
        newMask.merge(dependsOnTweakMask);
        nodeTask.mergeTweakPropertyDependenciesInto(newMask);
        dependsOnTweakMask = newMask;
      }
    }
  }

  /**
   * does not check for cacheability since this is only called from proxies and if you are in a
   * proxy the underlying node has to be cacheable
   */
  public final void updateDependsOnMask(TPDMask mask) {
    if (!mask.subsetOf(dependsOnTweakMask)) {
      synchronized (this) {
        // We specifically allocate a new instance to allow easy detection of
        TPDMask newMask = new TPDMask();
        // change
        newMask.merge(dependsOnTweakMask);
        newMask.merge(mask);
        dependsOnTweakMask = newMask;
      }
    }
  }

  /** Same as markPropertyTweaked but doesn't update the children, since they are already updated */
  protected final void markPropertyTweakedLocal() {
    ensureTweakMaskIsSet();
    updateFlags(WAS_PROPERTY_TWEAKED, 0);
  }

  /**
   * Simple example to consider:
   * <pre>
   * {@code @entity trait B}
   * {@code @entity class E extends B}
   * </pre>
   * Callers:
   * <pre>
   * // E class could be loaded here [1]
   * given(B.f := value) {  // Calls markPropertyTweaked from SSCacheID
   *    // E class could be loaded here [2]
   *    e.f needs to try to match E.f and B.f (walking the _matchOn array)
   * }
   *
   * </pre>
   * Synchronization invariant to maintain: After the exit from this function tweaks apply on ALL subclasses:
   * <pre>
   * {@code @entity trait B
   * @entity class E extends B
   * @entity class D extends E}
   * T1					T2
   * 					val d = D()			// register D on E and B
   * given(B.x){							// starts marking B and D and E
   *                                        // marks B and E
   * 					given(E.x) {        // calls markPropertyTweaked but D is not yet WAS_PROPERTY_TWEAKED!
   * 				                        // can't return yet! so stronger condition for short curcuit to avoid
   * 				                        // synchronized is needed: WAS_MARK_PTWEAKED_CALLED is used to say
   * 				                        // that this specific property was already called markPropertyTweaked on.
   * 					  d.x
   * }
   * </pre>
   * @see NodeTaskInfo#internalSetOn(NodeTaskInfo[])
   * @see NodeTaskInfo#registerSubclass(NodeTaskInfo)
   */
  public final void markPropertyTweaked() {
    ensureTweakMaskIsSet();
    if ((_flags & WAS_MARK_PTWEAKED_CALLED) != 0) return;

    synchronized (NodeTaskInfo.class) {
      if (wasTweakedByProperty()) return;
      // We support tweak inheritance so potentially we are now tweaking all the children
      // NOTE: another thread can modify _subclasses when first loading a new class on a different
      // thread
      if (_subclasses != null) {
        for (NodeTaskInfo subClasses : _subclasses) {
          subClasses.markPropertyTweakedLocal(); // The list is already flat so don't recurse
        }
      }
      updateFlags(WAS_PROPERTY_TWEAKED | WAS_MARK_PTWEAKED_CALLED, 0);
    }
  }

  protected final void internalSetOn(NodeTaskInfo[] parents) {
    _matchOn = parents; // Parents
    synchronized (NodeTaskInfo.class) {
      for (var parent : parents) {
        parent.registerSubclass(this);
        // If parent was already tweaked 'this' must be tweaked already even though it was just
        // loaded
        if (parent.wasTweakedByProperty()) this.markPropertyTweakedLocal();
      }
    }
  }

  public final void markInstanceTweaked() {
    ensureTweakMaskIsSet();
    updateFlags(WAS_INSTANCE_TWEAKED | TWEAKABLE, 0);
  }

  /** Do not use */
  public final void DEBUG_ONLY_markNeverInstanceTweaked() {
    updateFlags(WAS_INSTANCE_TWEAKED, false);
  }

  /** Do not use */
  public final void DEBUG_ONLY_markNeverPropertyTweaked() {
    updateFlags(WAS_PROPERTY_TWEAKED | WAS_MARK_PTWEAKED_CALLED, false);
  }

  public boolean trackForInvalidation() {
    return trackForInvalidation(_flags);
  }

  public static boolean trackForInvalidation(long flags) {
    return (flags & DONT_TRACK_FOR_INVALIDATION) == 0;
  }

  public final void markNotTrackedForInvalidation() {
    if (DiagnosticSettings.diag_showConsole) {
      NodeTrace.untrackedProperties.add(this);
    }
    updateFlags(DONT_TRACK_FOR_INVALIDATION, 0);
  }

  public final void markTrackedForInvalidation() {
    if (DiagnosticSettings.diag_showConsole) {
      NodeTrace.untrackedProperties.remove(this);
    }
    updateFlags(0, DONT_TRACK_FOR_INVALIDATION);
  }

  /**
   * True for properties that when tweaked transform the tweak by calling a ''tweak-handler''
   * `Node.transformTweak` to give the actual tweak(s) to apply.
   */
  public final boolean hasTweakHandler() {
    return (_flags & TWEAKHANDLER) != 0;
  }

  public final boolean isAttributableUserCode() {
    return (_flags & ATTRIBUTABLE_USER_CODE) != 0;
  }

  public final boolean isDirectlyTweakable() {
    return isDirectlyTweakable(_flags);
  }

  public static boolean isDirectlyTweakable(long flags) {
    return (flags & (TWEAKABLE | TWEAKHANDLER)) == TWEAKABLE;
  }

  @Override
  public String toString() {
    return _name + "[" + flagsAsString() + "]";
  }

  private volatile NodeTaskInfo _proxyInfo;
  private volatile NodeTaskInfo _tweakInfo;

  /**
   * Currently used by DelayedProxies in cache. Because it's cached it's critical that DONT_CACHE
   * flag is NOT set! // check uses of flags
   */
  public final NodeTaskInfo proxyInfo() {
    NodeTaskInfo pinfo = _proxyInfo;
    if (pinfo == null) {
      synchronized (this) {
        if (_proxyInfo == null) {
          long proxyFlags = NOTNODE | PROFILER_PROXY | PROFILER_INTERNAL;
          _proxyInfo = new ProxyPropertyInfo(_name, NAME_MODIFIER_PROXY, this, proxyFlags);
        }
        pinfo = _proxyInfo;
      }
    }
    return pinfo;
  }

  public final NodeTaskInfo tweakInfo() {
    NodeTaskInfo pinfo = _tweakInfo;
    if (pinfo == null) {
      synchronized (this) {
        var flags = NOTNODE | (defaultCacheByNameTweaks ? 0 : DONT_CACHE);
        if (_tweakInfo == null)
          _tweakInfo = new ProxyPropertyInfo(_name, NAME_MODIFIER_TWEAK, this, flags);
        pinfo = _tweakInfo;
      }
    }
    return pinfo;
  }

  /** Just for profile/debug tooling */
  public Class<?> runtimeClass() {
    return entityInfo == null ? null : entityInfo.runtimeClass();
  }

  /** Just for profile/debug tooling */
  static final class ProxyPropertyInfo extends NodeTaskInfo {
    private final NodeTaskInfo _source;
    private final String _modifier;

    ProxyPropertyInfo(String name, String modifier, NodeTaskInfo source, long flags) {
      super(name, flags);
      _modifier = modifier;
      _source = source;
    }

    // eventually we should refactor to return underlying name on its own too, but for now let's
    // keep behaviour the same
    @Override
    public String name() {
      return super.name() + _modifier;
    }

    @Override
    public String modifier() {
      return _modifier;
    }

    @Override
    public Class<?> runtimeClass() {
      return _source.runtimeClass();
    }

    @Override
    public PluginType reportingPluginType(ScenarioStack scenarioStack) {
      return _source.reportingPluginType(scenarioStack);
    }
  }

  public final String flagsAsString() {
    return flagsAsString(_flags);
  }

  public static String flagsAsString(long flags) {
    return flagsAsTestAssertions(flags, false);
  }

  public static String flagsAsTestAssertions(long flags, boolean forTest) {
    StringBuilder sb = new StringBuilder();

    if ((flags & TWEAKABLE) != 0) {
      if (!forTest) {
        sb.append("=");
      } else {
        sb.append(" | NodeTaskInfo.TWEAKABLE");
      }
      if ((flags & WAS_INSTANCE_TWEAKED) != 0) {
        if (!forTest) {
          sb.append("i");
        } else {
          sb.append(" | NodeTaskInfo.WAS_INSTANCE_TWEAKED");
        }
      }
      if ((flags & WAS_PROPERTY_TWEAKED) != 0) {
        if (!forTest) {
          sb.append("p");
        } else {
          sb.append(" | NodeTaskInfo.WAS_PROPERTY_TWEAKED");
        }
      }
    }

    if ((flags & PROFILER_IGNORE) != 0 && !forTest) sb.append("w");

    if ((flags & DONT_CACHE) == 0) {
      if (!forTest) {
        sb.append(CachedSuffix);
      }
      if ((flags & FAVOR_REUSE) != 0) {
        if (!forTest) {
          sb.append(XsSuffix);
        } else {
          sb.append(" | NodeTaskInfo.FAVOR_REUSE");
        }
      }
      if ((flags & LOCAL_CACHE) != 0) {
        if (!forTest) {
          sb.append("*");
        } else {
          sb.append(" | NodeTaskInfo.LOCAL_CACHE");
        }
      }
    } else if (forTest) {
      sb.append(" | NodeTaskInfo.DONT_CACHE");
    }

    if ((flags & SCENARIOINDEPENDENT) != 0) {
      if (!forTest) {
        sb.append("SI");
      } else {
        sb.append(" | NodeTaskInfo.SCENARIOINDEPENDENT");
      }
    }

    if ((flags & SINGLE_THREADED) != 0) {
      if (!forTest) {
        sb.append("[sync]");
      } else {
        sb.append(" | NodeTaskInfo.SINGLE_THREADED");
      }
    }
    if ((flags & ASYNC_NEEDED) != 0) {
      if (!forTest) {
        sb.append("a");
      } else {
        sb.append(" | NodeTaskInfo.ASYNC_NEEDED");
      }
    }
    if ((flags & EXTERNALLY_CONFIGURED_CUSTOM_CACHE) != 0
        || (flags & EXTERNALLY_CONFIGURED_POLICY) != 0) {
      if (!forTest) {
        sb.append("@");
      } else {
        sb.append(" | NodeTaskInfo.EXTERNALLY_CONFIGURED_"); // deliberate _
      }
    }
    if ((flags & PROFILER_UI_CONFIGURED) != 0) {
      if (!forTest) {
        sb.append("UI");
      } else {
        sb.append(" | NodeTaskInfo.PROFILER_UI_CONFIGURED");
      }
    }
    if ((flags & AT_MOST_ONE_WAITER) != 0) {
      if (!forTest) {
        sb.append("1");
      } else {
        sb.append(" | NodeTaskInfo.AT_MOST_ONE_WAITER");
      }
    }
    if ((flags & GIVEN_RUNTIME_ENV) != 0) {
      if (!forTest) {
        sb.append("R");
      } else {
        sb.append(" | NodeTaskInfo.GIVEN_RUNTIME_ENV");
      }
    }
    if ((flags & PROFILER_INTERNAL) != 0) {
      if (!forTest) {
        sb.append("g");
      } else {
        sb.append(" | NodeTaskInfo.PROFILER_INTERNAL");
      }
    }
    if ((flags & ATTRIBUTABLE_USER_CODE) != 0) {
      if (!forTest) {
        sb.append("u");
      } else {
        sb.append(" | NodeTaskInfo.ATTRIBUTABLE_USER_CODE");
      }
    }
    if ((flags & PROFILER_PROXY) != 0) {
      if (!forTest) {
        sb.append("~");
      } else {
        sb.append(" | NodeTaskInfo.PROFILER_PROXY");
      }
    }
    if ((flags & SHOULD_LOOKUP_PLUGIN) != 0) {
      if (!forTest) {
        sb.append("+");
      } else {
        sb.append(" | NodeTaskInfo.SHOULD_LOOKUP_PLUGIN");
      }
    }
    return sb.toString();
  }

  public void flagsAsStringVerbose(PrettyStringBuilder sb) {
    flagsAsStringVerbose(_flags, String.valueOf(_cachePolicy), sb);
  }

  public static void flagsAsStringVerbose(long flags, String policy, PrettyStringBuilder sb) {
    if ((flags & TWEAKABLE) != 0) {
      sb.bold("=");
      // redundant as WAS_TWEAKED = WAS_INSTANCE_TWEAKED | WAS_PROPERTY_TWEAKED
      if ((flags & WAS_TWEAKED) != 0) {
        boolean instanceTweaked = (flags & WAS_INSTANCE_TWEAKED) != 0;
        boolean propertyTweaked = (flags & WAS_PROPERTY_TWEAKED) != 0;
        if (instanceTweaked && propertyTweaked) {
          sb.bold("ip").append(" - instance and property tweaked").endln();
        } else {
          if (instanceTweaked) {
            sb.bold("i").append(" - instance tweaked").endln();
          }
          if (propertyTweaked) {
            sb.bold("p").append(" - property tweaked").endln();
          }
        }
      } else {
        sb.append(" - tweakable, but not tweaked").endln();
      }
    }

    if ((flags & PROFILER_IGNORE) != 0) {
      sb.bold("w").append(" - profiler ignore").endln();
    }

    if ((flags & DONT_CACHE) == 0) {
      sb.bold("$").append(" - cacheable").endln();
      if ((flags & FAVOR_REUSE) != 0) {
        sb.bold("x")
            .append(" - favor reuse (aka XS aka Cross Scenario Reuse): ")
            .append(policy)
            .endln();
      }
      if ((flags & LOCAL_CACHE) != 0) {
        sb.bold("*").append(" - local cache").endln();
      }
    }

    if ((flags & SCENARIOINDEPENDENT) != 0) {
      sb.bold("SI").append(" - scenario independent").endln();
    }

    if ((flags & SINGLE_THREADED) != 0) {
      sb.bold("[sync]").append(" - single threaded").endln();
    }
    if ((flags & ASYNC_NEEDED) != 0) {
      sb.bold("a").append(" - async was helpful (aka async needed)").endln();
    }

    if ((flags & EXTERNALLY_CONFIGURED_CUSTOM_CACHE) != 0) {
      sb.bold("@").append(" - externally configured custom cache").endln();
    }
    if ((flags & EXTERNALLY_CONFIGURED_POLICY) != 0) {
      sb.bold("@").append(" - externally configured cache policy").endln();
    }

    if ((flags & PROFILER_UI_CONFIGURED) != 0) {
      sb.bold("UI").append(" - configured in this Profiler UI session").endln();
    }
    if ((flags & AT_MOST_ONE_WAITER) != 0) {
      sb.bold("1").append(" - at most one waiter").endln();
    }
    if ((flags & GIVEN_RUNTIME_ENV) != 0) {
      sb.bold("R").append(" - given initial Runtime environment").endln();
    }
    if ((flags & PROFILER_INTERNAL) != 0) {
      sb.bold("g").append(" - internal graph node").endln();
    }
    if ((flags & ATTRIBUTABLE_USER_CODE) != 0) {
      sb.bold("u")
          .append(" - either [non-entity node] or [node-function] (see NodeTaskInfo Name column)")
          .endln();
    }
    if ((flags & PROFILER_PROXY) != 0) {
      sb.bold("~").append(" - proxy node").endln();
    }
    if ((flags & SHOULD_LOOKUP_PLUGIN) != 0) {
      sb.bold("+").append(" - may have plugin").endln();
    }
  }

  /**
   * isInternal @return true if the node is defined by optimus graph itself Currently used only to
   * improve formatting of some nodes Note: STATE_PROFILER_TRANSPARENT is set on all of them and
   * currently is one to one with isInternal = true This can change in the future
   */
  public final boolean isInternal() {
    return (_flags & PROFILER_INTERNAL) != 0;
  }

  /** Recursive node that can cause cycles for XSFT until tweak dependencies are learned */
  public final boolean isRecursive() {
    return (_flags & IS_RECURSIVE) != 0;
  }

  public final boolean isProxy() {
    return (_flags & PROFILER_PROXY) != 0;
  }

  public final boolean isNotNode() {
    return (_flags & NOTNODE) != 0;
  }
  /**
   * If true and trace mode is 'on' will trace this type of a node and its parents
   * [SEE_PARTIAL_TRACE]
   */
  public final void setTraceSelfAndParent(boolean enable) {
    updateFlags(TRACE_SELF_AND_PARENT, enable);
  }

  /** Included in trace means you want to browse it [SEE_PARTIAL_TRACE] */
  public final boolean isTraceSelfAndParent() {
    // TODO (OPTIMUS-44050, OPTIMUS-43938): review uses and unify behaviour in all observers. Users
    // should configure through config file
    return (_flags & TRACE_SELF_AND_PARENT) != 0;
  }

  public static NodeTaskInfo internal(String name) {
    return internal(name, 0);
  }

  public static NodeTaskInfo internal(String name, long flags) {
    return new NodeTaskInfo(name, flags | PROFILER_INTERNAL | NOTNODE | DONT_CACHE);
  }

  private static SequenceNodeTaskInfo sequence(String name) {
    return new SequenceNodeTaskInfo(name);
  }

  /**
   * Why make this a separate overload? To add this comment! It is critical to think about
   * cacheability of internal nodes. To at least avoid the following bug: TTracks record paths only
   * to 'cached' nodes in order to invalidate them. Nodes that can't be re-used aka DONT_CACHE don't
   * need to be tracked because they won't be 'found' during re-calculations. So if we are to add
   * DONT_CACHE flag to a DelayedPutIfAbsentPropertyProxyNode for example, it wouldn't be
   * invalidated and could be reused.
   *
   * <p>Note: This applies to locally cached values too.
   */
  private static NodeTaskInfo internalCached(String name, long flags) {
    return new NodeTaskInfo(name, flags | PROFILER_INTERNAL | NOTNODE);
  }

  public static NodeTaskInfo userCustomDefined(String name) {
    synchronized (customNodeTaskInfoMap) {
      if (customNodeTaskInfoMap.containsKey(name)) return customNodeTaskInfoMap.get(name);

      if (customNodeTaskInfoMap.size() >= maxCustomFrames)
        return NodeTaskInfo.outOfIndexNodeTaskInfo;

      NodeTaskInfo nodeTaskInfo = new NodeTaskInfo(name, NOTNODE | DONT_CACHE, true);
      customNodeTaskInfoMap.put(name, nodeTaskInfo);
      return nodeTaskInfo;
    }
  }

  /** User derived nodes that share a few executionInfos */
  public static NodeTaskInfo userDefined(String name, long flags) {
    return new NodeTaskInfo(name, flags | NOTNODE | DONT_CACHE);
  }

  /**
   * Anonymous node can reuse this, it's not internal in the sense that external nodes can use it
   */
  public static final NodeTaskInfo Default = userDefined("[default]", 0);

  private static final HashMap<String, NodeTaskInfo> customNodeTaskInfoMap = new HashMap<>();
  private static final NodeTaskInfo outOfIndexNodeTaskInfo =
      userDefined("[OUT_OF_INDEX_CUSTOM_FRAME]", 0);

  public static final NodeTaskInfo NonEntityNode =
      userDefined("[non-entity node]", ATTRIBUTABLE_USER_CODE);
  public static final NodeTaskInfo NonEntityAlwaysUniqueNode =
      userDefined("[non-entity always unique node]", ATTRIBUTABLE_USER_CODE | AT_MOST_ONE_WAITER);
  public static final NodeTaskInfo StoredNodeFunction =
      userDefined("[node-function]", ATTRIBUTABLE_USER_CODE);

  static final NodeTaskInfo EndOfChain = internal("[endOfChain]");
  public static final NodeTaskInfo Delay =
      internal("[delay]").setReportingPluginType(PluginType.apply("Delay"));
  public static final NodeTaskInfo ChanPut =
      internal("[chan.put]").setReportingPluginType(PluginType.apply("Chan"));
  public static final NodeTaskInfo ChanTake =
      internal("[chan.take]").setReportingPluginType(PluginType.apply("Chan"));
  public static final NodeTaskInfo ChanError =
      internal("[chan.error]").setReportingPluginType(PluginType.apply("Chan"));
  public static final NodeTaskInfo ChanSelect =
      internal("[chan.select]").setReportingPluginType(PluginType.apply("Chan"));
  public static final NodeTaskInfo Coro = internal("[coro]");
  public static final NodeTaskInfo Scheduled = internal("[scheduled]");
  public static final NodeTaskInfo DelayOnChildCompleted = internal("[delayOnChildCompleted]");
  public static final NodeTaskInfo Start = internalCached(Names.startName, AT_MOST_ONE_WAITER);
  public static final NodeTaskInfo UITrackDebug =
      internalCached("[uiTrackDebug]", AT_MOST_ONE_WAITER);
  public static final NodeTaskInfo When = internal("[whenClause]");
  public static final NodeTaskInfo Given = internal("[given]", AT_MOST_ONE_WAITER);
  public static final NodeTaskInfo RecursiveTranslate = internal("[recursiveTranslate]");
  public static final SequenceNodeTaskInfo Sequence = sequence("[sequence]");
  public static final SequenceNodeTaskInfo Map = sequence("[map]");
  public static final SequenceNodeTaskInfo Recover = sequence("[recover]");
  public static final SequenceNodeTaskInfo FlatMap = sequence("[flatMap]");
  public static final SequenceNodeTaskInfo ForAll = sequence("[forAll]");
  public static final SequenceNodeTaskInfo ForEach = sequence("[foreach]");
  public static final SequenceNodeTaskInfo Find = sequence("[find]");
  public static final SequenceNodeTaskInfo Exists = sequence("[exists]");
  public static final SequenceNodeTaskInfo Filter = sequence("[filter]");
  public static final SequenceNodeTaskInfo FoldLeft = sequence("[foldLeft]");
  public static final SequenceNodeTaskInfo AsyncIterator = sequence("[asyncIterator]");
  public static final SequenceNodeTaskInfo Accumulate = sequence("[accumulate]");
  public static final SequenceNodeTaskInfo AssociativeReduce = sequence("[associativeReduce]");
  public static final SequenceNodeTaskInfo CommutativeMapReduce =
      sequence("[commutativeMapReduce]");
  public static final SequenceNodeTaskInfo Pipeline = sequence("[pipeline]");
  public static final SequenceNodeTaskInfo Collect = sequence("[collect]");
  public static final SequenceNodeTaskInfo CollectFirst = sequence("[collectFirst]");
  public static final SequenceNodeTaskInfo WithFilter = sequence("[withFilter]");
  public static final SequenceNodeTaskInfo GroupBy = sequence("[groupBy]");
  public static final SequenceNodeTaskInfo Partition = sequence("[partition]");
  public static final SequenceNodeTaskInfo SortBy = sequence("[sortBy]");
  public static final SequenceNodeTaskInfo MinBy = sequence("[minBy]");
  public static final SequenceNodeTaskInfo MaxBy = sequence("[maxBy]");
  public static final SequenceNodeTaskInfo Count = sequence("[count]");
  public static final SequenceNodeTaskInfo Transform = sequence("[transform]");
  public static final SequenceNodeTaskInfo TransformValues = sequence("[transformValues]");
  public static final NodeTaskInfo PartialFunction = internal("[partial_func]");
  public static final NodeTaskInfo OrElse = sequence("[orElse]");
  public static final NodeTaskInfo Marker = internal("[marker]");
  public static final NodeTaskInfo UITrack = internal("[UITrack]");
  public static final NodeTaskInfo Constant = internal("[const]");
  public static final NodeTaskInfo LazyPickledReference = internalCached("[lazyPickled]", NOTNODE);
  public static final NodeTaskInfo SchedulerAsyncCallback =
      internal("[schedulerAsyncCallback]", AT_MOST_ONE_WAITER);
  public static final NodeTaskInfo SchedulerAsyncEvaluate =
      internal("[schedulerAsyncEvaluate]", AT_MOST_ONE_WAITER);
  public static final NodeTaskInfo TweakExpansion = internalCached("[tweakExpansion]", 0);
  public static final NodeTaskInfo TweakLookup = internalCached("[tweakLookup]", 0);
  public static final NodeTaskInfo ConvertByNameToByValue =
      internalCached("[convertByNameToByValue]", 0);
  public static final NodeTaskInfo HandlerResultBackgroundAction =
      internal("HandlerResultBackgroundAction", 0L);
  public static final NodeTaskInfo TweakModifyOriginal = internal("[tweakModifyOriginal]", 0);
  public static final NodeTaskInfo NodeResult = internal("[nodeResult]");
  public static final NodeTaskInfo ProfilerResult = internal("[profilerResult]");
  public static final NodeTaskInfo ThenFinally = internal("[thenFinally]");
  public static final NodeTaskInfo RecoverAndGet = internal("[recoverAndGet]");
  public static final NodeTaskInfo Promise = internal("[promise]");
  public static final NodeTaskInfo Future = internal("[future]");
  public static final NodeTaskInfo AsyncUsing = internal("[asyncUsing]");
  public static final NodeTaskInfo Throttled = internal("[throttled]");
  public static final NodeTaskInfo WitnessReactiveTemporalContext =
      internalCached("[witnessTemporalContext]", 0L);

  public static class Names {
    public static String startName = "[start]";
  }
}
