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

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * WARNING: If adding fields to this file, be aware that it will be loaded with the AGENT if the
 * EntityAgent is loaded via a -javaagent option at run time. This may lead to using an old version
 * of this class if an old version of the EntityAgent is referenced, and consequently runtime errors
 * (usually NoSuchFieldError)
 */
public class DiagnosticSettings {
  // Env variable exported by grid engine launcher script on grid engines
  private static final String IS_ON_ENGINE = "IS_ON_ENGINE"; // 1 = running on grid engine
  private static final String PROFILE_INSTRUMENT = "optimus.profile.instrument";
  private static final String ENABLE_XS_REPORTING = "optimus.profile.xsreporting";
  public static final String TRACE_TWEAKS = "optimus.profile.traceTweaks";
  public static final String CLASS_USAGE_MONITOR = "optimus.monitor.classUsage";
  public static final String CLASS_USAGE_MONITOR_OVERHEAD_TRACE =
      "optimus.monitor.classUsage.overheadTrace";
  private static final String SHOW_TOP_N_CLASS_USAGE = "optimus.monitor.classUsage.showTopN";
  private static final String ENABLE_JUNIT_RUNNER_MONITOR = "optimus.monitor.junit.dynamic";
  // aka --profile-graph
  private static final String PROFILE_PROPERTY = "optimus.scheduler.profile";
  // aka --profile-aggregation
  private static final String PROFILE_AGGREGATION_PROPERTY =
      "optimus.scheduler.profile.aggregation";
  // aka --profile-custom-metrics
  private static final String PROFILE_CUSTOM_FILTER_PROPERTY =
      "optimus.scheduler.profile.filters.custom";

  public static final String DEBUG_PROPERTY = "optimus.scheduler.console"; // console ???
  private static final String EVALUATE_NODE_ON_TOUCH = "optimus.scheduler.evaluateNodeOnTouch";

  public static final String CHAOS_CLASSNAME_PREFIX_PROPERTY =
      "optimus.graph.chaos.classname.prefixes";
  private static final String CHAOS_CLASSNAME_PREFIX_EXCLUDE_PROPERTY =
      "optimus.graph.chaos.classname.prefixes" + ".exclude";
  private static final String COLLECTION_TRACE_ENABLE_PROPERTY = "optimus.graph.collection.trace";

  private static final String RUN_OMAT_ENABLE_PROPERTY = "optimus.run.omat.oom";

  private static final String AGENT_DUMP_CLASS_PREFIX = "optimus.entityagent.dump.";
  private static final String AGENT_DUMP_LOCATION = AGENT_DUMP_CLASS_PREFIX + "location";
  private static final String AGENT_DUMP_CLASSES = AGENT_DUMP_CLASS_PREFIX + "classes";
  private static final String AGENT_BIOPSY_CLASSES = "optimus.entityagent.biopsy.classes";

  private static final String SYNTHETIC_GRAPH_METHODS = "optimus.graph.markGraphMethodsSynthetic";
  private static final String RESET_SS_ON_COMPLETION =
      "optimus.graph.resetScenarioStackOnCompletion";

  private static final String USE_STRICT_MATH = "optimus.strictfp";
  public static final boolean enableJunitRunnerMonitorInjection;
  public static final boolean isClassMonitorEnabled;
  public static final boolean isClassMonitorOverheadTraceEnabled;
  public static final int showThisNumberOfTopUsedClasses;
  public static final boolean keepFullTraceFile;
  public static final String alwaysAppendTraceToOverride; // [SEE_TRACE_TO_OVERRIDE]
  public static final String fullTraceDir;
  public static final boolean
      profileShowThreadSummary; // light profile shows per-thread breakdown at shutdown
  public static final boolean profilerDisableHotspotsCSV;
  public static final boolean profileThreadCPUTime; // light profile includes per-thread CPU time
  public static final boolean profileCacheContention;
  public static final boolean profileSummaryJson;

  /* This is the folder where heapdumps are saved. Defaults to fullTraceDir if not set. */
  public static final String heapProfileDir;
  /* Settings for the heap profile dumps as a list of colon separated key-value pairs. See HeapSampling.scala for details. */
  public static final String heapProfileSettings;

  /** If true entityplugin will inject additional fields into NodeTask. */
  public static boolean traceAvailable; // DO NOT USE IT ANYWHERE YOU WERE THINKING TO USE IT!!!
  /**
   * __profileId, getProfileId, cctor are injected in CompletableNode derived classes to better
   * attribute profile data
   */
  public static final boolean
      injectNodeMethods; // Defaults to traceAvailable, can't be turned off by a "nmi_off" flag

  /** Detect and report any non-RT behaviour */
  public static final boolean enableRTVerifier = getBoolProperty("optimus.rt.verifier", false);

  public static final boolean enableRTVNodeRerunner =
      enableRTVerifier && getBoolProperty("optimus.rt.verifier.node.rerunner", false);
  public static final boolean rtvNodeRerunnerExcludeByDefault =
      enableRTVNodeRerunner
          && getBoolProperty("optimus.rt.verifier.node.rerunner.excludeAll", false);

  public static final boolean rtvNodeRerunnerSkipBadEquality =
      enableRTVNodeRerunner
          && getBoolProperty("optimus.rt.verifier.node.rerunner.skipBadEquality", true);

  /** Publish RT violations as crumbs in splunk */
  public static final boolean publishRTVerifierCrumbs =
      enableRTVerifier && getBoolProperty("optimus.rt.verifier.crumbs", true);
  /** Report RT violations to file */
  public static final boolean writeRTVerifierReport =
      enableRTVerifier && getBoolProperty("optimus.rt.verifier.report", true);

  public static final boolean granularCacheSize =
      getBoolProperty("optimus.graph.timeline.granular.cache", false);

  /**
   * Used for test purposes, it accumulates RT violations in memory even if a report has not been
   * requested
   */
  public static final boolean rtvAccumulateViolations =
      enableRTVerifier && getBoolProperty("optimus.rt.verifier.accumulate.violations", false);

  public static final boolean loomTransformations =
      getBoolProperty("optimus.loom.transformations", LoomDefaults.enabled);

  public static final int loomCompilerLevel = getIntProperty("optimus.loom.compiler.level", 0);

  public static final boolean loomCompilerDebug =
      getBoolProperty("optimus.loom.compiler.debug", false);

  public static final boolean loomCompilerEnqueueEarlier =
      getBoolProperty("optimus.loom.compiler.enqueueEarlier", false);

  public static final boolean loomCompilerQueueSizeSensitive =
      getBoolProperty("optimus.loom.compiler.queueSizeSensitive", false);

  /** Report exactly why a cross-scenario lookup failed (for nodes with favorReuse = true) */
  public static final boolean enableXSReporting;

  /** Collect tweaks we actually depended on */
  public static final boolean traceTweaksEnabled;

  static final boolean internal_traceTweaksOnStartNoDefault;
  public static boolean traceTweaksOverflowDetected;
  /**
   * Used by graph team for testing async overloads (each will be given a unique name in profiling)
   */
  public static final boolean profileOverloads =
      getBoolProperty("optimus.profile.profileOverloads", false);
  /** Used by graph team for testing non-exisiting nodes from optconf in runtime */
  public static final String throwOnOptconfParsingFailureStr =
      "optimus.profile.testNonExistingNodesAtRuntime";

  public static final boolean throwOnOptconfParsingFailure =
      getBoolProperty(throwOnOptconfParsingFailureStr, false);

  // if optimus.profile.testNonExistingNodesAtRuntime is set to true then this sys prop does not do
  // anything
  public static final boolean warnOnOptconfParsingFailure =
      getBoolProperty("optimus.profiler.warnOnOptconfParseError", true);

  // if throwBadSchedulerStateException is true, when a test doesn't clean up scheduler state, it
  // will throw exception
  public static final boolean throwBadSchedulerStateException =
      getBoolProperty("optimus.profiler.throwBadSchedulerStateException", true);

  public static final int tweakUsageQWords = getIntProperty("optimus.graph.tweakUsageQWords", 6);
  // after there is tweakId overflow we will log a warning every time the number of tweaks increases
  // by a multiple of this
  public static final int tweakOverflowAmountToLog =
      getIntProperty("optimus.graph.tweakOverflowAmountToLog", 20);
  public static final String instrumentationConfig = getStringProperty("optimus.instrument.cfg");

  // semi-colon separated list of functions to cache - NOT for production!
  public static final List<String> instrumentationCache =
      getStringPropertyAsList("optimus.instrument.patch.cache");
  public static final boolean resetScenarioStackOnCompletion =
      getBoolProperty(RESET_SS_ON_COMPLETION, true);

  public static final boolean batchScopeTrackNodes =
      getBoolProperty("optimus.batchscope.trackNodes", false);

  /**
   * When set to 0 XSFT is not supported, and basically reverts to a previous version TPD won't be
   * modified by optimus.NodeTaskTransformer at all and _tpd fields will be removed from NodeTask
   * optimus.graph.NodeTask#isTweakPropertyDependencySubsetOf return 'true', this will stop updating
   * any further masks optimus.graph.NodeTask#tweakPropertyDependenciesIntersect return 'true', this
   * will stop looking up min ss optimus.graph.PropertyNode#initTrackingValue() will not
   * ensureTweakMask as the result bits for masks will only be given to the properties that are
   * actually tweaked (much smaller set) and wrap around is less likely to occur
   * [SEE_MASK_SUPPORT_GENERATION]
   */
  public static final boolean enablePerNodeTPDMask = tweakUsageQWords > 0;

  public static final boolean modifyScalaHashCollectionIterationOrder =
      getBoolProperty("optimus.diagnostic.modifyScalaHashCollectionIterationOrder", false);

  public static final int proxyChainStackLimit =
      getIntProperty("optimus.cache.proxyChainStackLimit", 100);
  /**
   * enable to see proxies in wait chains. Generally not for users, Note: not final so it's easy to
   * change in a debugger and see proxies
   */
  public static boolean proxyInWaitChain =
      getBoolProperty("optimus.diagnostic.proxyInWaitChain", false);

  /**
   * if true we capture the cancelled node in the ScopeWasCancelledException, which is expensive
   * (because we can't reuse the same exception for every cancelled node in that scope) but really
   * helpful for diagnosing unexpected cancellations
   */
  public static boolean includeCancelledNodeInException =
      getBoolProperty("optimus.diagnostic.includeCancelledNodeInException", false);

  public static final boolean isWindows =
      System.getProperty("os.name", "").toLowerCase().contains("windows");
  public static final String asyncProfilerSettings;
  public static final boolean awaitStacks;
  public static final boolean sampleCacheLookups;
  public static final boolean sampleCardinalities;

  public static final String
      samplingProfilerArg; // actual value of "optimus.sampling" for logging purposes mostly
  public static final boolean
      samplingProfilerDefaultOn; // default to sampling, even if no zk config
  public static final boolean samplingProfilerStatic; // do static initialization
  public static final boolean samplingProfilerZkConfigurable; // configurable via zk
  public static final boolean
      samplingProfilerAuto; // auto-start (even if it may be turned off later by zk)

  public static boolean samplingAsserts = getBoolProperty("optimus.sampling.asserts", false);

  public static final boolean pluginCounts;

  public static final boolean autoAsyncProfiler;
  public static final boolean repairLauncherChain;
  public static final int awaitChainHashStrategy;

  public static double infoDumpPeriodicityHours =
      getDoubleProperty("optimus.diagnostic.dump.period.hours", 0.0);

  public static int infoDumpEmergencyKillSec =
      getIntProperty("optimus.diagnostic.dump.emergency.kill.sec", 0);
  public static boolean fullHeapDumpOnKill = getBoolProperty("optimus.diagnostic.dump.heap", false);
  public static boolean fakeOutOfMemoryErrorOnKill =
      getBoolProperty("optimus.diagnostic.dump.fake.oom", false);
  public static boolean infoDumpOnShutdown =
      getBoolProperty("optimus.diagnostic.dump.shutdown", false);

  /**
   * For startup profiling (not wiring this through cmd line since application code will never need
   * to use it)
   */
  public static boolean shutdownAfterStartup =
      getBoolProperty("optimus.profile.shutdownAfterStartup", false);

  /** Whether we are running on a Client or on a Grid engine */
  public static final boolean onGrid;

  public static boolean profileRemote;
  public static boolean profileScenarioStackUsage;

  /**
   * Use standalone, offline graph debugger (ie, not attached to any graph process) for loading
   * ogtrace or graphprofile files
   */
  public static boolean offlineReview;

  /**
   * Avoid deleting ogtrace files (this is set in TraceReloaded to allow offline review of traces
   * written to $TEMP)
   */
  public static final boolean keepStaleTraces;

  public static final boolean showEnqueuedNotCompletedNodes =
      getBoolProperty("optimus.graph.showEnqueues", false);

  public static String diag_consoleTitle;
  /** Show GraphDebugger on the first use of graph if enabled */
  public static final boolean diag_showConsole;

  public static final boolean diag_stopOnGraphStart;
  /** Clear registry preferences before loading debugger (in case we messed them up) */
  public static final boolean diag_lustrate;

  public static final String initialProfileAggregation;
  public static final String[] initialProfileCustomFilter;

  public static final boolean debugAssist;

  public static boolean evaluateNodeOnTouch;

  public static final boolean chaosEnabled;
  public static final List<String> chaosClassnamePrefixes;
  public static final List<String> chaosClassnamePrefixExclusions;

  public static final boolean collectionTraceEnabled;

  public static final boolean runOmatEnabled;

  /**
   * If enabled marks methods in optimus.graph.* as synthetic and this allows for nicer step through
   * debugging
   */
  public static final boolean markGraphMethodsAsSynthetic;

  /**
   * If set, forces calls to transcendental functions in {@link Math} to use {@link StrictMath}
   * instead.
   */
  public static final boolean useStrictMath;

  public static final String classDumpLocation;
  public static final Set<String> classDumpClasses;
  public static final boolean classDumpEnabled;
  public static final Set<String> classBiopsyClasses;
  public static final boolean outOfProcess = getBoolProperty("optimus.graph.outOfProcess", false);
  public static final int clientPort = getIntProperty("optimus.graph.clientPort", 7700);
  public static final boolean outOfProcessAppConsole =
      parseConsoleArg(getStringProperty("optimus.graph.outOfProcessAppConsole", ""));

  public static final boolean explainPgoDecision =
      getBoolProperty("optimus.profile.explainPgoDecision", false);

  public static final boolean enableHotCodeReplace =
      getBoolProperty("optimus.graph.enableHotCodeReplace", false);
  // For some applications we don't want to hold references open to jars
  public static final boolean enableHotCodeReplaceAutoClose =
      getBoolProperty("optimus.graph.enableHotCodeReplaceAutoClose", false);
  public static final boolean enableHotCodeReplaceLogging =
      getBoolProperty("optimus.graph.enableHotCodeReplaceLogging", false);

  public static final boolean duplicateNativeAllocations =
      getBoolProperty("optimus.graph.native.duplicate", false);
  public static final boolean captureNativeAllocations =
      duplicateNativeAllocations || getBoolProperty("optimus.graph.native.capture", false);

  public static final boolean detectMemoryBugs =
      getBoolProperty("optimus.graph.native.memcheck", false);
  public static final boolean memoryBugsAreThrown = // or would you rather segfault later?
      getBoolProperty("optimus.graph.native.memcheck.throws", true);

  public static final boolean useDebugCppAgent =
      getBoolProperty("optimus.graph.debugCppAgent", false);

  public static final boolean rewriteDisposable =
      getBoolProperty("optimus.graph.disposable.rewrite", false);
  // these should be set when ensuring the specific SWIG objects are loaded
  public static String disposableInterfaceToRewrite = null;
  public static String disposablePackageToRewrite = null;

  // TODO (OPTIMUS-65703): Remove this option when asSignalEvents is gone.
  public static final boolean inReactiveTestSuites =
      getBoolProperty("optimus.reactive.in.test", false);

  @SuppressWarnings("unused") // Invoked by reflection
  public static List<String> forwardedProperties() {
    List<String> ret = new ArrayList<>();
    ret.add("optimus.graph.detectStalls");
    ret.add("optimus.graph.detectStallTimeoutSecs");
    ret.add("optimus.graph.detectStallAdaptedTimeoutSecs");
    ret.add("optimus.graph.detectStallAdapted");
    ret.add("optimus.graph.detectStallIntervalSecs");
    ret.add("optimus.seq.timings.experimental");
    return ret;
  }

  private static boolean parseConsoleArg(String arg) {
    return arg.equals("stop");
  }

  public static String envOrProp(String arg) {
    String asEnv = arg.toUpperCase().replaceAll("\\.", "_");
    // First look for FOO_BAR_BAZ
    var s = System.getenv(asEnv);
    if (Objects.nonNull(s)) return s;
    // then OPTIMUS_DIST_FOO_BAR_BAZ
    s = System.getenv("OPTIMUS_DIST_" + asEnv);
    if (Objects.nonNull(s)) return s;
    // finall as property foo.bar.baz
    s = System.getProperty(arg);
    return s;
  }

  public static int getIntProperty(String name, int deflt) {
    String p = System.getProperty(name);
    return (p == null) ? deflt : Integer.parseInt(p);
  }

  public static long getLongProperty(String name, long deflt) {
    String p = System.getProperty(name);
    return (p == null) ? deflt : Long.parseLong(p);
  }

  public static double getDoubleProperty(String name, Double deflt) {
    String p = System.getProperty(name);
    return (p == null) ? deflt : Double.parseDouble(p);
  }

  public static boolean parseBooleanWithDefault(String value, boolean deflt) {
    if (value == null) return deflt;
    else if ("1".equals(value) || "true".equals(value)) return true;
    else if ("0".equals(value) || "false".equals(value)) return false;
    else return deflt;
  }

  public static boolean getBoolProperty(String name, boolean deflt) {
    String p = System.getProperty(name);
    return parseBooleanWithDefault(p, deflt);
  }

  public static String getStringProperty(String name) {
    return System.getProperty(name);
  }

  public static String getStringProperty(String name, String deflt) {
    return System.getProperty(name, deflt);
  }

  public static String getStringPropertyOrThrow(String name, String propDesc) {
    String res = System.getProperty(name);
    if (res == null)
      throw new IllegalArgumentException(
          propDesc + " is not defined. Please specify a value for the sysprop " + name);

    return res;
  }

  public static String[] parseStringArrayProperty(String string_, String delim) {
    if (string_ == null) return null;
    else return string_.split(delim);
  }

  public static String[] parseStringArrayProperty(String string_) {
    return parseStringArrayProperty(string_, ";");
  }

  private static Set<String> getStringPropertyAsSet(String name) {
    String prop = getStringProperty(name);
    if (prop == null) return Collections.emptySet();
    String[] asArr = parseStringArrayProperty(prop);
    return Set.of(asArr);
  }

  private static List<String> getStringPropertyAsList(String name) {
    String prop = getStringProperty(name);
    if (prop == null) return Collections.emptyList();
    String[] asArr = parseStringArrayProperty(prop);
    return List.of(asArr);
  }

  public static boolean contains(String[] props, String flag) {
    if (props != null)
      for (String prop : props) {
        if (flag.equals(prop)) return true;
      }
    return false;
  }

  public static String valueOf(String[] props, String flag) {
    if (props != null)
      for (String prop : props) {
        if (prop.startsWith(flag)) return prop.substring(flag.length());
      }
    return null;
  }

  /**
   * Returns jvm arg value if set, null otherwise (name should include -, eg, "-Xmx"). If the arg is
   * a flag without a value, this just returns an empty string, but a non-null return value means
   * the flag is set. Caller should deal with any further parsing
   */
  public static String getJvmArg(String name) {
    RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
    List<String> args = bean.getInputArguments();
    for (String a : args) {
      if (a.startsWith(name)) {
        String withoutArgName = a.substring(name.length());
        if (withoutArgName.startsWith("=")) return withoutArgName.substring(1);
        else return withoutArgName;
      }
    }
    return null;
  }

  /**
   * Note that maxMemory is supposed to return an approximation of available memory, not necessarily
   * configured -Xmx. To get this, use getJvmArg("-Xmx") above
   */
  public static long getMaxHeapMb() {
    Runtime runtime = Runtime.getRuntime();
    int mb = 1024 * 1024;
    return runtime.maxMemory() / mb;
  }

  public static long jvmUpTimeInMs() {
    return ManagementFactory.getRuntimeMXBean().getUptime();
  }

  //
  // Handle all initialization here to control the order and ensure that
  // temporary vars (e.g. profileString) are discarded
  // WARNING: if 'onGrid' is TRUE, then make sure that nothing attempts to
  // open a GUI - will abort on a grid engine
  static {
    onGrid = parseBooleanWithDefault(System.getenv(IS_ON_ENGINE), false);

    {
      // In some cases we do things differently if there is a debug session currently on. Set this
      // to false if you want to debug what actually runs.
      var globalAssist = parseBooleanWithDefault(envOrProp("optimus.debug.assist"), true);
      var jvmHasJdwpAgent = getJvmArg("-agentlib:jdwp") != null;
      debugAssist = globalAssist && jvmHasJdwpAgent;
    }

    String[] graphConsoleArgs = parseStringArrayProperty(System.getProperty(DEBUG_PROPERTY));

    diag_showConsole =
        (!onGrid) && (graphConsoleArgs != null && !contains(graphConsoleArgs, "noconsole"));
    diag_consoleTitle = valueOf(graphConsoleArgs, "title=");
    diag_stopOnGraphStart = contains(graphConsoleArgs, "stop");
    diag_lustrate = contains(graphConsoleArgs, "lustrate");

    initialProfileAggregation = System.getProperty(PROFILE_AGGREGATION_PROPERTY);

    initialProfileCustomFilter =
        parseStringArrayProperty(System.getProperty(PROFILE_CUSTOM_FILTER_PROPERTY), ",");

    evaluateNodeOnTouch = getBoolProperty(EVALUATE_NODE_ON_TOUCH, false);
    profileScenarioStackUsage = contains(graphConsoleArgs, "stackUsageOnStart");

    // Flags control entityagent runtime code injection
    traceAvailable = getBoolProperty(PROFILE_INSTRUMENT, true) || debugAssist;
    injectNodeMethods = traceAvailable && !contains(graphConsoleArgs, "nmi_off");

    enableXSReporting = getBoolProperty(ENABLE_XS_REPORTING, traceAvailable);
    boolean traceNodesOnStartup =
        System.getProperty("sun.java.command").contains("traceNodes")
            || "traceNodes".equalsIgnoreCase(System.getProperty("optimus.scheduler.profile"));
    internal_traceTweaksOnStartNoDefault = getBoolProperty(TRACE_TWEAKS, false);
    traceTweaksEnabled =
        !onGrid && (internal_traceTweaksOnStartNoDefault || debugAssist || traceNodesOnStartup);

    profileShowThreadSummary = getBoolProperty("optimus.profile.showThreadSummary", false);

    profileThreadCPUTime = getBoolProperty("optimus.profile.threadCPUTime", true);

    profileCacheContention = getBoolProperty("optimus.profile.cacheContention", false);
    profileSummaryJson = getBoolProperty("optimus.profile.summaryJson", false);

    enableJunitRunnerMonitorInjection = getBoolProperty(ENABLE_JUNIT_RUNNER_MONITOR, false);

    isClassMonitorEnabled =
        getBoolProperty(CLASS_USAGE_MONITOR, false)
            || parseBooleanWithDefault(System.getenv("OPTIMUS_DIST_CLASS_MONITOR"), false);

    isClassMonitorOverheadTraceEnabled = getBoolProperty(CLASS_USAGE_MONITOR_OVERHEAD_TRACE, false);

    showThisNumberOfTopUsedClasses = getIntProperty(SHOW_TOP_N_CLASS_USAGE, 0);

    useStrictMath = getBoolProperty(USE_STRICT_MATH, false);

    chaosClassnamePrefixes = getStringPropertyAsList(CHAOS_CLASSNAME_PREFIX_PROPERTY);
    chaosClassnamePrefixExclusions =
        getStringPropertyAsList(CHAOS_CLASSNAME_PREFIX_EXCLUDE_PROPERTY);
    chaosEnabled = !chaosClassnamePrefixes.isEmpty();

    // -Doptimus.graph.collection.trace=true to enable the feature
    collectionTraceEnabled = getBoolProperty(COLLECTION_TRACE_ENABLE_PROPERTY, false);

    // -Doptimus.run.omat.on.panic=true to enable the feature
    runOmatEnabled = getBoolProperty(RUN_OMAT_ENABLE_PROPERTY, false);

    classDumpLocation = getStringProperty(AGENT_DUMP_LOCATION, null);
    classDumpClasses = getStringPropertyAsSet(AGENT_DUMP_CLASSES);
    classBiopsyClasses = getStringPropertyAsSet(AGENT_BIOPSY_CLASSES);
    classDumpEnabled = !classDumpClasses.isEmpty() || !classBiopsyClasses.isEmpty();

    // not to be used in the usual case (unless interested in distributedTasks case)
    alwaysAppendTraceToOverride = getStringProperty("optimus.traceTo", null);

    // set this to true to avoid deleting the backing storage for the full trace: this is intended
    // to be useful
    // when debugging the full trace profiler (the user-serviceable full trace is produced by
    // --profile-graph
    // fulltrace))
    keepFullTraceFile = getBoolProperty("optimus.profile.keepFullTraceFile", false);
    // the dir for the live trace file. Defaults to something like
    // C:\\Users\\username\\AppData\\Local\\Temp\\ogtrace
    String defaultTraceDir =
        System.getProperty("java.io.tmpdir")
            + File.separator
            + "ogtrace-"
            + System.getProperty("user.name");
    fullTraceDir = getStringProperty("optimus.profiler.fullTraceDir", defaultTraceDir);

    {
      var hpd = getStringProperty("optimus.graph.heap.profiler.dir");
      if (hpd == null) hpd = System.getenv("HEAP_PROFILER_DIR");
      if (hpd == null) hpd = fullTraceDir;
      heapProfileDir = hpd;
    }

    {
      var hpf = getStringProperty("optimus.graph.heap.profiler");
      if (hpf == null) hpf = System.getenv("HEAP_PROFILER_SETTINGS");
      heapProfileSettings = hpf;
    }

    // for the case where all you want is ogtrace/optconf and not the hotspots CSV. It can also be
    // suppressed using
    // filtesr, but this is simpler
    profilerDisableHotspotsCSV = getBoolProperty("optimus.profiler.disableHotspotsCSV", false);

    keepStaleTraces = getBoolProperty("optimus.profiler.keepStaleTraces", false);

    {
      // Look for async-profiler settings with old and new properties.
      var aps = envOrProp("optimus.graph.async.profiler");
      if (Objects.isNull(aps)) aps = envOrProp("async.profiler.settings");
      asyncProfilerSettings = aps;
      autoAsyncProfiler = Objects.nonNull(aps) && aps.contains("auto=true");
    }

    {
      boolean configurable;
      try {
        Class.forName("optimus.graph.diagnostics.sampling.SamplingProfilerSwitch");
        configurable = true;
      } catch (ClassNotFoundException e) {
        configurable = false;
      }
      samplingProfilerArg = envOrProp("optimus.sampling");
      if (autoAsyncProfiler
          || samplingProfilerArg == null
          || samplingProfilerArg.equals("false")
          || (!configurable && samplingProfilerArg.equals("config")))
        samplingProfilerZkConfigurable =
            samplingProfilerAuto = samplingProfilerStatic = samplingProfilerDefaultOn = false;
      else if (samplingProfilerArg.equals("manual")) {
        samplingProfilerStatic = true;
        samplingProfilerAuto = samplingProfilerZkConfigurable = samplingProfilerDefaultOn = false;
      } else {
        samplingProfilerStatic = samplingProfilerAuto = true;
        if (samplingProfilerArg.equals("force")) {
          samplingProfilerDefaultOn = true;
          samplingProfilerZkConfigurable = false;
        } else {
          samplingProfilerZkConfigurable = true;
          if (samplingProfilerArg.equals("config")) {
            samplingProfilerDefaultOn = false;
          } else if (samplingProfilerArg.equals("true")) {
            samplingProfilerDefaultOn = true;
          } else throw new IllegalArgumentException("sampling.profiler=false|true|force|config");
        }
      }
    }

    // sampleCacheLookups defaults to false because it can sometimes leak memory.
    sampleCacheLookups = parseBooleanWithDefault(envOrProp("optimus.sampling.cachelookups"), true);

    sampleCardinalities =
        samplingProfilerStatic
            && parseBooleanWithDefault(envOrProp("optimus.sampling.cardinality"), false);
    pluginCounts = samplingProfilerStatic || getBoolProperty("optimus.plugin.counts", false);
    awaitStacks =
        // Default on if sampling on
        parseBooleanWithDefault(envOrProp("optimus.await.stacks"), samplingProfilerStatic)
            // or turn on explicitly via AP settings
            || (asyncProfilerSettings != null && asyncProfilerSettings.contains("await=true")
                // or when jdwp is on
                || debugAssist);
    repairLauncherChain =
        getBoolProperty("optimus.graph.enqueue.repair", samplingProfilerStatic || awaitStacks);
    awaitChainHashStrategy = getIntProperty("optimus.graph.enqueue.hash.strategy", 0);
    markGraphMethodsAsSynthetic = getBoolProperty(SYNTHETIC_GRAPH_METHODS, debugAssist);
  }
}
