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
import java.util.Set;

/**
 * WARNING: If adding fields to this file, be aware that it will be loaded
 * with the AGENT if the EntityAgent is loaded via a -javaagent
 * option at run time. This may lead to using an old version of
 * this class if an old version of the EntityAgent is referenced,
 * and consequently runtime errors (usually NoSuchFieldError)
 */
public class DiagnosticSettings {
  // Env variable exported by grid engine launcher script on grid engines
  private final static String IS_ON_ENGINE = "IS_ON_ENGINE"; // 1 = running on grid engine

  private final static String PROFILE_INSTRUMENT = "optimus.profile.instrument";
  private final static String ENABLE_XS_REPORTING = "optimus.profile.xsreporting";
  public final static String TRACE_TWEAKS = "optimus.profile.traceTweaks";
  public final static String CLASS_USAGE_MONITOR = "optimus.monitor.classUsage";
  private final static String SHOW_TOP_N_CLASS_USAGE = "optimus.monitor.classUsage.showTopN";
  private final static String ENABLE_JUNIT_RUNNER_MONITOR = "optimus.monitor.junit.dynamic";

  private final static String PROFILE_LVL_TEMPORAL_SURFACE = "optimus.scheduler.profile.temporalsurface"; // aka --profile-temporal-surface
  private final static String PROFILE_TS_FOLDER_PROPERTY = "optimus.scheduler.profile.temporalsurface.folder"; // aka --profile-ts-folder

  private final static String PROFILE_PROPERTY = "optimus.scheduler.profile"; // aka --profile-graph
  private final static String PROFILE_FOLDER_PROPERTY = "optimus.scheduler.profile.folder"; // aka --profile-csvfolder
  private final static String PROFILE_AGGREGATION_PROPERTY = "optimus.scheduler.profile.aggregation"; // aka --profile-aggregation
  private final static String PROFILE_CUSTOM_FILTER_PROPERTY = "optimus.scheduler.profile.filters.custom"; // aka --profile-custom-metrics
  private final static String PROFILE_HOTSPOTS_FILTER_PROPERTY = "optimus.scheduler.profile.filters.hotspots"; // aka --profile-hotspot-filters

  public final static String DEBUG_PROPERTY = "optimus.scheduler.console"; // console ???
  private final static String EVALUATE_NODE_ON_TOUCH = "optimus.scheduler.evaluateNodeOnTouch";

  public final static String CHAOS_CLASSNAME_PREFIX_PROPERTY = "optimus.graph.chaos.classname.prefixes";
  private final static String CHAOS_CLASSNAME_PREFIX_EXCLUDE_PROPERTY = "optimus.graph.chaos.classname.prefixes" +
                                                                        ".exclude";
  private final static String COLLECTION_TRACE_ENABLE_PROPERTY = "optimus.graph.collection.trace";

  private final static String AGENT_DUMP_CLASS_PREFIX = "optimus.entityagent.dump.";
  private final static String AGENT_DUMP_LOCATION = AGENT_DUMP_CLASS_PREFIX + "location";
  private final static String AGENT_DUMP_CLASSES = AGENT_DUMP_CLASS_PREFIX + "classes";
  private final static String AGENT_BIOPSY_CLASSES = "optimus.entityagent.biopsy.classes";

  private final static String SYNTHETIC_GRAPH_METHODS = "optimus.graph.markGraphMethodsSynthetic";
  private final static String RESET_SS_ON_COMPLETION = "optimus.graph.resetScenarioStackOnCompletion";

  private final static String USE_STRICT_MATH = "optimus.strictfp";

  final public static boolean enableJunitRunnerMonitorInjection;
  final public static boolean isClassMonitorEnabled;
  final public static int showThisNumberOfTopUsedClasses;
  final public static boolean keepFullTraceFile;
  final public static String fullTraceDir;
  final public static boolean profileShowThreadSummary; // light profile shows per-thread breakdown at shutdown
  final public static boolean profilerDisableHotspotsCSV;
  final public static boolean profileThreadCPUTime; // light profile includes per-thread CPU time
  final public static boolean profileCacheContention;
  final public static boolean profileSummaryJson;

  /**
   * If true entityplugin will inject additional fields into NodeTask.
   */
  public static boolean traceAvailable;           // DO NOT USE IT ANYWHERE YOU WERE THINKING TO USE IT!!!
  /**
   * __profileId, getProfileId, cctor are injected in CompletableNode derived classes to better attribute profile data
   */
  final public static boolean injectNodeMethods;  // Defaults to traceAvailable, can't be turned off by a "nmi_off" flag
  /**
   * Report exactly why a cross-scenario lookup failed (for nodes with favorReuse = true)
   */
  final public static boolean enableXSReporting;
  /**
   * Collect tweaks we actually depended on
   */
  final public static boolean traceTweaksEnabled;
  final public static boolean traceTweaksOnStart;
  public static boolean traceTweaksOverflowDetected;
  /**
   * Used by graph team for testing async overloads (each will be given a unique name in profiling)
   */
  final public static boolean profileOverloads = getBoolProperty("optimus.profile.profileOverloads", false);
  /**
   * Used by graph team for testing non-exisiting nodes from optconf in runtime
   */
  final public static String throwOnOptconfParsingFailureStr = "optimus.profile.testNonExistingNodesAtRuntime";
  final public static boolean throwOnOptconfParsingFailure = getBoolProperty(throwOnOptconfParsingFailureStr, false);

  public final static String XSFT_PROPERTY = "optimus.profile.enableXSFT";
  /**
   * Not final, can be configured through command line.
   * Note: volatile in case it changes during a DistClientInfo snapshot. Writing happens in NodeTrace under lock.
   */
  public volatile static boolean enableXSFT = getBoolProperty(XSFT_PROPERTY, false);
  final public static int tweakUsageQWords = getIntProperty("optimus.graph.tweakUsageQWords", 6);

  final public static String instrumentationConfig = getStringProperty("optimus.instrument.cfg");

  // semi-colon separated list of functions to cache - NOT for production!
  final public static List<String> instrumentationCache = getStringPropertyAsList("optimus.instrument.patch.cache");
  final public static boolean resetScenarioStackOnCompletion = getBoolProperty(RESET_SS_ON_COMPLETION, true);

  final public static boolean batchScopeTrackNodes = getBoolProperty("optimus.batchscope.trackNodes", false);

  /**
   * When set to 0 XSFT is not supported, and basically reverts to a previous version
   * TPD won't be modified by optimus.NodeTaskTransformer at all and _tpd fields will be removed from NodeTask
   * optimus.graph.NodeTask#isTweakPropertyDependencySubsetOf return 'true', this will stop updating any further masks
   * optimus.graph.NodeTask#tweakPropertyDependenciesIntersect return 'true', this will stop looking up min ss
   * optimus.graph.PropertyNode#initTrackingValue() will not ensureTweakMask as the result bits for masks will only be
   * given to the properties that are actually tweaked (much smaller set) and wrap around is less likely to occur
   * [SEE_MASK_SUPPORT_GENERATION]
   *  */
  final public static boolean enablePerNodeTPDMask = tweakUsageQWords > 0;

  final public static int proxyChainStackLimit = getIntProperty("optimus.cache.proxyChainStackLimit", 100);
  /** enable to see proxies in wait chains. Generally not for users,
   * Note: not final so it's easy to change in a debugger and see proxies
   */
  public static boolean proxyInWaitChain = getBoolProperty("optimus.diagnostic.proxyInWaitChain", false);

  final public static boolean awaitStacks =  getBoolProperty("optimus.graph.async.profiler.awaitStacks", false);

  public static double infoDumpPeriodicityHours = getDoubleProperty("optimus.diagnostic.dump.period.hours", 0.0);
  public static boolean fullHeapDumpOnKill = getBoolProperty("optimus.diagnostic.dump.heap", false);
  public static String infoDumpDir;
  public static boolean infoDumpOnShutdown = getBoolProperty("optimus.diagnostic.dump.shutdown", false);

  /**
   * Whether we are running on a Client or on a Grid engine
   */
  final public static boolean onGrid;

  public static boolean profileRemote;
  public static boolean profileScenarioStackUsage;

  /**
   * Use standalone, offline graph debugger (ie, not attached to any graph process) for loading ogtrace or
   * graphprofile files
   */
  public static boolean offlineReview;

  /**
   * Avoid deleting ogtrace files (this is set in TraceReloaded to allow offline review of traces written to $TEMP)
   */
  final public static boolean keepStaleTraces;

  /**
   * Used for async stack traces.
   */
  final public static boolean traceEnqueuer;
  final public static String TRACE_ENQUEUER = "optimus.graph.traceEnqueuer";

  final public static boolean isAsyncStackTracesEnabled;

  final public static boolean showEnqueuedNotCompletedNodes = getBoolProperty("optimus.graph.showEnqueues", false);

  public static String diag_consoleTitle;
  /**
   * Show GraphDebugger on the first use of graph if enabled
   */
  public final static boolean diag_showConsole;
  public final static boolean diag_stopOnGraphStart;
  /**
   * Clear registry preferences before loading debugger (in case we messed them up)
   */
  public final static boolean diag_lustrate;

  public final static String initialProfile;
  public final static String initialProfileFolder;
  public final static String initialProfileAggregation;
  public final static String[] initialProfileCustomFilter;
  public final static String[] initialProfileHotspotFilter;
  public final static String initialTSProfile;
  public final static String initialTSProfileFolder;
  public final static boolean jvmDebugging;
  public final static boolean evaluateNodeOnTouch;

  public final static boolean chaosEnabled;
  public final static List<String> chaosClassnamePrefixes;
  public final static List<String> chaosClassnamePrefixExclusions;

  public final static boolean collectionTraceEnabled;

  /** If enabled marks methods in optimus.graph.* as synthetic and this allows for nicer step through debugging */
  public final static boolean syntheticGraphMethodsEnabled;

  /** If set, forces calls to transcendental functions in {@link Math} to use {@link StrictMath} instead. */
  public final static boolean useStrictMath;

  public final static String classDumpLocation;
  public final static Set<String> classDumpClasses;
  public final static boolean classDumpEnabled;
  public final static Set<String> classBiopsyClasses;
  public final static boolean outOfProcess = getBoolProperty("optimus.graph.outOfProcess", false);
  public final static int clientPort = getIntProperty("optimus.graph.clientPort", 7700);
  public final static boolean outOfProcessAppConsole = parseConsoleArg(getStringProperty("optimus.graph.outOfProcessAppConsole",
                                                                                         ""));

  public final static boolean sequenceTiming = getBoolProperty("optimus.seq.timings.capture", false);
  public final static boolean sequenceTimingExperimental = getBoolProperty("optimus.seq.timings.experimental", false);
  public final static boolean explainPgoDecision = getBoolProperty("optimus.profile.explainPgoDecision", false);

  public final static boolean enableHotCodeReplace = getBoolProperty("optimus.graph.enableHotCodeReplace", false);
  // For some applications we don't want to hold references open to jars
  public final static boolean enableHotCodeReplaceAutoClose = getBoolProperty("optimus.graph.enableHotCodeReplaceAutoClose",
                                                                              false);
  public final static boolean enableHotCodeReplaceLogging = getBoolProperty("optimus.graph.enableHotCodeReplaceLogging",
                                                                            false);

  public final static boolean duplicateNativeAllocations =
      getBoolProperty("optimus.graph.native.duplicate", false);
  public final static boolean captureNativeAllocations = duplicateNativeAllocations ||  getBoolProperty("optimus.graph.native.capture", false);

  public final static boolean useDebugCppAgent = getBoolProperty("optimus.graph.debugCppAgent", false);

  @SuppressWarnings("unused")   // Invoked by reflection
  public static List<String> forwardedProperties() {
    List<String> ret = new ArrayList<>();
    ret.add("optimus.graph.detectStalls");
    ret.add("optimus.graph.detectStallTimeoutSecs");
    ret.add("optimus.graph.detectStallAdaptedTimeoutSecs");
    ret.add("optimus.graph.detectStallAdapted");
    ret.add("optimus.graph.detectStallIntervalSecs");
    ret.add("optimus.seq.timings.capture");
    ret.add("optimus.seq.timings.experimental");
    return ret;
  }

  private static boolean parseConsoleArg(String arg) { return arg.equals("stop"); }

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

  private static boolean parseBooleanWithDefault(String value, boolean deflt) {
    if (value == null)
      return deflt;
    else if ("1".equals(value) || "true".equals(value))
      return true;
    else if ("0".equals(value) || "false".equals(value))
      return false;
    else
      return deflt;
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
      throw new IllegalArgumentException(propDesc + " is not defined. Please specify a value for the sysprop " + name);

    return res;
  }

  public static String[] parseStringArrayProperty(String string_) {
    if (string_ == null)
      return null;
    else
      return string_.split(";");
  }

  private static Set<String> getStringPropertyAsSet(String name) {
    String prop = getStringProperty(name);
    if (prop == null)
      return Collections.emptySet();
    String[] asArr = parseStringArrayProperty(prop);
    return Set.of(asArr);
  }

  private static List<String> getStringPropertyAsList(String name) {
    String prop = getStringProperty(name);
    if (prop == null)
      return Collections.emptyList();
    String[] asArr = parseStringArrayProperty(prop);
    return List.of(asArr);
  }

  public static boolean contains(String[] props, String flag) {
    if (props != null)
      for (String prop : props) {
        if (flag.equals(prop))
          return true;
    }
    return false;
  }

  public static String valueOf(String[] props, String flag) {
    if (props != null)
      for (String prop : props) {
        if (prop.startsWith(flag))
          return prop.substring(flag.length());
    }
    return null;
  }

  private static String getInitialProfile(String profileString, String[] graphConsoleArgs) {
    // normal use
    String result = profileString;
    // legacy use
    // profile=1 means hotspots
    // profile=true means hotspots
    if ("1".equals(profileString) || "true".equals(profileString))
      result = "hotspots";
    // console=traceOnStart means traceNodes even if profile= is set to something else
    // (it used to be that console=traceOnStart profile=true was required)
    if (contains(graphConsoleArgs, "traceOnStart"))
      result = "traceNodes";
    return result;
  }

  /**
   * Returns jvm arg value if set, null otherwise (name should include -, eg, "-Xmx"). If the arg is a flag without a
   * value, this just returns an empty string, but a non-null return value means the flag is set.
   * Caller should deal with any further parsing
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
   * Note that maxMemory is supposed to return an approximation of available memory, not necessarily configured -Xmx.
   * To get this, use getJvmArg("-Xmx") above
   */
  public static long getMaxHeapMb() {
    Runtime runtime = Runtime.getRuntime();
    int mb = 1024 * 1024;
    return runtime.maxMemory() / mb;
  }

  public static long jvmUpTimeInMs() { return ManagementFactory.getRuntimeMXBean().getUptime(); }

  private static boolean existsInCmdLine(String key) {
    String cmdLine = System.getProperty("sun.java.command");
    return cmdLine.contains(key);
  }

  //
  // Handle all initialization here to control the order and ensure that
  // temporary vars (e.g. profileString) are discarded
  // WARNING: if 'onGrid' is TRUE, then make sure that nothing attempts to
  // open a GUI - will abort on a grid engine
  static {
    onGrid = parseBooleanWithDefault(System.getenv(IS_ON_ENGINE), false);
    jvmDebugging = getJvmArg("-agentlib:jdwp") != null;

    String profileString = System.getProperty(PROFILE_PROPERTY);
    String[] graphConsoleArgs = parseStringArrayProperty(System.getProperty(DEBUG_PROPERTY));

    diag_showConsole = (!onGrid) && (graphConsoleArgs != null && !contains(graphConsoleArgs, "noconsole"));
    diag_consoleTitle = valueOf(graphConsoleArgs, "title=");
    diag_stopOnGraphStart = contains(graphConsoleArgs, "stop");
    diag_lustrate = contains(graphConsoleArgs, "lustrate");

    initialTSProfile = System.getProperty(PROFILE_LVL_TEMPORAL_SURFACE);
    initialTSProfileFolder = System.getProperty(PROFILE_TS_FOLDER_PROPERTY);
    initialProfile = getInitialProfile(profileString, graphConsoleArgs);
    initialProfileFolder = System.getProperty(PROFILE_FOLDER_PROPERTY);
    initialProfileAggregation = System.getProperty(PROFILE_AGGREGATION_PROPERTY);
    initialProfileCustomFilter = parseStringArrayProperty(System.getProperty(PROFILE_CUSTOM_FILTER_PROPERTY));
    initialProfileHotspotFilter = parseStringArrayProperty(System.getProperty(PROFILE_HOTSPOTS_FILTER_PROPERTY));

    evaluateNodeOnTouch = getBoolProperty(EVALUATE_NODE_ON_TOUCH, false);
    profileScenarioStackUsage = contains(graphConsoleArgs, "stackUsageOnStart");

    // Flags control entityagent runtime code injection
    traceAvailable = getBoolProperty(PROFILE_INSTRUMENT, true) || jvmDebugging;
    injectNodeMethods = traceAvailable && !contains(graphConsoleArgs, "nmi_off");

    boolean traceNodesOnStartup = existsInCmdLine("traceNodes") || "traceNodes".equalsIgnoreCase(initialProfile);
    enableXSReporting = getBoolProperty(ENABLE_XS_REPORTING, traceAvailable);
    traceTweaksOnStart = getBoolProperty(TRACE_TWEAKS, traceNodesOnStartup);
    traceTweaksEnabled = traceTweaksOnStart || jvmDebugging;

    profileShowThreadSummary = getBoolProperty("optimus.profile.showThreadSummary", false);

    profileThreadCPUTime = getBoolProperty("optimus.profile.threadCPUTime", true);

    profileCacheContention = getBoolProperty("optimus.profile.cacheContention", false);
    profileSummaryJson = getBoolProperty("optimus.profile.summaryJson", false);

    enableJunitRunnerMonitorInjection = getBoolProperty(ENABLE_JUNIT_RUNNER_MONITOR, false);

    isClassMonitorEnabled = getBoolProperty(CLASS_USAGE_MONITOR, false) || parseBooleanWithDefault(System.getenv(
        "OPTIMUS_DIST_CLASS_MONITOR"), false);

    showThisNumberOfTopUsedClasses = getIntProperty(SHOW_TOP_N_CLASS_USAGE, 0);

    useStrictMath = getBoolProperty(USE_STRICT_MATH, false);

    chaosClassnamePrefixes = getStringPropertyAsList(CHAOS_CLASSNAME_PREFIX_PROPERTY);
    chaosClassnamePrefixExclusions = getStringPropertyAsList(CHAOS_CLASSNAME_PREFIX_EXCLUDE_PROPERTY);
    chaosEnabled = !chaosClassnamePrefixes.isEmpty();

    // -Doptimus.graph.collection.trace=true to enable the feature
    collectionTraceEnabled = getBoolProperty(COLLECTION_TRACE_ENABLE_PROPERTY, false);

    classDumpLocation = getStringProperty(AGENT_DUMP_LOCATION, null);
    classDumpClasses = getStringPropertyAsSet(AGENT_DUMP_CLASSES);
    classBiopsyClasses = getStringPropertyAsSet(AGENT_BIOPSY_CLASSES);
    classDumpEnabled = !classDumpClasses.isEmpty() || !classBiopsyClasses.isEmpty();

    // set this to true to avoid deleting the backing storage for the full trace: this is intended to be useful
    // when debugging the full trace profiler (the user-serviceable full trace is produced by --profile-graph
    // fulltrace))
    keepFullTraceFile = getBoolProperty("optimus.profile.keepFullTraceFile", false);
    // the dir for the live trace file. Defaults to something like C:\\Users\\username\\AppData\\Local\\Temp\\ogtrace
    String defaultTraceDir = System.getProperty("java.io.tmpdir") + File.separator + "ogtrace-" + System.getProperty("user.name");
    fullTraceDir = getStringProperty("optimus.profiler.fullTraceDir", defaultTraceDir);
    // for the case where all you want is ogtrace/optconf and not the hotspots CSV. It can also be suppressed using
    // filtesr, but this is simpler
    profilerDisableHotspotsCSV = getBoolProperty("optimus.profiler.disableHotspotsCSV", false);

    keepStaleTraces = getBoolProperty("optimus.profiler.keepStaleTraces", false);

    infoDumpDir = getStringProperty("optimus.diagnostic.dump.dir");
    if(infoDumpDir == null)
      infoDumpDir = System.getenv("DIAGNOSTIC_DUMP_DIR");
    if(infoDumpDir == null)
      infoDumpDir = System.getProperty("java.io.tmpdir");

    traceEnqueuer = getBoolProperty(TRACE_ENQUEUER, jvmDebugging || awaitStacks);
    syntheticGraphMethodsEnabled = getBoolProperty(SYNTHETIC_GRAPH_METHODS, jvmDebugging || awaitStacks);
    isAsyncStackTracesEnabled = traceEnqueuer && syntheticGraphMethodsEnabled;
  }
}
