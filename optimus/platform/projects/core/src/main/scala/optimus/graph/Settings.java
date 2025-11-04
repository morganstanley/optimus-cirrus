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

import static optimus.graph.DiagnosticSettings.debugAssist;
import static optimus.graph.DiagnosticSettings.getBoolProperty;
import static optimus.graph.DiagnosticSettings.getDoubleProperty;
import static optimus.graph.DiagnosticSettings.getIntProperty;
import static optimus.graph.DiagnosticSettings.getLongProperty;
import static optimus.graph.DiagnosticSettings.getStringProperty;
import java.io.File;
import java.io.FileWriter;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.ExportException;
import java.util.Set;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import optimus.platform.dal.EntityResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Static settings that should be passed in as JVM args
public class Settings {
  private static final Logger log = LoggerFactory.getLogger(Settings.class);

  public static final boolean kludgeBind2BindOnce =
      getBoolProperty("optimus.compat.bind2BindOnce", false);

  /** Added 1/5/2017 backward compatible with the original design */
  // TODO (OPTIMUS-15699): flip it for everyone in main
  public static boolean throwOnDuplicateInstanceTweaks =
      getBoolProperty("optimus.compat.throwOnDuplicate", false);

  // TODO (OPTIMUS-77836): Enable this flag once legacy persist and hybrid entities have been
  // removed
  public static boolean compatAllowCrossRuntimeCacheHits =
      getBoolProperty("optimus.compat.allowCrossRuntimeCacheHits", false);

  public static boolean scopeXSCache = getBoolProperty("optimus.cache.scopeXS", true);
  public static boolean scopedCachesReducesGlobalSize =
      getBoolProperty("optimus.cache.scopedCachesReducesGlobalSize", scopeXSCache);

  public static final String ShowDuplicateInstanceTweakValuesProp =
      "optimus.graph.showDuplicateInstanceTweakValues";
  public static boolean showDuplicateInstanceTweakValues =
      getBoolProperty(ShowDuplicateInstanceTweakValuesProp, false);

  /** Added 10/3/2017 backward compatible with the original design when false */
  public static boolean throwOnInvalidInitRuntimeEnvUsage =
      getBoolProperty("optimus.compat.throwOnInvalidInitRuntimeEnvUsage", false);

  public static boolean throwOnInvalidSIToRuntimeEnvUsage =
      getBoolProperty("optimus.compat.throwOnInvalidSIToRuntimeEnvUsage", true);

  public static boolean perAppletProfile = getBoolProperty("optimus.graph.perAppletProfile", false);

  public static final boolean convertByNameToByValue =
      getBoolProperty("optimus.compat.byName2byValue", true);

  public static final boolean removeRedundantTweaks =
      getBoolProperty("optimus.graph.removeRedundantTweaks", true);

  /**
   * Cache Scenario.equals call to reduce expensive equality calculations for big scenarios used
   * over and over again.
   */
  public static final boolean cacheScenarioEqualities =
      getBoolProperty("optimus.graph.cacheScenarioEqualities", false);

  /** Will try hard to re-use SI scenario stacks */
  public static final boolean reuseSIStacks = getBoolProperty("optimus.cache.reuseSIStacks", false);

  /**
   * All new properties are default to this value for tracing, of course makes no difference if you
   * don't trace
   */
  public static boolean defaultToIncludeInTrace = getBoolProperty("optimus.trace.includeAll", true);

  // TODO (OPTIMUS-20571): This flag should always be true, and should be removed
  public static boolean tweakTranslatorsMustBePure =
      getBoolProperty("optimus.compat.tweakTranslatorsMustBePure", false);
  public static final boolean delayResolveXSCache =
      getBoolProperty("optimus.cache.delayResolveXS", true);

  // TODO(OPTIMUS-80221): remove after March 2026
  public static boolean asyncSISupport = getBoolProperty("optimus.compat.asyncSISupport", true);

  public static final boolean allowCachingWithAuditorCallbackDisabled =
      getBoolProperty("optimus.audit.allowCachingWithAuditorCallbackDisabled", false);

  public static boolean keepTaskSchedulerAffinity =
      getBoolProperty("optimus.graph.keepTaskSchedulerAffinity", false);

  /**
   * if this flag is true, we will collect minimal PGO stats and APPLY the learned cache settings at
   * runtime (see additional settings in OGAutoPGOObserver)
   */
  public static boolean livePGO = getBoolProperty("optimus.graph.livePGO", false);

  public static boolean livePGO_GivenNodes = getBoolProperty("optimus.graph.livePGO.given", false);

  public static final boolean useVirtualThreads = DiagnosticSettings.useVirtualThreads;
  public static final boolean limitThreads =
      getBoolProperty("optimus.graph.limitThreads", !useVirtualThreads);

  // marks tests that need a loom review -- mainly because of yet to be implemented features
  public static final boolean loomReview = !limitThreads;

  public static final boolean dangerousIncludeAwaitedTaskInSchedulerString =
      getBoolProperty("optimus.graph.includeAwaitedTaskInSchedulerString", false);

  private static boolean setupAudit() {
    String auditCls = System.getProperty("optimus.audit.cls");
    if (auditCls == null) auditCls = System.getenv("OPTIMUS_AUDIT_CLS");
    boolean enabled = false;
    if (auditCls != null && !auditCls.isEmpty()) {
      enabled = true;
      try {
        Class<?> auditor = Class.forName(auditCls);
        java.lang.reflect.Constructor<?> ctor = auditor.getConstructor();
        AuditVisitor visitor = (AuditVisitor) ctor.newInstance();
        AuditTrace.register(visitor, false);
      } catch (Exception e) {
        //noinspection CallToPrintStackTrace
        e.printStackTrace();
      }
    }
    return getBoolProperty("optimus.audit", enabled);
  }

  // Experimental code for audit purpose only!
  // Anything around these flags is likely to go....
  public static final boolean auditing = setupAudit();

  // generally auditor just 'observes' and does not affect behaviour, but this is useful for tests
  public static final boolean throwOnAuditorException =
      getBoolProperty("optimus.audit.throwOnAuditorException", false);
  private static final String dxdir = System.getProperty("optimus.dxdir");

  public static final boolean ignorePgoGraphConfig =
      getBoolProperty("optimus.config.pgo.ignore", false);

  // TODO (OPTIMUS-25496): set through build system
  public static final String graphConfigOutputDir = System.getProperty("optimus.config.output.dir");

  // If enabled does a number of checks, some are not that cheap though
  public static final boolean schedulerAsserts = DiagnosticSettings.schedulerAsserts;

  // Currently used for scheduler asserts to support tracking scenarios
  public static boolean schedulerAssertsExt =
      getBoolProperty("optimus.scheduler.asserts_ext", false);
  // If enabled, will perform and log some checking on very possible improvements around batching
  // opportunities
  static final boolean schedulerDiagnoseBatchers =
      getBoolProperty("optimus.scheduler.diag.batcher", false);

  // We will skip cleanup of tweakable trackers unless the ratio of GC-cleared nodes (or calls to
  // the track method) to the current number of ttrack roots exceeds the specified ratios. This
  // avoids cleaning up tracking graphs where there is unlikely to be much garbage. These thresholds
  // have been chosen to work for most apps.
  public static int ttrackClearedNodeRefToRootThresholdRatio =
      getIntProperty("optimus.tracking.cleanup.ttrackClearedNodeRefToRootThresholdRatio", 20);
  public static int ttrackTrackCallToRootThresholdRatio =
      getIntProperty("optimus.tracking.cleanup.ttrackTrackCallToRootThresholdRatio", 400);

  // We also have a variable delay timed cleanup (which is varied based on the cost:benefit ratio of
  // the previous cleanups). Note that this affects the decision about whether or not to clean a
  // particular tracker during a cleanup cycle. It does not affect when cleanup cycles are
  // scheduled.
  public static int ttrackCleanupMinDelayFromLastCleanupMs =
      getIntProperty("optimus.tracking.cleanup.ttrackCleanupMinDelayFromLastCleanupMs", 5_000);
  public static int ttrackCleanupMaxDelayFromLastCleanupMs =
      getIntProperty("optimus.tracking.cleanup.ttrackCleanupMaxDelayFromLastCleanupMs", 300_000);

  // The minimum delays after the tracking queue goes idle, and after the last cleanup, before we
  // attempt a new cleanup, and the maximum delay since the last successful cleanup after which we
  // will start ignoring busyness and process the cleanup uninterruptably. Note that these do not
  // affect which trackers actually get cleaned (if any), only when the cleanup cycles are scheduled
  // and when they become uninterruptable.
  public static int ttrackCleanupMinDelayFromIdleMs =
      getIntProperty("optimus.tracking.cleanup.minDelayFromIdleMs", 500);
  public static int ttrackCleanupMinDelayFromLastStartMs =
      getIntProperty("optimus.tracking.cleanup.minDelayFromLastStartMs", 15_000);
  public static int ttrackCleanupMaxDelaySinceLastCompletionMs =
      getIntProperty("optimus.tracking.cleanup.maxDelaySinceLastCompletionMs", 150_000);

  // Finally, if the queue never goes idle, we still want to schedule some cleanups, especially if
  // we accumulate a lot of ttracks for nodes that are no longer reachable. We force a cleanup
  // whenever there hasn't been a cleanup and we have seen 100K (by default) nodes getting cleared.
  public static int ttrackClearedNodeRefWatermark =
      getIntProperty("optimus.tracking.cleanup.ttrackClearedNodeRefWatermark", 100_000);

  public static final String threadsIdealName = "optimus.gthread.ideal";
  public static final String threadsMinName = "optimus.gthread.min";
  public static final String threadsMaxName = "optimus.gthread.max";

  public static final int threadsIdeal = getIntProperty(threadsIdealName, -1);

  // Min/Max number of threads if ideal is negative
  public static final int threadsMin = getIntProperty(threadsMinName, 4);
  public static final int threadsMax = getIntProperty(threadsMaxName, Integer.MAX_VALUE);

  private static final String threadPriorityName = "optimus.gthread.priority";
  static final int threadPriority = getIntProperty(threadPriorityName, Thread.NORM_PRIORITY);

  // since Treadmill allows burst capacity, we may want to use more threads than the stated limit
  static final double treadmillCpuFactor =
      getDoubleProperty("optimus.gthread.treadmillCpuFactor", 2.0);

  // must be placed before any calls to idealThreadCountToCount to avoid initialization ordering
  // isues
  private static final ThreadLimits defaultThreadLimits = new ThreadLimits(treadmillCpuFactor);

  public static final int threadsAuxIdeal = auxSchedulerThreads("optimus.gthread.aux.ideal");

  private static int auxSchedulerThreads(String propName) {
    // 1/4 of CPU, but a minimum 2 to avoid deadlocks when running batch updates on aux scheduler
    return Settings.idealThreadCountToCount(getIntProperty(propName, -4), 2, Integer.MAX_VALUE);
  }

  private static final String threadPriorityAuxName = "optimus.gthread.aux.priority";
  // Lower priority for background actions
  public static final int threadPriorityAux =
      getIntProperty(threadPriorityAuxName, Thread.NORM_PRIORITY - 1);

  public static final int lowerPriorityThreadsIdeal =
      auxSchedulerThreads("optimus.gthread.lowerprio.ideal");

  // even Lower priority than aux scheduler
  public static final int lowerThreadPriority =
      getIntProperty("optimus.gthread.lowerprio.priority", threadPriorityAux - 1);

  public static void assertNotSingleThreadedScheduler() {
    if (threadsIdeal == 0) {
      String msg =
          String.format(
              "%s must be configured to run this application, and the value must be non zero. Try re-running with -D%s=-1",
              threadsIdealName, threadsIdealName);
      throw new IllegalStateException(msg);
    }
  }

  // Simple debug helpers
  // When OGScheduler.wait will detect no progress for 'reportNoProgress' seconds it will dump
  // the information that should be helpful in resolving deadlocks.
  // Around that check is a great place to put breakpoint as it makes variable window available
  public static double reportNoProgress = getDoubleProperty("optimus.reportNoProgress", 30.0);
  // Scheduler in a MT case may not be able to detect deadlock until all threads would come to a
  // stop
  // This is defensive timeout for a blocked thread to re-run deadlock detection
  static final double deadlockDetectionTimeout =
      getDoubleProperty("optimus.deadlockDetectionTimeout", 1.0);
  public static final boolean detailedCircularReferenceExceptions =
      getBoolProperty("optimus.detailedCircularReferenceExceptions", false);

  public static double stallPrintInterval =
      getDoubleProperty("optimus.stallPrintInterval", reportNoProgress);
  // control when to print all graph state (stacktraces etc) when adapted tasks are on the waiting
  // chain
  public static double adaptedStallPrintAllInfoTimeout =
      getDoubleProperty("optimus.adaptedStallPrintTimeout", 3600.0); // seconds

  public static final long progressReportingTimeIntervalMs =
      getLongProperty("optimus.progressReportingTimeIntervalMs", 100);

  // Option to disable including stacktraces in graph stall logging. Current impl of thread dumping
  // requires waiting
  // for safepoints which can cause stalling in performance-critical applications (e.g. DAL broker)
  public static boolean stallPrintStacktraces =
      getBoolProperty("optimus.stallPrintStacktraces", true);

  // reportAllSchedulerStatus will report diagnostics for all schedulers when a stall is detected on
  // a scheduler
  public static final boolean reportAllSchedulerStatus =
      getBoolProperty("optimus.reportAllSchedulerStatus", true);

  // the maximum number of nodes to show in each queue in the scheduler status messages
  static final int nodeLimitInSchedulerStatusMessages =
      getIntProperty("optimus.scheduler.nodeLimitInStatusMsg", 1000);

  public static final boolean useScopedBatcher =
      getBoolProperty("optimus.scheduler.scopedBatcher", true);

  /**
   * Maximum time that we will wait for new elements in an incomplete batch. This is a default
   * applied to all NodeBatcherSchedulerPlugin. See
   * optimus.graph.NodeBatcherSchedulerPluginBase#mustCallDelay() for more info.
   */
  public static final int defaultBatcherMustCallDelay =
      getIntProperty("optimus.batcher.must.call.delay", -1);

  /**
   * Minimum time that we will wait for new elements in an incomplete batch. This is a default
   * applied to all NodeBatcherSchedulerPlugin. See
   * optimus.graph.NodeBatcherSchedulerPluginBase#mustWaitDelay() for more info.
   */
  public static final int defaultBatcherMustWaitDelay =
      getIntProperty("optimus.batcher.must.wait.delay", -1);

  // Diagnostics helpers
  static final boolean throwOnCriticalSyncStack =
      getBoolProperty("optimus.scheduler.throwOnCriticalSyncStack", false);
  private static final String dumpCriticalSyncStackDir =
      System.getProperty("optimus.scheduler.dumpCriticalSyncStacksDir");
  public static final boolean dumpCriticalSyncStacks =
      getBoolProperty("optimus.scheduler.dumpCriticalSyncStacks", true)
          || throwOnCriticalSyncStack
          || (dumpCriticalSyncStackDir != null);
  static boolean dumpCapturedStateMachine =
      getBoolProperty("optimus.graph.dumpCapturedStateMachine", debugAssist);

  public static final boolean dumpNodeTraceOnException =
      getBoolProperty("optimus.scheduler.dumpNodeTraceOnException", debugAssist);
  // Note that we now *always* record on exception, but we might not do so verbosely.  Keeping old
  // property
  // name for compatibility.
  public static final boolean recordVerboseNodeTraceOnException =
      getBoolProperty("optimus.scheduler.recordNodeTraceOnException", debugAssist)
          || getBoolProperty("optimus.scheduler.recordVerboseNodeTraceOnException", debugAssist)
          || dumpNodeTraceOnException;
  static final int exceptionThrowerCacheSize =
      getIntProperty("optimus.scheduler.exceptionThrowerCacheSize", 1000);
  public static final int maxTracedExceptionsPerClass =
      getIntProperty("optimus.scheduler.maxTracedExceptionsPerClass", 1000);

  /** Full trace on Limit/Second exceptions. < 0 means do NOT limit and 0 means Never */
  public static int fullTraceOnExceptionLimit =
      getIntProperty("optimus.scheduler.fullTraceOnExceptionLimit", 10);

  static final boolean fullNodeTraceOnCriticalSyncStack =
      getBoolProperty("optimus.scheduler.fullNodeTraceOnCriticalSyncStack", false);
  static final int criticalSyncStackAlertThreshold =
      getIntProperty("optimus.scheduler.criticalSyncStackAlertThreshold", 100);
  public static final String allowFailOnSyncStacksSysProp =
      "optimus.scheduler.allowFailOnSyncStacks";
  public static boolean allowFailOnSyncStacks =
      getBoolProperty(allowFailOnSyncStacksSysProp, false);
  private static final String traceOnSyncStacksSysProp = "optimus.scheduler.traceOnSyncStacks";
  static final boolean traceOnSyncStacks = getBoolProperty(traceOnSyncStacksSysProp, false);
  private static final String traceDalStacksEnabledProp = "optimus.scheduler.traceDalStacks";
  public static final boolean traceDalStacksEnabled =
      getBoolProperty(traceDalStacksEnabledProp, false);
  private static final String traceDalStacksMaxBatchProp =
      "optimus.scheduler.traceDalStacksMaxBatch";
  public static final int traceDalStacksMaxBatch = getIntProperty(traceDalStacksMaxBatchProp, 20);
  static final int criticalSyncStackMaxSize =
      getIntProperty("optimus.scheduler.criticalSyncStackMaxTraceNodes", 1000);
  public static final boolean showPriqlMethodInfo =
      getBoolProperty("optimus.priql.trace.showPriqlMethodInfo", true);

  static final int samplingInterval = getIntProperty("optimus.scheduler.samplingInterval", -1);
  static final int snapPrintInterval = getIntProperty("optimus.scheduler.snapPrintInterval", -1);
  static final int snapPrintConcurrencyLimit =
      getIntProperty("optimus.scheduler.snapPrintConcurrencyLimit", 6);
  static final int snapPrintBlockedLevel =
      getIntProperty("optimus.scheduler.snapPrintBlockLevel", 1);
  static final int snapConcurrencyLimit =
      getIntProperty("optimus.scheduler.snapConcurrencyLimit", 100);
  static final int snapStackConcurrencyLimit =
      getIntProperty("optimus.scheduler.snapStackConcurrencyLimit", 6);
  static final int stapStackHistory = getIntProperty("optimus.scheduler.snapStackHistory", 1);
  static final double parDecayRate =
      1.0 / getDoubleProperty("optimus.scheduler.parDecayTime", 250.0);

  // these can change in distribution, when processing a DistTask with trace enabled
  // set to true if you want profiler to collect temporal context data, including tweaked vt/tt,
  // vt/tt access,
  // created temporal contexts and temporal surface cmds
  public static final boolean profileTemporalContextDefaultValue = false;
  public static final boolean profileTemporalContext =
      getBoolProperty("optimus.temporalContext.profile", profileTemporalContextDefaultValue);
  public static final boolean traceCreateTemporalContextDefaultValue = false;
  public static final boolean traceCreateTemporalContext =
      getBoolProperty(
          "optimus.temporalSurface.traceCreate", traceCreateTemporalContextDefaultValue);
  public static final boolean warnOnVtTtTweakedDefaultValue = false;
  public static final boolean warnOnVtTtTweaked =
      getBoolProperty("optimus.temporalContext.warnOnVtTtTweaked", warnOnVtTtTweakedDefaultValue);
  public static final boolean warnOnVtTtAccessedDefaultValue = false;
  public static final boolean warnOnVtTtAccessed =
      getBoolProperty("optimus.temporalContext.warnOnVtTtAccessed", warnOnVtTtAccessedDefaultValue);
  // trace all cases that we cannot turn off legacy tweaks

  public static final boolean killOnUninitializedTime =
      getBoolProperty("optimus.temporalContext.killOnUninitializedTime", false);
  public static final boolean traceFailedLegacyTurnoffDefaultValue = false;
  public static final boolean traceFailedLegacyTurnoff =
      getBoolProperty(
          "optimus.temporalSurface.traceFailedLegacyTurnoff", traceFailedLegacyTurnoffDefaultValue);
  // trace each entity loading from DAL. This is used in new tracing feature. It traces DAL access
  // after receiving
  public static final boolean traceEntityForTCMigrationDefaultValue = false;
  public static final boolean traceEntityForTCMigration =
      getBoolProperty("optimus.temporalSurface.traceEntity", traceEntityForTCMigrationDefaultValue);
  // trace the execution of temporal surface commands. This is used in old tracing feature. It
  // traces DAL access
  // before sending. It will have increase DAL loads
  public static final boolean traceTemporalSurfaceCommandsDefaultValue = false;
  public static final boolean traceTemporalSurfaceCommands =
      getBoolProperty(
          "optimus.temporalSurface.traceCommands", traceTemporalSurfaceCommandsDefaultValue);
  public static final boolean traceDalAccessOrTemporalSurfaceCommands =
      traceEntityForTCMigration || traceTemporalSurfaceCommands;
  // keep track of any query that hit the TraceSurface
  public static final boolean enableTraceSurfaceDefaultValue = true;
  public static final boolean enableTraceSurface =
      getBoolProperty("optimus.temporalContext.enableTraceSurface", enableTraceSurfaceDefaultValue);
  public static final int matchTableCacheSize =
      getIntProperty("optimus.temporalContext.matchTableCache", 1000);

  /** time and log TrackingScenario update commands */
  public static final boolean timeTrackingScenarioUpdateCommands =
      getBoolProperty("optimus.graph.tracking.timeTrackingScenarioUpdateCommands", false);

  public static final boolean timeTrackingScenarioEvaluateCommands =
      getBoolProperty("optimus.graph.tracking.timeTrackingScenarioEvaluateCommands", false);
  public static final boolean timeTrackingScenarioCommandSummary =
      getBoolProperty("optimus.graph.tracking.timeTrackingScenarioCommandSummary", false);
  public static final boolean timeTrackingScenarioUpdate =
      timeTrackingScenarioUpdateCommands || timeTrackingScenarioCommandSummary;
  public static final boolean timeTrackingScenarioEvaluate =
      timeTrackingScenarioEvaluateCommands || timeTrackingScenarioCommandSummary;

  public static final boolean trackingScenarioLoggingEnabled =
      getBoolProperty("optimus.graph.tracking.trackingScenarioLoggingEnabled", false);

  public static final boolean trackingScenarioActionStackCaptureEnabled =
      getBoolProperty(
          "optimus.graph.tracking.trackingScenarioActionStackCaptureEnabled",
          trackingScenarioLoggingEnabled);

  private static final boolean warnOnActionAfterDispose =
      getBoolProperty(
          "optimus.graph.tracking.warnOnActionAfterDispose", trackingScenarioLoggingEnabled);
  private static final String trackingScenarioCommandCauseCaptureString =
      "optimus.graph.tracking.trackingScenarioCommandCauseCapture";
  public static boolean trackingScenarioCommandCauseCapture =
      getBoolProperty(trackingScenarioCommandCauseCaptureString, warnOnActionAfterDispose);

  public static final boolean trackingScenarioConcurrencyDebug =
      getBoolProperty("optimus.graph.tracking.trackingScenarioConcurrencyDebug", false);
  public static final int trackingScenarioActionTimeLimit =
      getIntProperty("optimus.graph.tracking.trackingScenarioActionTimeLimit", 3 /*seconds*/);

  public static final int trackingStallPrintIntervalSecs =
      getIntProperty("optimus.graph.tracking.stallPrintIntervalSecs", 60);

  public static long dtqLatencyWarningNanos =
      getIntProperty("optimus.tracking.queue.latencyWarningSecs", 10) * 1000_000_000L;
  public static long dtqTaskTimeLatencyWarningNanos =
      getIntProperty("optimus.tracking.queue.taskTimelatencyWarningSecs", 10) * 1000_000_000L;
  public static double dtqLatencyWarningGrowthFactor =
      getDoubleProperty("optimus.tracking.queue.latencyWarningGrowthFactor", 2.0);
  public static long dtqQueueSizeWarning = getIntProperty("optimus.tracking.queue.sizeWarning", 64);
  public static double dtqQueueSizeWarningGrowthFactor =
      getDoubleProperty("optimus.tracking.queue.sizeWarningGrowthFactor", 2.0);

  public static boolean dtqPrintStateOfQueuesOnWarnings =
      getBoolProperty("optimus.tracking.queue.printStateOnWarnings", false);

  public static final boolean traceTemporalContextMigration =
      profileTemporalContext
          || traceCreateTemporalContext
          || warnOnVtTtTweaked
          || warnOnVtTtAccessed
          || traceFailedLegacyTurnoff
          || traceDalAccessOrTemporalSurfaceCommands;
  public static final boolean syncStacksDetectionEnabled =
      allowFailOnSyncStacks || traceOnSyncStacks;

  public static final boolean cancelOnException =
      getBoolProperty("optimus.cancellation.on.exception", false);
  public static final boolean hideNonRTExceptions =
      getBoolProperty("optimus.hidden.on.exception", true);

  // Flag to turn off the beNice verifyOffGraph flag for At.now
  public static final boolean strictAtNow = getBoolProperty("optimus.strictAtNow", false);

  // remove this from runconfs
  public static boolean autoDetectSInodes =
      getBoolProperty("optimus.scheduler.autoDetectSInodes", false);
  public static final int cacheSize = getIntProperty("optimus.scheduler.cacheSize", 850000);
  public static final int cacheConcurrency =
      getIntProperty("optimus.cache.concurrency", idealThreadCountToCount(threadsIdeal));
  public static final int cacheSISize =
      getIntProperty("optimus.scheduler.scenarioIndependentCacheSize", 250000);
  public static final int cacheCtorSize =
      getIntProperty("optimus.scheduler.constructorCacheSize", 250000);
  public static final int cacheSSPrivateSize =
      getIntProperty("optimus.scheduler.scenarioPrivateCacheSize", 10000);

  // must be less than this to auto disable caching in PGO config generation (the lower this is, the
  // more significant a
  // difference cache time must make in the calculation in order to disable caching for a node)
  private static final double defaultCacheConfigBenefitThresholdMs = -10000.0;
  public static final double cacheConfigBenefitThresholdMs =
      getDoubleProperty("optimus.cache.benefitThresholdMs", defaultCacheConfigBenefitThresholdMs);

  // Cache hit ratio (calculated as hits/(hits + misses)). Aim is to keep caching nodes with high
  // hit ratio, even if
  // other PGO factors indicate that caching should be disabled - so we only disable caching for
  // nodes with low cache
  // benefit AND a low hit ratio. This number indicates how low the hit ratio must be for caching to
  // actually be
  // disabled. The lower this setting, the fewer nodes actually get caching disabled. Higher
  // settings allow more nodes
  // to have caching disabled.
  private static final double defaultCacheConfigHitRatio = 0.25;
  public static final double cacheConfigHitRatio = validateHitRatio();

  private static double validateHitRatio() {
    double ratio = getDoubleProperty("optimus.cache.benefitHitRatio", defaultCacheConfigHitRatio);
    if (ratio < 0 || ratio > 1)
      throw new IllegalStateException("optimus.cache.benefitHitRatio must be between 0 and 1");
    return ratio;
  }

  // For nodes with zero cache hits, number of misses must be greater than this to auto disable
  // caching in PGO config
  // generation
  private static final int defaultCacheConfigNeverHitThreshold = 10;
  public static final int cacheConfigNeverHitThreshold =
      getIntProperty("optimus.cache.neverHitThreshold", defaultCacheConfigNeverHitThreshold);

  // If true, the cache config will include 'equivalent' size for global and shared caches
  public static final boolean autoTrimCaches =
      getBoolProperty("optimus.cache.autoTrimCaches", false);

  public static boolean reuseCacheOnRemoteCalls =
      getBoolProperty("optimus.scheduler.reuseCacheOnRemoteCalls", true);
  static final boolean defaultCacheByNameTweaks =
      getBoolProperty("optimus.scheduler.cacheByNameTweaks", true);

  static final int maxCustomFrames = getIntProperty("optimus.graph.max.custom.frames", 1000);

  public static final boolean warnOnDemandCompiles =
      getBoolProperty("optimus.priql.warnOnDemandCompiles", false);

  private static final String libPath = System.getProperty("optimus.libpath");

  public static final String entityPluginPath = entityPluginPathFromLibPath(libPath);

  // if the sync stack depth is less than this limit, we will allow the scheduler thread to keep
  // taking on more work
  // very possibly unrelated to the sync node currently executing on JVM stack (as long as that work
  // cannot cause
  // inverted dependencies), but once this limit is exceeded we will only take work which is a
  // dependency of the node
  // we're waiting on (to avoid overflowing the stack)
  static final int syncStackSafetyLimit =
      getIntProperty("optimus.scheduler.syncStackSafetyLimit", 100);

  // switch for reactive range query, if true, means using DAL historical API (non legacy one) to do
  // range query
  // Note: this is a temporary flag and will be removed in near future.
  // Please contact graph team if you think that you want to use this
  // TODO (OPTIMUS-18021): should be deleted when deprecated legacy API
  public static final boolean useHistoricalRangeQuery =
      getBoolProperty("optimus.reactive.historicalRangeQuery", false);

  // switch for reactive range query, if true, means using DAL OpenVtRange historical API to do
  // range query
  // Note: this is a temporary flag and will be removed in near future.
  // Please contact graph team if you think that you want to use this
  // TODO (OPTIMUS-18021): should be deleted when deprecated legacy API
  public static final boolean useOpenVtTtRangeQuery =
      getBoolProperty("optimus.reactive.openVtTtRangeQuery", false);

  public static final String rangeQueryOptsUser =
      getStringProperty("optimus.reactive.rangeQueryOptsUser", null);

  // whether to enable notification warning/error message log to breadcrumb
  public static final boolean enableReactiveBreadcrumb =
      getBoolProperty("optimus.notification.enableBreadcrumb", true);

  static {
    if (!getBoolProperty("optimus.reactive.dalPubSub", true))
      throw new IllegalStateException(
          "-Doptimus.reactive.dalPubSub=false no longer allowed, please remove setting and migrate to DalPubSub");
  }

  public static String overrideAppId =
      getStringProperty("optimus.testsuite.override.appId", "OptimusTestRunner");

  public static String overrideZoneId =
      getStringProperty("optimus.testsuite.override.zoneId", "OptimusTestRunner");

  // reactive subscriptions will be marked entitled only rather than failing for unentitled
  // entities.
  public static boolean reactiveDalPubSubEntitledOnly =
      getBoolProperty("optimus.reactive.dalPubSubEntitledOnly", false);

  public static long reactiveTxTimeProviderConnectTimeoutMs =
      getLongProperty("optimus.reactive.txTimeProviderConnectTimeoutMs", 60 * 1000);

  public static boolean checkAsyncLogging = getBoolProperty("optimus.logging.checkAsync", true);

  // If set, do not respect @givenRuntimeEnv or @givenAnyRuntimeEnv, but rather use the caller's
  // scenario.
  // Should probably be used only in unit tests. TODO (OPTIMUS-52498): remove this flag
  public static final boolean allowIllegalOverrideOfInitialRuntimeEnvironment =
      getBoolProperty("optimus.runtime.allowIllegalOverrideOfInitialRuntimeEnvironment", false);

  // TODO (OPTIMUS-21846): switch to true when we're confident that causality
  //  tracking is working for UI and reactive
  public static boolean throwOnEventCauseErrors =
      getBoolProperty("optimus.tracking.causality.throwOnEventCauseErrors", false);
  // start start and stop call stack for token usage
  public static final boolean instrumentEventCauseToken =
      getBoolProperty("optimus.eventCause.instrumentEventCauseToken", schedulerAsserts);

  // If set to true (the default), wait for in background steps to complete before completing
  // parents. This is important for test stability.
  public static boolean waitForInBackgroundSteps =
      getBoolProperty("optimus.eventCause.waitForInBackground", true);

  // Used by StallDetector to detect hanging processes and kill them after timeout
  // These properties are also allowlisted for propagation to grid engines in GSFConfig so make sure
  // any changes to the
  // property names are also reflected there
  public static final boolean detectStalls = getBoolProperty("optimus.graph.detectStalls", false);
  public static final int detectStallInterval =
      getIntProperty("optimus.graph.detectStallIntervalSecs", 60); // seconds
  public static final int detectStallTimeout =
      getIntProperty("optimus.graph.detectStallTimeoutSecs", 600); // seconds
  public static final int detectStallAdaptedTimeout =
      getIntProperty("optimus.graph.detectStallAdaptedTimeoutSecs", 3600); // seconds
  // false means we will log a warning instead of killing the process; used to test before enabling
  // for regressions
  public static final boolean detectStallAdapted =
      getBoolProperty("optimus.graph.detectStallAdapted", false);
  public static String detectStallLogDir = getStringProperty("optimus.graph.detectStallLogDir", "");
  public static final int detectStallMaxRequests =
      getIntProperty("optimus.graph.detectStallMaxRequests", 64);

  public static final boolean allowTestableClock = TestableClock.allowTestableClock;

  public static final boolean handlerProfilingDataPublish =
      getBoolProperty("optimus.handler.publishProfilingData", false);

  public static final boolean publishThrottledHandlerProfilingStats =
      getBoolProperty("optimus.handler.publishThrottledHandlerProfilingStats", false);

  public static final boolean publishUiWorkerStats =
      getBoolProperty("optimus.handler.publishUiWorkerStats", false);

  public static final boolean profilingStatsSendNonConfigEvents =
      getBoolProperty("optimus.handler.profilingStatsSendNonConfigEvents", false);

  public static final boolean profilingStatsSendThrottledEvents =
      getBoolProperty("optimus.handler.profilingStatsSendThrottledEvents", false);

  public static final boolean profilingStatsDumpToFile =
      getBoolProperty("optimus.handler.profilingStatsDumpToFile", false);

  public static final String throttledHandlerProfilingConfigFile =
      getStringProperty("optimus.handler.publishProfilingDataConfigFile", "");

  public static final boolean isLocationTagRT =
      getBoolProperty("optimus.graph.isLocationTagRT", true);
  public static final boolean interceptFiles =
      getBoolProperty("optimus.graph.fileinterceptor", false);

  public static final boolean progressTrackingDebugMode =
      getBoolProperty("optimus.graph.progressTrackingDebugMode", false);
  public static final int stalledProgressIntervalSecs =
      getIntProperty("optimus.graph.stalledProgressIntervalSecs", 10);

  // When set, report all fullWait(), even if concurrency wasn't reduced.
  public static final boolean reportEveryFullWait =
      getBoolProperty("optimus.graph.reportEveryFullWait", false);

  // When true, re-entrance into a throttle block becomes an exception.
  public static final boolean failOnRecursiveThrottle =
      getBoolProperty("optimus.graph.failOnRecursiveThrottle", false);

  // You can temporarily set this to false to verify if the index/key ordering is breaking your
  // applications. If so, contact Graph and DAL teams immediately to discuss. This flag will always
  // be true in future. Don't set it to false without notifying us or you will get broken!
  //
  // TODO (OPTIMUS-79168): This flag will be removed once we are sure that no apps are broken by it.
  public static boolean enableStableIndexKeyOrdering =
      getBoolProperty("optimus.pickling.enableStableIndexKeyOrdering", true);

  /**
   * We can evaluate initial time in three different ways:
   * <li>"eager" -- request it now and wait for it
   * <li>"async" -- request it asynchronously
   * <li>"lazy" -- request it only on demand
   *
   * @see optimus.platform.runtime.RuntimeComponents#newTopScenario(EntityResolver)
   */
  @SuppressWarnings("JavadocReference")
  public static final String initialTimeResolutionMode =
      getStringProperty("optimus.initial.time.resolution", "eager");

  /** Turn on additional runtime check for optimus channels. */
  public static final boolean channelAsserts = getBoolProperty("optimus.channels.asserts", false);

  /** System property to control whether during case matches, we should */
  public static final boolean allowPickledPropertiesAsMaps =
      getBoolProperty("optimus.core.allowPickledPropertiesAsMaps", true);

  /**
   * System property to allow disabling strict-equality. Disabling this will turn off the unpickled
   * interning that relies on it. (see optimus.pickling.interningEnabledFor system property)
   */
  public static final boolean strictEqualityEnabled =
      getBoolProperty("optimus.core.strictEqualityEnabled", true);

  public enum InterningScope {
    PICKLED,
    UNPICKLED,
  }
  /**
   * Allows control of interning behavior during unpickling
   * <li>"pickled": Enables interning of only the pickled forms of data as they are retained in
   *     memory as part of any lazy unpickling -
   * <li>"unpickled": Enables interning of only the instances obtained from unpickling
   * <li>"all": Enables interning of both the above
   * <li>"none": Disables interning of both the above This property defaults to "all" unless
   *     optimus.core.strictEqualityEnabled is set to false in which case it defaults to "pickled"
   *     since interning of unpickled instances relies on strict-equality
   */
  public static final Set<InterningScope> interningEnabledFor = parseFromProperty();

  private static Set<InterningScope> parseFromProperty() {
    var setting =
        getStringProperty(
            "optimus.pickling.interningEnabledFor", strictEqualityEnabled ? "all" : "pickled");
    return switch (setting) {
      case "all" -> Set.of(InterningScope.values());
      case "none" -> Set.of();
      case "pickled" -> Set.of(InterningScope.PICKLED);
      case "unpickled" -> Set.of(InterningScope.UNPICKLED);
      default -> throw new IllegalArgumentException(
          "The string "
              + setting
              + " is not a valid value for the "
              + "optimus.pickling.interningEnabledFor system property.");
    };
  }
  /*
   * Controls whether values in the pickled representation of instances are interned. The interning
   * is based on statistics per shape, per field (of the pickled format). To turn off interning
   * completely, set it to a negative value
   */
  public static final double hitRatioForInterning =
      getDoubleProperty("optimus.pickling.hitRatioForInterning", 0.2);

  /**
   * Controls the size of collections for which we want to generate and use SlottedBuffers for
   * pickled representations of instances. SlottedBuffers are good for small collections with mixed
   * members including primitive types as we are able to avoid boxing them at the item level, rather
   * than putting them in Object arrays.
   */
  public static final int slottedBufferForSeqThreshold =
      getIntProperty("optimus.pickling.slottedBufferForSeqThreshold", 10);

  /**
   * Controls the number of unique Shapes we want to generate slotted buffers for to hold the
   * unpickled representation of small collections (called SlottedBufferAsSeq). The number of
   * SlottedBufferAsSeq instances is limited by the total number of unique shapes including those
   * that are for embeddables and entities (SlottedBufferAsMap). At the point where this threshold
   * is reached, we will allow creation of new SlottedBufferAsMap classes but no new
   * SlottedBufferAsSeq class will be generated and we'll fall back to using ArraySeq (as we do for
   * large collections)
   */
  public static final int uniqueShapeThreshold =
      getIntProperty("optimus.pickling.uniqueShapeThreshold", 2000);

  /** */
  public static final int interningObservationThreshold =
      getIntProperty("optimus.pickling.interningObservationThreshold", 100);

  public static final boolean profileInterningStats =
      getBoolProperty("optimus.pickling.profileStats", false);

  private static String entityPluginPathFromLibPath(String libPath) {
    return pluginPathFromLibPath(libPath, "entityplugin");
  }

  public static String pluginPathFromLibPath(String libPath, String pluginName) {
    if (libPath == null) return null;
    else {
      File f = new File(libPath, pluginName + ".jar");
      if (f.exists()) return f.getPath();
      else
        return new File(new File(libPath, pluginName), "optimus.platform." + pluginName + ".jar")
            .getPath();
    }
  }

  public static ObjectName getGraphObjectName() throws MalformedObjectNameException {
    return new ObjectName("optimus.graph:type=graph");
  }

  public static int idealThreadCountToCount() {
    return idealThreadCountToCount(threadsIdeal);
  }

  public static int idealThreadCountToCount(
      int count, int minimumIfNegative, int maximumIfNegative) {
    return defaultThreadLimits.idealThreadCountToCount(count, minimumIfNegative, maximumIfNegative);
  }

  public static int idealThreadCountToCount(int count) {
    return defaultThreadLimits.idealThreadCountToCount(count);
  }

  /**
   * The class initializer below checks that the entityagent jar is installed and unsets
   * `DiagnosticSettings.traceAvailable` if it is not. There is an initialization race between this
   * class and {@link NodeTrace}, where the latter uses reflection to get at the injected profiling
   * fields. In the interest of not duplicating that code, this hook is used to ensure that such
   * check is made before reflecting on the synthetic fields.
   */
  public static void ensureTraceAvailable() {
    // just to run <clinit>
  }

  static {
    try {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      mbs.registerMBean(
          new StandardMBean(new GraphMBeanImpl(), GraphMBean.class, false), getGraphObjectName());

      if (dxdir != null) {
        String host = InetAddress.getLocalHost().getHostName();
        String jvmName = ManagementFactory.getRuntimeMXBean().getName();

        int port = 18000; // Initial port
        for (; port < 18100; port++) {
          try {
            LocateRegistry.createRegistry(port);
            break;
          } catch (ExportException ex) {
            log.error(ex.getMessage());
          }
        }
        JMXServiceURL url =
            new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:" + port + "/jmxrmi");
        JMXConnectorServer cs = JMXConnectorServerFactory.newJMXConnectorServer(url, null, mbs);
        cs.start();
        File f = new File(dxdir + File.separatorChar + host + "_" + port);
        FileWriter fw = new FileWriter(f.getAbsolutePath());
        fw.write(jvmName);
        fw.close();
      }
    } catch (Exception ex) {
      // The code should run even if bean registration fails
      log.error(ex.getMessage());
    }
  }
}
