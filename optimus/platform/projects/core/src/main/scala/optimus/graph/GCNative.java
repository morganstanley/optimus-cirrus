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

import static java.util.regex.Pattern.compile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import javax.management.MBeanServer;

import com.google.common.base.Preconditions;
import com.sun.management.HotSpotDiagnosticMXBean;
import optimus.breadcrumbs.Breadcrumbs;
import optimus.breadcrumbs.crumbs.Crumb;
import optimus.breadcrumbs.crumbs.Properties;
import optimus.config.InstallPathLocator;
import optimus.core.MonitoringBreadcrumbs$;
import optimus.core.config.StaticConfig;
import optimus.graph.cache.Caches;
import optimus.graph.cache.CauseGCNative$;
import optimus.graph.diagnostics.GCNativeStats;
import optimus.graph.diagnostics.InfoDumper;
import optimus.graph.tracking.DependencyTrackerRoot$;
import optimus.graph.tracking.TrackingGraphCleanupTrigger;
import optimus.platform.AdvancedUtils;
import optimus.logging.Pid;
import optimus.utils.SystemFinalization;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * JNI hook into libgcnative.so. This class loads gcnative, allows callers to shut it down, and
 * provides hooks for gcnative to call back into jvm code to clear caches or log
 *
 * <p>To use GCNative, set -Doptimus.gcnative.version=3. To opt-out, set -Doptimus.gcnative.disable
 */
public class GCNative {
  private static final Logger log = LoggerFactory.getLogger(GCNative.class);
  private static final Properties.Elem<String> crumbDesc =
      Properties.description().elem("GCNative");

  // The initial native watermark.
  // When native heap exceeds this, GCNative will act.
  private static final int watermarkMB =
      DiagnosticSettings.getIntProperty("optimus.gcnative.watermark.mb", -1);
  private static final String legacyWatermarkEnv = System.getenv("GC_ON_NATIVE_WATERMARK_MB");
  private static final double managedPercent =
      DiagnosticSettings.getDoubleProperty("optimus.gcnative.managed.percent.noclear", 0.0);

  // Generate jemalloc dumps frequently
  private static final boolean jemallocVerbose =
      Optional.ofNullable(System.getenv("JEMALLOC")).map(x -> x.contains("verbose")).orElse(false);

  // The emergency shutdown watermark.
  // When native heap exceeds this, JVM will System.exit.
  // If this is not provided, it defaults to watermark * 1.8
  public static final int emergencyWatermarkMB =
      DiagnosticSettings.getIntProperty("optimus.gcnative.emergency.watermark.mb", -1);
  private static final String legacyEmergencyWatermarkEnv =
      System.getenv("GC_ON_NATIVE_EXCEPTION_WATERMARK_MB");

  public static final int surrogateMaxRSSMB =
      DiagnosticSettings.getIntProperty("optimus.gcnative.surrogate.maxrss.mb", 2 * 1024 * 1024);

  // GCNative will System.exit if this many cache clears happen with no low-watermark reset
  // inbetween
  // (this guards against endless loops of clear cache - repopulate cache - clear cache again)
  private static final int maxAllClears =
      DiagnosticSettings.getIntProperty("optimus.gcnative.total.clears", -1);
  private static final String legacyMaxClearsEnv = System.getenv("GC_ON_NATIVE_TOTAL_CLEARS");

  // If this is enabled (it is enabled by default), the watermark will increase when cache clear
  // fails to bring the
  // native heap down
  private static final boolean dynamicWMEnabled =
      DiagnosticSettings.getBoolProperty("optimus.gcnative.steady.state.enabled", true);
  // Initial steady state mark, estimates the amount of native heap used by the app with its optimus
  // caches cleared
  private static final int steadyStateMB =
      DiagnosticSettings.getIntProperty("optimus.gcnative.steady.state.mb", -1);
  private static final String legacySteadyStateEnv = System.getenv("GC_ON_NATIVE_STEADY_STATE_MB");
  // if the algorithm demands that the dynamic watermark is raised above emergency watermark, we
  // clamp it to EWM
  // times this factor
  private static final double dynamicWMClampFactor =
      DiagnosticSettings.getIntProperty("optimus.gcnative.dynamic.clamp.pct", 80) / 100.0;
  // when cache clear fails to drop native heap (as seen in glibc) watermark grows by this factor
  private static final double dynamicWMGrowthFactor =
      DiagnosticSettings.getIntProperty("optimus.gcnative.dynamic.growth.pct", 5) / 100.0;

  // when custom allocators are not loaded, we can use malloc_info to determine native heap size, or
  // look at the diff
  // between RSS
  // and JVM footprint
  // the differences is used by default because it is more reflective of what total memory footprint
  // of the app is
  // even when the difference is used, malloc_info-reported size is logged in logSizes
  private static final boolean useMallocInfo =
      DiagnosticSettings.getBoolProperty("optimus.gcnative.mallocinfo.enabled", false);

  // simple GC is attempted this many times before deciding to proceed with full cache clear
  private static final int resetGCCount =
      DiagnosticSettings.getIntProperty("optimus.gcnative.gc.attempts", 5);

  // length of the sliding window for tracking recent heap details
  private static final int slidingWindowSize =
      DiagnosticSettings.getIntProperty("optimus.gcnative.sliding.window", 5);

  // if heap drops below this, cache clear was a success, the state machine will reset
  private static final double lowWMFactor =
      DiagnosticSettings.getIntProperty("optimus.gcnative.low.watermark.pct", 95) / 100.0;

  // GCNative thread controls
  // Delay before the thread begins looking at native heap
  private static final int startupQuietSeconds =
      DiagnosticSettings.getIntProperty("optimus.gcnative.startup.quiet.seconds", -1);
  private static final String legacyStartupQuietSecondsEnv =
      System.getenv("GC_ON_NATIVE_QUIET_STARTUP_SEC");
  // The GCNative thread looks at native heap at these intervals
  private static final int timerPeriodMs =
      DiagnosticSettings.getIntProperty("optimus.gcnative.period.ms", -1);
  private static final String legacyTimerPeriodMsEnv =
      System.getenv("GC_ON_NATIVE_TIMER_PERIOD_MS");

  private static final long heartbeatDurationMs =
      DiagnosticSettings.getLongProperty("optimus.gcnative.heartbeat.duration.ms", 1000);

  // controls for JVM heap dump after multiple cache clears do not help
  // central switch
  private static final boolean dumpHeap =
      DiagnosticSettings.getBoolProperty("optimus.gcnative.dumpHeap", false);
  // heap dump will use this file, and will append underscore, the current JVM's PID, and .hprof
  // extension
  private static final String dumpHeapFilePrefix =
      DiagnosticSettings.getStringProperty(
          "optimus.gcnative.dumpHeapFilePrefix",
          System.getProperty("java.io.tmpdir") + File.separator + "gcnative_heap_dump");
  // if full cache clear happens this many times in a row, with no low-watermark reset inbetween,
  // dump heap
  private static final int dumpHeapAfter =
      DiagnosticSettings.getIntProperty("optimus.gcnative.dumpHeapAfter", 5);

  // Fine-tuning controls for cache-clearing policy
  // At LEVEL_CLEAR_MAIN_CACHE, instead of clearing the entire main optimus cache, only clear the
  // nodes whose
  // 'gcnative' marker is set via optconf file (optimus command line parameter --config-graph)
  private static final boolean clearSubset =
      DiagnosticSettings.getBoolProperty("optimus.gcnative.clearSubset", false);
  private static final boolean legacyClearSubsetEnv =
      System.getenv("GC_ON_NATIVE_CLEAR_SUBSET") != null;
  // Note: Experimental, requires that Heap Explorer is loaded via -agentpath
  // At any cache clear level, auto-detect nodes that use finalizers (may take several cache clear
  // cycles)
  private static final boolean detectFinalizers =
      DiagnosticSettings.getBoolProperty("optimus.gcnative.detectFinalizers", false);
  // time, in seconds, to spend on finalizer detection during each cache clear cycle
  private static final int detectFinalizerSeconds =
      DiagnosticSettings.getIntProperty("optimus.gcnative.detectFinalizersTimeout", 5);

  private static final int maxFinalizerClasses =
      DiagnosticSettings.getIntProperty("optimus.gcnative.max.finalizer.classes", 10);
  private static final double finalizerCutoffFraction =
      DiagnosticSettings.getDoubleProperty("optimus.gcnative.finalizer.fraction", 0.9);

  // GCNative is currently disabled on Windows because getNativeAllocation takes too much time
  // TODO (OPTIMUS-12765): enable by default when its performance is acceptable
  private static final boolean enabledOnWindows =
      DiagnosticSettings.getBoolProperty("optimus.gcnative.windows", false);

  // when GCNative is requested, but cannot be loaded, we System.exit, unless this is set
  private static final boolean notBothered =
      Boolean.parseBoolean(System.getProperty("optimus.gcnative.notBothered"));

  private static final long tcmalloc_max_total_thread_cache_bytes =
      DiagnosticSettings.getLongProperty(
          "optimus.gcnative.tcmalloc.max_total_thread_cache_bytes", -1);

  // Version and path overriders
  // zero is treated the same as optimus.gcnative.disable: it's a way to opt-out
  // 3 is the only option supported, and is treated as the opt-in
  private static final String GCNATIVE_VERSION_PROP = "optimus.gcnative.version";
  public static final int version = Integer.getInteger(GCNATIVE_VERSION_PROP, 0);

  // The kill switch to disable gcnative
  public static final boolean disableGCNative =
      DiagnosticSettings.getBoolProperty("optimus.gcnative.disable", false);

  private static final boolean surrogateLightLog =
      DiagnosticSettings.getBoolProperty("optimus.gcnative.surrogate.light.log", false);

  private static final long periodicLoggingIntervalMs =
      DiagnosticSettings.getLongProperty("optimus.gcnative.logging.interval.ms", 15000L);

  // The next three options are used by InstallPathLocator:

  // Ignore AFS path and load a custom gcnative from this location (e.g. local build)
  public static final String GCNATIVE_PATH_OVERRIDE_PROP = "optimus.gcnative.path.override";
  // TODO (OPTIMUS-12766): remove this when ptools and gcnative merge
  // Ignore ms/dist and load a custom ptools from this location (e.g. local build)
  public static final String PTOOLS_PATH_OVERRIDE_PROP = "optimus.ptools.path.override";
  public static final String INTERCEPTOR_PATH_OVERRIDE_PROP =
      "optimus.file.interceptor.path.override";

  // End of options

  // GCNative levels
  // level -1: no action, only report native heap change
  private static final int LEVEL_INFO_ONLY = -1;
  public static final int LEVEL_GC = 0; // Level 0 cleanup: just run gc
  public static final int LEVEL_CLEAR_MAIN_CACHE = 1; // Level 1 cleanup: "clear all caches"
  // Level 2 cleanup: clear all caches with si and constructor cache
  public static final int LEVEL_CLEAR_ALL_GLOBAL_CACHES = 2;
  // Level 3 cleanup: clear all caches with si and per-property caches.
  // This also calls the default callbacks
  public static final int LEVEL_CLEAR_ALL_CACHES_CALLBACKS = 3;
  public static final int LEVEL_SHUTDOWN = 4; // Level 4: immediate JVM shutdown
  public static final int LEVEL_N = 5;
  public static final int LEVEL_EXTERNAL = 6;

  /** Whether GCNative has been successfully loaded */
  public static volatile boolean loaded = false;

  public static boolean isLoaded() {
    return loaded;
  }

  /** registered callbacks to be notified when cache is being cleaned */
  private static final Map<Integer, List<scala.Function0<scala.Unit>>> callbacks = new HashMap<>();

  private static void incrementGcNativeCleared(int level) {
    switch (level) {
      case LEVEL_GC:
        GCNativeStats.incrementCounter(GCNativeStats.keys().totalInvocationsGC(), 1);
      case LEVEL_CLEAR_MAIN_CACHE:
        GCNativeStats.incrementCounter(GCNativeStats.keys().totalInvocationsMainCache(), 1);
      case LEVEL_CLEAR_ALL_GLOBAL_CACHES:
        GCNativeStats.incrementCounter(GCNativeStats.keys().totalInvocationsGlobalCache(), 1);
      case LEVEL_CLEAR_ALL_CACHES_CALLBACKS:
        GCNativeStats.incrementCounter(GCNativeStats.keys().totalInvocationsPrivateCache(), 1);
    }
  }

  private static final AtomicLong timesGcNativeInvoked = new AtomicLong();
  private static final AtomicLong gcNativeHeapChange = new AtomicLong();

  private static final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
  private static final boolean win =
      System.getProperty("os.name").toLowerCase().contains("windows");

  public enum AllocatorType {
    TCMALLOC("TCMalloc", "libtcmalloc", false),
    JVM("RSS-JVM", "default", true),
    JVM_MALLOC_INFO("malloc_info", "default", true),
    JEMALLOC("jemalloc", "libjemalloc", false);

    final String name;
    final String libraryName;
    final boolean doTrim;

    AllocatorType(String allocName, String libraryName, boolean doTrim) {
      this.name = allocName;
      this.libraryName = libraryName;
      this.doTrim = doTrim;
    }
  }

  private static AllocatorType getAllocator() {
    if (isLibraryLoaded(AllocatorType.TCMALLOC.libraryName)) {
      return AllocatorType.TCMALLOC;
    } else if (isLibraryLoaded(AllocatorType.JEMALLOC.libraryName)) {
      return AllocatorType.JEMALLOC;
    } else if (useMallocInfo) {
      return AllocatorType.JVM_MALLOC_INFO;
    }
    return AllocatorType.JVM;
  }

  public static final AllocatorType allocator = getAllocator();
  // JNI call to load jemallocAPI must be lazy, or we can get linkage problems
  private static boolean tcmallocAPILoaded = false;
  private static boolean jemallocAPILoaded = false;
  private static boolean noTcMalloc = win;
  private static boolean jemallocProfilingConfig = false;

  static boolean isLibraryLoaded(String library) {
    if (win) {
      return false;
    } else {
      // the only good way to detect an LD_PRELOAD'd library
      try (Stream<String> stream = Files.lines(Paths.get("/proc/self/maps"))) {
        return stream.anyMatch((line) -> line.contains(".so") && line.contains(library));
      } catch (IOException | UncheckedIOException e) {
        log.error("failed reading /proc: " + e);
        return false;
      }
    }
  }

  private static boolean gcNativeLoadAttempted = false;

  private static synchronized void ensureGcNativeLibraryLoaded() {
    if (!gcNativeLoadAttempted) {
      try {
        InstallPathLocator.Resolver resolver = InstallPathLocator.system();
        resolver.loadGCNative();
      } catch (UnsatisfiedLinkError e) {
        if (e.getLocalizedMessage().contains("already loaded")) {
          log.warn("Attempted to load gcnative twice");
        } else {
          throw e;
        }
      }
      gcNativeLoadAttempted = true;
    }
  }

  public static synchronized void ensureLoaded() {
    if (!loaded && !(disableGCNative || version == 0)) {
      if (version != 3) {
        log.error(
            "gcnative version "
                + version
                + " is obsolete. Use -Doptimus.gcnative.version=3 to enable GCNative, "
                + "-Doptimus.cppagent.version or -Doptimus.gcnative.path.override to override the default.");
      }
      if (win && !enabledOnWindows) {
        var windowsAlternative = StaticConfig.string("nativeProfilingOnWindows");
        log.info("gcnative loading on Windows is not yet enabled: use " + windowsAlternative);
        GCMonitor.gcCrumb(
            crumbDesc,
            Properties.gcNative().elem("unconfigured"),
            Properties.gcNativePath().elem("NONE"));
      } else {
        // if you explicitly request GCNative via NativeSetup OR if you are using v3, we'll try to
        // load it
        InstallPathLocator.Resolver resolver = InstallPathLocator.system();
        try {
          if (!resolver.disableGCNative()) {
            GCMonitor.gcCrumb(
                crumbDesc,
                Properties.gcNative().elem("load"),
                Properties.gcNativePath()
                    .elem(resolver.gcNativePathOverride().getOrElse(() -> "runtime")),
                Properties.gcNativeAllocator().elem(allocator.name));
            ensureGcNativeLibraryLoaded();
            if (tcmallocAPILoaded) {
              if (tcmalloc_max_total_thread_cache_bytes > 0) {
                log.info(
                    "overriding "
                        + MallocExtensionProperty.TCMalloc.max_total_thread_cache_bytes.getName()
                        + " to "
                        + tcmalloc_max_total_thread_cache_bytes
                        + " bytes");
                setTcMallocExtensionNumericProperty(
                    MallocExtensionProperty.TCMalloc.max_total_thread_cache_bytes,
                    tcmalloc_max_total_thread_cache_bytes);
                if (getTcMallocExtensionNumericProperty(
                        MallocExtensionProperty.TCMalloc.max_total_thread_cache_bytes)
                    != tcmalloc_max_total_thread_cache_bytes) {
                  throw new RuntimeException("malloc extension override did not take effect");
                }
              }
              dumpTCMallocInfo(null);
            }

            loaded = true;
            setNativeWatermark(
                getGCNativeSetting(
                    1024 * 1024L * 1024L, watermarkMB, legacyWatermarkEnv, "watermark"));
            setEmergencyWatermark(
                getGCNativeSetting(
                    (long) (watermark * 1.8),
                    emergencyWatermarkMB,
                    legacyEmergencyWatermarkEnv,
                    "emergency shutdown watermark"));
            Thread t = new Thread(GCNative::thread);
            t.setDaemon(true);
            t.setName("GC Native");
            t.start();
          } else {
            GCMonitor.gcCrumb(
                crumbDesc,
                Properties.gcNative().elem("load"),
                Properties.gcNativePath().elem("NONE"));
            log.info("gcnative disabled");
          }
        } catch (UnsatisfiedLinkError ex) {
          // Windows launcher scripts specify gcnative.so, do not log this failure
          // TODO (OPTIMUS-12765) once gcnative.dll is disted and battle-tested,
          // migrate all such scripts
          if (!win) {
            log.error("loading gcnative failed with exception: {}", ex.toString());
            // failing to load gcnative when requested should be treated as a fatal error.
            // Many forms of testing will fail to detect that gcnative is not working because it's
            // only apparent
            // in longer running processes / under certain heavy usage patterns.
            // We want to avoid situations where code only fails in production.
            if (!notBothered) {
              log.error(
                  "Calling System.exit because GCNative failed to initialize correctly. "
                      + "If this has broken your "
                      + "prod process because you haven't tested the (prod) launch script at all on Linux, sorry. "
                      + "Luckily there is a flag you can set -Doptimus.gcnative.notBothered=true to bypass this hard "
                      + "exit. "
                      + "Preferably you can fix you launch script to use enable GCNative correctly, or don't"
                      + " enable it at all.");
              MonitoringBreadcrumbs$.MODULE$.sendGraphFatalErrorCrumb(
                  "GCNative failed to initialize correctly", null, ex, null, null);
              Breadcrumbs.flush();
              System.exit(1);
            }
          }
        }
      }
    }
  }

  private static long getGCNativeSetting(
      long dflt, int settingMB, String legacySettingEnv, String desc) {
    long setting = dflt;
    if (settingMB != -1 && legacySettingEnv != null) {
      log.info(
          desc
              + " configured through both JVM option and environment variable. JVM option will be used");
    }
    if (settingMB == -1 && legacySettingEnv != null) {
      setting = Integer.parseInt(legacySettingEnv) * 1024L * 1024L;
    } else if (settingMB != -1) {
      setting = settingMB * 1024L * 1024L;
    }
    return setting;
  }

  private static int getGCNativeIntegerSetting(
      int dflt, int settingVM, String legacySettingEnv, String desc) {
    int setting = dflt;
    if (settingVM != -1 && legacySettingEnv != null) {
      log.info(
          desc
              + " configured through both JVM option and environment variable. JVM option will be used");
    }
    if (settingVM == -1 && legacySettingEnv != null) {
      setting = Integer.parseInt(legacySettingEnv);
    } else if (settingVM != -1) {
      setting = settingVM;
    }
    return setting;
  }

  // "committed" JVM memory as reported by MEMORY_MXBEAN. This is what JVM got from the OS, it may
  // temporarily
  // outpace RSS
  private static MemoryMXBean memBean;

  private static long getJVMFootprint() {
    if (memBean == null) {
      try {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        memBean =
            ManagementFactory.newPlatformMXBeanProxy(
                server, ManagementFactory.MEMORY_MXBEAN_NAME, MemoryMXBean.class);
      } catch (IOException e) {
        log.error("cannot determine JVM footprint: " + e);
      }
      return 0;
    }
    MemoryUsage heap = memBean.getHeapMemoryUsage();
    MemoryUsage nonHeap = memBean.getNonHeapMemoryUsage();
    // note: Runtime.getRuntime().totalMemory() equals heap.getUsed(), but it's not helpful in
    // footprint determination
    return heap.getCommitted() + nonHeap.getCommitted();
  }

  private static DescriptiveStatistics jvmHeapTracker;
  private static DescriptiveStatistics rssTracker;

  private static Set<Integer> surrogatePidSet = Collections.synchronizedSet(new HashSet<Integer>());

  private static Pattern pRSS = compile("VmRSS:\\s+(\\d+) kB");
  private static Pattern pRSSAnon = compile("RssAnon:\\s+(\\d+) kB");

  public static void addPidToTrackRSS(int pid) {
    synchronized (surrogatePidSet) {
      surrogatePidSet.add(pid);
    }
  }

  public static void removePidToTrackRSS(int pid) {
    synchronized (surrogatePidSet) {
      surrogatePidSet.remove(pid);
    }
  }

  private static SurrogateStatus getSurrogateStatus() {
    List<SurrogateInfo> surrogates = new ArrayList<>(surrogatePidSet.size());
    synchronized (surrogatePidSet) {
      for (Iterator<Integer> it = surrogatePidSet.iterator(); it.hasNext(); ) {
        Integer pid = it.next();
        long pidRSS = getRSSAnonBytesFromPid(pid);
        // If we get an RSS of 0 then the process is probably no longer alive so we remove it from
        // the check
        if (pidRSS == 0) {
          it.remove();
        } else {
          surrogates.add(new SurrogateInfo(pid, pidRSS));
        }
      }
    }
    SurrogateStatus surrogateStatus = new SurrogateStatus(surrogates);
    return surrogateStatus;
  }

  private static class SurrogateStatus {
    final List<SurrogateInfo> surrogates;
    final long totalSurrogateBytes;

    SurrogateStatus(List<SurrogateInfo> surrogates) {
      this.surrogates = surrogates;
      totalSurrogateBytes = surrogates.stream().mapToLong(s -> s.anonRss).sum();
    }
  }

  private static class SurrogateInfo {
    final long anonRss;
    final int pid;

    SurrogateInfo(int pid, long anonRss) {
      this.anonRss = anonRss;
      this.pid = pid;
    }
  }

  // returns RSS from the given pid in bytes. pid=-1 means self process
  private static long getRSSBytesFromPid(int pid) {
    return getProcStatusValueBytes(pid, pRSS);
  }

  private static long getRSSAnonBytesFromPid(int pid) {
    return getProcStatusValueBytes(pid, pRSSAnon);
  }

  private static long getProcStatusValueBytes(int pid, Pattern pat) {
    if (!win) {
      long rss = 0; // in kb
      String procStatus = pid == -1 ? "/proc/self/status" : "/proc/" + pid + "/status";
      try (BufferedReader br = new BufferedReader(new FileReader(procStatus))) {
        String line;
        while ((line = br.readLine()) != null) {
          Matcher mRSS = pat.matcher(line); // example line: "VmRSS:   4943460 kB"
          if (mRSS.matches()) {
            // values are in KB but we return bytes
            return 1024 * Long.parseLong(mRSS.group(1));
          }
        }
      } catch (IOException | NumberFormatException e) {
        String proc = pid == -1 ? "self" : String.valueOf(pid);
        log.error("cannot obtain RSS(" + proc + ") " + e);
      }
    }
    return 0L;
  }

  private static long dumpSurrogateRSS() {
    if (!win) {
      if (surrogateLightLog) {
        dumpSurrogateRSSLight();
      } else {
        dumpSurrogateRSSFull();
      }
    }
    return 0L;
  }

  private static String procStatus(int pid) {
    return pid == -1 ? "/proc/self/status" : "/proc/" + pid + "/status";
  }

  private static void dumpSurrogateRSSFull() {
    synchronized (surrogatePidSet) {
      for (int pid : surrogatePidSet) {
        String status = procStatus(pid);
        try (BufferedReader br = new BufferedReader(new FileReader(status))) {
          String line;
          while ((line = br.readLine()) != null) {
            log.info(pid + ": " + line);
          }
        } catch (IOException | NumberFormatException e) {
          log.error("cannot obtain RSS(" + status + ") " + e);
        }
      }
    }
  }

  private static final String POS_COLUMN = "Pos";
  private static final String NAME_COLUMN = "Name";
  private static final String STATE_COLUMN = "State";
  private static final String PID_COLUMN = "Pid";
  private static final String PPID_COLUMN = "PPid";
  private static final String VM_PEAK_COLUMN = "VmPeak";
  private static final String VM_SIZE_COLUMN = "VmSize";
  private static final String VM_HWM_COLUMN = "VmHWM";
  private static final String VM_RSS_COLUMN = "VmRSS";
  private static final String THREADS = "Threads";
  private static final String ERROR_COLUMN = "Error";
  private static final String EMPTY = "";
  private static final String FORMAT =
      "| %-15s | %-15s | %-15s | %-15s | %-15s | %-15s | %-15s | %-15s | %-15s | %-15s | %-15s";
  private static final String HEADERS =
      String.format(
          FORMAT,
          POS_COLUMN,
          NAME_COLUMN,
          STATE_COLUMN,
          PID_COLUMN,
          PPID_COLUMN,
          VM_PEAK_COLUMN,
          VM_SIZE_COLUMN,
          VM_HWM_COLUMN,
          VM_RSS_COLUMN,
          THREADS,
          ERROR_COLUMN);

  private static void dumpSurrogateRSSLight() {
    synchronized (surrogatePidSet) {
      List<Integer> surrogatePidList = new ArrayList<>(surrogatePidSet);
      String rows =
          IntStream.range(0, surrogatePidList.size())
              .mapToObj(index -> surrogateRSSRow(index, surrogatePidList.get(index)))
              .collect(Collectors.joining(System.lineSeparator()));
      log.info(System.lineSeparator() + HEADERS + System.lineSeparator() + rows);
    }
  }

  private static String surrogateRSSRow(int index, int pid) {
    int pos = index + 1;
    Path path = Paths.get(procStatus(pid));
    try (Stream<String> lines = Files.lines(path)) {
      Map<String, String> metrics =
          lines
              .filter(line -> line.matches(".+:.+"))
              .collect(
                  Collectors.toMap(
                      line -> line.split(":")[0].trim(), line -> line.split(":")[1].trim()));

      return String.format(
          FORMAT,
          pos,
          metrics.getOrDefault(NAME_COLUMN, EMPTY),
          metrics.getOrDefault(STATE_COLUMN, EMPTY),
          metrics.getOrDefault(PID_COLUMN, EMPTY),
          metrics.getOrDefault(PPID_COLUMN, EMPTY),
          metrics.getOrDefault(VM_PEAK_COLUMN, EMPTY),
          metrics.getOrDefault(VM_SIZE_COLUMN, EMPTY),
          metrics.getOrDefault(VM_HWM_COLUMN, EMPTY),
          metrics.getOrDefault(VM_RSS_COLUMN, EMPTY),
          metrics.getOrDefault(THREADS, EMPTY),
          EMPTY);
    } catch (IOException e) {
      return String.format(
          FORMAT,
          pos,
          EMPTY,
          EMPTY,
          EMPTY,
          EMPTY,
          EMPTY,
          EMPTY,
          EMPTY,
          EMPTY,
          EMPTY,
          e.getMessage());
    }
  }

  private static long getSelfRSS() {
    return getRSSBytesFromPid(-1);
  }

  // TCMALLOC
  // o.g.GCNative 32110 - native heapsize (TCMalloc): 101 MB JVM commit: 1064 MB RSS: 880 MB recent
  // JVM variation: 0
  // MB recent RSS variation: 2 MB
  // GLIBC - malloc_info:false (default)
  // o.g.GCNative 32295 - native heapsize (RSS-JVM): -267 MB malloc_info: 241 MB JVM commit: 1664 MB
  // RSS: 1397 MB
  // recent JVM variation: 30 MB recent RSS variation: 89 MB
  // GLIBC - malloc_info:true
  // o.g.GCNative 33065 - native heapsize (malloc_info): 276 MB JVM commit: 1364 MB RSS: 1482 MB
  // recent JVM
  // variation: 32 MB recent RSS variation: 11 MB
  private static Sizes sizes = null;

  private static class Sizes {
    long logSizesTime;
    String allocatorName;
    int nativeAlloc;
    int managedAlloc;
    int jvmFootprint;
    SurrogateStatus surrogate;
    int surrogateRss;
    int heapUsed;
    int rss;
    int rssVariation;
    int jvmVariation;
    int finalizerCount;
    int hash;
    String finalizerQueue;

    void log() {
      StringJoiner sj = new StringJoiner(" ");
      sj.add("native heapsize (" + allocatorName + "): " + nativeAlloc + " MB");
      if (allocator == AllocatorType.JVM) {
        sj.add("malloc_info: " + inMB(getMallocInfoSize()) + " MB");
      }
      sj.add("JVM commit: " + jvmFootprint + " MB");
      sj.add("RSS: " + rss + " MB");
      sj.add("Surrogate RSS(size: " + surrogate.surrogates.size() + "): " + surrogateRss + " MB");

      sj.add("recent JVM variation: " + jvmVariation + " MB");
      sj.add("recent RSS variation: " + rssVariation + " MB");
      sj.add("finalizerCount=" + finalizerCount);
      sj.add("finalizerQueue=" + finalizerQueue);
      sj.add("managedSize=" + managedAlloc + " MB");

      log.info(sj.toString());

      if (surrogate.totalSurrogateBytes > 1 * 1024 * 1024 * 1024) {
        dumpSurrogateRSS();
      }
    }

    void logSurrogateDetails() {
      dumpSurrogateRSS();
    }

    void writeCrumb() {
      GCMonitor.gcCrumb(
          crumbDesc,
          Properties.gcNative().elem("sizes"),
          Properties.gcNativeIndex().elem(gcIndex),
          Properties.gcNativeAlloc().elem(nativeAlloc),
          Properties.gcNativeManagedAlloc().elem(managedAlloc),
          Properties.gcNativeJVMFootprint().elem(jvmFootprint),
          Properties.gcNativeJVMHeap().elem(heapUsed),
          Properties.gcNativeRSS().elem(rss),
          Properties.gcNativeSurrogateRSS().elem(surrogateRss),
          Properties.gcFinalizerCount().elem(finalizerCount),
          Properties.gcFinalizerQueue().elem(finalizerQueue),
          Properties.gcNativeAllocator().elem(allocator.name));
    }
  }

  private static void updateAndLogSizes(long abateIfNoChangeMs) {
    Sizes newSizes = new Sizes();
    newSizes.logSizesTime = System.currentTimeMillis();
    newSizes.allocatorName = allocator.name;
    newSizes.nativeAlloc = inMB(getNativeAllocation());
    newSizes.managedAlloc = inMB(managedSize());
    newSizes.jvmFootprint = inMB(getJVMFootprint());
    newSizes.surrogate = getSurrogateStatus();
    newSizes.surrogateRss = inMB(newSizes.surrogate.totalSurrogateBytes);
    MemoryUsage heap = memoryBean.getHeapMemoryUsage();
    // Note that this is inaccurate if a GC hasn't been done recently.
    newSizes.heapUsed = inMB(heap.getUsed());
    newSizes.rss = inMB(getSelfRSS());
    newSizes.rssVariation = inMB(recentRSSUncertainty());
    newSizes.jvmVariation = inMB(recentJVMHeapUncertainty());
    newSizes.finalizerCount = memoryBean.getObjectPendingFinalizationCount();
    newSizes.hash = newSizes.nativeAlloc ^ newSizes.heapUsed ^ newSizes.rss;
    if (sizes != null
        && newSizes.hash == sizes.hash
        && abateIfNoChangeMs > 0l
        && newSizes.logSizesTime < (sizes.logSizesTime + abateIfNoChangeMs)) {
      return;
    }

    newSizes.finalizerQueue = newSizes.finalizerCount > 0 ? readFinalizerQueue() : "empty";
    sizes = newSizes;

    newSizes.log();
    newSizes.writeCrumb();
    ;
  }

  private static int inMB(long val) {
    return (int) (val / (1024 * 1024));
  }

  // These variables are only accessed by the gcnative thread
  private static long watermark;
  private static long initialWatermark; // unadjusted by steady state
  private static int maxClears;
  private static long steadyState;
  private static long emergencyWatermark;
  private static long clampedWatermark;

  // to delay DescriptiveStatistics init, since it's relatively expensive (always called in a
  // synchronized block)
  private static void initializeDescriptiveStatistics() {
    if (jvmHeapTracker == null) jvmHeapTracker = new DescriptiveStatistics(slidingWindowSize);
    if (rssTracker == null) rssTracker = new DescriptiveStatistics(slidingWindowSize);
  }

  private static synchronized void updateSlidingWindow() {
    initializeDescriptiveStatistics();
    jvmHeapTracker.addValue(getJVMFootprint());
    rssTracker.addValue(getNativeAllocation());
  }

  private static synchronized long recentJVMHeapUncertainty() {
    initializeDescriptiveStatistics();
    return (long) (jvmHeapTracker.getMax() - jvmHeapTracker.getMin());
  }

  private static synchronized long recentRSSUncertainty() {
    initializeDescriptiveStatistics();
    return (long) (rssTracker.getMax() - rssTracker.getMin());
  }

  // TODO: (OPTIMUS-66467) Remove guard after some observation of gcNativeCacheClearCount to assure
  // ourselves that apps won't be shut down erroneously, as this feature hasn't worked for years.
  private static String ActuallyShutdownBasedOnMaxClears =
      "optimus.gcnative.actuallyShutdownBasedOnMaxClears";
  private static boolean actuallyShutdownBasedOnMaxClears =
      DiagnosticSettings.getBoolProperty(ActuallyShutdownBasedOnMaxClears, false);

  private static void thread() {
    maxClears =
        getGCNativeIntegerSetting(
            Integer.MAX_VALUE, maxAllClears, legacyMaxClearsEnv, "max total clears");
    steadyState =
        getGCNativeSetting(
            250 * 1024L * 1024, steadyStateMB, legacySteadyStateEnv, "initial steady state mark");
    log.info(
        "GC Native is up... wm: "
            + inMB(watermark)
            + " MB  ewm: "
            + inMB(emergencyWatermark)
            + " MB");
    log.info("LD_PRELOAD=" + System.getenv("LD_PRELOAD"));
    log.info(
        "1. The application will terminate when native heap is above ewm ("
            + inMB(emergencyWatermark)
            + " MB)");
    log.info(
        "2. The application will terminate if 'clear ALL caches' takes place "
            + maxClears
            + " times");
    log.info(
        "3. The watermark (but not the ewm) will be raised at every 'clear ALL caches' event that fails to bring "
            + "heap"
            + " down");
    log.debug("Noting that OGTrace is set to " + OGTraceIsolation.isTracing());
    if (!win) {
      if (tcmallocLoaded()) {
        log.info(
            "libtcmalloc.so is loaded; will use generic.current_allocated_bytes to measure native memory use");
        if (useMallocInfo) log.warn("malloc_info was enabled but will not be used");
      } else if (jemallocLoaded()) {
        log.info("Using jemalloc; will use mallctl to measure native memory use");
        if (useMallocInfo) log.warn("malloc_info was enabled but will not be used");
        if (jemallocProfiling()) log.info("jemalloc profiling enabled");
      } else if (useMallocInfo) {
        log.info(
            "custom allocator is not loaded, malloc_info enabled: will use malloc_info to measure native memory use");
      } else {
        log.info(
            "custom allocator is not loaded, malloc_info disabled: will use RSS-JVM to measure native memory use");
      }
    }
    try {
      Thread.sleep(
          getGCNativeIntegerSetting(
                  10, startupQuietSeconds, legacyStartupQuietSecondsEnv, "startup quiet period")
              * 1000);
      // 15 seconds on Windows (heap walk locks the heaps while walking)
      // 1 second on Linux (native byte count is cheap)
      int periodMs =
          getGCNativeIntegerSetting(
              win ? 15000 : 1000, timerPeriodMs, legacyTimerPeriodMsEnv, "timer period");
      log.info("timer period, millisecs: " + periodMs);
      long lastLogTime = System.currentTimeMillis();
      while (!shutdownRequested.get()) {
        updateSlidingWindow();
        long currentSize = doCleanup(emergencyWatermark);
        long now = System.currentTimeMillis();
        if ((now - lastLogTime) > periodicLoggingIntervalMs) {
          updateAndLogSizes(300000);
          lastLogTime = now;
        }
        Thread.sleep(periodMs);
      }
    } catch (InterruptedException ex) {
      log.info("sleep interrupted");
    }
  }

  // The following algorithm runs on the GCNative thread
  private static final int CLEARSTATE_NONE = 0;
  private static final int CLEARSTATE_NORMAL = 1;
  private static final int CLEARSTATE_ALL = 2;
  private static final int CLEARSTATE_CALLBACKS = 3;
  private static int lastClear = CLEARSTATE_NONE;
  private static long oldSize = 0;
  private static int countGCed = 0;
  private static int countAllClears = 0;
  private static int countEmergencyWatermark = 0;
  private static long hiMark = 0;

  private static int gcIndex = 0;

  private static final class GCNativeTrigger implements TrackingGraphCleanupTrigger {}

  private static final GCNativeTrigger trigger = new GCNativeTrigger();

  private static ReferenceQueue<Object> referenceQueue = null;
  private static Object referenceLock;
  private static Method referenceQueueForeach;

  static {
    try {
      Class<?> finalizerClass = Class.forName("java.lang.ref.Finalizer");
      Class<?> referenceQueueClass = Class.forName("java.lang.ref.ReferenceQueue");
      Field queueStaticField = finalizerClass.getDeclaredField("queue");
      queueStaticField.setAccessible(true);
      referenceQueue = (ReferenceQueue) queueStaticField.get(null);
      Class<?> classDefiningLock;
      if (Runtime.version().feature() < 19) classDefiningLock = referenceQueueClass;
      // the lock field moved to NativeReferenceQueue in Java 19
      else classDefiningLock = Class.forName("java.lang.ref.NativeReferenceQueue");
      Field lockField = classDefiningLock.getDeclaredField("lock");
      lockField.setAccessible(true);
      referenceLock = lockField.get(referenceQueue);
      referenceQueueForeach = referenceQueueClass.getDeclaredMethod("forEach", Consumer.class);
      referenceQueueForeach.setAccessible(true);
    } catch (Exception e) {
      log.warn("Failed to access finalizer queue", e);
      referenceQueue = null;
    }
  }

  private static long delayQueueReadingUntil = 0l;

  public static String readFinalizerQueue() {
    return readFinalizerQueue(true);
  }

  public static String readFinalizerQueue(boolean autoDelay) {
    if (maxFinalizerClasses <= 0) {
      return "disabled";
    }
    long t0 = System.nanoTime();
    if (autoDelay && t0 < delayQueueReadingUntil) {
      return "delayed";
    }
    Map<Class<?>, Long> counts = new HashMap<Class<?>, Long>();
    try {
      synchronized (referenceLock) {
        referenceQueueForeach.invoke(
            referenceQueue,
            new Consumer<Reference<?>>() {
              @Override
              public void accept(Reference<?> r) {
                Object o = r.get();
                if (o != null) {
                  Class<?> c = o.getClass();
                  if (c != null) {
                    counts.compute(c, (c1, i) -> i == null ? 1 : i + 1);
                  }
                }
              }
            });
      }
      if (counts.size() == 0) {
        return "empty";
      }
      long total =
          (long)
              (counts.values().stream().collect(Collectors.summingLong(Long::longValue))
                  * finalizerCutoffFraction);
      Object entries[] = counts.entrySet().toArray();
      Arrays.sort(
          entries,
          (o1, o2) ->
              -Long.compare(
                  ((Map.Entry<Class<?>, Long>) o1).getValue(),
                  ((Map.Entry<Class<?>, Long>) o2).getValue()));
      StringBuilder sb = new StringBuilder();
      int i = 0;
      long subTotal = 0;
      for (Object o : entries) {
        if (i > 0) {
          sb.append(',');
        }
        i += 1;
        Map.Entry<Class<?>, Long> entry = (Map.Entry<Class<?>, Long>) o;
        sb.append((entry).getKey().getCanonicalName());
        sb.append('(');
        sb.append(entry.getValue());
        sb.append(')');
        subTotal += entry.getValue();
        if (i > maxFinalizerClasses || subTotal > total) {
          break;
        }
      }
      return sb.toString();
    } catch (Exception e) {
      log.warn("Failed to read finalizer queue", e);
      return "error";
    } finally {
      long t1 = System.nanoTime();
      // Refuse to spend more than 1/1000 of our time reading queues.
      delayQueueReadingUntil = t1 + (t1 - t0) * 1000;
    }
  }

  private static long doCleanup(long emergencyWatermark) {
    long recentUncertaintyCorrection = !useMallocInfo ? recentJVMHeapUncertainty() : 0;
    long currentSize = getNativeAllocation();
    long managedSize = managedSize();
    // on the first pass it will equal currentSize so that "change since last call" is zero
    oldSize = currentSize;
    gcIndex += 1;
    if (currentSize >= emergencyWatermark) {
      countEmergencyWatermark++;
    } else {
      countEmergencyWatermark = 0;
    }

    if (sizes != null && sizes.surrogateRss > surrogateMaxRSSMB) {
      log.error(
          "Surrogate RSS has exceed the limit: "
              + sizes.surrogateRss
              + " MB > "
              + surrogateMaxRSSMB
              + " MB");
      sizes.log();
      // orderly shutdown will include surrogate shutdown in shutdown hook
      GCNative.clearCacheExt(LEVEL_SHUTDOWN, currentSize, currentSize - oldSize);
    }

    if (currentSize >= (emergencyWatermark + recentUncertaintyCorrection)) {
      log.info(
          "native heap "
              + inMB(currentSize)
              + " MB exceeds hard limit "
              + inMB(emergencyWatermark)
              + " MB (despite uncertainty correction of "
              + inMB(recentUncertaintyCorrection)
              + " MB) Exiting immediately"
              + (lastClear == CLEARSTATE_NONE
                  ? " (note: clear not attempted due to extreme allocation rate)"
                  : ""));
      updateAndLogSizes(0);
      jemallocDump("emergency-shutdown-" + gcIndex);
      dumpTCMallocInfo();
      GCNative.clearCacheExt(LEVEL_SHUTDOWN, currentSize, currentSize - oldSize);
    } else if (currentSize >= watermark) {
      if (currentSize >= emergencyWatermark) {
        log.info(
            "native heap "
                + inMB(currentSize)
                + " MB exceeded hard limit ("
                + inMB(emergencyWatermark)
                + " MB) but we will not kill yet as we can't rule out this being due to recent jvm "
                + "heap "
                + "movement of "
                + inMB(recentUncertaintyCorrection)
                + " MB");
      }

      log.info("About to GC (size: " + inMB(currentSize) + " MB)");
      updateAndLogSizes(0);
      GCNative.clearCacheExt(LEVEL_GC, currentSize, currentSize - oldSize);

      // recompute currentSize
      oldSize = currentSize;
      currentSize = getNativeAllocation();

      // Check if we are staying above low water mark
      if (currentSize >= watermark * lowWMFactor) {
        if (dynamicWMEnabled
            && lastClear == CLEARSTATE_CALLBACKS
            && countGCed == 0
            && currentSize > steadyState) {
          // if this was right after clearall with callbacks
          // if we are here, size after the most aggressive cache clear stays above steadyState.
          // This could mean
          // * we leaked native memory (and this new level is forever)
          // * there's a non-optimus cache that didn't register a callback (and this new level may
          // be forever or short)
          // * current nodes on stack/queue really need that much native heap (and this sill clear
          // out soon)
          // In any case, we need to stop expecting to dip below the watermark, it's not happening
          // for a while
          {
            // when backed by glibc malloc, there is no steady state to speak of. We just gently
            // grow the watermark.
            steadyState = currentSize;
            watermark *= 1.0 + dynamicWMGrowthFactor;
          }
          // if WM is 4 G and after total cache clear we're at 5 G, WM becomes 9 G to once again
          // give 4 G of headroom
          // for graph to use.
          if (watermark > emergencyWatermark * dynamicWMClampFactor) {
            // ...except the hard limit can't be exceeded, so we'll set WM just below it, to give
            // GCNative a chance
            // to try a cache clear and see if whatever caused steady state to grow has eased off
            watermark = (long) (emergencyWatermark * dynamicWMClampFactor);
          }
          log.info(
              "Raising watermark to "
                  + inMB(watermark)
                  + " MB, steady state mark to "
                  + inMB(steadyState)
                  + " MB, low watermark to "
                  + inMB((long) (watermark * lowWMFactor))
                  + " MB");
        }
        countGCed++;
      } else { // We just dipped below watermark, that last cache clear was a success
        if (countGCed != 0) {
          log.info(
              "Native heap dipped below the low watermark "
                  + inMB((long) (watermark * lowWMFactor))
                  + " MB. Resetting.");
        }
        countGCed = 0;
        lastClear = CLEARSTATE_NONE; // Forgive full clear
        countAllClears = 0; // reset the doomsday clock as well
        if (dynamicWMEnabled && (currentSize < steadyState)) {
          // we did not just dip below lowWM, we dropped below steadyState! Whatever caused it, is
          // gone.
          steadyState = currentSize;
          watermark = initialWatermark;
          log.info(
              "Lowering watermark to "
                  + inMB(watermark)
                  + " MB, steady state mark to "
                  + inMB(steadyState)
                  + " MB, low watermark to "
                  + inMB((long) (watermark * lowWMFactor))
                  + " MB");
        }
        // no action, but we should report the native heap change to the Java side
        GCNative.clearCacheExt(LEVEL_INFO_ONLY, currentSize, currentSize - oldSize);
      }

      log.info(
          "After GC (size: "
              + inMB(currentSize)
              + " MB) count to clear cache "
              + (resetGCCount - countGCed));

      // If memory is mostly due to managed threads, hold off on cache clears.
      if (managedPercent > 0.0 && managedSize > currentSize * managedPercent * 0.01) {
        log.info(
            "Deferring cache clears since managedSize "
                + managedSize
                + " > "
                + managedPercent
                + " % of total native "
                + currentSize);
        return currentSize;
      }

      if (countGCed >= resetGCCount || countEmergencyWatermark >= slidingWindowSize / 2) {
        // did not dip below watermark after this GC (and after 4 earlier ones, too)
        // or is above the emergency watermark
        log.info("About to clear caches (size: " + inMB(currentSize) + " MB)");
        if (lastClear
            != CLEARSTATE_NONE) { // did not dip below watermark after the most recent cache clear,
          // try ALL
          if (countAllClears >= maxClears) {
            log.error("GC native would exceed max clear ALL caches attempts (" + maxClears + ")!");
            if (actuallyShutdownBasedOnMaxClears)
              GCNative.clearCacheExt(LEVEL_SHUTDOWN, currentSize, currentSize - oldSize);
            else
              log.error(
                  "Exiting has been suppressed because "
                      + ActuallyShutdownBasedOnMaxClears
                      + " is false");
          }
          log.info(
              "About to clear ALL caches since not enough memory was recovered (size: "
                  + inMB(currentSize)
                  + " MB)");
          dumpTCMallocInfo();
          if (lastClear == CLEARSTATE_ALL || lastClear == CLEARSTATE_CALLBACKS) {
            // last clear was also an ALL clear; run extra user cleaners
            GCNative.clearCacheExt(
                LEVEL_CLEAR_ALL_CACHES_CALLBACKS, currentSize, currentSize - oldSize);
            lastClear = CLEARSTATE_CALLBACKS;
          } else {
            GCNative.clearCacheExt(
                LEVEL_CLEAR_ALL_GLOBAL_CACHES, currentSize, currentSize - oldSize);
            lastClear = CLEARSTATE_ALL;
          }
          // The increment was originally predicated on use of the MTS allocator, and
          // was removed entirely when we switched to tcmalloc.  There's no obvious reason
          // why it should be associated with _any_ particular allocator, but we assume that
          // the "alternative" (currently jemalloc and tcmalloc) allocators report more accurate
          // sizes.
          if (altMallocLoaded()) countAllClears++;
        } else { // low wm was seen since the last cache clear, try regular cache clear
          GCNative.clearCacheExt(LEVEL_CLEAR_MAIN_CACHE, currentSize, currentSize - oldSize);
          lastClear = CLEARSTATE_NORMAL;
        }
        countGCed = 0; // Cleared caches let's wait for a few GCs
        currentSize = getNativeAllocation();

        DependencyTrackerRoot$.MODULE$.runCleanupNow(trigger);
      }
    } else {
      // no action, but we should report the native heap change to the Java side
      GCNative.clearCacheExt(LEVEL_INFO_ONLY, currentSize, currentSize - oldSize);
    }

    if (currentSize > hiMark) {
      log.info("New high: " + inMB(currentSize) + " MB");
      if (hiMark == 0
          && currentSize > steadyState) { // first new high already higher than steady state
        log.info(
            "also updating steady state from "
                + inMB(steadyState)
                + " to "
                + inMB(currentSize)
                + " MB");
        steadyState = currentSize;
      }
      hiMark = currentSize;
    }
    oldSize = currentSize;
    return currentSize;
  }

  /** Obtains the current native heap used size, in bytes */
  // the format is fixed (this isn't really XML, just a bunch of printfs in glibc's malloc_info)
  private static Pattern mallocInfoRegex =
      compile("^.*<system type=\"current\" size=\"(\\d*)\"/>.*$", Pattern.DOTALL);

  private static long getMallocInfoSize() {
    String minfo = getMallocInfo();
    // from malloc_info, extract the one <system type="current" size="3286491136"/> that appears
    // outside of any heap
    // (there are several heaps marked with <heap nr=...>...</heap>, each with its own current size)
    // we will look for the last </heap> and start from there
    int beg = minfo.lastIndexOf("</heap>");
    if (beg == -1) {
      log.error("Failure parsing malloc_info: no </heap> in " + minfo);
      return 0;
    }
    minfo = minfo.substring(beg);
    Matcher regexMatcher = mallocInfoRegex.matcher(minfo);
    if (regexMatcher.matches()) {
      String heap = regexMatcher.group(1);
      try {
        return Long.parseLong(heap);
      } catch (NumberFormatException e) {
        log.error("Failure parsing malloc_info: " + heap + " is not a number");
        return 0;
      }
    } else {
      log.info("parsing malloc_info failed: no match in " + minfo);
      return 0;
    }
  }

  public static int getNativeAllocationMB() {
    return inMB(getNativeAllocation());
  }

  public static long getNativeAllocation() {
    if (jemallocLoaded()) return jemallocSize();
    else if (tcmallocLoaded()) {
      return getTCMallocByteCounts();
    } else {
      return useMallocInfo ? getMallocInfoSize() : getSelfRSS() - getJVMFootprint();
    }
  }

  private static void dumpTCMallocInfo() {
    dumpTCMallocInfo(null);
  }

  public static void dumpTCMallocInfo(StringBuilder sb) {
    if (tcmallocLoaded()) {
      Stream.concat(
              MallocExtensionProperty.Generic.values.stream(),
              MallocExtensionProperty.TCMalloc.values.stream())
          .forEach(
              prop -> {
                if (prop instanceof MallocExtensionProperty.Readable) {
                  long v =
                      getTcMallocExtensionNumericProperty((MallocExtensionProperty.Readable) prop);
                  if (sb == null) log.info(prop.getName() + "=" + v);
                  else {
                    sb.append(prop.getName());
                    sb.append("=");
                    sb.append(v);
                    sb.append("\n");
                  }
                }
              });
    }
  }

  private static native String getMallocInfo();

  private static native int tcMallocTrim(int pad);

  private static native long getTcMallocExtensionNumericProperty(String property);

  private static long getTcMallocExtensionNumericProperty(
      MallocExtensionProperty.Readable property) {
    return getTcMallocExtensionNumericProperty(property.getName());
  }

  private static native void setTcMallocExtensionNumericProperty(String property, long value);

  private static void setTcMallocExtensionNumericProperty(
      MallocExtensionProperty.Writable property, long value) {
    setTcMallocExtensionNumericProperty(property.getName(), value);
  }

  private static long getTCMallocByteCounts() {
    return getTcMallocExtensionNumericProperty(
        MallocExtensionProperty.Generic.current_allocated_bytes);
  }

  interface MallocExtensionProperty {
    public String getName();

    class MallocExtensionPropertyBase implements MallocExtensionProperty {
      private final String name;

      MallocExtensionPropertyBase(String name) {
        this.name = name;
      }

      public String getName() {
        return name;
      }
    }

    class ReadOnly extends MallocExtensionPropertyBase implements Readable {
      ReadOnly(String name) {
        super(name);
      }
    }

    class WriteOnly extends MallocExtensionPropertyBase implements Writable {
      WriteOnly(String name) {
        super(name);
      }
    }

    class ReadWrite extends MallocExtensionPropertyBase implements Writable, Readable {
      ReadWrite(String name) {
        super(name);
      }
    }

    interface Readable extends MallocExtensionProperty {}

    interface Writable extends MallocExtensionProperty {}

    // taken from gperftools/malloc_extensions.h
    // -------------------------------------------------------------------
    // Control operations for getting and setting malloc implementation
    // specific parameters.  Some currently useful properties:
    static class Generic {
      // "generic.current_allocated_bytes"
      //      Number of bytes currently allocated by application
      //      This property is not writable.
      static ReadOnly current_allocated_bytes = new ReadOnly("generic.current_allocated_bytes");

      // "generic.heap_size"
      //      Number of bytes in the heap ==
      //            current_allocated_bytes +
      //            fragmentation +
      //            freed memory regions
      //      This property is not writable.
      static ReadOnly heap_size = new ReadOnly("generic.heap_size");

      static List<MallocExtensionProperty> values =
          Arrays.asList(current_allocated_bytes, heap_size);
    }

    static class TCMalloc {
      // "tcmalloc.max_total_thread_cache_bytes"
      //      Upper limit on total number of bytes stored across all
      //      per-thread caches.  Default: 16MB.
      static ReadWrite max_total_thread_cache_bytes =
          new ReadWrite("tcmalloc.max_total_thread_cache_bytes");

      // "tcmalloc.current_total_thread_cache_bytes"
      //      Number of bytes used across all thread caches.
      //      This property is not writable.
      static ReadOnly current_total_thread_cache_bytes =
          new ReadOnly("tcmalloc.current_total_thread_cache_bytes");

      // "tcmalloc.central_cache_free_bytes"
      //      Number of free bytes in the central cache that have been
      //      assigned to size classes. They always count towards virtual
      //      memory usage, and unless the underlying memory is swapped out
      //      by the OS, they also count towards physical memory usage.
      //      This property is not writable.
      static ReadOnly central_cache_free_bytes = new ReadOnly("tcmalloc.central_cache_free_bytes");

      // "tcmalloc.transfer_cache_free_bytes"
      //      Number of free bytes that are waiting to be transfered between
      //      the central cache and a thread cache. They always count
      //      towards virtual memory usage, and unless the underlying memory
      //      is swapped out by the OS, they also count towards physical
      //      memory usage. This property is not writable.
      static ReadOnly transfer_cache_free_bytes =
          new ReadOnly("tcmalloc.transfer_cache_free_bytes");

      // "tcmalloc.thread_cache_free_bytes"
      //      Number of free bytes in thread caches. They always count
      //      towards virtual memory usage, and unless the underlying memory
      //      is swapped out by the OS, they also count towards physical
      //      memory usage. This property is not writable.
      static ReadOnly thread_cache_free_bytes = new ReadOnly("tcmalloc.thread_cache_free_bytes");

      // "tcmalloc.pageheap_free_bytes"
      //      Number of bytes in free, mapped pages in page heap.  These
      //      bytes can be used to fulfill allocation requests.  They
      //      always count towards virtual memory usage, and unless the
      //      underlying memory is swapped out by the OS, they also count
      //      towards physical memory usage.  This property is not writable.
      static ReadOnly pageheap_free_bytes = new ReadOnly("tcmalloc.pageheap_free_bytes");

      // "tcmalloc.pageheap_unmapped_bytes"
      //        Number of bytes in free, unmapped pages in page heap.
      //        These are bytes that have been released back to the OS,
      //        possibly by one of the MallocExtension "Release" calls.
      //        They can be used to fulfill allocation requests, but
      //        typically incur a page fault.  They always count towards
      //        virtual memory usage, and depending on the OS, typically
      //        do not count towards physical memory usage.  This property
      //        is not writable.
      static ReadOnly pageheap_unmapped_bytes = new ReadOnly("tcmalloc.pageheap_unmapped_bytes");

      static List<MallocExtensionProperty> values =
          Arrays.asList(
              max_total_thread_cache_bytes,
              current_total_thread_cache_bytes,
              central_cache_free_bytes,
              transfer_cache_free_bytes,
              thread_cache_free_bytes,
              pageheap_free_bytes,
              pageheap_unmapped_bytes);
    }
  }

  public static long getNativeWatermark() {
    return watermark;
  }

  private static long getNativeWatermarkMB() {
    return inMB(watermark);
  }

  public static long getHighWatermark() {
    return hiMark;
  }

  public static void setNativeWatermark(long size) {
    initialWatermark = size;
    watermark = size;
  }

  public static long getEmergencyWatermark() {
    return emergencyWatermark;
  }

  public static int getEmergencyWatermarkMB() {
    return inMB(emergencyWatermark);
  }

  public static long getClampedWatermarkMB() {
    return inMB(clampedWatermark);
  }

  private static void setEmergencyWatermark(long size) {
    emergencyWatermark = size;
    clampedWatermark = (long) (size * dynamicWMClampFactor);
  }

  public static void resetForNewTask() {
    log.info("resetting for new task");
    countAllClears = 0;
  }

  public static void shutdownMonitoringThread() {
    shutdownRequested.set(true);
  }

  private static native void shutdown();

  public static final Watchdog watchdog = new Watchdog();

  private static native void exitOnGcPauseOverLimit(long seconds);

  private static native void exitOnProcessPauseOverLimit(long seconds);

  private static native void updateAliveTime();

  private static native void startDetectNativeThread(long detectDuration);

  private static native void shutdownNative();

  private static native int jemallocDumpC(String file);

  public static native long jemallocSizeC(String stat);

  private static native int jemallocLoadC();

  private static native int tcmallocLoadC();

  private static native int manageThreadC(long maxAlloc, long maxManaged, long sleepMs);

  private static native long managedSize();

  public static int managedSizeMB() {
    return inMB(managedSize());
  }

  public static native String[] logMessages(int n);

  private static native String jemallocStatsC(String opts);

  public static native long memoryWastingTest(long bytes, int n, long sleepMs, boolean doLock);

  public static synchronized boolean altMallocLoaded() {
    return tcmallocLoaded() || jemallocLoaded();
  }

  public static synchronized boolean tcmallocLoaded() {
    if (noTcMalloc) return false;
    if (tcmallocAPILoaded) return true;
    if (allocator != AllocatorType.TCMALLOC) return false;
    if (tcmallocLoadC() == 0) {
      tcmallocAPILoaded = true;
      return true;
    } else {
      noTcMalloc = true;
      return false;
    }
  }

  private static final Pattern jemallocPattern =
      Pattern.compile("(.*:)?([^\\s:]*jemalloc(-debug)?.so.2)\\b.*");
  private static boolean nojemalloc = win;

  public static boolean usingJemalloc() {
    return jemallocLoaded();
  }

  public static boolean jemallocProfiling() {
    return jemallocProfilingConfig;
  }

  private static synchronized boolean jemallocLoaded() {
    if (nojemalloc) {
      return false;
    }
    if (jemallocAPILoaded) {
      return true;
    }
    ensureGcNativeLibraryLoaded();
    try {
      String preload = System.getenv("LD_PRELOAD");
      Matcher m = null;
      if (preload != null) {
        m = jemallocPattern.matcher(preload);
      }
      if (m != null && m.find()) {
        String conf = System.getenv("MALLOC_CONF");
        if (conf != null && conf.contains("prof:true") && !conf.contains("prof_active:false"))
          jemallocProfilingConfig = true;
        else {
          // jemalloc will still work, but profiling won't
          jemallocProfilingConfig = false;
          log.warn("jemalloc on ld path, but MALLOC_CONF!=prof:true; profiling is not enabled");
        }
        var status = jemallocLoadC();
        if (status != 0) {
          log.error("Failed to load jemalloc API: " + status);
        } else jemallocAPILoaded = true;
        GCMonitor.gcCrumb(
            crumbDesc,
            Properties.logMsg().apply("jemalloc loaded: " + m.group() + " MALLOC_CONF=" + conf));
        return jemallocAPILoaded;
      } else {
        nojemalloc = true;
        return false;
      }
    } catch (Throwable t) {
      log.error("Exception loading jemalloc", t);
      nojemalloc = true;
      return false;
    }
  }

  public static synchronized boolean manageThread(long maxAlloc, long maxManaged, long sleepMs) {

    return manageThreadC(maxAlloc, maxManaged, sleepMs) == 0;
  }

  public static synchronized int managedThreadCheck() {
    return manageThreadC(-1, 0, 0);
  }

  public static synchronized boolean unmanageThread() {
    return manageThreadC(0, 0, 0) == 0;
  }

  public static synchronized String jemallocStats() {
    if (!jemallocProfiling()) {
      return "ERROR:NOTLOADED";
    }
    String ret = jemallocStatsC("J");
    return ret;
  }

  public static synchronized Path jemallocDump(String prefix) {
    if (!jemallocProfiling()) return null;
    try {
      String tmpdir = System.getProperty("java.io.tmpdir");
      if (prefix == null) {
        prefix = "jeprof";
      }
      String pid = Long.toString(Pid.pidOrZero());
      String name = prefix + "-" + pid + "-" + Instant.now().toString() + ".heap";
      Path dir = Paths.get(tmpdir, "jeprof", pid);
      dir.toFile().mkdirs();
      Path path = dir.resolve(name);
      return jemallocDump(path);
    } catch (Throwable t) {
      log.error("Exception preparing for jemalloc dump", t);
      jemallocProfilingConfig = false;
      return null;
    }
  }

  public static synchronized Path jemallocDump(Path path) {
    if (!jemallocProfiling()) return null;
    else
      try {
        String fileName = path.toString();
        log.info("Invoking jemalloc mallctl(prof.dump) -> %s".format(fileName));
        ensureGcNativeLibraryLoaded();
        int res = jemallocDumpC(fileName);
        if (res != 0) {
          log.error("mallctl failed %d - jemalloc profiling disabled", res);
          jemallocProfilingConfig = false;
          return null;
        }
        return Paths.get(fileName);
      } catch (Throwable t) {
        log.error("Exception generating jemalloc dump", t);
        nojemalloc = true;
        return null;
      }
  }

  public static class JemallocSizes {
    public long allocated;
    public long active;
    public long retained;
    public long resident;
    public long mapped;
    public long metadata;

    static JemallocSizes get() {
      var sizes = new JemallocSizes();
      sizes.allocated = jemallocSizeC("stats.allocated");
      sizes.active = jemallocSizeC("stats.active");
      sizes.retained = jemallocSizeC("stats.retained");
      sizes.resident = jemallocSizeC("stats.resident");
      sizes.mapped = jemallocSizeC("stats.mapped");
      sizes.metadata = jemallocSizeC("stats.metadata");
      return sizes;
    }

    public Map<String, Integer> toMapMB() {
      Map<String, Integer> map = new HashMap<>();
      map.put("allocated", inMB(allocated));
      map.put("retained", inMB(retained));
      map.put("resident", inMB(resident));
      map.put("mapped", inMB(mapped));
      map.put("metadata", inMB(metadata));
      map.put("active", inMB(active));
      return map;
    }
  }

  public static JemallocSizes jemallocSizes() {
    if (!jemallocLoaded()) return null;
    else return JemallocSizes.get();
  }

  public static long jemallocSize() {
    if (!jemallocLoaded()) return -1;
    try {
      return jemallocSizeC("stats.allocated");
    } catch (Throwable t) {
      log.error("Exception calling jemallocSizeC", t);
      nojemalloc = true;
      return -1;
    }
  }

  private static final AtomicReference<Thread> detectThread = new AtomicReference(null);
  private static final AtomicBoolean livenessSuspended = new AtomicBoolean();

  private static void startLongPauseDetectThreads() {
    synchronized (detectThread) {
      if (detectThread.get() == null) {
        log.info("starting long pause detection threads...");

        Thread th =
            new Thread(
                () -> {
                  while (true) {
                    if (!livenessSuspended.get()) {
                      updateAliveTime();
                    }
                    try {
                      Thread.sleep(heartbeatDurationMs);
                    } catch (InterruptedException e) {
                      log.info("Process long pause detection thread is interrupted!", e);
                    }
                  }
                });
        th.setName("ServerAliveUpdateThread");
        th.setDaemon(true);
        th.start();

        startDetectNativeThread(heartbeatDurationMs);

        detectThread.set(th);
        log.info("long pause detection threads started");
      } else {
        log.warn("long pause detection threads have been started already!");
      }
    }
  }

  private static void stopLongPauseDetectThreads() {
    synchronized (detectThread) {
      Thread th = detectThread.get();
      if (th != null) {
        log.info("stopping long pause detection threads...");
        th.interrupt();
        shutdownNative();
        detectThread.set(null);
        log.info("long pause detection threads stopped");
      } else {
        log.warn("long pause detection threads are not running");
      }
    }
  }

  public static void taskCompleted() {
    // nothing to do for now
  }

  public static void dontExitOnGcPauseOverLimit() {
    exitOnGcPauseOverLimit(0);
  }

  /** Called by libgcnative.so to do logging in a way that matches the rest of optimus apps */
  // TODO(OPTIMUS-63323): Unsuppress when cppagent is fixed to call with CallStaticVoidMethod
  public static void logInfo_SUPPRESSED(String msg) {
    log.info(msg);
    GCMonitor.gcCrumb(
        crumbDesc, Properties.logMsg().elem(msg), Properties.gcNativeIndex().elem(gcIndex));
  }

  public static Tuple2<Boolean, Boolean> convertLevelToCleans(int level) {
    Boolean cleanSI =
        (level == LEVEL_CLEAR_ALL_GLOBAL_CACHES) || (level == LEVEL_CLEAR_ALL_CACHES_CALLBACKS);
    Boolean cleanLocal = (level == LEVEL_CLEAR_ALL_CACHES_CALLBACKS);
    return new Tuple2<>(cleanSI, cleanLocal);
  }

  public static int convertToLevel(boolean includeSI, boolean includeLocal) {
    if (includeLocal && includeSI) {
      return LEVEL_CLEAR_ALL_CACHES_CALLBACKS;
    } else if (includeSI) { // but !includeLocal
      return LEVEL_CLEAR_ALL_GLOBAL_CACHES;
    } else if (!includeLocal) { // both false
      return LEVEL_CLEAR_MAIN_CACHE;
    } else /* !includeSI && includeLocal */ {
      throw new IllegalArgumentException("No level for this : !includeSI && includeLocal");
    }
  }

  /**
   * Called by libgcnative.so to clear caches and trigger user-registered callbacks (executes on
   * gcnative's own thread)
   *
   * <p>level is explained inline below
   *
   * <p>heapChanges is the change in native heap since the last time clearCacheExt was called,
   * measured in bytes (same as getNativeAllocation)
   */
  private static void clearCacheExt(int level, long heapChange) {
    clearCacheExt(level, -1, heapChange);
  }

  private static void clearCacheExt(int level, long nativeHeapBeforeClear, long nativeHeapChange) {
    log.debug(
        String.format(
            "clearCacheExt called for level = %d and heap change %.2f MB since the last call",
            level, nativeHeapChange / (1024.0 * 1024.0)));
    GCNativeStats.incrementCounter(GCNativeStats.keys().heapChange(), nativeHeapChange);
    gcIndex += 1;
    if (level != LEVEL_INFO_ONLY) {
      if (jemallocVerbose
          && jemallocAPILoaded
          && level != LEVEL_SHUTDOWN) { // on shutdown, we'll already be generating the dump
        InfoDumper.upload(Crumb.gcSource(), jemallocDump("pre-clear-" + gcIndex));
      }
      clearCache(level, nativeHeapBeforeClear, nativeHeapChange);
      if (jemallocVerbose && jemallocAPILoaded)
        InfoDumper.upload(Crumb.gcSource(), jemallocDump("post-clear-" + gcIndex));
    }
  }

  public static void clearCache(int level) {
    clearCache(level, -1, -1);
  }

  private static MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();

  private static void clearCache(
      int level, long nativeHeapBeforeClear, long nativeHeapChangeSincePrevious) {
    timesGcNativeInvoked.incrementAndGet();
    incrementGcNativeCleared(level);
    GCNativeStats.incrementCounter(GCNativeStats.keys().totalInvocations(), 1);
    int finalizerCount = 0;
    long jvmHeap = 0;

    long removed = 0;
    if (level == LEVEL_GC) {
      // Level 0 cleanup: just run gc
      GCMonitor.forceGC();
      finalizerCount = memoryBean.getObjectPendingFinalizationCount();
      jvmHeap = memoryBean.getHeapMemoryUsage().getUsed();
      SystemFinalization.runFinalizers();
      if (allocator.doTrim) tcMallocTrim(0);
    } else if ((level == LEVEL_CLEAR_MAIN_CACHE)
        || (level == LEVEL_CLEAR_ALL_GLOBAL_CACHES)
        || (level == LEVEL_CLEAR_ALL_CACHES_CALLBACKS)) {
      invokeRegisteredCleanersThrough(level);
      // Level 1 cleanup: "clear all caches"
      // Level 2 cleanup: clear all caches with si and constructor cache
      // Level 3 cleanup: clear all caches with si and per-property caches. This also calls the
      // default callbacks
      Tuple2<Boolean, Boolean> t = convertLevelToCleans(level);
      Boolean cleanSI = t._1();
      Boolean cleanCtor = cleanSI; // clean constructor cache as well since it is also SI
      Boolean cleanLocal = t._2();
      if (level == LEVEL_CLEAR_MAIN_CACHE && clearSubset) {
        // this clears the nodes specified as native memory users in the optimus config file
        Caches.clearAllCachesWithNativeMarker(CauseGCNative$.MODULE$);
      } else {
        if (detectFinalizers) {
          try {
            long evicted =
                AdvancedUtils.clearAllCachesWithFinalizers(
                    CauseGCNative$.MODULE$, cleanCtor, cleanSI, cleanLocal, detectFinalizerSeconds);
            GCMonitor.gcCrumb(
                crumbDesc,
                Properties.gcNativeIndex().elem(gcIndex),
                Properties.gcNative().elem("eviction"),
                Properties.gcNativeEvicted().elem(evicted));
            log.info("evicted {} nodes with finalizers", evicted);
          } catch (UnsatisfiedLinkError ex) {
            GCMonitor.gcCrumb(
                crumbDesc,
                Properties.gcNativeIndex().elem(gcIndex),
                Properties.gcNative().elem("fullclear"));
            log.info(
                "could not use finalizer-guided eviction due to missing heapexp library, falling back to full cache "
                    + "clear");
            removed = Caches.clearAllCaches(CauseGCNative$.MODULE$, cleanSI, cleanLocal);
          }
        } else {
          removed = Caches.clearAllCaches(CauseGCNative$.MODULE$, cleanSI, cleanLocal);
        }

        // GC after cache clearing, or we won't reap the benefits.
        GCMonitor.forceGC();
        finalizerCount = memoryBean.getObjectPendingFinalizationCount();
        SystemFinalization.runFinalizers();

        if (dumpHeap) {
          synchronized (GCNative.class) {
            if (level == LEVEL_CLEAR_ALL_GLOBAL_CACHES) {
              dumpHeapCounter = 0;
            } else if (level == LEVEL_CLEAR_ALL_CACHES_CALLBACKS) {
              dumpHeapCounter++;
              if (dumpHeapCounter > dumpHeapAfter) {
                // heap dump: this takes place immediately after total cache clear with local
                dumpHeap(dumpHeapFilePrefix, true);
                dumpHeapCounter = 0;
              }
            }
          }
        }
      }
    } else if (level == LEVEL_SHUTDOWN) {
      // Level 4 means gcnative has no other choice but to kill the JVM
      // (the reason why shutdown is requested is logged by libgcnative_mts.cpp)
      // TODO (OPTIMUS-12121): Raise alert once alerter api is in place
      GCMonitor.gcCrumb(
          crumbDesc,
          Properties.gcNativeIndex().elem(gcIndex),
          Properties.gcNative().elem("shutdown"),
          Properties.gcNativeAllocator().elem(allocator.name),
          Properties.clearCacheLevel().elem(level),
          Properties.gcNativeCacheClearCount().elem(countAllClears),
          Properties.gcNativeJVMHeap().elem(inMB(jvmHeap)),
          Properties.gcNativeAlloc().elem(inMB(nativeHeapBeforeClear)),
          Properties.gcNativeHeapChange().elem(inMB(nativeHeapChangeSincePrevious)),
          Properties.gcFinalizerCount().elem(finalizerCount));
      GCMonitor.kill("GCNative");
    }
    int finalizerCountAfter = memoryBean.getObjectPendingFinalizationCount();
    GCMonitor.gcCrumb(
        crumbDesc,
        Properties.gcNative().elem("clear"),
        Properties.gcNativeAllocator().elem(allocator.name),
        Properties.gcNativeIndex().elem(gcIndex),
        Properties.clearCacheLevel().elem(level),
        Properties.gcNativeCacheClearCount().elem(countAllClears),
        Properties.gcCacheRemoved().elem((int) removed),
        Properties.gcNativeAlloc().elem(inMB(nativeHeapBeforeClear)),
        Properties.gcNativeAllocAfter().elem(getNativeAllocationMB()),
        Properties.gcNativeJVMHeap().elem(inMB(jvmHeap)),
        Properties.gcNativeHeapChange().elem(inMB(nativeHeapChangeSincePrevious)),
        Properties.gcNativeJVMFootprint().elem(inMB(getJVMFootprint())),
        Properties.gcFinalizerCount().elem(finalizerCount),
        Properties.gcFinalizerCountAfter().elem(finalizerCountAfter),
        Properties.gcNativeWatermark().elem(getNativeWatermarkMB()),
        Properties.gcNativeHighWatermark().elem(GCNative.getHighWatermark()));
  }

  public static void registerCleaner(scala.Function0<scala.Unit> cleaner) {
    registerCleaner(cleaner, LEVEL_CLEAR_ALL_CACHES_CALLBACKS);
  }

  /**
   * Register callback to clear custom node caches that might be holding on to native memory.
   *
   * @param cleaner scala callback
   * @param level level of cache cleaning at which to call (0..4: see clearCache for the meaning of
   *     each level). If unspecified, the default level is 3, corresponding to the stage where all
   *     optimus caches were not enough
   */
  public static void registerCleaner(scala.Function0<scala.Unit> cleaner, int level) {
    synchronized (callbacks) {
      if (!callbacks.containsKey(level)) {
        callbacks.put(level, new ArrayList<>());
      }
      callbacks.get(level).add(cleaner);
    }
  }

  public static void invokeRegisteredCleaners() {
    invokeRegisteredCleaners(LEVEL_CLEAR_ALL_CACHES_CALLBACKS); // the default level is 3
  }

  /**
   * Invoke all registered callbacks to clear custom node caches that might be holding on to native
   * memory.
   *
   * @param level level of cache cleaning at which to call (0..4: see clearCache for the meaning of
   *     each level). If unspecified, the default level is 3, corresponding to the stage where all
   *     optimus caches were not enough
   */
  public static void invokeRegisteredCleaners(int level) {
    synchronized (callbacks) {
      if (callbacks.containsKey(level)) {
        for (scala.Function0<scala.Unit> c : callbacks.get(level)) {
          c.apply();
        }
      }
    }
  }

  public static void invokeRegisteredCleanersThrough(int levelMax) {
    for (int level = 0; level <= levelMax; level++) {
      invokeRegisteredCleaners(level);
    }
  }

  private static volatile Optional<HotSpotDiagnosticMXBean> hotspotMBean;
  private static int dumpHeapCounter;

  private static void initHotspotMBean() {
    if (hotspotMBean == null) {
      synchronized (GCNative.class) {
        if (hotspotMBean == null) {
          hotspotMBean = getHotspotMBean();
        }
      }
    }
  }

  private static Optional<HotSpotDiagnosticMXBean> getHotspotMBean() {
    try {
      return Optional.of(
          ManagementFactory.newPlatformMXBeanProxy(
              ManagementFactory.getPlatformMBeanServer(),
              "com.sun.management:type=HotSpotDiagnostic",
              HotSpotDiagnosticMXBean.class));
    } catch (IOException re) {
      log.warn("Could not get the hotspot diagnostic bean: " + re);
      return Optional.empty();
    }
  }

  public static void dumpHeap(String fileNamePrefix, boolean live) {
    initHotspotMBean();
    if (hotspotMBean == null || !hotspotMBean.isPresent()) {
      return;
    }
    try {
      scala.Option pid = Pid.pid();
      String fname =
          fileNamePrefix + "_" + gcIndex + "_" + (pid.isEmpty() ? "unknown" : pid.get()) + ".hprof";
      log.info("Attempting to dump JVM heap to " + fname);
      hotspotMBean.get().dumpHeap(fname, live);
    } catch (IOException re) {
      log.warn("dumpHeap failed with " + re);
    }
  }

  public static class Watchdog {
    private boolean active = true;
    @Nullable private Long gcPauseTimeout = null;
    @Nullable private Long keepAliveTimeout = null;

    private long exitOnGcPauseOverLimit = 0;

    private long exitOnProcessPauseOverLimit = 0;

    private void onActiveChange(boolean oldVal, boolean newVal) {
      updateExitOnGcPauseOverLimit();
      updateExitOnProcessPauseOverLimit();
    }

    private void onGcPauseTimeoutChange(@Nullable Long oldVal, @Nullable Long newVal) {
      updateExitOnGcPauseOverLimit();
    }

    private void onKeepAliveTimeoutChange(@Nullable Long oldVal, @Nullable Long newVal) {
      updateExitOnProcessPauseOverLimit();
    }

    private void updateExitOnGcPauseOverLimit() {
      setExitOnGcPauseOverLimit(
          Optional.ofNullable(this.gcPauseTimeout).filter(__ -> active).orElse(0L));
    }

    private void onExitOnGcPauseOverLimitChange(long oldVal, long newVal) {
      exitOnGcPauseOverLimit(newVal);
    }

    private void updateExitOnProcessPauseOverLimit() {
      setExitOnProcessPauseOverLimit(
          Optional.ofNullable(this.keepAliveTimeout).filter(__ -> active).orElse(0L));
    }

    private void onExitOnProcessPauseOverLimitChange(long oldVal, long newVal) {
      if (newVal <= 0 && oldVal > 0) {
        stopLongPauseDetectThreads();
      } else if (newVal > 0 && oldVal <= 0) {
        updateAliveTime();
        startLongPauseDetectThreads();
      }
      exitOnProcessPauseOverLimit(newVal);
    }

    public synchronized void timeCritical(Long timeoutOverride, Runnable task) {
      Long originalTimeout = keepAliveTimeout;
      try {
        Preconditions.checkNotNull(timeoutOverride);
        Preconditions.checkNotNull(task);
        Preconditions.checkArgument(timeoutOverride > 0);
        Preconditions.checkState(!livenessSuspended.get());

        livenessSuspended.set(true);
        updateAliveTime();
        setKeepAliveTimeout(timeoutOverride);

        task.run();
      } catch (Throwable e) {
        log.error("time-critical task failed with exception, exiting ...", e);
        System.exit(-1);
      } finally {
        livenessSuspended.set(false);
        if (getKeepAliveTimeout().equals(Optional.of(timeoutOverride))) {
          // only reset timeout, if task.run didn't modify the timeout settings
          setKeepAliveTimeout(originalTimeout);
        }
      }
    }

    // Boilerplate

    public synchronized boolean isActive() {
      return active;
    }

    public synchronized void setActive(boolean active) {
      boolean oldActive = this.active;
      if (oldActive != active) {
        this.active = active;
        onActiveChange(oldActive, active);
      }
    }

    public synchronized Optional<Long> getGcPauseTimeout() {
      return Optional.ofNullable(gcPauseTimeout);
    }

    public synchronized void setGcPauseTimeout(@Nullable Long gcPauseTimeout) {
      Preconditions.checkArgument(gcPauseTimeout == null || gcPauseTimeout > 0);
      Long oldGcPauseTimeout = this.gcPauseTimeout;
      if (!Objects.equals(oldGcPauseTimeout, gcPauseTimeout)) {
        this.gcPauseTimeout = gcPauseTimeout;
        onGcPauseTimeoutChange(oldGcPauseTimeout, gcPauseTimeout);
      }
    }

    public synchronized Optional<Long> getKeepAliveTimeout() {
      return Optional.ofNullable(keepAliveTimeout);
    }

    public synchronized void setKeepAliveTimeout(@Nullable Long keepAliveTimeout) {
      Preconditions.checkArgument(keepAliveTimeout == null || keepAliveTimeout > 0);
      Long oldKeepAliveTimeout = this.keepAliveTimeout;
      if (!Objects.equals(oldKeepAliveTimeout, keepAliveTimeout)) {
        this.keepAliveTimeout = keepAliveTimeout;
        onKeepAliveTimeoutChange(oldKeepAliveTimeout, keepAliveTimeout);
      }
    }

    public synchronized long getExitOnGcPauseOverLimit() {
      return exitOnGcPauseOverLimit;
    }

    private synchronized void setExitOnGcPauseOverLimit(long exitOnGcPauseOverLimit) {
      long oldExitOnGcPauseOverLimit = this.exitOnGcPauseOverLimit;
      if (oldExitOnGcPauseOverLimit != exitOnGcPauseOverLimit) {
        this.exitOnGcPauseOverLimit = exitOnGcPauseOverLimit;
        onExitOnGcPauseOverLimitChange(oldExitOnGcPauseOverLimit, exitOnGcPauseOverLimit);
      }
    }

    public synchronized long getExitOnProcessPauseOverLimit() {
      return exitOnProcessPauseOverLimit;
    }

    private synchronized void setExitOnProcessPauseOverLimit(long exitOnProcessPauseOverLimit) {
      long oldExitOnProcessPauseOverLimit = this.exitOnProcessPauseOverLimit;
      if (oldExitOnProcessPauseOverLimit != exitOnProcessPauseOverLimit) {
        this.exitOnProcessPauseOverLimit = exitOnProcessPauseOverLimit;
        onExitOnProcessPauseOverLimitChange(
            oldExitOnProcessPauseOverLimit, exitOnProcessPauseOverLimit);
      }
    }
  }
}
