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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import optimus.core.CoreAPI;
import optimus.graph.diagnostics.pgo.Profiler;
import optimus.graph.diagnostics.gridprofiler.GridProfiler$;
import optimus.graph.diagnostics.SchedulerProfileEntry;
import optimus.graph.diagnostics.gridprofiler.ProcessMetricsEntry$;
import optimus.utils.ClassLoaderStats;

/** All general tracking of overall graph times is here - consider merging with profiler summary */
public class OGSchedulerTimes {
  private static final boolean stallingCrumbEnabled =
      DiagnosticSettings.getBoolProperty("optimus.graph.stalling.crumb.enabled", true);
  public static final int stallTimeCrumbThresholdSecs =
      DiagnosticSettings.getIntProperty("optimus.graph.stalled.crumb.threshold", 30);

  /**
   * Currently all time operations are under one lock (timeLock) - use it when reading multiple
   * fields, not needed when you're just returning a long
   */
  private static final Object timeLock = new Object();

  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
  public static final class StallDetailedTime implements Serializable {
    public final PluginType pluginType;
    public final boolean syncStackPresent;
    public long time; // total time stalled

    public StallDetailedTime(PluginType pluginType, boolean syncStackPresent, long time) {
      this.pluginType = pluginType;
      this.syncStackPresent = syncStackPresent;
      this.time = time;
    }

    @Override
    public boolean equals(Object o) {
      StallDetailedTime that = (StallDetailedTime) o;
      return syncStackPresent == that.syncStackPresent
          && Objects.equals(pluginType, that.pluginType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(pluginType, syncStackPresent);
    }
  }

  private static long inGraphWallTotal = 0L; // Wall time while any scheduler is running or waiting
  private static long inGraphWallReset = 0L;
  private static long graphStallTotal = 0L;
  private static long graphStallReset = 0L;
  private static long graphHolesTotal = 0L; // Holes between graph exits and enters
  private static long graphHolesReset = 0L;

  private static long firstGraphEnterTime = 0L;
  // Time from JVM start to first graph enter (in ms) - deliberately not reset
  private static long timeToFirstGraphEnterTime = 0L;
  private static long lastGraphEnterTime = 0L;
  private static long lastGraphExitTime = 0L;
  // Counts active schedulers (at least 1 work or wait queue thread)
  private static int schedulersWorking;
  private static volatile long graphStallStartTime;
  private static SchedulerStallSource currentGraphStallInfo;
  private static volatile boolean crumbsWereSent = false;
  private static ScheduledFuture<?> crumbsCancellationFuture = null;
  private static final HashMap<StallDetailedTime, StallDetailedTime> graphStallInfos =
      new HashMap<>();
  private static long inGraphClassLoadingTime = 0L;
  private static long lastGraphEnterClassLoadingTime = 0L;

  // startup time breakdown (these are all nanoTime timestamps)
  // Three major divisions
  // 1. off-graph startup
  // OptimusApp: from start of JVM to just before OptimusApp calls withOptimus
  // further breakdown likely. In particular, user code in def setup should be separated out
  private static long preOptimusStartupTimestamp = 0L; // timestamp (nanoTime)
  private static long preOptimusStartupTime = 0L; // duration (from start of JVM), in nanos
  // 2. optimus env startup
  // OptimusApp: from just before withOptimus to just after
  // further breakdown may be to separate DAL setup from og.scheduler setup, but it's 99% DAL
  // (serverTime and roles)
  private static long optimusStartupTimestamp = 0L; // timestamp (nanoTime)
  // 3. on-graph startup
  // OptimusApp: from just after withOptimus to just before run
  // further breakdown we may to separate out optimus code in super.preRun vs user code in
  // overriders
  // OptimusUIApp: from just after withOptimus to just before mainGui
  // further breakdown may be to separate optimus vs user code in this period
  private static long postOptimusAppStartupTimestamp = 0L; // timestamp (nanoTime)
  private static long postOptimusLastGraphEnter = 0L; // timestamp (nanoTime)
  private static long postOptimusLastGraphExit = 0L; // timestamp (nanoTime)
  private static long postOptimusOnGraphAppStartupTime = 0L; // duration (nanos)
  // duration (nanos) during "graph holes"
  private static long postOptimusBetweenGraphAppStartupTime = 0L;

  static void enterGraphTime(long nanoTime) {
    synchronized (timeLock) {
      schedulersWorking++;
      if (schedulersWorking == 1) {
        lastGraphEnterTime = nanoTime;
        lastGraphEnterClassLoadingTime = ClassLoaderStats.snap().findClassTime();
        if (firstGraphEnterTime == 0) { // only set it once (per reset cycle)
          firstGraphEnterTime = nanoTime;
          timeToFirstGraphEnterTime =
              ProcessMetricsEntry$.MODULE$.isFreshJvm() ? DiagnosticSettings.jvmUpTimeInMs() : 0L;
        }
        if (lastGraphExitTime != 0) graphHolesTotal += lastGraphEnterTime - lastGraphExitTime;

        // during post-optimus app startup
        if (optimusStartupTimestamp != 0 && postOptimusAppStartupTimestamp == 0) {
          postOptimusLastGraphEnter = nanoTime;
          if (postOptimusLastGraphExit != 0)
            postOptimusBetweenGraphAppStartupTime +=
                postOptimusLastGraphEnter - postOptimusLastGraphExit;
        }
      }
    }
  }

  static void exitGraphTime(long nanoTime) {
    synchronized (timeLock) {
      schedulersWorking--;
      if (schedulersWorking == 0) {
        lastGraphExitTime = nanoTime;
        inGraphWallTotal += nanoTime - lastGraphEnterTime;
        lastGraphEnterTime = 0;
        inGraphClassLoadingTime +=
            ClassLoaderStats.snap().findClassTime() - lastGraphEnterClassLoadingTime;
        lastGraphEnterClassLoadingTime = 0;

        // during post-optimus app startup
        if (optimusStartupTimestamp != 0 && postOptimusAppStartupTimestamp == 0) {
          postOptimusLastGraphExit = nanoTime;
          if (postOptimusLastGraphEnter != 0)
            postOptimusOnGraphAppStartupTime +=
                postOptimusLastGraphExit - postOptimusLastGraphEnter;
        }
      }
    }
  }

  static void preOptimusStartupComplete(long nanoTime) {
    synchronized (timeLock) {
      if (preOptimusStartupTimestamp != 0)
        Profiler.log().javaLogger().debug("preOptimusStartupComplete called twice; ignoring");
      else {
        preOptimusStartupTimestamp = nanoTime;
        preOptimusStartupTime = 1000 * 1000 * DiagnosticSettings.jvmUpTimeInMs();
      }
    }
  }

  public static long getPreOptimusStartupTime() {
    return preOptimusStartupTime;
  }

  static void optimusStartupComplete(long nanoTime) {
    synchronized (timeLock) {
      if (optimusStartupTimestamp != 0)
        Profiler.log().javaLogger().debug("optimusStartupComplete called twice; ignoring");
      else {
        optimusStartupTimestamp = nanoTime;
        if (lastGraphEnterTime != 0) // if on-graph
        postOptimusLastGraphEnter = optimusStartupTimestamp;
        else postOptimusLastGraphExit = optimusStartupTimestamp;
      }
    }
  }

  public static long getOffGraphOptimusStartupTime() {
    synchronized (timeLock) {
      return (preOptimusStartupTimestamp == 0)
          ? 0
          : firstGraphEnterTime - preOptimusStartupTimestamp;
    }
  }

  public static long getOnGraphOptimusStartupTime() {
    synchronized (timeLock) {
      return optimusStartupTimestamp - firstGraphEnterTime;
    }
  }

  static void postOptimusAppStartupComplete(long nanoTime) {
    synchronized (timeLock) {
      if (postOptimusAppStartupTimestamp != 0)
        Profiler.log().javaLogger().debug("postOptimusAppStartupComplete called twice; ignoring");
      else postOptimusAppStartupTimestamp = nanoTime;
    }
  }

  public static long getPostOptimusOnGraphAppStartupTime() {
    return postOptimusOnGraphAppStartupTime;
  }

  public static long getPostOptimusBetweenGraphAppStartupTime() {
    return postOptimusBetweenGraphAppStartupTime;
  }

  public static long getInGraphWallTime(long baseline) {
    synchronized (timeLock) {
      // We consumed at least that much (and we may not have exited graph yet to get the most up to
      // date time)
      return inGraphWallTotal
          - baseline
          + (lastGraphEnterTime != 0 ? OGTrace.nanoTime() - lastGraphEnterTime : 0);
    }
  }

  public static long getInGraphWallTime() {
    return getInGraphWallTime(inGraphWallReset);
  }

  public static long getPostGraphTime() {
    synchronized (timeLock) {
      return lastGraphEnterTime == 0 ? OGTrace.nanoTime() - lastGraphExitTime : 0;
    }
  }

  public static long getGraphHolesTime(long baseline) {
    return graphHolesTotal - baseline;
  }

  public static long getGraphHolesTime() {
    return getGraphHolesTime(graphHolesReset);
  }

  public static ArrayList<StallDetailedTime> getStallingReasons() {
    synchronized (timeLock) {
      return new ArrayList<>(graphStallInfos.values());
    }
  }

  public static long getGraphStallTime(long baseline) {
    synchronized (timeLock) {
      // We stalled at least that much (and we may not have exited stall yet to get the most up to
      // date time)
      return graphStallTotal
          - baseline
          + (graphStallStartTime != 0 ? System.nanoTime() - graphStallStartTime : 0);
    }
  }

  public static long getGraphStallTime() {
    return getGraphStallTime(graphStallReset);
  }

  /**
   * Warning - this assumes one scheduler because it relies on idealThreadCount (if we have multiple
   * schedulers with different thread counts then this calculation will be wrong)
   */
  public static long getUnderUtilizedTime() {
    synchronized (timeLock) {
      return getInGraphWallTime() * Settings.idealThreadCountToCount()
          - getSchedulerTimes().userGraphTime();
    }
  }

  public static long getGraphSpanTime() {
    synchronized (timeLock) {
      return lastGraphExitTime - firstGraphEnterTime;
    }
  }

  public static long getTimeToFirstGraphEnterTime() {
    return timeToFirstGraphEnterTime;
  }

  public static long getFirstGraphEnterTime() {
    return firstGraphEnterTime;
  }

  // Note: we're making progress on other threads while FindClassTime is ticking
  // instrumenting loadClass to issue OGTraceCounter events to attribute to threads or even nodes
  // would be best
  // as-is, this is just a wall time of classloading that took place while on-graph
  public static long getClassLoadingTime() {
    synchronized (timeLock) {
      return inGraphClassLoadingTime
          + (lastGraphEnterClassLoadingTime != 0
              ? ClassLoaderStats.snap().findClassTime() - lastGraphEnterClassLoadingTime
              : 0);
    }
  }

  private static PluginType.Counter pluginStallTimes = new PluginType.Counter();

  public static PluginType.Counter snapCumulativePluginStallTimes() {
    synchronized (timeLock) {
      PluginType.Counter c = pluginStallTimes.snap();
      if (graphStallStartTime > 0 && currentGraphStallInfo != null) {
        PluginType pt = currentGraphStallInfo.atieoc().getPluginType();
        c.update(pt, System.nanoTime() - graphStallStartTime);
      }
      return c;
    }
  }

  // every thread of the graph is waiting, log these conditions separately
  static void schedulerStallingBegin(SchedulerStallSource stallReason) {
    synchronized (timeLock) {
      if (graphStallStartTime == 0) {
        // OGTrace.nanoTime doesn't synchronize times across threads
        long stallStartTime = System.nanoTime();
        if (stallingCrumbEnabled) {
          crumbsCancellationFuture =
              CoreAPI.optimusScheduledThreadPool()
                  .scheduleAtFixedRate(
                      () -> {
                        crumbsWereSent = true;
                        GraphEvents$.MODULE$.publishGraphStallingCrumb(
                            System.nanoTime() - stallStartTime, stallReason);
                      },
                      stallTimeCrumbThresholdSecs,
                      stallTimeCrumbThresholdSecs,
                      TimeUnit.SECONDS);
        }
        graphStallStartTime = stallStartTime;
        currentGraphStallInfo = stallReason;
      }
    }
  }

  /** Note not paired with schedulerStallingBegin() i.e. can be missing schedulerStallingBegin() */
  static void schedulerStallingEnd() {
    if (graphStallStartTime == 0) return; // avoid extra lock taking
    synchronized (timeLock) {
      if (graphStallStartTime != 0) {
        // OGTrace.nanoTime doesn't synchronize times across threads
        long stallTime = System.nanoTime() - graphStallStartTime;
        graphStallStartTime = 0;
        graphStallTotal += stallTime;

        if (currentGraphStallInfo != null) {
          NodeTaskInfo endOfChain = currentGraphStallInfo.atieoc();
          PluginType pluginType = endOfChain.getPluginType();
          StallDetailedTime key =
              new StallDetailedTime(pluginType, currentGraphStallInfo.syncStackPresent(), 0);
          // time is not considered in equals method so key will still match if we had an entry
          // before
          StallDetailedTime prevValue = graphStallInfos.putIfAbsent(key, key);
          StallDetailedTime entry = prevValue == null ? key : prevValue;
          entry.time += stallTime;
          pluginStallTimes.update(pluginType, stallTime);

          // recordStallTime deals with nulls itself (whereas publishGraphStalledCrumb doesn't, so
          // check first)
          GridProfiler$.MODULE$.recordStallTime(
              currentGraphStallInfo.awaitedTask(),
              endOfChain,
              stallTime,
              currentGraphStallInfo.name());

          if (crumbsCancellationFuture != null) {
            crumbsCancellationFuture.cancel(false);
            crumbsCancellationFuture = null;
            // at this point the scheduled task has completed, and the task can never be rescheduled
            // again
            if (crumbsWereSent) {
              GraphEvents$.MODULE$.publishGraphStalledCrumb(stallTime, currentGraphStallInfo);
              crumbsWereSent = false;
            }
          }
          currentGraphStallInfo = null;
        }
      }
    }
  }

  /** Called at the end of distribution */
  public static void reset() {
    OGLocalTables.resetTables();
    synchronized (timeLock) {
      inGraphWallReset = inGraphWallTotal;
      graphHolesReset = graphHolesTotal;
      graphStallReset = graphStallTotal;
      if (lastGraphEnterTime
          != 0) { // we're on-graph, make sure subsequent exitGraph produces sane inGraphWallTime
        lastGraphEnterTime = OGTrace.nanoTime();
        lastGraphEnterClassLoadingTime = ClassLoaderStats.snap().findClassTime();
      }
      firstGraphEnterTime = 0;
      lastGraphExitTime = 0;
      graphStallInfos.clear();
      currentGraphStallInfo = null;
    }
  }

  public static SchedulerProfileEntry getSchedulerTimes() {
    return OGLocalTables.getSchedulerTimes();
  }
}
