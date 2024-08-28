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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import msjava.slf4jutils.scalalog.Logger;
import msjava.slf4jutils.scalalog.package$;
import optimus.core.MonitoringBreadcrumbs$;
import optimus.graph.diagnostics.PNodeTaskInfoLight;
import optimus.graph.diagnostics.SchedulerProfileEntry;
import optimus.graph.diagnostics.gridprofiler.GridProfiler$;

/**
 * Either updated live or collected later from a mmap file Times are initially aggregated per
 * thread. Hence no synchronization.
 */
public final class PThreadContext {
  static final Logger log = package$.MODULE$.getLogger("PThreadContext");

  public static long dbgGlobalStart; // Global start of time to ease debugging, set it debugger
  public static final int SPAN_RUN = 1;
  public static final int SPAN_WAIT = 3;
  public static final int SPAN_WAIT_SYNC = 4;
  public static final int SPAN_SPIN = 5;

  public static final int GRAPH_ENTER = 0x10; // Thread 'entered' graph execution
  public static final int GRAPH_EXIT = 0x11; // Thread 'exited' graph execution
  public static final int GRAPH_ENTER_WAIT = 0x12; // Thread entered 'waiting' state
  public static final int GRAPH_EXIT_WAIT = 0x13; // Thread entered 'waiting' state
  public static final int GRAPH_ENTER_SPIN = 0x14; // Thread started spinning while 'in-graph'
  public static final int GRAPH_EXIT_SPIN = 0x15; // Thread stopped spinning while 'in-graph'

  private static final ThreadMXBean tmxb = ManagementFactory.getThreadMXBean();
  public static PThreadContext none = new PThreadContext(0, "NA");

  public long id; // Thread ID
  public String name; // Thread name
  public Process process; // Process indicator
  long jvmProfilingCookie;

  /**
   * Aggregates (Reported by light profile and higher) graphTime as defined in code includes
   * waitTime and spinTime Note: userVisibleGraphTime = graphTime - waitTime - spinTime
   *
   * <p>GEn = graphEnter, GEx = graphExit, WS = waitStart, WE = waitEnd, SS = spinStart, SE =
   * spinEnd
   *
   * <p>The only possible interleaving: GEn GEx GEn WS WE GEx GEn SS SE GEx GEn WS SS SE WE GEx //
   * [SEE_SPIN_IN_WAIT] The last possibility is there when a thread in fullWait() decides it's safe
   * to steal other tasks and not just wait We don't attribute the time to spin because we want the
   * numbers to add up: see above userVisibleGraphTime. This way waitTime never have nested spinTime
   * (even though the scheduler will report them as such)
   */
  long graphTime;

  long spinTime; // aggregated
  long waitTime; // aggregated
  // aggregated self times of all nodes that ran on this context (thread) so far
  public long selfTime;
  private long cpuTime; // aggregated CPU time of this thread, consumed while it was on-graph
  private long cacheTimeWR; // aggregated time of all cache look ups while also running....
  private long cacheTimeMisc; // aggregated time of all cache look ups while NOT running....

  /** Effectively per thread profiler state temporary variables */
  public NodeTask ntsk; // Current running task
  // BlockID of ntsk, helps with postComplete since SS may change after complete
  public int profileBlockID;
  public long startTime; // Current node start time
  // Current cache lookup: number of times equalsForCaching returned false
  public int cacheCollisions;

  private long waitStartTime; // Current wait start time
  private long spinStartTime; // Current spin start time
  // Most recent OGTrace.GRAPH_ENTER event (> 0 implies 'running') -- revisit
  private long graphEnterTime;
  private long cpuTimeStart; // per-thread CPU time at the most recent OGTrace.GRAPH_ENTER event
  // the time at which the current node last suspended or completed
  public long nodeSuspendOrCompleteTime;
  // on this thread (or zero if it didn't)

  // for diagnostics - we keep a record of recent events here (these are only populated in
  // OGTraceReader when we reconstruct events)
  public ArrayList<Span> spans = new ArrayList<>();
  public ArrayList<Span> eventRecord = new ArrayList<>();

  public static class Span {
    public Span(int type) {
      this.type = type;
    }

    public Span(int type, long start) {
      this.start = start;
      this.type = type;
    }

    public long start;
    public long end;
    public int type; // One of SPAN_XXX Flags

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object other) {
      Span o = (Span) other;
      return start == o.start && end == o.end && type == o.type;
    }

    @Override
    public int hashCode() {
      return Objects.hash(start, end, type);
    }

    @Override
    public String toString() {
      String stype = "<>";
      switch (type) {
        case GRAPH_ENTER:
          stype = "Enter";
          break;
        case GRAPH_EXIT:
          stype = "Exit";
          break;
        case GRAPH_ENTER_WAIT:
          stype = "WAIT_S";
          break;
        case GRAPH_EXIT_WAIT:
          stype = "WAIT_E";
          break;
        case GRAPH_ENTER_SPIN:
          stype = "SPIN_S";
          break;
        case GRAPH_EXIT_SPIN:
          stype = "SPIN_E";
          break;
        case SPAN_RUN:
          stype = "SPAN_RUN";
          break;
        case SPAN_WAIT:
          stype = "SPAN_WAIT";
          break;
        case SPAN_WAIT_SYNC:
          stype = "SPAN_WAIT_SYNC";
          break;
        case SPAN_SPIN:
          stype = "SPAN_SPIN";
          break;
      }
      String result = stype;
      if (end != 0) {
        result += " dur: " + (end - start) * 1e-6;
      }
      if (dbgGlobalStart != 0) {
        result += " off: " + (start - dbgGlobalStart) * 1e-6;
      }
      return result;
    }
  }

  public static class Process {
    public static final String separator = "_";
    public static final Process none = new Process("na", 0, 0);
    private static volatile Process current;

    String nickName;
    public int nickPID;

    String host;
    long pid;
    long startTime;

    public Process(String host, long pid, long startTime) {
      this.host = host;
      this.pid = pid;
      this.startTime = startTime;
    }

    static Process from(String asString) {
      String[] parts = asString.split(separator);
      var skipExtension = parts[2].lastIndexOf('.');
      var lastPart = skipExtension > 0 ? parts[2].substring(0, skipExtension) : parts[2];
      return new Process(parts[0], Long.parseLong(parts[1]), Long.parseLong(lastPart));
    }

    public static Process current() {
      if (current == null) {
        synchronized (Process.class) {
          if (current == null) {
            String host = "local";
            try {
              host = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
              e.printStackTrace();
            }
            var runtime = ManagementFactory.getRuntimeMXBean();
            current = new Process(host, runtime.getPid(), runtime.getStartTime());
          }
        }
      }
      return current;
    }

    public String displayName() {
      if (pid == 0) return "";
      return (nickName == null ? host : nickName) + ":" + (nickPID == 0 ? pid : nickPID);
    }

    @Override
    public String toString() {
      return host + separator + pid + separator + startTime;
    }
  }

  public PThreadContext(long id) {
    this.id = id;
  }

  public PThreadContext(long id, String name) {
    this.id = id;
    this.name = name;
  }

  public Span push(int type, long time) {
    Span span = new Span(type);
    span.start = time;
    spans.add(span);
    return span;
  }

  private static final PNodeTaskInfoLight[] emptyPNTIS = new PNodeTaskInfoLight[0];
  private static final PNodeTaskInfoLight[][] emptyPNTISBlocks = new PNodeTaskInfoLight[0][0];
  private PNodeTaskInfoLight[] pntis = emptyPNTIS;
  private PNodeTaskInfoLight[][] scopedPntis = emptyPNTISBlocks;

  PNodeTaskInfoLight[] getUnscopedPNTIs() {
    return pntis;
  }

  Map<Integer, PNodeTaskInfoLight[]> getScopedPntis() {
    Map<Integer, PNodeTaskInfoLight[]> result = new HashMap<>();
    if (scopedPntis != null) {
      for (int i = 2 /* skip Property and Global */; i < scopedPntis.length; i++) {
        PNodeTaskInfoLight[] pntisForThisScope = scopedPntis[i];
        if (pntisForThisScope != null) result.put(i, pntisForThisScope);
      }
    }
    return result;
  }

  PNodeTaskInfoLight[] getAllPNodeTaskInfos() {
    return pntis;
  }

  void resetAllPNodeTaskInfos() {
    pntis = emptyPNTIS;
    scopedPntis = emptyPNTISBlocks;
  }

  private PNodeTaskInfoLight getGlobalPNTI(int id) {
    var pntis = this.pntis; // Local var is important because reset() can update this.pntis
    if (pntis.length <= id) this.pntis = pntis = Arrays.copyOf(pntis, id * 2);
    PNodeTaskInfoLight pnti = pntis[id];
    if (pnti == null) pnti = pntis[id] = new PNodeTaskInfoLight(id);
    return pnti;
  }

  private PNodeTaskInfoLight getScopedPNTI(int id, int blk) {
    // Local var is important because reset() can update this.scopedPntis
    var scopedPntis = this.scopedPntis;
    if (scopedPntis.length <= blk)
      this.scopedPntis = scopedPntis = Arrays.copyOf(scopedPntis, blk * 2);

    PNodeTaskInfoLight[] pntisForThisScope = scopedPntis[blk];
    if (pntisForThisScope == null)
      scopedPntis[blk] = pntisForThisScope = new PNodeTaskInfoLight[id + 1];
    if (pntisForThisScope.length <= id)
      scopedPntis[blk] = pntisForThisScope = Arrays.copyOf(pntisForThisScope, id * 2);

    var pnti = pntisForThisScope[id];
    if (pnti == null) pnti = pntisForThisScope[id] = new PNodeTaskInfoLight(id);

    return pnti;
  }

  public PNodeTaskInfoLight getPNTI(int id, int blk) {
    return (blk == OGTrace.BLOCK_ID_UNSCOPED) ? getGlobalPNTI(id) : getScopedPNTI(id, blk);
  }

  public PNodeTaskInfoLight getPNTI(NodeTask ntsk) {
    return getPNTI(ntsk.getProfileId(), ntsk.scenarioStack().profileBlockID());
  }

  public PNodeTaskInfoLight getPNTI(NodeTask ntsk, int profileBlockID) {
    return getPNTI(ntsk.getProfileId(), profileBlockID);
  }

  public PNodeTaskInfoLight getPNTI() {
    return getPNTI(ntsk.getProfileId(), profileBlockID);
  }

  boolean lastSpan(int expectedSpanType, long time) {
    int spanSize = spans.size();

    if (spanSize <= 0) {
      return false; // We saw partial recording only and there is no match
    }
    Span span = spans.get(spanSize - 1);

    // Confidence check ...
    int verifyType = span.type == SPAN_WAIT_SYNC ? SPAN_WAIT : span.type;
    if (expectedSpanType != verifyType || span.end != 0) {
      return false;
    }
    // Update the endTime
    span.end = time;
    return true;
  }

  public void taskStart(NodeTask task, long time) {
    this.ntsk = task;
    this.profileBlockID = task.profileBlockID();
    this.startTime = time;
  }

  public void resetRunningTask() {
    this.ntsk = null;
    this.profileBlockID = 0;
  }

  public void graphEnter(long now) {
    graphEnterTime = now;
    if (DiagnosticSettings.profileThreadCPUTime && !Settings.useVirtualThreads)
      cpuTimeStart = tmxb.getCurrentThreadCpuTime();
  }

  public void graphExit(long now) {
    if (graphEnterTime == 0) panic();

    graphTime += now - graphEnterTime;
    graphEnterTime = 0L;

    if (DiagnosticSettings.profileThreadCPUTime && !Settings.useVirtualThreads) {
      cpuTime += tmxb.getCurrentThreadCpuTime() - cpuTimeStart;
      cpuTimeStart = 0L;
    }
  }

  public void waitStart(long time) {
    if (waitStartTime != 0 || graphEnterTime == 0) panic();

    waitStartTime = time;
  }

  public void waitEnd(long time) {
    if (waitStartTime == 0 || graphEnterTime == 0) panic();

    waitTime += time - waitStartTime;
    waitStartTime = 0;
  }

  public void spinStart(long time) {
    if (spinStartTime != 0 || graphEnterTime == 0) panic();

    // [SEE_SPIN_IN_WAIT]
    if (waitStartTime == 0) spinStartTime = time;
  }

  public void spinEnd(long time) {
    if ((spinStartTime == 0 && waitStartTime == 0) || graphEnterTime == 0) panic();

    // [SEE_SPIN_IN_WAIT]
    if (waitStartTime == 0) {
      spinTime += time - spinStartTime;
      spinStartTime = 0;
    }
  }

  public void lookupEnd(long cacheTime, NodeTask task) {
    if (cacheTime > (1000L * 1000 * 1000 * 100))
      MonitoringBreadcrumbs$.MODULE$.cacheTimeMisreported(task, cacheTime);

    if (startTime != 0) cacheTimeWR += cacheTime;
    else cacheTimeMisc += cacheTime;
  }

  /** [SEE_OBSERVED_VALUE] [SEE_LOCAL_TABLES_LOCK] */
  SchedulerProfileEntry lastObservedValues;

  SchedulerProfileEntry observedValues() {
    return new SchedulerProfileEntry(
        graphTime, selfTime, cacheTimeWR, cacheTimeMisc, spinTime, waitTime, cpuTime);
  }

  /**
   * [SEE_PROFILER_UNSTABLE] [SEE_OBSERVED_VALUE] There is a race here because we observe the values
   * (graphEnterTime and graphTime) on one thread but we might update them as we exit graph on
   * another thread
   */
  SchedulerProfileEntry observedValuesSinceLast(long currentTime) {
    SchedulerProfileEntry observed = observedValues();
    long graphEnterTime = this.graphEnterTime;
    // Equivalent to checking if currently 'running' (graphEnterTime is reset on graph exit)
    if (graphEnterTime > 0) {
      // we ONLY pass 0 in expungeThreads, when the thread is 'out of scope' so we
      // definitely should not be currently 'running' anything
      if (currentTime == 0) panic();
      else observed.graphTime_$eq(observed.graphTime() + (currentTime - graphEnterTime));
    }

    if (lastObservedValues != null) return observed.since(lastObservedValues);
    return observed;
  }

  public void record() {
    GridProfiler$.MODULE$.recordSchedulerProfile(name, observedValues());
  }

  private void panic() {
    GraphInInvalidState e =
        new GraphInInvalidState("Unexpected sequence of events"); // Series of unfortunate events...
    if (Settings.schedulerAsserts) throw e;
    else log.javaLogger().warn("Exception in PThreadContext", e);
  }
}
