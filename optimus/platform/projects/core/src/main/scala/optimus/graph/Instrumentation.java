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

import static java.lang.management.ManagementFactory.*;
import static optimus.graph.Settings.parDecayRate;
import static optimus.graph.Settings.samplingInterval;
import static optimus.graph.Settings.snapConcurrencyLimit;
import static optimus.graph.Settings.snapPrintBlockedLevel;
import static optimus.graph.Settings.snapPrintConcurrencyLimit;
import static optimus.graph.Settings.snapPrintInterval;
import static optimus.graph.Settings.snapStackConcurrencyLimit;
import static optimus.graph.Settings.stapStackHistory;

import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.PriorityQueue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import msjava.slf4jutils.scalalog.Logger;
import optimus.platform.util.ThreadDumper;
import optimus.platform.EvaluationContext;
import optimus.profiling.JProfiler;
import scala.Option;

public class Instrumentation {

  private final OGScheduler scheduler;
  private final OGScheduler.Contexts workers;
  private final OGScheduler.Contexts idlePool;
  private final OGScheduler.Contexts waiters;
  private final PriorityQueue<NodeTask> lowPriorityQueue;
  private final OGSchedulerContext.WorkQueue quickQueue;

  Instrumentation(
      OGScheduler scheduler,
      OGScheduler.Contexts workers,
      OGScheduler.Contexts waiters,
      OGScheduler.Contexts idlePool,
      PriorityQueue<NodeTask> lowPriorityQueue,
      OGSchedulerContext.WorkQueue quickQueue) {
    this.workers = workers;
    this.waiters = waiters;
    this.idlePool = idlePool;
    this.scheduler = scheduler;
    this.lowPriorityQueue = lowPriorityQueue;
    this.quickQueue = quickQueue;
  }

  private static final Logger log =
      msjava.slf4jutils.scalalog.package$.MODULE$.getLogger("optimus.graph");

  private final ThreadMXBean bean = getThreadMXBean();
  private Deque<Snapshot> snapshots;
  private static long lastSnapshotPrintTime; // limit this globally

  public static Instrumentation getInstance() {
    return ((OGScheduler) EvaluationContext.scheduler()).instr;
  }

  /*
  Utility class for tracking scheduler statistics.
  It is used in two ways, depending on which update method is called.  See documentation for those
  methods below.
  */
  public static class Stats {
    public long n = 0;
    public long t0 = 0L;
    long lastSnapshotTime;
    public double avgPar;
    int lastPar; // This one gets requested specially
    double avgAdapted;
    int lastAdapted;
    public double avgBlocked;
    int lastBlocked;
    double avgSyncStack;
    int lastSyncStack;
    double avgDeclining;
    int lastDeclining;
    int lastWork;
    double avgWork;
    int lastIdle;
    double avgIdle;
    int lastWait;
    double avgWait;
    int lastPriority;
    double avgPriority;
    int lastQuick;
    double avgQuick;

    public Stats(
        long n,
        long lastSnapshotTime,
        double avgPar,
        int lastPar,
        double avgAdapted,
        int lastAdapted,
        double avgBlocked,
        int lastBlocked,
        double avgSyncStack,
        int lastSyncStack,
        double avgWork,
        int lastWork,
        double avgIdle,
        int lastIdle,
        double avgWait,
        int lastWait,
        double avgQuick,
        int lastQuick,
        double avgPriority,
        int lastPriority,
        double avgDeclining,
        int lastDeclining) {
      this.n = n;
      this.lastSnapshotTime = lastSnapshotTime;
      this.avgPar = avgPar;
      this.lastPar = lastPar;
      this.avgAdapted = avgAdapted;
      this.lastAdapted = lastAdapted;
      this.avgBlocked = avgBlocked;
      this.lastBlocked = lastBlocked;
      this.avgSyncStack = avgSyncStack;
      this.lastSyncStack = lastSyncStack;
      this.avgWork = avgWork;
      this.lastWork = lastWork;
      this.avgIdle = avgIdle;
      this.lastIdle = lastIdle;
      this.avgWait = avgWait;
      this.lastWait = lastWait;
      this.avgQuick = avgQuick;
      this.lastQuick = lastQuick;
      this.avgPriority = avgPriority;
      this.lastPriority = lastPriority;
      this.avgDeclining = avgDeclining;
      this.lastDeclining = lastDeclining;
    }

    public String averages() {
      return String.format(
          "average n=%d par=%.2f adapt=%.2f block=%.2f, ss=%.2f, work=%.2f, idle=%.2f, wait=%.2f, qq=%.2f, pq=%.2f, decl=%.2f",
          n,
          avgPar,
          avgAdapted,
          avgBlocked,
          avgSyncStack,
          avgWork,
          avgIdle,
          avgWait,
          avgQuick,
          avgPriority,
          avgDeclining);
    }

    public String lastVals() {
      long dt = TimeUnit.NANOSECONDS.toMillis(lastSnapshotTime - t0);
      String t =
          DateTimeFormatter.ISO_INSTANT.format(
              Instant.ofEpochMilli(TimeUnit.NANOSECONDS.toMillis(lastSnapshotTime)));
      return String.format(
          "last: t=%s, dt=%d, par=%d adapt=%d block=%d, ss=%d, work=%d, "
              + "idle=%d, wait=%d, qq=%d, pq=%d, decl=%d",
          t,
          dt,
          lastPar,
          lastAdapted,
          lastBlocked,
          lastSyncStack,
          lastWork,
          lastIdle,
          lastWait,
          lastQuick,
          lastPriority,
          lastDeclining);
    }

    public String toString() {
      return "Stats: " + lastVals() + " " + averages();
    }

    public Stats() {

      this(0, 0, 0.0, 0, 0.0, 0, 0.0, 0, 0.0, 0, 0.0, 0, 0.0, 0, 0.0, 0, 0.0, 0, 0.0, 0, 0.0, 0);
    }

    // This should not happen often.  See throttling in getStats()
    private synchronized Stats copy() {
      return new Stats(
          n,
          lastSnapshotTime,
          avgPar,
          lastPar,
          avgAdapted,
          lastAdapted,
          avgBlocked,
          lastBlocked,
          avgSyncStack,
          lastSyncStack,
          avgWork,
          lastWork,
          avgIdle,
          lastIdle,
          avgWait,
          lastWait,
          avgQuick,
          lastQuick,
          avgPriority,
          lastPriority,
          avgDeclining,
          lastDeclining);
    }

    // Update, maintaining exponentially decaying averages.  This is expected to be called
    // continually over the life of the scheduler.
    private synchronized void updateExp(
        long t,
        int par,
        int adapted,
        int blocked,
        int sync,
        int work,
        int idle,
        int wait,
        int quick,
        int priority,
        int declining) {
      double a = Math.exp(-TimeUnit.NANOSECONDS.toMillis(t - lastSnapshotTime) * parDecayRate);
      double b = 1.0 - a;
      n += 1; // This value is not obviously useful, but someone might care....
      lastSnapshotTime = t;
      lastPar = par;
      avgPar = avgPar * a + b * par;
      lastAdapted = adapted;
      avgAdapted = avgAdapted * a + b * adapted;
      lastBlocked = blocked;
      avgBlocked = avgBlocked * a + b * blocked;
      lastSyncStack = sync;
      avgSyncStack = avgSyncStack * a + b * sync;
      lastWork = work;
      avgWork = a * avgWork + b * work;
      lastIdle = idle;
      avgIdle = a * avgIdle + b * idle;
      lastWait = wait;
      avgWait = a * avgWait + b * wait;
      lastQuick = quick;
      avgQuick = a * avgQuick + b * quick;
      lastPriority = priority;
      avgPriority = a * avgPriority + b * priority;
      lastDeclining = declining;
      avgDeclining = avgDeclining * a + b * declining;
    }

    // Update, maintaining arithmetic sums.  It is expected to be called a finite number of times
    // over the length
    // of some procedure, such as evaluation of a SequenceNode, and then normalized into averages by
    // calling
    // normalizeAdditive, after which it would be discarded.
    public void updateAdditive(Instrumentation instr) {
      Stats from = instr.getStats();
      n += 1; // In this case, the n is necessary to compute the final average.
      lastPar = from.lastPar;
      avgPar += from.avgPar;
      lastAdapted = from.lastAdapted;
      avgAdapted += lastAdapted;
      lastBlocked = from.lastBlocked;
      avgBlocked += lastBlocked;
      lastSyncStack = from.lastSyncStack;
      avgSyncStack += lastSyncStack;
      lastWork = from.lastWork;
      avgWork += lastWork;
      lastIdle = from.lastIdle;
      avgIdle += lastIdle;
      lastWait = from.lastWait;
      avgWait += lastWait;
      lastQuick = from.lastQuick;
      avgQuick += lastQuick;
      lastPriority = from.lastPriority;
      avgPriority += lastPriority;
      lastDeclining = from.lastDeclining;
      avgDeclining += lastDeclining;
    }

    /*
    Note that, once the averages have been computed, there is no point ever updating it again.
    You should just get a new instance.
     */
    public void normalizeAdditive() {
      avgPar /= n;
      avgAdapted /= n;
      avgBlocked /= n;
      avgSyncStack /= n;
      avgWork /= n;
      avgIdle /= n;
      avgWait /= n;
      avgQuick /= n;
      avgPriority /= n;
      avgDeclining /= n;
    }
  }

  // Don't return stats too often!
  private volatile Stats prevStats = null;
  private final Stats stats = new Stats();
  private long lastGetStats = 0;

  private Stats getStats() {
    long t = System.nanoTime();
    if (prevStats == null || (TimeUnit.NANOSECONDS.toMillis(t - lastGetStats) > 2)) {
      lastGetStats = t;
      prevStats = stats.copy();
    }
    return prevStats;
  }

  private final TimerTask snapTimerTask =
      new TimerTask() {
        @Override
        public void run() {
          takeSnapshot();
        }
      };

  {
    if (samplingInterval > 0) {
      Timer snapTimer = new Timer(true);
      snapTimer.schedule(snapTimerTask, 100L, samplingInterval);
    }
  }

  public int getThreadCount() {
    return scheduler.getIdealThreadCount();
  }

  /** Returns number of working threads as of most recent snap */
  static final boolean isSampling = samplingInterval > 0;

  public static class ContextState {
    String name;
    long threadID;
    NodeTask awaiting = null;
    boolean blocked = false;
    boolean parked = false;
    ArrayList<NodeTask> nodes;
    ArrayList<NodeTask> syncNodes = null;
    ArrayList<NodeTask> localQ = null;
    StackTraceElement[] javaStackTrace = null;

    ContextState(long threadID, String name) {
      this.threadID = threadID;
      this.name = name;
      this.nodes = new ArrayList<>();
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(name);
      if (localQ != null) {
        sb.append("\n    local=");
        sb.append(
            localQ.stream()
                .map(n -> n.stackLine(true))
                .collect(Collectors.joining("\n    ", "\n    ", "")));
      }
      if (javaStackTrace != null) {
        sb.append("\n    java=");
        sb.append(
            Arrays.stream(javaStackTrace)
                .map(StackTraceElement::toString)
                .collect(Collectors.joining("\n    ", "\n    ", "")));
      }
      if (syncNodes != null) {
        sb.append("\n    sync=");
        sb.append(
            syncNodes.stream()
                .map(n -> n.stackLine(true))
                .collect(Collectors.joining("\n    ", "\n    ", "")));
      }
      sb.append("\n    nodes=");
      sb.append(
          nodes.stream()
              .map(n -> n.stackLine(true))
              .collect(Collectors.joining("\n    ", "\n    ", "")));
      return sb.toString();
    }
  }

  static final class Snapshot {
    Stats stats;
    int spinner;
    ArrayList<ContextState> states;

    Snapshot(ArrayList<ContextState> states, int spinner, Stats stats) {
      this.spinner = spinner;
      this.states = states;
      this.stats = stats;
    }

    public String toString() {
      long dt = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stats.lastSnapshotTime);
      return "age="
          + dt
          + " spin="
          + spinner
          + " par="
          + stats
          + " "
          + states.stream()
              .map(ContextState::toString)
              .collect(Collectors.joining("\n  ", "\n  ", ""));
    }
  }

  private void getAndResetSnapshots() {
    if (samplingInterval < 0 || snapPrintInterval < 0) return;
    Deque<Snapshot> ss = null;
    long t = System.nanoTime();
    synchronized (this) {
      if (TimeUnit.NANOSECONDS.toMillis(t - lastSnapshotPrintTime) < snapPrintInterval) return;
      ss = snapshots;
      if (ss == null || ss.size() == 0) return;
      snapshots = null;
    }

    int par = -1;
    int blocked = 0;
    int awaiting = 0;
    int nStates = 0;
    for (Snapshot s : ss) {
      nStates += s.states.size();
      // par > stats.lastPar always false?
      if (par < 0 || par > stats.lastPar) par = stats.lastPar;
      if (blocked < stats.lastBlocked) blocked = stats.lastBlocked;
      if (awaiting < stats.lastWait) awaiting = stats.lastWait;
    }

    if (nStates == 0) {
      log.javaLogger().info("No states recorded.");
      return;
    }

    if (par <= snapPrintConcurrencyLimit || blocked >= snapPrintBlockedLevel) {
      lastSnapshotPrintTime = t;
      final String msg = String.format("Snap %s", stats);

      JProfiler.BookmarkColor color;
      if (blocked > 0) color = JProfiler.red();
      else if (awaiting > 0) color = JProfiler.blue();
      else color = JProfiler.green();

      int n = JProfiler.addBookmark(() -> msg, color, true);
      log.javaLogger()
          .info(
              "["
                  + n
                  + "]"
                  + msg
                  + " "
                  + ss.stream()
                      .map(Snapshot::toString)
                      .collect(Collectors.joining("\n", "\n", "")));
    }
  }

  private synchronized void takeSnapshot() {
    int workCount = workers.size();
    int idleCount = idlePool.size();
    int waitCount = waiters.size();
    int pqSize = lowPriorityQueue.size();
    int qqSize = quickQueue.size();

    ArrayList<OGScheduler.Context> contexts = new ArrayList<>();
    workers.foreach(contexts::add);
    waiters.foreach(contexts::add);

    ThreadDumper.ThreadInfos allThreadInfos = ThreadDumper.getAllThreadInfos();

    // First pass, just to estimate real concurrency.
    int par = 0;
    int blocked = 0;
    int adapted = 0;
    int sync = 0;
    int declining = 0;
    for (OGScheduler.Context ctx : contexts) {
      OGSchedulerContext ogc = ctx.schedulerContext;
      if (ogc != null) {
        sync += ogc.syncStackCount();
        Thread.State ts = ctx.thread.getState();
        if (ts == Thread.State.BLOCKED) blocked++;
        else if (ogc.getCurrentNodeTask() != null) par++;
        else {
          declining += 1;
        }
      }
    }
    long t = System.nanoTime();

    stats.updateExp(
        t, par, adapted, blocked, sync, workCount, idleCount, waitCount, qqSize, pqSize, declining);

    // Don't capture anything if we're not going to display it, or if parallelism is high enough.
    // Unless properties are specially set, we alwyas bail out here.
    if (snapPrintInterval < 0 || par > snapConcurrencyLimit) return;

    // Second pass, deferring stack trace capture
    if (snapshots == null) snapshots = new ArrayDeque<>();
    ArrayList<ContextState> states = new ArrayList<>();
    ArrayList<Long> blockingThreadIds = new ArrayList<>();
    int spinner = scheduler.getSpinningThreadID();

    for (OGScheduler.Context ctx : contexts) {
      if (ctx == null) continue;
      else {
        Thread th = ctx.thread;
        NodeTask ntsk = ctx.schedulerContext.getCurrentNodeTask();
        if (ctx.parked) {
          if (ntsk == null) break;
        }

        if (ntsk == null) break;

        Thread.State ts = th.getState();

        int ntskCausality = ntsk.causalityID();
        int awaitedCausality = ctx.awaitedCausalityID;

        long tid = th.getId();
        int lq = ctx.queue.size();
        ContextState contextState =
            new ContextState(
                tid,
                th.getName()
                    + " "
                    + ts
                    + " tcid="
                    + ntskCausality
                    + " acid="
                    + awaitedCausality
                    + " lq="
                    + lq);
        states.add(contextState);

        contextState.nodes.add(ntsk);

        // Don't capture local queue for waiting states
        if (lq > 0 && !ctx.parked) {
          ArrayList<NodeTask> list = new ArrayList<>();
          ctx.queue.appendToList(list);
          if (list.size() > 0) contextState.localQ = list;
        }

        if (ctx.schedulerContext.syncStackCount() > 1) {
          contextState.syncNodes = new ArrayList<>();
          ctx.schedulerContext.appendSyncStackNodes(contextState.syncNodes);
        }

        if (ts == Thread.State.BLOCKED) {
          contextState.blocked = true;
          ThreadInfo info = bean.getThreadInfo(tid);
          long lockingThreadId = info.getLockOwnerId();
          if (lockingThreadId >= 0) {
            if (!blockingThreadIds.contains(lockingThreadId))
              blockingThreadIds.add(lockingThreadId);
            contextState.name =
                contextState.name
                    + " blocker="
                    + "="
                    + info.getLockOwnerName()
                    + "="
                    + info.getLockName();
          } else
            contextState.name =
                contextState.name + " blocker=" + info.getLockOwnerName() + "=unknown";
        } else if (ctx.parked) {
          contextState.parked = true;
          contextState.name = contextState.name + " PARKED";
          NodeTask w = ntsk.getWaitingOn();
          if (w != null) {
            contextState.awaiting = w;
            contextState.name += " awaiting=" + w.toString();
          }
        }
      }
    }

    // Final pass, grabbing stack traces for blocking nodes.
    for (ContextState contextState : states) {
      if (par <= snapStackConcurrencyLimit) {
        NodeTask ntsk = contextState.nodes.get(0);
        // contextState.nodes.clear();
        ntsk.appendNodeStack(contextState.nodes);
      }
      long tid = contextState.threadID;
      int bidx = blockingThreadIds.indexOf(tid);
      if (bidx >= 0) {
        blockingThreadIds.remove(bidx);
        contextState.name += " BLOCKING";
        ThreadInfo lockingInfo = threadInfo(allThreadInfos, bean, tid);
        if (lockingInfo != null) contextState.javaStackTrace = lockingInfo.getStackTrace();
      } else if (!contextState.parked
          && (contextState.nodes.size() <= 2
              || contextState.blocked
              || contextState.syncNodes != null)) {
        ThreadInfo blockedInfo = threadInfo(allThreadInfos, bean, tid);
        if (blockedInfo != null) contextState.javaStackTrace = blockedInfo.getStackTrace();
      }
    }
    // Deal with remaining blocking threads, which might not be og
    for (long tid : blockingThreadIds) {
      ThreadInfo info = threadInfo(allThreadInfos, bean, tid);
      ContextState contextState = new ContextState(tid, info.getThreadName());
      contextState.name += " BLOCKING";
      contextState.javaStackTrace = info.getStackTrace();
      states.add(contextState);
    }

    snapshots.addFirst(new Snapshot(states, spinner, stats));
    if (snapshots.size() > stapStackHistory) snapshots.removeLast();
    getAndResetSnapshots();
  }

  private ThreadInfo threadInfo(
      ThreadDumper.ThreadInfos cachedInfos, ThreadMXBean bean, Long threadId) {
    Option<ThreadInfo> cached = cachedInfos.forThreadId(threadId);
    if (cached.isEmpty()) {
      log.javaLogger()
          .warn(
              "Did not find info for thread id: "
                  + threadId
                  + " in the cache. Now getting thread info directly.");
      return bean.getThreadInfo(threadId, 100);
    } else return cached.get();
  }
}
