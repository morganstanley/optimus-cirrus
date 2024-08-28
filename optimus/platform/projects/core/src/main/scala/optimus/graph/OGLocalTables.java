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

import static java.lang.System.nanoTime;
import static optimus.graph.OGTrace.trace;
import static optimus.graph.diagnostics.PNodeTaskInfoUtils.merge;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import optimus.core.IndexedArrayList;
import optimus.graph.diagnostics.PNodeTaskInfoLight;
import optimus.graph.diagnostics.PNodeTaskInfoUtils;
import optimus.graph.diagnostics.SchedulerProfileEntry;
import optimus.graph.diagnostics.messages.ProfilerEventsWriter;
import optimus.platform.EvaluationQueue;

// Anything declared here is available both in LocalTables and in the static removedLocalTables
// member.

/**
 * Need to generalize when/if we will support multiple trace stores (note the use of
 * OGTrace.trace.XXX)
 */
public class OGLocalTables extends RemovableLocalTables {
  private static class LocalTablesList extends IndexedArrayList<OGLocalTables> {
    @Override
    protected OGLocalTables[] createInitialArray() {
      return new OGLocalTables[8];
    }
  }

  /** Use for any static member manipulations */
  private static final Object _lock = new Object();
  /**
   * Active tables: Attached to EC.current.prfCtx OR temporary and bracketed by getOrAcquire /
   * release Threads other than helper threads attach LocalTable and keep it until they are GCed
   * Helper threads (lots in the case of virtual threads) will be nicer and will release LocalTables
   * while idle (non VT case) or waiting (VT case) and aggressively return on exit
   * [SEE_LOCK_LOCAL_TABLES]
   */
  private static final LocalTablesList activeTables = new LocalTablesList();

  private static final LocalTablesList parkedTables = new LocalTablesList();

  /**
   * GC will "register" removed threads into this queue we copy events from a thread's local tables
   * into dead thread sums
   */
  private static final ReferenceQueue<Thread> _refQueue = new ReferenceQueue<>();

  private static SchedulerProfileEntry schedulerTimesForRemovedThreads =
      SchedulerProfileEntry.apply();
  private static PNodeTaskInfoLight[] pntisForRemovedThreads;
  private static Map<Integer, PNodeTaskInfoLight[]> pntisForRemovedThreadsScoped;

  @Override
  public String toString() {
    return "LT" + (shortUsageCount > 0 ? "s" : "") + ownerID + " " + ownerName;
  }

  /** Make sure to release() after use */
  public static OGLocalTables getOrAcquire() {
    return getOrAcquire(null);
  }

  /** Make sure to release() after use */
  public static OGLocalTables getOrAcquire(EvaluationQueue eq) {
    OGSchedulerContext ec;
    if (eq instanceof OGSchedulerContext) ec = (OGSchedulerContext) eq;
    else ec = OGSchedulerContext._TRACESUPPORT_unsafe_current();
    if (ec != null) {
      return ec.prfCtx;
    } else {
      var lt = borrowedTable.get();
      if (lt == null) {
        lt = acquireProfilingContext(false);
        borrowedTable.set(lt);
      }
      lt.shortUsageCount++;
      return lt;
    }
  }

  public void release() {
    // It's not a temporary table
    if (shortUsageCount <= 0) return;

    shortUsageCount--;
    if (shortUsageCount == 0) {
      borrowedTable.remove();
      releaseActive();
    }
  }

  public static OGLocalTables acquireProfilingContext(boolean threadAttached) {
    OGLocalTables prfCtx;
    synchronized (_lock) {
      prfCtx = parkedTables.pop();
      if (prfCtx != null) {
        activeTables.add(prfCtx);
      } else {
        prfCtx = new OGLocalTables();
        activeTables.add(prfCtx);
      }
    }
    if (threadAttached) prfCtx.owner = new LTRef(prfCtx);
    prfCtx.ownerName = Thread.currentThread().getName();
    prfCtx.ownerID = Thread.currentThread().getId();

    return prfCtx;
  }

  /** Allows thread to give up its profiling context aggressively as opposed to waiting for GC */
  public static void releaseProfilingContext(OGSchedulerContext schedulerContext) {
    schedulerContext.prfCtx.releaseActive();
    schedulerContext.prfCtx = null;
  }

  private void releaseActive() {
    if (owner != null) {
      owner.clear();
      owner = null;
    }
    synchronized (_lock) {
      activeTables.remove(this);
      parkedTables.add(this);
    }
  }

  /* Used when OGScheduler.current.prfCtx is not available and only inside of acquire/release */
  private static final ThreadLocal<OGLocalTables> borrowedTable = new ThreadLocal<>();

  private static class LTRef extends WeakReference<Thread> {
    OGLocalTables lt;

    LTRef(OGLocalTables lt) {
      super(Thread.currentThread(), _refQueue);
      this.lt = lt;
    }
  }

  /**
   * Scans for removed threads and updates the stats, removes/returns tables on those threads
   * [SEE_LOCAL_TABLES_LOCK]
   */
  static void expungeRemovedThreads() {
    Reference<?> ref;
    while ((ref = _refQueue.poll()) != null) {
      LTRef ltRef = (LTRef) ref;
      OGLocalTables lt = ltRef.lt;

      synchronized (_lock) {
        activeTables.remove(lt);
        incorporateExpunged(lt);
        schedulerTimesForRemovedThreads.add(lt.ctx.observedValuesSinceLast(0));
        lt.ctx.record(); // Same as above but GridProfiler keeps it separately
        pntisForRemovedThreads = merge(pntisForRemovedThreads, lt.ctx.getUnscopedPNTIs());
        Map<Integer, PNodeTaskInfoLight[]> scopedPntis = lt.ctx.getScopedPntis();
        if (pntisForRemovedThreadsScoped == null) pntisForRemovedThreadsScoped = new HashMap<>();
        for (Map.Entry<Integer, PNodeTaskInfoLight[]> e : scopedPntis.entrySet())
          pntisForRemovedThreadsScoped.merge(e.getKey(), e.getValue(), PNodeTaskInfoUtils::merge);
      }
      // Add to tracing bp: "Giving up table: " + lt.eventsTrace.getTable()
      trace.postProcessTable(lt.eventsTrace.getTable());
      trace.postProcessTable(lt.edgeTrace);
    }
  }

  public final ProfilerEventsWriter eventsTrace;
  public OGTraceStore.Table edgeTrace;
  public final PThreadContext ctx;
  LTRef owner;
  String ownerName;
  long ownerID;
  int shortUsageCount;

  public OGLocalTables() {
    super();
    this.ctx = new PThreadContext(Thread.currentThread().getId(), Thread.currentThread().getName());
    this.eventsTrace = OGTrace.writer_proto.createCopy(trace);
  }

  /** [SEE_LOCAL_TABLES_LOCK] */
  public static void resetTables() {
    synchronized (_lock) {
      forAllContexts(
          (lt, observedSchedulerProfileEntry) -> {
            lt.ctx.lastObservedValues = observedSchedulerProfileEntry; // this is how we 'reset'
            lt.ctx.resetAllPNodeTaskInfos();
          });
      schedulerTimesForRemovedThreads = SchedulerProfileEntry.apply();
      pntisForRemovedThreads = null;
      pntisForRemovedThreadsScoped = null;
    }
  }

  /** Grabs the copy of tables for catchUp calls [SEE_LOCAL_TABLES_LOCK] */
  static ArrayList<OGTraceStore.Table> snapTables() {
    ArrayList<OGTraceStore.Table> r = new ArrayList<>();
    forAllContexts(
        (lt, schedulerProfileEntry) -> {
          var table = lt.edgeTrace;
          if (table != null) r.add(table);

          table = lt.eventsTrace.getTable();
          if (table != null) r.add(table);
        });
    return r;
  }

  static ArrayList<PNodeTaskInfoLight[]> snapPNTIS() {
    ArrayList<PNodeTaskInfoLight[]> r = new ArrayList<>();
    forAllContexts((lt, schedulerProfileEntry) -> r.add(lt.ctx.getAllPNodeTaskInfos()));
    synchronized (_lock) {
      if (pntisForRemovedThreads != null && pntisForRemovedThreads.length > 0)
        r.add(pntisForRemovedThreads);
    }
    return r;
  }

  static ArrayList<Map<Integer, PNodeTaskInfoLight[]>> snapAllScopedPNTIS() {
    ArrayList<Map<Integer, PNodeTaskInfoLight[]>> s = new ArrayList<>();
    forAllContexts((lt, schedulerProfileEntry) -> s.add(lt.ctx.getScopedPntis()));
    synchronized (_lock) {
      if (pntisForRemovedThreadsScoped != null && !pntisForRemovedThreadsScoped.isEmpty())
        s.add(pntisForRemovedThreadsScoped);
    }
    return s;
  }

  /** Calls 'process' for every context [SEE_LOCAL_TABLES_LOCK] */
  public static void forAllContexts(BiConsumer<OGLocalTables, SchedulerProfileEntry> process) {
    expungeRemovedThreads();
    synchronized (_lock) {
      activeTables.foreach(lt -> process.accept(lt, lt.ctx.observedValues()));
      parkedTables.foreach(lt -> process.accept(lt, lt.ctx.observedValues()));
    }
  }

  public static void forAllRemovables(Consumer<RemovableLocalTables> process) {
    expungeRemovedThreads();
    synchronized (_lock) {
      activeTables.foreach(lt -> process.accept(lt));
      parkedTables.foreach(lt -> process.accept(lt));
      applyToExpunged(process);
    }
  }

  /**
   * Collects all contexts into a map of SchedulerProfileEntry by name WATCH OUT: Thread names are
   * not guaranteed to be unique and this function will select one of them
   */
  public static Map<String, SchedulerProfileEntry> getContexts() {
    Map<String, SchedulerProfileEntry> m = new HashMap<>();
    getSchedulerTimes(m);
    return m;
  }

  public static SchedulerProfileEntry getSchedulerTimes() {
    return getSchedulerTimes(null);
  }

  private static SchedulerProfileEntry getSchedulerTimes(Map<String, SchedulerProfileEntry> m) {
    SchedulerProfileEntry total = SchedulerProfileEntry.apply();
    long currentTime = nanoTime();
    synchronized (_lock) {
      forAllContexts(
          (localTable, spe) -> {
            SchedulerProfileEntry sinceLast = localTable.ctx.observedValuesSinceLast(currentTime);
            if (m != null)
              // Consider: Threads with the same name!
              m.putIfAbsent(localTable.ownerName + "@" + localTable.ownerID, sinceLast);
            total.add(sinceLast);
          });

      // make sure we account for any threads that were gc-ed

      total.add(schedulerTimesForRemovedThreads);
      if (m != null) m.putIfAbsent("GCed Thread(s)", schedulerTimesForRemovedThreads);
    }
    return total;
  }
}
