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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static optimus.graph.OGTrace.BLOCK_ID_ALL;
import static optimus.graph.OGTrace.BLOCK_ID_UNSCOPED;
import static optimus.graph.OGTrace.BLOCK_ID_PROPERTY;
import static optimus.graph.OGTrace.FILE_HEADER_SIZE;
import static optimus.graph.OGTrace.gen;
import static optimus.graph.OGTrace.reader_proto;
import static optimus.graph.OGTraceStore.maxUnrecycledTables;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import optimus.core.EdgeList;
import optimus.graph.diagnostics.OGCounterView;
import optimus.graph.diagnostics.PNodeTask;
import optimus.graph.diagnostics.PNodeTaskInfo;
import optimus.graph.diagnostics.PNodeTaskInfoLight;
import optimus.graph.diagnostics.PNodeTaskInfoUtils;
import optimus.graph.diagnostics.PNodeTaskRecorded;
import optimus.graph.diagnostics.SchedulerProfileEntryForUI;
import optimus.graph.diagnostics.messages.ProfilerMessages;
import optimus.graph.diagnostics.messages.ProfilerMessagesGenerator;
import optimus.graph.diagnostics.messages.ProfilerMessagesReader;
import optimus.graph.diagnostics.messages.Signature;
import optimus.graph.diagnostics.trace.OGEventsNewHotspotsObserver;
import sun.misc.Unsafe;

public abstract class OGTraceReader {
  protected int format = -1; // so that we can tell when uninitialised
  int versionMarker; // Int in order to have a magic un-initialized value of MAX_INT
  PThreadContext.Process currentProcess = PThreadContext.Process.none;
  // remap thread IDs between processes
  HashMap<Long, Integer> inProcessThreadIDMap = new HashMap<>();
  private ArrayList<PThreadContext> threads;
  private HashMap<String, DistributedEvents> distributedEvents;

  public static class DistributedEvents {
    public String chainedID;
    public String label;

    public PNodeTaskRecorded sentTask;
    public long sentTime;
    public PThreadContext sentThreadContext;

    public PNodeTaskRecorded receivedTask;
    public long receivedTime;
    public PThreadContext receivedThreadContext;

    public long startedTime;
    public PThreadContext startedThreadContext;

    public long completedTime;
    public PThreadContext completedThreadContext;

    public long serializedResultArrivedTime;
    public PThreadContext serializedResultArrivedThreadContext;

    public long resultReceivedTime;
    public PThreadContext resultReceivedThreadContext;

    public DistributedEvents(String chainedID) {
      this.chainedID = chainedID;
    }

    // Consider making this logic more configurable or at least more explainable
    void onAllEventsMerged() {
      if (sentTask != null) {
        if (sentTask.firstStartTime == 0)
          sentTask.firstStartTime = sentTime; // Alternative: adaptedTime
        if (sentTask.completedTime == 0)
          sentTask.completedTime = resultReceivedTime; // Alternative: resultArrivedTime
        if (sentTask.values == null) sentTask.values = new PNodeTaskRecorded.Values();
        sentTask.values.result = "->" + label;
      }

      if (receivedTask != null) {
        if (receivedTask == sentTask) {
          // Local execution
          // We don't record publishNodeStarted / publishTaskCompleted for locally executed nodes,
          // consider changing!
          if (startedTime == 0) startedTime = receivedTime;
          if (completedTime == 0) completedTime = resultReceivedTime;

          startedThreadContext = receivedThreadContext;
          completedThreadContext = resultReceivedThreadContext;
        }
        receivedTask.firstStartTime = startedTime;
        receivedTask.completedTime = completedTime;

        if (receivedTask.values == null) receivedTask.values = new PNodeTaskRecorded.Values();
        receivedTask.values.result = "<-" + label;
      }
    }
  }

  boolean liveProcess; // mirrored in eventProcessor (remember to update both places when
  // initializing/resetting)
  private final boolean constructPNodes;
  private final boolean ignoreBlocks;
  private final Signature signature = new Signature();

  long firstBaseTimeMillis;
  long firstBaseTimeNanos;

  public final String nanoToUTC(long timestamp) {
    var millis =
        firstBaseTimeMillis + TimeUnit.NANOSECONDS.toMillis(timestamp - firstBaseTimeNanos);
    return Instant.ofEpochMilli(millis).toString();
  }

  public final String nanoToUTCTimeOnly(long timestamp) {
    var millis =
        firstBaseTimeMillis + TimeUnit.NANOSECONDS.toMillis(timestamp - firstBaseTimeNanos);
    var formatSubSecondTime = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    var utcZone = ZoneId.of("UTC");
    return Instant.ofEpochMilli(millis).atZone(utcZone).format(formatSubSecondTime);
  }

  /** The following three fields (tasks, pntis, root) are shared with eventProcessor */
  protected ArrayList<PNodeTaskRecorded> tasks;
  // pntis(0) holds PNTIs that originate from Properties tables
  // pntis(1) holds PNTIs that originate from Events tables of global scope
  // pntis(2...) hold PNTIs that originate from Events tables of profiled() scopes
  // next refactor we should consider making pntis(1...) PNTILights, not PNTIs
  // When isLive = true, contains full set of PNTIs
  protected ArrayList<PNodeTaskInfo[]> pntis = new ArrayList<>();
  protected PNodeTaskRecorded root;

  protected OGTraceReaderEventProcessor eventProcessor;
  protected ProfilerMessagesReader eventReader;

  public final Collection<PNodeTask> getRawTasks() {
    return getRawTasks(true);
  }

  /** Returns true if there are some recorded tasks available */
  public boolean hasRecordedTasks() {
    return !tasks.isEmpty();
  }

  public Collection<PNodeTask> getRawTasks(boolean includeProxies) {
    List<PNodeTask> r = new ArrayList<>();
    for (PNodeTaskRecorded p : tasks) {
      // Only partial information is available if the task is started and then just dropped
      if (p.info == PNodeTaskRecorded.fakeInfo) continue;
      if (includeProxies || !p.isProxy()) r.add(p);
    }
    return r;
  }

  // If offline recorded trace, stats.edges will be incremented when events are written
  // [SEE_READER_EDGES]
  // If live reader, check if any collected data has parents or children (and update stats so that
  // next check is fast)
  public boolean hasEdges() {
    if (stats.hasEdges || stats.edges > 0) return true;
    var rawTasks = getRawTasks();
    for (PNodeTask p : rawTasks) {
      if (p.hasEdges()) {
        stats.hasEdges = true;
        return true;
      }
    }
    return false;
  }

  // Stats about this reader (most useful when loading an offline file)
  public static class OGTraceReaderStatistics implements Serializable {
    public int eventTables;
    public int edgeTables;
    public int edges;
    public boolean hasEdges; // updated for live readers
    public int props;
    public int[] eventCounts;

    OGTraceReaderStatistics(int eventCount) {
      eventCounts = new int[eventCount + 1]; // eventID are 1 based to reserve '0'
    }
  }

  public OGTraceReaderStatistics stats;

  public OGTraceReader(boolean liveProcess, boolean constructPNodes, boolean ignoreBlocks) {
    this.liveProcess = liveProcess;
    this.constructPNodes = constructPNodes;
    this.ignoreBlocks = ignoreBlocks;
    this.eventReader = reader_proto.createCopy();
    reset();
  }

  void processTable(OGTraceStore.Table table, int versionMarker) {
    if (versionMarker != table.getVersionMarker()) {
      return; // Reached a table that was created before reset()
    }

    eventProcessor.ctx_$eq(threadOf(table.getThreadID()));

    // Add to tracing bp: "Processing table: " + table.toString()
    switch (table.getType()) {
      case OGTraceStore.TABLE_EDGES:
        processEdges(table);
        break;
      case OGTraceStore.TABLE_EVENTS:
        processEvents(table);
        break;
      case OGTraceStore.TABLE_HEADER:
        processHeader(table);
        break;
      case OGTraceStore.TABLE_FREE:
        // It's OK to observe freed table
        break;
      default:
        OGTrace.log
            .javaLogger()
            .error("Invalid ogtrace table format: unknown table type {}", table.getType());
    }
  }

  private static String[] createMapping(OGTraceStore.Table table, int size) {
    String[] map = new String[size];
    for (int i = 0; i < size; i++) {
      map[i] = table.getString();
    }
    return map;
  }

  /* Return existing or create a new one */
  private PThreadContext threadOf(long tid) {
    int newID = threads.size();
    var virtualID = inProcessThreadIDMap.putIfAbsent(tid, newID);
    if (virtualID == null) {
      virtualID = newID;
      var ctx = new PThreadContext(virtualID);
      ctx.process = currentProcess;
      threads.add(ctx);
    }
    return threads.get(virtualID);
  }

  //  reads byte buf
  private void processHeader(OGTraceStore.Table table) {
    int version = table.getInt();

    long baseTimeMillis = table.getLong();
    long baseTimeNanos = table.getLong();
    if (firstBaseTimeMillis == 0) {
      firstBaseTimeMillis = baseTimeMillis;
      firstBaseTimeNanos = baseTimeNanos;
    }

    // In order for 'live' times to workout the offset needs to work out to zero
    var offset =
        baseTimeNanos
            - firstBaseTimeNanos
            - MILLISECONDS.toNanos(baseTimeMillis - firstBaseTimeMillis);
    eventProcessor.timeOffset_$eq(offset);

    if (table.size > FILE_HEADER_SIZE) {
      int nameSize = table.getInt();
      signature.setNames(createMapping(table, nameSize));
      int descSize = table.getInt();
      signature.setDescriptors(createMapping(table, descSize));
      if (!signature.equals(gen.getSignature())) {
        OGTrace.log
            .javaLogger()
            .debug(
                "The event mapping from the ogtrace file does not match the default event mapping. Event mapping from the"
                    + " file will be used.");
        eventReader =
            (ProfilerMessagesReader)
                ProfilerMessagesGenerator.createFromBytes(signature, ProfilerMessages.class)
                    .createReaderObject(ProfilerMessagesReader.class);
        eventReader.processor = eventProcessor;
      }
    }
    if (version != OGTrace.FORMAT) {
      String msg =
          String.format(
              "OGTraceReader format %d did not match OGTraceStore format %d. Make sure you are using the same code "
                  + "version to read the ogtrace file as was used to write it.",
              OGTrace.FORMAT, version);
      throw new IllegalArgumentException(msg);
    }
    format = version;
  }

  private void processEvents(OGTraceStore.Table table) {
    stats.eventTables++;
    eventReader.table = table;

    while (table.hasMoreCommands()) {
      int cmd = table.getInt(); // Add to tracing bp eventReader.nameOfEvent(cmd)
      if (cmd < 1 || cmd > eventReader.eventCount) break; // Buffer wasn't full

      stats.eventCounts[cmd]++;
      eventReader.dispatchEvent(cmd);
    }
    eventReader.table = null; // Cleanup ref
  }

  private void processEdges(OGTraceStore.Table table) {
    stats.edgeTables++;
    while (table.hasMoreCommands()) {
      int idFrom = table.getInt();
      PNodeTask from = eventProcessor.taskOf(idFrom);

      int sizeOrType = table.getInt();
      int size;
      if (sizeOrType < 0) {
        from.callees = EdgeList.newSeqOps(-sizeOrType);
        size = table.getInt();
      } else {
        from.callees = EdgeList.newDefault();
        size = sizeOrType;
      }

      for (int i = 0; i < size; i++) {
        int idTo = table.getInt();
        PNodeTask to = eventProcessor.taskOf(Math.abs(idTo));
        if (idTo > 0) {
          if (to.callers == null) to.callers = new ArrayList<>();
          to.callers.add(from);
          from.callees.add(to);
        } else from.callees.addEnqueueEdge(to);
        stats.edges++; // [SEE_READER_EDGES]
      }
    }
  }

  public final ArrayList<PNodeTaskInfo> getScopedTaskInfos(int blk) {
    return getScopedTaskInfos(blk, true);
  }

  /** Returns task infos for a given block, always aggregates proxies */
  public final ArrayList<PNodeTaskInfo> getScopedTaskInfos(int blk, boolean alwaysIncludeTweaked) {
    catchUp();
    return getHotspots(blk, alwaysIncludeTweaked, false, true);
  }

  public final ArrayList<PNodeTaskInfo> getScopedTaskInfos(
      int blk, boolean alwaysIncludeTweaked, boolean includeTweakablesNotTweaked) {
    catchUp();
    return getHotspots(blk, alwaysIncludeTweaked, includeTweakablesNotTweaked, true);
  }

  public final ArrayList<PNodeTaskInfo> getHotspots() {
    return getHotspots(true); // Not likely you want to look at un-aggregated stats
  }

  public ArrayList<PNodeTaskInfo> getHotspots(boolean aggregateProxies) {
    catchUp();
    var infos = getHotspots(BLOCK_ID_ALL, true);
    return aggregateProxies ? PNodeTaskInfoUtils.aggregate(infos) : infos;
  }

  // only used to send everything as-is from dist engine to client
  public ArrayList<PNodeTaskInfo[]> getRawScopedTaskInfos() {
    catchUp();
    return getRawScopedHotspots();
  }

  /** Note that this is 'combined', ie, doesn't respect scopes */
  public PNodeTaskInfo getTaskInfo(int profileID) {
    catchUp();
    // flatten from all Event-originated pntis into the main one
    PNodeTaskInfo pi = pntis.get(OGTrace.BLOCK_ID_PROPERTY)[profileID];
    for (int idx = BLOCK_ID_UNSCOPED; idx < pntis.size(); idx++) {
      PNodeTaskInfo[] pntisForIdx = pntis.get(idx);
      if (pntisForIdx != null && pntisForIdx.length > profileID && pntisForIdx[profileID] != null)
        pi = pi.combine(pntisForIdx[profileID]);
    }
    return pi;
  }

  protected void liveCatchUp() throws InterruptedException {}

  protected void catchUp() {}

  private ArrayList<PNodeTaskInfo> getHotspots(int blk, boolean alwaysIncludeTweaked) {
    return getHotspots(blk, alwaysIncludeTweaked, false, false);
  }

  // skip the PNTIs that have nothing to show (typically this means appears in Properties but
  // not in Events)
  // [SEE_TWEAKED] note: tweakables that were never run may not be included here
  public static boolean includeProperty(
      PNodeTaskInfo p, boolean alwaysIncludeTweaked, boolean includeTweakableNotTweaked) {
    boolean tweakCheck =
        alwaysIncludeTweaked
            && ((includeTweakableNotTweaked && p.isDirectlyTweakable()) || p.wasTweaked());
    return tweakCheck
        || p.hasValuesOfInterest()
        || (p.dontTrackForInvalidation() && !p.isInternal());
  }

  private ArrayList<PNodeTaskInfo> getHotspots(
      int blk,
      boolean alwaysIncludeTweaked,
      boolean includeTweakableNotTweaked,
      boolean aggregateProxies) {
    ArrayList<PNodeTaskInfo> r = new ArrayList<>();
    PNodeTaskInfo[] combined = getAllTaskInfosCombined(blk);
    for (PNodeTaskInfo p : combined) {
      if (p == null) continue;
      if (aggregateProxies || includeProperty(p, alwaysIncludeTweaked, includeTweakableNotTweaked))
        r.add(p);
    }

    if (aggregateProxies) {
      r = PNodeTaskInfoUtils.aggregate(r);
      r.removeIf(p -> !includeProperty(p, alwaysIncludeTweaked, includeTweakableNotTweaked));
    }

    return r;
  }

  /** Returns all infos without filtering */
  public PNodeTaskInfo[] getAllTaskInfosCombined() {
    catchUp();
    return getAllTaskInfosCombined(BLOCK_ID_ALL);
  }

  private PNodeTaskInfo[] getAllTaskInfosCombined(int blk) {
    // iterate PNTIs that originate from Property Tables (blk == BLOCK_ID_PROPERTY)
    PNodeTaskInfo[] properties = pntis.get(OGTrace.BLOCK_ID_PROPERTY);
    PNodeTaskInfo[] r = new PNodeTaskInfo[properties.length];

    for (PNodeTaskInfo pi : properties) {
      PNodeTaskInfo p = pi;
      if (p == null) continue;

      if (blk == BLOCK_ID_ALL) {
        for (int blockID = BLOCK_ID_UNSCOPED; blockID < pntis.size(); blockID++) {
          PNodeTaskInfo[] pntisForBlock = pntis.get(blockID);
          if (pntisForBlock != null
              && pntisForBlock.length > pi.id
              && pntisForBlock[pi.id] != null) {
            p = p.combine(pntisForBlock[pi.id]);
          }
        }
      } else if (pntis.size() > blk) {
        // merge in the PNTIs that originate from Event tables of the requested blk
        PNodeTaskInfo[] pntisFromEvents = pntis.get(blk);
        if (pntisFromEvents != null
            && pntisFromEvents.length > pi.id
            && pntisFromEvents[pi.id] != null) {
          p = p.combine(pntisFromEvents[pi.id]);
        }
      } else {
        p = p.dup();
      }

      p.freezeLiveInfo(); // re-populates flags, cache name, etc, from the live NTI into this PNTI
      r[pi.id] = p;
    }
    return r;
  }

  private ArrayList<PNodeTaskInfo[]> getRawScopedHotspots() {
    return pntis;
  }

  public final boolean hasSchedulerTimes() {
    return !(threads.isEmpty() && eventProcessor.counters().isEmpty());
  }

  public ArrayList<OGCounterView<?>> getOGCounters() {
    ArrayList<OGCounterView<?>> counters = new ArrayList<>();
    getSchedulerTimes(counters, false);
    return counters;
  }

  public ArrayList<PThreadContext> getThreads() {
    ArrayList<OGCounterView<?>> counters = new ArrayList<>();
    return getSchedulerTimes(counters, false);
  }

  /**
   * onlyWithSpans = true used in tests to make assertions only about threads that had something run
   * on them..
   */
  public ArrayList<PThreadContext> getSchedulerTimes(
      ArrayList<OGCounterView<?>> counters, boolean onlyWithSpans) {
    catchUp();
    counters.clear();
    for (var counter : eventProcessor.counters()) {
      if (counter != null && !counter.events().isEmpty()) counters.add(counter.asViewable());
    }

    ArrayList<PThreadContext> threads = new ArrayList<>();
    for (PThreadContext thread : this.threads) {
      if (!onlyWithSpans || !thread.spans.isEmpty()) threads.add(thread);
    }
    return threads;
  }

  // won't work in live mode, need a copy!!!
  public ArrayList<DistributedEvents> getDistributedEvents() {
    ArrayList<DistributedEvents> r = new ArrayList<>();
    for (DistributedEvents event : distributedEvents.values()) {
      event.onAllEventsMerged();
      r.add(event);
    }
    return r;
  }

  public Map<String, SchedulerProfileEntryForUI> getSchedulerProfiles() {
    catchUp();

    // the reconstructed PThreadContexts in this.threads have the event traces we need to determine
    // on-graph spans
    // but the live PThreadContexts in OGLocalTables have the thread names and live metrics
    // let's get the live ones, including ID, not just thread names (OGTrace.getContexts should
    // probably do that)
    Map<String, SchedulerProfileEntryForUI> result = new HashMap<>();
    OGLocalTables.forAllContexts(
        (lt, schedulerProfileEntry) -> {
          SchedulerProfileEntryForUI prf =
              SchedulerProfileEntryForUI.apply(schedulerProfileEntry, 0, 0);
          result.put(lt.ctx.name, prf);
        });

    return result;
  }

  public interface PNTVisitor {
    void apply(PNodeTask pnt, PNodeTaskInfo pnti);
  }

  public void visitNodes(PNTVisitor f) {
    for (PNodeTask tsk : tasks) {
      f.apply(tsk, getTaskInfo(tsk.infoId()));
    }
  }

  public void dropOGCounters() {
    eventProcessor.counters().clear();
  }

  public void dropLeftOf(long time) {
    for (PThreadContext thread : this.threads) {
      ArrayList<PThreadContext.Span> spans = new ArrayList<>();
      for (PThreadContext.Span span : thread.spans) {
        if (span.end >= time) {
          span.start = Math.max(span.start, time);
          spans.add(span);
        }
      }
      thread.spans = spans;
    }
  }

  public void dropRightOf(long time) {
    for (PThreadContext thread : this.threads) {
      ArrayList<PThreadContext.Span> spans = new ArrayList<>();
      for (PThreadContext.Span span : thread.spans) {
        if (span.start <= time) {
          span.end = Math.min(span.end, time);
          spans.add(span);
        }
      }
      thread.spans = spans;
    }
  }

  private void collectParents(ArrayList<PNodeTask> preTasks, PNodeTask tsk, long start, long end) {
    PNodeTask cur = tsk;
    while (cur.visitedID == 0) {
      cur.visitedID = 1;
      if (cur.isActive(start, end) && !cur.isRunning(start, end)) preTasks.add(cur);
      if (cur.callers == null || cur.callers.size() != 1) break;
      cur = cur.callers.get(0);
    }
  }

  /**
   * 1. For each running task in range: a. Collect all of the tasks parents, until root or # parents
   * > 1 b. For each of these parents: i. Collect children who only start after range end
   *
   * <p>Idea is: a. Select a range in the timeline where we have reduced concurrency - This is one
   * where we have few green lines (indicating running nodes) and many blank lines (indicating idle
   * threads)
   *
   * <p>b. Concurrency branches will print the children of the parents of the currently running
   * nodes (i.e. the green lines) which don't start until after the range end
   *
   * <p>c. If these nodes were scheduled earlier then we might expect greater concurrency during the
   * selected time range
   *
   * <p>d. Causes of 'reduced concurrency' could be: 1. Order dependency (expected or otherwise) 2.
   * Seq stacks 3. Sync stacks
   */
  public HashMap<PNodeTaskInfo, ArrayList<PNodeTaskInfo>> getConcurrencyBranches(
      long start, long end) {
    ArrayList<PNodeTask> runningTasks = new ArrayList<>();
    for (PNodeTask tsk : tasks) {
      tsk.visitedID = 0; // Reset flag
      if (tsk.isActive(start, end) && tsk.isRunning(start, end)) runningTasks.add(tsk);
    }

    ArrayList<PNodeTask> parentsTasks = new ArrayList<>();
    for (PNodeTask tsk : runningTasks) {
      collectParents(parentsTasks, tsk, start, end);
    }

    HashMap<PNodeTaskInfo, ArrayList<PNodeTaskInfo>> branchesToPrint = new HashMap<>();
    for (PNodeTask tsk : parentsTasks) {
      PNodeTaskInfo parentInfo = getTaskInfo(tsk.infoId());
      for (PNodeTask child : tsk.callees) {
        // We want to find tasks that didn't start until after the period
        if (child.firstStartTime < end) continue;

        branchesToPrint
            .computeIfAbsent(parentInfo, k -> new ArrayList<>())
            .add(getTaskInfo(child.infoId()));
      }
    }

    return branchesToPrint;
  }

  public void printConcurrencyBranches(long start, long end) {
    HashMap<PNodeTaskInfo, ArrayList<PNodeTaskInfo>> branches = getConcurrencyBranches(start, end);

    if (branches.isEmpty()) {
      OGTrace.log.javaLogger().info("Concurrency branches were empty.");
    } else {
      OGTrace.log
          .javaLogger()
          .info(
              "Found " + branches.size() + " possible methods where concurrency could be improved");
      for (Map.Entry<PNodeTaskInfo, ArrayList<PNodeTaskInfo>> entry : branches.entrySet()) {
        PNodeTaskInfo parentInfo = entry.getKey();
        for (PNodeTaskInfo childInfo : entry.getValue()) {
          OGTrace.log
              .javaLogger()
              .info(
                  parentInfo.fullNameAndSource()
                      + " should schedule a call to "
                      + childInfo.fullNameAndSource()
                      + "earlier");
        }
      }
    }
  }

  public PNodeTask getSyncRootTask() {
    return root;
  }

  public void reset() {
    versionMarker = Integer.MAX_VALUE;
    eventProcessor = new OGTraceReaderEventProcessor(constructPNodes, ignoreBlocks);
    eventReader.processor = eventProcessor;

    pntis = eventProcessor.pntisByBlockID();
    tasks = eventProcessor.tasks();
    root = eventProcessor.root();
    distributedEvents = eventProcessor.distributedEvents();
    threads = new ArrayList<>();
    inProcessThreadIDMap = new HashMap<>();
    stats = new OGTraceReaderStatistics(eventReader.eventCount);
  }

  // REVIEW resetting liveProcess when switching modes
  void setLiveProcess(boolean liveProcess) {
    this.liveProcess = liveProcess;
  }

  public final boolean isLive() {
    return liveProcess;
  }

  public final boolean isConstructing() {
    return constructPNodes;
  }

  public int getFormat() {
    return format;
  }

  @Override
  public String toString() {
    return String.format("OGTraceReader[%d]", format);
  }
}

/** Singleton object */
class OGTraceReaderLive extends OGTraceReader implements StoreTraceObserver {
  private final OGTraceStore trace;

  abstract static class LiveProcessorMessage {
    OGTraceStore.Table table;

    LiveProcessorMessage(OGTraceStore.Table table) {
      this.table = table;
    }

    abstract void afterProcessing();
  }

  private final LinkedBlockingDeque<LiveProcessorMessage> tablesQ =
      new LinkedBlockingDeque<>(maxUnrecycledTables);

  OGTraceReaderLive(OGTraceStore trace) {
    super(true, false, false);
    this.firstBaseTimeMillis = System.currentTimeMillis();
    this.firstBaseTimeNanos = System.nanoTime();
    this.eventReader = reader_proto.createCopy();
    this.eventReader.processor = eventProcessor;
    this.trace = trace;
    trace.observer = this;
  }

  @Override
  public void reset() {
    synchronized (this) {
      super.reset();
    }
  }

  @Override
  public Collection<PNodeTask> getRawTasks(boolean includeProxies) {
    return NodeTrace.getTraceBy(n -> true, includeProxies);
  }

  /**
   * Offer the table to live catchup processor/reader only if current mode recycles tables Currently
   * the only mode that doesn't recycle is traceEvents, where we preserve the entire event log in
   * the ogtrace file [SEE_STORE_LOCK]
   */
  @Override
  public void postProcess(OGTraceStore.Table table) {
    // Add to tracing bp "Offering a table: " + table;
    if (liveProcess) {
      LiveProcessorMessage msg =
          new LiveProcessorMessage(table) {
            void afterProcessing() {
              trace.returnTable(table);
            }
          };
      if (!tablesQ.offerLast(msg)) processLiveMessage(msg);
    }
  }

  @Override
  protected void liveCatchUp() throws InterruptedException {
    LiveProcessorMessage msg;
    //noinspection ConstantConditions
    while ((msg = tablesQ.take()) != null) {
      OGLocalTables.expungeRemovedThreads();
      processLiveMessage(msg);
    }
  }

  private void processLiveMessage(LiveProcessorMessage msg) {
    synchronized (this) {
      pntis.set(BLOCK_ID_PROPERTY, trace.getCollectedPNTIs());
      processTable(msg.table.copyForReadingAndObservePosition(), trace.versionMarker);
    }
    msg.afterProcessing();
  }

  @Override
  protected void catchUp() {
    NodeTrace.registerTweakablePropertiesWithOGTrace();
    ArrayList<OGTraceStore.Table> processTables = OGLocalTables.snapTables();
    if (!processTables.isEmpty()) {
      // We will wait for all our tables
      CountDownLatch latch = new CountDownLatch(processTables.size());

      for (OGTraceStore.Table table : processTables) {
        try {
          tablesQ.putLast(
              new LiveProcessorMessage(table) {
                void afterProcessing() {
                  latch.countDown();
                }
              });
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      // shouldn't need the timeout but let's follow good practice
      try {
        latch.await(5000, MILLISECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    updatePNTIsFromLive();
  }

  private void updatePNTIsFromLive() {
    // populate PNTIs from OGLocalTables
    synchronized (this) {
      updatePropertyPNTIsFromLive();
      updateGlobalPNTIsFromLive();
      updateScopedPNTIsFromLive();
    }
  }

  private void updatePropertyPNTIsFromLive() {
    PNodeTaskInfo[] propertyPntis = trace.getCollectedPNTIs();
    if (pntis.isEmpty()) pntis.add(propertyPntis);
    else pntis.set(BLOCK_ID_PROPERTY, propertyPntis);
  }

  // if new hotspots, replace (not merge) the contents of pnti[1] with what snapPNTIs gives us
  // if old hotspots - do nothing. catchUp already populated pntis
  // other modes, e.g. none, also do nothing: switch to none calls catchUp
  private void updateGlobalPNTIsFromLive() {
    if (OGTrace.getTraceMode() instanceof OGEventsNewHotspotsObserver) {
      int propCount = pntis.get(BLOCK_ID_PROPERTY).length;
      // get global pntis from both live and dead scheduler threads
      ArrayList<PNodeTaskInfoLight[]> globalPNTILs = OGLocalTables.snapPNTIS();
      PNodeTaskInfo[] pntisToInsert = new PNodeTaskInfo[propCount];
      // and put them into pntis[1]
      for (PNodeTaskInfoLight[] t_pntis : globalPNTILs) {
        for (int i = 0; i < t_pntis.length; ++i) {
          if (t_pntis[i] != null) {
            if (i >= pntisToInsert.length) continue;
            if (pntisToInsert[i] == null) pntisToInsert[i] = new PNodeTaskInfo(i);
            pntisToInsert[i] = pntisToInsert[i].combine(t_pntis[i]);
          }
        }
      }

      // guaranteed to not be empty if this is called after updatePropertyPNTIsFromLive
      if (pntis.size() == 1) pntis.add(pntisToInsert);
      else pntis.set(BLOCK_ID_UNSCOPED, pntisToInsert);
    }
  }

  // if new hotspots, replace (not merge) the contents of pnti[2+] with what snapPNTIs gives us
  // if old hotspots - do nothing. catchUp already populated pntis
  // other modes, e.g. none, also do nothing: switch to none calls catchUp
  private void updateScopedPNTIsFromLive() {
    if (OGTrace.getTraceMode() instanceof OGEventsNewHotspotsObserver) {
      int propCount = pntis.get(BLOCK_ID_PROPERTY).length;
      // get scoped pntis from both live and dead scheduler threads
      ArrayList<Map<Integer, PNodeTaskInfoLight[]>> allScoped = OGLocalTables.snapAllScopedPNTIS();
      // and insert them here
      Map<Integer, PNodeTaskInfo[]> pntisToInsert = new HashMap<>();
      for (Map<Integer, PNodeTaskInfoLight[]> m : allScoped) {
        for (Map.Entry<Integer, PNodeTaskInfoLight[]> e : m.entrySet()) {
          int blk = e.getKey();
          PNodeTaskInfo[] pntisBlk =
              pntisToInsert.computeIfAbsent(blk, k -> new PNodeTaskInfo[propCount]);
          PNodeTaskInfoLight[] t_pntis = e.getValue();
          for (int i = 0; i < t_pntis.length; ++i) {
            if (t_pntis[i] != null) {
              if (i >= propCount) continue;
              if (pntisBlk[i] == null) pntisBlk[i] = new PNodeTaskInfo(i);
              pntisBlk[i] = pntisBlk[i].combine(t_pntis[i]);
            }
          }
        }
      }

      for (Map.Entry<Integer, PNodeTaskInfo[]> e : pntisToInsert.entrySet()) {
        int blk = e.getKey();
        while (pntis.size() <= blk) pntis.add(null);
        pntis.set(blk, e.getValue());
      }
    }
  }
}

class OGTraceFileReader extends OGTraceReader {

  private static final Unsafe U = UnsafeAccess.getUnsafe();
  private PNodeTaskInfo[] allTasksInfosCombined;

  OGTraceFileReader(String name) throws IOException {
    super(false, true, false);
    processAndReleaseFile(PThreadContext.Process.none, new RandomAccessFile(name, "r"), true);
  }

  OGTraceFileReader(RandomAccessFile readFile, boolean constructPNodes, boolean ignoreBlocks)
      throws IOException {
    super(false, constructPNodes, ignoreBlocks);
    processAndReleaseFile(PThreadContext.Process.none, readFile, false);
  }

  private void parseProcessesFromFiles(File[] files) throws IOException {
    // Files are sorted in the startTime entry just to have pretty (non-negative) time offsets in
    // the UI
    Arrays.sort(
        files,
        (o1, o2) -> {
          var p1 = PThreadContext.Process.from(o1.getName());
          var p2 = PThreadContext.Process.from(o2.getName());
          return (int) (p1.startTime - p2.startTime);
        });

    // Give different hosts nice names like A, B, C....
    // and different pids nice numbers like 1, 2, 3
    var nickNames = new HashMap<String, String>();
    var nickPID = 1;
    for (var file : files) {
      var process = PThreadContext.Process.from(file.getName());
      process.nickPID = nickPID++;
      process.nickName = nickNames.get(process.host);
      if (process.nickName == null) {
        process.nickName = Character.toString('A' + nickNames.size());
        nickNames.put(process.host, process.nickName);
      }
      // Real processing call ...
      processAndReleaseFile(process, new RandomAccessFile(file, "r"), false);
    }
  }

  OGTraceFileReader(File[] files, boolean multipleProcesses) throws IOException {
    super(false, true, false);
    if (multipleProcesses) parseProcessesFromFiles(files);
    else
      for (var file : files) {
        processAndReleaseFile(PThreadContext.Process.none, new RandomAccessFile(file, "r"), false);
      }
  }

  /** only close file channel when file is processed once, e,g, OGTraceFileReader constructor */
  private void processAndReleaseFile(
      PThreadContext.Process process, RandomAccessFile readFile, boolean release)
      throws IOException {
    FileChannel fileChannel = readFile.getChannel();
    long fileChannelSize = fileChannel.size();
    long memoryMapSize = OGTraceStore.fileMapSize;

    eventProcessor.__inProcessInfoIDs_$eq(new Int2IntOpenHashMap());
    this.currentProcess = process;

    long readLength = 0;
    while (true) {
      // Map some large chunk into memory
      long mapSize = Math.min(fileChannelSize - readLength, memoryMapSize);
      MappedByteBuffer buf = fileChannel.map(FileChannel.MapMode.READ_ONLY, readLength, mapSize);
      buf.order(ByteOrder.nativeOrder());
      int read = processTable(buf, mapSize < memoryMapSize);

      // See https://docs.oracle.com/javase/8/docs/api/java/nio/MappedByteBuffer.html
      // A mapped byte buffer and the file mapping that it represents remain valid until the buffer
      // itself is GC-ed.
      if (release) U.invokeCleaner(buf);

      if (read == 0) break;
      readLength += read;
    }

    // Release file locks
    if (release) {
      fileChannel.close();
      readFile.close();
    }

    inProcessThreadIDMap.clear(); // only while processing files
    eventProcessor.__inProcessTaskIDs().clear();
    eventProcessor.__inProcessInfoIDs_$eq(null);
    versionMarker = Integer.MAX_VALUE;
  }

  private int processTable(ByteBuffer buf, boolean finalChunk) {
    int readLength = 0;
    while (buf.remaining() > OGTraceStore.Table.HEADER_SIZE) {
      OGTraceStore.Table storeTable = new OGTraceStore.Table(buf, buf.position(), 0, -1);

      int tableSize = storeTable.getSize();
      int tableVersionMarker = storeTable.getVersionMarker();
      int tableType = storeTable.getType();
      // Some clear memory is found and we can stop processing....
      if (tableType == OGTraceStore.TABLE_NEVER_USED) return 0;
      if (this.versionMarker == Integer.MAX_VALUE) this.versionMarker = tableVersionMarker;

      if (tableSize > buf.remaining()) {
        if (finalChunk) {
          tableSize = buf.remaining();
          storeTable.setSize(tableSize);
        } else {
          break; // Need to re-map large chunk....
        }
      }

      readLength += tableSize; // Total size updated
      buf.position(buf.position() + tableSize);

      processTable(storeTable, this.versionMarker);
    }
    return readLength;
  }

  @Override
  public PNodeTaskInfo[] getAllTaskInfosCombined() {
    if (allTasksInfosCombined == null) allTasksInfosCombined = super.getAllTaskInfosCombined();
    return allTasksInfosCombined;
  }
}

class OGTraceLocalReader extends OGTraceReader {
  private final PNodeTaskInfo[] allTasksInfos;

  OGTraceLocalReader(PNodeTaskInfo[] allTasksInfos) {
    super(false, false, false);
    this.allTasksInfos = allTasksInfos;
  }

  @Override
  public ArrayList<PNodeTaskInfo> getHotspots(boolean aggregateProxies) {
    List<PNodeTaskInfo> list = Arrays.asList(allTasksInfos);
    return aggregateProxies ? PNodeTaskInfoUtils.aggregate(list) : new ArrayList<>(list);
  }
}
