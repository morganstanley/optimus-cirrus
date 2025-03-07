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

import static optimus.graph.DiagnosticSettings.getIntProperty;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import msjava.slf4jutils.scalalog.Logger;
import msjava.tools.util.MSProcess;
import optimus.breadcrumbs.Breadcrumbs;
import optimus.core.EdgeIDList;
import optimus.core.MonitoringBreadcrumbs$;
import optimus.core.TPDMask;
import optimus.graph.cache.NCSupport;
import optimus.graph.diagnostics.PNodeTaskInfo;
import optimus.graph.diagnostics.gridprofiler.GridProfiler$;
import optimus.graph.diagnostics.messages.ProfilerEventsWriter;
import optimus.utils.JavaVersionCompat;

interface StoreTraceObserver {
  // [SEE_STORE_LOCK]
  void postProcess(OGTraceStore.Table table);
}

/**
 * Basic ideas: 1. We allocate large chunks from memory mapped files at a time. 2. We should be able
 * to crash at any point (system will flash the mapping) and then read the state
 *
 * <p>Consider: 1. Copy of FullTraceBuffer Remove one, consider moving into cpp space 2. Have isLive
 * mode = T/F 3. The buffer manager, uses (optionally) memory-mapped files with direct byte buffers
 * as fallback
 */
public class OGTraceStore {
  public static final Charset charSet = StandardCharsets.UTF_8;
  private static final Logger log =
      msjava.slf4jutils.scalalog.package$.MODULE$.getLogger("OGTraceStore");

  private static final short VERSION_INVALID = 0;
  static final short TABLE_NEVER_USED = 0;
  public static final short TABLE_EVENTS = 1;
  static final short TABLE_EDGES = 2;
  static final short TABLE_HEADER = 4;
  static final short TABLE_FREE = 5;
  private static final String[] tableNames =
      new String[] {"?", "Events", "Edges", "Properties", "Header", "Free"};

  // To debug table allocation set to LARGEST_MESSAGE * 2
  static final int fileMapSize =
      getIntProperty("optimus.profiler.fullTraceInitialMapBytes", 10 * 1024 * 1024);
  private static final int perThreadSize =
      getIntProperty("optimus.profiler.preThreadAlloc", 1024 * 1024);
  static int maxUnrecycledTables = getIntProperty("optimus.profiler.maxUnrecycledTables", 200);
  // complete record, but smaller than perThreadSize)

  private File fileLocation;
  private RandomAccessFile backingFile;
  private FileChannel fileChannel;
  private final String filePrefix;
  private long totalMappedSize;

  // To support quick reset() on stores backed by the same memory mapped file.
  // Alternatives:
  // 1. Create a new mmap file every time.
  //    Easier in C++, as handles are controlled and the old file can be safely deleted
  // 2. Zero out headers or at least 1 byte from tables previously allocated
  // 3. Zero out just the header of the next table, after table allocation, need to carefully not
  // overreach
  volatile short versionMarker = 1;

  private ByteBuffer bstrBuf; // Backing Store Buffer (bstrBuf) We allocate buffer at a time....
  private final boolean isLive; // Optimize for 'live' session vs store and view later....

  // Contains full set of PNTIs, to avoid writing data twice, effectively BLOCK_ID_PROPERTY copy
  private volatile PNodeTaskInfo[] pntis = new PNodeTaskInfo[0];
  private ConcurrentHashMap<String, Integer> stringIDs; // String constants
  private ConcurrentHashMap<Long, Long> threadIDs; // Written out thread names for ids
  // Lock around any Table management [SEE_STORE_LOCK]
  private final ReentrantLock lock = new ReentrantLock();
  // Allows for linking and separation of OGTraceStore and OGTraceReader
  StoreTraceObserver observer;

  // ByteBuffers that appears as empty buffer with the current versionMarker already set
  // a. owned by threads that terminated
  // b. stolen by a refresh command in the middle of the run
  //
  // Future ideas:
  // OGScheduler.exitGraph could add the EvaluationContext's tables here to allow another thread to
  // top off the block
  // Off-graph manipulations such as Profiler UI setting a new cache could write to one of the
  // freeEvents tables
  // Restore back the use of BlockingQueue? and order by the largest remaining space?
  private ArrayDeque<Table> freeTables;

  public final PNodeTaskInfo[] getCollectedPNTIs() {
    return pntis;
  }

  /** Useful to just show the name and basic description information */
  public final PNodeTaskInfo getCollectedPNTI(int profileID) {
    PNodeTaskInfo[] pntis = this.pntis;
    return profileID >= pntis.length ? null : pntis[profileID];
  }

  /**
   * After table has been processed (by reader) return to free pool. It's under lock just like
   * get/allocate table calls. So setting threadID = 0 effective marks it as in not in use
   */
  final void returnTable(Table table) {
    if (table == null) return;
    lock.lock();
    try {
      addToFreeList(table);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Must be called under [SEE_STORE_LOCK] Note: ALL version markers, per table or global are set
   * under this lock
   */
  private void addToFreeList(Table table) {
    table.setExternallyOwned(false);
    table.setVersionTypeAndThread(VERSION_INVALID, TABLE_FREE, 0L);
    freeTables.add(table);
  }

  /**
   * After thread goes away we collect its tables or when a thread gives up its table If Reset was
   * called and this table is 'old' (version doesn't match), we must add it to the free list and
   * can't just drop it, because allocator could have given away space past this table's location
   * and that would result in a memory leak
   */
  final void postProcessTable(Table table) {
    if (table == null) return;
    boolean callObserver = false;
    lock.lock();
    try {
      if (table.getVersionMarker() != versionMarker) addToFreeList(table);
      // To avoid lock nesting we call outside of the lock!
      else if (observer != null) callObserver = true;
    } finally {
      lock.unlock();
    }
    if (callObserver) observer.postProcess(table);
  }

  OGTraceStore(String filePrefix, boolean isLive) {
    this.filePrefix = filePrefix;
    this.isLive = isLive;
    initialize();
  }

  /** reader could have tables in tableQ / processTable() when reset is called on the writer */
  void reset() {
    lock.lock();
    try {
      initialize();
    } finally {
      lock.unlock();
    }
  }

  /** In ctor or under lock! */
  private void initialize() {
    totalMappedSize = 0;
    //noinspection NonAtomicOperationOnVolatileField (already under lock)
    versionMarker++;
    freeTables = new ArrayDeque<>();
    try {
      allocateBackingStore(0);
    } catch (Exception e) {
      OGTrace.panic(e, fileLocation);
      return;
    }
    String[] names = OGTrace.gen.getNames();
    String[] descriptors = OGTrace.gen.getDescriptors();
    int signatureSize =
        8 /* 2 x size of arrays */
            + getSizeofStringArray(names)
            + getSizeofStringArray(descriptors);
    int headerTableSize = OGTrace.FILE_HEADER_SIZE + signatureSize;

    Table headerTable;
    try {
      headerTable = allocateTable(null, TABLE_HEADER, headerTableSize);
    } catch (Exception e) {
      OGTrace.panic(e, fileLocation);
      return;
    }

    headerTable.setExternallyOwned(false); // Always under lock [SEE_STORE_LOCK]
    headerTable.putInt(OGTrace.FORMAT);
    long baseTimeMillis = System.currentTimeMillis();
    long baseTimeNanos = System.nanoTime();
    headerTable.putLong(baseTimeMillis);
    headerTable.putLong(baseTimeNanos);
    headerTable.putInt(names.length);
    for (String s : names) headerTable.putString(s);
    headerTable.putInt(descriptors.length);
    for (String s : descriptors) headerTable.putString(s);

    pntis = new PNodeTaskInfo[0];
    stringIDs = new ConcurrentHashMap<>();
    threadIDs = new ConcurrentHashMap<>();
  }

  /*the size of the string array in form of strings' bytes + size of the bytes of each string*/
  private int getSizeofStringArray(String[] strs) {
    int sum = 0;
    for (String str : strs) {
      sum += str.getBytes().length;
      sum += 4;
    }
    return sum;
  }

  public final long fileSize() throws IOException {
    return fileChannel == null ? 0L : fileChannel.size();
  }

  public final File backingStoreLocation() {
    return fileLocation;
  }

  /**
   * Called at shutdown time, to copy the current ogtrace to a permanent location Note: it clobbers
   * fileChannel, this OGTraceStore is no longer usable
   */
  void copyTraceTo(String name) throws IOException {
    try (var fos = new FileOutputStream(name);
        var ch = fos.getChannel()) {
      // not Files.copy because we pre-delete the temp file on Linux to allow directory wiping
      fileChannel.position(0); // rewind needed on Windows, not on Linux
      ch.transferFrom(fileChannel, 0, fileChannel.size());
    }
  }

  private static void uninterruptibilize(FileChannel channel) {
    try {
      var uninterruptibilizer =
          Class.forName("sun.nio.ch.FileChannelImpl").getMethod("setUninterruptible");
      uninterruptibilizer.invoke(channel);
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException ex) {
      log.javaLogger()
          .warn(
              "Failed to make OGTraceStore channel resistant to interruptions. We'll probably be fine.");
    }
  }
  /**
   * Produce a minimal ogtrace holding just the end-of-run Properties table, and nothing else, on a
   * freshly-constructed OGTraceStore. WARNING: if DiagnosticSettings.alwaysAppendTraceToOverride is
   * set then this will append to the existing trace, since OGTraceStore.allocateBackingStore will
   * get the same fileChannel [SEE_TRACE_TO_OVERRIDE]
   */
  static void writeAllTaskInfos(String name, ArrayList<PNodeTaskInfo> pntis) throws IOException {
    var store = new OGTraceStore("optimus", false);
    var writer = OGTrace.writer_proto.createCopy(store);

    if (!pntis.isEmpty()) {
      int maxId = pntis.stream().map(x -> x.id).max(Integer::compare).get();
      for (PNodeTaskInfo pnti : pntis) {
        if (pnti != null && OGTraceReader.includeProperty(pnti, true, false)) {
          if (pnti.id == 0) {
            pnti = pnti.dup(); // we don't want to modify the one that was passed in!
            pnti.id = ++maxId;
          }
          store.writeProfileDesc(pnti, writer);
          store.writeProfileData(pnti, writer);
        }
      }
    }

    try (var fos = new FileOutputStream(name);
        var ch = fos.getChannel()) {
      store.fileChannel.position(0);
      var lastTable = writer.getTable(); // pntis just wrote
      var lastTableUnused = lastTable != null ? lastTable.size - lastTable.position : 0;
      // since the loop above did not reuse any tables (observer == null, there is no reader
      // attached)
      // we can calculate exact number of bytes in use:
      long sz =
          store.totalMappedSize // how many bytes in the file were mapped to buffers
              - store.bstrBuf.limit()
              + store.bstrBuf.position() // minus the unused end of the last buffer
              - lastTableUnused;
      // in the usual case, pnti table is smaller than the initial buffer size, this simply ==
      // lastTable.position
      ch.transferFrom(store.fileChannel, 0, sz);
    }
  }

  private File setupDir() {
    File dir = new File(DiagnosticSettings.fullTraceDir);
    if (dir.exists() && !dir.isDirectory()) {
      throw new RuntimeException(
          "The full trace directory " + dir.getAbsolutePath() + " is not a directory");
    }
    //noinspection ResultOfMethodCallIgnored
    dir.mkdirs();
    // check for stale files in it
    File[] oldFiles = dir.listFiles();
    if (oldFiles == null) {
      throw new RuntimeException(
          "Cannot list files in the full trace directory " + dir.getAbsolutePath());
    }
    long len = 0;
    if (!DiagnosticSettings.keepStaleTraces) {
      for (File f : oldFiles) {
        if (f.isFile()) {
          len += f.length();
          //noinspection ResultOfMethodCallIgnored OK we tried....
          f.delete();
        }
      }
    }
    if (len != 0)
      log.javaLogger()
          .info(String.format("cleaned %.2f MB from %s", len / 1e6, dir.getAbsolutePath()));
    return dir;
  }

  private String customTraceToFileName(String filePrefix) {
    String pathPrefix = filePrefix.endsWith("/") ? filePrefix : filePrefix + "/";
    return pathPrefix + PThreadContext.Process.current() + ".ogtrace";
  }

  private void allocateBackingStore(int remaining) throws Exception {
    if (fileChannel == null) {
      // First time allocation [SEE_TRACE_TO_OVERRIDE]
      var customTraceToPrefix = DiagnosticSettings.alwaysAppendTraceToOverride;
      if (customTraceToPrefix != null) {
        log.javaLogger()
            .warn(
                "-Doptimus.traceTo is set.\n"
                    + "This can cause issues because we can't guarantee that the file will be empty or not reused by other processes.\n"
                    + "Unless you are debugging OGTrace itself, you probably should be using -Doptimus.scheduler.profile.folder= instead.");
      }
      File file =
          customTraceToPrefix != null
              ? new File(customTraceToFileName(customTraceToPrefix))
              : File.createTempFile(
                  filePrefix, String.format(".%d.ogtrace", MSProcess.getPID()), setupDir());
      log.javaLogger().info("trace file: " + file.getAbsolutePath());
      // not file itself to allow pre-deleting noinspection ResultOfMethodCallIgnored
      fileLocation = file.getParentFile();
      // create parent directories if they don't exist (usually if traceTo property was set)
      fileLocation.mkdirs();
      backingFile = new RandomAccessFile(file, "rw");
      fileChannel = backingFile.getChannel();
      uninterruptibilize(fileChannel);
      var doNotDelete = DiagnosticSettings.keepFullTraceFile || customTraceToPrefix != null;
      if (!doNotDelete) { // traceTo should never delete
        file.deleteOnExit();
        // on Linux, pre-deleting makes it OK to wipe temp dir on startup, and actually guarantees
        // deleteOnExit
        // on Windows this silently does nothing
        //noinspection ResultOfMethodCallIgnored
        file.delete();
      }
    }
    totalMappedSize -= remaining;
    bstrBuf = fileChannel.map(MapMode.READ_WRITE, totalMappedSize, fileMapSize);
    // a) Performance is faster and b) we only have x86 for now
    bstrBuf.order(ByteOrder.nativeOrder());
    totalMappedSize += fileMapSize;
    long space = fileLocation.getUsableSpace();
    double spaceMB = space / 1024.0 / 1024.0;
    // is there room for max unrecycled tables plus one file mapping?
    if (space < (long) maxUnrecycledTables * perThreadSize + fileMapSize) {
      if (space < fileMapSize) {
        // there is not even room for one file mapping: we can't use OGTrace profiler
        // here at this point, we don't know if profiling is going to be enabled, so
        // we don't try to abort
        bstrBuf = null;
        log.javaLogger()
            .warn(
                "insufficient space ("
                    + spaceMB
                    + " MB) for any profiler event tables in "
                    + fileLocation.getAbsolutePath()
                    + " ");
      } else {
        // this may increase the overhead of hotspot profiler (will trigger syncCatchUp more often)
        maxUnrecycledTables = (int) ((space - fileMapSize) / perThreadSize);
        log.javaLogger()
            .info(
                "insufficient space ("
                    + spaceMB
                    + " MB) for max profiler event tables in "
                    + fileLocation.getAbsolutePath()
                    + " will catch "
                    + "up when "
                    + maxUnrecycledTables
                    + " tables are not recycled");
      }
    }
  }

  OGTraceReader storeForReading() {
    try {
      return new OGTraceFileReader(backingFile, true, false);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null; // Should we return some empty reader?
  }

  /** Give away part of large bstrBuffer */
  private Table allocateFixedSizeTable(Table returnTable, short type) throws Exception {
    return allocateTable(returnTable, type, perThreadSize);
  }

  /**
   * Give away part of large bstrBuffer, It's called EXACTLY from 2 places (initialize and the safe
   * wrapper above!) If you change this, it will break in some very magical ways! (like giving away
   * memory that is still in use)
   */
  private Table allocateTable(Table returnTable, short type, int size) throws Exception {
    if (bstrBuf == null)
      throw new RuntimeException(
          "Backing store was not allocated, possibly insufficient space in " + fileLocation);

    postProcessTable(returnTable); // Has its own locking logic

    Table table;
    lock.lock();
    try {
      if (!freeTables.isEmpty() && freeTables.peekFirst().remaining(size)) {
        // We should probably look at free tables beyond the first one to find one that fits
        // Add to tracing bp "Recycling table: " + table.toString()
        table = freeTables.removeFirst();
        table.setVersionTypeAndThread(versionMarker, type);
      } else {
        while (true) {
          if (bstrBuf.remaining() < size) allocateBackingStore(bstrBuf.remaining());

          int tableStart = bstrBuf.position();
          bstrBuf.position(tableStart + size);
          if (Table.isNotExternallyOwned(bstrBuf, tableStart)) {
            table = new Table(bstrBuf, versionMarker, type, tableStart, size);
            break; // Add to tracing bp "Allocating table: " + table.toString()
          }
          // Add to tracing bp "Skipping table that's in use..."
        }
      }
      table.observedLastCmdPosition = 0;
      if (type == TABLE_EVENTS) {
        threadIDs.computeIfAbsent(
            table.getThreadID(),
            id -> {
              table.needsThreadName = true;
              return id;
            });
      }
      table.setExternallyOwned(true);
    } finally {
      lock.unlock();
    }
    return table;
  }

  private Table integrityCheck(Table table) {
    if (!Settings.schedulerAsserts) return table;

    boolean shouldFail = false;

    if (table.integrityCheck()) {
      String errorToReport =
          "OGTraceStore Integrity Check failed: We can't give away table that is off its deep end";
      log.javaLogger().error(errorToReport);
      MonitoringBreadcrumbs$.MODULE$.sendGraphFatalErrorCrumb(errorToReport);
      shouldFail = true;
    }

    if (Table.isNotExternallyOwned(table.buf, table.start)) {
      String errorToReport =
          "OGTraceStore Integrity Check failed: We can't give away table that is in use";
      log.javaLogger().error(errorToReport);
      MonitoringBreadcrumbs$.MODULE$.sendGraphFatalErrorCrumb(errorToReport);
      shouldFail = true;
    }

    if (shouldFail) {
      GridProfiler$.MODULE$.disable();
      log.javaLogger().error(String.format("OGTraceStore failure : %s", table.toString()));
      Thread.dumpStack();

      Breadcrumbs.flush();
      System.exit(7);
      table = null;
    }
    return table;
  }

  /** Verify that the table can be used or allocate a new one */
  public final Table getTableBuffer(Table tryTable, short type, int minSize) throws Exception {
    if (tryTable != null && tryTable.remaining(minSize) && tryTable.version == versionMarker)
      return integrityCheck(tryTable);

    return integrityCheck(allocateFixedSizeTable(tryTable, type));
  }

  public void writeEdges(OGLocalTables ctx, int parentID, EdgeIDList ids) {
    try {
      ctx.edgeTrace = getTableBuffer(ctx.edgeTrace, TABLE_EDGES, ids.serializedSize());
    } catch (Exception e) {
      OGTrace.panic(e, fileLocation);
      return;
    }
    Table table = ctx.edgeTrace;
    ids.serializeTo(table, parentID);
    table.setLastCmdPosition();
  }

  /**
   * If we can see any value there, some thread have stored or is storing property descriptor (see
   * writeProfile)
   */
  private boolean profileNotRecorded(int id) {
    PNodeTaskInfo[] arr = this.pntis;
    return arr.length <= id || arr[id] == null;
  }

  /**
   * Use ensureProfileRecorded when a node might not be executed at all but its profile needs to be
   * written. Consider making this eager rather than lazy (ie do it upfront) If we can see any value
   * there, some thread have stored or is storing property descriptor (see writeProfile)
   */
  public final void ensureProfileRecorded(int pid, NodeTask task) {
    if (profileNotRecorded(pid))
      registerProfileDesc(new PNodeTaskInfo(pid, task)); // Consider delay writing....
  }

  public final void ensureProfileRecorded(OGLocalTables lt, int pid, NodeTask task) {
    if (profileNotRecorded(pid))
      registerProfileDesc(lt, new PNodeTaskInfo(pid, task)); // Consider delay writing....
  }

  /**
   * If we can see any value there, some thread has stored or is storing property descriptor (see
   * writeProfile)
   */
  public final void ensureProfileRecorded(int pid, NodeTaskInfo nti) {
    // Consider delay writing....
    if (profileNotRecorded(pid)) registerProfileDesc(new PNodeTaskInfo(pid, nti));
  }

  final void ensureProfileRecorded(int pid, String pkgName, String name) {
    // Consider delay writing....
    if (profileNotRecorded(pid)) registerProfileDesc(new PNodeTaskInfo(pid, pkgName, name));
  }

  // come back to string interning
  final int ensureStringRecorded(String text, ProfilerEventsWriter writer) {
    return stringIDs.computeIfAbsent(
        text,
        newText -> {
          int id = stringIDs.size() + 1;
          writer.stringID(id, newText);
          return id;
        });
  }

  // This checks if there is a live PNTI with the same profile ID and restores the connection with
  // the in-memory NTI
  //
  // Needed for Profiler UI and Cache Config writer  in order to handle changes to cache names and
  // other live updates
  // made through Profiler UI (see all addMenuFE_NTI calls in UNPTableTraceTags.scala)
  //
  // It could be possible to avoid this by recording an OGTrace event for every addMenuFE_NTI and
  // immediately
  // re-reading OGTrace and re-applying that event to emulate a "live" update to PTNIs, but that
  // means the
  // live profiler's changes would have no effect on the actual, live nodes, in the same run.
  public final void connectToLiveProfile(PNodeTaskInfo pnti) {
    PNodeTaskInfo[] arr = this.pntis;
    if (arr.length > pnti.id && arr[pnti.id] != null) {
      NodeTaskInfo nti = arr[pnti.id].nti;
      if (nti != null) { // there is a live NTI to connect to
        pnti.nti = nti;
        // re-populates flags, cache name, etc, from the live NTI into this PNTI
        pnti.freezeLiveInfo();
      }
    }
  }

  private void registerProfileDesc(PNodeTaskInfo pnti) {
    var lt = OGLocalTables.getOrAcquire();
    registerProfileDesc(lt, pnti);
    lt.release();
  }

  private void registerProfileDesc(OGLocalTables lt, PNodeTaskInfo pnti) {
    if (isLive) return; // Nothing gets stored in buffers

    int id = pnti.id;
    try {
      lock.lock();
      PNodeTaskInfo[] arr = this.pntis;
      if (arr.length <= id) {
        arr = this.pntis = Arrays.copyOf(arr, id * 2);
      }
      if (arr[id] != null) {
        return;
      } // Some other thread won the race....
      arr[id] = pnti;
    } finally {
      lock.unlock();
    }
    writeProfileDesc(pnti, lt.eventsTrace);
  }

  /** Write static info */
  private void writeProfileDesc(PNodeTaskInfo pnti, ProfilerEventsWriter writer) {
    pnti.freezeLiveInfo();
    writer.profileDesc(
        pnti.id,
        pnti.flags,
        pnti.name,
        pnti.pkgName,
        pnti.modifier,
        pnti.cacheName,
        pnti.cachePolicy);
  }

  /** Full summaries of table */
  private void writeProfileData(PNodeTaskInfo pnti, ProfilerEventsWriter writer) {
    writer.profileData(
        pnti.id,
        pnti.start,
        pnti.cacheHit,
        pnti.cacheMiss,
        pnti.cacheHitTrivial,
        pnti.evicted,
        pnti.invalidated,
        pnti.reuseCycle,
        pnti.reuseStats,
        pnti.selfTime,
        pnti.ancAndSelfTime,
        pnti.postCompleteAndSuspendTime,
        pnti.tweakLookupTime,
        pnti.wallTime,
        pnti.cacheTime,
        pnti.nodeUsedTime,
        pnti.tweakID,
        pnti.tweakDependencies != TPDMask.empty ? pnti.tweakDependencies.toString() : null);
  }

  public void writeProfileData(ArrayList<PNodeTaskInfo> pntis, ProfilerEventsWriter writer) {
    for (var pnti : pntis) {
      writeProfileData(pnti, writer);
    }
  }

  public void memory(Map<Class<?>, NCSupport.JVMClassInfo> clsInfos) {
    try {
      lock.lock();

      for (PNodeTaskInfo pnti : pntis) {
        if (pnti != null) {
          pnti.jvmInfo = clsInfos.get(pnti.nodeCls); // orNull
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /** Wrapper around ByteBuffer, note volatile bstrBuf which can be removed from under thread */
  public static class Table {
    // HEADER_SIZE = sizeOf(size) (4) + sizeOf(version) (2) + sizeOf(tableType) (2) +
    // sizeOf(threadID) (8)
    // + sizeOf(ext_use)  (4) + (lastCmdPosition) (4)
    static final int HEADER_SIZE = 24;
    static final int VERSION_OFFSET = 4; // since sizeOf(size) = 4
    static final int TYPE_OFFSET = 6; // VERSION_OFFSET + sizeOf(version) = 4 + 2 = 6
    static final int THREADID_OFFSET = 8; // TYPE_OFFSET + sizeOf(type) = 6 + 2 = 8
    // Owned by a thread that won't lock around usage [SEE_STORE_LOCK]
    static final int EXT_USE_OFFSET = 16;
    // Position of the last command written (payload bytes written)
    static final int LAST_CMD_POSITION_OFFSET = 20;

    ByteBuffer buf;
    short version; // Version marker at the time of giving away table to a thread
    int position; // Current read/write pos as offset into buf
    int size; // Size of the table
    int start; // Start of this table within buf
    int limit; // Limit offset for reading/writing within buf

    static final byte[] emptyBuffer = new byte[0];

    // The next field is used only for reading partial tables on live recording! see
    // copyFoReadingAndObservePosition
    int observedLastCmdPosition; // Only reader/processing thread updates and reads this value!
    // [SEE_OBSERVED_VALUE]
    public boolean needsThreadName;

    /** For reading only!!!!!!!!!!!!!! */
    Table(ByteBuffer buf, int start, int firstCmdOffset, int lastCmdOffset) {
      this.buf = buf;
      this.start = start;
      this.size = getSize();
      if (lastCmdOffset == -1) lastCmdOffset = getLastCmdPosition();
      this.position = start + HEADER_SIZE + firstCmdOffset;
      this.limit = start + HEADER_SIZE + lastCmdOffset;
    }

    final void setSize(int size) {
      this.size = size;
      this.limit = start + size;
    }

    Table(ByteBuffer buf, short versionMarker, short tableType, int start, int size) {
      this.buf = buf;
      this.start = start;
      this.size = size;
      this.limit = start + size;
      setVersionTypeAndThread(versionMarker, tableType);
    }

    final void setVersionTypeAndThread(short versionMarker, short type) {
      setVersionTypeAndThread(versionMarker, type, Thread.currentThread().getId());
    }

    final void setVersionTypeAndThread(short versionMarker, short type, long threadID) {
      version = versionMarker;
      position = start + HEADER_SIZE;
      buf.putInt(start, size); // Allows for a walk/jump to the next table
      buf.putShort(start + VERSION_OFFSET, versionMarker);
      buf.putShort(start + TYPE_OFFSET, type);
      buf.putLong(start + THREADID_OFFSET, threadID);
      setLastCmdPosition();
    }

    /** In the case of reading live data (potentially as it's being collected) */
    final Table copyForReadingAndObservePosition() {
      int fromPosition = observedLastCmdPosition;
      observedLastCmdPosition = getLastCmdPosition();
      return new Table(buf, start, fromPosition, observedLastCmdPosition);
    }

    final int getSize() {
      return buf.getInt(start);
    }

    final short getVersionMarker() {
      return buf.getShort(start + VERSION_OFFSET);
    }

    final short getType() {
      return buf.getShort(start + TYPE_OFFSET);
    }

    final long getThreadID() {
      return buf.getLong(start + THREADID_OFFSET);
    }

    final int getLastCmdPosition() {
      return buf.getInt(start + LAST_CMD_POSITION_OFFSET);
    }

    public final void setLastCmdPosition() {
      // Technically this should Unsafe.putOrdered (we don't need full barrier)
      buf.putInt(start + LAST_CMD_POSITION_OFFSET, position - start - HEADER_SIZE);
    }

    static boolean isNotExternallyOwned(ByteBuffer buf, int start) {
      return buf.getInt(start + EXT_USE_OFFSET) == 0;
    }

    final void setExternallyOwned(boolean isOwned) {
      buf.putInt(start + EXT_USE_OFFSET, isOwned ? 1 : 0);
    }

    // we keep track of last cmd position, so this could be 1 (or even 0) - we just need to check we
    // read all commands
    final boolean hasMoreCommands() {
      return remaining(4);
    }

    final boolean remaining(int minBufferSize) {
      return position + minBufferSize < limit;
    }

    public final void putShort(short v1) {
      buf.putShort(position, v1);
      position += 2;
    }

    public final void putInt(int v1) {
      buf.putInt(position, v1);
      position += 4;
    }

    private void putString(String str) {
      putInt(str.getBytes().length);
      put(str.getBytes());
    }

    public final int getInt() {
      int v = buf.getInt(position);
      position += 4;
      return v;
    }

    public final void putLong(long v1) {
      buf.putLong(position, v1);
      position += 8;
    }

    public final long getLong() {
      long v = buf.getLong(position);
      position += 8;
      return v;
    }

    public final void put(byte[] name) {
      // ByteBuffer doesn't have an overload (index, bytes[])
      for (byte b : name) buf.put(position++, b);
    }

    public final String getString() {
      var bytes = getBytes();
      if (bytes == null) return null;
      if (bytes == emptyBuffer) return "";
      return new String(bytes, OGTraceStore.charSet);
    }

    public final byte[] getBytes() {
      int size = getInt();
      if (size == -1) return null;
      if (size == 0) return emptyBuffer;

      byte[] bytes = new byte[size];
      for (int i = 0; i < bytes.length; i++)
        bytes[i] = buf.get(position++); // ByteBuffer doesn't have an overload (index, bytes[])
      return bytes;
    }

    @Override
    public String toString() {
      String type = getType() < tableNames.length ? tableNames[getType()] : "?";
      String ownedExt = (isNotExternallyOwned(buf, start) ? "-" : "X");
      long address = JavaVersionCompat.directBuffer_address(buf);

      return " Type: "
          + type
          + " Ver: "
          + getVersionMarker()
          + " Valid: "
          + integrityCheck()
          + " Ext: "
          + ownedExt
          + " ThreadID: "
          + getThreadID()
          + " Size: "
          + getSize()
          + " Used: "
          + (position - start - HEADER_SIZE)
          + " Position: "
          + position
          + " Last Cmd: "
          + (getLastCmdPosition())
          + " Address: "
          + Long.toHexString(address)
          + " Full Address: "
          + Long.toHexString(address + start)
          + " Hash: "
          + Integer.toHexString(System.identityHashCode(this));
    }

    public boolean integrityCheck() {
      return (position - start - HEADER_SIZE != getLastCmdPosition());
    }
  }
}
