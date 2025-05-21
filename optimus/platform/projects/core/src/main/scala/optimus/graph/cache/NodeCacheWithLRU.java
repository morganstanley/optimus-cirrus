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
package optimus.graph.cache;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import optimus.graph.DiagnosticSettings;
import optimus.graph.GraphInInvalidState;
import optimus.graph.OGTrace;
import optimus.graph.OGTraceIsolation;
import optimus.graph.PropertyNode;
import optimus.graph.diagnostics.EvictionReason;
import optimus.graph.diagnostics.messages.CacheClearCounter$;
import optimus.graph.diagnostics.messages.CacheClearEvent;
import optimus.graph.diagnostics.messages.CacheEntriesCounter$;
import optimus.graph.diagnostics.messages.CacheEntriesEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * One cache to rule them all? ( child classes as just configuration of the class)
 * <p>
 * Goals/reasons:
 * We want just minimum set of operations to support our cache
 * For example by not supporting remove(key) we can get away with CAS all the time (Unlike e.g. CHMv8)
 * <p>
 * Highly integrated: graph needs a set of nodes we don't have to / want to pay for
 * extra bucket info
 * <p>
 * Notes: We caching only PropertyNode(s) and have close connection with them.
 * For example we rely on the fact that they contain hashCode and we don't have
 * to store it again
 * <p>
 * Implementation Note
 * This cache operates as in an optimised manner roughly as follows
 * cacheConcurrency - indicates the number of ledgers that are maintained
 * cacheBatchSize - is the size of each ledger
 * cacheBatchSizePadding - is the padding on the ledger entries
 * <p>
 * if profiling is enabled the concurrency is disabled as you cannot calculate the reuse size accurately
 * otherwise
 * The ledger serves as a staging point. The cacheConcurrency in the number of ledgers
 * and they are assigned to different threads. several threads may share the ledgers, but ideally this would be minimised
 * <p>
 * When an entry is retrieved from the cache we want to update the LRU, and the ledger manage that.
 * We are prepared that the LRU may not be exactly accurate, but the max inaccuracy is the multiple of the
 * cacheConcurrency * (batchSize + batchSizePadding)
 * <p>
 * If any ledger already contains this update then its a no-op
 * otherwise the cache will attempt to add to the ledger (assigned to this thread)
 * <p>
 * If the number of entries exceed the cacheBatchSize then this thread will try to lock all ledgers and update the LRU
 * if this lock is unsuccessful then it will just add to the ledger ( using this additional space set by cacheBatchSize)
 * If there is no space available the lock will be force locked (waiting for other threads)
 * <p>
 * if/when the lock is assigned, the victim thread will update the LRU for all ledgers.
 * <p>
 * By the process above the contention is reduced on the cache and the LRU
 * <p>
 *
 * Notes on equivalent cache size algorithm:
 * We want to compute the smallest new size P <= this.size, such that the number of cache hits wouldn't change
 * under the same order of cache requests. If set on a next run this will result in larger number of evictions, which
 * is a good thing!
 * <p>
 * Consider a set of node requests: A B C D
 * Logically we have an LRU:
 * [header] - D - C - B - A -[header]    // D being the latest and A is the closest to being evicted
 * We keep a global counter insertCount which we increment on every insertion (aka cache miss).
 * We don't update it on re-use (aka cache hit)
 * On cache hit we assign the current value of insertCount to the node 'after' (closer to being evicted)
 * On eviction we inspect the insertMarker of every node and compute the min(insertCount - insertMarker)
 * At any point we can compute P = size - min(insertCount - insertMarker)
 * When external request to return P comes in we need to account for the nodes still not reaching the eviction.
 * In this case we 'simulate' eviction by walking from the header back until we find a node with a marker and asumming
 * size of the cache is shorter by that distance.
 * <p>
 * Assuming node B is cache hit and we mark A, consider the following exhaustive list of possibilities:
 * 1. No node after A was hit ... Eventually A will be evicted after (insertCount - insertMarker)
 * insertions, that means that the cache had to be at least P = size - (insertCount - insertMarker)
 * 2. A was hit... In this case the node after A will get a marker (larger or equal to the one A has) that means
 * the new P = size - (insertCount - insertMarker) will be larger as the node after A is closer to getting evicted
 * 3. Nodes before A were hit. (This is really the same as B being hit)
 * <p>
 * Example:
 * No hits P = size - min(size+ - 0) = 0 (insertCount will reach size on the first eviction and will grow afterwards)
 */
abstract class NodeCacheWithLRU extends NodeCacheBase {
  // [profileSizeBruteForce]
  // Uncomment all the places with this marker to enable brute force approach if suspecting normal
  // one is lying
  // Tests are pretty extensive and this is left here on 2/26/2016 as a precaution
  // private int maxCacheSizeUsed;                  // profileCache
  private int insertCount; // profileCache (number of new inserts to cache)
  private long insertCountLong; // for people who don't want to deal with overflow
  // profileCache (value we can NOT actually observe)
  private int minTailObserved = Integer.MAX_VALUE;
  private String name; // Displayed in the profiler
  private int maxSize;

  // Some operations have a global lock. Currently [updateLock]
  private final ReentrantLock updateLock = new ReentrantLock();
  // The update ledger is used during catchup as a temporary holding space for nodes so that
  // we can release the working ledgers quicker.
  private final Ledger updateLedger;
  // Each ledger has its own locking
  private final Ledger[] ledgers; // Records hits and misses (insertions)
  private final int cacheBatchSize; // Start processing records when this
  private static final int minimumCacheSize = 16;
  private final int cacheConcurrency;
  private final int cacheBatchSizePadding;
  private NCEntryLRU header; // maintain double link list of approximate LRU [updateLock]
  private int size; // current size of valid entries in LRU [updateLock] After catchUp = cache size
  private final boolean evictOnOverflow;
  private final boolean evictOnLowMemory;
  private final CacheCounters cacheCounters = new CacheCounters();
  protected static final Logger log = LoggerFactory.getLogger(NodeCacheWithLRU.class);

  NodeCacheWithLRU(
      String name,
      int maxSize,
      boolean evictOnOverflow,
      boolean evictOnLowMemory,
      int requestedConcurrency,
      int cacheBatchSize,
      int cacheBatchSizePadding) {
    this.name = name;
    this.maxSize = maxSize;
    this.cacheBatchSize = cacheBatchSize;
    this.cacheConcurrency = requestedConcurrency;
    this.cacheBatchSizePadding = cacheBatchSizePadding;

    requestedConcurrency = NCSupport.roundUpToPowerOf2(1, requestedConcurrency);
    if (requestedConcurrency <= 1) {
      ledgers = null;
      updateLedger = null;
    } else {
      ledgers = new Ledger[requestedConcurrency];
      updateLedger = new Ledger(cacheBatchSize + Math.max(0, cacheBatchSizePadding));
    }

    this.evictOnOverflow = evictOnOverflow;
    this.evictOnLowMemory = evictOnLowMemory;
    reset();
  }

  public CacheCountersSnapshot getCountersSnapshot() {
    // blakedav - safest to read insertCount under updateLock
    return this.cacheCounters.snap(getSizeIndicative(true), insertCountLong, getMaxSize());
  }

  public boolean isEvictOnOverflow() {
    return evictOnOverflow;
  }

  public boolean isEvictOnLowMemory() {
    return evictOnLowMemory;
  }

  public final int cacheBatchSize() {
    return cacheBatchSize;
  }

  public final int cacheBatchSizePadding() {
    return cacheBatchSizePadding;
  }

  private boolean isProfilingContention() {
    return DiagnosticSettings.profileCacheContention;
  }

  /** Touch entry to update it in LRU */
  @Override
  final void touchEntry(NCEntry entry) {
    touchEntry(getLedger(), entry);
  }

  @Override
  protected final void newEntryAdded(Ledger l, NCEntry entry) {
    // touch entry to update it in LRU
    touchEntry(l, entry);
  }

  @Override
  protected final void foundTombstone() {
    // LRU cache doesn't support parallel resizing
  }

  @Override
  final void touchEntry(Ledger l, NCEntry entryRaw) {
    var entry = (NCEntryLRU) entryRaw;
    if (l == null) {
      // Given no specific ledger we would need to catch up
      updateLock.lock();
      try {
        catchupEntry(entry);
        removeOverflow();
      } finally {
        updateLock.unlock();
      }
    } else {
      // There is race here and entry could end up on multiple ledgers anyways
      // That's fine, other parts are dealing with this. This is just an optimization
      if (entry.inLedger) return;

      boolean needToCatchup = false;
      boolean hasLock = false;
      // All ops are per ledger that is passed in
      //noinspection SynchronizationOnLocalVariableOrMethodParameter
      synchronized (l) {
        entry.inLedger = true;
        if (l.count == l.records.length) needToCatchup = true;
        else if (l.count >= cacheBatchSize && updateLock.tryLock()) needToCatchup = hasLock = true;
        else l.records[l.count++] = entry;
      }
      if (needToCatchup) {
        if (!hasLock) {
          if (isProfilingContention()) {
            cacheCounters.incrementUpdateLockAttempt();
            long startTime = OGTrace.nanoTime();
            updateLock.lock(); // [LOCK_1]
            cacheCounters.addLockContentionTime(OGTrace.nanoTime() - startTime);
          } else {
            updateLock.lock(); // [LOCK_1]
          }
        }
        try {
          // Check that we still need to catchup
          // We could have been blocked on [updateLock] waiting for another thread to catchup
          // It's ok to read outside of a lock, it's only an optimization
          if (l.count >= cacheBatchSize) catchupCore();

          // We are already holding the lock, might as well update our entry
          catchupEntry(entry);
          removeOverflow();
        } finally {
          updateLock.unlock();
        }
      }
    }
  }

  private void recordCacheClear(String clearCacheDescription, long numRemoved) {
    cacheCounters.nodesRemoved(numRemoved);
    if (OGTraceIsolation.isTracing()) {
      var numEntries = Caches.approxNumCacheEntries();
      CacheEntriesCounter$.MODULE$.publish(new CacheEntriesEvent(numEntries));
      var event =
          new CacheClearEvent(clearCacheDescription, name, numRemoved, getSize(), numEntries);
      CacheClearCounter$.MODULE$.publish(event);
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " name:" + getName();
  }

  /** Returns Ledger that tries to be thread specific to avoid contention */
  @Override
  protected Ledger getLedger() {
    return ledgers == null
        ? null
        : ledgers[(int) Thread.currentThread().threadId() & (ledgers.length - 1)];
  }

  private void remove(NCEntryLRU old) {
    // 1. It's possible to record an invalidated node in a ledger multiple times
    // 2. It's possible to remove old item, while another thread finds it and records in a ledger
    if (old.removed()) return;

    OGTrace.observer.evicted(old.getValue());

    NCEntry[] t = table;
    int i = offset(t, old.hash);

    while (true) {
      var e = (NCEntry) vh_table.getVolatile(t, i);

      if (e == null) {
        // TODO (OPTIMUS-57931): Because of the cache resize bug described in setMaxSize, it is
        // possible that we
        //  have nodes in the LRU that are not present in the hash table. Here we delete those ones
        // from the
        //  LRU (by calling .remove()) to avoid a possible infinite loop in removeOverflow.
        size -= old.remove();
        return;
      }

      if (e == old) {
        if (vh_table.compareAndSet(t, i, e, old.next)) {
          size -= old.remove(); // remove from LRU list
          break;
        }
      } else if (e.invalid()) { // continue to retry at head because we failed cas
        // Remove the dead entry at head
        if (vh_table.compareAndSet(t, i, e, e.next)) {
          // An entry could become invalid before it's added to the double linked list
          size -= e.remove();
        }
        // continue to retry at head.... regardless if we succeeded in CAS
        // next entry find the element that points to the 'old' and re-parent its next element
      } else {
        NCEntry ne;
        while ((ne = e.next) != old) {

          if (ne == null) {
            // TODO (OPTIMUS-57931): See comment above. In this case
            //  here we walked the entire bin and didn't find
            //  "old" so we get rid of it now and call it quit
            size -= old.remove();
            return;
          }

          if (ne.invalid()) { // Remove deleted entries
            e.next = ne.next;
            // An entry could become invalid before it's added to the double linked list
            size -= ne.remove();
            continue;
          }
          e = e.next;
        }
        // There will be a memory barrier soon after as we exit
        // lock and it's ok to read old value for now
        e.next = old.next;
        size -= old.remove(); // This should always return size of entry
        break;
      }
    }
  }

  /** Called in ctor only */
  private void reset() {
    size = 0;

    // Profiling can be turned on/off on the fly otherwise would use NCEntryV()
    header = new NCEntryProfile();

    if (ledgers != null)
      for (int i = 0; i < ledgers.length; ++i) ledgers[i] = new Ledger(updateLedger.records.length);

    int capacity = NCSupport.roundUpToPowerOf2(minimumCacheSize, maxSize);
    table = new NCEntryLRU[capacity];
  }

  /**
   * Process all the entries in the ledgers [updateLock] Remember to take the lock if calling
   * catchupCore to avoid NPEs caused by null entries as we traverse ledgers
   */
  private void catchupCore() {
    if (ledgers == null) return;

    for (Ledger l : ledgers) {
      if (l.count == 0) continue;
      // Without locking take a peek at it's worth doing any work no inspection
      // SynchronizationOnLocalVariableOrMethodParameter: We are touching all
      // Ledger and need to sync up
      //noinspection SynchronizationOnLocalVariableOrMethodParameter
      synchronized (l) {
        l.halfSwap(updateLedger); // Effectively swaps the entries out
      }
      int count = updateLedger.count; // Cache count...
      for (int i = 0; i < count; ++i) {
        var entry = updateLedger.records[i];
        updateLedger.records[i] = null;
        catchupEntry(entry);
      }
    }
  }

  /** if evictOnOverflow, keep removing entries until cache size drops below maxSize [updateLock] */
  private void removeOverflow() {
    if (evictOnOverflow) {
      int initialSize = size;
      // Check if we are over maxSize and remove the older items
      NCEntryLRU last = header.after;
      int maxSize = adjustedMaxSize(this.maxSize);
      while (size > maxSize) {
        if (OGTrace.observer.collectsCacheDetails()) {
          int markedInsertCount = header.getInsertMark();

          // last.insertMark is the number of entries that had to be
          // inserted to cause last to be evicted
          // (ie, how far away it was from the header).
          // Set this on header before evicting last.
          header.setInsertMark(last.getInsertMark());

          int tailDistance = insertCount - markedInsertCount - 1;
          if (tailDistance < minTailObserved) minTailObserved = tailDistance;
        }
        remove(last);
        // in theory we could be doing [header.after] to get
        // the new last after removal, but that does not work
        // well and will cause an infinite loop for NCEntries
        // that never really remove a value (e.g. NCEntryMark)
        last = last.after;
      }
      OGTrace.evict(EvictionReason.overflow, initialSize - size);
      if (isProfilingContention())
        cacheCounters.addNumEvictionsByCause(EvictionReason.overflow, initialSize - size);
    }
  }

  /** Allows on the fly temporary adjustment of maxSize */
  protected int adjustedMaxSize(int maxSize) {
    return maxSize;
  }

  /** [updateLock] */
  private void catchupEntry(NCEntryLRU e) {
    if (e.removed()) return; // Same entry can be scheduled for removal more than once

    if (e.before == null) {
      // New Entry....
      // Note: Node could have become invalid at any point.
      // However it's ok to reinsert at the head anyways.
      // Assumption is that this is by far more common case
      size += e.getSize();
      e.setRequestCount(insertCount); // [profileCache]
      insertCount++; // [profileCache]
      insertCountLong++;
      e.addBefore(header);
      e.inLedger = false;
    } else if (e.invalid()) {
      // Invalid entries on bucket line
      remove(e);

      OGTrace.evict(EvictionReason.invalidEntry, 1);

      // TODO(OPTIMUS-24360): Look into merging with
      //  OGTrace stats and integrating with remove method
      if (isProfilingContention())
        cacheCounters.addNumEvictionsByCause(
            EvictionReason.invalidEntry, 1); // is this too expensive to always profile?
    } else {
      // Cache hit
      if (OGTrace.observer.collectsCacheDetails()) {
        // if we got a cache hit on e, then we are about to move the NCEntry.before pointer (see
        // e.recordAccess below), so that e will be at the front of the list (ie, it becomes the
        // most recently used). We set insertMark on the entry in front of e (before it moves to the
        // front) because that entry is NOT about to move. Also, if that entry is evicted (it will
        // be more likely to be evicted than e, which just got a hit), then we take its insertMark
        // in removeOverflow and set it as the latest insertMark on header to keep track of distance
        // from the front of the list.
        //
        // In summary - if we got a cache hit on an entry behind (ie, older than) e.before, and
        // e.before was never a cache hit, then we will want to know how far the item was from the
        // header so we can later compute equivalent size. If, however, e.before DOES get a cache
        // hit later, then we set the insertMark on the entry before it. This value must be at least
        // as much as the insertMark on the entry that got the hit.

        // [profileSizeBruteForce] dbgUpdateMaxDistanceBruteForce(e):
        e.before.setInsertMark(insertCount);

        int rcount = insertCount - e.getRequestCount();
        PropertyNode<?> value = e.getValue();
        if (value != null) OGTrace.observer.reuseUpdate(value, rcount);
        e.setRequestCount(insertCount);
        e.hitCount++;
      }
      e.recordAccess(header);
      e.inLedger = false;
    }
  }

  /** Helper to visualize cache as LRU (oldest to youngest) */
  @SuppressWarnings("unused") // for debugging only, not part of the api
  private ArrayList<Object> dbgAsArray() {
    catchupCore();
    var r = new ArrayList<>();
    NCEntryLRU ce = header.after;
    while (ce != header) {
      var entryValue = ce.getValue();
      if (entryValue == null) r.add(ce);
      else r.add(ce.getValue());
      ce = ce.after;
    }
    return r;
  }

  /**
   * [profileSizeBruteForce] This is a brute force approach for getting required min size of cache
   * with the same performance
   */
  @SuppressWarnings("unused") // for debugging only, not part of the api
  private void dbgUpdateMaxDistanceBruteForce(NCEntryLRU e) {
    // This is a brute force
    int distance = 0; // Distance from head
    NCEntryLRU ce = e;
    while (ce != header) {
      distance++;
      ce = ce.after;
    }
    // Need to uncomment in debug cases if (distance > maxCacheSizeUsed) maxCacheSizeUsed =
    // distance;
  }

  /**
   * Returns maxCacheSize observed <= maxCacheSize, such that using this value as maxCacheSize and
   * the same pattern of accesses one would achieve the same cache hits It's currently loops over
   * entire cache, however you need to call it only once. At the end. There is a room to improve and
   * stop the loop short, but it's not worth it. Note - cache behaviour is not deterministic in
   * multi-threaded case, so neither is this equivalent size. We hold updateLock here because we
   * need to call catchupCore (and avoid NPEs as we go through cache entries)
   */
  @Override
  public int profMaxSizeUsed() {
    updateLock.lock();
    try {
      catchupCore(); // to flush entries from ledgers in multi-threaded case
      int tailDistance = minTailObserved;

      int tail = 0;
      NCEntryLRU ce = header;
      do {
        // simulate evictions (see similar logic in removeOverflow)
        int dist = insertCount - ce.getInsertMark() + tail;
        if (dist < tailDistance) tailDistance = dist;
        ce = ce.after;
        tail++;
      } while (ce != header);

      return size - tailDistance;
    } finally {
      updateLock.unlock();
    }
  }

  @Override
  public long profLockContentionTime() {
    return cacheCounters.getLockContentionTime();
  }

  @Override
  public int profCacheBatchSize() {
    return cacheBatchSize;
  }

  @Override
  public int profCacheBatchSizePadding() {
    return cacheBatchSizePadding;
  }

  @Override
  public long profLockNumAttempts() {
    return cacheCounters.getUpdateLockAttempts();
  }

  @Override
  public long profNumEvictions(EvictionReason reason) {
    return cacheCounters.getNumEvictionsByCause(reason);
  }

  /**
   * Creates the most appropriate entry for the cache. Derived implementation can implement
   * SoftReference based cache entries, profiler enriched entries etc.
   *
   * @param keyGroupParticipant - entry is a participant in the key group see UncommonCaching.md
   */
  protected NCEntry newNCEntry(
      int hash, PropertyNode<?> value, NCEntry next, boolean keyGroupParticipant) {
    if (keyGroupParticipant) return new NCEntryGrouped(hash, value, next);
    if (OGTrace.observer.collectsCacheDetails()) return new NCEntryProfile(hash, value, next);
    return new NCEntryV(hash, value, next);
  }

  /** Returns cache name */
  @Override
  public final String getName() {
    return name;
  }

  @Override
  public final void setName(String n) {
    name = n;
  }

  @Override
  public final int getMaxSize() {
    return maxSize;
  }

  // Move entries from a table `from` to this UNodeCache's table. Any entry which is not null after
  // a call to this
  // function was added to the `from` table after moveEntries was called. Returns true if an entry
  // was moved.
  //
  // This function must be called under updateLock.
  private boolean moveEntries(NCEntry[] from) {
    var modified = false;

    for (int i = 0; i < from.length; i++) {
      // Grab the linked list from the bucket and zero it out. This works because the only insertion
      // is in putIfAbsentCore where we do CAS over from[i]. The interleaved case would look like
      // this,
      //
      // This thread                Other thread
      //      |                   getVolatile(head)
      //   getAndSet(head)               |
      //      |                   CAS(head, new node)
      //
      // In which case the CAS will fail, the other thread will attempt to grab head again and get
      // an empty (null) list at which point it will insert into an empty bucket in `from` (which is
      // ok).
      var bucket = (NCEntry) vh_table.getAndSet(from, offset(from, i), null);
      if (bucket == null) continue;
      modified = true; // we do have something to move!

      // Now `bucket` is disconnected from `from`. NCEntry.next is only ever updated under lock (and
      // this function is called under lock only), which means that our disconnected linked list is
      // now safe from any modification. We insert all of its entries into our table.
      var t = table;
      NCEntry nextNode;
      do {
        // we grab our next node right now because we are going to be updating bucket.next when we
        // insert it into table.
        nextNode = bucket.next;
        int insertLocation = offset(t, bucket.hash);

        // Note that we don't check whether head is already in the target table, because we want to
        // make sure that *every* node in the LRU as a corresponding entry in the table, even if the
        // nodes are (logical) duplicates (otherwise we might end up removing a node twice, which
        // will crash.)
        while (true) {
          var head = (NCEntry) vh_table.getVolatile(t, insertLocation);
          // We link bucket early so that once it is swapped into our table any reader can
          // immediately start walking the new bucket list.
          bucket.next = head;
          if (vh_table.compareAndSet(t, insertLocation, head, bucket)) break;
        }

      } while ((bucket = nextNode) != null); // move to next bucket
    }

    return modified;
  }

  @Override
  public void setMaxSize(int newMaxSize) {
    updateLock.lock();
    try {
      clearOldest(newMaxSize, CacheResizeCause$.MODULE$); // Trim the LRU to the new size.

      maxSize = newMaxSize; // note: even if maxSize is 0, the table.length > 0, which is required
      // to support proxies.

      // Under lock, LRU updates go to the ledgers and the global LRU isn't actually modified at
      // all, so we don't need to
      // touch the LRU at all.

      int newCapacity = NCSupport.roundUpToPowerOf2(minimumCacheSize, maxSize);
      if (newCapacity != table.length) {
        // Note that the table is *never* locked or synchronized, by design. This makes resizing it
        // tricky, as stuff can get inserted while we are moving to the new table. We want to avoid
        // losing any insertion because the rest of UNodeCache relies on the fact that the LRU and
        // table are in sync.

        // First, we swap the table for a new one. Note that at this point any new lookups on
        // `table` will find empty buckets, and so might get false negative cache hits for stuff
        // that is held in `old`. Because the UNodeCache is a cache, we have at-least-once
        // semantics, so it should be ok to execute a node twice (even concurrently!)
        var old = table;
        table = new NCEntryLRU[newCapacity];

        // At this point other threads might still be searching the buckets in `old` and possibly
        // inserting in them so we have to be careful to move every entries in the old table to the
        // new one, even those inserted while we complete the move. We keep doing moves until the
        // old table is empty.
        while (moveEntries(old))
          ;

        // TODO (OPTIMUS-57931):
        //  Note that at this point there is still a potential race: if another thread is extremely
        //  slow in updating its bucket, it might hold onto a reference in old longer than it takes
        //  for us to walk `old` and set it all to null, then walk it *again* verifying that it is
        //  in fact null everywhere. This means that in some (hopefully) rare cases we are still
        //  dropping entries, which means that we have to add null checks to remove().
        //
        //  Is this fixable? Does a solution exists that is faster than the null checks? ¯\_(`_`)_/¯
      }
    } finally {
      updateLock.unlock();
    }
  }

  /**
   * If you are thinking about using this PLEASE consider concurrency - this is NOT reliable and
   * depends on requested cache concurrency, which in turn depends on number of threads. Note: we
   * take updateLock to make sure the call to catchupCore does not NPE
   */
  @Override
  public final int getSize() {
    updateLock.lock();
    try {
      catchupCore();
      return size;
    } finally {
      updateLock.unlock();
    }
  }

  @SuppressWarnings("unused") // Used to enable debugging see the comment b
  private int DEBUG_ONLY_sizeIsConsistent() {
    // Insert as the last line catchupCore()
    // if (size != DEBUG_ONLY_sizeIsConsistent()) throw new GraphInInvalidState("");
    int actualSize = 0;
    NCEntryLRU last = header.after;
    while (last != header) {
      actualSize += last.getSize();
      last = last.after;
    }
    return actualSize;
  }

  @Override
  public final int getSizeIndicative() {
    return getSizeIndicative(false);
  }

  public final int getSizeIndicative(boolean forceCatchup) {
    if (DiagnosticSettings.granularCacheSize || forceCatchup) {
      updateLock.lock();
      try {
        catchupCore();
      } finally {
        updateLock.unlock();
      }
    }
    return size;
  }

  public final int getCacheConcurrency() {
    return this.cacheConcurrency;
  }

  public final void injectMarker(int i) {
    updateLock.lock();
    try {
      catchupCore();
      NCMarkerEntry ncmarker = new NCMarkerEntry(i);
      size += ncmarker.getSize();
      ncmarker.addBefore(header);
    } finally {
      updateLock.unlock();
    }
  }

  // Called under [updateLock]
  private NCEntryLRU findMarker(int i) {
    NCEntryLRU last = header.after;
    while (last != header) {
      if (last instanceof NCMarkerEntry) {
        if (((NCMarkerEntry) last).id == i) {
          return last;
        }
      }
      last = last.after;
    }
    return null;
  }

  public final long clearUpToMarker(int i) {
    long removed = 0L;
    updateLock.lock();
    try {
      catchupCore();
      // this is O(N) but it should be very quick in practice
      var target = findMarker(i);

      if (target != null) {
        NCEntryLRU last = header.after;
        while (last != header) {
          if (last instanceof NCMarkerEntry) {
            size -= last.remove(); // Remove from LRU
            if (last == target) break;
          }

          remove(last);
          removed++;
          last = last.after;
        }
      }
    } finally {
      updateLock.unlock();
    }
    recordCacheClear("UNodeCache.clearUpToMarker", removed);
    return removed;
  }

  /** Clears cache */
  @Override
  public final long clear(ClearCacheCause cause) {
    log.debug("clearing cache " + getName() + " shared: " + sharable());
    long removed = 0L;
    updateLock.lock();
    try {
      // This function is used concurrently with get/put
      // Those threads can grab table/ledger and we can't swap on under them
      // One specific issue, is that calling reset()
      // will introduce into a new ledger an item from an old table, and the remove
      // can't deal with that

      // This version just loops are calls remove on all items in the LRU list
      // This is legal since updateLock really is about LRU list.
      // Another implementation (faster) would take all the ledgers locks
      // Clear the table (can't move the table) reset head of LRU and release the locks
      catchupCore();
      int startSize = size; // We are holding updateLock and only remove() will be updating size
      NCEntryLRU ce = header.after;
      while (ce != header) {
        remove(ce);
        removed++;
        ce = ce.after;
      }

      OGTrace.evict(cause.evictionReason(), startSize - size);
      if (isProfilingContention())
        cacheCounters.addNumEvictionsByCause(EvictionReason.explicitUserCleared, startSize - size);
    } finally {
      updateLock.unlock();
    }
    recordCacheClear(cause.toString(), removed);
    return removed;
  }

  /** iterates over items in cache */
  @Override
  public final void foreach(Consumer<PropertyNode<?>> consumer) {
    iterateAndClearMatching(
        false,
        i -> {
          consumer.accept(i);
          return false; // don't actually clear
        },
        null);
  }

  /** Clears cache off items that match filter */
  @Override
  public final long clear(Predicate<PropertyNode<?>> filter, ClearCacheCause cause) {
    return iterateAndClearMatching(true, filter, cause);
  }

  /** Implementation of clear or iterate from oldest to newest */
  private long iterateAndClearMatching(
      boolean report, Predicate<PropertyNode<?>> filter, ClearCacheCause cause) {
    long removed;
    updateLock.lock();
    try {
      catchupCore();
      removed = iterateAndClearMatching(header, filter);
    } finally {
      updateLock.unlock();
    }
    if (report && cause != null) recordCacheClear(cause.toString(), removed);

    if (isProfilingContention())
      cacheCounters.addNumEvictionsByCause(EvictionReason.filteredClear, removed);

    OGTrace.evict(EvictionReason.filteredClear, removed);

    return removed;
  }

  /** Implementation of clear or iterate from oldest to newest */
  private long iterateAndClearMatching(NCEntryLRU header, Predicate<PropertyNode<?>> filter) {
    long removed = 0L;

    NCEntryLRU ce = header.after;
    while (ce != header) {
      PropertyNode<?> value = ce.getValue();
      // value is null when invalidated, so just silently clear it
      // but don't count it in the returned value, as it was not cleared by
      // the filter
      if (value == null) {
        remove(ce);
        if (isProfilingContention())
          cacheCounters.addNumEvictionsByCause(EvictionReason.invalidEntry, 1);
        OGTrace.evict(EvictionReason.invalidEntry, 1);
      } else if (filter.test(value)) {
        remove(ce);
        removed++;
      }
      ce = ce.after;
    }

    return removed;
  }

  /** Clears oldest nodes (ratio of 0.1 means 10% of all nodes in each segment) */
  @Override
  public final CleanupStats clearOldest(double ratio, ClearCacheCause cause) {
    var capped = ratio;
    if (ratio < 0) {
      log.error("clear ratio " + ratio + " was less than 0! Setting to 0.");
      capped = 0;
    }
    if (ratio > 1) {
      log.error("clear ratio " + ratio + " was greater than 1! Setting to 1.0.");
      capped = 1.0;
    }
    var newSize = size - (int) (capped * size + 0.5);
    return clearOldest(newSize, cause);
  }

  private CleanupStats clearOldest(int requestedSize, ClearCacheCause cause) {
    long removed;
    long remaining;
    int newSize = Math.max(requestedSize, 0);

    updateLock.lock();
    try {
      int prevSize = size;
      catchupCore();
      NCEntryLRU last = header.after;
      while (size > newSize && last != header) {
        remove(last);
        last = last.after;
      }
      remaining = size;
      removed = prevSize - remaining; // We could have removed some invalid entries
    } finally {
      updateLock.unlock();
    }
    recordCacheClear(cause.toString(), removed);

    if (isProfilingContention())
      cacheCounters.addNumEvictionsByCause(EvictionReason.memoryPressure, removed);

    OGTrace.evict(cause.evictionReason(), removed);

    return CleanupStats.apply(removed, remaining);
  }

  /**
   * Verify that the cache invariants are satisfied. This is very expensive! Note that it will
   * produce false errors if stuff is entered into the cache while the invariants are being
   * verified, so it should be called only on a quiescent graph.
   */
  public void verifyInvariants() {
    updateLock.lock();
    try {
      // lock all ledgers
      catchupCore(); // flush all ledgers
      var diagnostics = new ArrayList<String>();
      var lruSize = 0;
      // LRU navigation
      var entry = header.after;
      var t = table;

      {
        while (entry != header) {
          int i = offset(t, entry.hash);
          var head = (NCEntry) vh_table.getVolatile(t, i);

          var found = false;
          for (var e = head; e != null; e = e.next) {
            if (e == entry) {
              found = true;
              break;
            }
          }
          // TODO (OPTIMUS-57931): This should be fatal.
          if (!entry.removed() && !found) {
            log.warn(
                "entry with hash " + entry.hash + " exists in LRU but is absent from the table");
          }
          if (entry.removed() && found) {
            diagnostics.add(
                "entry with hash "
                    + entry.hash
                    + " exists in LRU and table but is marked as removed");
          }
          if (!entry.removed()) lruSize += 1;
          if (entry.inLedger)
            diagnostics.add("entry with hash " + entry.hash + " exists in LRU and in a ledger.");
          entry = entry.after;
        }
      }

      var tableSize = 0;
      for (var ti : t) {
        for (var e = ti; e != null; e = e.next) {
          if (!e.inLedger() && !e.removed())
            tableSize += 1; // stuff might be added to the ledgers while we are checking
        }
      }

      // TODO (OPTIMUS-57931): Any size mismatch should be fatal, but we know the resize logic is
      // buggy and so sometimes
      //  there are nodes that are on the LRU that are not in the table. The opposite should never
      // happen, because if a
      //  node is on table but not the LRU it will *never* get cleared!
      if (lruSize < tableSize) diagnostics.add("Size mismatch between LRU and UNodeCache.size");

      var sizeString =
          String.format(
              "\n\tUNodeCache.size: %d\n\tLRU size: %d\n\ttable size: %d\n",
              size, lruSize, tableSize);
      if (diagnostics.isEmpty()) {
        log.info("Invariants verfied on cache " + name);
      } else {
        log.error("Failed to verify invariants on cache " + name + sizeString);
        throw new GraphInInvalidState(String.join("\n", diagnostics));
      }
    } finally {
      updateLock.unlock();
    }
  }
} // UNodeCache
