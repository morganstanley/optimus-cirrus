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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import optimus.graph.GraphInInvalidState;
import optimus.graph.PropertyNode;
import optimus.graph.Settings;

/**
 * The cache that only can grow (but unlike UNodeCache dynamically) No LRU lists Note: lots of code
 * copied from UNodeCache and needs to be refactored/reused
 * <li>Ledgers are not needed
 * <li>Consider removing dead entries on the fly, but can we even get them? In the first version
 *     it's used for XS only and no-invalidation is happening there, also no soft references
 */
public class NodeScopedCache extends NodeCacheBase {
  private static final VarHandle vh_size;
  private static final double loadFactor = 0.75;
  private static final int minimumCacheSize = 16;
  private static final LongAdder totalSize = new LongAdder();

  static {
    try {
      var lookup = MethodHandles.lookup();
      vh_size = lookup.findVarHandle(NodeScopedCache.class, "size", int.class);
    } catch (Exception ex) {
      throw new Error(ex);
    }
  }

  // Currently single atomic counter is used, but consider splitting into LongAdder style value.
  @SuppressWarnings("unused") // Updated via var handle
  private int size;

  private int threshold; // Updated on resize

  public static int getScopedCachesTotalSize() {
    return totalSize.intValue();
  }

  @Override
  public void foreach(Consumer<PropertyNode<?>> consumer) {}

  /**
   * This is really for debugging only. To clear some cycles while inspecting the memory In general
   * scope cache is just dropped!
   *
   * @param cause Ignored
   * @return size before clear...
   */
  @Override
  public long clear(ClearCacheCause cause) {
    synchronized (this) {
      int prevSize = size;
      size = 0;
      this.table = new NCEntry[minimumCacheSize];
      return prevSize;
    }
  }

  public NodeScopedCache(int initialSize) {
    int capacity = NCSupport.roundUpToPowerOf2(minimumCacheSize, initialSize);
    threshold = (int) (capacity * loadFactor);
    table = new NCEntry[capacity];
  }

  @Override
  public int getSize() {
    return size;
  }

  public void resetSizeReporting() {
    if (!Settings.scopedCachesReducesGlobalSize) return;
    int currentSize = size;
    while (!vh_size.compareAndSet(this, currentSize, Integer.MIN_VALUE)) {
      currentSize = size;
    }
    if (currentSize <= 0) return; // Notice double reset is OK, but needs more testing
    totalSize.add(-currentSize);
    if (Settings.schedulerAsserts && totalSize.intValue() < 0)
      throw new GraphInInvalidState("Unbalanced scoped cache size accounting");
  }

  @Override
  protected NCEntry newNCEntry(int hash, PropertyNode<?> value, NCEntry next, boolean kgp) {
    return new NCEntry(hash, value, next);
  }

  @Override
  protected void foundTombstone() {
    //noinspection EmptySynchronizedStatement
    synchronized (this) {
      // Do nothing, just wait for the resize
      // Consider helping the resize thread
    }
  }

  @Override
  protected void newEntryAdded(Ledger l, NCEntry entry) {
    var prevSize = (int) vh_size.getAndAdd(this, 1);
    // If enabled, increment the global size counter, ignore after resetSizeReporting call
    if (Settings.scopedCachesReducesGlobalSize && prevSize >= 0) totalSize.increment();
    if (prevSize > threshold) {
      synchronized (this) {
        if (prevSize > threshold) resize();
      }
    }
  }

  private void resize() {
    NCEntry[] oldTable = table;
    int oldCapacity = oldTable.length;
    int newCapacity = oldCapacity << 1;
    if (newCapacity < 0) throw new IllegalStateException("Cache is too large");

    // Update the threshold first to allow inserts while resize is in progress
    threshold = (int) (newCapacity * loadFactor);

    var newTable = new NCEntry[newCapacity];
    for (int i = 0; i < oldTable.length; i++) {
      var entry = grabEntry(oldTable, i);
      while (entry != null) {
        NCEntry next = entry.next;
        int hash = entry.hash;
        int index = hash & (newCapacity - 1);
        entry.next = newTable[index];
        newTable[index] = entry;
        entry = next;
      }
    }
    table = newTable; // Publish the new table
  }

  /** Atomically grab the entry from the table, leave the tombstone */
  private NCEntry grabEntry(NCEntry[] oldTable, int i) {
    var entry = oldTable[i];
    while (!vh_table.compareAndSet(oldTable, i, entry, tombstone)) {
      entry = oldTable[i]; // re-read the value
    }
    return entry;
  }

  /** Helper to visualize cache as LRU (oldest to youngest) */
  public ArrayList<Object> dbgAsArray() {
    var r = new ArrayList<>();
    for (var entry : table) {
      while (entry != null) {
        r.add(entry.getValue());
        entry = entry.next;
      }
    }
    return r;
  }
}
