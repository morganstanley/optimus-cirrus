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
package optimus.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import optimus.interception.StrictEquality;

/*
 * A weak interner that supports custom hashCode/equals. For items that implement
 * InternerHashEquals, the interner will use the custom hashCode/equals methods.
 */
public class WeakInterner {
  private static final double loadFactor = 0.75;
  private static final VarHandle vh_table = MethodHandles.arrayElementVarHandle(Node[].class);
  private static final VarHandle vh_size;

  /** Used to mark a bucket during resize and also to mark deleted entries */
  private static final Node tombstone = new Node(null, 0, null);

  // The table is a power of two, so we can use & instead of % to get the index
  private volatile Node[] table;
  // Currently single atomic counter is used, but consider splitting into LongAdder style value.
  @SuppressWarnings("unused") // Updated via var handle
  private int size;

  private volatile int threshold; // Updated on resize

  // Profiling fields only
  private long profNodesCleared = 0; // Number of nodes cleared updated under lock

  static {
    try {
      var lookup = MethodHandles.lookup();
      vh_size = lookup.findVarHandle(WeakInterner.class, "size", int.class);
    } catch (Exception ex) {
      throw new Error(ex);
    }
  }

  private final ReferenceQueue<Object> queue = new ReferenceQueue<>();

  public WeakInterner() {
    int capacity = 1024;
    threshold = (int) (capacity * loadFactor);
    table = new Node[capacity];
  }

  private static class Node extends WeakReference<Object> {
    private final int hash;
    private Node next;

    Node(Object referent, int hash, ReferenceQueue<Object> queue) {
      super(referent, queue);
      this.hash = hash;
    }
  }

  private static int rehash(int h) {
    return (h ^ (h >>> 16));
  }

  public Object intern(Object o) {
    var fence = StrictEquality.enter();
    try {
      return doIntern(o);
    } finally {
      StrictEquality.exit(fence);
    }
  }

  public Object nonStrictIntern(Object o) {
    return doIntern(o);
  }

  private Object doIntern(Object o) {
    int hash = rehash(hashOf(o));
    Node candidate = null;

    cleanupQueue();

    Node[] t = this.table;
    int i = hash & (t.length - 1);
    Node tail = null;

    while (true) {
      var head = (Node) vh_table.getVolatile(t, i);
      if (head == tombstone) {
        foundTombstone(); // wait for resize to complete
        t = table; // re-read the table and other variables
        i = hash & (t.length - 1);
        tail = null;
        continue;
      }

      var io = getIfPresent(head, tail, hash, o);
      if (io != null) {
        if (candidate != null)
          candidate.clear(); // candidate is not used and don't want it to get on the queue
        return io;
      }
      if (candidate == null) candidate = new Node(o, hash, queue);
      candidate.next = head;
      if (vh_table.compareAndSet(t, i, head, candidate)) {
        newEntryAdded();
        return o;
      }
      // On next scan (getIfPresent), skip checking older items, we never insert in the middle
      tail = head;
    }
  }

  public int getSize() {
    return size;
  }

  public long getProfNodesCleared() {
    return profNodesCleared;
  }

  private Object getIfPresent(Node node, Node tail, int hash, Object o) {
    while (node != tail && node != null) {
      if (node.hash == hash) {
        Object io = node.get();
        if (equals(io, o)) return io;
      }
      node = node.next;
    }
    return null;
  }

  private void foundTombstone() {
    //noinspection EmptySynchronizedStatement
    synchronized (this) {
      // Do nothing, just wait for the resize
      // Consider helping the resize thread
    }
  }

  private void newEntryAdded() {
    var prevSize = (int) vh_size.getAndAdd(this, 1);
    if (prevSize > threshold) {
      synchronized (this) {
        // Have to re-read the size as it might actually be smaller
        prevSize = (int) vh_size.getVolatile(this);
        if (prevSize > threshold) resize();
      }
    }
  }

  /** During resize, matches from other threads may proceed and size may be updated */
  private void resize() {
    var oldTable = table;
    int oldCapacity = oldTable.length;
    int newCapacity = oldCapacity << 1;
    if (newCapacity < 0) throw new IllegalStateException("Too large");

    threshold = (int) (newCapacity * loadFactor);

    int removed = 0;
    var newTable = new Node[newCapacity];
    for (int i = 0; i < oldTable.length; i++) {
      var entry = grabEntry(oldTable, i);
      while (entry != null) {
        var next = entry.next;
        if (!entry.refersTo(null)) {
          int hash = entry.hash;
          int index = hash & (newCapacity - 1);
          entry.next = newTable[index];
          newTable[index] = entry;
        } else {
          removed += remove(entry);
        }
        entry = next;
      }
    }
    profNodesCleared += removed;
    vh_size.getAndAdd(this, -removed); // Update size
    table = newTable; // Publish the new table
  }

  private int remove(Node node) {
    node.next = tombstone; // mark the node as removed and avoid double removing it on clean
    return 1;
  }

  private Node grabEntry(Node[] oldTable, int i) {
    var entry = oldTable[i];
    while (!vh_table.compareAndSet(oldTable, i, entry, tombstone)) {
      entry = oldTable[i]; // re-read the value
    }
    return entry;
  }

  /** Cleanup runs concurrently with requests, but not resize */
  private void cleanupQueue() {
    var ref = (Node) queue.poll();
    if (ref == null) return;

    int removed = 0;
    synchronized (this) {
      while (ref != null) {
        if (ref.next == tombstone) {
          // Could have been removed as part of resize or another thread cleaning up a cache line
          ref = (Node) queue.poll();
          continue;
        }
        var t = table;
        var i = ref.hash & (t.length - 1);
        var head = (Node) vh_table.getVolatile(t, i);
        // eat front (new items may be added to the front)
        while (head != null && head.refersTo(null)) {
          if (vh_table.compareAndSet(table, i, head, head.next)) {
            removed += remove(head);
          }
          head = (Node) vh_table.getVolatile(t, i);
        }

        // walk the cache line...
        while (head != null) {
          var next = head.next;
          if (next != null && next.refersTo(null)) {
            head.next = next.next;
            removed += remove(next);
          } else head = next; // move to the next
        }
        ref = (Node) queue.poll();
      }
      profNodesCleared += removed;
      vh_size.getAndAdd(this, -removed);
    }
  }

  protected int hashOf(Object o) {
    return InternerHashEquals.hashOf(o);
  }

  protected boolean equals(Object o1, Object o2) {
    return InternerHashEquals.equals(o1, o2);
  }
}
