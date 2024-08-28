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

import java.util.Arrays;
import java.util.function.Consumer;

import optimus.graph.GraphInInvalidState;
import optimus.graph.Settings;

/**
 * Basic FILO with by index remove functionality. 1. No checking is done 2. All methods are expected
 * to be called under a lock 3. Items are always packed 4. All methods are O(1)
 */
public abstract class IndexedArrayList<T extends IndexedArrayList.IndexedItem> {
  public static class IndexedItem {
    int index = -1;
  }

  public int count;

  private volatile T[] q = createInitialArray();

  protected abstract T[] createInitialArray();

  public final boolean isEmpty() {
    return count == 0;
  }

  public final int size() {
    return count;
  }

  public final T[] getUnderlyingList() {
    return q;
  }

  public final void add(T item) {
    if (count >= q.length) {
      //noinspection NonAtomicOperationOnVolatileField
      q = Arrays.copyOf(q, count * 2);
    }
    if (Settings.schedulerAsserts && item.index != -1) throw new GraphInInvalidState();
    q[count] = item;
    item.index = count++;
  }

  public final T pop() {
    if (count == 0) return null;
    T r = q[--count];
    r.index = -1;
    q[count] = null;
    return r;
  }

  public final T popFirst() {
    if (count == 0) return null;
    T r = q[0];
    remove(r);
    return r;
  }

  /** Note: returns the last item that was added */
  public final T peek() {
    if (count == 0) return null;
    return q[count - 1];
  }

  /** Debug only usage */
  public final boolean contains(T item) {
    if (item.index < 0 || item.index >= count) return false;
    return q[item.index] == item;
  }

  /** Returns true if item was found and item was successfully removed */
  public final boolean tryRemove(T item) {
    int i = item.index;
    if (i >= 0 && i < count && q[i] == item) {
      count--; // Ordering here is very important, item could be the last item
      q[i] = q[count]; // Move the last item in
      q[i].index = i;
      q[count] = null;
      // In the case where item is the last item, this line must be after .index = i
      item.index = -1;
      return true;
    }
    return false;
  }

  public final void remove(T item) {
    boolean removed = tryRemove(item);
    if (Settings.schedulerAsserts && !removed) throw new GraphInInvalidState();
  }

  public final void foreach(Consumer<T> consumer) {
    var qCopy = q;
    for (T item : qCopy) {
      if (item == null) break;
      consumer.accept(item);
    }
  }

  @Override
  public String toString() {
    return this.getClass().getName() + "[" + count + "]";
  }
}
