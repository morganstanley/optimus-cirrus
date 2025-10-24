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

import optimus.graph.PropertyNode;

public class NCEntry {
  protected PropertyNode<?> value;
  int hash; // It really is final, we just don't want to pay the price
  NCEntry next; // Updated only under [updateLock]. OK to see the old value!

  public NCEntry(int hash, PropertyNode<?> value, NCEntry next) {
    this.value = value;
    this.next = next;
    this.hash = hash;
  }

  /** Sanitized value */
  PropertyNode<?> getValue() {
    PropertyNode<?> r = value; // volatile read
    return (r == null || r.isInvalidCache()) ? null : r;
  }

  boolean invalid() {
    PropertyNode<?> r = value; // volatile read
    return r == null || r.isInvalidCache();
  }

  /** Returns true if the entry was removed */
  public boolean removed() {
    return value == null;
  }

  /** Remove ref to value and set a 'null' */
  void removeValue() {
    value = null; // lazySet()
  }

  void setInsertMark(int evictCount) {
    // We don't record anything in a regular case
  }

  int getInsertMark() {
    return 0; // Reasonable default
  }

  void setRequestCount(int requestCount) {
    // We don't record anything in a regular case
  }

  int getRequestCount() {
    return 0; // Reasonable default
  }

  int getSize() {
    return 1; // Default size: 1
  }

  int remove() {
    return 0; // Removed nothing
  }

  boolean inLedger() {
    return false;
  }
}

abstract class NCEntryLRU extends NCEntry {
  NCEntryLRU before, after; // Updated by cleanup thread only [updateLock]; this tracks the LRU list
  boolean inLedger; // Set when added to a ledger. Cleared when the ledger is processed;
  int hitCount;

  public NCEntryLRU(int hash, PropertyNode<?> value, NCEntry next) {
    super(hash, value, next);
  }

  /**
   * Returns entry's size if in fact it was removed from the list, 0 otherwise Notes:
   *
   * <p>1. Code in UNodeCache.remove aggressively removes invalid entries, Invalid entries can be in
   * the table, before processed off a ledger. The result is that remove can be called on NCEntry
   * before it ever seen by ledger processor CatchupEntry This is what this guard against null and
   * return 0 is about.
   *
   * <p>2. After removing from table, we can encounter the entry in a ledger, but will NOT process
   * it again, because It's effectively removed, removeValue called here and if(removed)) guard is
   * on UNodeCache.remove()
   *
   * <p>3. It's possible that such an entry however will linger in LRU list for entire round trip
   * worth of UNodeCache.size. See comments in catchupEntry
   */
  final int remove() {
    removeValue();
    if (before != null) {
      before.after = after;
      after.before = before;
      return getSize();
    }
    return 0;
  }

  @Override
  boolean inLedger() {
    return inLedger;
  }
  /** Inserts into the head [updateLock] */
  final void addBefore(NCEntryLRU existingEntry) {
    after = existingEntry;
    before = existingEntry.before;
    before.after = this;
    after.before = this;
  }

  /** Move from existing place to the head [updateLock] */
  final void recordAccess(NCEntryLRU header) {
    // Next 2 lines are remove() without deleting the value
    // Also no need to protect again before != null
    before.after = after;
    after.before = before;
    addBefore(header);
  }
}

class NCEntryV extends NCEntryLRU {

  /* Special case of the root entry */
  NCEntryV() {
    super(-1, null, null);
    before = after = this;
  }

  NCEntryV(int hash, PropertyNode<?> value, NCEntry next) {
    super(hash, value, next);
  }
}

/** Support markGroupCached() functionality */
final class NCEntryGrouped extends NCEntryV {
  final NCGroupedChildren groupedChildren;

  NCEntryGrouped(int hash, PropertyNode<?> value, NCEntry next) {
    super(hash, value, next);
    groupedChildren = new NCGroupedChildren(this);
  }

  @Override
  void removeValue() {
    super.removeValue();
    // This is race-y because we are potentially clearing while someone is holding onto the node, so
    // we may end up inserting too few children. That's probably fine though, given that we are
    // currently removing the node. We clearly don't care all that much.
    groupedChildren.children().clear();
  }
}

/** Stores extra profile data */
class NCEntryProfile extends NCEntryV {
  // For frequency estimation, record operation count at the time of insert (for reuse cycle)
  private int requestCount;
  private int insertMark; // Insert count at the time of update

  NCEntryProfile() {
    super();
  }

  NCEntryProfile(int hash, PropertyNode<?> value, NCEntry next) {
    super(hash, value, next);
  }

  // note - this is set to insertCount on cache hits and to the insertMark on the last (ie, least
  // recently used) entry
  // on cache eviction. Since clearing from cache is not unified, we only set this in removeOverflow
  // (ie, if there are evictions) but not when entries are cleared through some other means
  void setInsertMark(int insertCount) {
    this.insertMark = insertCount;
  }

  int getInsertMark() {
    return insertMark;
  }

  // This gets set in catchupEntry to the current insertCount when we create a new entry or when we
  // have a cache hit.
  // It is the number of requests to read from cache (ie hits or new inserts) so far
  void setRequestCount(int requestCount) {
    this.requestCount = requestCount;
  }

  int getRequestCount() {
    return requestCount;
  }
}

/**
 * Will remove (assume invalid) all entries with Exceptions on them This is not a common case
 * (CancellationScopes should probably be used) but DSI uses it
 */
class NCEntryNotValidWithException extends NCEntryV {
  public NCEntryNotValidWithException(int hash, PropertyNode<?> value, NCEntry next) {
    super(hash, value, next);
  }

  @Override
  PropertyNode<?> getValue() {
    return invalid() ? null : value;
  }

  @Override
  boolean invalid() {
    PropertyNode<?> r = value; // volatile read
    return r == null || r.isInvalidCache() || r.isDoneWithException();
  }
}

/** Stores hinted size */
class NCSizedEntryV extends NCEntryV {
  private final int size;

  public NCSizedEntryV(int hash, PropertyNode<?> value, int size, NCEntry next) {
    super(hash, value, next);
    this.size = size;
  }

  @Override
  int getSize() {
    return size;
  }
}

/**
 * Not a real entry and will not be inserted into the table but only used to mark a place in to
 * doubly linked list
 */
final class NCMarkerEntry extends NCEntryLRU {
  public final int id;

  public NCMarkerEntry(int id) {
    super(-1, null, null);
    this.id = id;
  }

  @Override
  void removeValue() {}

  @Override
  boolean invalid() {
    return false;
  }

  @Override
  public boolean removed() {
    return true;
  }

  @Override
  int getSize() {
    return 0;
  }
}
