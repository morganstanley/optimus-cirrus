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
import optimus.graph.CacheTopic$;
import optimus.graph.GraphInInvalidState;
import optimus.graph.NodeTaskInfo;
import optimus.graph.OGTrace;
import optimus.graph.PropertyNode;
import optimus.platform.EvaluationQueue;
import optimus.platform.storable.EntityImpl;

/** Common functionality for UNodeCache and NodeScopedCache */
public abstract class NodeCacheBase extends NodeCCache {
  protected volatile NCEntry[] table; // hash bucket table
  protected static final VarHandle vh_table = MethodHandles.arrayElementVarHandle(NCEntry[].class);
  protected static final NCEntry tombstone = new NCEntry(0, null, null);

  /**
   * Looks up the item in the cache and returns it if found (updating the age) Else caches the node
   * evicting the oldest item
   */
  @Override
  public final <T> PropertyNode<T> putIfAbsent(
      NCPolicy suppliedPolicy, PropertyNode<T> key, EvaluationQueue eq) {
    if (key.isInvalidCache()) throw new GraphInInvalidState("Use full keys");

    var policy = suppliedPolicy.switchPolicy(key);
    if (policy.noCache) return key; // if the policy is DontCache don't even try to look it up

    boolean tryLocalCache = policy.tryLocalCache(key);
    if (tryLocalCache) {
      PropertyNode<T> localMatch = policy.localMatch(key);
      if (localMatch != null) return localMatch;
    }

    PropertyNode<T> r = putIfAbsentCore(policy, key, eq);
    if (tryLocalCache) EntityImpl.setLocalCache(r.entity(), r);

    return r;
  }

  /**
   * Looks up the item in the cache and returns it if found (updating the age) else returns null
   * Does NOT create any delay resolving proxies and will rather return null if can't answer
   * immediately
   */
  @Override
  public final <T> PropertyNode<T> getIfPresent(
      NodeTaskInfo info, PropertyNode<T> key, EvaluationQueue eq) {

    Ledger l = getLedger();
    NCPolicy policy = info.cachePolicy().switchPolicy(key);
    int hash = policy.hashOf(key);

    return getIfPresentCore(l, headAt(table, hash), null, key, policy, hash, eq);
  }

  /**
   * Looks up the item in the cache and returns it if found (updating the age) else caches the node
   *
   * <p>Note: EvaluationQueue is used for OGTrace
   */
  private <T> PropertyNode<T> putIfAbsentCore(
      NCPolicy policy, PropertyNode<T> key, EvaluationQueue eq) {

    // [SEE_XSFT_SPECULATIVE_PROXY]
    // Consider for training runs 'disabling' alternative lookup
    // The only purpose for alternativeLookup is to reduce the number of proxies we are creating
    // by directly returning the upProxy if it's complete and usable
    // var alternative = (NCPolicy.LookupResult<T>)NCPolicy.LookupResult.empty;
    NCPolicy.LookupResult<T> alternative = policy.alternativeLookup(this, key, eq);
    if (alternative.done) return alternative.result;

    // don't attribute the time if we short-circuited the lookup (in alternativeLookup)
    OGTrace.consumeTimeInMs(OGTrace.TESTABLE_FIXED_DELAY_CACHE, CacheTopic$.MODULE$);

    // These values don't change in the loops below
    int hash = policy.hashOf(key);
    Ledger l = getLedger();
    boolean keyGroupParticipant = key.propertyInfo().isGroupCached();

    // These values CAN change in the loops below
    NCEntry[] t = table;
    int i = offset(t, hash);
    NCEntry tail = null;
    DelayedProxyNode<T> proxy = null; // Lazily assign

    while (true) {
      // Try finding it first
      var head = (NCEntry) vh_table.getVolatile(t, i); // aka table[bucket] and start of scanning
      if (head == tombstone) {
        foundTombstone();
        t = table;
        i = offset(t, hash);
        tail = null;
        continue;
      }

      var insertNode = key; // If not found anything will insert the requested node
      var found = getIfPresentCore(l, head, tail, key, policy, hash, eq);

      if (found != null) { // Found potential match, and we just need to adjust for different CS
        if (policy.acceptAnyUsableCS) return found; // [0] Handling CS internally

        PropertyNode<?> kvn = NCSupport.isUsableWRTCancelScopeAndEnv(key, found);
        if (kvn == found) return found; // [1] Complete match and can re-use directly
        // [2] Can't answer yet and we need a proxy
        if (kvn == null) insertNode = proxy = policy.updateProxy(this, key, found, proxy);
      }

      if (policy.alwaysNeedsProxy) {
        var potentialHit = found != null ? found : alternative.result;
        insertNode = proxy = policy.updateProxy(this, key, potentialHit, proxy);
      }

      // Try to add the entry to the head of the bucket list. It can fail because another thread
      // pushed an entry onto the list. The new entry from the other thread might be the entry we
      // are looking for therefore we need to rescan new node(s), that is from the beginning of the
      // (updated) bucket to the current non-updated "head".
      var ne = newNCEntry(hash, insertNode, head, keyGroupParticipant);
      if (vh_table.compareAndSet(t, i, head, ne)) {
        if (keyGroupParticipant) CacheGroupKey.onNodeInserted(this, key, ne);
        newEntryAdded(l, ne);
        return insertNode;
      }

      // On next scan (getIfPresent), skip checking older items, we never insert in the middle
      tail = head;
    }
  }

  protected abstract NCEntry newNCEntry(int hash, PropertyNode<?> node, NCEntry head, boolean kgp);

  /**
   * Doesn't create proxies and returns a match or a potential match in the place where proxy would
   * need to be created
   *
   * <p>Note: comparison used is not symmetric see equalsForCaching
   *
   * <p>Note: EvaluationQueue is used for OGTrace
   */
  <T> PropertyNode<T> getIfPresentCore(
      Ledger l,
      NCEntry head,
      NCEntry tail,
      PropertyNode<T> key,
      NCPolicy policy,
      int hash,
      EvaluationQueue eq) {
    PropertyNode<T> found = null;
    NCEntry foundEntry = null;
    NCEntry invalid = null; // We want to remove invalid entries as soon as possible
    int collisionCount = 0; // Number of times equalsForCaching returned false

    for (NCEntry e = head; e != tail && e != null; e = e.next) {
      @SuppressWarnings("unchecked")
      var value = (PropertyNode<T>) e.getValue(); // null on early cleanup/soft refs
      if (value == null)
        invalid = e; // Any one entry on the same bucket line will cause cleanup of that line
      else if (e.hash == hash) { // Quickest test
        collisionCount++; // Subtract later if we do find a match

        // Value and key are not interchangeable here [SEE_NO_SYMMETRY_EQUALS_FOR_CACHING]
        if (!value.equalsForCaching(key)) continue;

        // isUsableWRTCS is arbitrarily slow in presence of exceptions, so keep this check first
        if (!policy.matchesScenario(value, key)) continue;
        // a.k.a. key or value or null
        PropertyNode<?> kvn = NCSupport.isUsableWRTCancelScopeAndEnv(key, value);
        if (kvn == key) continue;

        if (kvn == value || policy.acceptAnyUsableCS) {
          found = value;
          foundEntry = e;
          break;
          // we want the latest insert into cache (avoids degenerate case of multiple retries)
        } else if (found == null) {
          // tentative answer but don't break (keep looking for a 'better' match)
          // [SEE_PROXY_CHAINING]
          found = value;
          foundEntry = e;
        }
      }
    } // for loop

    if (invalid != null) touchEntry(l, invalid);

    if (found != null) {
      touchEntry(l, foundEntry);
      if (key.propertyInfo().isGroupCached()) CacheGroupKey.onNodeFound(this, key, foundEntry);
      collisionCount--; // don't count this as a collision if we found a match
    }

    if (collisionCount > 0) OGTrace.observer.lookupCollision(eq, collisionCount);

    return found;
  }

  protected abstract void foundTombstone();

  protected abstract void newEntryAdded(Ledger l, NCEntry entry);
  /** Update ledger if logic is needed */
  void touchEntry(Ledger l, NCEntry entry) {}
  /** Touch entry to update it in LRU */
  void touchEntry(NCEntry entry) {}

  protected Ledger getLedger() {
    return null;
  }

  /*
   * Hold batched list of updates
   * NOTE: there are a number of ways to minimize this synchronization, but would need real testing...
   */
  protected static class Ledger {
    NCEntryLRU[] records; // cache misses and cache hits go here
    int count; // Current size of the segment

    /* Effectively by how much to delay dealing with LRU and evictions */
    Ledger(int maxRecordCount) {
      records = new NCEntryLRU[maxRecordCount];
    }

    final void halfSwap(Ledger o) {
      var s_records = records;
      records = o.records;
      o.records = s_records;
      o.count = count;
      count = 0;
    }
  }

  /** Must be specific to an implementation */
  private NCEntry headAt(NCEntry[] t, int hash) {
    return (NCEntry) vh_table.getVolatile(t, offset(t, hash));
  }

  static int offset(NCEntry[] t, int hash) {
    return hash & (t.length - 1);
  }
}
