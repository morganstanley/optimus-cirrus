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

import java.io.Serializable;
import optimus.graph.GraphInInvalidState;
import optimus.graph.NodeTaskInfo;
import optimus.graph.OGLocalTables;
import optimus.graph.OGSchedulerContext;
import optimus.graph.OGTrace;
import optimus.graph.PropertyNode;

/**
 * Strategy to select the cache based on previously passed information via ScenarioStack and in the
 * near future based on the node itself Very tempting to separate default into default scenario
 * scenario (in)dependent selectors
 */
@SuppressWarnings("StaticInitializerReferencesSubClass")
public abstract class CacheSelector {
  public static final CacheSelector DEFAULT = new Default();

  public static class Moniker implements Serializable {
    public final String cacheName;
    public final boolean readFromGlobalCaches;

    public Moniker(String cacheName, boolean readFromGlobalCaches) {
      this.cacheName = cacheName;
      this.readFromGlobalCaches = readFromGlobalCaches;
    }
  }

  static class Default extends CacheSelector {
    @Override
    public <R> PropertyNode<R> putIfAbsent(
        NodeTaskInfo info, PropertyNode<R> node, OGSchedulerContext ec) {
      return getCacheForInfo(info).putIfAbsent(info, node, ec);
    }

    @Override
    protected <R> PropertyNode<R> getIfPresent(
        NodeTaskInfo info, PropertyNode<R> node, OGSchedulerContext ec) {
      return getCacheForInfo(info).getIfPresent(info, node, ec);
    }

    @Override
    public NodeCCache cacheForXSOwner(PropertyNode<?> key, NodeCCache proxyCache) {
      return proxyCache != null ? proxyCache : getCacheForInfo(key.propertyInfo());
    }

    @Override
    protected boolean readFromGlobalCaches() {
      return true;
    }
  }

  static class Scoped extends CacheSelector {
    private final CacheSelector prev;
    private final NodeCCache cache;

    Scoped(NodeCCache cache, CacheSelector prev) {
      this.prev = prev;
      this.cache = cache;
    }

    @Override
    protected <R> PropertyNode<R> putIfAbsent(
        NodeTaskInfo info, PropertyNode<R> node, OGSchedulerContext ec) {
      return cache.putIfAbsent(info, node, ec);
    }

    @Override
    public CacheSelector withoutScope() {
      return prev;
    }

    @Override
    public NodeCCache cacheForXSOwner(PropertyNode<?> key, NodeCCache proxyCache) {
      if (cache != proxyCache) throw new GraphInInvalidState("Scoped cache mismatch");
      // XS needs to escape the scope and at this point we don't have a cache to suggest
      return prev.cacheForXSOwner(key, null);
    }

    @Override
    public CacheSelector newScoped() {
      return new Scoped(new NodeScopedCache(0), prev);
    }

    @Override
    public CacheSelector onOwnerReset() {
      cache.resetSizeReporting();
      return DEFAULT;
    }
    /** If the outer cache cache is private we need to read from it */
    @Override
    public Moniker toMoniker() {
      return prev.toMoniker();
    }

    /** Scope caches are basically transparent and just forward */
    @Override
    protected boolean readFromGlobalCaches() {
      return prev.readFromGlobalCaches();
    }
  }

  static class Private extends CacheSelector {
    private final NodeCCache cache;
    private final CacheSelector readFrom;

    Private(NodeCCache cache, CacheSelector readFrom) {
      this.cache = cache;
      this.readFrom = readFrom;
    }

    @Override
    protected <R> PropertyNode<R> putIfAbsent(
        NodeTaskInfo info, PropertyNode<R> node, OGSchedulerContext ec) {
      if (readFrom != null) {
        // We always insert in this private cache, but we try to read from other caches first. We
        // read in order from the closest cache scopes to the outermost, and then maybe read from
        // global caches.
        PropertyNode<R> found = readFrom.getIfPresent(info, node, ec);
        if (found != null) return found;
      }
      return cache.putIfAbsent(info, node, ec);
    }

    @Override
    protected <R> PropertyNode<R> getIfPresent(
        NodeTaskInfo info, PropertyNode<R> node, OGSchedulerContext ec) {
      if (readFrom != null) {
        PropertyNode<R> found = readFrom.getIfPresent(info, node, ec);
        if (found != null) return found;
      }
      return cache.getIfPresent(info, node, ec);
    }

    // Monikers don't need to carry the entire set of nested caches because on the remote side the
    // caches aren't themselves cached from one call to the next, so you won't get reuse.
    @Override
    public Moniker toMoniker() {
      return new Moniker(cache.getName(), readFromGlobalCaches());
    }

    @Override
    public NodeCCache cacheForXSOwner(PropertyNode<?> key, NodeCCache proxyCache) {
      // Default to use proxyCache [1],
      // If it's null (came from Scoped), use the `current` [2] private cache
      // Note: Both [1] and [2] are very debatable
      return proxyCache != null ? proxyCache : cache;
    }

    @Override
    protected boolean readFromGlobalCaches() {
      return readFrom != null && readFrom.readFromGlobalCaches();
    }

    @Override
    public NodeCCache dbgCache() {
      return cache;
    }
  }

  static NodeCCache getCacheForInfo(NodeTaskInfo info) {
    var customCache = info.customCache();
    if (customCache != null) return customCache;
    if (info.isScenarioIndependent()) return UNodeCache.siGlobal();
    return UNodeCache.global();
  }

  /** Main entry point for any code that wants to lookup a node */
  public final <R> PropertyNode<R> lookupAndInsert(
      NodeTaskInfo info, PropertyNode<R> candidate, OGSchedulerContext ec) {
    var startTime = OGTrace.observer.lookupStart();
    var result = putIfAbsent(info, candidate, ec);
    var lt = OGLocalTables.getOrAcquire(ec);
    OGTrace.lookupEnd(lt, startTime, candidate, result);
    lt.release();
    return result;
  }

  /** Lookup in cache with insert */
  protected abstract <R> PropertyNode<R> putIfAbsent(
      NodeTaskInfo info, PropertyNode<R> node, OGSchedulerContext ec);

  /** Lookup in cache without insert */
  protected <R> PropertyNode<R> getIfPresent(
      NodeTaskInfo nti, PropertyNode<R> node, OGSchedulerContext ec) {
    return null;
  }

  /** Currently only private cache has moniker */
  public Moniker toMoniker() {
    return null;
  }

  /**
   * Returns true if in the chain of cache selectors Default is present.
   *
   * <p>Only Default (currently) reads from global caches
   */
  protected abstract boolean readFromGlobalCaches();

  /** Transition to a new scope cache */
  public CacheSelector newScoped() {
    return new Scoped(new NodeScopedCache(0), this);
  }

  /** Only scoped caches can `unwrap` */
  public CacheSelector withoutScope() {
    return this;
  }

  /** Where should we place the xs owner ? */
  public abstract NodeCCache cacheForXSOwner(PropertyNode<?> key, NodeCCache proxyCache);

  public CacheSelector onOwnerReset() {
    return DEFAULT;
  }

  /** Transition to a new private cache */
  public CacheSelector newPrivate(NodeCCache cache, boolean readFromOutside) {
    return new Private(cache, readFromOutside ? this : null);
  }

  /** Test only API */
  public NodeCCache dbgCache() {
    return null;
  }
}
