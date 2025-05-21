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
package optimus.graph.cache

import java.util
import util.Collections
import util.concurrent.ConcurrentHashMap
import java.lang.{Boolean => JBoolean}
import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.Breadcrumbs

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.collection.concurrent
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.LogPropertiesCrumb
import optimus.breadcrumbs.crumbs.Properties
import optimus.core.CoreAPI
import optimus.graph.GCNative
import optimus.graph.GCNative.LEVEL_CLEAR_ALL_CACHES_CALLBACKS
import optimus.graph.NodeTask
import optimus.graph.PropertyNode
import optimus.graph.diagnostics.EvictionReason
import optimus.graph.diagnostics.GCMonitorDiagnostics
import optimus.graph.tracking.AsyncClear
import optimus.graph.tracking.CacheClearMode
import optimus.graph.tracking.NoClear
import optimus.graph.tracking.SyncClear

import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.function.Predicate

sealed trait CacheFilter {
  val description: String
}
object CacheFilter {
  final case object None extends CacheFilter { override val description: String = "none" }
  final case class Filter(filter: PropertyNode[_] => Boolean, description: String) extends CacheFilter
  def apply(filter: PropertyNode[_] => Boolean, description: String = "unknown"): CacheFilter =
    Filter(filter, description)

  /**
   * Make a Java predicate out of a Scala Function.
   */
  def predicate(filter: PropertyNode[_] => Boolean): Predicate[PropertyNode[_]] = (t: PropertyNode[_]) => filter(t)
}

object Caches {
  private val perPropertyCaches: mutable.Set[BasePerPropertyCache] =
    Collections.newSetFromMap(new ConcurrentHashMap[BasePerPropertyCache, JBoolean]).asScala

  private val sharedCaches: concurrent.Map[String, UNodeCache] = new ConcurrentHashMap[String, UNodeCache].asScala
  private val evictedButRetainedSharedCaches =
    util.Collections.newSetFromMap(new util.WeakHashMap[UNodeCache, JBoolean]).asScala
  val log: Logger = getLogger(getClass)

  def getSharedCache(name: String): Option[UNodeCache] = sharedCaches.get(name)
  private[optimus] def getPropertyCache(name: String): Option[BasePerPropertyCache] =
    perPropertyCaches.find(_.getName == name)

  private[cache] def inUse(ppc: BasePerPropertyCache) = perPropertyCaches.add(ppc)
  private[cache] def notInUse(ppc: BasePerPropertyCache) = perPropertyCaches.remove(ppc)

  /** For cleaning up between tests. Unregister anything that can be returned from Caches.allCaches, except globals */
  private[optimus] def TEST_ONLY_unregisterAllCaches(): Unit = {
    val toRemove = sharedCaches.keys.filterNot(UNodeCache.globalNames.contains)
    toRemove.foreach(unregisterSharedCache)
    perPropertyCaches.clear()
    evictedButRetainedSharedCaches.clear()
  }

  private[cache] def register(sharedCache: UNodeCache): Unit = {
    sharedCaches.put(sharedCache.getName, sharedCache) foreach { old =>
      log.warn(s"shared cache with name ${old.getName} replaced")
      // potential memory leak if the shared cache is not known. We keep a WeakReference to all UNode caches
      // and Caches.allCaches() and related will still see caches that have been replaced until
      // they are GCed
      evictedButRetainedSharedCaches.synchronized(evictedButRetainedSharedCaches += old)
    }
  }
  def unregisterSharedCache(name: String): Option[UNodeCache] = sharedCaches.remove(name)

  /**
   * provides a list of a snapshot of the caches that are in use
   * @param includeSiGlobal
   *   (default true)
   * @param includeGlobal
   *   (default true)
   * @param includeNamedCaches
   *   (default true) - include names shared caches other than global and siGlobal
   * @param includePerPropertyCaches
   *   (default true) include per property ( private) caches
   * @return
   *   a list of a snapshot of the caches
   */
  def allCaches(
      includeSiGlobal: Boolean = true,
      includeGlobal: Boolean = true,
      includeNamedCaches: Boolean = true,
      includePerPropertyCaches: Boolean = true,
      includeEvictedButRetained: Boolean = true
  ): List[NodeCCache] = {

    val shared = {
      val evictedButReachable =
        if (!includeEvictedButRetained) Seq.empty[UNodeCache]
        else {
          evictedButRetainedSharedCaches
            .synchronized {
              evictedButRetainedSharedCaches.toList
            }
            .filter(_ ne null)
        }
      evictedButReachable foreach { cache =>
        log.warn(s"Evicted shared cache is still reachable! name:'${cache.getName}'")
      }
      var shared = sharedCaches.values ++ evictedButReachable
      if (!includeGlobal) shared = shared.filter(_.getName != UNodeCache.globalCacheName)
      if (!includeSiGlobal) shared = shared.filter(_.getName != UNodeCache.globalSICacheName)
      if (!includeNamedCaches) shared = shared.filter { cache =>
        cache.getName == UNodeCache.globalCacheName ||
        cache.getName == UNodeCache.globalSICacheName
      }
      shared
    }
    val perProperty = if (includePerPropertyCaches) perPropertyCaches.toList else Nil
    (shared ++ perProperty).toList
  }

  private val ClearCachesDescription = "Caches.clearCaches"

  def approxNumCacheEntries: Int = allCaches().map(_.getSizeIndicative).sum

  // Note all cache clear should happen through this method to perform consistent logging and breadcrumbs.
  def clearCaches(
      cause: ClearCacheCause,
      includeSiGlobal: Boolean = true,
      includeGlobal: Boolean = true,
      includeNamedCaches: Boolean = true,
      includePerPropertyCaches: Boolean = true,
      invokeGCNativeRegisteredCleaners: Boolean = false,
      filter: CacheFilter = CacheFilter.None
  ): Long = {
    val numRemoved = {
      val toClear =
        allCaches(includeSiGlobal, includeGlobal, includeNamedCaches, includePerPropertyCaches)
          .filter(c => if (cause.isMemoryPressureRelated) c.isEvictOnLowMemory else c.isEvictOnOverflow)
      filter match {
        case CacheFilter.None         => toClear.map(_.clear(cause))
        case CacheFilter.Filter(f, _) => toClear.map(_.clear(CacheFilter.predicate(f), cause))
      }
    }.sum

    log.info(
      s"allowed clearCaches(cause = $cause, includeSiGlobal = $includeSiGlobal, includeGlobal = $includeGlobal, includeNamedCaches = $includeNamedCaches, includePerPropertyCaches = $includePerPropertyCaches, filterDescription = ${filter.description}) - numRemoved: $numRemoved")
    if (invokeGCNativeRegisteredCleaners) {
      log.info("clearing all callback caches")
      GCNative.invokeRegisteredCleanersThrough(LEVEL_CLEAR_ALL_CACHES_CALLBACKS)
    }

    Breadcrumbs(
      ChainedID.root,
      LogPropertiesCrumb(
        _,
        Properties.description -> ClearCachesDescription,
        Properties.clearCacheIncludeSiGlobal -> includeSiGlobal,
        Properties.clearCacheIncludeGlobal -> includeGlobal,
        Properties.clearCacheIncludeNamedCaches -> includeNamedCaches,
        Properties.clearCacheIncludePerPropertyCaches -> includePerPropertyCaches,
        Properties.clearCacheFilterDescription -> filter.description,
        Properties.numRemoved -> numRemoved
      )
    )
    GCMonitorDiagnostics.incrementCounter(GCMonitorDiagnostics.totalNumberNodesCleared, numRemoved)
    numRemoved
  }

  // clear non local caches
  def clearAllCaches(cause: ClearCacheCause): Long = {
    log.info("Clearing all non local Optimus Caches")
    clearCaches(cause, includePerPropertyCaches = false)
  }

  def clearAllCaches(cause: ClearCacheCause, includeSI: Boolean, includePropertyLocal: Boolean): Long =
    clearAllCaches(cause, includeSI, includePropertyLocal, includeNamed = true)

  def clearAllCaches(
      cause: ClearCacheCause,
      includeSI: Boolean,
      includePropertyLocal: Boolean,
      includeNamed: Boolean
  ): Long = {
    val ret = clearCaches(
      cause,
      includeSiGlobal = includeSI,
      includePerPropertyCaches = includePropertyLocal,
      includeNamedCaches = includeNamed
    )
    ret
  }

  // clear both local and non local caches
  def clearAllCachesWithLocal(cause: ClearCacheCause): Long = {
    log.info("Clearing all local and non local Optimus Caches")
    clearCaches(cause)
  }

  def clearAllCachesWithNativeMarker(cause: ClearCacheCause): Long = {
    log.info("Clearing all Optimus Caches with Native Marker")
    clearCaches(
      cause,
      includePerPropertyCaches = false,
      filter = CacheFilter(_.executionInfo().getHoldsNativeMemory(), "NativeMarker")
    )
  }

  def clearOldestForAllSSPrivateCache(ratio: Double, cause: ClearCacheCause): Long = {
    log.info("Clearing oldest for scenario stack private Caches")
    val privateCaches = sharedCaches.values.filter(_.getName.startsWith(UNodeCache.scenarioStackPrivateCachePrefix))
    privateCaches
      .filter(c => if (cause.isMemoryPressureRelated) c.isEvictOnLowMemory else c.isEvictOnOverflow)
      .map(_.clearOldest(ratio, cause).removed)
      .sum
  }

  private var schedulerCleanTask: ScheduledFuture[_] = _
  private def clearNodesWithDisposedTrackersFromCaches(): Unit = {
    def clearFn(n: NodeTask): Boolean = n.scenarioStack().tweakableListener.isDisposed
    val clearTime = System.nanoTime()
    Caches.allCaches(includeSiGlobal = false) foreach {
      _.clear(clearFn, CauseDisposeDependencyTracker)
    }
    log.info(s"cache cleanup after dispose took ${((System.nanoTime() - clearTime) / 1000) / 1000.0} ms")
  }

  def lazyClearDisposedTrackers(cacheClearMode: CacheClearMode): Unit = cacheClearMode match {
    case NoClear   =>
    case SyncClear => clearNodesWithDisposedTrackersFromCaches()
    case AsyncClear =>
      if (schedulerCleanTask != null) // If a new cleanup request arrives before previous run we cancel it
        schedulerCleanTask.cancel(false)

      log.info("Schedule cleanup")
      schedulerCleanTask = CoreAPI.optimusScheduledThreadPool.schedule(
        new Runnable { override def run(): Unit = clearNodesWithDisposedTrackersFromCaches() },
        25,
        TimeUnit.MILLISECONDS)
  }
}

sealed trait ClearCacheCause {
  val evictionReason: EvictionReason

  val isMemoryPressureRelated: Boolean = false
}

// These are good reasons
case object CauseGCNative extends ClearCacheCause {
  override val evictionReason: EvictionReason = EvictionReason.gcNative
  override val isMemoryPressureRelated: Boolean = true
}
case object CauseGCMonitor extends ClearCacheCause {
  override val evictionReason: EvictionReason = EvictionReason.gcMonitor
  override val isMemoryPressureRelated: Boolean = true
}
case object CauseWin64MemoryWatcher extends ClearCacheCause {
  override val evictionReason: EvictionReason = EvictionReason.memoryPressure
  override val isMemoryPressureRelated: Boolean = true
}
case object CauseUntrackedTweakableTweaked extends ClearCacheCause {
  override val evictionReason: EvictionReason = EvictionReason.untrackedTweaked
}
case object CauseDisposeDependencyTracker extends ClearCacheCause {
  override val evictionReason: EvictionReason = EvictionReason.depTracker
}
case object CauseDisposePrivateCache extends ClearCacheCause {
  override val evictionReason: EvictionReason = EvictionReason.privateCache
}

// These are appropriate reasons
case object CauseProfiler extends ClearCacheCause {
  override val evictionReason: EvictionReason = EvictionReason.profiler
}
case object CauseGraphMBean extends ClearCacheCause {
  override val evictionReason: EvictionReason = EvictionReason.bean
}

// Some tests it is appropriate, but they will slow down other tests where there is potential reuse
case object CauseTestCase extends ClearCacheCause {
  override val evictionReason: EvictionReason = EvictionReason.test
}

// Maybe a user should be given the choice to clear caches, but probably not
case object CauseUIHandler extends ClearCacheCause {
  override val evictionReason: EvictionReason = EvictionReason.uiHandler
}
private[optimus] case object CauseEntityResolverClosed extends ClearCacheCause {
  override val evictionReason: EvictionReason = EvictionReason.entityResolverClosed
}
private[optimus] case object MemoryLeakCheckCacheClear extends ClearCacheCause {
  override val evictionReason: EvictionReason = EvictionReason.memoryLeakChecker
}
private[optimus] case object CacheResizeCause extends ClearCacheCause {
  override val evictionReason: EvictionReason = EvictionReason.resize
}

// String typed user causes. These are often hasty bug fixes, and should be looked at more closely.
final case class CauseClearedByUser(user: String) extends ClearCacheCause {
  override def toString: String = s"ClearedByUser:$user"
  override val evictionReason: EvictionReason = EvictionReason.explicitUserCleared
}

// I'm not sure people should be encouraged to clear caches using custom code paths
case object CauseExample extends ClearCacheCause {
  override val evictionReason: EvictionReason = EvictionReason.test
}
