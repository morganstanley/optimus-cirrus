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

import optimus.graph.GraphInInvalidState
import optimus.graph.NodeKey
import optimus.graph.PropertyNode
import optimus.graph.Settings
// TODO (OPTIMUS-18724): revisit default values
private[optimus] object CacheDefaults {
  val DEFAULT_SHARED_SIZE: Int = 1000

  val DEFAULT_CONCURRENCY: Int = Settings.cacheConcurrency
  val DEFAULT_BATCH_SIZE: Int = 1
  val DEFAULT_BATCH_SIZE_PADDING: Int = 1

  def sizeBasedDefaultBatchSize(size: Int): Int =
    if (size > 100000) NodeCCache.defaultCacheBatchSize
    else DEFAULT_BATCH_SIZE

  def sizeBasedDefaultBatchSizePadding(size: Int): Int =
    if (size > 10000) NodeCCache.defaultCacheBatchSizePadding
    else DEFAULT_BATCH_SIZE_PADDING

  def sizeBasedDefaultConcurrency(size: Int): Int =
    if (size > 1000) DEFAULT_CONCURRENCY
    else 1
}

private[optimus] object UNodeCache {
  val globalCacheName: String = "global"
  val globalSICacheName: String = "siGlobal"
  val globalCtorCacheName: String = "ctorGlobal"
  val globalNames: Seq[String] = Seq(globalCacheName, globalSICacheName, globalCtorCacheName)

  val scenarioStackPrivateCachePrefix: String = "privateCache"

  private[this] var _global: NodeCCache = _
  final def global: NodeCCache = _global
  final def global_=(newValue: NodeCCache): Unit = {
    if (Settings.schedulerAsserts) {
      val lookupCache = Caches.getSharedCache(newValue.getName)
      if (lookupCache.isEmpty || (lookupCache.get ne newValue))
        throw new GraphInInvalidState("Cannot reassign global cache var without registering it first")
    }
    _global = newValue
  }
  var siGlobal: NodeCCache = _
  var constructorNodeGlobal: NodeCCache = _

}
final class UNodeCache(
    name: String,
    maxSize: Int,
    val requestedConcurrency: Int,
    cacheBatchSize: Int,
    cacheBatchSizePadding: Int,
    evictOnOverflow: Boolean = true)
    extends BaseUNodeCache(
      name,
      maxSize,
      evictOnOverflow,
      requestedConcurrency,
      cacheBatchSize,
      cacheBatchSizePadding) {
  Caches.register(this)

  // this constructor is not used for global caches
  def this(name: String, maxSize: Int) = {
    this(
      name,
      maxSize,
      CacheDefaults.sizeBasedDefaultConcurrency(maxSize),
      CacheDefaults.sizeBasedDefaultBatchSize(maxSize),
      CacheDefaults.sizeBasedDefaultBatchSizePadding(maxSize),
      true
    )
  }

  /**
   * UNode based cache is recorded via Caches.register(this), as it is accessed via named ids, rather that usage count
   */
  private[optimus] def sharable: Boolean = true
}

private[cache] abstract class BasePerPropertyCache(
    name: String,
    maxSize: Int,
    evictOnOverflow: Boolean,
    requestedConcurrency: Int,
    cacheBatchSize: Int,
    cacheBatchSizePadding: Int)
    extends BaseUNodeCache(
      name,
      maxSize,
      evictOnOverflow,
      requestedConcurrency,
      cacheBatchSize,
      cacheBatchSizePadding) {
  private var inUseCount: Int = 0
  override final private[cache] def recordInUse(): Unit = synchronized {
    if ({ inUseCount += 1; inUseCount } == 1) Caches.inUse(this)
  }
  override final private[cache] def recordNotInUse(): Unit = synchronized {
    if ({ inUseCount -= 1; inUseCount } == 0) Caches.notInUse(this)
  }
  private[optimus] final def sharable: Boolean = false
}
object PerPropertyCache {
  val defaultName = "perPropertyCache"
}

/**
 * Mostly to maintain backward compatibility with existing code
 */
final class PerPropertyCache(
    name: String,
    maxSize: Int,
    requestedConcurrency: Int,
    cacheBatchSize: Int,
    cacheBatchSizePadding: Int,
    evictOnOverflow: Boolean = true)
    extends BasePerPropertyCache(
      name,
      maxSize,
      evictOnOverflow,
      requestedConcurrency,
      cacheBatchSize,
      cacheBatchSizePadding) {
  def this(name: String, maxSize: Int) = {
    this(
      name,
      maxSize,
      CacheDefaults.sizeBasedDefaultConcurrency(maxSize),
      CacheDefaults.sizeBasedDefaultBatchSize(maxSize),
      CacheDefaults.sizeBasedDefaultBatchSizePadding(maxSize),
      true
    )
  }
  def this(maxSize: Int, evictOnOverflow: Boolean) = {
    this(
      PerPropertyCache.defaultName,
      maxSize,
      CacheDefaults.sizeBasedDefaultConcurrency(maxSize),
      CacheDefaults.sizeBasedDefaultBatchSize(maxSize),
      CacheDefaults.sizeBasedDefaultBatchSizePadding(maxSize),
      evictOnOverflow
    )
  }
  def this(maxSize: Int) = {
    this(PerPropertyCache.defaultName, maxSize)
  }
}

/**
 * Experimental class (do NOT use it unless you talk to graph team SMEs first)
 */
final class PerPropertySoftCache(
    name: String,
    maxSize: Int,
    requestedConcurrency: Int,
    cacheBatchSize: Int,
    cacheBatchSizePadding: Int)
    extends BasePerPropertyCache(name, maxSize, true, requestedConcurrency, cacheBatchSize, cacheBatchSizePadding) {
  @deprecated("only used by C2 edge build and should be removed", "Nov 2017")
  def this(size: Int) = {
    this("", size, 1, 1, 1)
  }

  override protected def newNCEntry(hash: Int, value: PropertyNode[_], next: NCEntry): NCEntry =
    new NCSoftEntry(hash, value, next)
}

/**
 * Cache for property nodes with custom sizes
 */
final class PerPropertySizedCache(
    name: String,
    maxSize: Int,
    requestedConcurrency: Int,
    cacheBatchSize: Int,
    cacheBatchSizePadding: Int,
    sizeOf: NodeKey[_] => Integer)
    extends BasePerPropertyCache(name, maxSize, true, requestedConcurrency, cacheBatchSize, cacheBatchSizePadding) {

  override protected def newNCEntry(hash: Int, value: PropertyNode[_], next: NCEntry): NCEntry =
    new NCSizedEntryV(hash, value, sizeOf(value), next)
}
