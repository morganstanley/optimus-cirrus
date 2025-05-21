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

import java.util.function.Consumer
import optimus.graph.NodeTaskInfo
import optimus.graph.PropertyNode
import optimus.graph.diagnostics.EvictionReason
import optimus.graph.{PropertyNode => PN}
import optimus.platform.EvaluationQueue

import java.util.function.Predicate

final case class CleanupStats(removed: Long, remaining: Long) {
  def +(other: CleanupStats): CleanupStats = CleanupStats(removed + other.removed, remaining + other.remaining)
}

object NodeCCache {
  // Per ledger batch size
  val defaultCacheBatchSize = 80
  val defaultCacheBatchSizePadding = 80
}

/**
 * Make sure that this is the only interface to node cache left standing around
 */
abstract class NodeCCache {

  /** This is for profile display only */
  def getName: String = "auto" + System.identityHashCode(this)
  def setName(name: String): Unit = {}

  /**
   * If you are thinking about using this PLEASE consider concurrency - this is NOT reliable and depends on requested
   * cache concurrency, which in turn depends on number of threads
   */
  def getSize: Int = Integer.MIN_VALUE

  def getSizeIndicative: Int = Integer.MIN_VALUE
  def getCountersSnapshot: CacheCountersSnapshot = CacheCountersSnapshot.Empty

  /** Cache size limit if available */
  def getMaxSize: Int = Integer.MIN_VALUE
  def setMaxSize(newMaxSize: Int): Unit = {}

  /** Cache size actually used */
  def profMaxSizeUsed: Int = Integer.MIN_VALUE
  def profLockContentionTime: Long = Long.MinValue
  def profLockNumAttempts: Long = Integer.MIN_VALUE
  def profCacheBatchSize: Int = CacheDefaults.DEFAULT_BATCH_SIZE
  def profCacheBatchSizePadding: Int = CacheDefaults.DEFAULT_BATCH_SIZE_PADDING
  def profNumEvictions(reason: EvictionReason): Long = Integer.MIN_VALUE
  def getCacheConcurrency: Int = 0
  def isEvictOnOverflow: Boolean = false
  def isEvictOnLowMemory: Boolean = false

  /** For scoped caches that temporarily decrease the size of the global cache, restore the count */
  def resetSizeReporting(): Unit = {}

  /** Clear oldest items */
  def clearOldest(ratio: Double, cause: ClearCacheCause): CleanupStats = CleanupStats(0, 0)

  /** Clear entire cache */
  def clear(cause: ClearCacheCause): Long

  /** Update stats */
  def updateStats(): Unit = {}

  def foreach(consumer: Consumer[PropertyNode[_]]): Unit

  /** Verify that cache obeys some invariants. This is very expensive! Throws if the invariants are not satisfied. */
  def verifyInvariants(): Unit = {}

  /** Remove items that match filter */
  def clear(filter: Predicate[PropertyNode[_]], cause: ClearCacheCause): Long = 0

  final private[cache] def putIfAbsent[T](info: NodeTaskInfo, key: PN[T], eq: EvaluationQueue): PN[T] = {
    putIfAbsent(info.cachePolicy(), key, eq)
  }

  /**
   * Looks up the item in the cache and returns it if found (updating the age) Else caches the node evicting the oldest
   * item Note: EvaluationQueue is used for OGTrace
   */
  private[cache] def putIfAbsent[T](policy: NCPolicy, key: PropertyNode[T], eq: EvaluationQueue): PropertyNode[T]

  /**
   * Looks up the item in the cache and returns it if found (updating the age) Else returns null Note: it takes scenario
   * stack as the key is not always setup to have the correct scenario stack Note: EvaluationQueue is used for OGTrace
   */
  def getIfPresent[T](info: NodeTaskInfo, key: PropertyNode[T], eq: EvaluationQueue): PropertyNode[T]

  /**
   * record that a cache is use on a specific property
   */
  private[cache] def recordInUse(): Unit = {}

  /**
   * record that a cache is no longer used on a specific property
   */
  private[cache] def recordNotInUse(): Unit = {}

  /**
   * the batch size used in the cache See the implementation notes in [[NodeCacheWithLRU]]
   */
  private[optimus] def cacheBatchSize: Int = 0

  /**
   * the batch padding size See the implementation notes in [[NodeCacheWithLRU]]
   */
  private[optimus] def cacheBatchSizePadding: Int = 0

  // indicates if the cache is a shared cache or a per property cache
  private[optimus] def sharable: Boolean = true
}
