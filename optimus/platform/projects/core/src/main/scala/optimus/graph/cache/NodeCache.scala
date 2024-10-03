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

import optimus.graph._

object NodeCache {
  type OGSC = OGSchedulerContext
  type PN[T] = PropertyNode[T]
  type NTI = NodeTaskInfo

  def createPerPropertyCache(maxSize: Int) = new PerPropertyCache(maxSize)

  def createCacheControlConcurrencyAndOverflow(maxSize: Int, concurrent: Boolean = true, evict: Boolean = false) =
    new PerPropertyCache(
      PerPropertyCache.defaultName,
      maxSize,
      if (concurrent) CacheDefaults.DEFAULT_CONCURRENCY else 1,
      CacheDefaults.DEFAULT_BATCH_SIZE,
      CacheDefaults.DEFAULT_BATCH_SIZE_PADDING,
      evict
    )

  /**
   * Look up a node in the Optimus caches, inserting it if it is absent.
   *
   * This function has all the caching logic in it. The scenario stack is used to extract information about private
   * caches and the evaluation context is used for profiling information. They can both be null: constructor nodes (case
   * class Foo @node() (...)) can be created outside of graph, with a null EC.
   *
   * While it might be convenient to use one of the other methods on this object, you should make sure that the private
   * cache, the SI and pinfo custom cache are all correctly obeyed.
   */
  def lookupAndInsert[R](info: NTI, pnode: PN[R], privateCache: PrivateCache, ec: OGSC): PN[R] = {
    if (privateCache ne null) {
      val found = {
        // We always insert in this private cache, but we try to read from other caches first. We read in order from
        // the closest cache scopes to the outermost, and then maybe read from global caches.
        var found: PN[R] = null
        var current = privateCache.readFrom // start at parent of current

        while ((found eq null) && (current ne null)) {
          val cache = current match {
            case pc: PrivateCache =>
              current = pc.readFrom
              pc.cache
            case PrivateCache.NoOtherCache =>
              current = null
              null
            case PrivateCache.GlobalCaches =>
              current = null
              getCacheForInfo(info)
          }

          found = if (cache ne null) cache.getIfPresent(info, pnode, null) else null
        }

        found
      }
      if (found == null) NodeCache.cacheLookup(privateCache.cache, info, pnode, ec) else found
    } else NodeCache.cacheLookup(getCacheForInfo(info), info, pnode, ec)
  }

  /** Lookup in cache with insert */
  private def cacheLookup[R](cache: NodeCCache, info: NTI, candidate: PN[R], ec: OGSC): PN[R] = {
    val startTime = OGTrace.observer.lookupStart()
    val rnode = cache.putIfAbsent(info, candidate, ec)
    val lt = OGLocalTables.getOrAcquire(ec)
    OGTrace.lookupEnd(lt, startTime, candidate, rnode)
    lt.release()
    rnode
  }

  private def getCacheForInfo(info: NTI): NodeCCache = {
    val customCache = info.customCache()
    if (customCache ne null) customCache
    else if (info.isScenarioIndependent) UNodeCache.siGlobal
    else UNodeCache.global
  }
}
