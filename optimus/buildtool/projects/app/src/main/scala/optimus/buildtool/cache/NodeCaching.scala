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
package optimus.buildtool.cache

import optimus.graph.Settings
import optimus.graph.cache.NodeCCache
import optimus.graph.cache.UNodeCache

object NodeCaching {
  // custom cache to ensure that the most important nodes are not evicted (if they are, it can cause redundant
  // recompilation during a full build or incorrect hashing)
  private[buildtool] val reallyBigCache = new UNodeCache(
    name = "CacheThatShouldNotEvictAnythingEver",
    maxSize = 50000,
    requestedConcurrency = Settings.cacheConcurrency,
    cacheBatchSize = NodeCCache.defaultCacheBatchSize,
    cacheBatchSizePadding = NodeCCache.defaultCacheBatchSizePadding
  )

  // custom cache to ensure that nodes that would be undesirable to evaluate more than necessary are not evicted
  private[buildtool] val optimizerCache = new UNodeCache(
    name = "OptimizerCache",
    maxSize = 10000,
    requestedConcurrency = Settings.cacheConcurrency,
    cacheBatchSize = NodeCCache.defaultCacheBatchSize,
    cacheBatchSizePadding = NodeCCache.defaultCacheBatchSizePadding
  )
}
