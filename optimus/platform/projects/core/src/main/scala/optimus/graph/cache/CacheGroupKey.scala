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

import optimus.graph.PropertyNode
import optimus.platform.NonForwardingPluginTagKey

import java.util.concurrent.ConcurrentHashMap

/**
 * Helper class to maintain tree of group dependencies see [[optimus.graph.cache.CacheGroupKey]]
 */
//noinspection ScalaFileName
private[cache] class NCGroupedChildren(val current: NCEntryGrouped) {
  val children = new ConcurrentHashMap[NCEntryGrouped, BaseUNodeCache]()

  def add(newChild: NCEntryGrouped, cache: BaseUNodeCache): Unit = children.putIfAbsent(newChild, cache)

  def requeryAllChildren(): Unit = {
    val it = children.entrySet().iterator()
    while (it.hasNext) {
      val child = it.next()
      val ncentry = child.getKey
      if (!ncentry.inLedger) {
        child.getValue.updateLedger(ncentry)
        ncentry.groupedChildren.requeryAllChildren()
      }
    }
  }
}

/**
 * Support markGroupCached() functionality See tests in AdvancedCacheTests (groupCache and others)
 */
object CacheGroupKey extends NonForwardingPluginTagKey[NCGroupedChildren] {
  private def linkupToParent(parentGroup: NCGroupedChildren, key: NCEntryGrouped, cache: BaseUNodeCache): Unit = {
    parentGroup.add(key, cache)
  }

  /**
   *   1. top level aka root of the CacheGroupKey tree and NEW in cache (aka found.cacheUnderlying eq key)
   *   1. have a parent and NEW in cache (aka found.cacheUnderlying eq key)
   */
  def onNodeInserted(cache: BaseUNodeCache, key: PropertyNode[_], entry: NCEntry): Unit = {
    // Cast is always valid because we only get a call to this methods for property nodes
    val groupedEntry = entry.asInstanceOf[NCEntryGrouped]

    // This line has to be here before node.replace()
    val parentGroup = key.scenarioStack.findPluginTag(CacheGroupKey)

    // Prepare propagation of the NCGroupedChildren down the compute tree (if there are children markGroupCached)
    val node = groupedEntry.getValue
    val newSS = node.scenarioStack.withPluginTag(CacheGroupKey, groupedEntry.groupedChildren)
    node.replace(newSS)

    if (parentGroup.nonEmpty)
      linkupToParent(parentGroup.get, groupedEntry, cache)
  }

  /**
   *   1. top level aka root of the CacheGroupKey tree but was cached from before
   *   1. have a parent but was cached from before
   */
  def onNodeFound(cache: BaseUNodeCache, key: PropertyNode[_], entry: NCEntry): Unit = {
    entry match {
      case groupedEntry: NCEntryGrouped =>
        val parentGroup = key.scenarioStack.findPluginTag(CacheGroupKey)
        val selfGroup: NCGroupedChildren = groupedEntry.groupedChildren

        selfGroup.requeryAllChildren()
        // from the viewpoint of the found node:
        // note: this supports a -> b and c -> b (b can now be linkedUp to a and c)
        if (parentGroup.nonEmpty)
          linkupToParent(parentGroup.get, groupedEntry, cache)

      case _ => // hit from before it was marked, ignored
    }
  }
}
