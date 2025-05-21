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
package optimus.graph.tracking

import optimus.graph.NodeKey
import optimus.graph.cache.Caches

import scala.collection.mutable

/**
 * Implementation trait which provides support for disposing a DependencyTracker correctly
 */
trait DisposalSupport {
  self: DependencyTracker =>
  import DisposalSupport._

  @volatile final private[this] var _disposed = false

  private[tracking] def assertNotDisposed(): Unit = {
    if (_disposed) throw new DependencyTrackerDisposedException(name)
  }
  final def isDisposed: Boolean = _disposed

  private[tracking] def doDispose(
      cacheClearMode: CacheClearMode,
      cause: EventCause,
      observer: TrackedNodeInvalidationObserver): Unit = {
    if (!_disposed) {
      val disposalMutableInfo = mutable.Map.empty[DependencyTracker, Map[TrackingScope[_], KeyAndTracked]]

      disposeThisAndChildren(disposalMutableInfo, cause, observer)
      this match {
        case root: DependencyTrackerRoot => DependencyTrackerRoot.removeRoot(root)
        case _                           => parent.removeDisposedChildren()
      }
      root.removedDisposedNamedChildren()

      val disposalInfo = new DisposalInformation(this, disposalMutableInfo.toMap)
      root.notifyScenarioDisposal(disposalInfo, cause)

      // do our best to ensure that any user code cant hold onto the internal data structures
      disposalInfo.dispose()

      // we need to clear from the caches explicitly as invalidation will just mark the node as invalid in the cache
      // and still used the slot in the cache used
      Caches.lazyClearDisposedTrackers(cacheClearMode)
    }
  }

  private[tracking] def disposeThisAndChildren(
      info: mutable.Map[DependencyTracker, Map[TrackingScope[_], KeyAndTracked]],
      cause: EventCause,
      observer: TrackedNodeInvalidationObserver): Unit = {
    if (!_disposed) {
      // Report final queue stats to the root BEFORE we dispose this tracker. The queue will then stop reporting
      // regular snapped stats (even though it's still reachable). If we did this after disposing, there would be a
      // brief period where the queue was not reachable but the final stats were missing, causing a drop in the
      // cumulative stats across all queues.
      if (queueOwner == this) DependencyTrackerRoot.collectDisposedQueueStats(queue)

      children foreach { c =>
        c.disposeThisAndChildren(info, cause, observer)
      }
      _disposed = true
      removeDisposedChildren()

      info(this) = allUserNodeTrackers.map(u => u.scope -> u.flat).toMap

      disposeChildList()
      tweakableTracker.disposeTweakableTracker()
      allUserNodeTrackers.foreach(_.disposeUserNodeTracker())
      disposeSnapshot(cause, observer)
    }
  }
}

object DisposalSupport {
  type KeyAndTracked = Seq[(NodeKey[_], TrackedNode[_])]
}
