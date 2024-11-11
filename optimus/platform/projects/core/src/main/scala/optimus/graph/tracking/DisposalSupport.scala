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
import scala.util.Success
import scala.util.Try

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

  private[tracking] def doDispose(clearCacheOfDisposedNodes: Boolean, immediate: Boolean, cause: EventCause): Unit = {
    if (!_disposed) {
      val disposalMutableInfo = mutable.Map.empty[DependencyTracker, KeyAndTracked]

      disposeThisAndChildren(disposalMutableInfo, cause)
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
      if (clearCacheOfDisposedNodes)
        Caches.lazyClearDisposedTrackers(immediate)
    }
  }

  private[tracking] def disposeThisAndChildren(
      info: mutable.Map[DependencyTracker, KeyAndTracked],
      cause: EventCause): Unit = {
    if (!_disposed) {
      children foreach { c =>
        c.disposeThisAndChildren(info, cause)
      }
      _disposed = true
      removeDisposedChildren()

      info(this) = userNodeTracker.flat

      disposeChildList()
      tweakableTracker.disposeTweakableTracker()
      userNodeTracker.disposeUserNodeTracker()
      disposeSnapshot(cause)
    }
  }

  private[tracking] class TSA_Dispose(clearDisposedNodes: Boolean, immediate: Boolean) extends TSA_BasicUpdateAction {
    def this(clearDisposedNodes: Boolean) = this(clearDisposedNodes, false)

    /**
     * special case is for a dispose - we are forgiving and regard a dispose of an already disposed scenario as a
     * Success NO-OP
     */
    override private[tracking] def alreadyDisposedResult: Try[Unit] = Success(())
    override protected def doUpdate(): Unit = doDispose(clearDisposedNodes, immediate, cause)
  }
}

object DisposalSupport {
  type KeyAndTracked = Seq[(NodeKey[_], TrackedNode[_])]
}
