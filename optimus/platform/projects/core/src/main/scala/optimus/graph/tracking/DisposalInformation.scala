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
import optimus.graph.tracking.DisposalSupport.KeyAndTracked

/**
 * information passed to a callback when a scenario is disposed Note: tracked is a var unmodified after this is
 * constructed apart from during dispose
 *
 * After a listener has been called back no reference should be made to this class of it methods, as the owning
 * DependencyTracker will [[dispose()]] this
 *
 * @param disposedTracker
 *   the scenario that was requested to be disposed
 * @param tracked
 *   internal information as to the utracks, used to service the additional methods
 */
class DisposalInformation(
    private var disposedTracker: DependencyTracker,
    private var tracked: Map[DependencyTracker, Map[TrackingScope[_], KeyAndTracked]]) {

  def topDisposedScenario: DependencyTracker = {
    checkNotDisposed()
    disposedTracker
  }

  def allScenarios: Set[DependencyTracker] = {
    checkNotDisposed()
    tracked.keySet
  }

  def trackedInScenario[S >: Null <: TrackingMemo](
      trackingScenario: DependencyTracker,
      scope: TrackingScope[S]): Iterator[(NodeKey[_], TrackedNode[_], S)] = {
    checkNotDisposed()
    tracked
      .getOrElse(trackingScenario, Map.empty)
      .getOrElse(scope, Nil)
      .iterator
      .map { case (key, uTrack) =>
        (key, uTrack, uTrack.getMemo.asInstanceOf[S])
      }
      .filter(_._3 ne null)
  }

  /**
   * called after we call to the listeners, just in case one of the listeners keeps a reference
   */
  private[tracking] def dispose(): Unit = {
    tracked = null
    disposedTracker = null
  }
  private def checkNotDisposed(): Unit = {
    assert(tracked != null, "Illegal late reference to DisposalInformation after it has been disposed")
  }
}
