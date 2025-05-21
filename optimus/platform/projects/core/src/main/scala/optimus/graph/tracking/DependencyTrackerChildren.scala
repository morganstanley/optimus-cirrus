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

import optimus.graph.tracking.ttracks.Invalidators
import optimus.graph.tracking.ttracks.TTrack
import optimus.platform.EvaluationContext
import optimus.platform.Tweak
import optimus.platform.annotations.nodeLiftByName
import optimus.platform.annotations.expectingTweaks
import optimus.ui.ScenarioReference

/**
 * Manages the child DependencyTrackers of a parent DependencyTracker
 */
private[tracking] trait DependencyTrackerChildren {
  self: DependencyTracker =>

  private[this] final var childrenImpl: List[DependencyTracker] = Nil

  /** protection to changes in the children or childrenDependency */
  private final val dependencyLock = new Object
  private var childrenDependency: TTrack = _

  private def takeChildDependency(): Unit = dependencyLock.synchronized {
    if (childrenDependency == null) {
      childrenDependency = new TTrack(null)
    }
    EvaluationContext.currentNode.combineXinfo(childrenDependency)
  }

  private def fireChildDependency(): Unit = dependencyLock.synchronized {
    if (childrenDependency != null) {
      Invalidators.invalidate(childrenDependency, root, root.unbatchedTrackedNodeInvalidationObserver())
    }
    childrenDependency = null
  }

  protected def disposeChildList(): Unit = {
    childrenImpl = Nil
  }

  private def addChild(newChild: DependencyTracker): Unit = dependencyLock.synchronized {
    childrenImpl ::= newChild
    fireChildDependency()
  }

  private[tracking] def removeDisposedChildren(): Unit = dependencyLock.synchronized {
    val size = childrenImpl.size
    childrenImpl = childrenImpl.filterNot(_.isDisposed)
    if (size != childrenImpl.size) fireChildDependency()
  }

  private[optimus] def children: List[DependencyTracker] = childrenImpl
  private[optimus] def childrenWithDependency: List[DependencyTracker] = {
    takeChildDependency()
    childrenImpl
  }

  /**
   * Creates a new child tracking scenario Note: keeps track between parent and child
   */
  def newChildScenario(name: String): DependencyTracker =
    newChildScenario(name, introduceConcurrentSubtree = false, rejectTweaks = false)
  def newChildScenario(
      name: String,
      introduceConcurrentSubtree: Boolean,
      rejectTweaks: Boolean,
      tweaks: Tweak*): DependencyTracker = {
    val scenarioRef =
      if (introduceConcurrentSubtree) scenarioReference.concurrentChild(name, rejectTweaks)
      else scenarioReference.consistentChild(name, rejectTweaks)
    newChildScenario(scenarioRef, tweaks: _*)
  }

  @expectingTweaks
  @nodeLiftByName
  def newChildScenario(ref: ScenarioReference, tweaks: Tweak*): DependencyTracker = {
    val nm = ref.name
    if ((nm ne null) && root.childByName(nm, takeDependency = false).isDefined)
      throw new IllegalArgumentException("Scenario names must be unique :" + nm)
    require(ref != ScenarioReference.Dummy, s"cannot create DependencyTracker for ScenarioReference '$nm'")
    val child = new DependencyTracker(root, ref, scenarioStack, null)
    // add the child before adding the tweaks, because adding the tweaks calls getOrCreateScenario which calls back here
    addChild(child)
    root.addedChild(child)
    if (tweaks.nonEmpty) child.addTweaks(tweaks, throwOnDuplicate = true)
    child
  }

  private[tracking] def hasChild(ts: DependencyTracker): Boolean = {
    if (ts.root ne this.root)
      throw new IllegalArgumentException("no common parent if tracking scenarios are in different roots")
    if (ts.level < this.level) false
    else this eq ts.parentOrSelfAtLevel(this.level)
  }
}
