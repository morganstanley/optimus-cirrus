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
package optimus.graph

import optimus.core.MonitoringBreadcrumbs
import optimus.graph.tracking.ttracks.RefCounter

import java.util.concurrent.TimeUnit
import optimus.platform._

import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.ArrayBuffer

object TweakableListener {
  // lock when you are updating the parents of TweakableListener so that you do not get into a race condition see OPTIMUS-ADR-OPTIMUS-70536-XSNoWait.md for details
  private val updateParentLock = new ReentrantLock
  private val contentionThreshold = TimeUnit.MILLISECONDS.toNanos(100)

  type SS = ScenarioStack
  private[optimus] def intersectsAndRecordParent(proposedSS: SS, cachedSS: SS, debugKey: NodeTask): Boolean = {
    var start = 0L
    try {
      if (!TweakableListener.updateParentLock.tryLock) {
        start = System.nanoTime
        TweakableListener.updateParentLock.lock()
      }

      if (start > 0) {
        val t = System.nanoTime - start
        if (t > contentionThreshold)
          MonitoringBreadcrumbs.sendXSLockContentionCrumb(t, debugKey.executionInfo().toString)
      }
      // Real functionality
      checkAndSetParent(proposedSS.tweakableListener, cachedSS.tweakableListener)
    } finally TweakableListener.updateParentLock.unlock()
  }

  private def checkAndSetParent(proposedTl: TweakableListener, cachedTl: TweakableListener): Boolean = {
    val intersects =
      if (proposedTl.getWaiterTls.isEmpty) false // Assume common case of non-recursive XS
      else {
        val visited = new ArrayBuffer[TweakableListener]()
        def containsInParents(tl: TweakableListener, cachedTl: TweakableListener): Boolean = {
          val parents = tl.getWaiterTls
          if (parents.isEmpty) return false

          tl.visited = true
          visited += tl
          val it = parents.iterator
          while (it.hasNext) {
            val ptl = it.next()
            if (ptl eq cachedTl) return true
            if (!ptl.visited && containsInParents(ptl, cachedTl)) return true
          }
          false
        }

        val r = containsInParents(proposedTl, cachedTl)
        var i = 0
        while (i < visited.size) {
          visited(i).visited = false
          i += 1
        }
        r
      }

    if (!intersects) cachedTl.recordWaiter(proposedTl)
    intersects
  }
}

// this is an abstract class rather than a trait because class methods are slightly faster to invoke than interface methods
private[optimus] abstract class TweakableListener {
  def getWaiterTls: Set[TweakableListener] = Set.empty
  // noinspection ScalaUnusedSymbol (used in overrides)
  protected def recordWaiter(parentTl: TweakableListener): Unit = {}

  /** Listener that should be used for scenario stack sensitive batchers */
  def underlyingListener: TweakableListener = this

  /** Logically a val, but to save space it's a def */
  def respondsToInvalidationsFrom: Set[TweakableListener] = Set.empty
  def isDisposed: Boolean = false

  def onOwnerCompleted(task: NodeTask): Unit = {}

  /**
   * Counts TTrackRefs that referred to GC-ed nodes.
   *
   * This is used as a heuristic to determine whether a DependencyTracker cleanup or can be skipped.
   */
  def refQ: RefCounter[NodeTask] = null

  /** Returns TL for the given block [SEE_WHEN_CLAUSE_DEPENDENCIES] */
  def withWhenTarget(givenNode: Node[_]): TweakableListener =
    new WhenTargetTweakableListener(givenNode, original = this)

  /** Returns true if further processing is needed i.e. not WhenTargetTweakableListener */
  def mergeWhenNodeTPD(task: NodeTask): Boolean = true

  /** Returns outer tracking proxy -> a node that is in the original TrackingScenario... Not in the RecordingScenario */
  def trackingProxy: NodeTask = null

  /** Callback to ScenarioStacks. Currently only TrackingScenario */
  def onTweakableNodeCompleted(node: PropertyNode[_]): Unit

  /** Callback to ScenarioStacks. TrackingScenario uses node argument and RecordingScenario doesn't */
  def onTweakableNodeUsedBy(key: PropertyNode[_], node: NodeTask): Unit

  /** Callback to ScenarioStacks. TrackingScenario uses node argument and RecordingScenario doesn't */
  def onTweakableNodeHashUsedBy(key: NodeHash, node: NodeTask): Unit

  /**
   * Callback to ScenarioStacks. ttn.trackingDepth -> to quickly filter out interest in the tweaks (Tracking &
   * Recording) ttn.key -> to identify ttrack roots (Tracking and not Recording) ttn.node -> to attach ttracks (Tracking
   * and not Recording)
   */
  def onTweakUsedBy(ttn: TweakTreeNode, node: NodeTask): Unit

  /** ScenarioStacks that retain tweakable data will pass it onto node */
  def reportTweakableUseBy(node: NodeTask): Unit

  /** Listeners interested in when-clauses are given opportunity to collect that info */
  def reportWhenClause(key: PropertyNode[_], predicate: AnyRef, res: Boolean, rt: RecordedTweakables): Unit = {}

  /** Avoid annoying casts all over */
  def recordedTweakables: RecordedTweakables = null

  /** Allow TweakableListener to add flags when constructing ScenarioStack */
  def extraFlags: Int = EvaluationState.NOT_TRANSPARENT_FOR_CACHING

  /** If tweakableListener has owner with type dependencyTrackerRoot, return its name */
  def dependencyTrackerRootName: String = "[not tracking]"

  def visited: Boolean = true
  def visited_=(v: Boolean): Unit = {}
}

private[optimus] object NoOpTweakableListener extends TweakableListener with Serializable {
  private val moniker: AnyRef = new Serializable {
    // noinspection ScalaUnusedSymbol @Serial
    def readResolve(): AnyRef = NoOpTweakableListener
  }
  // noinspection ScalaUnusedSymbol @Serial
  private def writeReplace(): AnyRef = moniker

  override def extraFlags: Int = 0
// we don't respond to invalidations from anyone, not even ourselves
  override def respondsToInvalidationsFrom: Set[TweakableListener] = Set.empty
  override def onTweakableNodeCompleted(node: PropertyNode[_]): Unit = {}
  override def onTweakableNodeUsedBy(key: PropertyNode[_], node: NodeTask): Unit = {}
  override def onTweakableNodeHashUsedBy(key: NodeHash, node: NodeTask): Unit = {}
  override def onTweakUsedBy(ttn: TweakTreeNode, node: NodeTask): Unit = {}
  override def reportTweakableUseBy(node: NodeTask): Unit = {}
}
