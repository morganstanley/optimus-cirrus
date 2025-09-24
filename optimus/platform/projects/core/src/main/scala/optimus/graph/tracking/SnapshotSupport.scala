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

import optimus.platform.Scenario
import optimus.platform.ScenarioStack
import optimus.platform.SimpleValueTweak
import optimus.platform.inputs.loaders.FrozenNodeInputMap
import optimus.platform.util.PrettyStringBuilder
import optimus.ui.ScenarioReference

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.MurmurHash3

/**
 * Implementation and API for "snapshot" scenario stacks which are a frozen, non-tracking view of a DependencyTracker's
 * mutable scenario state.
 */
trait SnapshotSupport {
  self: DependencyTracker =>

  // will be added to TSQueue when @handle def has an InBackground step
  private[tracking] class TSA_SnapshotScenario
      extends DependencyTrackerActionEvaluate(snapshotScenarioStack _)
      with InScenarioAction

  // Any race condition is eventually consistent, and this is quick anyway
  @volatile private var _asBasicScenarioStack: ScenarioStack = _
  @volatile private var _snapshotScenarioStack: SnapshotScenarioStack = _

  /**
   * Snapshot the state of the subtree of tracking scenarios beneath the consistent tracking root, returning a basic
   * ScenarioStack, containing a tweak holding the value of the SnapshotScenarioStack representing the tree. Successive
   * calls to this without any intervening addition or removal of tweaks will return a cached stack (so the object will
   * be shared).
   */
  private[tracking] def snapshotScenarioStack: ScenarioStack = {
    snapshotScenarioStack(root.getScenarioOrThrow(scenarioReference.rootOfConsistentSubtree))
  }

  private[tracking] def snapshotScenarioStack(consistentRoot: DependencyTracker): ScenarioStack = {
    var snapshot = _asBasicScenarioStack
    if (snapshot eq null) {
      this.synchronized {
        snapshot = _asBasicScenarioStack
        if (snapshot eq null) {
          snapshot = self.scenarioStack.asBasicScenarioStack // we cache the conversion to basic SS
          _asBasicScenarioStack = snapshot
        }
      }
    }
    val sss = consistentRoot.snapshotConsistentSubtree // snapshot from consistent root in case overlay runs in snapshot
    val currentSnapshotTweak = SimpleValueTweak(ScenarioReference.currentSnapshot$newNode)(sss)
    snapshot.createChild(Scenario(currentSnapshotTweak), classOf[SnapshotScenarioStack] /* id for debugger */ )
  }

  private[tracking] def snapshotConsistentSubtree: SnapshotScenarioStack = {
    // Caching partly to avoid cost of building, but also to allow possibility of fast reference equality. This matters
    // because the SnapshotScenarioStack ends up in a tweak which will be compared for equality during SSCacheID lookup
    var snapshot = _snapshotScenarioStack
    if (snapshot eq null) {
      this.synchronized {
        snapshot = _snapshotScenarioStack
        if (snapshot eq null) {
          snapshot = buildConsistentSubtree
          _snapshotScenarioStack = snapshot
        }
      }
    }
    snapshot
  }

  // capture snapshots of our whole consistent subtree of scenarios in case they are needed for givenOverlay/evaluateIn
  private def buildConsistentSubtree: SnapshotScenarioStack = {
    val childSnapshots =
      if (children.isEmpty) SnapshotScenarioStack.EmptyChildren
      else {
        val b = mutable.ArrayBuffer[SnapshotScenarioStack]()
        val childIt = children.iterator
        while (childIt.hasNext) {
          val child = childIt.next()
          // snapshot consistent children as they can be overlayed
          if (!child.scenarioReference.introduceConcurrentSubtree) b += child.snapshotConsistentSubtree
        }
        b.toArray
      }
    val scenario = scenarioStack.nestScenariosUpTo(parentScenarioStack)
    val tags = scenarioStack.siParams.nodeInputs.freeze
    SnapshotScenarioStack(scenarioReference, scenario, childSnapshots, tags)
  }

  /**
   * Invalidate the scenario stack on this scenario as well as on all affected ancestors and descendants.
   * This should be called when tweaks are added or removed.
   */
  private[tracking] def invalidateSnapshot(): Unit = {
    invalidateChildSnapshots()
    invalidateParentSnapshots()
  }

  private[tracking] def invalidateChildSnapshots(): Unit = {
    // the basic scenarioStack depends on tweaks in all parents, so we must invalidate cache in all our transitive
    // children
    _asBasicScenarioStack = null
    children.foreach(_.invalidateChildSnapshots())
  }

  private[tracking] def invalidateParentSnapshots(): Unit = {
    // the snapshotScenarioStack depends on tweaks in _consistent_ children, so we must invalidate in our _consistent_
    // parents
    _snapshotScenarioStack = null
    // if consistent with respect to our parent, it is consistent wrt. us
    if (!scenarioReference.introduceConcurrentSubtree) {
      parentDependencyTracker.foreach(_.invalidateParentSnapshots())
    }
  }

  private[tracking] def disposeSnapshot(cause: EventCause, observer: TrackedNodeInvalidationObserver): Unit = {
    _asBasicScenarioStack = null
    _snapshotScenarioStack = null
    tweakContainer.doRemoveTweaks(Seq(ScenarioReference.currentSnapshot$newNode), cause, observer)
  }
}

/** Representation of the (frozen) tracking scenario tree structure, from the consistent root of the tree */
final case class SnapshotScenarioStack(
    ref: ScenarioReference,
    scenario: Scenario,
    children: Array[SnapshotScenarioStack],
    nodeInputs: FrozenNodeInputMap) {

  /** always called on the root (of consistent subtree) */
  private[optimus] def overlay(baseSS: ScenarioStack, overlayRef: ScenarioReference): ScenarioStack = {
    val scenarios = ArrayBuffer[SnapshotScenarioStack]() // collect the path to common root of baseRef and overlayRef
    val presentScenarioRefs = baseSS.scenarioReferences
    var done = false
    var found = false
    def visit(sss: SnapshotScenarioStack): Unit = {
      if (sss.ref == overlayRef)
        found = true
      else {
        var i = 0
        while (i < sss.children.length && !found) {
          val child = sss.children(i)
          visit(child)
          if (found) {
            done = done || presentScenarioRefs.contains(child.ref)
            if (!done)
              scenarios += child
          }
          i += 1
        }
      }
    }
    visit(this)
    if (!found) throw new IllegalArgumentException(s"Trying to overlay $overlayRef but it wasn't captured")

    var i = scenarios.length - 1
    var curr = baseSS
    while (i >= 0) {
      val overlayScenario = scenarios(i)
      curr = curr.withOverlay(overlayScenario.scenario, overlayScenario.nodeInputs, overlayScenario.ref)
      i -= 1
    }
    curr
  }

  private def findChild(scenRef: ScenarioReference): SnapshotScenarioStack =
    // should only ever be one child that matches
    children.find(sss => scenRef.isSelfOrChildOf(sss.ref)).orNull

  /**
   * applies the scenarios from current snapshot up until child scenario ref and gives you the SnapshotScenarioStack of
   * child (if child isn't found then it returns (null, null))
   */
  private[optimus] def nestScenariosUpTo(scenRef: ScenarioReference): (Scenario, SnapshotScenarioStack) = {
    var scen = scenario
    var currentSnap = this
    while (currentSnap.ref != scenRef) {
      currentSnap = currentSnap.findChild(scenRef)
      if (currentSnap == null) return (null, null)
      scen = scen.nest(currentSnap.scenario)
    }
    (scen, currentSnap)
  }

  def prettyString: String = {
    def visit(sss: SnapshotScenarioStack, sb: PrettyStringBuilder): Unit = {
      sb.append(sss.ref.toString).append(" ").append("[")
      val len = sss.children.length
      var i = len - 1
      while (i > -1) {
        val child = sss.children(i)
        if (i == len - 1)
          sb.appendln("")
        sb.indent()
        visit(child, sb)
        sb.unIndent()
        i -= 1
      }
      sb.appendln("]")
    }
    val sb = new PrettyStringBuilder
    visit(this, sb)
    sb.toString()
  }

  /** note that we don't include pluginTags here (they should not affect node results or caching) */
  override def equals(obj: Any): Boolean = obj match {
    case s: SnapshotScenarioStack =>
      (this eq s) || (s.ref == ref && s.scenario == scenario && s.children.sameElements(children))
    case _ => false
  }

  override def hashCode(): Int = ref.hashCode * 17 + scenario.hashCode + MurmurHash3.unorderedHash(children)

  override def toString: String = {
    val childrenStr = if (children.nonEmpty) s" and ${children.length} immediate children" else ""
    s"Snapshot($ref$childrenStr)"
  }
}

object SnapshotScenarioStack {
  private[tracking] val EmptyChildren = new Array[SnapshotScenarioStack](0)
  val Dummy: SnapshotScenarioStack = SnapshotScenarioStack(ScenarioReference.Dummy, null, EmptyChildren, null)

  private[optimus] def current(ss: ScenarioStack): SnapshotScenarioStack = {
    val n = ScenarioReference.currentSnapshot$newNode
    // No Tracking is valid here because there are no when clauses for currentSnapshot
    val snapshotTweak = ss.getTweakNoWhenClauseTracking(n)
    if (snapshotTweak eq null)
      throw new IllegalArgumentException("Cannot use overlay in non-tracking scenario (unless in snapshot)")
    snapshotTweak.tweakValue.asInstanceOf[SnapshotScenarioStack]
  }
}
