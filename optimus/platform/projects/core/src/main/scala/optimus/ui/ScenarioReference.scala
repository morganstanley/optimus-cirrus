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
package optimus.ui

import java.util.UUID
import optimus.core.MonitoringBreadcrumbs
import optimus.graph.AlreadyCompletedPropertyNode
import optimus.graph.Node
import optimus.graph.NodeTaskInfo
import optimus.graph.PropertyInfo
import optimus.graph.PropertyNode
import optimus.graph.tracking.DependencyTracker
import optimus.graph.tracking.DependencyTrackerRoot
import optimus.graph.tracking.SnapshotScenarioStack
import optimus.graph.tracking.ttracks.TweakableTracker
import optimus.platform._
import optimus.platform.annotations.nodeSync
import optimus.platform.storable.FakeModuleEntity

import scala.annotation.tailrec

/**
 * A by-name reference to a DependencyTracker.
 *
 * @param name
 *   Name of the DependencyTracker to reference.
 * @param parent
 *   The parent reference.
 */
final class ScenarioReference private (
    val name: String,
    val parent: Option[ScenarioReference],
    val introduceConcurrentSubtree: Boolean,
    val rejectTweaks: Boolean
) extends Serializable {

  override lazy val hashCode: Int =
    ((name.## * introduceConcurrentSubtree.##) + parent.##) * rejectTweaks.##

  override def equals(obj: Any): Boolean = obj match {
    case other: ScenarioReference =>
      (this eq other) || (
        other.name == this.name
          && other.parent == this.parent
          && other.introduceConcurrentSubtree == this.introduceConcurrentSubtree
          && other.rejectTweaks == this.rejectTweaks
      )
    case _ => false
  }

  override def toString: String = {
    val separator = if (introduceConcurrentSubtree) "//" else "/"
    s"${parent.getOrElse("")}$separator$name"
  }

  /** the depth of this reference, starting with 0 for root, 1 for child of root and so forth */
  val depth: Int = parent.map(_.depth + 1).getOrElse(0)

  private[optimus] def getTracker: DependencyTracker =
    DependencyTracker.getDependencyTrackerRoot.getScenarioOrThrow(this)

  def atDepth(depth: Int): ScenarioReference = {
    require(depth <= this.depth, s"depth parameter $depth must be <= the current depth ${this.depth}")
    require(depth >= 0, s"depth parameter $depth cannot be less than 0")
    @tailrec def findAtDepth(ref: ScenarioReference): ScenarioReference = {
      if (depth == ref.depth) ref
      else findAtDepth(ref.parent.get)
    }
    findAtDepth(this)
  }

  def isSelfOrChildOf(ref: ScenarioReference): Boolean = {
    if (ref.depth > this.depth) false
    else atDepth(ref.depth) == ref
  }
  def isChildOf(ref: ScenarioReference): Boolean = {
    if (ref.depth >= this.depth) false
    else atDepth(ref.depth) == ref
  }

  /** returns true if 'this' is the same as ref, or is any ancestor of ref (ie, not only direct parent) */
  def isSelfOrParentOf(ref: ScenarioReference): Boolean = {
    if (ref.depth < this.depth) false
    else this == ref.atDepth(this.depth)
  }

  /** returns true if 'this' is any ancestor of ref (ie, not only direct parent) */
  def isParentOf(ref: ScenarioReference): Boolean = {
    if (ref.depth <= this.depth) false
    else this == ref.atDepth(this.depth)
  }

  /** returns true if 'this' is a child within the consistent subtree of ancestor */
  def isConsistentDescendantOf(ancestor: ScenarioReference): Boolean =
    isChildOf(ancestor) && rootOfConsistentSubtree == ancestor.rootOfConsistentSubtree

  // all prefixes of this path (starting with root and ending with this path)
  // e.g. Seq(//root, //root/a, //root/a/b, //root/a/b/c)
  def prefixes: collection.Seq[ScenarioReference] = prefixesReversed.reverse

  // it's easier to build it in reverse...
  private def prefixesReversed: List[ScenarioReference] = this :: parent.map(_.prefixesReversed).getOrElse(Nil)

  private[optimus] def shareParentQueue = !introduceConcurrentSubtree

  // create a child and specify that actions on this child must run on a shared queue
  def consistentChild(name: String, rejectTweaks: Boolean = false): ScenarioReference =
    new ScenarioReference(name, Some(this), introduceConcurrentSubtree = false, rejectTweaks)

  // specify that actions on this child can run concurrently on a new queue
  def concurrentChild(name: String, rejectTweaks: Boolean = false): ScenarioReference =
    new ScenarioReference(name, Some(this), introduceConcurrentSubtree = true, rejectTweaks)

  // compare paths, ignoring concurrency mode
  def equalsIgnoringConcurrency(ref: ScenarioReference): Boolean = name == ref.name && parent == ref.parent

  @tailrec private[optimus] def rootOfConsistentSubtree: ScenarioReference = {
    if (parent.isEmpty) this
    else if (introduceConcurrentSubtree) this
    else parent.get.rootOfConsistentSubtree // if we're neither root nor dummy, we have a parent
  }
}

import ScenarioReferencePropertyHelper._
object ScenarioReference
    extends FakeModuleEntity(currentScenarioProp :: currentOverlayScenarioProp :: currentSnapshotScenarioProp :: Nil) {

  final case class ScenarioReferenceState(private[optimus] val tweaks: Seq[Tweak]) {
    def targetAt(targetRef: ScenarioReference): ApplySnapshotTweakGesture =
      ApplySnapshotTweakGesture(targetRef, ScenarioReferenceState(tweaks))
  }

  def tweaksInCurrentScenarioRef: ScenarioReferenceState = {
    val ss = EvaluationContext.scenarioStack
    // check that the scenario is not transitively cached (because the result depends on the state of
    // all potential tweakables, whether or not they are currently tweaked, and we don't track that dependency
    // at all) AND has the same cacheID (because if we're in a different scenario, e.g. a given block,
    // it could be confusing that those tweaks are not included in the result)
    val inRawHandle = ss.tweakableListener match {
      case tt: TweakableTracker => sameCacheId(ss, tt.owner.nc_scenarioStack) && !ss.cachedTransitively
      case _                    => false
    }
    if (!(ss.isTrackingIndividualTweakUsage && inRawHandle))
      throw new UnsupportedOperationException("Can only call this function in an @handle not in given block or node!")

    ScenarioReferenceState(ScenarioReference.current.getTracker.tweakContainer.copyTweaks)
  }

  private def sameCacheId(ss1: ScenarioStack, ss2: ScenarioStack): Boolean = ss1._cacheID eq ss2._cacheID

  def newAnonymousScenario(parent: ScenarioReference): ScenarioReference =
    parent.consistentChild("_AnonScenario_" + UUID.randomUUID().toString)

  val Root = new ScenarioReference(
    DependencyTrackerRoot.defaultRootName,
    None,
    introduceConcurrentSubtree = true,
    rejectTweaks = false)
  private[optimus] val Dummy =
    new ScenarioReference("<not in tracking scenario>", None, introduceConcurrentSubtree = false, rejectTweaks = false)

  /**
   * Returns the currentScenario reference if we are in a TrackingScenarioStack, or a child or snapshot thereof, else
   * returns DummyScenarioReference if we are not.
   */
  @nodeSync
  def current: ScenarioReference = forStack
  def current$queued: Node[ScenarioReference] = {
    MonitoringBreadcrumbs.sendCurrentScenarioReferenceCrumb()
    current$newNode.lookupAndEnqueue
  }

  // note that this node is tweaked by DependencyTracker initialization to the appropriate value. It is reset to the
  // original entry-point scenario when we enter a givenOverlay block (optimus.platform.AdvancedUtils.correctCurrentScenario)
  // so that the current scenario still refers to the one the handler widget is bound to in UI
  private[optimus] val current$newNode: PropertyNode[ScenarioReference] =
    new AlreadyCompletedPropertyNode[ScenarioReference](Dummy, this, currentScenarioProp)

  // This node is tweaked by DependencyTracker initialization and keeps track of the scenario we are in if there is an
  // overlay. It is used when constructing the overlay SS to find all parents of the overlay
  private[optimus] val currentOverlay$newNode: PropertyNode[ScenarioReference] =
    new AlreadyCompletedPropertyNode[ScenarioReference](Dummy, this, currentOverlayScenarioProp)

  private[optimus] val currentSnapshot$newNode: PropertyNode[SnapshotScenarioStack] =
    new AlreadyCompletedPropertyNode[SnapshotScenarioStack](
      SnapshotScenarioStack.Dummy,
      this,
      currentSnapshotScenarioProp)

  // note that we handle the case of null scenarioStack in case we are called by off-graph application init code
  private[optimus] def forStack: ScenarioReference = {
    val scenarioStack = EvaluationContext.scenarioStackOrNull
    if (scenarioStack eq null) Dummy else current$newNode.lookupAndGet
  }

  private[optimus] def forOverlayStack(scenarioStack: ScenarioStack): ScenarioReference =
    if (scenarioStack eq null) Dummy else scenarioStack.getNode(currentOverlay$newNode).get

  private[optimus] def activeScenarios: collection.Seq[String] = activeScenarioReferences.map(_.name)
  def activeScenarioReferences: collection.Seq[ScenarioReference] =
    DependencyTracker.getDependencyTrackerRoot.getScenarioReferences

  /**
   * return the common parent if it is a meaningful operation. If it is not meaningful then throw an exception it is not
   * a meaningful question if either `ref1` or `ref2` are not Root or a child of root `ref1` and `ref2` are inconsistent
   * \- they have common names at the same level, but have different consistency
   */
  private[optimus] def commonParent(ref1: ScenarioReference, ref2: ScenarioReference): ScenarioReference = {

    @tailrec def commonParentRec(sr1: ScenarioReference, sr2: ScenarioReference): ScenarioReference = {
      if (sr1 == sr2) sr1
      else if (sr1.equalsIgnoringConcurrency(sr2))
        throw new IllegalArgumentException(
          s"the common parent is inconsistent - $sr1 and $sr2 have different concurrency at this level")
      else commonParentRec(sr1.parent.get, sr2.parent.get)
    }

    require(ref1.atDepth(0) eq ScenarioReference.Root, s"'$ref1' is not a child of Root")
    require(ref2.atDepth(0) eq ScenarioReference.Root, s"'$ref2' is not a child of Root")

    val commonDepth = Math.min(ref1.depth, ref2.depth)
    commonParentRec(ref1.atDepth(commonDepth), ref2.atDepth(commonDepth))
  }

}

private[optimus] object ScenarioReferenceTrackingHelper {
  // note that properties marked with these flags are marked as TWEAKABLE so that tweaks will be looked up, but also as
  // DONT_TRACK_FOR_INVALIDATION since we never change the tweaked value and don't want the expense of a huge ttrack graph
  val flags: Long =
    NodeTaskInfo.DONT_CACHE | NodeTaskInfo.DONT_TRACK_FOR_INVALIDATION | NodeTaskInfo.TWEAKABLE | NodeTaskInfo.PROFILER_INTERNAL
}

private[optimus] object ScenarioReferencePropertyHelper {
  val currentScenarioProp = new PropertyInfo("currentScenario", ScenarioReferenceTrackingHelper.flags)
  val currentOverlayScenarioProp = new PropertyInfo("currentOverlayScenario", ScenarioReferenceTrackingHelper.flags)
  val currentSnapshotScenarioProp = new PropertyInfo("currentSnapshotScenario", ScenarioReferenceTrackingHelper.flags)
}

trait ScenarioOps {
  final val Root = ScenarioReference.Root
  implicit def scenarioRef2Name(r: ScenarioReference): String = r.name
}
