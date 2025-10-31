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

import optimus.graph.CancellationScope
import optimus.graph.MutableSSCacheID
import optimus.graph.NodeKey
import optimus.graph.NodeTaskInfo
import optimus.graph.SSCacheID
import optimus.graph.Settings
import optimus.graph.TweakExpander
import optimus.graph.tracking.handler.IllegalScenarioReferenceTweak
import optimus.graph.tracking.ttracks.TweakableTracker
import optimus.platform._
import optimus.platform.storable.Entity
import optimus.ui.ScenarioReference

private[tracking] sealed abstract class TweakContainer(root: DependencyTrackerRoot, scenarioRef: ScenarioReference) {
  val cacheId: SSCacheID

  /**
   * Add tweaks to the tracking scenario. This will also invalidate any snapshots, and will start an invalidation trace
   * for all targets of the new tweaks.
   *
   * @param tweaks
   *   The tweaks to add.
   * @param throwOnDuplicate
   *   Throw an exception for any duplicate tweaks.
   */
  def doAddTweaks(
      tweaks: Iterable[Tweak],
      throwOnDuplicate: Boolean,
      cause: EventCause,
      cancelScope: Option[CancellationScope] = None,
      observer: TrackedNodeInvalidationObserver): Unit

  def doRemoveTweaksFromGivenEntity(
      entity: Entity,
      keys: Seq[NodeKey[_]],
      excludes: Seq[NodeKey[_]],
      cause: EventCause,
      observer: TrackedNodeInvalidationObserver): Unit

  /**
   * Remove a set of tweaks from this DependencyTracker. This will invalidate any snapshots and will start an
   * invalidation trace for each tweak that was removed.
   *
   * @param nks
   *   NodeKeys from which to remove tweaks.
   */
  def doRemoveTweaks(nks: Iterable[NodeKey[_]], cause: EventCause, observer: TrackedNodeInvalidationObserver): Unit

  /**
   * Remove all tweaks from this DependencyTracker. This will invalidate any snapshots and will start an invalidation
   * trace for each tweak that was removed.
   */
  def doRemoveAllTweaks(cause: EventCause, observer: TrackedNodeInvalidationObserver): Unit

  protected[this] val permanentTweaks: List[Tweak] = List( // see TweakContainer.PermanentlyTweaked
    SimpleValueTweak(ScenarioReference.current$newNode)(scenarioRef),
    SimpleValueTweak(ScenarioReference.currentOverlay$newNode)(scenarioRef),
    SimpleValueTweak(DependencyTrackerRootWeakReference.current$newNode)(
      DependencyTrackerRootWeakReference.create(root))
  )

  private[optimus] def copyTweaks: Seq[Tweak] = {
    val tweaks = cacheId.allTweaksAsIterable.filterNot(permanentTweaks.contains)
    val byNames = tweaks.filter { !_.tweakTemplate.resultIsStable }
    if (byNames.nonEmpty)
      throw new UnsupportedOperationException(
        s"Can only copy byValue tweaks, not byName tweaks like: ${byNames.map(_.prettyString(false)).mkString(", ")}")
    tweaks
  }
}

private[tracking] object TweakContainer {
  val PermanentlyTweaked: Set[NodeTaskInfo] = Set(
    ScenarioReference.current$newNode.propertyInfo,
    ScenarioReference.currentOverlay$newNode.propertyInfo,
    DependencyTrackerRootWeakReference.current$newNode.propertyInfo
  )
}

/**
 * Implementation class which manages adding and removing tweaks to a MutableSSCacheID and triggering appropriate
 * invalidations of tracked nodes
 */
private[tracking] final class MutableTweakContainer(
    root: DependencyTrackerRoot,
    scenarioRef: ScenarioReference,
    scenarioStack: Option[CancellationScope] => ScenarioStack,
    tweakableTracker: TweakableTracker,
    snapshotter: SnapshotSupport
) extends TweakContainer(root, scenarioRef) {
  val cacheId: MutableSSCacheID = new MutableSSCacheID
  cacheId.putAll(permanentTweaks)

  def doAddTweaks(
      tweaks: Iterable[Tweak],
      throwOnDuplicate: Boolean,
      cause: EventCause,
      cancelScope: Option[CancellationScope],
      observer: TrackedNodeInvalidationObserver): Unit =
    if (tweaks.nonEmpty) {
      if (Settings.trackingScenarioLoggingEnabled) DependencyTrackerLogging.logDoAddTweaks(scenarioRef.name, tweaks)

      // now actually apply the tweaks and invalidate dependent nodes
      val scenario = Scenario(tweaks)
      val expanded =
        if (scenario.hasUnresolvedOrMarkerTweaks) TweakExpander.expandTweaks(scenario, scenarioStack(cancelScope))
        else scenario
      val allTweaks = expanded.topLevelTweaks
      val changedTweaks = cacheId.changesFrom(allTweaks)
      if (changedTweaks.nonEmpty) {
        cacheId.putAll(changedTweaks)
        snapshotter.invalidateSnapshot()
        OverInvalidationDetection.set(cause)
        tweakableTracker.invalidateByTweaks(changedTweaks, cause, observer)
        OverInvalidationDetection.set(null)
      }
    }

  // if keys is empty, we clear all tweaks on that entity
  override def doRemoveTweaksFromGivenEntity(
      entity: Entity,
      keys: Seq[NodeKey[_]],
      excludes: Seq[NodeKey[_]],
      cause: EventCause,
      observer: TrackedNodeInvalidationObserver): Unit = {
    // Note: This means that we *will* remove tweaks with instead-sets if keys is empty, i.e. if we are removing all
    // tweaks from this entity! This is rather difficult to fix.
    throwOnAlsoSetNodeKeyRemovals(keys)
    val keySet = keys.toSet
    val excludesSet = excludes.toSet
    doRemoveTweaksBasedOnPredicate(
      nk => nk.entity == entity && (keySet.isEmpty || keySet(nk)) && !excludesSet(nk),
      cause,
      observer)
  }

  def doRemoveTweaks(nks: Iterable[NodeKey[_]], cause: EventCause, observer: TrackedNodeInvalidationObserver): Unit = {
    throwOnAlsoSetNodeKeyRemovals(nks)
    val keySet = nks.toSet
    doRemoveTweaksBasedOnPredicate(keySet, cause, observer)
  }

  def doRemoveAllTweaks(cause: EventCause, observer: TrackedNodeInvalidationObserver): Unit = {
    // scenarioStack may change so invalidate the snapshot
    snapshotter.invalidateSnapshot()

    val allTweaks = cacheId.allTweaksAsIterable
    val toInvalidate = allTweaks.filterNot(n => TweakContainer.PermanentlyTweaked(n.target.propertyInfo))
    tweakableTracker.invalidateByTweaks(toInvalidate, cause, observer)

    cacheId.clearTweaks()
    cacheId.putAll(permanentTweaks)
  }

  private def doRemoveTweaksBasedOnPredicate(
      predicate: NodeKey[_] => Boolean,
      cause: EventCause,
      observer: TrackedNodeInvalidationObserver): Unit = {
    val removed = {
      val removed = Seq.newBuilder[Tweak]
      cacheId.removeTweaks { tweak =>
        tweak.target.hashKey match {
          case nk: NodeKey[_] if predicate(nk) =>
            removed += tweak
            true
          case _ => false
        }
      }
      removed.result()
    }

    if (removed.nonEmpty) {
      snapshotter.invalidateSnapshot()
      tweakableTracker.invalidateByTweaks(removed, cause, observer)
    }
  }

  private def throwOnAlsoSetNodeKeyRemovals(iter: Iterable[NodeKey[_]]): Unit = {
    iter.find(_.propertyInfo.hasTweakHandler).foreach { nk =>
      throw new UnsupportedOperationException(
        s"Tweak removal is not currently supported for nodes with tweak handlers (also known as 'also-sets' or 'instead-sets'), in this case ${nk.tidyKey.nodeName}")
    }
  }
}

/**
 * An implementation of [[TweakContainer]] which does not allow tweaks to be added; created by passing `rejectTweaks =
 * true` to [[ScenarioReference#*Child]].
 */
private[tracking] final class ImmutableTweakContainer(
    root: DependencyTrackerRoot,
    scenarioRef: ScenarioReference
) extends TweakContainer(root, scenarioRef) {
  val cacheId = { val mid = new MutableSSCacheID(); mid.putAll(permanentTweaks); mid.dup }
  override def doAddTweaks(
      tweaks: Iterable[Tweak],
      throwOnDuplicate: Boolean,
      cause: EventCause,
      cancelScope: Option[CancellationScope],
      observer: TrackedNodeInvalidationObserver) =
    throw new IllegalScenarioReferenceTweak(scenarioRef, tweaks.toSeq)

  override def doRemoveTweaks(
      nks: Iterable[NodeKey[_]],
      cause: EventCause,
      observer: TrackedNodeInvalidationObserver): Unit = {}
  override def doRemoveAllTweaks(cause: EventCause, observer: TrackedNodeInvalidationObserver): Unit = {}
  override def doRemoveTweaksFromGivenEntity(
      entity: Entity,
      keys: Seq[NodeKey[_]],
      excludes: Seq[NodeKey[_]],
      cause: EventCause,
      observer: TrackedNodeInvalidationObserver): Unit = {}
}
