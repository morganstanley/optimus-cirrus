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

import optimus.graph.NodeKey
import optimus.graph.tracking.DependencyTrackerBatchUpdater
import optimus.graph.tracking.RootEventCause
import optimus.platform._
import optimus.platform.annotations.handle
import optimus.platform.storable.Entity
import optimus.ui.ScenarioReference.ScenarioReferenceState

final case class GestureEventCause(override val cause: String) extends RootEventCause
object GestureEventCause {
  def createCause(gesture: Gesture) = GestureEventCause(gesture.getClass.getSimpleName)
}

trait Gesture {
  private[optimus] def allTargetedScenarioReferences(current: ScenarioReference): Set[ScenarioReference] =
    Set(current)
}

trait DependencyTrackerUpdatingGesture extends Gesture {
  private[optimus] def doGesture(updater: DependencyTrackerBatchUpdater): Unit
}

final case class ClearEntityInstanceTweaksGesture private (
    scenario: ScenarioReference,
    entityInstance: Entity,
    nodes: Option[Traversable[NodeKey[_]]] = None,
    excludes: Option[Traversable[NodeKey[_]]] = None)
    extends DependencyTrackerUpdatingGesture {
  override private[optimus] def allTargetedScenarioReferences(current: ScenarioReference) = Set(scenario)

  override private[optimus] def doGesture(updater: DependencyTrackerBatchUpdater): Unit =
    updater
      .updaterFor(scenario)
      .removeTweaksForEntityInstanceWithKeysImmediate(
        entityInstance,
        nodes.getOrElse(Seq.empty[NodeKey[_]]).toSeq,
        excludes.getOrElse(Seq.empty[NodeKey[_]]).toSeq)

}
object ClearEntityInstanceTweaksGesture {
  @handle def apply(entityInstance: Entity): ClearEntityInstanceTweaksGesture =
    new ClearEntityInstanceTweaksGesture(ScenarioReference.current, entityInstance)

  // TODO (OPTIMUS-72542): If nodeKeys is empty, then all tweaks are cleared! That should probably be changed.
  @handle def apply(entityInstance: Entity, nodeKeys: Traversable[NodeKey[_]]): ClearEntityInstanceTweaksGesture =
    new ClearEntityInstanceTweaksGesture(ScenarioReference.current, entityInstance, Some(nodeKeys))

  @handle def apply(scenario: ScenarioReference, entityInstance: Entity): ClearEntityInstanceTweaksGesture =
    new ClearEntityInstanceTweaksGesture(scenario, entityInstance)

  @handle def apply(
      scenario: ScenarioReference,
      entityInstance: Entity,
      nodeKeys: Traversable[NodeKey[_]]): ClearEntityInstanceTweaksGesture =
    new ClearEntityInstanceTweaksGesture(scenario, entityInstance, Some(nodeKeys))

  @handle def withExcludes(
      entityInstance: Entity,
      excludes: Traversable[NodeKey[_]]): ClearEntityInstanceTweaksGesture =
    new ClearEntityInstanceTweaksGesture(ScenarioReference.current, entityInstance, None, Some(excludes))

  @handle def withExcludes(
      scenario: ScenarioReference,
      entityInstance: Entity,
      excludes: Traversable[NodeKey[_]]): ClearEntityInstanceTweaksGesture =
    new ClearEntityInstanceTweaksGesture(scenario, entityInstance, None, Some(excludes))
}

// if nodes is None, all tweaks in scenario will be removed, including instance tweaks and property tweaks
final case class ClearTweaksGesture private (scenario: ScenarioReference, nodes: Option[Traversable[NodeKey[_]]] = None)
    extends DependencyTrackerUpdatingGesture {
  override private[optimus] def allTargetedScenarioReferences(current: ScenarioReference) = Set(scenario)

  override private[optimus] def doGesture(updater: DependencyTrackerBatchUpdater): Unit =
    nodes match {
      case Some(nodesToClear) => updater.updaterFor(scenario).removeTweaksImmediate(nodesToClear.toIterable)
      case None               => updater.updaterFor(scenario).removeAllTweaksImmediate()
    }
}
object ClearTweaksGesture {
  @handle def apply(scenario: ScenarioReference): ClearTweaksGesture = new ClearTweaksGesture(scenario)
  @handle def apply(scenarioOpt: Option[ScenarioReference]): ClearTweaksGesture =
    scenarioOpt.map(new ClearTweaksGesture(_)).getOrElse(ClearTweaksGesture())
  @handle def apply(): ClearTweaksGesture = new ClearTweaksGesture(ScenarioReference.current)
  @handle def apply(nodes: Traversable[NodeKey[_]]): ClearTweaksGesture =
    new ClearTweaksGesture(ScenarioReference.current, Some(nodes))
}

final case class SetPluginTag[P] private (scenario: ScenarioReference, key: PluginTagKey[P], value: P)
    extends DependencyTrackerUpdatingGesture {
  private[optimus] override def allTargetedScenarioReferences(current: ScenarioReference) = Set(scenario)
  override private[optimus] def doGesture(updater: DependencyTrackerBatchUpdater): Unit =
    updater.updaterFor(scenario).setPluginTag(key, value)
}

object SetPluginTag {
  @handle def apply[P](scenario: ScenarioReference, key: PluginTagKey[P], value: P): SetPluginTag[P] =
    new SetPluginTag(scenario: ScenarioReference, key, value)
}

final case class ClearPluginTag[P] private (scenario: ScenarioReference, key: PluginTagKey[P])
    extends DependencyTrackerUpdatingGesture {
  private[optimus] override def allTargetedScenarioReferences(current: ScenarioReference): Set[ScenarioReference] =
    Set(scenario)
  override private[optimus] def doGesture(updater: DependencyTrackerBatchUpdater): Unit =
    updater.updaterFor(scenario).clearPluginTag(key)
}

object ClearPluginTag {
  @handle def apply[P](scenario: ScenarioReference, key: PluginTagKey[P], value: P): ClearPluginTag[P] =
    new ClearPluginTag(scenario: ScenarioReference, key)
}

final case class ApplySnapshotTweakGesture private (
    targetRef: ScenarioReference,
    sealedTweakContainer: ScenarioReferenceState)
    extends DependencyTrackerUpdatingGesture {
  private[optimus] override def allTargetedScenarioReferences(current: ScenarioReference): Set[ScenarioReference] =
    Set(targetRef)

  private[optimus] def doGesture(updater: DependencyTrackerBatchUpdater): Unit = {
    val currScen = updater.target.scenarioReference
    // since this will run in a BatchUpdater we hold the lock on the root of our consistent subtree so the consistent root of the
    // target and of the current scenario need to be the same
    if (currScen.rootOfConsistentSubtree != targetRef.rootOfConsistentSubtree)
      throw new UnsupportedOperationException("Can only copy tweaks to scenarios in the same consistent subtree!")
    else updater.updaterFor(targetRef).addTweaksImmediate(sealedTweakContainer.tweaks)
  }
}

object ApplySnapshotTweakGesture {
  @handle private[optimus] def apply(
      scenario: ScenarioReference,
      tweaks: ScenarioReferenceState): ApplySnapshotTweakGesture =
    new ApplySnapshotTweakGesture(scenario, tweaks)
}

/**
 * Set an underlay scenario beneath the current ScenarioReference.
 *
 * Each ScenarioReference can have a single nested Scenario inserted underneath it (between it and its parent).
 *
 * i.e.: ScenRef (parent) Tweak.byValue(a:= 1) ScenRef (child) Tweak.byName(b:= a + 1) // b == 2 - a from parent
 *
 * applying the following gesture SetUnderlayGesture(Scenario(Tweak.byName(a:= 2) // on the child
 *
 * gives ScenRef (parent) Tweak.byValue(a:= 1) UnderlayScen (Tweak.byName(a:= 2) ScenRef (child) Tweak.byName(b:= a + 1)
 * // b == 3 - a from underlay
 *
 * To clear the underlay - set an empty scenario SetUnderlayGesture(Scenario())
 *
 * A typically usecase might be showing market data in a tab for a scenario generated in a ScenarioAPI and switching
 * being able to replace it without rebuilding the gui bindings.
 *
 * The underlay cannot be amended, only set and reset.
 *
 * @param scenario
 *   The scenario to set underneath the current ScenarioReference.
 */
final case class SetUnderlayGesture private (scenarioRef: ScenarioReference, scenario: Scenario)
    extends DependencyTrackerUpdatingGesture {
  private[optimus] override def allTargetedScenarioReferences(current: ScenarioReference) = Set(scenarioRef)

  private[optimus] def doGesture(updater: DependencyTrackerBatchUpdater): Unit =
    updater.updaterFor(scenarioRef).setUnderlayImmediate(scenario, updater.cause.root)
}

object SetUnderlayGesture {
  @handle def apply(targetScenarioRef: ScenarioReference, underlayScenario: Scenario): SetUnderlayGesture =
    new SetUnderlayGesture(targetScenarioRef, underlayScenario)
}

final case class CreateScenarioGesture private (ref: ScenarioReference) extends Gesture {
  private[optimus] override def allTargetedScenarioReferences(current: ScenarioReference) = Set(ref)

}
object CreateScenarioGesture {
  @handle def apply(ref: ScenarioReference): CreateScenarioGesture = new CreateScenarioGesture(ref)
}

final case class DisposeScenarioGesture private (ref: ScenarioReference) extends DependencyTrackerUpdatingGesture {
  private[optimus] override def allTargetedScenarioReferences(current: ScenarioReference) = Set(ref)
  override private[optimus] def doGesture(updater: DependencyTrackerBatchUpdater): Unit =
    updater.target.root.getScenarioIfExists(ref).foreach(_.disposeAsync())
}
object DisposeScenarioGesture {
  @handle def apply(ref: ScenarioReference): DisposeScenarioGesture = new DisposeScenarioGesture(ref)
}
