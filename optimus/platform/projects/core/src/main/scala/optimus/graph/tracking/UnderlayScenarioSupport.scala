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

import optimus.graph.EvaluationState
import optimus.graph.tracking.ttracks.TweakableTracker
import optimus.platform.EvaluationContext
import optimus.platform.Scenario
import optimus.platform.ScenarioStack
import optimus.platform.Tweak
import optimus.platform.inputs.loaders.FrozenNodeInputMap

import scala.collection.mutable.ArrayBuffer

/**
 * Implementation trait for supporting "underlay scenarios" - nested immutable Scenarios which can be inserted (and
 * replaced) underneath a MutableTweakContainer. Handles regeneration of ScenarioStacks and notification of invalidated
 * tweakables.
 */
private[tracking] trait UnderlayScenarioSupport {
  self: DependencyTracker =>

  final private[tracking] def doSetUnderlay(scenario: Scenario, cause: EventCause): Unit = {
    val newUnderlayStack = regenerateUnderlay(scenario, scenarioStack, cause)
    scenarioStack.setParent(newUnderlayStack)
    nc_scenarioStack.setParent(newUnderlayStack)
  }

  /**
   * Creates a new underlay stack which will apply the (nested) Scenario underneath this TrackingScenarioStack.
   * Invalidates any tweakables which may have changed due to the replacement of the old underlay Scenario with the new
   * one. Does not actually reset the parent of the TrackingScenarioStack (this is the responsibility of the caller)
   *
   * n.b. We must always have an underlay scenario (we use a non-squashable Scenario when no underlay is required. The
   * initial version is set during construction of DependencyTracker.
   *
   * @param scenario
   *   The new scenario to insert as the underlay scenario.
   */
  def regenerateUnderlay(scenario: Scenario, ss: ScenarioStack, cause: EventCause): ScenarioStack = {
    // n.b. we invalidate all tweaks from the old underlay and the new underlay - this leads to over
    // invalidation (if the same byValue tweak is applied in new and old stack it does not need invalidating.
    // This leads to correct but potentially sub-optimal performance.

    // capture all tweaks from the old underlay
    val oldTweaksToInvalidate = tweaksFromStack(ss.parent, parentScenarioStack)

    // create the new underlay untweaked scenario state off of the parent tracking scenario
    val newStack0 =
      parentScenarioStack.createChild(
        scenario,
        FrozenNodeInputMap.empty,
        EvaluationState.TRACKING_SCENARIO_STACK_FLAGS,
        EvaluationContext.currentNode,
        squashIfNoContent = false)

    // reset the tweakable listener to us rather than the default (parent) so we can track and invalidate
    val newStack = setUnderlayScenarioStackTweakableListenerAndDepth(
      newStack0,
      parentScenarioStack,
      underlayTweakableTracker,
      ss.trackingDepth - 1)

    // capture all tweaks in the newly created underlay stack
    val newTweaksToInvalidate = tweaksFromStack(newStack, parentScenarioStack)
    val allInvalidations = oldTweaksToInvalidate ++ newTweaksToInvalidate

    if (allInvalidations.nonEmpty) {
      invalidateSnapshot()
      underlayTweakableTracker.invalidateByTweaks(allInvalidations, cause)
    }

    newStack
  }

  /**
   * Collect all of the tweak targets referenced in the stack 'stackBase (inclusive) to 'stackTop' (exclusive)
   *
   * @param stackBase
   *   The bottom of the scenario stack (furthest from root)
   * @param stackTop
   *   The top of the scenario stack (closest to root)
   * @return
   *   tweak targets in this stack
   */
  private def tweaksFromStack(stackBase: ScenarioStack, stackTop: ScenarioStack): ArrayBuffer[Tweak] = {
    var curScenarioSS = stackBase
    val tweaks = ArrayBuffer.empty[Tweak]

    while (curScenarioSS ne stackTop) {
      tweaks ++= curScenarioSS.expandedTweaks
      curScenarioSS = curScenarioSS.parent
    }

    tweaks
  }

  /**
   * Change the tweakable listener of all nested scenario from 'stackBase (inclusive) to 'stackTop' (exclusive)
   *
   * @param ss
   *   The bottom of the scenario stack (furthest from root)
   * @param stackTop
   *   The top of the scenario stack (closest to root), not updated
   * @param tweakableListener
   *   The tweakable listener to set
   */
  private def setUnderlayScenarioStackTweakableListenerAndDepth(
      ss: ScenarioStack,
      stackTop: ScenarioStack,
      tweakableListener: TweakableTracker,
      depth: Int): ScenarioStack = {
    if (ss eq stackTop) ss
    else {
      ss.copy(
        // change tweakable listener and depth to match our underlay tracker (it's fine for all of the underlay stacks
        // to have the same depth - it's only used by the tracker for figuring out whether or not the tweaks came
        // from above or below it)
        _tweakableListener = tweakableListener,
        // depth is flat because we have a single tweak listener
        trackingDepth = depth,
        // important: give the SS a unique ID otherwise if two sibling DependencyTrackers have underlays of the same
        // Scenario then we will get cache hits between them which is not OK because they have different
        // underlayTweakableTrackers (so things which are only tracked in one might get used from the other)
        _cacheID = ss._cacheID.dup,
        _stableSS = null,
        _parent = setUnderlayScenarioStackTweakableListenerAndDepth(ss.parent, stackTop, tweakableListener, depth)
      )
    }
  }

  private[tracking] class TSA_SetUnderlay(scenario: Scenario) extends TSA_BasicUpdateAction {
    override protected def doUpdate(): Unit = doSetUnderlay(scenario, cause.root)
  }
}
