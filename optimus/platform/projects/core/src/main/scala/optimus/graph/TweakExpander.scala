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

import java.util.{IdentityHashMap => JIdentityHashMap}
import msjava.slf4jutils.scalalog.getLogger
import optimus.graph.tracking.NoOpTrackedNodeInvalidationObserver
import optimus.platform.Scenario
import optimus.platform.ScenarioStack
import optimus.platform.Tweak
import optimus.platform.dal.Marker
import optimus.platform.storable.Entity

import scala.collection.immutable.ArraySeq
import scala.collection.mutable.ArrayBuffer

/**
 * Expands tweaks which have tweak translators (a.k.a. also-sets) and also Markers (i.e. validTime and friends).
 *
 * The expansion is done in a cached node so that we don't waste time redoing work and also so that it can be
 * invalidated by the normal mechanisms if any tweakables used during expansion are modified.
 */
private[optimus] object TweakExpander {
  private[this] val log = getLogger(TweakExpander)

  /**
   * Returns the fully expanded version of scenario under evaluateIn. scenario must be a single layer scenario (no
   * nesting)
   */
  def expandTweaks(scenario: Scenario, evaluateIn: ScenarioStack): Scenario = {
    if (Settings.schedulerAsserts && scenario.nestedScenarios.nonEmpty)
      throw new GraphInInvalidState("Scenario for expansion should not be nested")
    val node = new ExpandTweaksNode(scenario)
    // since scenario stack creation is not async we may have sync stacks here
    evaluateIn.withIgnoreSyncStack.getNode(node).get
  }

  private class ExpandTweaksNode(val scenario: Scenario) extends PropertyNode[Scenario]() {
    // our identity is just the scenario (plus evaluateIn, implicitly)
    override def propertyInfo: NodeTaskInfo = NodeTaskInfo.TweakExpansion
    override def argsHash: Int = scenario.hashCode()
    override def argsEquals(that: NodeKey[_]): Boolean = scenario == that.asInstanceOf[ExpandTweaksNode].scenario
    override def entity: Entity = null
    override def run(ec: OGSchedulerContext): Unit = {
      var expanded: Scenario = null
      try expanded = func
      catch {
        case e: Exception => completeWithException(e, ec)
      }
      if (expanded ne null) completeWithResult(expanded, ec)
    }

    override def func: Scenario = {
      try {
        doExpansion()
      } catch {
        case e: GraphFailedTrustException if !Settings.tweakTranslatorsMustBePure =>
          log.error(
            s"Tweak expansion was impure for tweaks ${scenario.topLevelTweaks}! This will soon be banned! " +
              "Retrying without caching...",
            e)
          // re-run without the cacheableTransitively flag so that @impure checks won't be applied, and pre-invalidate
          // us so that the current caller(s) will get this value, but no subsequent callers will
          replace(scenarioStack.withoutCacheableTransitively)
          invalidateCache(NoOpTrackedNodeInvalidationObserver) // it's our own node and nobody is observing it
          doExpansion()
      }
    }

    private def doExpansion(): Scenario = {
      val tweaks = scenario.topLevelTweaks
      val expB = ArraySeq.newBuilder[Tweak]
      expB.sizeHint(tweaks.size)
      val it = tweaks.iterator
      while (it.hasNext) {
        val tweak: Tweak = it.next()
        if (tweak.target.unresolved) tweak.expandInto(expB, scenarioStack)
        else expB += tweak
      }

      val expanded = expB.result()

      val results = new ArrayBuffer[Tweak](expanded.size)
      var markers: JIdentityHashMap[Marker[_], Tweak] = null
      val size = expanded.size
      var i = 0
      while (i < size) {
        val twk = expanded(i)
        val target = twk.target
        val hashKey = target.hashKey

        if (target.isMarker) {
          if (markers eq null) markers = new JIdentityHashMap[Marker[_], Tweak]
          markers.put(hashKey.asInstanceOf[Marker[_]], twk)
        } else {
          results += twk
        }
        i += 1
      }

      if (markers ne null) processMarkers(markers, results)
      val expandedScenario = Scenario.validate(
        results.toList,
        "After tweak expansion",
        scenario.unorderedTweaks,
        scenario.flagsWithoutUnresolved)
      expandedScenario
    }

    private[this] def processMarkers(
        markers: JIdentityHashMap[Marker[_], Tweak],
        outputBuffer: ArrayBuffer[Tweak]): Unit = {
      // Unfortunately entityResolver.translateMarkers relies on EC.current...
      // In the cases where EC scenario is not updated right away (e.g. nest scenario, sequences etc..)
      // We need to make it look as if it has been set! A better solution would to have translateMarkers take in evaluateIn
      val twks = scenarioStack.env.entityResolver.translateMarkers(markers)
      outputBuffer ++= twks
    }
  }
}
