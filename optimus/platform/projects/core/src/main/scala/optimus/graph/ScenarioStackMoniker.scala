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
import optimus.dist.HasDmcBinaryIdentity
import optimus.graph.cache.CacheSelector.Moniker
import optimus.platform.EvaluationContext
import optimus.platform.RuntimeEnvironment
import optimus.platform.Scenario
import optimus.platform.ScenarioStack
import optimus.platform.inputs.loaders.FrozenNodeInputMap
import optimus.platform.inputs.loaders.OptimusNodeInputStorage

import scala.collection.mutable.ArrayBuffer

sealed trait ScenarioStackMoniker extends Serializable {
  def restoreOnEnv(env: RuntimeEnvironment): ScenarioStack
}

private class ActualScenarioStackMoniker(
    flags: Int,
    inputScenarios: Array[Scenario],
    initTimeScenarioPresent: Boolean,
    siParams: SIParams,
    privateCache: Moniker)
    extends ScenarioStackMoniker
    with HasDmcBinaryIdentity {

  override def restoreOnEnv(env: RuntimeEnvironment): ScenarioStack = {
    // merge the node inputs coming from the client, with those coming from the underlying process' system properties
    // and environment variables
    val nodeInputs = OptimusNodeInputStorage.loadScopedState.mergeWith(siParams.nodeInputs.freeze)
    val adjSIParams = siParams.copy(nodeInputs = nodeInputs)

    // the root scenario stack which points to the currently available RuntimeEnvironment (at this stage already installed by `dist`).
    // UntweakedScenarioState takes care of fixing the _cacheID by tying it to the runtime environment,
    // so that cache reuse still happens across different executions on the engine.
    val sss = ScenarioStackShared(env, adjSIParams, privateCache, uniqueID = false)
    var ss = if (isSelfOrAncestorScenarioIndependent) sss.siStack else sss.scenarioStack

    var i = inputScenarios.length - 1
    var setUpInitTimeScenarioStack = initTimeScenarioPresent
    while (i >= 0) {
      val scenario = inputScenarios(i)
      val proposedCacheID = SSCacheID.newUninitialized(scenario)
      // fixup our _cacheID to share with this engine's cache.
      val newCacheID =
        if (Settings.reuseCacheOnRemoteCalls && !isRecordingTweakUsage) {
          val cacheKey = new SSCacheKey(ss._cacheID.id, scenario)
          CacheIDs.putIfAbsent(cacheKey, proposedCacheID) // also invokes `proposedCacheID.ensureInitialized`
        } else proposedCacheID
      newCacheID.ensureInitialized()

      ss = ss.initChild(scenario, FrozenNodeInputMap.empty, extraFlags = flags, NoOpTweakableListener, newCacheID, null)
      if (setUpInitTimeScenarioStack) {
        sss.setInitTimeScenarioStack(ss)
        setUpInitTimeScenarioStack = false
      }
      i -= 1
    }
    ss
  }

  // noinspection ScalaUnusedSymbol
  private def readResolve(): AnyRef = {
    if (!EvaluationContext.isInitialised)
      throw new GraphInInvalidState("Scenario stack must be deserialized on a graph thread")
    restoreOnEnv(EvaluationContext.env)
  }

  private def isRecordingTweakUsage: Boolean = (flags & EvaluationState.RECORD_TWEAK_USAGE) != 0
  private def isSelfOrAncestorScenarioIndependent: Boolean =
    (flags & EvaluationState.SCENARIO_INDEPENDENT_SELF_OR_ANCESTOR) != 0

  override def dmcBinaryIdentity: java.io.Serializable = DmcActualScenarioStackMonikerReplacement(
    inputScenarios.toVector)
}

private final case class DmcActualScenarioStackMonikerReplacement(inputScenarios: Vector[Scenario])

private[optimus] object ScenarioStackMoniker {

  // `readResolve` is stripped off objects in Scala 2.13, so we cannot
  // use them to define ConstantScenarioStackMoniker and ConstantScenarioStackNCMoniker

  private val ConstantScenarioStackMoniker = new ScenarioStackMoniker {
    // noinspection ScalaUnusedSymbol
    private def readResolve(): AnyRef = ScenarioStack.constant
    override def restoreOnEnv(env: RuntimeEnvironment): ScenarioStack = ScenarioStack.constant
  }

  private val ConstantScenarioStackNCMoniker = new ScenarioStackMoniker {
    // noinspection ScalaUnusedSymbol
    private def readResolve(): AnyRef = ScenarioStack.constantNC
    override def restoreOnEnv(env: RuntimeEnvironment): ScenarioStack = ScenarioStack.constantNC
  }

  def apply(ss: ScenarioStack): ScenarioStackMoniker = {
    if (ss.isConstant) {
      if (ss.cachedTransitively) ConstantScenarioStackMoniker
      else ConstantScenarioStackNCMoniker
    } else {
      if (ss.isTrackingIndividualTweakUsage)
        throw new GraphException("Cannot distribute tracking scenarios")
      else if (ss.isRecordingTweakUsage) {
        // TODO (OPTIMUS-74952): We currently don't have a reliable serialization approach for recording tweakable
        val owner = ss.tweakableListener match {
          case listener: RecordingTweakableListener => listener.trackingProxy
          case _                                    => null
        }
        val current = EvaluationContext.currentNodeOrNull
        MonitoringBreadcrumbs.serializingXScenarioStack(owner, current)
      }
      val cleanedUpFlags = ss.flags & ~EvaluationState.ENGINE_PROPAGATION_CLEAR_FLAGS
      val privateCache = ss.cacheSelector.toMoniker

      val cacheIDArray = buildSSCacheIDArray(ss)
      val mayNeedInitTimes = !ss.isSelfOrAncestorScenarioIndependent && cacheIDArray.length > 0
      val hasInitTimes = mayNeedInitTimes && ss.ssShared.initTimePresentWithCheck(cacheIDArray.last)
      val res = new ActualScenarioStackMoniker(cleanedUpFlags, cacheIDArray, hasInitTimes, ss.siParams, privateCache)
      res
    }
  }

  /**
   * @return
   *   an ordered [[Array]] of [[Scenario]], beginning with the innermost [[ScenarioStack]]'s up to the root also skips
   *   scenario stacks with empty scenarios (which specifically means skipping top level)
   */
  private[optimus] def buildSSCacheIDArray(ss: ScenarioStack): Array[Scenario] = {
    // cleanup scenario stack by removing tracking related metadata (xs or tracking), if any such exist
    val cleanedUpSS = ss.asBasicScenarioStack
    val scenarios = new ArrayBuffer[Scenario]()
    var curr = cleanedUpSS
    var lastCacheID: SSCacheID = null
    while (curr ne null) {
      val cacheID = curr._cacheID
      val inputScenario = cacheID.inputScenario
      if (!inputScenario.isEmpty && (cacheID ne lastCacheID)) {
        scenarios += inputScenario
        lastCacheID = cacheID
      }
      curr = curr.parent
    }
    scenarios.toArray
  }

}
