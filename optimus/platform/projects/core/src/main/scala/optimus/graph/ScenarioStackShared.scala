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

import optimus.breadcrumbs.ChainedID
import optimus.config.RuntimeComponents
import optimus.config.scoped.ScopedSchedulerPlugin
import optimus.core.MonitoringBreadcrumbs
import optimus.graph.cache.NodeCCache
import optimus.graph.cache.PrivateCache
import optimus.graph.cache.UNodeCache
import optimus.platform.RuntimeEnvironment
import optimus.platform.RuntimeEnvironmentKnownNames._
import optimus.platform.Scenario
import optimus.platform.ScenarioStack
import optimus.platform.StartNode
import optimus.platform.inputs.loaders.FrozenNodeInputMap
import optimus.platform.util.html.HtmlBuilder
import optimus.platform.util.html.NoStyleNamed

import java.util.concurrent.ConcurrentMap

/**
 * This is a private object and a private class for ScenarioStack and we just don't want ScenarioStack class to get even
 * larger. It really ideally would be a private inner class of ScenarioStack But ABSOLUTELY no code outside of
 * ScenarioStack should touch this class!!!!!!!!!!!
 */
private[optimus /*ScenarioStack*/ ] object ScenarioStackShared {
  def apply(
      env: RuntimeEnvironment,
      inputs: FrozenNodeInputMap,
      scopedConfiguration: Map[NodeTaskInfo, ScopedSchedulerPlugin],
      uniqueID: Boolean = false): ScenarioStackShared = {
    val trackingID =
      if (env != RuntimeEnvironment.minimal && env.config != null && env.config.envName != EnvNone)
        env.config.runtimeConfig.rootID
      else ChainedID.create()
    val siParams = SIParams(
      nodeInputs = inputs,
      parentTrackingNodeID = ChainedID.root,
      trackingNodeID = trackingID,
      scopedPlugins = scopedConfiguration)
    apply(env, siParams, privateCacheMoniker = null, uniqueID = uniqueID)
  }

  private[optimus] def apply(
      env: RuntimeEnvironment,
      siParams: SIParams,
      privateCacheMoniker: PrivateCache.Moniker,
      uniqueID: Boolean): ScenarioStackShared = {
    val cacheID =
      if (uniqueID) SSCacheID.newUnique()
      else CacheIDs.putIfAbsent(RuntimeEnvironmentCacheKey(env), SSCacheID.newUnique())

    val ssShared = new ScenarioStackShared(env)
    var ss = new ScenarioStack(
      flags = EvaluationState.NOT_TRANSPARENT_FOR_CACHING,
      _cacheID = cacheID,
      ssShared = ssShared,
      siParams = siParams)

    if (privateCacheMoniker ne null) {
      ss = ss.withPrivateProvidedCache(
        createPrivateCache(privateCacheMoniker.cache),
        privateCacheMoniker.readFromGlobalCaches)
    }

    ssShared.setScenarioStack(ss) // Link up shared state to scenario stack
    ssShared
  }

  private def createPrivateCache(privateCacheName: String): UNodeCache =
    new UNodeCache(
      privateCacheName,
      Settings.cacheSSPrivateSize,
      Settings.cacheConcurrency,
      NodeCCache.defaultCacheBatchSize,
      NodeCCache.defaultCacheBatchSizePadding)
}

/**
 * Helper class to managed shared data between different scenario stacks Also allows for quick access to things like
 * RuntimeEnvironment, siStack etc... `siStack`, `siStackWithIgnoreSyncStack` and `siRootCache` are all basically caches
 * in that they hold scenario stacks
 */
private[optimus /*ScenarioStack*/ ] final class ScenarioStackShared private (val environment: RuntimeEnvironment) {
  def config: RuntimeComponents = environment.config

  // `siStack`, `siStackWithIgnoreSyncStack` and `siRootCache` are all basically caches in that they hold scenario stacks in which
  // we want to often compute. we regenerate them each time and so not share them across the engine, because the probability of such cache
  // hit between different executions is very remote (the key to the cache would have to include things like chainedID etc which change
  // in each distributed task).
  // !!! the reason why things such as chainedID matter here is because we are caching **scenario stacks**, not computation values. !!!
  // !!! if we ignored chainedID, we would execute a node under the wrong chainedID.                                                !!!

  @volatile
  private var _scenarioStackWithInitialTime: ScenarioStack = _
  var scenarioStack: ScenarioStack = _
  var siStack: ScenarioStack = _ // because we often need the SI version of `scenarioStack`
  var siStackWithIgnoreSyncStack: ScenarioStack = _ // same as above, but adds IGNORE_SYNC_STACK

  // conceptually identical to `siStack` except that this cache's values are variations of `siStack` with added siParams.
  // the siParams are the key of the cache. used by siRootWithTagsFrom called from LazyPickledReference constructor,
  // this imbues LPR's SIRoot stack with tags.
  var siRootCache: ConcurrentMap[AnyRef, ScenarioStack] = CacheIDs.createWeakCache() // Some key to SI

  def setScenarioStack(ss: ScenarioStack): Unit = {
    scenarioStack = ss
    siStack = ss.copy(
      _parent = scenarioStack,
      // we set both SI flags - note that SCENARIO_INDEPENDENT_SELF_OR_ANCESTOR is inherited by non-SI children but
      // SCENARIO_INDEPENDENT_STACK is not
      flags = EvaluationState.ALL_SI_FLAGS | EvaluationState.NOT_TRANSPARENT_FOR_CACHING
    )
    siStackWithIgnoreSyncStack = siStack.withIgnoreSyncStack
  }

  /** Useful in checking if the the scenarioStack is based on the top of initialTime scenario */
  def initTimePresentWithCheck(scenario: Scenario): Boolean = {
    if (
      (_scenarioStackWithInitialTime ne null) && ((_scenarioStackWithInitialTime.topScenario eq scenario) || (_scenarioStackWithInitialTime.topScenario == scenario))
    )
      true
    else if (_scenarioStackWithInitialTime ne null) {
      if (Settings.throwOnInvalidInitRuntimeEnvUsage)
        throw new GraphInInvalidState()
      else
        try {
          val realInitScenario = _scenarioStackWithInitialTime.topScenario
          val foundScenarioString = realInitScenario.toString + realInitScenario.createdAtAsString
          val topLevelScenarioString = scenario.toString + scenario.createdAtAsString
          MonitoringBreadcrumbs.sendEvilScenarioStackInitTimeRandom(topLevelScenarioString, foundScenarioString)
        } catch {
          case _: Throwable =>
        }
      true
    } else
      false
  }

  def setInitTimeScenarioStack(ss: ScenarioStack): Unit = _scenarioStackWithInitialTime = ss

  /** Most of the code doesn't care to re-query server time and should just use this ScenarioStack */
  def scenarioStackWithInitialTime: ScenarioStack = scenarioStackWithInitialTime(requeryTime = false)

  def scenarioStackWithInitialTime(requeryTime: Boolean): ScenarioStack = {
    if (requeryTime || (_scenarioStackWithInitialTime eq null)) synchronized {
      val scenario = config.newTopScenario(environment.entityResolver)
      val newSS = scenarioStack.createChild(scenario, this /* id for the debugger UI*/ )
      if (requeryTime && (_scenarioStackWithInitialTime ne null)) {
        val clonedWithNewShared = newSS.copy(ssShared = copy)
        clonedWithNewShared.ssShared._scenarioStackWithInitialTime = clonedWithNewShared
        clonedWithNewShared
      } else {
        _scenarioStackWithInitialTime = newSS
        newSS
      }
    }
    else _scenarioStackWithInitialTime
  }

  // consider: optimus.graph.OGScheduler.weakView could live here instead of SIParams
  private def copy: ScenarioStackShared = {
    val shared = new ScenarioStackShared(environment)
    shared.scenarioStack = scenarioStack
    shared.siStack = siStack
    shared.siStackWithIgnoreSyncStack = siStackWithIgnoreSyncStack
    shared.siRootCache = siRootCache
    shared
  }

  def _cacheID: SSCacheID = scenarioStack._cacheID

  // Always give a new node...
  // Most common reason to ask for a new nullNode is to create a new EvaluationContext
  // Every EC has to have a private nullNode because it's legal to set default entry scenarioStack
  // in each thread to a different value
  def nullNode = new StartNode(scenarioStack)

  def writeHtml(hb: HtmlBuilder): HtmlBuilder = {
    hb.namedGroup("ScenarioStack") {
      hb.buildLeaf(NoStyleNamed("UntweakedScenarioState")) {
        _ ++= "UntweakedScenarioState@" ++= environment.hashCode.toHexString
      }
    }
  }

  def withTrackingNodeID(id: ChainedID): ScenarioStackShared = {
    val ssShared = new ScenarioStackShared(environment)
    val ssCopy = scenarioStack.copy(ssShared = ssShared, siParams = scenarioStack.siParams.copy(trackingNodeID = id))
    ssShared.setScenarioStack(ssCopy)
    ssShared
  }
}
