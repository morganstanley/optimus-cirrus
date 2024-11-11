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

import optimus.graph.cache.NodeCache
import optimus.platform.EvaluationQueue
import optimus.platform.RecordingScenarioStack
import optimus.platform.ScenarioStack
import optimus.platform.ScenarioStack.isSyntheticScenario
import optimus.platform.Tweak
import optimus.platform.storable.Entity

object TwkResolver {

  /** Only looks up instance tweaks */
  private[graph] def findInstanceTweak(key: NodeKey[_], ss: ScenarioStack): Tweak = {
    val info = key.propertyInfo
    var cur = ss
    while (!cur.isScenarioIndependent && !cur.isRoot) {
      val cacheID = cur._cacheID
      val tweak = if (cacheID.mayContainTweak(info)) cacheID.get(key) else null
      if (tweak ne null) return tweak
      cur = cur.parent
    }
    null
  }
}

/* To avoid allocation on a common path of not finding anything TwkResolver also implements PropertyNode
    @requestingNode reported if SI violation detected (getNode sets this)
 */
final private[optimus] class TwkResolver[R](
    requestingNode: NodeTask,
    info: NodeTaskInfo,
    key: NodeKey[R],
    startSS: ScenarioStack)
    extends PropertyNode[R] {

  initAsRunning(startSS)
  override def run(ec: OGSchedulerContext): Unit = throw new GraphInInvalidState("Can't run TwkResolver")
  override def entity: Entity = null

  override def executionInfo: NodeTaskInfo = NodeTaskInfo.TweakLookup

  /**
   * State of the current search:
   * <br> -3 done with current scenario stack
   * <br> -2 lookup instance tweaks (also an initial state)
   * <br> -1 Property tweaks
   * <br> 0+ Property tweaks on the parent classes/interfaces of this property info
   */
  var matchOnIdx: Int = -2
  var infoTarget: NodeTask = _ // If provided, will be the target of combineInfo
  var cur: ScenarioStack = startSS
  var evaluateInSS: ScenarioStack = _ // used when evaluating underlying node (if no tweak) - set during asyncResolve
  var whenNode: Node[Boolean] = _
  var tweak: Tweak = _ // Result of resolution of this tweak
  var resolved = false

  private def valueOfWhenClause(eq: EvaluationQueue): Boolean = {
    val trackingProxy = if (infoTarget ne null) infoTarget else startSS.tweakableListener.trackingProxy
    if (trackingProxy ne null) {
      // trackingProxy can be shared in xs resolving cases (overall an unusual case)
      trackingProxy.synchronized { trackingProxy.combineInfo(whenNode, eq) }
    } else {
      this.combineInfo(whenNode, eq)
    }

    resolved = whenNode.isDoneWithException || whenNode.result
    if (startSS.isRecordingWhenDependencies) {
      val whenNodeRT = whenNode.scenarioStack().tweakableListener.recordedTweakables
      val whenNodePredicate = tweak.target.whenPredicate
      startSS.combineWhenClauseData(key.asInstanceOf[PropertyNode[_]], whenNodePredicate, resolved, whenNodeRT)
    }

    // If when clause throws propagate to the caller
    if (whenNode.isDoneWithException) this.completeWithException(whenNode.exception(), eq)
    whenNode = null // Very important for async logic to null this node out
    resolved
  }

  /** If tweak found, create tweak node.... else return the original candidate */
  private def getValueNode(ec: OGSchedulerContext): PropertyNode[R] = {
    val evalSS = evaluateInSS
    evaluateInSS = null
    if (!resolved) throw new IllegalArgumentException("Can't call getValueNode before resolving!")
    if (isDoneWithException) this
    else if (tweak eq null) {
      val underlyingNode = key.prepareForExecutionIn(evalSS).asInstanceOf[PropertyNode[R]]
      startSS.completeGetNode(info, underlyingNode, ec)
    } else {
      // Found a tweak and will return some TweakNode....
      // cur is pointing to the scenarioStack where the tweak was found
      // startSS is pointing to the scenarioStack that requested the node/tweak
      val candidate = key.asInstanceOf[PropertyNode[R]]

      val tweakInfo = info.tweakInfo
      // It's only worth caching (and/or recording) a byName tweak
      val unstable = !tweak.tweakTemplate.resultIsStable()
      val shouldCache = unstable && tweakInfo.getCacheable
      val tweakNode = tweak.createNode(startSS, cur, candidate)
      tweakNode.markAsTrackingValue()

      val rss =
        if (shouldCache) NodeCache.lookupAndInsert(tweakInfo, tweakNode, startSS.privateCache, ec)
        else tweakNode
      rss
    }
  }

  /* Makes the next step in a search for a tweak in the current scenario stack */
  private def nextInCurrent(): Unit = {
    val cacheID = cur._cacheID

    def matchByProperty(pinfo: NodeTaskInfo): Unit = {
      cacheID.get(pinfo) match {
        case tweakExtractor: TweakKeyExtractor =>
          val extractorKey = ExtractorTweakableKey(tweakExtractor.key(key), pinfo)
          tweak = cacheID.get(extractorKey)

        case propertyTweak: Tweak =>
          tweak = propertyTweak
          whenNode = tweak.target.whenClauseNode(key, cur.parent)
          if ((whenNode ne null) && startSS.isRecordingWhenDependencies) {
            val whenClauseRTL = new WhenNodeRecordingTweakableListener(whenNode.scenarioStack())
            whenNode.replace(RecordingScenarioStack.withExistingListener(whenNode.scenarioStack(), whenClauseRTL))
          }
        case _ =>
      }
    }

    // If we share _cacheID with our parent, we must be some kind of non-Scenario-related ScenarioStack (e.g. we're
    // just here for plugin tags etc.), so skip resolving tweaks and go to parent. Otherwise byName tweaks
    // could end up resolving in us, and then re-resolving in the parent when the RHS is computed (consider x := x+1)
    if (isSyntheticScenario(cur)) matchOnIdx = -3
    else if (matchOnIdx == -2) {
      // Initial search on the level cur scenario stack
      // may be able to avoid the hash map lookup if mayContainTweak returns true
      tweak =
        if (info.wasTweakedByInstance() && cacheID.mayContainTweak(info)) cacheID.get(key)
        else null
    } else if (info.wasTweakedByProperty() && cacheID.mayContainTweak(info)) { // SEE_TWEAKED_BY_PROPERTY: consider unifying with tpdMask
      if (matchOnIdx == -1) matchByProperty(info)
      else {
        val parents = info.matchOn()
        if ((parents ne null) && matchOnIdx < parents.length) {
          if (parents(matchOnIdx).wasTweakedByProperty())
            matchByProperty(parents(matchOnIdx))
        } else matchOnIdx = -3
      }
    } else matchOnIdx = -3
  }

  /** Make the next step across scenario stacks, stop at tweaks and possible tweaks */
  private def next(ec: OGSchedulerContext, sync: Boolean): Unit = {
    if (!cur.isRoot) { // Test could be if(cur ne uptoSS) if need to limit the walk up arises
      if (cur.isScenarioIndependent) throw new IllegalScenarioDependenceException(key, requestingNode)
      nextInCurrent()
      if (tweak eq null) {
        if (matchOnIdx == -3) cur = cur.parent
      } else if (whenNode eq null)
        resolved = true
      else if (sync || !whenNode.isFSM) {
        ec.runAndWait(whenNode) // infoTarget
        if (!valueOfWhenClause(ec))
          tweak = null // When Clause returned false
      }
      matchOnIdx += 1
    } else resolved = true
  }

  def syncResolve(): TwkResolver[R] = {
    val ec = OGSchedulerContext.current()
    val startTime = OGTrace.observer.startTweakLookup(ec.prfCtx)
    while (!resolved) next(ec, sync = true)
    OGTrace.observer.stopTweakLookup(ec.prfCtx, startTime, info)
    this // Allow for nice flow
  }

  override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
    // Cast never throws because whenClause will always run and complete on a graph thread
    val ec = eq.asInstanceOf[OGSchedulerContext]

    // Either were waiting on a whenClause OR a tweak/original value...
    if (whenNode ne null) {
      if (valueOfWhenClause(ec))
        getValueNode(ec).enqueueAttached.continueWith(this, ec)
      else {
        tweak = null // when clause returned false and the tweak is not the correct tweak
        val resolutionNode = asyncResolve(ec)
        if (resolutionNode ne this) resolutionNode.enqueueAttached.continueWith(this, ec)
      }
    } else completeFromNode(child.asInstanceOf[Node[R]], ec)
  }

  /**
   * Returns one of the following:
   *  1. Itself (tweakResolver) if is waiting on some async when clauses
   *  1. Resolved tweak common case if byValue or all when clauses were sync
   *  1. Value node (really same as 2) when no tweaks were found
   */
  def startAsyncResolve(ec: OGSchedulerContext, evaluateInSS: ScenarioStack): PropertyNode[R] = {
    this.evaluateInSS = evaluateInSS
    val r = asyncResolve(ec)
    // bypassed tweakResolver may have extra info from when clauses in sync case (rare case)
    if (r ne this) {
      val requestingNode = ec.getCurrentNodeTask
      requestingNode.combineInfo(this, ec)
    }
    r
  }

  private def asyncResolve(ec: OGSchedulerContext): PropertyNode[R] = {
    val startTime = OGTrace.observer.startTweakLookup(ec.prfCtx)
    while (!resolved && (whenNode eq null)) next(ec, Settings.compatSyncTweakWhen)
    if (whenNode ne null) {
      val cWhenNode = whenNode
      // We want to avoid recursion here for accounting, note this works only for whenClauses that are not cached
      // also note we stored the value of whenNode because continueWithIfEverRuns adds whenNode to the waitOnChain
      // of some parent and another thread can find this whenNode and run/complete/onChildCompleted -> whenNode = null
      cWhenNode.continueWithIfEverRuns(this, ec)
      cWhenNode.enqueueAttached
      OGTrace.observer.stopTweakLookup(ec.prfCtx, startTime, info)
      this /* Not done resolving */
    } else {
      val valNode = getValueNode(ec)
      OGTrace.observer.stopTweakLookup(ec.prfCtx, startTime, info)
      valNode
    }
  }
}
