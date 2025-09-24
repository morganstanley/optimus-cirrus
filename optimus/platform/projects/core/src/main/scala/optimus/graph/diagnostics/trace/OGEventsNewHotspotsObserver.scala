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
package optimus.graph.diagnostics.trace

import optimus.graph.NodeAwaiter
import optimus.graph.NodeTask
import optimus.graph.NodeTask.ScenarioStackNullTask
import optimus.graph.NodeTaskInfo
import optimus.graph.OGTrace
import optimus.graph.OGTrace.trace
import optimus.graph.PThreadContext
import optimus.graph.diagnostics.PNodeTaskInfoLight
import optimus.graph.{OGLocalTables => LT}
import optimus.graph.{PropertyNode => PN}
import optimus.platform.{EvaluationQueue => EQ}
import optimus.utils.LogLogHistogram

// Cache reuse histogram.  Each bucket is a factor of 8, i.e. 1st bucket means re-use within 1-2^3, then 9-2^6, up to 2^36 (12 buckets)
// Each bucket counts up to 2^(2^5)
object ReuseHistogram extends LogLogHistogram(3, 5)

class OGEventsNewHotspotsObserver private[trace] extends OGEventsGlobalGraphObserver {
  override def name: String = if (OGTraceMode.testNewHS) "hotspots" else "hotspotsLight"
  override def title: String = "Hotspots"
  override def description: String =
    "Lighter hotspots mode, avoiding PNodeTask allocation (but currently doesn't support profiled blocks)"

  override def includeInUI: Boolean = true
  override def supportsProfileBlocks: Boolean = true
  override def recordLostConcurrency: Boolean = true
  override def collectsHotspots: Boolean = true
  override def collectsAccurateCacheStats: Boolean = true
  override def collectsCacheDetails(): Boolean = true

  override def reuseUpdate(task: NodeTask, rcount: Int): Unit = {
    val info = task.executionInfo()
    // accumulate histogram
    val lt = LT.getOrAcquire()
    val reg: Long = ReuseHistogram.add(info.reuseStats, rcount, lt.logRandom) // accumulate histogram
    lt.release()
    info.reuseStats = reg
    // accumulate maximum
    if (rcount > info.reuseCycle)
      info.reuseCycle = rcount
  }

  override def dependency(fromTask: NodeTask, toTask: NodeTask, eq: EQ): Unit = {
    if (!toTask.executionInfo.getCacheable)
      fromTask.addSelfPlusANCTime(toTask.getSelfPlusANCTime)
    else {
      val lt = LT.getOrAcquire(eq)
      lt.ctx.getPNTI(toTask, fromTask.profileBlockID).nodeUsedTime += toTask.getSelfPlusANCTime
      lt.release()
    }
  }

  override def initializeAsCompleted(task: NodeTask): Unit = {
    val nti = OGEventsObserver.profileInfo(task)
    if (nti == NodeTaskInfo.Start) return
    val time = OGTrace.nanoTime
    trace.ensureProfileRecorded(nti.profile, nti)
    val lt = LT.getOrAcquire()
    completed(lt, task, time, initAsCompleted = true)
    lt.release()
  }

  override def start(lCtx: LT, task: NodeTask, isNew: Boolean): Unit = {
    val ctx = lCtx.ctx
    if (ctx.ntsk ne null) // This is the case of sync entry into another node
      stop(lCtx, ctx.ntsk)
    start(ctx, task, OGTrace.nanoTime)
  }

  override def startAfterSyncStack(lCtx: LT, task: NodeTask): Unit = {
    if (lCtx.ctx.ntsk == null) {
      val restoreTask = task match {
        // Special case where the node being "restored" should be unwrapped
        case ssnt: ScenarioStackNullTask => ssnt.orgTask
        case _                           => task
      }
      // When a child node didn't run (e.g. it was already done) node is not restarting
      start(lCtx.ctx, restoreTask, OGTrace.nanoTime)
    }
  }

  override def stop(lCtx: LT, task: NodeTask): Unit = {
    val time = OGTrace.nanoTime
    val ctx = lCtx.ctx
    ctx.selfTime += time - ctx.startTime

    if (ctx.nodeSuspendOrCompleteTime == 0) // not seen a suspend()
      stopish(ctx, time)
    else
      postStopish(ctx, time)

    ctx.ntsk = null
    ctx.startTime = 0
    ctx.nodeSuspendOrCompleteTime = 0
  }

  override def suspend(eq: EQ, awaiter: NodeAwaiter): Unit = {
    val time = OGTrace.nanoTime
    val lCtx = LT.getOrAcquire(eq)
    val ctx = lCtx.ctx
    // Node is 'suspending' only if it is currently running
    if ((ctx.ntsk eq awaiter) && ctx.nodeSuspendOrCompleteTime == 0) // times between multiple suspends are 'overhead'
      stopish(ctx, time)

    lCtx.release()
  }

  private def completed(lCtx: LT, task: NodeTask, time: Long, initAsCompleted: Boolean): Unit = {
    val ctx = lCtx.ctx
    if ((ctx.ntsk eq task) && ctx.nodeSuspendOrCompleteTime == 0) // in case of suspend then complete on the same thread
      stopish(ctx, time)

    val pid = task.getProfileId
    trace.ensureProfileRecorded(lCtx, pid, task)
    val pnti = ctx.getPNTI(task)
    if (!initAsCompleted) pnti.start += 1 // want the timestamps for these for more sensible timeline, but no starts
    pnti.ancAndSelfTime += task.getSelfPlusANCTime // before node completes this field is just ancSelfTime
  }

  override def completed(eq: EQ, task: NodeTask): Unit = {
    val time = OGTrace.nanoTime
    val lCtx = LT.getOrAcquire(eq)
    completed(lCtx, task, time, initAsCompleted = false)
    lCtx.release()
  }

  private def start(ctx: PThreadContext, task: NodeTask, time: Long): Unit = {
    ctx.taskStart(task, time)
    ctx.nodeSuspendOrCompleteTime = 0
  }

  private def postStopish(ctx: PThreadContext, time: Long): Unit = {
    ctx.getPNTI.postCompleteAndSuspendTime += time - ctx.nodeSuspendOrCompleteTime
  }

  private def stopish(ctx: PThreadContext, time: Long): Unit = {
    ctx.nodeSuspendOrCompleteTime = time
    val addSelfTime = time - ctx.startTime
    ctx.getPNTI.selfTime += addSelfTime
    ctx.ntsk.addSelfPlusANCTime(addSelfTime)
  }

  /**
   * CACHE EVENTS
   */
  override def lookupStart(): Long = OGTrace.nanoTime

  private def lookupEndCommon(
      ctx: PThreadContext,
      startTime: Long,
      keyOrProxy: NodeTask,
      lookupResult: NodeTask,
      recursiveLookup: Boolean = false,
      countTowardsLookup: Boolean = true): PNodeTaskInfoLight = {
    val cacheTime = OGTrace.nanoTime - startTime
    ctx.lookupEnd(cacheTime, lookupResult)

    val pnti = ctx.getPNTI(lookupResult, keyOrProxy.profileBlockID())
    pnti.cacheTime += cacheTime
    pnti.collisionCount += ctx.cacheCollisions
    ctx.cacheCollisions = 0

    // [CHILD_NODE_LOOKUP] via parent PNTI
    // [SEE_RACEY_SPECULATIVE_INSERTION]
    if (ctx.ntsk ne null) {
      val parentPnti = ctx.getPNTI()
      if (countTowardsLookup)
        parentPnti.childNodeLookupCount += 1
      // need to add to childNodeLookupTime even if not to count to not have any gaps in profiling
      parentPnti.childNodeLookupTime += cacheTime
    }
    pnti
  }

  override def lookupEndPartial(
      lCtx: LT,
      startTime: Long,
      parent: PN[_],
      task: PN[_],
      countTowardsLookup: Boolean): Unit = {
    lookupEndCommon(lCtx.ctx, startTime, parent, task, recursiveLookup = true, countTowardsLookup = countTowardsLookup)
  }

  // these can be PropertyNodes, refactor from NodeTask
  override def lookupEnd(lt: LT, startTime: Long, candidate: NodeTask, lookupResult: NodeTask): Unit = {
    val pnti = lookupEndCommon(lt.ctx, startTime, candidate, lookupResult)

    // [SEE_LOOKUP_END_HITS]
    trace.ensureProfileRecorded(pnti.id, lookupResult)
    val candidateID = candidate.getProfileId
    if (candidateID != pnti.id)
      trace.ensureProfileRecorded(candidateID, candidate)

    val underlying = lookupResult.cacheUnderlyingNode
    if (candidate ne underlying) {
      if (candidate.computedInCacheID() == lookupResult.computedInCacheID()) {
        pnti.cacheHit += 1
        if (lookupResult.isSeenBefore)
          pnti.cacheHitFromDifferentTask += 1
      } else {
        // this definitely was not a trivial match, attribute to underlying. Note - this looks like over counting, but
        // will work out once we aggregate profiling information from proxies and their original nodes [SEE_PROXY_NON_TRIVIAL]
        val underlyingPNTI = lt.ctx.getPNTI(candidate)
        underlyingPNTI.cacheHit += 1
        if (lookupResult.isSeenBefore)
          underlyingPNTI.cacheHitFromDifferentTask += 1
      }
    } else
      pnti.cacheMiss += 1
  }

  override def lookupEndProxy(eq: EQ, underlying: PN[_], proxy: PN[_], cacheHit: Boolean, countMiss: Boolean): Unit = {
    val lt = LT.getOrAcquire(eq)
    val ctx = lt.ctx
    val pnti = ctx.getPNTI(underlying) // underlying node accounting

    // Every time proxy uses the result of the underlying node we 'move' the time over to proxy
    val underlyingTime = underlying.getSelfPlusANCTime
    // [SEE_PROXY_ANC]: consider addSelfPlusANCTime instead of 'set', results in higher node reuse time, fewer disabledCache nodes
    proxy.setSelfPlusANCTime(underlyingTime)
    // When proxy uses underlying (i.e. called proxy.completeFromNode(src)) we should exclude that time, it's not a real use
    pnti.nodeUsedTime -= underlyingTime
    if (cacheHit) { // [SEE_LOOKUP_END_HITS]
      pnti.cacheHit += 1
      if (proxy.isSeenBefore)
        pnti.cacheHitFromDifferentTask += 1
    } else if (countMiss)
      pnti.cacheMiss += 1
    trace.ensureProfileRecorded(pnti.id, underlying)
    lt.release()
  }

  override def lookupAdjustCacheHit(eq: EQ, hit: PN[_]): Unit = {
    val lt = LT.getOrAcquire(eq)
    val ctx = lt.ctx
    val pnti = ctx.getPNTI(hit) // underlying node accounting
    pnti.cacheHit += 1
    if (hit.isSeenBefore)
      pnti.cacheHitFromDifferentTask += 1
    lt.release()
  }

  override def lookupAdjustCacheStats(nti: NodeTaskInfo, hit: Boolean, startTime: Long): Unit = {
    val lt = LT.getOrAcquire()
    val ctx = lt.ctx
    trace.ensureProfileRecorded(nti.profile, nti)
    val pnti = ctx.getPNTI(nti.profile, OGTrace.BLOCK_ID_UNSCOPED) // underlying node accounting
    if (hit) pnti.cacheHit += 1
    else pnti.cacheMiss += 1
    pnti.cacheTime += OGTrace.nanoTime() - startTime
    lt.release()
  }

  override def lookupCollision(eq: EQ, collisionCount: Int): Unit = {
    val lt = LT.getOrAcquire(eq)
    val lCtx = lt.ctx
    lCtx.cacheCollisions += collisionCount
    lt.release()
  }

  override def evicted(task: NodeTask): Unit =
    if (task != null) {
      val lt = LT.getOrAcquire()
      lt.ctx.getPNTI(task, OGTrace.BLOCK_ID_UNSCOPED).evicted += 1
      lt.release()
    }

  override def invalidated(task: NodeTask): Unit = {
    val lt = LT.getOrAcquire()
    lt.ctx.getPNTI(task).invalidated += 1
    lt.release()
  }

  /**
   * TWEAK EVENTS
   */
  override def startTweakLookup(lCtx: LT): Long = OGTrace.nanoTime()

  override def stopTweakLookup(lCtx: LT, startTime: Long, tweakNTI: NodeTaskInfo): Unit = {
    val ctx = lCtx.ctx
    ctx.getPNTI(tweakNTI.profile, OGTrace.BLOCK_ID_UNSCOPED).tweakLookupTime += OGTrace.nanoTime - startTime
  }
}
