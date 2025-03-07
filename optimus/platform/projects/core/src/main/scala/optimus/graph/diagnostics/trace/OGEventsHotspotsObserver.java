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
package optimus.graph.diagnostics.trace;

import static optimus.graph.OGTrace.trace;
import optimus.graph.GraphInInvalidState;
import optimus.graph.NodeAwaiter;
import optimus.graph.NodeTask;
import optimus.graph.NodeTaskInfo;
import optimus.graph.NodeTrace;
import optimus.graph.OGLocalTables;
import optimus.graph.OGTrace;
import optimus.graph.PThreadContext;
import optimus.graph.PropertyNode;
import optimus.graph.Settings;
import optimus.graph.diagnostics.PNodeTask;
import optimus.platform.EvaluationQueue;

// OLD hotspots observer
public class OGEventsHotspotsObserver extends OGEventsGlobalGraphObserver {
  @Override
  public String name() {
    return "hotspots";
  }

  OGEventsHotspotsObserver() {
    super();
  }

  @Override
  public String description() {
    return "<html>Per property counters are recorded<br>&lt;10% impact on CPU and minimal impact on memory</html>";
  }

  @Override
  public boolean requiresNodeID() {
    return true;
  }

  // currently unused, but a useful way of checking that hotspots are already written to ogtrace
  @Override
  public boolean writesDirectlyToTrace() {
    return true;
  }

  @Override
  public boolean collectsHotspots() {
    return true;
  }

  @Override
  public boolean collectsAccurateCacheStats() {
    return true;
  }

  @Override
  public boolean recordLostConcurrency() {
    return true;
  }

  @Override
  public boolean collectsEnqueuing() {
    return true;
  }

  @Override
  public boolean collectsCacheDetails() {
    return true;
  }

  /**************************************************************************************************************
   * NODE EVENTS:
   * void start(OGLocalTables lCtx, NodeTask task, boolean isNew)
   * void startAfterSyncStack(OGLocalTables lCtx, NodeTask task)
   * void stop(OGLocalTables lCtx, NodeTask task)
   * void suspend(EvaluationQueue eq, NodeAwaiter awaiter)
   * void completed(EvaluationQueue eq, NodeTask task)
   * void initializeAsCompleted(NodeTask task, NodeTaskInfo nti)
   * void dependency(NodeTask fromTask, NodeTask toTask)
   * void enqueue(NodeTask fromTask, NodeTask toTask)
   **************************************************************************************************************/

  @Override
  public void start(OGLocalTables lCtx, NodeTask task, boolean isNew) {
    long time = OGTrace.nanoTime();
    PThreadContext ctx = lCtx.ctx;

    // This is the case of sync entry into another node
    if (ctx.ntsk != null) stop(lCtx, ctx.ntsk, ctx.startTime, time);
    start(lCtx, task, time, isNew);
  }

  @Override
  public void startAfterSyncStack(OGLocalTables lCtx, NodeTask task) {
    // When a child node didn't run (e.g. it was already done) node is not restarting
    if (lCtx.ctx.ntsk == null) start(lCtx, task, OGTrace.nanoTime(), false);
  }

  @Override
  public void manualStart(NodeTask task) {
    PNodeTask pnt = NodeTrace.accessProfile(task);
    pnt.firstStartTime = OGTrace.nanoTime();
  }

  @Override
  public void stop(OGLocalTables lCtx, NodeTask task) {
    PThreadContext ctx = lCtx.ctx;
    long time = OGTrace.nanoTime();
    ctx.resetRunningTask();
    stop(lCtx, task, ctx.startTime, time);
  }

  @Override
  public void suspend(EvaluationQueue eq, NodeAwaiter awaiter) {
    long suspendTime = OGTrace.nanoTime();
    OGLocalTables lCtx = OGLocalTables.getOrAcquire(eq);
    PThreadContext ctx = lCtx.ctx;
    // We're only dealing with the case where the current node is suspending - we don't have any
    // time to attribute
    // if some other waiter is being added.
    if (ctx.ntsk == awaiter) {
      NodeTask task = ctx.ntsk; // (== awaiter)

      // a completed node shouldn't be suspending (why would it?) and we don't want to update
      // selfTime after
      // completion as noted elsewhere
      if (Settings.schedulerAsserts && task.isDone())
        throw new GraphInInvalidState("OGTrace.suspend called on completed node " + task);

      ctx.nodeSuspendOrCompleteTime = suspendTime;

      PNodeTask pnt = NodeTrace.accessProfile(task);
      suspend(ctx, pnt, suspendTime);
    }
    lCtx.release();
  }

  protected void suspend(PThreadContext ctx, PNodeTask pnt, long suspendTime) {
    pnt.selfTime += suspendTime - ctx.startTime;
  }

  @Override
  public void completed(EvaluationQueue eq, NodeTask task) {
    OGLocalTables lt = OGLocalTables.getOrAcquire(eq);
    PThreadContext ctx = lt.ctx;
    long completedTime = OGTrace.nanoTime();
    long startTime;
    PNodeTask pnt = NodeTrace.accessProfile(task);
    if (ctx.ntsk == task) {
      // this is the usual case (completed while running, rather than completed asynchronously e.g.
      // by a plugin), so capture the self time from start or suspend up to completion
      // (time after completion but before stop is captured as postCompleteTime)
      startTime =
          lt.ctx.nodeSuspendOrCompleteTime != 0 ? lt.ctx.nodeSuspendOrCompleteTime : ctx.startTime;
      lt.ctx.nodeSuspendOrCompleteTime = completedTime;
    } else {
      // case when we complete while not running, so the best we can do is a point-in-time event
      startTime = completedTime;
    }

    int pid = task.getProfileId();
    int blk = task.profileBlockID();
    trace.ensureProfileRecorded(pid, task);

    completed(lt, pnt, blk, task.getId(), pid, startTime, completedTime, false);
    lt.release();
  }

  protected void completed(
      OGLocalTables lCtx,
      PNodeTask pnt,
      int blk,
      int taskId,
      int pid,
      long startTime,
      long completedTime,
      boolean initAsCompleted) {
    pnt.completedTime = completedTime;
    pnt.selfTime += completedTime - startTime; // (might be zero if we're not currently running)
    lCtx.eventsTrace.summary(
        blk,
        taskId,
        pid, // IDs
        pnt.firstStartTime,
        pnt.completedTime,
        pnt.selfTime,
        pnt.ancSelfTime,
        pnt.postCompleteAndSuspendTime,
        pnt.childNodeLookupCount,
        pnt.childNodeLookupTime,
        pnt.enqueuingPropertyId,
        initAsCompleted);
  }

  private PNodeTask initializeAsCompleted(NodeTask task, NodeTaskInfo nti, long time) {
    PNodeTask pnt = NodeTrace.accessProfile(task);
    trace.ensureProfileRecorded(nti.profile, nti);
    pnt.firstStartTime = time;
    if (task.executionInfo() != NodeTaskInfo.Start) {
      var lt = OGLocalTables.getOrAcquire();
      completed(
          lt, pnt, task.profileBlockID(), task.getId(), task.getProfileId(), time, time, true);
      lt.release();
    }
    return pnt;
  }

  @Override
  public void initializeAsCompleted(NodeTask task) {
    NodeTaskInfo nti = OGEventsObserver.profileInfo(task);
    if (nti == NodeTaskInfo.Start) return;
    if (nti.isTraceSelfAndParent()) {
      long timestamp = OGTrace.nanoTime();
      PNodeTask pnt = initializeAsCompleted(task, nti, timestamp);
      pnt.traceSelfAndParents = true;
    }
  }

  @Override
  public void dependency(NodeTask fromTask, NodeTask toTask, EvaluationQueue eq) {
    OGLocalTables lCtx = OGLocalTables.getOrAcquire(eq);
    PNodeTask caller = NodeTrace.accessProfile(fromTask);
    PNodeTask callee = NodeTrace.accessProfile(toTask);

    if (fromTask.getId() != 0) {
      long ancTime = callee.ancSelfTime + callee.selfTime;
      if (ancTime != 0) { // Would be 0 for AlreadyCompletedNodes for example
        if (!toTask.executionInfo().getCacheable()) caller.ancSelfTime += ancTime;
        else {
          int blk = fromTask.profileBlockID();
          lCtx.eventsTrace.cacheReuse(blk, toTask.getProfileId(), ancTime);
        }
      }
    }
    lCtx.release();
  }

  @Override
  public void enqueue(NodeTask fromTask, NodeTask toTask) {
    PNodeTask toPnt = NodeTrace.accessProfile(toTask);
    if (toPnt.enqueuingPropertyId == 0) {
      NodeTaskInfo toNti = toTask.executionInfo();
      // If this is a property or raw node, then it is its own enqueuing property, i.e. an
      // "important" node.
      if (toNti.entityInfo != null || toNti.isAttributableUserCode())
        setAsOwnEnqueuingProperty(toTask, toPnt);
      else {
        PNodeTask fromPnt = NodeTrace.accessProfile(fromTask);
        // If immediate enqueuer has no enqueuing property, assume we're important after all,
        if (fromPnt.enqueuingPropertyId == 0) setAsOwnEnqueuingProperty(toTask, toPnt);
        // otherwise inherit it
        else toPnt.enqueuingPropertyId = Math.abs(fromPnt.enqueuingPropertyId);
      }
      // Don't write to trace yet - defer until task is done.
    }
  }

  /**************************************************************************************************************
   * private helper methods for node events APIs above
   **************************************************************************************************************/

  private void start(OGLocalTables lCtx, NodeTask task, long time, boolean firstRun) {
    lCtx.ctx.taskStart(task, time);
    start(task, firstRun, time);
  }

  protected void start(NodeTask task, boolean firstRun, long time) {
    PNodeTask pnt = NodeTrace.accessProfile(task);
    // If for some reason startTime wasn't initialized (e.g. hotspots not on yet),
    // start "recording" now.
    if (firstRun || pnt.firstStartTime == 0) pnt.firstStartTime = time;
  }

  private void stop(OGLocalTables lCtx, NodeTask task, long startTime0, long stopTime) {
    long startTime = startTime0 > 0 ? startTime0 : stopTime;
    // currently context selfTime includes the whole period between start and stop (but should it?)
    lCtx.ctx.selfTime += stopTime - startTime;

    int blk = task.profileBlockID();
    PNodeTask pnt = NodeTrace.accessProfile(task);

    long suspendOrCompleteTime = lCtx.ctx.nodeSuspendOrCompleteTime;
    long selfTime =
        suspendOrCompleteTime == 0 ? stopTime - startTime : suspendOrCompleteTime - startTime;

    stop(lCtx, pnt, task, suspendOrCompleteTime, startTime, stopTime, selfTime, blk);
    lCtx.ctx.nodeSuspendOrCompleteTime = 0;
  }

  protected void stop(
      OGLocalTables lCtx,
      PNodeTask pnt,
      NodeTask task,
      long suspendOrCompleteTime,
      long startTime,
      long stopTime,
      long selfTime,
      int blk) {
    long postCompleteAndSuspendTime = stopTime - startTime - selfTime;
    if (suspendOrCompleteTime != 0)
      // note that we don't add to selfTime if the node is suspended or complete, because that was
      // done already in
      // OGTrace.suspend or OGTrace.complete
      pnt.postCompleteAndSuspendTime += postCompleteAndSuspendTime;
    else if (!task.isDone())
      // the node started and stopped without completing or "suspending" by waiting on another node.
      // It's probably
      // adapted or manually written, or it's temporarily stopping due to a syncstack, so we'll
      // attribute all of this
      // time to selfTime
      pnt.selfTime += selfTime;

    // There can be material work between completing a node and stopping for the last time
    // (notifying waiters,
    // updating _xinfo etc.). We capture this separately from selfTime partly to give better detail,
    // and
    // partly because we can't safely add to selftime after we are completed (because if we did, the
    // ANC
    // time reported by our caller could differ depending on whether they took dependency on us
    // before or after we
    // stopped (it will always be after we complete, so that part is ok)), and also the  times
    // wouldn't get included
    // in the summary trace written by OGTrace.completed.
    if (task.isDone() && postCompleteAndSuspendTime != 0) {
      int pid = task.getProfileId();
      lCtx.eventsTrace.postComplete(blk, pid, postCompleteAndSuspendTime);
    }
  }

  private static void setAsOwnEnqueuingProperty(NodeTask task, PNodeTask pnt) {
    int epid = task.getProfileId();
    trace.ensureProfileRecorded(epid, task);
    pnt.enqueuingPropertyId = -epid; // note, negative
  }

  /**************************************************************************************************************
   * CACHE EVENTS:
   * void lookupStart(OGLocalTables lCtx)
   * long lookupStartLight()
   * void lookupEnd(OGLocalTables lCtx, NodeTask task, NodeTask lookupResult)
   * void lookupCollision(EvaluationQueue eq, int collisionCount)
   * void lookupAddTime(EvaluationQueue eq, PropertyNode<?> task, long startTime)
   * void xsLookupAddTime(EvaluationQueue eq, PropertyNode<?> task, long startTime)
   * void lookupProxyCompleted(EvaluationQueue eq, PropertyNode<?> key, PropertyNode<?> hit)
   * void evicted(NodeTask task)
   * void invalidated(NodeTask task)
   * void reuseUpdate(NodeTask task, int rcount)
   **************************************************************************************************************/

  @Override
  public long lookupStart() {
    return OGTrace.nanoTime();
  }

  @Override
  public void lookupEnd(OGLocalTables lt, long startTime, NodeTask task, NodeTask lookupResult) {
    long time = lookupEndCommon(lt.ctx, startTime, lookupResult);

    var cacheCollisions = lt.ctx.cacheCollisions;
    lt.ctx.cacheCollisions = 0;
    int pid = lookupResult.getProfileId();
    trace.ensureProfileRecorded(pid, lookupResult);

    int blk = task.profileBlockID(); // this is questionable - blk from task not lookupResult?
    if (task != lookupResult.cacheUnderlyingNode())
      if (task.scenarioStack()._cacheID() != lookupResult.scenarioStack()._cacheID())
        lt.eventsTrace.cacheHit(
            blk, task.getProfileId(), cacheCollisions, time); // [SEE_PROXY_NON_TRIVIAL]
      else lt.eventsTrace.cacheHit(blk, pid, cacheCollisions, time);
    else lt.eventsTrace.cacheMiss(blk, pid, cacheCollisions, time);
  }

  @Override
  public void lookupCollision(EvaluationQueue eq, int collisionCount) {
    OGLocalTables lCtx = OGLocalTables.getOrAcquire(eq);
    lCtx.ctx.cacheCollisions += collisionCount;
    lCtx.release();
  }

  @Override
  public void lookupEndPartial(
      OGLocalTables lCtx,
      long startTime,
      PropertyNode<?> parent,
      PropertyNode<?> task,
      boolean countTowardsLookup) {
    long cacheTime = lookupEndCommon(lCtx.ctx, startTime, task, countTowardsLookup);
    int blk = task.profileBlockID();
    lCtx.eventsTrace.cacheLookup(blk, task.getProfileId(), cacheTime);
  }

  private long lookupEndCommon(PThreadContext ctx, long startTime, NodeTask task) {
    return lookupEndCommon(ctx, startTime, task, true);
  }

  private long lookupEndCommon(
      PThreadContext ctx, long startTime, NodeTask task, boolean countTowardsLookup) {
    long cacheTime = OGTrace.nanoTime() - startTime;
    ctx.lookupEnd(cacheTime, task);

    // [CHILD_NODE_LOOKUP]
    // [SEE_RACEY_SPECULATIVE_INSERTION]
    if (ctx.ntsk != null) {
      // ctx.ntsk can be null when non graph node is asking for a node
      PNodeTask pnt = NodeTrace.accessProfile(ctx.ntsk);
      if (countTowardsLookup) pnt.childNodeLookupCount++;
      // need to add to childNodeLookupTime even if not to count to not have any gaps in profiling
      pnt.childNodeLookupTime += cacheTime;
    }
    return cacheTime;
  }

  @Override
  public void lookupEndProxy(
      EvaluationQueue eq,
      PropertyNode<?> underlying,
      PropertyNode<?> proxy,
      boolean cacheHit,
      boolean countMiss) {
    // same logic as in dependency
    PNodeTask parent = NodeTrace.accessProfile(proxy);
    PNodeTask child = NodeTrace.accessProfile(underlying);

    // Every time proxy uses the result of the underlying node we 'move' the time over to proxy
    long underlyingTime = child.ancSelfTime + child.selfTime;
    // [SEE_PROXY_ANC]: consider += instead of =, results in higher node reuse time, and fewer
    // disabledCache nodes
    parent.ancSelfTime = underlyingTime;

    int blk = proxy.profileBlockID();
    OGLocalTables lCtx = OGLocalTables.getOrAcquire(eq);
    lCtx.eventsTrace.cacheProxy(
        blk, underlying.getProfileId(), cacheHit, countMiss, underlyingTime);
    lCtx.release();
  }

  @Override
  public void evicted(NodeTask task) {
    if (task != null) {
      int blk = task.profileBlockID();
      var lCtx = OGLocalTables.getOrAcquire();
      lCtx.eventsTrace.cacheEvicted(blk, task.getProfileId());
      lCtx.release();
    }
  }

  @Override
  public void invalidated(NodeTask task) {
    if (task != null) { // task can be null here???
      int blk = task.profileBlockID();
      OGLocalTables lCtx = OGLocalTables.getOrAcquire();
      lCtx.eventsTrace.cacheInvalidated(blk, task.getProfileId());
      lCtx.release();
    }
  }

  @Override
  public void reuseUpdate(NodeTask task, int rcount) {
    NodeTaskInfo info = task.executionInfo();
    if (rcount > info.reuseCycle) {
      info.reuseCycle = rcount;
      OGLocalTables lCtx = OGLocalTables.getOrAcquire();
      lCtx.eventsTrace.reuseCycle(task.getProfileId(), rcount);
      lCtx.release();
    }
  }

  /**************************************************************************************************************
   * TRACKING EVENTS
   */
  @Override
  public void nodeHashCollision(NodeTask task) {
    OGLocalTables lCtx = OGLocalTables.getOrAcquire();
    lCtx.eventsTrace.nodeHashCollision(task.getProfileId());
    lCtx.release();
  }

  /**************************************************************************************************************
   * TWEAK EVENTS:
   * void startTweakLookup(OGLocalTables lCtx)
   * void stopTweakLookup(OGLocalTables lCtx, NodeTaskInfo tweakNTI)
   **************************************************************************************************************/

  @Override
  public long startTweakLookup(OGLocalTables lCtx) {
    return OGTrace.nanoTime();
  }

  @Override
  public void stopTweakLookup(OGLocalTables lCtx, long startTime, NodeTaskInfo tweakNTI) {
    long stopTime = OGTrace.nanoTime();
    long tweakLookupTime = stopTime - startTime;
    int pid = tweakNTI.profile;
    lCtx.eventsTrace.tweakLookup(pid, tweakLookupTime);
  }
}
