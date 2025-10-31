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

import optimus.graph.NodeAwaiter;
import optimus.graph.NodeTask;
import optimus.graph.NodeTaskInfo;
import optimus.graph.OGLocalTables;
import optimus.graph.PluginType;
import optimus.graph.PropertyNode;
import optimus.graph.diagnostics.messages.OGEvent;
import optimus.platform.EvaluationQueue;

public abstract class OGEventsObserver implements Cloneable {
  public boolean traceNodes; // Records actual nodes in memory (NodeTrace.trace, NodeTrace.roots)

  public OGEventsObserver createCopy() {
    try {
      return (OGEventsObserver) clone();
    } catch (CloneNotSupportedException ignored) {
    }
    return null; // shouldn't happen since we implement cloneable
  }

  OGEventsObserver() {}

  /** Name can be supplied via arg/java args etc.... */
  public abstract String name();
  /**
   * Not as long as a description but more informative than name, UI should probably display this
   */
  public String title() {
    return name();
  }
  /** A long description text, UI should show them in tooltips */
  public abstract String description();

  public abstract boolean requiresNodeID();

  public abstract boolean writesDirectlyToTrace();

  /**
   * Anything deriving from Hotspots or NewHotspots returns true - the difference is in
   * writesDirectlyToTrace
   */
  public abstract boolean collectsHotspots();

  public abstract boolean recordLostConcurrency();

  public abstract boolean traceTweaks();

  public abstract boolean traceWaits();

  public boolean collectsTimeLine() {
    return false;
  }

  public boolean liveProcess() {
    return true;
  }

  public boolean collectsEnqueuing() {
    return false;
  }

  public boolean collectsCacheDetails() {
    return false;
  }

  public boolean collectsAccurateCacheStats() {
    return false;
  }

  public boolean ignoreReset() {
    return false;
  }

  public boolean supportsReadingFromFile() {
    return false;
  }
  /** Used in test to enable conditional testing */
  public boolean supportsProfileBlocks() {
    return true;
  }

  public boolean includeInUI() {
    return false;
  }

  @Override
  public final String toString() {
    return name();
  }

  public static NodeTaskInfo profileInfo(NodeTask task) {
    return task instanceof PropertyNode<?>
        ? ((PropertyNode<?>) task).propertyInfo()
        : task.executionInfo();
  }

  /** NODE EVENTS */
  public void startAfterSyncStack(OGLocalTables lCtx, NodeTask task) {}

  public void stop(OGLocalTables lCtx, NodeTask task) {}

  public void suspend(EvaluationQueue eq, NodeAwaiter awaiter) {}

  public void initializeAsCompleted(NodeTask task) {}

  // The following events are called via OGTrace.XXX front functions only
  // Which provide common (non-observer specific) functionality
  public void dependency(NodeTask fromTask, NodeTask toTask, EvaluationQueue eq) {}

  public void enqueue(NodeTask fromTask, NodeTask toTask) {}

  /** GRID EVENTS */
  public void publishNodeSending(NodeTask task, long time) {}

  public void publishNodeReceived(NodeTask task) {}

  public void publishNodeStolenForLocalExecution(NodeTask task) {}

  public void publishNodeExecuted(NodeTask task) {}

  public void publishNodeResultReceived(NodeTask task) {}

  public void publishSerializedNodeResultArrived(NodeTask task) {}

  public boolean resetBetweenTasks() {
    return true;
  }

  // TODO (OPTIMUS-43368): Generalize for any custom logic e.g. TweakNode
  public void enqueueFollowsSequenceLogic(NodeTask task, int maxConcurrency) {}

  public void start(OGLocalTables prfCtx, NodeTask task, boolean isNew) {}

  // some internal nodes (e.g. UTrack) don't 'start' but run multiple child nodes
  public void manualStart(NodeTask task) {}

  public void completed(EvaluationQueue eq, NodeTask task) {}

  public void adapted(EvaluationQueue eq, PluginType pluginType) {}

  /** GRAPH EVENTS */
  public void graphEnter(OGLocalTables lCtx) {}

  public void graphExit(OGLocalTables lCtx) {}

  public void graphEnterWait(OGLocalTables lCtx, int causalityID) {}

  public void graphExitWait(OGLocalTables lCtx) {}

  public void graphSpinEnter(OGLocalTables lCtx) {}

  public void graphSpinExit(OGLocalTables lCtx) {}

  /** CACHE EVENTS */
  public long lookupStart() {
    return 0;
  }

  public void lookupCollision(EvaluationQueue eq, int collisionCount) {}

  public void lookupEnd(OGLocalTables lCtx, long startTime, NodeTask key, NodeTask lookupResult) {}

  public void lookupEndPartial(
      OGLocalTables lCtx,
      long startTime,
      PropertyNode<?> parent,
      PropertyNode<?> task,
      boolean countTowardsLookup) {}

  public void lookupEndProxy(
      EvaluationQueue eq,
      PropertyNode<?> underlying,
      PropertyNode<?> proxy,
      boolean cacheHit,
      boolean countMiss) {}

  public void lookupAdjustCacheHit(EvaluationQueue eq, PropertyNode<?> hit) {}

  public void lookupAdjustCacheStats(NodeTaskInfo nti, boolean hit, long startTime) {}

  /**
   * Hint comes from a user who can supply the flag to a scenario, it can be ignored by PGO system
   */
  public LookupState lookupStartScenario(boolean hintDontCache, Object couldBeGiven) {
    return hintDontCache ? LookupState.NoCache : LookupState.Default;
  }

  public void lookupAdjustCacheStats(LookupState ls, boolean hit) {}

  public void evicted(NodeTask task) {}

  public void invalidated(NodeTask task) {}

  public void reuseUpdate(NodeTask task, int count) {}

  /** TRACKING EVENTS */
  public void nodeHashCollision(NodeTask task) {}

  /** TWEAK EVENTS */
  public long startTweakLookup(OGLocalTables lCtx) {
    return 0;
  }

  public void stopTweakLookup(OGLocalTables lCtx, long startTime, NodeTaskInfo tweakNTI) {}

  /** CUSTOM EVENTS */
  public boolean isCollectingEvents() {
    return false;
  }

  public void writeEvent(OGEvent event) {}

  public void writeEventComplete(int counterID, int id) {}

  public void markEndOfCycle() {}
}
