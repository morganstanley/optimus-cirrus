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
package optimus.graph.diagnostics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import optimus.core.EdgeIDList;
import optimus.core.EdgeList;
import optimus.graph.AlreadyCompletedNode;
import optimus.graph.NodeTask;
import optimus.graph.NodeTaskInfo;
import optimus.graph.NodeTrace;
import optimus.graph.OGSchedulerContext;
import optimus.graph.PThreadContext;
import optimus.platform.ScenarioStack;
import optimus.platform.storable.Entity;

/**
 * Per instance of NodeTask, attached to a NodeTask when quick and continuous summaries are
 * requested
 */
public abstract class PNodeTask {
  static final NodeTask fakeTask = new AlreadyCompletedNode<>(Integer.MIN_VALUE);

  public static final String fakeInfoName = "fakeInfo";
  public static final PNodeTaskInfo fakeInfo =
      new PNodeTaskInfo(Integer.MIN_VALUE, "", fakeInfoName);

  public static final PNodeTaskInfo startNodeInfo =
      new PNodeTaskInfo(OGSchedulerContext.NODE_ID_ROOT, "", NodeTaskInfo.Names.startName);
  public static final PNodeTaskInfo syncEntryGroupInfo =
      new PNodeTaskInfo(Integer.MIN_VALUE, "", "[SyncEntryGroup]");

  private static final int IGNORE_RESULT_IN_COMPARE = 0x1;
  private static final int IGNORE_ARGUMENTS_IN_COMPARE = 0x2;
  private static final int SEEN_BEFORE = 0x4;

  @SuppressWarnings("StaticInitializerReferencesSubClass")
  public static final PNodeTask fake = new PNodeTaskRecorded(0);

  @SuppressWarnings("unused")
  private static final long serialVersionUID = 1L;

  /** Simple converter that skips non-live tasks */
  public static <T extends PNodeTask> ArrayList<NodeTask> toLive(Collection<T> src) {
    ArrayList<NodeTask> r = new ArrayList<>();
    for (PNodeTask task : src) {
      if (task.getTask() != null) r.add(task.getTask());
    }
    return r;
  }

  /** Simple converter that skips non-live tasks */
  public static ArrayList<PNodeTask> fromLive(ArrayList<NodeTask> src) {
    ArrayList<PNodeTask> r = new ArrayList<>();
    for (NodeTask task : src) {
      r.add(NodeTrace.accessProfile(task));
    }
    return r;
  }

  public long firstStartTime; // the timestamp at which the node was *first* started
  public long completedTime; // the timestamp at which the node was completed
  public long selfTime;
  public long ancSelfTime; // ancillary self time (sum of all direct non-cached self times)
  public long
      postCompleteAndSuspendTime; // Time between complete/suspend and stop (mostly notifying
  // waiters & processing xinfo)

  public int childNodeLookupCount;
  public long childNodeLookupTime;

  public int
      enqueuingPropertyId; // id of nearest "interesting" node, or negative f interesting ourselves
  public boolean traceSelfAndParents; // [SEE_PARTIAL_TRACE]
  public volatile boolean addedToTrace;
  private int inspectFlags; // Flags used in inspection of the trees

  public final boolean ignoreResultInComparison() {
    return (inspectFlags & IGNORE_RESULT_IN_COMPARE) != 0;
  }

  public final boolean ignoreArgumentsInComparison() {
    return (inspectFlags & IGNORE_ARGUMENTS_IN_COMPARE) != 0;
  }

  public final boolean seenBefore() {
    return (inspectFlags & SEEN_BEFORE) != 0;
  }

  public final void setIgnoreResultInComparison() {
    inspectFlags |= IGNORE_RESULT_IN_COMPARE;
  }

  public final void setIgnoreArgumentsInComparison() {
    inspectFlags |= IGNORE_ARGUMENTS_IN_COMPARE;
  }

  public final void setSeenBefore() {
    inspectFlags |= SEEN_BEFORE;
  }

  public abstract int id();

  public abstract int infoId();

  public abstract Object subProfile();

  public abstract NodeTask getTask();

  public abstract boolean isLive();

  /** The following variables needs only for some post processing */
  public int visitedID;

  public ArrayList<Span> spans;

  public ArrayList<PNodeTask> callers;
  public EdgeList callees;
  /**
   * If in recording mode edges are accumulated until the node completion Note: this affects GC and
   * slightly more efficient way is to create a pool of blocks in native memory
   */
  public EdgeIDList calleeIDs;

  @Override
  public String toString() {
    return toPrettyName(false, true);
  }

  public final long wallTime() {
    long wallTime = completedTime - firstStartTime;
    return wallTime < 0 ? 0 : wallTime;
  }

  @Override
  public int hashCode() {
    return id();
  }

  @SuppressWarnings(
      "EqualsWhichDoesntCheckParameterClass") // We will throw if someone tries a different object
  @Override
  public boolean equals(Object obj) {
    return obj != null && id() == ((PNodeTask) obj).id();
  }

  /** Returns true if time regions overlap */
  public final boolean isActive(long start, long end) {
    return firstStartTime <= end && completedTime >= start;
  }

  public final boolean isRunning(long start, long end) {
    if (spans == null) return false;
    for (Span span : spans) {
      if (span.intersect(start, end)) {
        return true;
      }
    }
    return false;
  }

  public static final class Span {
    public Span(long start, long end) {
      this.start = start;
      this.end = end;
    }

    public boolean intersect(long start, long end) {
      return (this.start <= end) && (this.end >= start);
    }

    public void setEnd(long end) {
      this.end = end;
    }

    long start;
    long end;
  }

  public final void resetRelationships() {
    callees = null;
    callers = null;
    spans = null;
  }

  public final ArrayList<PNodeTask> getCallers() {
    return callers == null ? EdgeList.empty : callers;
  }

  public final Iterator<PNodeTask> getCallees() {
    return callees == null ? EdgeList.empty.iterator() : callees.withoutEnqueues();
  }

  public final boolean hasEdges() {
    return (callees != null && !callees.isEmpty()) || (callers != null && !callers.isEmpty());
  }

  public final Iterator<PNodeTask> getCalleesWithEnqueuesWhenNotDone() {
    return callees == null ? EdgeList.empty.iterator() : callees.withNotDoneEnqueues();
  }

  public final EdgeList getCalleesWithEnqueues() {
    return callees == null ? EdgeList.empty : callees;
  }

  public final boolean hasEnqueueEdges() {
    return callees != null && callees.hasEnqueues();
  }

  public final void addCaller(PNodeTask n) {
    synchronized (this) {
      if (callers == null) callers = new ArrayList<>(4);
      callers.add(n);
    }
  }

  public abstract String stateAsString();

  public final boolean isUnattachedNode() {
    return !getCallees().hasNext() && getCallers().isEmpty();
  }

  public final boolean isSpeculativeProxy() {
    return isProxy() && getCallers().isEmpty() && getCallees().hasNext();
  }

  public String dependsOnTweakableString() {
    return "";
  }

  public String dependsOnTweakableMaskFixedWidth() {
    return "";
  }

  public String dependsOnTweakableMask() {
    return "";
  }

  public int dependsOnTweakableMaskBitCount() {
    return 0;
  }

  public boolean dependsOnIsPoison() {
    return false;
  }

  public abstract NodeName nodeName();

  /** Useless value for recording tasks */
  public Class<?> nodeClass() {
    return getClass();
  }

  public Object methodThis() {
    return null;
  }

  public Object methodThisKey() {
    return methodThis();
  }

  /** Returns entity, for live data and node task that is property node */
  public Entity getEntity() {
    return null;
  }

  /** Best effort for getting arguments */
  public Object[] args() {
    return null;
  }

  public abstract String resultAsString();

  /** for UI display */
  public abstract String resultDisplayString();

  /**
   * For live PNodeTask this could be just result, for recorded tasks, this could be
   * result/result.toString/hash
   */
  public abstract Object resultKey();

  public ScenarioStack scenarioStack() {
    return null;
  }

  public Object scenarioStackCacheID() {
    return "Not collected";
  }

  public boolean isDone() {
    return true;
  }

  public boolean isDoneWithResult() {
    return true;
  }

  public boolean isDoneWithException() {
    return false;
  }

  public long getSelfPlusANCTime() {
    return selfTime + ancSelfTime;
  }

  /** Short name for some display features */
  public final String nameAndModifier() {
    return nodeName().nameAndModifier();
  }

  public final String toPrettyName(boolean entityType, boolean simpleName) {
    return toPrettyName(entityType, simpleName, false);
  }

  public abstract String toPrettyName(boolean entityType, boolean simpleName, boolean includeHint);

  public void printSource() {
    System.out.println("Not collected");
  }

  public abstract String propertyFlagsAsString();

  public abstract String propertyCachePolicy();

  public abstract boolean isScenarioIndependent();

  public abstract boolean isInternal();

  public abstract boolean isTweakNode();

  public abstract boolean isCacheable();

  public abstract boolean isProxy();

  public abstract boolean isUITrack();

  public PThreadContext.Process process() {
    return PThreadContext.Process.none;
  }
}
