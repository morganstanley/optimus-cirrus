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

import optimus.core.EdgeIDList;
import optimus.core.EdgeList;
import optimus.graph.AlreadyCompletedNode;
import optimus.graph.NodeClsIDSupport;
import optimus.graph.NodeTask;
import optimus.graph.NodeTaskInfo;
import optimus.graph.NodeTrace;
import optimus.graph.PropertyNode;
import optimus.graph.ProxyPropertyNode;
import optimus.graph.TweakNode;
import optimus.graph.cache.NCPolicy;
import optimus.platform.ScenarioStack;
import optimus.platform.storable.Entity;

public class PNodeTaskLive extends PNodeTask {
  private NodeTask ntsk; // For live traces this points to a underlying NodeTask

  public PNodeTaskLive(NodeTask ntsk) {
    if (ntsk == null) throw new IllegalArgumentException("task argument cannot be null");
    this.ntsk = ntsk;
  }

  @Override
  public int id() {
    return ntsk.getId();
  }

  @Override
  public NodeTask getTask() {
    return ntsk;
  }

  @Override
  public boolean isLive() {
    return true;
  }

  @Override
  public int infoId() {
    return ntsk.getProfileId();
  }

  @Override
  public Object subProfile() {
    return ntsk.subProfile();
  }

  @Override
  public String stateAsString() {
    return ntsk.stateAsString();
  }

  @Override
  public String dependsOnTweakableString() {
    return NodeTrace.dependsOnTweakableString(ntsk);
  }

  @Override
  public String dependsOnTweakableMaskFixedWidth() {
    return NodeTrace.dependsOnTweakableMaskFixedWidth(ntsk);
  }

  public String dependsOnTweakableMask() {
    return NodeTrace.dependsOnTweakableMask(ntsk);
  }

  @Override
  public int dependsOnTweakableMaskBitCount() {
    return NodeTrace.dependsOnTweakableMaskBitCount(ntsk);
  }

  @Override
  public boolean dependsOnIsPoison() {
    return NodeTrace.dependsOnIsPoison(ntsk);
  }

  @Override
  public NodeName nodeName() {
    return ntsk.nodeName();
  }

  @Override
  public Class<?> nodeClass() {
    return ntsk.getClass();
  }

  @Override
  public Object methodThis() {
    return ntsk.methodThis();
  }

  @Override
  public Entity getEntity() {
    return ntsk instanceof PropertyNode<?> ? ((PropertyNode<?>) ntsk).entity() : null;
  }

  @Override
  public Object[] args() {
    var args = ntsk.args();
    return args != NodeTask.argsUnknown ? args : NodeClsIDSupport.getArgs(ntsk);
  }

  @Override
  public ScenarioStack scenarioStack() {
    return ntsk.scenarioStack();
  }

  @Override
  public Object scenarioStackCacheID() {
    return ntsk.scenarioStack()._cacheID();
  }

  @Override
  public boolean isDone() {
    return ntsk.isDone();
  }

  @Override
  public boolean isDoneWithResult() {
    return ntsk.isDoneWithResult();
  }

  @Override
  public boolean isDoneWithException() {
    return ntsk.isDoneWithException();
  }

  @Override
  public String resultAsString() {
    return ntsk.resultAsString();
  }

  @Override
  public String resultDisplayString() {
    if (ntsk instanceof AlreadyCompletedNode<?>) return resultAsString();
    return (ntsk instanceof ProxyPropertyNode<?> || !isInternal()) ? resultAsString() : "";
  }

  @Override
  public Object resultKey() {
    if (ntsk.isDoneWithResult()) return ntsk.resultObject();
    else if (ntsk.isDoneWithException()) return ntsk.exception();
    return ntsk.isInvalidCache() ? "[Invalidated]" : "[Node Has Not Completed]";
  }

  @Override
  public long getSelfPlusANCTime() {
    // For some hotspots live modes (not tracing) this field is directly computed on the node itself
    return ntsk.getSelfPlusANCTime() != 0 ? ntsk.getSelfPlusANCTime() : super.getSelfPlusANCTime();
  }

  @Override
  public String toPrettyName(boolean entityType, boolean simpleName, boolean includeHint) {
    return ntsk.toPrettyName(entityType, false, false, simpleName, false, includeHint);
  }

  @Override
  public void printSource() {
    NodeName.printSource(ntsk);
  }

  @Override
  public String propertyFlagsAsString() {
    return ntsk.executionInfo().flagsAsString();
  }

  @Override
  public String propertyCachePolicy() {
    NodeTaskInfo info = ntsk.executionInfo();
    if (info.isScenarioIndependent()) return NCPolicy.SCENARIO_INDEPENDENT_POLICY_NAME;
    String policyName = info.cachePolicy().policyName();
    return policyName != null ? policyName : NCPolicy.DEFAULT_POLICY_NAME;
  }

  @Override
  public boolean isScenarioIndependent() {
    return ntsk.executionInfo().isScenarioIndependent();
  }

  @Override
  public boolean isInternal() {
    return ntsk.executionInfo().isInternal();
  }

  @Override
  public boolean isTweakNode() {
    return ntsk instanceof TweakNode<?>;
  }

  @Override
  public boolean isCacheable() {
    return ntsk.executionInfo().getCacheable();
  }

  @Override
  public boolean isProxy() {
    return ntsk.executionInfo().isProxy();
  }

  @Override
  public boolean isUITrack() {
    return ntsk.executionInfo() == NodeTaskInfo.UITrack;
  }

  /**
   * Callee(s) are added and dealt with in general 1 by 1 therefore no synchronization is required
   * here 'Live' means ntsk is not null
   */
  public final void addLiveCallee(PNodeTaskLive callee) {
    if (callees == null) callees = EdgeList.newDefault();
    callees.add(callee);
  }

  public final void addLiveEnqueued(PNodeTaskLive n) {
    if (callees == null) callees = EdgeList.newDefault();
    callees.addEnqueueEdge(n);
  }

  /** Should reduce duplication here, see addLiveCallee */
  public final void addLiveCalleeID(int id) {
    if (calleeIDs == null) calleeIDs = EdgeIDList.newDefault();
    calleeIDs.add(id);
  }

  /** Should reduce duplication here, see addLiveEnqueued */
  public final void addLiveIDEnqueued(int id) {
    if (calleeIDs == null) calleeIDs = EdgeIDList.newDefault();
    calleeIDs.addEnqueueEdge(id);
  }
}
