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
package optimus.graph;

import java.util.ArrayList;

import optimus.platform.EvaluationQueue;

public abstract class NodeCauseInfo extends NodeAwaiter implements NodeCause {
  @Override
  public final void onChildCompleted(EvaluationQueue eq, NodeTask node) {}

  @Override
  public final NodeCause awaiter() {
    return this;
  }

  public abstract String details();
}
/**
 * this interface encapsulated some information that is metadata about why a task was scheduled this
 * information comes from the NodeTask.waitersToCallStack. Usually the node stack provides
 * sufficient information to detail/diagnose, but sometimes nodes can seem to have no cause (e.g. in
 * a TrackingScenario, when nodes are scheduled with a completion, rather than FSM) In these cases a
 * NodeCauseInfo may be attached to the node to provide some diagnosis
 *
 * @see NodeTask#waitersToNodeStack
 */
// The only direct implementations of this are NodeCauseInfo and NodeTask
// only used in NodeTask#waitersToFullMultilineNodeStack
interface NodeCause {}
