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

import optimus.platform.EvaluationQueue;

/**
 * This can really be an interface. However because a call via interface is just a drop slower it
 * will stay a class until we really need it
 *
 * <p>node.continueWith(new NodeAwaiter { override def onChildCompleted(eq: EvaluationQueue, child:
 * NodeTask) = ... work on node completion (node eq child)... })
 */
public abstract class NodeAwaiter {

  /**
   * Used for resolving dependencies and cycle detection a.k.a. forward edge... It's only valid
   * while waiting and will be reset afterwards
   */
  private transient NodeTask _waitingOn;

  public final NodeTask getWaitingOn() {
    return _waitingOn;
  }

  public final void setWaitingOn(NodeTask node) {
    // OGScheduler.wait() will walk the forward dependency chain and setting awaiter will expose to
    // scheduling. The node has to be ready to run at this point. This check is parallel to the one
    // in OGSchedulerContext.enqueue.
    if (Settings.schedulerAsserts && node != null && node.scenarioStack() == null)
      throw new GraphInInvalidState();
    _waitingOn = node;
  }

  /**
   * When the node completes its work it will call all the registered waiters. registration is done
   * which continueWith(...)
   */
  protected abstract void onChildCompleted(EvaluationQueue eq, NodeTask node);

  /** Used for dependency walkers */
  NodeCause awaiter() {
    return null;
  }

  protected boolean breakProxyChains(EvaluationQueue eq) {
    return false;
  }
}
