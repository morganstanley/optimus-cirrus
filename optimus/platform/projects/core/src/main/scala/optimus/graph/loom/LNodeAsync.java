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
package optimus.graph.loom;

import java.io.Serial;
import optimus.graph.CompletableNode;
import optimus.graph.HNCMoniker;
import optimus.graph.NodeTaskInfo;
import optimus.graph.OGSchedulerContext;

/**
 * NodeMetaFactory uses this class as a base for @async(exposeArgTypes = true) and @node on
 * non-entities
 */
@SuppressWarnings("unused")
public abstract class LNodeAsync extends CompletableNode<Object> {
  @Override
  public void run(OGSchedulerContext ec) {
    completeWithResult(func(), ec);
  }

  /** Instance of the method wrapped into LNodeAsync */
  protected abstract Object entity();

  @Override
  public NodeTaskInfo executionInfo() {
    return NodeTaskInfo.NonEntityNode;
  }

  @Serial
  protected Object writeReplace() {
    return new HNCMoniker(this, entity());
  }

  @Override
  public int getProfileId() {
    return LPropertyDescriptor.get(_clsID()).profileID;
  }
}
