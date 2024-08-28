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
package optimus.core;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import optimus.graph.OGTraceStore;

public class EdgeIDList extends IntArrayList {
  public static EdgeIDList newDefault() {
    return new EdgeIDList();
  }

  public static EdgeIDList newSeqOps(int maxConcurrency) {
    return new EdgeIDListSequence(maxConcurrency);
  }

  public void addEnqueueEdge(int id) {
    super.add(-id); // Notice: negative value to record as enqueue edge
  }

  public int serializedSize() {
    return size() * 4 + 4 + 4; // All items + parentID + length prefix
  }

  public void serializeTo(OGTraceStore.Table table, int parentID) {
    table.putInt(parentID);
    table.putInt(size());
    for (int i = 0; i < size(); i++) table.putInt(getInt(i));
  }
}

class EdgeIDListSequence extends EdgeIDList {
  private int maxConcurrency;

  EdgeIDListSequence(int maxConcurrency) {
    this.maxConcurrency = maxConcurrency;
  }

  public int serializedSize() {
    return size() * 4 + 4 + 4 + 4; // All items + parentID + length prefix + maxConcurrency inverted
  }

  public void serializeTo(OGTraceStore.Table table, int parentID) {
    table.putInt(parentID);
    table.putInt(-maxConcurrency); // Also serves as the 'type' of the edge list
    table.putInt(size());
    for (int i = 0; i < size(); i++) table.putInt(getInt(i));
  }

  @Override
  public void addEnqueueEdge(int child) {
    // Ignore on purpose
  }
}
