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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToIntFunction;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import optimus.graph.loom.LPropertyDescriptor;

/**
 * A Utility to keep track of the NodeTaskInfo that we encounter used by both core and entityagent.
 */
public class NodeTaskInfoRegistry {

  private static final Object2IntMap<LPropertyDescriptor> descriptorIDs =
      new Object2IntOpenHashMap<>();

  private static LPropertyDescriptor[] descriptors = new LPropertyDescriptor[1024];

  private static final AtomicInteger counter = new AtomicInteger(1); // skipping PROFILE_ID_INVALID

  public static int nextId() {
    return counter.getAndIncrement();
  }

  public static int currentTotal() {
    return counter.get();
  }

  public static int allocateId(String className, String methodName) {
    synchronized (descriptorIDs) {
      var propDescriptor = new LPropertyDescriptor(className, methodName);
      ToIntFunction<LPropertyDescriptor> allocateId = k -> nextId();
      var id = descriptorIDs.computeIfAbsent(propDescriptor, allocateId);
      if (id >= descriptors.length) descriptors = Arrays.copyOf(descriptors, id * 2);
      descriptors[id] = propDescriptor;
      return id;
    }
  }

  /** Return -1 if the descriptor is not yet know */
  public static int getId(String className, String methodName) {
    synchronized (descriptorIDs) {
      return descriptorIDs.getOrDefault(new LPropertyDescriptor(className, methodName), -1);
    }
  }

  public static LPropertyDescriptor getDescriptor(int id) {
    return descriptors[id];
  }
}
