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

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Utility to keep track of the NodeTaskInfo that we encounter used by both core and entityagent.
 */
public class NodeTaskInfoRegistry {

  static class PropertyDescriptor {
    public final String className;
    public final String methodName;

    public PropertyDescriptor(String className, String methodName) {
      this.className = className;
      this.methodName = methodName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      PropertyDescriptor that = (PropertyDescriptor) o;
      return Objects.equals(className, that.className)
          && Objects.equals(methodName, that.methodName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(className, methodName);
    }
  }

  private static final ConcurrentHashMap<PropertyDescriptor, Integer> properties =
      new ConcurrentHashMap<>();

  private static final AtomicInteger counter = new AtomicInteger(1); // skipping PROFILE_ID_INVALID

  public static int nextId() {
    return counter.getAndIncrement();
  }

  public static int currentTotal() {
    return counter.get();
  }

  public static int getId(String className, String methodName) {
    return getId(new PropertyDescriptor(className, methodName));
  }

  public static int getId(PropertyDescriptor key) {
    return properties.computeIfAbsent(key, (k -> nextId()));
  }
}
