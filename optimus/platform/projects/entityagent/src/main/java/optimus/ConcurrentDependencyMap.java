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
package optimus;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// To avoid any risk to make thread unsafe access, the structures are hidden here and access heavily restricted
//
// Uses `HashSet` internally since they are guarded by the CHM's `compute` atomicity
public class ConcurrentDependencyMap<V> {
  private final Map<String, Set<V>> dependencies = new ConcurrentHashMap<>();

  public void add(String key, V value) {
    dependencies.compute(key, (k, v) -> {
      Set<V> entries = (v == null) ? new HashSet<>() : v;
      entries.add(value);
      return entries;
    });
  }

  // @return Never `null`.
  Set<V> getSafeCopy(String key) {
    Set<V> smuggledCopy = new HashSet<>();
    dependencies.computeIfPresent(key, (k, v) -> {
      smuggledCopy.addAll(v);
      return v;
    });
    return smuggledCopy;
  }

  // The copy itself is not thread safe.
  // @return Never `null`.
  Map<String, Set<V>> getSafeCopy() {
    Map<String, Set<V>> dependenciesCopy = new HashMap<>();
    Set<String> safeKeys = new HashSet<>(dependencies.keySet()); // Fast copy
    for (String key : safeKeys) {
      dependenciesCopy.put(key, getSafeCopy(key));
    }
    return dependenciesCopy; // Defensive copy
  }

  int getDependenciesCount() {
    return dependencies.values().stream().mapToInt(Set::size).sum();
  }

  int size() {
    return dependencies.size();
  }
}
