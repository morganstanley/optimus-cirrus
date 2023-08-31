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
package optimus.deps;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class CmiCollectedDependencies {
  public final Set<String> usedClasses;
  public final Set<String> classDependencies;
  public final Set<ResourceDependency> resourceDependencies;
  public final List<String> internalEvents;
  public final CmiCollectionStateSnapshot snapshot;

  public CmiCollectedDependencies(
      Set<String> usedClasses,
      Set<String> classDependencies,
      Set<ResourceDependency> resourceDependencies,
      List<String> internalEvents,
      CmiCollectionStateSnapshot snapshot) {
    this.usedClasses = Set.copyOf(usedClasses);
    this.classDependencies = Set.copyOf(classDependencies);
    this.resourceDependencies = Set.copyOf(resourceDependencies);
    this.internalEvents = List.copyOf(internalEvents);
    this.snapshot = snapshot;
  }

  public Set<String> getUsedClasses() {
    return usedClasses;
  }

  public Set<String> getClassDependencies() {
    return classDependencies;
  }

  public Set<ResourceDependency> getResourceDependencies() {
    return resourceDependencies;
  }

  public List<String> getInternalEvents() {
    return internalEvents;
  }

  public CmiCollectionStateSnapshot getSnapshot() {
    return snapshot;
  }
}
