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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CollectedDependencies {
  private final Set<String> usedClasses;
  private final Set<String> classDependencies;
  private final Set<String> resourceDependencies;
  private final List<String> internalEvents;

  public CollectedDependencies(Set<String> usedClasses, Set<String> classDependencies,
      Set<String> resourceDependencies, List<String> internalEvents) {
    this.usedClasses = new HashSet<>(usedClasses);
    this.classDependencies = new HashSet<>(classDependencies);
    this.resourceDependencies = new HashSet<>(resourceDependencies);
    this.internalEvents = new ArrayList<>(internalEvents);
  }

  public CollectedDependencies(CollectedDependencies set1, CollectedDependencies set2) {
    this.usedClasses = new HashSet<>(set1.usedClasses);
    this.usedClasses.addAll(set2.usedClasses);
    this.classDependencies = new HashSet<>(set1.classDependencies);
    this.classDependencies.addAll(set2.classDependencies);
    this.resourceDependencies = new HashSet<>(set1.resourceDependencies);
    this.resourceDependencies.addAll(set2.resourceDependencies);
    this.internalEvents = new ArrayList<>(set1.internalEvents);
    this.internalEvents.addAll(set2.internalEvents);
  }

  public Set<String> getUsedClasses() {
    return usedClasses;
  }

  public Set<String> getClassDependencies() {
    return classDependencies;
  }

  public Set<String> getResourceDependencies() {
    return resourceDependencies;
  }

  public List<String> getInternalEvents() {
    return internalEvents;
  }
}
