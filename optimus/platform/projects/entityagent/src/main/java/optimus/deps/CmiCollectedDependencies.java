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
  private final Set<String> usedClasses;
  private final Set<String> classDependencies;
  private final Set<String> resourceDependencies;
  private final List<String> internalEvents;
  private final Map<String, String> systemProperties;
  private final Map<String, String> environmentVariables;

  public CmiCollectedDependencies(Set<String> usedClasses,
                                  Set<String> classDependencies,
                                  Set<String> resourceDependencies,
                                  List<String> internalEvents,
                                  Map<String, String> systemProperties,
                                  Map<String, String> environmentVariables) {
    this.usedClasses = Set.copyOf(usedClasses);
    this.classDependencies = Set.copyOf(classDependencies);
    this.resourceDependencies = Set.copyOf(resourceDependencies);
    this.internalEvents = List.copyOf(internalEvents);
    this.systemProperties = Map.copyOf(systemProperties);
    this.environmentVariables = Map.copyOf(environmentVariables);
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

  public Map<String, String> getSystemProperties() {
    return systemProperties;
  }

  public Map<String, String> getEnvironmentVariables() {
    return environmentVariables;
  }
}
