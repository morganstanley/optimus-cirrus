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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class CollectedDependencies {
  private final Set<String> usedClasses;
  private final Set<String> classDependencies;
  private final Set<String> resourceDependencies;
  private final List<String> internalEvents;
  private final Properties systemProperties;
  private final Map<String, String> environmentVariables;
  private final Set<String> arguments;
  private final List<String> classpath;

  public CollectedDependencies() {
    this(
        new HashSet<>(),
        new HashSet<>(),
        new HashSet<>(),
        new ArrayList<>(),
        new Properties(),
        new HashMap<>(),
        new HashSet<>(),
        new ArrayList<>());
  }

  public CollectedDependencies(Set<String> usedClasses,
                               Set<String> classDependencies,
                               Set<String> resourceDependencies,
                               List<String> internalEvents) {
    this(
        usedClasses,
        classDependencies,
        resourceDependencies,
        internalEvents,
        new Properties(),
        new HashMap<>(),
        new HashSet<>(),
        new ArrayList<>());
  }

  public CollectedDependencies(Set<String> usedClasses,
                               Set<String> classDependencies,
                               Set<String> resourceDependencies,
                               List<String> internalEvents,
                               List<String> classpath) {
    this(
        usedClasses,
        classDependencies,
        resourceDependencies,
        internalEvents,
        new Properties(),
        new HashMap<>(),
        new HashSet<>(),
        new ArrayList<>(classpath));
  }

  public CollectedDependencies(Set<String> usedClasses,
                               Set<String> classDependencies,
                               Set<String> resourceDependencies,
                               List<String> internalEvents,
                               Properties systemProperties,
                               Map<String, String> environmentVariables,
                               HashSet<String> arguments,
                               ArrayList<String> classpath) {
    this.usedClasses = new HashSet<>(usedClasses);
    this.classDependencies = new HashSet<>(classDependencies);
    this.resourceDependencies = new HashSet<>(resourceDependencies);
    this.internalEvents = new ArrayList<>(internalEvents);
    this.systemProperties = new Properties();
    this.systemProperties.putAll(systemProperties);
    this.environmentVariables = new HashMap<>(environmentVariables);
    this.arguments = new HashSet<>(arguments);
    this.classpath = new ArrayList<>(classpath);
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

    this.systemProperties = new Properties();
    this.systemProperties.putAll(set1.systemProperties);
    this.systemProperties.putAll(set2.systemProperties);

    this.environmentVariables = new HashMap<>(set1.environmentVariables);
    this.environmentVariables.putAll(set2.environmentVariables);

    this.arguments = new HashSet<>(set1.arguments);
    this.arguments.addAll(set2.arguments);

    this.classpath = new ArrayList<>(set1.classpath);
    this.classpath.addAll(set2.classpath);
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

  public List<String> getClasspath() {
    return this.classpath;
  }

  public CollectedDependencies withClasspath(List<String> classpath) {
    this.classpath.addAll(classpath);
    return this;
  }
}
