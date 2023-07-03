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

public class CmiCollectionStateSnapshot {
  public final int collectedClasses;
  public final int totalClassDependencies; // sum of dependencies from collected classes
  public final int collectedClassesWithResourceDependencies;
  public final int
      totalResourceDependencies; // sum of dependencies from class with known resource dependencies

  public final int transformedClasses;
  public final int obtBuiltJars;
  public final int inspectedJars;
  public final int obtBuildClasses;
  public final int inspectedClasses;

  public CmiCollectionStateSnapshot(
      int collectedClasses,
      int totalClassDependencies,
      int collectedClassesWithResourceDependencies,
      int totalResourceDependencies,
      int transformedClasses,
      int obtBuiltJars,
      int inspectedJars,
      int obtBuildClasses,
      int inspectedClasses) {
    super();

    this.collectedClasses = collectedClasses;
    this.totalClassDependencies = totalClassDependencies;
    this.collectedClassesWithResourceDependencies = collectedClassesWithResourceDependencies;
    this.totalResourceDependencies = totalResourceDependencies;

    this.transformedClasses = transformedClasses;
    this.obtBuiltJars = obtBuiltJars;
    this.inspectedJars = inspectedJars;
    this.obtBuildClasses = obtBuildClasses;
    this.inspectedClasses = inspectedClasses;
  }
}
