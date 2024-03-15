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

public class CmiTransformationStateSnapshot {
  public final long ignored;
  public final long unchanged;
  public final long visited;
  public final long instrumented;
  public final long optimusClasses;
  public final long exemptedInstrumentationClasses;
  public final long failures;

  public CmiTransformationStateSnapshot(
      long ignored,
      long unchanged,
      long visited,
      long instrumented,
      long optimusClasses,
      long exemptedInstrumentationClasses,
      long failures) {
    this.ignored = ignored;
    this.unchanged = unchanged;
    this.visited = visited;
    this.instrumented = instrumented;
    this.optimusClasses = optimusClasses;
    this.exemptedInstrumentationClasses = exemptedInstrumentationClasses;
    this.failures = failures;
  }
}
