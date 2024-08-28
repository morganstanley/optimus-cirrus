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
package optimus.graph.cache;

public enum Reachability {

  /**
   * This object is referenced by a cache, but is not a node or referenced by a node, e.g {@code
   * NCEntry}
   */
  Value("-"),

  /** An object of this type is directly referenced by an {@code NCEntry} */
  Cached("cached"),

  /**
   * An object in JVM heap referenced by more than one cached node.
   *
   * <p>Evicting a root node does not mean that any of these objects will be removed from JVM heap.
   */
  Shared("shared"),

  /** A directly cached node that is also referenced by other cached nodes */
  CachedShared("cached, shared"),

  /**
   * An object in JVM heap only referenced by one cached node (possibly transitively)
   *
   * <p>Note: a "unique" object may still have multiple root nodes if each node references a single
   * object. Unique indicates that evicting a node will definitely remove at least one of these
   * objects.
   */
  Unique("unique");

  private final String name;

  Reachability(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }
}
