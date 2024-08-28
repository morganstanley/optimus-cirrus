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

import optimus.platform.storable.Entity;

@SuppressWarnings("unused")
public abstract class NodeConstructors { // [PLUGIN_ENTRY]
  private NodeConstructors() {
    /* static only */
  }

  public static <T> PropertyNode<T> newConstantPropertyNode(
      T value, Entity entity, NodeTaskInfo propertyInfo) {
    // The auditor expects that nodes will run and then complete with result. If auditing is not
    // enabled then create node as already-completed since it's more efficient than running.
    if (Settings.auditing) return new ConstantPropertyNodeSync<>(value, entity, propertyInfo);
    else return new AlreadyCompletedPropertyNode<>(value, entity, propertyInfo);
  }
}
