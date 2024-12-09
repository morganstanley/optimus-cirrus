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
package optimus.graph.loom;

import optimus.graph.PropertyNode;
/**
 * Loom implements this marker method on nodes/lambda node that don't have side effects. Don't call
 * any other methods.
 */
public interface TrivialNode {
  default Object trivialResult() {
    if (this instanceof PropertyNode) {
      //noinspection unchecked
      return ((PropertyNode<Object>) this).func();
    }
    throw new UnsupportedOperationException("Needs more work");
  }
}
