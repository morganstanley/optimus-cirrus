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
package optimus.core

import optimus.graph.NodeKey
import optimus.platform.annotations.poisoned

object CoreSupport {

  /**
   * This method is used by plugin to effectively cast any child of NodeKey[T] to just NodeKey[T] relying on type
   * inference, in the cases where T is not known upfront TODO (OPTIMUS-0000): Consider not using it in plugin, but rather if method
   * overrides the base one, consider deriving from the base node class generated in the base entity. This will a) reuse
   * all of defs that are parameters related and b) expose higher types
   */
  final def toNode[T](node: NodeKey[T]): NodeKey[T] = node

  @poisoned("This call should have been replaced by the compiler.")
  private[optimus] def pluginShouldHaveReplacedThis(): Nothing = ???
}
