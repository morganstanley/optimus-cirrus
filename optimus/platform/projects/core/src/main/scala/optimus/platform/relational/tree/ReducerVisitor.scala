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
package optimus.platform.relational.tree

trait ReducerVisitor {
  self: QueryTreeVisitor =>

  def reduce(tree: RelationElement, executeOptions: ExecuteOptions = ExecuteOptions.Default): RelationElement
}

final case class ExecuteOptions(entitledOnly: Boolean, pos: MethodPosition = MethodPosition.unknown) {

  /**
   * override the previous value of entitledOnly if previous value is false and value parameter is true.
   */
  def asEntitledOnlyIf(value: Boolean): ExecuteOptions = {
    if (value && !entitledOnly) copy(entitledOnly = value) else this
  }
}

object ExecuteOptions {
  val Default = ExecuteOptions(false)
}
