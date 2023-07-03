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
package optimus.platform.relational.data

import optimus.graph.Node
import optimus.platform.RelationKey
import optimus.platform.relational.KeyPropagationPolicy
import optimus.platform.relational.tree.ExecuteOptions
import optimus.platform.relational.tree.MethodPosition
import optimus.platform.relational.tree.ProviderRelation
import optimus.platform.relational.tree.TypeInfo

abstract class DataProvider(
    typeInfo: TypeInfo[_],
    key: RelationKey[_],
    pos: MethodPosition,
    val keyPolicy: KeyPropagationPolicy = KeyPropagationPolicy.NoKey)
    extends ProviderRelation(typeInfo, key, pos) {
  def execute[T](
      command: QueryCommand,
      projector: Either[FieldReader => Node[T], FieldReader => T],
      projKey: RelationKey[_],
      shapeType: TypeInfo[_],
      executeOptions: ExecuteOptions): ProviderRelation

  def executeDeferred[T](
      command: QueryCommand,
      projector: Either[FieldReader => Node[T], FieldReader => T],
      projKey: RelationKey[Any],
      paramValues: List[Any],
      shapeType: TypeInfo[_],
      executeOptions: ExecuteOptions): ProviderRelation
}
