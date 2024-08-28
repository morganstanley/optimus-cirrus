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

import optimus.platform.RelationKey

/**
 * ProviderRelation is a key construct within priql
 *
 * ProviderRelation represents a part of the intermediate tree (see ElementType) This part of the intermediate tree
 * represents some physical data somewhere eg. a database table, entity depot construct or reference to a raw array
 *
 * The provider is given the entire tree and may manipulate it into a new tree any number of times.
 *
 * This allows for numerous levels of indirection, for example we can write a query for data that doesn't exist yet and
 * calculate that data in the provider before we allow the execution.
 *
 * Once only one provider remains the tree is executed.
 */
abstract class ProviderRelation(
    val typeInfo: TypeInfo[_],
    key: RelationKey[_],
    pos: MethodPosition = MethodPosition.unknown)
    extends MultiRelationElement(ElementType.Provider, typeInfo, key, pos) {

  def reducersFor(category: ExecutionCategory): List[ReducerVisitor] = {
    category match {
      case ExecutionCategory.Execute => Nil
      case _                         => throw new IllegalArgumentException(s"Unsupported ExecutionCategory $category")
    }
  }

  def getProviderName: String

  def makeKey(newKey: RelationKey[_]): ProviderRelation
}
