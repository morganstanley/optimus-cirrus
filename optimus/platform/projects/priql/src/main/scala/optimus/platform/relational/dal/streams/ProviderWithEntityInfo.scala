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
package optimus.platform.relational.dal.streams

import optimus.entity.ClassEntityInfo
import optimus.graph.Node
import optimus.platform.RelationKey
import optimus.platform.relational.KeyPropagationPolicy
import optimus.platform.relational.data.DataProvider
import optimus.platform.relational.data.FieldReader
import optimus.platform.relational.data.QueryCommand
import optimus.platform.relational.tree.ExecuteOptions
import optimus.platform.relational.tree.MethodPosition
import optimus.platform.relational.tree.MultiRelationElement
import optimus.platform.relational.tree.ProviderRelation
import optimus.platform.relational.tree.TypeInfo

private[platform] trait ProviderWithEntityInfo { self: ProviderRelation =>
  def classEntityInfo: ClassEntityInfo
  def typeInfo: TypeInfo[_]

  final override def prettyPrint(indent: Int, out: StringBuilder): Unit = {
    indentAndStar(indent, out)
    out ++= serial + " Provider:" + " '" + getProviderName + "' " + "of" + " type " + rowTypeInfo.runtimeClass.getName + "\n"
  }
}