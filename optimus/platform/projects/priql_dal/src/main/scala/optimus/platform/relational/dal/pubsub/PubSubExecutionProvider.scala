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
package optimus.platform.relational.dal.pubsub

import optimus.platform.dal.DalAPI
import optimus.platform._
import optimus.platform.dsi.expressions.Expression
import optimus.platform.relational.data.tree.ProjectionElement
import optimus.platform.relational.tree.ExecuteOptions
import optimus.platform.relational.tree.ProviderRelation
import optimus.platform.relational.tree.TypeInfo

class PubSubExecutionProvider[T](
    val expression: Expression,
    proj: ProjectionElement,
    shape: TypeInfo[_],
    key: RelationKey[_],
    val clientFilter: Option[NodeFunction1[Any, Boolean]],
    dalApi: DalAPI,
    val executeOptions: ExecuteOptions)
    extends ProviderRelation(shape, key) {

  override def getProviderName: String = "PubSubExecutionProvider"

  override def makeKey(newKey: RelationKey[_]): PubSubExecutionProvider[T] = {
    new PubSubExecutionProvider(expression, proj, shape, newKey, clientFilter, dalApi, executeOptions)
  }
}
