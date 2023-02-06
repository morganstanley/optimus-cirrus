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

import optimus.platform.BusinessEvent
import optimus.platform.relational.RelationalException
import optimus.platform.relational.data.tree.DbQueryTreeVisitor
import optimus.platform.relational.tree.ConstValueElement
import optimus.platform.relational.tree.RelationElement
import optimus.platform.storable.BusinessEventReference
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReference

object ClientSideFilterValidator {
  def validate(element: RelationElement): RelationElement = {
    val v = new ClientSideFilterValidator
    v.visitElement(element)
  }
}

class ClientSideFilterValidator extends DbQueryTreeVisitor {

  override protected def handleConstValue(element: ConstValueElement): RelationElement = {
    if ((element ne null) && containsEntity(element.value)) {
      throw new RelationalException("Cannot filter on a field referring to an entity")
    } else element
  }

  private def containsEntity(v: Any): Boolean = v match {
    case _: EntityReference        => true
    case _: BusinessEventReference => true
    case _: Entity                 => true
    case _: BusinessEvent          => true
    case p: Product                => p.productIterator exists containsEntity
    case i: Iterable[_]            => i exists containsEntity
    case _                         => false
  }

}
