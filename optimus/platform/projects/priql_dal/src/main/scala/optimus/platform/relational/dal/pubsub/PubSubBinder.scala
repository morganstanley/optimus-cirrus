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

import optimus.platform.Query
import optimus.platform.relational.RelationalException
import optimus.platform.relational.dal.core._
import optimus.platform.relational.tree._
import optimus.platform.relational.data.mapping._
import optimus.platform.relational.data.tree.SelectElement

class PubSubBinder(mapper: QueryMapper, root: RelationElement) extends DALBinder(mapper, root) {
  override def bind(e: RelationElement): RelationElement = {
    val elem = visitElement(e)
    maybeDoPermitTableScanCheck(elem)
    elem
  }

  override protected def hasFilterCondition(s: SelectElement): Boolean = {
    if (s.where eq null) false
    else {
      val psLanguage = language match {
        case p: PubSubLanguage => p
        case x                 => throw new RelationalException(s"Unexpected language $x")
      }
      Query.flattenBOOLANDConditions(s.where) exists { cond =>
        psLanguage.canBeServerWhere(cond)
      }
    }
  }
}

object PubSubBinder extends DALQueryBinder {
  def bind(mapper: QueryMapper, e: RelationElement): RelationElement = {
    new PubSubBinder(mapper, e).bind(e)
  }
}
