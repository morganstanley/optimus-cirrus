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

import optimus.platform.cm.Known
import optimus.platform.relational.RelationalException
import optimus.platform.relational.data.tree.ColumnElement
import optimus.platform.relational.data.tree.ContainsElement
import optimus.platform.relational.data.tree.DbQueryTreeVisitor
import optimus.platform.relational.data.tree.KnowableValueElement
import optimus.platform.relational.tree.BinaryExpressionType
import optimus.platform.relational.tree.ConstValueElement
import optimus.platform.relational.tree.ElementFactory
import optimus.platform.relational.tree.ParameterElement
import optimus.platform.relational.tree.RelationElement
import optimus.platform.relational.tree.RuntimeFieldDescriptor
import optimus.platform.relational.tree.TypeInfo

object ClientSideFilterRewriter {
  def rewrite(p: ParameterElement, element: RelationElement): RelationElement = {
    val c = new ClientSideFilterRewriter(p)
    c.visitElement(element)
  }
}

class ClientSideFilterRewriter(p: ParameterElement) extends DbQueryTreeVisitor {
  override protected def handleColumn(column: ColumnElement): RelationElement = {
    val desc = new RuntimeFieldDescriptor(p.rowTypeInfo, column.name, column.rowTypeInfo)
    ElementFactory.makeMemberAccess(p, desc)
  }

  override protected def handleContains(contains: ContainsElement): RelationElement = {
    contains.values match {
      case Left(_) =>
        throw new RelationalException("Expect ConstValueElement list in ContainsElement but got ScalarElement")
      case Right(values) =>
        val valueSet: Set[Any] = values.iterator.map { case const: ConstValueElement =>
          const.value
        }.toSet
        val (left, constSet) = contains.element match {
          case KnowableValueElement(kn) => (kn, valueSet.map(Known(_)))
          case e                        => (e, valueSet)
        }
        val constEle = ElementFactory.constant(constSet, TypeInfo.SET)
        ElementFactory.makeBinary(BinaryExpressionType.ITEM_IS_IN, visitElement(left), constEle)
    }
  }
}
