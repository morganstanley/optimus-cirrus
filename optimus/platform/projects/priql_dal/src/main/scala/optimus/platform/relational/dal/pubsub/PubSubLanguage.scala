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
import optimus.platform.relational.data.mapping.MappingEntityLookup
import optimus.platform.relational.data.tree.ColumnElement
import optimus.platform.relational.data.tree.ContainsElement
import optimus.platform.relational.data.tree.DbEntityElement
import optimus.platform.relational.tree.FuncElement
import optimus.platform.relational.tree.MethodCallee
import optimus.platform.relational.tree.BinaryExpressionElement
import optimus.platform.relational.tree.BinaryExpressionType._
import optimus.platform.relational.tree.ConstValueElement
import optimus.platform.relational.tree.MemberElement
import optimus.platform.relational.tree.RelationElement

class PubSubLanguage(lkup: MappingEntityLookup) extends DALLanguage(lkup) {

  override def canBeWhere(e: RelationElement): Boolean = {
    Query
      .flattenBOOLANDConditions(e)
      .forall {
        case BinaryExpressionElement(_, left, right, _)                  => canBeWhere(left) && canBeWhere(right)
        case c: ContainsElement if c.element.isInstanceOf[ColumnElement] => true
        case _: ColumnElement                                            => true
        case _: ConstValueElement                                        => true
        case FuncElement(
              mc: MethodCallee,
              List(_: ConstValueElement),
              _: ColumnElement | MemberElement(_: DbEntityElement, _)) =>
          mc.name == "contains"
        case x => throw new RelationalException(s"Unsupported filter condition $x")
      }
  }

  def canBeServerWhere(e: RelationElement): Boolean = {
    def isIndex(column: ColumnElement): Boolean = column.columnInfo match {
      case _: IndexColumnInfo => true
      case _                  => false
    }
    def isCollection(column: ColumnElement): Boolean = column.columnInfo match {
      case i: IndexColumnInfo => i.isCollection
      case _                  => false
    }

    Query
      .flattenBOOLANDConditions(e)
      .forall({
        case BinaryExpressionElement(EQ, c: ColumnElement, _: ConstValueElement, _) => isIndex(c) && !isCollection(c)
        case BinaryExpressionElement(EQ, _: ConstValueElement, c: ColumnElement, _) => isIndex(c) && !isCollection(c)
        case c: ContainsElement if c.element.isInstanceOf[ColumnElement] =>
          isIndex(c.element.asInstanceOf[ColumnElement]) && !isCollection(c.element.asInstanceOf[ColumnElement])
        case c: ColumnElement     => !isCollection(c)
        case _: ConstValueElement => true
        case FuncElement(mc: MethodCallee, List(_: ConstValueElement), c: ColumnElement) if isCollection(c) =>
          mc.name == "contains"
        case _ => false
      })
  }

}
