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
package optimus.platform.relational.dal.core

import optimus.platform.Query
import optimus.platform.relational.data.QueryTranslator
import optimus.platform.relational.data.language.QueryDialect
import optimus.platform.relational.data.language.QueryLanguage
import optimus.platform.relational.data.mapping.MappingEntityLookup
import optimus.platform.relational.data.tree.ColumnElement
import optimus.platform.relational.data.tree.ColumnType
import optimus.platform.relational.tree.BinaryExpressionElement
import optimus.platform.relational.tree.BinaryExpressionType._
import optimus.platform.relational.tree.ConstValueElement
import optimus.platform.relational.tree.RelationElement

class DALRegisteredIndexLanguage(lookup: MappingEntityLookup) extends DALLanguage(lookup) {

  override def createDialect(translator: QueryTranslator): QueryDialect = {
    new DALRegisteredIndexDialect(this, translator)
  }

  override def canBeWhere(e: RelationElement): Boolean = {
    def isNonUniqueIndex(column: ColumnElement): Boolean = column.columnInfo.columnType match {
      case ColumnType.Index => true // we don't support registered index on keys and unique indexes
      case _                => false
    }
    Query
      .flattenBOOLANDConditions(e)
      .forall {
        case BinaryExpressionElement(op, c: ColumnElement, _: ConstValueElement, _) if isNonUniqueIndex(c) =>
          op match {
            case LT | LE | GT | GE => true
            case _                 => super.canBeWhere(e)
          }
        case BinaryExpressionElement(op, _: ConstValueElement, c: ColumnElement, _) if isNonUniqueIndex(c) =>
          op match {
            case LT | LE | GT | GE => true
            case _                 => super.canBeWhere(e)
          }
        case _ => super.canBeWhere(e)
      }
  }
}

class DALRegisteredIndexDialect(lan: QueryLanguage, tran: QueryTranslator) extends DALDialect(lan, tran) {
  override def format(e: RelationElement): ExpressionQuery = {
    DALRegisteredIndexFormatter.format(e)
  }
}
