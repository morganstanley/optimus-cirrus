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
import optimus.platform.relational.tree.FuncElement
import optimus.platform.relational.tree.MethodCallee
import optimus.platform.relational.tree.RelationElement

import java.time._

class DALRegisteredIndexLanguage(lookup: MappingEntityLookup) extends DALLanguage(lookup) {

  override def createDialect(translator: QueryTranslator): QueryDialect = {
    new DALRegisteredIndexDialect(this, translator)
  }

  override def canBeWhere(e: RelationElement): Boolean = {

    def isCollection(column: ColumnElement): Boolean = column.columnInfo match {
      case i: IndexColumnInfo => i.isCollection
      case _                  => false
    }

    def isNonUniqueIndex(column: ColumnElement): Boolean = column.columnInfo.columnType match {
      case ColumnType.Index => true // we don't support registered index on keys and unique indexes
      case _                => false
    }

    Query
      .flattenBOOLANDConditions(e)
      .forall {
        case BinaryExpressionElement(op, c: ColumnElement, _: ConstValueElement, _) if isNonUniqueIndex(c) =>
          op match {
            case LT | LE | GT | GE if !isCollection(c) => true
            case _                                     => super.canBeWhere(e)
          }
        case BinaryExpressionElement(op, _: ConstValueElement, c: ColumnElement, _) if isNonUniqueIndex(c) =>
          op match {
            case LT | LE | GT | GE if !isCollection(c) => true
            case _                                     => super.canBeWhere(e)
          }
        // This is to allow "!dt.isAfter(someDt)" and "!dt.isBefore(someDt)" into <= and >=
        // expressions respectively, where "dt" field is registered @indexed.
        case BinaryExpressionElement(
              EQ,
              FuncElement(mc: MethodCallee, List(_: ConstValueElement), c: ColumnElement),
              ConstValueElement(false, _),
              _) if isNonUniqueIndex(c) && isValidDateTimeExpr(mc) =>
          true
        case BinaryExpressionElement(
              EQ,
              FuncElement(mc: MethodCallee, List(c: ColumnElement), _: ConstValueElement),
              ConstValueElement(false, _),
              _) if isNonUniqueIndex(c) && isValidDateTimeExpr(mc) =>
          true
        case FuncElement(mc: MethodCallee, List(_: ConstValueElement), c: ColumnElement)
            if isNonUniqueIndex(c) && isValidDateTimeExpr(mc) =>
          true
        case FuncElement(mc: MethodCallee, List(c: ColumnElement), _: ConstValueElement)
            if isNonUniqueIndex(c) && isValidDateTimeExpr(mc) =>
          true
        case _ =>
          super.canBeWhere(e)
      }
  }

  private def isValidDateTimeExpr(mc: MethodCallee): Boolean = {
    if (mc.method != null) {
      val desc = mc.method
      val declType = desc.declaringType
      if (
        declType <:< classOf[LocalDate] ||
        declType <:< classOf[LocalTime] ||
        declType <:< classOf[OffsetTime] ||
        declType <:< classOf[ZonedDateTime]
      ) desc.name match {
        case "isAfter" | "isBefore" => true
        case _                      => false
      }
      else false
    } else false
  }
}

class DALRegisteredIndexDialect(lan: QueryLanguage, tran: QueryTranslator) extends DALDialect(lan, tran) {
  override def format(e: RelationElement): ExpressionQuery = {
    DALRegisteredIndexFormatter.format(e)
  }
}
