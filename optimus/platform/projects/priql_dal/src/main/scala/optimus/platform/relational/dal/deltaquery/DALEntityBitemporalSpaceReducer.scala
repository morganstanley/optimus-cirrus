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
package optimus.platform.relational.dal.deltaquery

import optimus.entity.EntityInfoRegistry
import optimus.platform.Query
import optimus.platform.dsi.bitemporal.QueryPlan
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.dal.core.DALLanguage
import optimus.platform.relational.dal.core.DALMapping
import optimus.platform.relational.dal.core.ExpressionQuery
import optimus.platform.relational.dal.core.IndexColumnInfo
import optimus.platform.relational.data.DbQueryTreeReducerBase
import optimus.platform.relational.data.QueryCommand
import optimus.platform.relational.data.QueryParameter
import optimus.platform.relational.data.language.QueryLanguage
import optimus.platform.relational.data.mapping.MappingEntity
import optimus.platform.relational.data.mapping.MappingEntityLookup
import optimus.platform.relational.data.mapping.QueryMapping
import optimus.platform.relational.data.translation.NamedValueGatherer
import optimus.platform.relational.data.tree.ColumnElement
import optimus.platform.relational.data.tree.ContainsElement
import optimus.platform.relational.data.tree.ProjectionElement
import optimus.platform.relational.tree.BinaryExpressionElement
import optimus.platform.relational.tree.BinaryExpressionType
import optimus.platform.relational.tree.ConstValueElement
import optimus.platform.relational.tree.FuncElement
import optimus.platform.relational.tree.MethodCallee
import optimus.platform.relational.tree.RelationElement
import optimus.platform.relational.tree.TypeInfo

class DALEntityBitemporalSpaceReducer(override val provider: DALProvider) extends DbQueryTreeReducerBase {
  import DALEntityBitemporalSpaceReducer._

  override def createMapping(): QueryMapping = new DALEntityBitemporalSpaceMapping

  override def createLanguage(lookup: MappingEntityLookup): QueryLanguage = new DALEntityBitemporalSpaceLanguage(lookup)

  override protected def buildInner(e: RelationElement): RelationElement =
    throw new RelationalUnsupportedException("Inner query is not supported")

  override def handleProjection(projection: ProjectionElement): RelationElement = {
    val formattedQuery = getFormattedQuery(projection)
    val namedValues = NamedValueGatherer.gather(projection.select)
    val command = QueryCommand(formattedQuery, namedValues.map(v => QueryParameter(v.name, v.rowTypeInfo)))

    val expression = command.formattedQuery match {
      case ExpressionQuery(ex, QueryPlan.Default) => ex
      case ExpressionQuery(_, QueryPlan.Accelerated | QueryPlan.Sampling | QueryPlan.FullTextSearch) =>
        throw new RelationalUnsupportedException("Query not supported")
    }
    new DALEntityBitemporalSpaceProvider(
      expression,
      projection.projector.rowTypeInfo,
      projection.key,
      provider.dalApi,
      executeOptions)
  }
}

object DALEntityBitemporalSpaceReducer {
  class DALEntityBitemporalSpaceLanguage(l: MappingEntityLookup) extends DALLanguage(l) {
    override def canBeWhere(e: RelationElement): Boolean = {
      import BinaryExpressionType._

      def isCollection(column: ColumnElement): Boolean = column.columnInfo match {
        case i: IndexColumnInfo => i.isCollection
        case _                  => false
      }

      def isIndexedField(column: ColumnElement): Boolean = column.columnInfo match {
        case i: IndexColumnInfo => i.indexed && !i.unique
        case _                  => false
      }

      val result = Query
        .flattenBOOLANDConditions(e)
        .forall {
          case BinaryExpressionElement(EQ, c: ColumnElement, _: ConstValueElement, _) =>
            isIndexedField(c) && !isCollection(c)
          case BinaryExpressionElement(EQ, _: ConstValueElement, c: ColumnElement, _) =>
            isIndexedField(c) && !isCollection(c)
          case c: ContainsElement if c.element.isInstanceOf[ColumnElement] =>
            val column = c.element.asInstanceOf[ColumnElement]
            isIndexedField(column) && !isCollection(column)
          case c: ColumnElement     => isIndexedField(c) && !isCollection(c)
          case _: ConstValueElement => true
          case FuncElement(mc: MethodCallee, List(_: ConstValueElement), c: ColumnElement)
              if isIndexedField(c) && isCollection(c) =>
            mc.name == "contains"
          case _ => false
        }

      if (!result) {
        throw new RelationalUnsupportedException(
          "DAL delta query only supports '==' or 'contains' or combinations with '&&' on indexed fields in filter.")
      }
      result
    }
  }

  class DALEntityBitemporalSpaceMapping extends DALMapping {
    protected override def getEntityImpl(shape: TypeInfo[_]): MappingEntity = {
      val info = EntityInfoRegistry.getClassInfo(shape.clazz)
      DALEntityBitemporalSpaceMappingEntityFactory.create(info, shape, shape.runtimeClassName)
    }
  }
}
