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
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.data.QueryTranslator
import optimus.platform.relational.data.language.QueryDialect
import optimus.platform.relational.data.language.QueryLanguage
import optimus.platform.relational.data.mapping.MappingEntityLookup
import optimus.platform.relational.data.translation.OrderByRewriter
import optimus.platform.relational.data.tree.ColumnDeclaration
import optimus.platform.relational.data.tree.ColumnElement
import optimus.platform.relational.data.tree.ContainsElement
import optimus.platform.relational.data.tree.DALHeapEntityElement
import optimus.platform.relational.data.tree.DbQueryTreeVisitor
import optimus.platform.relational.data.tree.ProjectionElement
import optimus.platform.relational.data.tree.SelectElement
import optimus.platform.relational.tree.BinaryExpressionElement
import optimus.platform.relational.tree.BinaryExpressionType
import optimus.platform.relational.tree.ConstValueElement
import optimus.platform.relational.tree.ElementFactory
import optimus.platform.relational.tree.FuncElement
import optimus.platform.relational.tree.MemberDescriptor
import optimus.platform.relational.tree.MethodCallee
import optimus.platform.relational.tree.RelationElement
import optimus.platform.relational.tree.RuntimeMethodDescriptor

import scala.collection.mutable

class DALLanguage(val lookup: MappingEntityLookup) extends QueryLanguage {
  override def createDialect(translator: QueryTranslator): QueryDialect = {
    new DALDialect(this, translator)
  }

  override def canBeWhere(e: RelationElement): Boolean = {
    import BinaryExpressionType._

    def isCollection(column: ColumnElement): Boolean = column.columnInfo match {
      case i: IndexColumnInfo => i.index.isCollection
      case _                  => false
    }

    Query
      .flattenBOOLANDConditions(e)
      .forall(ex =>
        ex match {
          case BinaryExpressionElement(EQ, c: ColumnElement, _: ConstValueElement, _) => !isCollection(c)
          case BinaryExpressionElement(EQ, _: ConstValueElement, c: ColumnElement, _) => !isCollection(c)
          case c: ContainsElement if c.element.isInstanceOf[ColumnElement] =>
            !isCollection(c.element.asInstanceOf[ColumnElement])
          case c: ColumnElement     => !isCollection(c)
          case _: ConstValueElement => true
          case FuncElement(mc: MethodCallee, List(_: ConstValueElement), c: ColumnElement) if isCollection(c) =>
            mc.name == "contains"
          case _ => false
        })
  }

  override def isAggregate(member: MemberDescriptor): Boolean = {
    (member ne null) && member.declaringType <:< classOf[Query[_]] && member.name == "count"
  }
  override def isHeadLast(member: MemberDescriptor): Boolean = false
}

class DALDialect(lan: QueryLanguage, tran: QueryTranslator) extends QueryDialect(lan, tran) {
  override def translate(e: RelationElement): RelationElement = {
    val elem = OrderByRewriter.rewrite(language, e)
    new EntityRefRewritter().visitElement(elem)
  }

  override def format(e: RelationElement): ExpressionQuery = {
    DALFormatter.format(e)
  }

  class EntityRefRewritter extends DbQueryTreeVisitor {
    private val rewrittenColumns = new mutable.HashSet[String]
    override protected def handleProjection(proj: ProjectionElement): RelationElement = {
      val projector = visitElement(proj.projector)
      val select = rewriteSelect(proj.select)
      updateProjection(proj, select, projector, proj.aggregator)
    }

    private def rewriteSelect(s: SelectElement): SelectElement = {
      if (rewrittenColumns.isEmpty) s
      else {
        val newCols = s.columns.map(_ match {
          case decl @ ColumnDeclaration(name, c: ColumnElement) if rewrittenColumns.contains(name) =>
            val method =
              new RuntimeMethodDescriptor(DALProvider.EntityRefType, "toEntity", DALProvider.PersistentEntityType)
            ColumnDeclaration(name, ElementFactory.call(null, method, c :: Nil))
          case x => x
        })
        updateSelect(s, s.from, s.where, s.orderBy, s.groupBy, s.skip, s.take, newCols)
      }
    }

    override protected def handleDALHeapEntity(he: DALHeapEntityElement): RelationElement = {
      he.members.head match {
        // If the EntityRef column is in DALHeapEntityElement of Projector, we will rewrite it to "toEntity(c)" in Select and change its type to PersistentEntity.
        case c: ColumnElement
            if (c.rowTypeInfo eq DALProvider.EntityRefType) && c.name.startsWith(DALProvider.EntityRef) =>
          rewrittenColumns.add(c.name)
          val column = new ColumnElement(DALProvider.PersistentEntityType, c.alias, c.name, c.columnInfo)
          new DALHeapEntityElement(he.companion, he.rowTypeInfo, column :: Nil, he.memberNames)
        case _ => he
      }
    }
  }
}
