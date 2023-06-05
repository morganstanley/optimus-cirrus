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
package optimus.platform.relational.dal.sampling

import optimus.platform.dsi.bitemporal.QueryPlan
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.dal.DALQueryMethod
import optimus.platform.relational.dal.core.DALBinder
import optimus.platform.relational.dal.core.DALDialect
import optimus.platform.relational.dal.core.DALLanguage
import optimus.platform.relational.dal.core.DALMapper
import optimus.platform.relational.dal.core.DALMapping
import optimus.platform.relational.dal.core.DALQueryBinder
import optimus.platform.relational.dal.core.DALReducer
import optimus.platform.relational.dal.core.ExpressionQuery
import optimus.platform.relational.data.QueryCommand
import optimus.platform.relational.data.QueryTranslator
import optimus.platform.relational.data.language.QueryDialect
import optimus.platform.relational.data.language.QueryLanguage
import optimus.platform.relational.data.mapping.MappingEntityLookup
import optimus.platform.relational.data.mapping.QueryBinder
import optimus.platform.relational.data.mapping.QueryMapper
import optimus.platform.relational.data.mapping.QueryMapping
import optimus.platform.relational.data.tree.ProjectionElement
import optimus.platform.relational.data.tree.SelectElement
import optimus.platform.relational.tree.ConstValueElement
import optimus.platform.relational.tree.LambdaElement
import optimus.platform.relational.tree.MethodElement
import optimus.platform.relational.tree.QueryMethod
import optimus.platform.relational.tree.RelationElement

class DALSamplingReducer(p: DALProvider) extends DALReducer(p) {

  override def createMapping(): QueryMapping = new DALSamplingMapping
  override def createLanguage(lookup: MappingEntityLookup): DALLanguage = new DALSamplingLanguage(lookup)

  protected override def compileAndExecute(
      command: QueryCommand,
      projector: LambdaElement,
      proj: ProjectionElement): RelationElement = {
    if (proj.entitledOnly)
      throw new RelationalUnsupportedException("'entitledOnly' is not supported by sampling query")
    provider.execute(command, Right(DALProvider.getEntity), proj.key, proj.projector.rowTypeInfo, executeOptions)
  }

  private class DALSamplingLanguage(l: MappingEntityLookup) extends DALLanguage(l) {
    override def canBeWhere(e: RelationElement): Boolean = {
      val result = super.canBeWhere(e)
      if (!result)
        throw new RelationalUnsupportedException(
          "DAL sampling query only supports '==' or 'contains' or combinations with '&&' on key/index fields in filter.")
      result
    }

    override def createDialect(translator: QueryTranslator): QueryDialect = {
      new DALSamplingDialect(this, translator)
    }
  }

  private class DALSamplingDialect(lan: QueryLanguage, tran: QueryTranslator) extends DALDialect(lan, tran) {
    override def format(e: RelationElement): ExpressionQuery = {
      super.format(e).copy(plan = QueryPlan.Sampling)
    }
  }

  private class DALSamplingMapping extends DALMapping {
    override def createMapper(translator: QueryTranslator): DALMapper = {
      new DALSamplingMapper(this, translator)
    }
  }

  private class DALSamplingMapper(m: DALMapping, t: QueryTranslator) extends DALMapper(m, t) {
    override val binder: QueryBinder = DALSamplingBinder
  }

  object DALSamplingBinder extends DALQueryBinder {
    def bind(mapper: QueryMapper, e: RelationElement): RelationElement = {
      new DALSamplingBinder(mapper, e).bind(e)
    }
  }

  private class DALSamplingBinder(mapper: QueryMapper, root: RelationElement) extends DALBinder(mapper, root) {
    override def bind(e: RelationElement): RelationElement = {
      visitElement(e) match {
        case p @ ProjectionElement(sel, _, _) if sel.take ne null =>
          new ConditionOptimizer().optimize(p)
        case _ =>
          throw new RelationalUnsupportedException("DAL sampling query requires whole query to run on the server side")
      }
    }

    override def handleMethod(method: MethodElement): RelationElement = {
      import QueryMethod._

      method.methodCode match {
        case WHERE                   => bindWhere(method)
        case PERMIT_TABLE_SCAN       => bindPermitTableScan(method)
        case DALQueryMethod.Sampling => bindSampling(method)
        case mc =>
          throw new RelationalUnsupportedException(s"DAL sampling query doesn't support '${mc}' operator")
      }
    }

    protected def bindSampling(method: MethodElement): RelationElement = {
      val (s :: nArg :: _) = method.methodArgs
      val source = super.visitElement(s.param)
      val projection = convertToSequence(source)
      if ((projection eq null) || (projection.select.take ne null)) {
        throw new RelationalUnsupportedException("DAL sampling query requires whole query to run on the server side")
      } else {
        val take @ ConstValueElement(n: Int, _) = nArg.arg
        val s = projection.select
        val newSel = new SelectElement(
          s.alias,
          s.columns,
          s.from,
          s.where,
          s.orderBy,
          s.groupBy,
          s.skip,
          take,
          s.isDistinct,
          s.reverse)
        new ProjectionElement(
          newSel,
          projection.projector,
          projection.key,
          projection.keyPolicy,
          entitledOnly = projection.entitledOnly)
      }
    }

    override protected def bindPermitTableScan(method: MethodElement): RelationElement = {
      visitSource(method.methodArgs.head.param)
    }
  }
}
