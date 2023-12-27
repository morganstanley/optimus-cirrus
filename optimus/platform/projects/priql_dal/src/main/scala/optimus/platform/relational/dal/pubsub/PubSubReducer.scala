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
import optimus.platform.asNode
import optimus.platform.dsi.bitemporal.QueryPlan
import optimus.platform.relational.RelationalException
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.asm.LambdaCompiler
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.dal.core.ExpressionQuery
import optimus.platform.relational.data.DbQueryTreeReducerBase
import optimus.platform.relational.data.mapping.MappingEntityLookup
import optimus.platform.relational.data.tree.DALHeapEntityElement
import optimus.platform.relational.data.tree.ProjectionElement
import optimus.platform.relational.data.tree.SelectElement
import optimus.platform.relational.tree.ElementFactory
import optimus.platform.relational.tree.RelationElement

class PubSubReducer(val provider: DALProvider) extends DbQueryTreeReducerBase {
  override def createMapping() =
    if (provider.supportsRegisteredIndexes) new PubSubRegisteredIndexMapping
    else new PubSubMapping

  override def createLanguage(lookup: MappingEntityLookup) = new PubSubLanguage(lookup)

  override protected def buildInner(e: RelationElement): RelationElement =
    throw new RelationalUnsupportedException("Inner query is not supported")

  protected override def handleProjection(proj: ProjectionElement): RelationElement = {
    proj.projector match {
      case d: DALHeapEntityElement =>
        val s = proj.select
        val where = s.where
        var serverWhere: RelationElement = null
        var clientWhere: RelationElement = null
        val language = translator.language match {
          case x: PubSubLanguage => x
          case x                 => throw new RelationalException(s"Unexpected language $x")
        }
        for (cond <- Query.flattenBOOLANDConditions(where)) {
          if (!language.canBeServerWhere(cond))
            clientWhere = if (clientWhere eq null) cond else ElementFactory.andAlso(clientWhere, cond)
          else serverWhere = if (serverWhere eq null) cond else ElementFactory.andAlso(serverWhere, cond)
        }

        val newWhere = serverWhere
        val newSel = new SelectElement(
          s.alias,
          s.columns,
          s.from,
          newWhere,
          s.orderBy,
          s.groupBy,
          s.skip,
          s.take,
          s.isDistinct,
          s.reverse)
        val pe =
          new ProjectionElement(newSel, proj.projector, proj.key, proj.keyPolicy, entitledOnly = proj.entitledOnly)

        val (serverQueryCommand, _) = createQueryCommand(pe)
        val expression = serverQueryCommand.formattedQuery match {
          case ExpressionQuery(ex, QueryPlan.Default) => ex
          case ExpressionQuery(_, QueryPlan.Accelerated | QueryPlan.Sampling | QueryPlan.FullTextSearch) =>
            throw new RelationalUnsupportedException("Query not supported")
        }

        val clientFilterFunc = Option(clientWhere) map { cf =>
          ClientSideFilterValidator.validate(cf)
          val param = ElementFactory.parameter("t", d.rowTypeInfo)
          val body = ClientSideFilterRewriter.rewrite(param, cf)
          val lambda = ElementFactory.lambda(body, Seq(param))
          LambdaCompiler.compileAsNodeOrFunc1[Any, Boolean](lambda) match {
            case Left(nf) => asNode.apply$withNode(nf)
            case Right(f) =>
              asNode { a: Any =>
                f(a)
              }
          }
        }

        new PubSubExecutionProvider(
          expression,
          proj,
          d.rowTypeInfo,
          proj.key,
          clientFilterFunc,
          provider.dalApi,
          executeOptions.asEntitledOnlyIf(proj.entitledOnly))
      case x =>
        throw new RelationalUnsupportedException(s"Unsupported query: $x")
    }
  }
}
