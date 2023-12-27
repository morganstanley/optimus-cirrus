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
package optimus.platform.relational.dal

import java.time.Instant

import optimus.platform._
import optimus.platform.dal.BitemporalSpaceResolver
import optimus.platform.dal.EntityResolverReadImpl
import optimus.platform._
import optimus.platform.dsi.Feature
import optimus.platform.dsi.expressions.DeltaUpdate
import optimus.platform.relational.PriqlSettings
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.dal.deltaquery.DALEntityBitemporalSpaceProvider
import optimus.platform.relational.dal.deltaquery.EntityBitemporalSpaceProviderFinder
import optimus.platform.relational.inmemory.IterableRewriter
import optimus.platform.relational.tree.ExecutionCategory.QueryEntityBitemporalSpace
import optimus.platform.relational.tree._
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityChange
import optimus.platform.storable.ReferenceHolder
import optimus.platform.dsi.bitemporal.EntityClassQuery
import optimus.platform.dsi.bitemporal.EventClassQuery

trait DALQueryExtensions {
  private def resolver = EvaluationContext.entityResolver.asInstanceOf[EntityResolverReadImpl]

  implicit class QueryExtension[T](query: Query[T]) {
    def projectedViewOnly(implicit pos: MethodPosition): Query[T] = {
      val src = query.element
      val method =
        new MethodElement(DALQueryMethod.ProjectedViewOnly, List(MethodArg("src", src)), src.rowTypeInfo, src.key, pos)
      query.provider.createQuery(method)
    }

    def fullTextSearchOnly(implicit pos: MethodPosition): Query[T] = {
      val src = query.element
      val method =
        new MethodElement(DALQueryMethod.FullTextSearchOnly, List(MethodArg("src", src)), src.rowTypeInfo, src.key, pos)
      query.provider.createQuery(method)
    }

    @impure @async def nonTemporalCount: Long = {
      query.element match {
        case e: EventMultiRelation[_] =>
          if (e.entitledOnly)
            throw new RelationalUnsupportedException(core.DALExecutionProvider.entitledOnlyUnsupportedForCount)
          val eventClassName = e.classInfo.runtimeClass.getName
          resolver.countGroupings(EventClassQuery(eventClassName), e.getReadTxTime)
        case d: DALProvider =>
          if (d.entitledOnly)
            throw new RelationalUnsupportedException(core.DALExecutionProvider.entitledOnlyUnsupportedForCount)
          val dsiAt = core.DALExecutionProvider.dalTemporalContext(d.dalApi.loadContext, d.classInfo.runtimeClass, None)
          resolver.countGroupings(EntityClassQuery(d.classInfo.runtimeClass.getName), dsiAt.readTxTime)
        case _ =>
          throw new IllegalArgumentException("nonTemporalCount must be used right after 'from' clause.")
      }
    }
  }

  implicit class ExecuteReferenceExtension(val q: Query.type) {
    @node def executeReference[T <: Entity](query: Query[T]): Iterable[ReferenceHolder[T]] = {
      import ExecutionCategory._

      val element: RelationElement = query.element
      val elem = ReducerLocator.locate(element, ExecuteReference).foldLeft(element)((tree, r) => r.reduce(tree))
      IterableRewriter.rewriteAndCompile[Iterable[ReferenceHolder[T]]](elem).apply()
    }
  }

  implicit class EntityQueryExtensions[T <: Entity](val query: Query[T]) {

    @impure @async def sample(n: Int): Iterable[T] = {
      if (n <= 0)
        throw new RelationalUnsupportedException("Only positive 'n' is supported by sampling query")

      val sourceType = query.elementType
      val key = query.element.key
      val element: RelationElement = new MethodElement(
        DALQueryMethod.Sampling,
        List(MethodArg("src", query.element), MethodArg("n", new ConstValueElement(n))),
        sourceType,
        key,
        query.element.pos
      )
      val elem =
        ReducerLocator.locate(element, DALExecutionCategory.Sampling).foldLeft(element)((tree, r) => r.reduce(tree))
      resolver.withFeatureCheck(Feature.SamplingQuery)(asNode(() => true))
      IterableRewriter.rewriteAndCompile[Iterable[T]](elem).apply()
    }

    @node def executeReference: Iterable[ReferenceHolder[T]] = {
      Query.executeReference(query)
    }

    /**
     * Rows is sorted by TT in ascending order.
     */
    def sortByTT(implicit pos: MethodPosition): Query[T] = {
      sortByTT(true, pos)
    }

    /**
     * Rows is sorted by TT in descending order.
     */
    def sortByTTDesc(implicit pos: MethodPosition): Query[T] = {
      sortByTT(false, pos)
    }

    private def sortByTT(isAsc: Boolean, pos: MethodPosition): Query[T] = {
      val elementType = query.elementType
      val key = query.element.key

      val method = new MethodElement(
        DALQueryMethod.SortByTT,
        List(MethodArg("src", query.element), MethodArg("ordering", new ConstValueElement(isAsc))),
        elementType,
        key,
        pos)
      query.provider.createQuery(method)
    }
  }

  implicit class UpdateInLine(val q: Query.type) {
    import DALQueryExtensions._

    @node def deltaSinceTT[T <: Entity](query: Query[T], fromTt: Instant): Seq[EntityChange[T]] = {
      findBitemporalSpaceQueryExecutor(query).getDeltaInTT(fromTt, DeltaUpdate)
    }

    @node def deltaSinceTTLegacy[T <: Entity](query: Query[T], fromTt: Instant): Seq[EntityChange[T]] = {
      findBitemporalSpaceQueryExecutor(query).getDeltaInTTLegacy(fromTt, DeltaUpdate)
    }

    @node def deltaSinceVT[T <: Entity](query: Query[T], fromVt: Instant): Seq[EntityChange[T]] = {
      findBitemporalSpaceQueryExecutor(query).getDeltaInVT(fromVt, DeltaUpdate)
    }

    @node def deltaSinceTTForEntity[T <: Entity](query: Query[T], fromTt: Instant): Seq[(Option[T], Option[T])] = {
      findBitemporalSpaceQueryExecutor(query).getDeltaInTTForEntity(fromTt, DeltaUpdate)
    }

    @node def deltaSinceVTForEntity[T <: Entity](query: Query[T], fromVt: Instant): Seq[(Option[T], Option[T])] = {
      findBitemporalSpaceQueryExecutor(query).getDeltaInVTForEntity(fromVt, DeltaUpdate)
    }
  }

  implicit val bitemporalSpaceResolver: BitemporalSpaceResolver = new BitemporalSpaceResolverImpl
}

object DALQueryExtensions {
  private[dal] def findBitemporalSpaceQueryExecutor[T <: Entity](query: Query[T]): DALEntityBitemporalSpaceProvider = {
    val element: RelationElement = query.element
    val elem =
      ReducerLocator.locate(element, QueryEntityBitemporalSpace).foldLeft(element)((tree, r) => r.reduce(tree))
    EntityBitemporalSpaceProviderFinder.find(elem)
  }
}

object DALExecutionCategory {
  // impure sampling of n results
  case object Sampling extends ExecutionCategory
}
