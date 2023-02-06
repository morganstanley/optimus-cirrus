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
import optimus.platform.dal.EntityEventModule.EntityBitemporalSpaceImpl
import optimus.platform.dal.EntityNotFoundByRefException
import optimus.platform.dal.EntityResolverReadImpl
import optimus.platform._
import optimus.platform.bitemporal.EntityBitemporalSpace
import optimus.platform.bitemporal.EntityRectangle
import optimus.platform.dsi.Feature
import optimus.platform.dsi.bitemporal.DSIQueryTemporality
import optimus.platform.dsi.bitemporal.ExpressionQueryCommand
import optimus.platform.dsi.bitemporal.QueryPlan
import optimus.platform.dsi.bitemporal.SelectSpaceResult
import optimus.platform.dsi.{expressions => ex}
import optimus.platform.relational._
import optimus.platform.relational.dal.core.AsyncValueEvaluator
import optimus.platform.relational.dal.deltaquery.DALEntityBitemporalSpaceProvider._
import optimus.platform.relational.dal.serialization.StorableDalImplicits._
import optimus.platform.relational.tree._
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReference
import optimus.platform.temporalSurface.DataFreeTemporalSurfaceMatchers
import optimus.platform.temporalSurface.EntityTemporalInformation
import optimus.platform.temporalSurface.advanced.TemporalContextUtils
import optimus.platform.temporalSurface.impl.FlatTemporalContext
import optimus.platform.relational.from

import scala.util.Success

private class BitemporalSpaceResolverImpl extends BitemporalSpaceResolver {
  import DALQueryExtensions._
  import ex.BinaryOperator._

  @scenarioIndependent @node override def getEntityBitemporalSpace[T <: Entity](
      e: T,
      predicate: LambdaElement,
      fromTemporalContext: Option[TemporalContext],
      toTemporalContext: Option[TemporalContext]): EntityBitemporalSpace[T] = {
    val paramSize = predicate.parameters.size
    val bodyType = predicate.body.rowTypeInfo
    require(paramSize == 1, s"'predicate' contains $paramSize parameters, but the method only allow 1.")
    require(bodyType == TypeInfo.BOOLEAN, s"'predicate' has invalid body type: $bodyType")
    require(!e.dal$isTemporary, "Cannot load the bitemporal points for the entity that is not loaded from DAL.")
    val rtt = TemporalContextUtils.loadTemporalCoordinates(e) match {
      case info: EntityTemporalInformation => info.tt
      case other                           => e.dal$temporalContext.unsafeTxTime
    }

    // this is the same logic in EntityHistoricalOps$.getTemporalSpace, but we need DSIQueryTemporality here.
    val (vtInterval, ttInterval) = (fromTemporalContext, toTemporalContext) match {
      case (None, None) => (TimeInterval.max, TimeInterval(TimeInterval.NegInfinity, rtt))
      case (None, Some(toContext)) =>
        (
          TimeInterval(TimeInterval.NegInfinity, toContext.unsafeValidTime),
          TimeInterval(TimeInterval.NegInfinity, toContext.unsafeTxTime))
      case (Some(fromContext), None) =>
        (TimeInterval(fromContext.unsafeValidTime, TimeInterval.Infinity), TimeInterval(fromContext.unsafeTxTime, rtt))
      case (Some(fromContext), Some(toContext)) =>
        (
          TimeInterval(fromContext.unsafeValidTime, toContext.unsafeValidTime),
          TimeInterval(fromContext.unsafeTxTime, toContext.unsafeTxTime))
    }

    val bitempRange = DSIQueryTemporality.BitempRange(vtInterval, ttInterval, false)

    require(
      ttInterval.from.isBefore(rtt),
      s"argument tt should be earlier than context transaction time, tt=${ttInterval.from} rtt=$rtt"
    )

    // constructor the PriQL query using the lambda
    val conv = mkEntityClassConverter[T]
    val entityType = predicate.parameters.head.rowTypeInfo.cast[T]
    val fromQuery = from(e.getClass.asInstanceOf[Class[T]])(conv, entityType, MethodPosition.unknown)

    val lambdaElement = PostMacroRewriter.rewrite(predicate)
    val lambda = Lambda1(Some((_: T) => true), None, Some(() => Success(lambdaElement)))
    val query = fromQuery.provider.filter(fromQuery, lambda)(MethodPosition.unknown)

    // reduce the query and fill in the extra condition on eref
    val expression = AsyncValueEvaluator.evaluate(findBitemporalSpaceQueryExecutor(query).expression)
    val formatted = ConstFormatter.format(expression, null)
    val s @ ex.Select(bs: ex.EntityBitemporalSpace, _, Some(where), _, _, _, _, _, _, _) = formatted
    val erefProp = ex.Property.special(bs.id, DALProvider.EntityRef)
    val erefCond = ex.Binary(Equal, erefProp, ex.Constant(e.dal$entityRef, ex.TypeCode.Reference))
    val newWhere = ex.Binary(AndAlso, erefCond, where)
    val select =
      s.copy(
        from = bs.copy(when = bitempRange, kind = ex.AllRects),
        properties = rectangle_props(bs.id),
        where = Some(newWhere))
    val command = ExpressionQueryCommand(select, QueryPlan.Default, false)

    // execute the query
    val resolver = EvaluationContext.env.entityResolver.asInstanceOf[EntityResolverReadImpl]
    val resultSet = resolver.withFeatureCheck(Feature.GetTemporalSpaceWithFilter) {
      asNode { () =>
        resolver.findByExpressionCommand(command)
      }
    }

    val data: Vector[SelectSpaceResult.Rectangle] =
      resultSet.result.iterator.map(_(0).asInstanceOf[SelectSpaceResult.Rectangle]).toVector
    if (data.isEmpty) EmptyBitemporalSpace(e.dal$entityRef, rtt) else EntityBitemporalSpaceImpl[T](data.toSet, rtt)
  }
}

private final case class EmptyBitemporalSpace[E <: Entity](eref: EntityReference, rtt: Instant)
    extends EntityBitemporalSpace[E] {
  @scenarioIndependent @node def all: Seq[EntityRectangle[E]] = Nil
  @scenarioIndependent @node def onTxTime(tt: Instant): Seq[EntityRectangle[E]] = Nil
  @scenarioIndependent @node def onValidTime(vt: Instant): Seq[EntityRectangle[E]] = Nil
  @scenarioIndependent @node def atOption(vt: Instant, tt: Instant): Option[EntityRectangle[E]] = None
  @scenarioIndependent @node def at(vt: Instant, tt: Instant): EntityRectangle[E] = {
    throw new EntityNotFoundByRefException(eref, FlatTemporalContext(DataFreeTemporalSurfaceMatchers.all, vt, tt, None))
  }
}
