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

import java.time.Instant

import optimus.entity.EntityInfoRegistry
import optimus.platform._
import optimus.platform.dal.DALImpl
import optimus.platform.dal.DalAPI
import optimus.platform.dal.EntityEventModule.EntityBitemporalSpaceImpl
import optimus.platform.dal.EntityResolverReadImpl
import optimus.platform.dal.EntitySerializer
import optimus.platform.{TemporalContext => TC}
import optimus.platform.AsyncImplicits._
import optimus.platform._
import optimus.platform.dsi.Feature
import optimus.platform.dsi.bitemporal.DSIQueryTemporality
import optimus.platform.dsi.bitemporal.DSIQueryTemporality.BitempRange
import optimus.platform.dsi.bitemporal.ExpressionQueryCommand
import optimus.platform.dsi.bitemporal.QueryPlan
import optimus.platform.dsi.bitemporal.QueryResultMetaData
import optimus.platform.dsi.bitemporal.SelectSpaceResult
import optimus.platform.dsi.expressions.UpdateKind
import optimus.platform.dsi.expressions.ConvertOps
import optimus.platform.dsi.expressions.EntityBitemporalSpace
import optimus.platform.dsi.expressions.Expression
import optimus.platform.dsi.expressions.Function
import optimus.platform.dsi.expressions.Id
import optimus.platform.dsi.expressions.Property
import optimus.platform.dsi.expressions.PropertyDef
import optimus.platform.dsi.expressions.PropertyType
import optimus.platform.dsi.expressions.Select
import optimus.platform.relational.dal.core.AsyncValueEvaluator
import optimus.platform.relational.dal.core.DALQueryUtil
import optimus.platform.relational.tree.ExecuteOptions
import optimus.platform.relational.tree.ProviderRelation
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityChange
import optimus.platform.storable.EntityVersionHolder
import optimus.platform.storable.PersistentEntity

import scala.collection.Seq

object DALEntityBitemporalSpaceProvider extends DALQueryUtil {
  import QueryResultMetaData._
  private[dal] final def rectangle_props(id: Id) = List(
    PropertyDef(Rectangle, Property.special(id, Rectangle))
  )
  private final def entity_props(id: Id) =
    List(
      PropertyDef(
        EntityRef,
        Function(ConvertOps.ToEntity.Name, Property(PropertyType.Special, Seq(EntityRef), id) :: Nil)))
}
class DALEntityBitemporalSpaceProvider(
    private[dal] val expression: Expression,
    shape: TypeInfo[_],
    key: RelationKey[_],
    val dalApi: DalAPI,
    val executeOptions: ExecuteOptions)
    extends ProviderRelation(shape, key) {
  import DALEntityBitemporalSpaceProvider._
  override def getProviderName: String = "DeltaExecutionProvider"
  override def makeKey(newKey: RelationKey[_]): ProviderRelation =
    new DALEntityBitemporalSpaceProvider(expression, shape, newKey, dalApi, executeOptions)

  private def bitempSpace(
      dsiAt: DSIQueryTemporality.At,
      fromVt: Option[Instant],
      fromTt: Option[Instant]): DSIQueryTemporality.BitempRange = {
    require(fromTt.isEmpty == fromVt.isDefined, "provide either fromTt or fromVt")
    if (fromVt.isDefined) {
      DSIQueryTemporality.BitempRange(
        TimeInterval(fromVt.get, dsiAt.validTime),
        TimeInterval(dsiAt.txTime, dsiAt.txTime),
        false)
    } else {
      DSIQueryTemporality.BitempRange(
        TimeInterval(dsiAt.validTime, dsiAt.validTime),
        TimeInterval(fromTt.get, dsiAt.txTime),
        false)
    }
  }

  @async private def buildExpressionQueryCommand(
      changeKind: UpdateKind,
      fromVt: Option[Instant] = None,
      fromTt: Option[Instant] = None,
      sendEntity: Boolean = false): (ExpressionQueryCommand, BitempRange) = {
    require(fromTt.isEmpty == fromVt.isDefined, "provide either fromTt or fromVt")
    val s @ Select(e: EntityBitemporalSpace, _, where, _, _, _, _, _, _, _) =
      ConstFormatter.format(AsyncValueEvaluator.evaluate(expression), dalApi.loadContext)
    val entity = EntityInfoRegistry.getClassInfo(e.name).runtimeClass
    val dsiAt = dalTemporalContext(dalApi.loadContext, entity, where)
    val bitempRange = bitempSpace(dsiAt, fromVt, fromTt)
    val newEx = s.copy(
      from = e.copy(when = bitempRange, kind = changeKind),
      properties = if (sendEntity) entity_props(e.id) else rectangle_props(e.id))
    (ExpressionQueryCommand(newEx, QueryPlan.Default, executeOptions.entitledOnly), bitempRange)
  }

  @async private def executeQuery[T <: Entity](
      command: ExpressionQueryCommand,
      range: BitempRange): Seq[EntityChange[T]] = {
    val resolver = EvaluationContext.env.entityResolver.asInstanceOf[EntityResolverReadImpl]
    val qRes = resolver.withFeatureCheck(Feature.DeltaUpdatePriqlApi) {
      asNode { () =>
        resolver.findByExpressionCommand(command)
      }
    }

    val rects = qRes.result map {
      case Array(a: SelectSpaceResult.Rectangle) => a
      case s =>
        throw new IllegalArgumentException(s"Expected Array[SelectSpaceResult], found: ${s}")
    }
    rects
      .groupBy(_.eref)
      .iterator
      .flatMap { case (eref, rects) =>
        val (from, to) = rects match {
          case a :: Nil =>
            if (a.containsPoint(range.vtRange.from, range.ttRange.from)) {
              (Some(a), None)
            } else {
              (None, Some(a))
            }
          case List(a, b) if a.vref == b.vref => (None, None)
          case List(a, b) =>
            if (a.containsPoint(range.vtRange.from, range.ttRange.from)) {
              (Some(a), Some(b))
            } else {
              (Some(b), Some(a))
            }
          case s =>
            throw new IllegalArgumentException(s"Unexpected number of changes for eref:${eref}, Rectangles: ${s}")
        }
        (from orElse to) map { _ =>
          EntityChange(
            eref,
            from map { fr =>
              EntityVersionHolder(
                EntityBitemporalSpaceImpl[T](Set(fr), fr.txInterval.from, Some(fr.vtInterval), Some(fr.txInterval)))
            },
            to map { t =>
              EntityVersionHolder(
                EntityBitemporalSpaceImpl[T](Set(t), t.txInterval.from, Some(t.vtInterval), Some(t.txInterval)))
            },
            None
          )
        }
      }
      .toIndexedSeq
  }

  @async
  private def getEntity[T](pe: PersistentEntity, loadContext: TC): T = {
    EntitySerializer.hydrate(pe, loadContext)
  }

  @async private def executeQueryForEntity[T <: Entity](
      command: ExpressionQueryCommand,
      range: BitempRange): Seq[(Option[T], Option[T])] = {
    val frLoadContext = DALImpl.TemporalContext(range.vtRange.from, range.ttRange.from)
    val toLoadContext = DALImpl.TemporalContext(range.vtRange.to, range.ttRange.to)
    val resolver = EvaluationContext.env.entityResolver.asInstanceOf[EntityResolverReadImpl]
    val qRes = resolver.withFeatureCheck(Feature.DeltaUpdatePriqlApi) {
      asNode { () =>
        resolver.findByExpressionCommand(command)
      }
    }

    val entity = qRes.result map {
      case Array(a: PersistentEntity) => a
      case s =>
        throw new IllegalArgumentException(s"Expected Array[PersistentEntity], found: ${s}")
    }
    // after groupBy, we have to call '.toVector' to turn the collection type from Map[_,_] to Vector[_]
    // otherwise if multiple 'from's are None, we will get wrong results.
    entity.groupBy(_.serialized.entityRef).toVector.apar map { case (eref, ent) =>
      val (from, to) = ent match {
        case a :: Nil =>
          if (a.vtInterval.contains(range.vtRange.from) && a.txInterval.contains(range.ttRange.from)) {
            (Some(a), None)
          } else {
            (None, Some(a))
          }
        case List(a, b) =>
          if (a.vtInterval.contains(range.vtRange.from) && a.txInterval.contains(range.ttRange.from)) {
            (Some(a), Some(b))
          } else {
            (Some(b), Some(a))
          }
        case s =>
          throw new IllegalArgumentException(s"Unexpected number of changes for eref:${eref}, Rectangles: ${s}")
      }

      (from map (fr => getEntity[T](fr, frLoadContext)), to map (t => getEntity[T](t, toLoadContext)))
    }
  }

  @async def getDeltaInVT[T <: Entity](fromVt: Instant, changeKind: UpdateKind): Seq[EntityChange[T]] = {
    val (command, range) = buildExpressionQueryCommand(changeKind, fromVt = Some(fromVt))
    executeQuery[T](command, range)
  }

  @async def getDeltaInTT[T <: Entity](fromTt: Instant, changeKind: UpdateKind): Seq[EntityChange[T]] = {
    val (command, range) = buildExpressionQueryCommand(changeKind, fromTt = Some(fromTt))
    executeQuery[T](command, range)
  }

  @async def getDeltaInVTForEntity[T <: Entity](
      fromVt: Instant,
      changeKind: UpdateKind): Seq[(Option[T], Option[T])] = {
    val (command, range) = buildExpressionQueryCommand(changeKind, fromVt = Some(fromVt), sendEntity = true)
    executeQueryForEntity[T](command, range)
  }

  @async def getDeltaInTTForEntity[T <: Entity](
      fromTt: Instant,
      changeKind: UpdateKind): Seq[(Option[T], Option[T])] = {
    val (command, range) = buildExpressionQueryCommand(changeKind, fromTt = Some(fromTt), sendEntity = true)
    executeQueryForEntity[T](command, range)
  }
}
