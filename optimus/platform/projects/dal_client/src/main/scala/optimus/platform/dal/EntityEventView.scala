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
package optimus.platform.dal

import java.time.Instant

import optimus.platform.AsyncImplicits._
import optimus.platform._
import optimus.platform.temporalSurface.DataFreeTemporalSurfaceMatchers
import optimus.platform.temporalSurface.EntityTemporalInformation
import optimus.platform.temporalSurface.advanced.TemporalContextUtils
import optimus.platform.temporalSurface.impl.FlatTemporalContext
import optimus.platform.bitemporal._
import optimus.platform.dsi.bitemporal.DSIQueryTemporality
import optimus.platform.dsi.bitemporal.DSISpecificError
import optimus.platform.dsi.bitemporal.EmptyBitemporalSpaceException
import optimus.platform.dsi.bitemporal.SelectSpaceResult
import optimus.platform.dsi.expressions.ExpressionHelper
import optimus.platform.relational.tree.LambdaElement
import optimus.platform.storable.Entity
import optimus.platform.storable.SerializedKey
import optimus.platform.storable._

import scala.annotation.tailrec
import scala.collection.SortedMap
import scala.collection.immutable.ListMap
import optimus.scalacompat.collection._

import scala.reflect.ClassTag

object EntityEventModule {
  private val temporalSurfaceTag = Some("$$FromEvent")

  @entity class EntityValidTimelineImpl[A <: Entity](
      d: Seq[(Option[SerializedBusinessEventWithTTTo], Instant)],
      override val rtt: Instant,
      override val eref: EntityReference)
      extends EntityValidTimeline[A] {

    override val ttc: FixedTransactionTimeContext = FixedTransactionTimeContext(rtt)

    @scenarioIndependent @node private def pe(vt: Instant) = {
      val atVtTt = DALImpl.resolver.getByRefWithTemporalityAt(eref, QueryTemporality.At(vt, rtt))
      atVtTt.headOption
    }

    // we only want to load all persisted entity if user tried to access gaps/events/validTimes
    // we are using apar to take advantage of batching while loading all entities from dal
    @scenarioIndependent @node private def pes =
      d.apar.flatMap { case (_, vt) => pe(vt) } sortBy (e => e.vtInterval.from)

    @scenarioIndependent @node override def eventVersionHandles = {
      d.map {
        case (Some(evt), vt) => BusinessEventVersionHandle(evt, rtt)
        case (None, vt)      => NoneEventVersionHandle(vt, eref, rtt)
      }.sortBy(h => h.validTime)
    }

    // we only want to collect events if user access it
    @scenarioIndependent @node override def events: Seq[BusinessEvent] =
      eventVersionHandles.flatMap(evh => evh.ttFrom.flatMap(tt => evh.eventOptionAtTT(tt)))

    @scenarioIndependent @node override def entityOptionAtVT(vt: Instant): Option[A] = {
      pe(vt) map {
        EntitySerializer
          .hydrate[A](_, FlatTemporalContext(DataFreeTemporalSurfaceMatchers.all, vt, rtt, temporalSurfaceTag))
      }
    }

    @scenarioIndependent @node override def entityAtVT(vt: Instant): A = {
      val pent = pe(vt)
      val tc = FlatTemporalContext(DataFreeTemporalSurfaceMatchers.all, vt, rtt, temporalSurfaceTag)
      if (pent.isDefined) EntitySerializer.hydrate[A](pent.get, tc)
      else throw new EntityNotFoundByRefException(eref, tc)
    }

    @scenarioIndependent @node override def entityEventPairAtVT(vt: Instant): Option[(BusinessEvent, Option[A])] = {

      val matchingVt = eventVersionHandles.toList match {
        case Nil =>
          None

        case singleEventVersionHandle :: Nil =>
          if (!singleEventVersionHandle.validTime.isAfter(vt)) Some(singleEventVersionHandle) else None

        case multipleHandles @ (_ :: tail) =>
          multipleHandles.iterator
            .zip(tail.iterator)
            .collectFirst {
              case (handleA, handleB) if !handleA.validTime.isAfter(vt) && handleB.validTime.isAfter(vt) =>
                handleA
            }
            .orElse {
              if (!multipleHandles.last.validTime.isAfter(vt)) Some(multipleHandles.last) else None
            }
      }

      matchingVt.flatMap {
        _.eventOptionAtTT(rtt).map { event =>
          (event, entityOptionAtVT(vt))
        }
      }

    }

    // we only want to collect gaps if user access it
    @scenarioIndependent @node override def gaps = {
      val middle: Seq[TimelineInterval] = if (pes.size > 1) {
        pes.sliding(2) flatMap { s =>
          val first = s.head
          val second = s.last
          if (first.vtInterval.to.isBefore(second.vtInterval.from)) {
            Some(TimelineInterval(first.vtInterval.to, second.vtInterval.from))
          } else {
            assert(first.vtInterval.to == second.vtInterval.from)
            None
          }
        } toSeq
      } else Seq.empty

      // check if the vtTo of last rect is bounded
      val last =
        if (pes.last.vtInterval.to.isBefore(TimeInterval.Infinity))
          Seq(TimelineInterval(pes.last.vtInterval.to, TimeInterval.Infinity))
        else Seq.empty
      middle ++ last
    }

    // we only want to collect valid times if user access it
    @scenarioIndependent @node override def validTimes: Seq[Instant] = eventVersionHandles.map { _.validTime }

  }

  private class EntityTransactionTimelineImpl[A <: Entity](
      private val data: Seq[PersistentEntity],
      vt: Instant,
      override val rtt: Instant,
      val eref: EntityReference)
      extends EntityTransactionTimeline[A] {
    if (data.isEmpty)
      throw new DSISpecificError(
        s"Entity Transaction Timeline is empty EntityReference ${eref}, ValidTime: $vt, read transaction time: $rtt")

    override val vtc: FixedValidTimeContext = FixedValidTimeContext(vt)

    override def entityOptionAtTT(tt: Instant): Option[A] = {
      if (tt.isAfter(rtt)) None
      else {
        val pe = peAtTT(tt)
        pe map {
          EntitySerializer
            .hydrate[A](_, FlatTemporalContext(DataFreeTemporalSurfaceMatchers.all, vt, tt, temporalSurfaceTag))
        }
      }
    }

    override def entityAtTT(tt: Instant): A = {
      if (tt.isAfter(rtt)) throw new OutOfBoundEntityRead(eref, rtt)
      else {
        val tc = FlatTemporalContext(DataFreeTemporalSurfaceMatchers.all, vt, tt, temporalSurfaceTag)
        val pe = peAtTT(tt)
        if (pe.isDefined) EntitySerializer.hydrate[A](pe.get, tc)
        else throw new EntityNotFoundByRefException(eref, tc)
      }
    }

    override val gaps = {
      val middle = if (data.size > 1) {
        data.sliding(2) flatMap { s =>
          val first = s.head
          val second = s.last
          if (first.txInterval.to.isBefore(second.txInterval.from)) {
            Some(TimelineInterval(first.txInterval.to, second.txInterval.from))
          } else {
            assert(first.txInterval.to == second.txInterval.from)
            None
          }
        }
      } toSeq
      else Seq.empty
      // check if the ttTo of last rect is bounded
      val last =
        if (data.last.txInterval.to.isBefore(TimeInterval.Infinity))
          Seq(TimelineInterval(data.last.txInterval.to, TimeInterval.Infinity))
        else Seq.empty
      middle ++ last
    }

    private def peAtTT(tt: Instant): Option[PersistentEntity] = data find { d =>
      d.txInterval.contains(tt)
    }

    override val transactionTimes: Seq[Instant] = data.map { _.txInterval.from } sortBy (tt => tt)
  }

  @entity class EntityRectangleImpl[A <: Entity](
      private[optimus /*platform*/ ] val eref: EntityReference,
      private[optimus /*platform*/ ] val vref: VersionedReference,
      override val rtt: Instant,
      val vtInterval: ValidTimeInterval,
      val txInterval: TimeInterval,
      val peOpt: Option[PersistentEntity] = None)
      extends EntityRectangle[A] {

    override def vtTo = vtInterval.to
    override def vtFrom = vtInterval.from
    override def ttTo = txInterval.to
    override def ttFrom = txInterval.from

    def isBefore(oRect: EntityRectangle[A]): Boolean = {
      ttFrom.isBefore(oRect.ttFrom) ||
      (ttFrom.equals(oRect.ttFrom) && vtFrom.isBefore(oRect.vtFrom))
    }

    @node @scenarioIndependent def entity: A = entityAt(vtFrom, ttFrom)

    @node @scenarioIndependent def entityAt(vt: Instant, tt: Instant): A = {
      if (tt.isAfter(rtt)) throw new OutOfBoundEntityRead(eref, rtt)
      if (!(vtInterval.contains(vt) && txInterval.contains(tt)))
        throw new OutOfRectEntityRead(eref, vtInterval, txInterval)
      EntitySerializer
        .hydrate[A](pe, FlatTemporalContext(DataFreeTemporalSurfaceMatchers.all, vt, tt, temporalSurfaceTag))
    }

    @node @scenarioIndependent def entityOptionAt(vt: Instant, tt: Instant, entitledOnly: Boolean): Option[A] = {
      if (!tt.isAfter(rtt) && vtInterval.contains(vt) && txInterval.contains(tt))
        pEntityOption(entitledOnly) map { pe =>
          EntitySerializer
            .hydrate[A](pe, FlatTemporalContext(DataFreeTemporalSurfaceMatchers.all, vt, tt, temporalSurfaceTag))
        }
      else
        None
    }

    @node @scenarioIndependent private[optimus /*platform*/ ] def peOption: Option[PersistentEntity] =
      pEntityOption(false)
    @node @scenarioIndependent private[optimus /*platform*/ ] def pEntityOption(
        entitledOnly: Boolean): Option[PersistentEntity] = {
      peOpt.orElse {
        val atVtTt = DALImpl.resolver.getByRefWithTemporalityAt(eref, QueryTemporality.At(vtFrom, ttFrom), entitledOnly)
        atVtTt.headOption
      }
    }

    @node @scenarioIndependent private[optimus /*platform*/ ] def pe: PersistentEntity = {
      peOpt.getOrElse {
        pEntityOption(false).getOrElse(
          throw new IllegalArgumentException(s"Entity $eref not resolved at vt=$vtFrom tt=$ttFrom"))
      }
    }
  }

  @entity private[optimus /*platform*/ ] class EntityBitemporalSpaceImpl[A <: Entity](
      private[optimus /*platform*/ ] val data: Set[SelectSpaceResult.Rectangle],
      override val rtt: Instant,
      val vtRange: Option[ValidTimeInterval] = None,
      val ttRange: Option[TimeInterval] = None)
      extends EntityBitemporalSpace[A] {
    private[optimus /*platform*/ ] val eref = data.headOption
      .map(_.eref)
      .getOrElse(throw new EmptyBitemporalSpaceException(rtt))

    @node @scenarioIndependent def all =
      data.toSeq
        .map { case SelectSpaceResult.Rectangle(txInterval, vtInterval, vref, eref, peOpt) =>
          EntityRectangleImpl[A](eref, vref, rtt, vtInterval, txInterval, peOpt)
        }
        .sortWith { case (l, r) => l.isBefore(r) }

    @node @scenarioIndependent def onTxTime(tt: Instant): Seq[EntityRectangle[A]] = {
      if (tt.isAfter(rtt)) throw new OutOfBoundEntityRead(eref, rtt)
      all filter { r =>
        r.txInterval.contains(tt)
      } sortBy { r =>
        r.vtInterval.from
      }
    }

    @node @scenarioIndependent def onValidTime(vt: Instant): Seq[EntityRectangle[A]] = {
      all filter { r =>
        r.vtInterval.contains(vt)
      } sortBy { r =>
        r.txInterval.from
      }
    }

    @node @scenarioIndependent def atOption(vt: Instant, tt: Instant): Option[EntityRectangle[A]] = {
      if (tt.isAfter(rtt)) None
      else {
        all find { r =>
          r.vtInterval.contains(vt) && r.txInterval.contains(tt)
        }
      }
    }

    @node @scenarioIndependent def at(vt: Instant, tt: Instant): EntityRectangle[A] = {
      if (tt.isAfter(rtt)) throw new OutOfBoundEntityRead(eref, rtt)
      val res = all find { r =>
        r.vtInterval.contains(vt) && r.txInterval.contains(tt)
      }
      res.getOrElse(
        throw new EntityNotFoundByRefException(
          eref,
          FlatTemporalContext(DataFreeTemporalSurfaceMatchers.all, vt, tt, temporalSurfaceTag)))
    }
  }

  final case class EntityWrappingRectangle[UnderlyingEntity <: Entity, WrappedEntity <: Entity](
      underlying: EntityRectangle[UnderlyingEntity],
      wrapper: NodeFunction1[UnderlyingEntity, WrappedEntity])
      extends EntityRectangle[WrappedEntity] {
    override def vtFrom: Instant = underlying.vtFrom
    @scenarioIndependent @node override def entityOptionAt(
        vt: Instant,
        tt: Instant,
        entitledOnly: Boolean): Option[WrappedEntity] =
      underlying.entityOptionAt(vt, tt, entitledOnly).map(wrapper(_))
    @scenarioIndependent @node override def entity: WrappedEntity = wrapper(underlying.entity)
    override def ttFrom: Instant = underlying.ttFrom
    @scenarioIndependent @node override def entityAt(vt: Instant, tt: Instant): WrappedEntity =
      wrapper(underlying.entityAt(vt, tt))
    override def vtTo: Instant = underlying.vtTo
    override def ttTo: Instant = underlying.ttTo
    override val rtt: Instant = underlying.rtt
    override private[optimus /*platform*/ ] def vref: VersionedReference = underlying.vref
    override private[optimus /*platform*/ ] def eref: EntityReference = underlying.eref
    @scenarioIndependent @node override private[optimus /*platform*/ ] def pe: PersistentEntity = underlying.pe
    @scenarioIndependent @node override private[optimus /*platform*/ ] def peOption: Option[PersistentEntity] =
      underlying.peOption
  }

  final case class EntityWrappingTemporalSpace[UnderlyingEntity <: Entity, WrappedEntity <: Entity](
      underlying: EntityBitemporalSpace[UnderlyingEntity],
      wrapper: NodeFunction1[UnderlyingEntity, WrappedEntity])
      extends EntityBitemporalSpace[WrappedEntity] {
    @scenarioIndependent @node override def all = underlying.all.map(wrapperFactory)
    @scenarioIndependent @node override def onTxTime(tt: Instant) = underlying.onTxTime(tt).map(wrapperFactory)
    @scenarioIndependent @node override def onValidTime(vt: Instant) = underlying.onValidTime(vt).map(wrapperFactory)
    @scenarioIndependent @node override def atOption(vt: Instant, tt: Instant) =
      underlying.atOption(vt, tt).map(wrapperFactory(_))
    @scenarioIndependent @node override def at(vt: Instant, tt: Instant) = wrapperFactory(underlying.at(vt, tt))
    override private[optimus /*platform*/ ] def eref: EntityReference = underlying.eref
    override val rtt: Instant = underlying.rtt
    private def wrapperFactory = EntityWrappingRectangle(_: EntityRectangle[UnderlyingEntity], wrapper)
  }

  class EntityOps[A <: Entity](val entity: A) {
    @node @scenarioIndependent def initiatingEvent: Option[BusinessEvent] = {
      require(!entity.dal$isTemporary, "Cannot load the initiating event for the entity that is not loaded from DAL.")
      val tc = entity.dal$temporalContext
      val vt = entity.validTime.from
      DALImpl.resolver.getInitiatingEvent(entity.dal$entityRef, entity.getClass, vt, tc)
    }

    @scenarioIndependent @node private[optimus] def getEntityTransactionTime(e: A): Instant = {
      val res = TemporalContextUtils.loadTemporalCoordinates(e)
      res match {
        case info: EntityTemporalInformation => info.tt
        case other                           => e.dal$temporalContext.unsafeTxTime
      }
    }

    @scenarioIndependent @node private def getEntityVtTt(e: A): (Instant, Instant) = {
      val res = TemporalContextUtils.loadTemporalCoordinates(e)
      res match {
        case EntityTemporalInformation(vt, tt, _, _) => (vt, tt)
        case other => (e.dal$temporalContext.unsafeValidTime, e.dal$temporalContext.unsafeTxTime)
      }
    }

    /**
     * Provide the events timeline view of an entity that is loaded from DAL at transaction time tt.
     */
    @node def validTimeline: EntityValidTimeline[A] = {
      require(!entity.dal$isTemporary, "Cannot load the valid timeline for the entity that is not loaded from DAL.")
      val tt = getEntityTransactionTime(entity)
      val res = DALImpl.resolver.getEntityEventValidTimeline(entity.dal$entityRef, tt, None)
      EntityValidTimelineImpl[A](res, tt, entity.dal$entityRef)
    }

    @node def validTimeline(fromTime: Instant, toTime: Instant): EntityValidTimeline[A] = {
      require(!entity.dal$isTemporary, "Cannot load the valid timeline for the entity that is not loaded from DAL.")
      val tt = getEntityTransactionTime(entity)
      val res = DALImpl.resolver.getEntityEventValidTimeline(
        entity.dal$entityRef,
        tt,
        Some(ValidTimeInterval(fromTime, toTime)))
      EntityValidTimelineImpl[A](res, tt, entity.dal$entityRef)
    }

    @node def transactionTimeline: EntityTransactionTimeline[A] = {
      require(
        !entity.dal$isTemporary,
        "Cannot load the transaction timeline for the entity that is not loaded from DAL.")
      val (vt, rtt) = getEntityVtTt(entity)
      val peSeq =
        DALImpl.resolver.getByRefWithTemporalityWithRtt(entity.dal$entityRef, QueryTemporality.ValidTime(vt), rtt)
      new EntityTransactionTimelineImpl[A](peSeq.toSeq.sortBy(_.txInterval.from), vt, rtt, entity.dal$entityRef)
    }

    @node def transactionTimeline(fromTime: Instant, toTime: Instant): EntityTransactionTimeline[A] = {
      require(
        !entity.dal$isTemporary,
        "Cannot load the transaction timeline for the entity that is not loaded from DAL.")
      val (vt, rtt) = getEntityVtTt(entity)
      require(toTime.compareTo(rtt) <= 0, s"Read transaction time $rtt should always be greater than toTime $toTime")
      val peSeq =
        DALImpl.resolver.getByRefWithTemporalityWithRtt(
          entity.dal$entityRef,
          QueryTemporality.BitempRange(ValidTimeInterval(vt, vt), TimeInterval(fromTime, toTime), inRange = false),
          rtt)
      new EntityTransactionTimelineImpl[A](peSeq.toSeq.sortBy(_.txInterval.from), vt, rtt, entity.dal$entityRef)
    }

    @scenarioIndependent @node def bitemporalSpace: EntityBitemporalSpace[A] =
      bitemporalSpace()

    @scenarioIndependent @node def bitemporalSpace(
        loadTt: Instant,
        vtFrom: Option[Instant],
        ttFrom: Option[Instant]): EntityBitemporalSpace[A] = {
      require(!entity.dal$isTemporary, "Cannot load the bitemporal points for the entity that is not loaded from DAL.")
      EntityOps.getBitemporalSpace[A](entity.dal$entityRef, loadTt, EntityOps.getFromContext(vtFrom, ttFrom), None)
    }

    def bitemporalSpaceWithFilter(
        predicate: A => Boolean,
        fromTemporalContext: Option[TemporalContext] = None,
        toTemporalContext: Option[TemporalContext] = None)(implicit
        resolver: BitemporalSpaceResolver): EntityBitemporalSpace[A] =
      macro EntityOpsMacros.bitemporalSpaceWithFilter[A]

    @scenarioIndependent @node def bitemporalSpace(
        fromTemporalContext: Option[TemporalContext] = None,
        toTemporalContext: Option[TemporalContext] = None): EntityBitemporalSpace[A] = {
      require(!entity.dal$isTemporary, "Cannot load the bitemporal points for the entity that is not loaded from DAL.")
      val rtt = getEntityTransactionTime(entity)
      EntityOps.getBitemporalSpace[A](entity.dal$entityRef, rtt, fromTemporalContext, toTemporalContext)
    }
  }

  object EntityOps {
    def getFromContext(vtFrom: Option[Instant], ttFrom: Option[Instant]): Option[TemporalContext] = {
      (vtFrom, ttFrom) match {
        case (None, None)                 => None
        case (None, Some(ttFrom))         => Some(FlatTemporalContext(TimeInterval.NegInfinity, ttFrom, None))
        case (Some(vtFrom), None)         => Some(FlatTemporalContext(vtFrom, TimeInterval.NegInfinity, None))
        case (Some(vtFrom), Some(ttFrom)) => Some(FlatTemporalContext(vtFrom, ttFrom, None))
      }
    }

    @node @scenarioIndependent private[optimus /*platform*/ ] def getBitemporalSpace[A <: Entity](
        key: SerializedKey,
        qt: QueryTemporality,
        rtt: Instant): EntityBitemporalSpace[A] = {
      val data = DALImpl.resolver.getSpaceByKeyWithTemporality(key, qt, rtt)
      val (vtInterval, ttInterval) = qt match {
        case QueryTemporality.At(vt, tt) =>
          (Some(ValidTimeInterval.point(vt)), Some(TimeInterval.point(tt)))
        case QueryTemporality.ValidTime(vt)                    => (Some(ValidTimeInterval.point(vt)), None)
        case QueryTemporality.TxTime(txTime)                   => (None, Some(TimeInterval.point(txTime)))
        case QueryTemporality.BitempRange(vtRange, ttRange, _) => (Some(vtRange), Some(ttRange))
        case QueryTemporality.All                              => (None, None)
      }
      EntityBitemporalSpaceImpl[A](data.toSet, rtt, vtInterval, ttInterval)
    }

    @node @scenarioIndependent def getBitemporalSpaceWithFilter[A <: Entity](
        wrapper: EntityOps[A],
        predicate: LambdaElement,
        fromTemporalContext: Option[TemporalContext],
        toTemporalContext: Option[TemporalContext],
        resolver: BitemporalSpaceResolver): EntityBitemporalSpace[A] =
      resolver.getEntityBitemporalSpace[A](wrapper.entity, predicate, fromTemporalContext, toTemporalContext)

    private[optimus /**/ ] def getVtTtIntervalsFromTemporalContexts(
        fromTemporalContext: Option[TemporalContext],
        toTemporalContext: Option[TemporalContext],
        rtt: Instant): (ValidTimeInterval, TimeInterval) = {
      require(
        fromTemporalContext == None || fromTemporalContext.get.unsafeTxTime.isBefore(rtt),
        s"argument tt should be earlier than context transaction time, tt=${fromTemporalContext.get.unsafeTxTime} rtt=${rtt}"
      )
      (fromTemporalContext, toTemporalContext) match {
        case (None, None) => (ValidTimeInterval.max, TimeInterval(TimeInterval.NegInfinity, rtt))
        case (None, Some(toContext)) =>
          (
            ValidTimeInterval(TimeInterval.NegInfinity, toContext.unsafeValidTime),
            TimeInterval(TimeInterval.NegInfinity, toContext.unsafeTxTime))
        case (Some(fromContext), None) =>
          (
            ValidTimeInterval(fromContext.unsafeValidTime, TimeInterval.Infinity),
            TimeInterval(fromContext.unsafeTxTime, rtt))
        case (Some(fromContext), Some(toContext)) =>
          (
            ValidTimeInterval(fromContext.unsafeValidTime, toContext.unsafeValidTime),
            TimeInterval(fromContext.unsafeTxTime, toContext.unsafeTxTime))
      }
    }

    @node @scenarioIndependent def getBitemporalSpace[A <: Entity](
        ref: EntityReference,
        rtt: Instant,
        fromTemporalContext: Option[TemporalContext] = None,
        toTemporalContext: Option[TemporalContext] = None): EntityBitemporalSpace[A] = {
      val (vtInterval, ttInterval) = getVtTtIntervalsFromTemporalContexts(fromTemporalContext, toTemporalContext, rtt)
      val qt = QueryTemporality.BitempRange(vtInterval, ttInterval, false)
      val data = DALImpl.resolver.getSpaceByRefWithTemporality(ref, qt, rtt)
      EntityBitemporalSpaceImpl[A](data.toSet, rtt, Some(vtInterval), Some(ttInterval))
    }

    @node @scenarioIndependent private[optimus] def getEntityVersionsInRangeWithoutManifest[A <: Entity](
        fromTemporalContext: TemporalContext,
        toTemporalContext: TemporalContext,
        typeName: String,
        rangeQueryOpts: RangeQueryOptions
    ): Seq[EntityVersionHolder[A]] = {
      val data = DALImpl.resolver.getSpaceByClassAppIdUserIdInRange(
        fromTemporalContext,
        toTemporalContext,
        typeName,
        rangeQueryOpts)
      val vrefHolders = data.groupBy(_.vref) map { case (_, rect) =>
        EntityVersionHolder(EntityBitemporalSpaceImpl[A](rect.toSet, toTemporalContext.unsafeTxTime))
      }
      vrefHolders.toSeq.sortBy(vr => (vr.creationContext.unsafeTxTime, vr.creationContext.unsafeValidTime))
    }

    @node @scenarioIndependent private[optimus] def getEntityChangesInRangeWithFilters[T <: Entity](
        keysSeq: Seq[Seq[SerializedKey]],
        entityCls: Class[T],
        range: DSIQueryTemporality.OpenVtTxRange): Seq[Seq[EntityChange[T]]] = {
      require(keysSeq.nonEmpty && keysSeq.forall(_.nonEmpty))

      def buildValidatedEntityChanges(changes: Seq[Seq[EntityChange[T]]]) = {
        // if last rect ttTo is earlier then range.to, this means that there is no version at the end of the range
        // so we need to add an entity change from last rect to None to capture this change
        val entityChangesWithEndChange = changes map { changes =>
          val lastRectTTTo = changes.last.to.map { lastChangeTo =>
            lastChangeTo.asInstanceOf[EntityVersionHolder[T]].bitempSpace.all.last.ttTo
          }
          lastRectTTTo
            .map { tt =>
              if (tt isBefore range.range.to) changes :+ EntityChange(changes.last.eref, changes.last.to, None, None)
              else changes
            }
            .getOrElse(changes)
        }

        // since we got persistent entity from server, so we might have missed out invalidated rects if they happen to fall in between rects
        val entityChangesWithInvalidates = entityChangesWithEndChange map { changes =>
          changes flatMap { ch =>
            if (ch.from.isDefined && ch.to.isDefined) {
              val fromTTTo = ch.from.get.asInstanceOf[EntityVersionHolder[T]].bitempSpace.all.last.ttTo
              val toTTFrom = ch.to.get.asInstanceOf[EntityVersionHolder[T]].bitempSpace.all.head.ttFrom
              if (fromTTTo.equals(toTTFrom)) ch :: Nil
              else {
                val change1 = EntityChange(ch.eref, ch.from, None, None)
                val change2 = EntityChange(ch.eref, None, ch.to, ch.readTemporalContext)
                change1 :: change2 :: Nil
              }

            } else ch :: Nil
          }
        }

        val res = entityChangesWithInvalidates.map { ec =>
          ec.filter { ch =>
            if (ch.readTemporalContext.isDefined)
              !(ch.readTemporalContext.get.asInstanceOf[FlatTemporalContext].tt isBefore range.range.from)
            else true
          }
        }
        require(res.forall(_.nonEmpty))
        res
      }

      val cmd = ExpressionHelper.buildExpressionQueryCommand(keysSeq, entityCls, range)
      val pes: Iterable[PersistentEntity] = EvaluationContext.env.entityResolver
        .asInstanceOf[EntityResolverReadImpl]
        .findByExpressionCommand(cmd)
        .result
        .map(a => a(0).asInstanceOf[PersistentEntity])
      val data: Iterable[SelectSpaceResult.Rectangle] =
        pes.map(p => SelectSpaceResult.Rectangle(p.txInterval, p.vtInterval, p.versionedRef, p.entityRef, Some(p)))
      val entityChanges = buildEntityChangeFromSpace[T](data, range.readTxTime)
      buildValidatedEntityChanges(entityChanges)
    }

    private def buildEntityChangeFromSpace[A <: Entity](
        data: Iterable[SelectSpaceResult.Rectangle],
        rtt: Instant): Seq[Seq[EntityChange[A]]] = {
      // This method will iterate from backwards for the parent sequence and will break once it finds a version whose vt <= currentVersion.vt and tt < currentVersion.tt
      @tailrec def getReverseId(
          it: Iterator[SelectSpaceResult.Rectangle],
          id: Int,
          currVersion: SelectSpaceResult.Rectangle): Option[Int] = {
        if (it.isEmpty) None
        else {
          val vrefHolder = it.next()
          if (
            !(vrefHolder.vtInterval.from isAfter currVersion.vtInterval.from) &&
            (vrefHolder.txInterval.from isBefore currVersion.txInterval.from)
          ) {
            Some(id)
          } else {
            getReverseId(it, id + 1, currVersion)
          }
        }
      }

      // This method will return the offset for those timeslices who have the same tt and lower vts that the current version
      @tailrec def incrementIdxForSameTtOffset(
          it: Iterator[SelectSpaceResult.Rectangle],
          id: Int,
          currVersion: SelectSpaceResult.Rectangle): Int = {
        if (it.isEmpty) id
        else {
          val vrefHolder = it.next()
          require(!(vrefHolder.txInterval.from isAfter currVersion.txInterval.from))
          if (vrefHolder.vtInterval.from isAfter currVersion.vtInterval.from) {
            id
          } else {
            incrementIdxForSameTtOffset(it, id + 1, currVersion)
          }
        }
      }

      val entityChanges = data
        .groupBy(_.eref)
        .apar
        .map {
          case (e, rects) =>
            // creating this var because we need to ignore the clonewithclosedvt timeslices
            var seen = Set.empty[VersionedReference]
            // this var will basically contain all the entity changes for a particular eref
            // this couldn't be returned as the output of foldLeft because we are creating this sequence as a side-effect of the fold-left
            // the fold-left in itself creates the sequence of timeslices in the order of parents
            var changes = Seq.empty[ChangeHolder]
            val sortedRects = rects.toSeq.sortBy(r => (r.txInterval.from, r.vtInterval.from))
            sortedRects.foldLeft(Vector.empty[SelectSpaceResult.Rectangle]) { (parentSeq, currentV) =>
              parentSeq match {
                case x if x.isEmpty =>
                  seen += currentV.vref
                  currentV.vref match {
                    case VersionedReference.Nil => // do nothing
                    case _ =>
                      changes = changes :+ ChangeHolder(
                        ParentVersionHolder(None),
                        ChildVersionHolder(Some(currentV), currentV.vtInterval.from, currentV.txInterval.from))
                  }
                  Vector(currentV)
                case _ =>
                  if ((seen contains currentV.vref) && currentV.vref != VersionedReference.Nil) {
                    parentSeq
                  } else {
                    seen += currentV.vref
                    val it = parentSeq.reverseIterator
                    val idx = getReverseId(it, 0, currentV) match {
                      case Some(i) => {
                        // If there are no parents of the tsc, then idx would have evaluated to 0 and hence the value of idx-1 would
                        // would be -1. We are adding 1 to the index to avoid this fencepost error and hence now it will evaluate to 1.
                        (parentSeq.size - i) + 1
                      }
                      case None => 1
                    }
                    val parentVersion = idx match {
                      case id if id > 1 =>
                        // we are offsetting by 2 to account for the extra +1 we did earlier
                        if (parentSeq(id - 2).vref != VersionedReference.Nil) Some(parentSeq(id - 2))
                        else None
                      case _ => None
                    }
                    val childVersionWithVtTt = currentV.vref match {
                      case VersionedReference.Nil =>
                        ChildVersionHolder(None, currentV.vtInterval.from, currentV.txInterval.from)
                      case _ => ChildVersionHolder(Some(currentV), currentV.vtInterval.from, currentV.txInterval.from)
                    }
                    if (parentVersion.isDefined || childVersionWithVtTt.rectOpt.isDefined) {
                      changes = changes :+ ChangeHolder(ParentVersionHolder(parentVersion), childVersionWithVtTt)
                    }
                    // splitting here because we want to "insert" the current node at its proper place in the parent sequence
                    // we are also offsetting by 1 to account for the +1 we did earlier
                    val (pre, post) = parentSeq.splitAt(idx - 1)
                    val postIdx = incrementIdxForSameTtOffset(post.iterator, 0, currentV)
                    // this split is required to adjust for the same tt multi vt scenario
                    val (preSameTt, postSameTt) = post.splitAt(postIdx)
                    (pre ++ (preSameTt :+ currentV)) ++ postSameTt
                  }
              }
            }
            val vrefData = rects groupBy (_.vref)
            val changeList = changes map { case ChangeHolder(parent, childVersionHolder) =>
              val parentHolder = parent.rectOpt match {
                case Some(rect) =>
                  Some(EntityVersionHolder(EntityBitemporalSpaceImpl[A](vrefData(rect.vref).toSet, rtt)))
                case _ => None
              }
              val childHolder = childVersionHolder.rectOpt match {
                case Some(rect) =>
                  Some(EntityVersionHolder(EntityBitemporalSpaceImpl[A](vrefData(rect.vref).toSet, rtt)))
                case _ => None
              }
              val unsafeValidTime = childVersionHolder.unsafeValidTime
              val unsafeTxTime = childVersionHolder.unsafeTxTime
              EntityChange(
                e,
                parentHolder,
                childHolder,
                Some(FlatTemporalContext(DataFreeTemporalSurfaceMatchers.all, unsafeValidTime, unsafeTxTime, None)))
            }
            changeList
          case _ => Nil
        }(Seq.breakOut)
      entityChanges
    }

    @node @scenarioIndependent private[optimus] def getEntityChangesWithoutManifest[A <: Entity](
        fromTemporalContext: TemporalContext,
        toTemporalContext: TemporalContext,
        typeName: String,
        rangeQueryOpts: RangeQueryOptions = RangeQueryOptions(inRange = true)
    ): Seq[Seq[EntityChange[A]]] = {
      if (!rangeQueryOpts.inRange)
        throw new IllegalArgumentException(
          s"getEntityChangesWithoutManifest expects inRange to be true, but received: $rangeQueryOpts")
      val vtFrom = fromTemporalContext.unsafeValidTime
      val ttFrom = fromTemporalContext.unsafeTxTime

      val data = DALImpl.resolver.getSpaceByClassAppIdUserIdInRange(
        fromTemporalContext,
        toTemporalContext,
        typeName,
        rangeQueryOpts)
      val entityChanges = buildEntityChangeFromSpace[A](data, toTemporalContext.unsafeTxTime)
      // we are filtering all those changes which occur before the user's given fromTc
      val res = entityChanges.map { ec =>
        ec.filter { ch =>
          require(ch.readTemporalContext.isDefined)
          (!(ch.readTemporalContext.get.unsafeValidTime isBefore vtFrom) && !(ch.readTemporalContext.get.unsafeTxTime isBefore ttFrom))
        }
      }
      res.filterNot(_.isEmpty)
    }
  }

  class EntityCompanionOps[A <: Entity](val companion: EntityCompanionBase[A]) extends TemporalContextAPI {

    @node @scenarioIndependent def entityVersionsInRange(
        fromTemporalContext: TemporalContext,
        appId: Option[String] = None,
        userId: Option[String] = None): Seq[EntityVersionHolder[A]] = {
      val rangeQueryOpts = RangeQueryOptions(appId = appId, userId = userId)
      val typeName = companion.info.runtimeClass.getName
      EntityOps
        .getEntityVersionsInRangeWithoutManifest(fromTemporalContext, loadContext, typeName, rangeQueryOpts)
    }

    @node @scenarioIndependent def entityChangesInRange(
        fromTemporalContext: TemporalContext,
        appId: Option[String] = None,
        userId: Option[String] = None): Seq[Seq[EntityChange[A]]] = {
      val rangeQueryOpts = RangeQueryOptions(appId, userId, inRange = true)
      val typeName = companion.info.runtimeClass.getName
      EntityOps.getEntityChangesWithoutManifest(fromTemporalContext, loadContext, typeName, rangeQueryOpts)
    }
  }

  class BusinessEventOps[A <: BusinessEvent](val event: A) {
    @node def entities: Iterable[Entity] = {
      DALImpl.resolver.getAssociatedEntities(event.dal$eventRef, event.getClass, event.validTime, event.dal$loadTT)
    }

    @node def entitiesOfType[T <: Entity: ClassTag]: Iterable[Entity] = {
      val typeName = implicitly[ClassTag[T]].runtimeClass.getName
      DALImpl.resolver.getAssociatedEntities(
        event.dal$eventRef,
        event.getClass,
        event.validTime,
        event.dal$loadTT,
        Some(typeName))
    }

    @node private[optimus /*platform*/ ] def persistentEntities: Iterable[PersistentEntity] = {
      DALImpl.resolver.getAssociatedPersistentEntities(
        event.dal$eventRef,
        event.getClass,
        event.validTime,
        event.dal$loadTT)
    }

    @node def transactions: ListMap[TemporalContext, Set[EntityPointHolder]] = {
      val data = DALImpl.resolver.getEventTransactions(event.dal$eventRef, event.getClass, event.dal$loadTT)
      ListMap(SortedMap(data.groupBy { _.txInterval.from }.toSeq: _*).iterator.map { case (tt, versions) =>
        val context: TemporalContext = FlatTemporalContext(event.validTime, tt, None)
        context -> versions.map(v => EntityPointHolder(v.eref, context)).toSet
      }.toSeq: _*)
    }
  }
}

final case class Knot(transactionTime: Instant)
private[optimus /*dal*/ ] final case class ParentVersionHolder(rectOpt: Option[SelectSpaceResult.Rectangle])
private[optimus /*dal*/ ] final case class ChildVersionHolder(
    rectOpt: Option[SelectSpaceResult.Rectangle],
    unsafeValidTime: Instant,
    unsafeTxTime: Instant)
private[optimus /*dal*/ ] final case class ChangeHolder(
    parentVersionHolder: ParentVersionHolder,
    childVersionHolder: ChildVersionHolder)
private[optimus] final case class RangeQueryOptions(
    appId: Option[String] = None,
    userId: Option[String] = None,
    inRange: Boolean = false)
