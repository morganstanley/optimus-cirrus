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

import scala.annotation.nowarn
import optimus.platform._
import optimus.entity._
import optimus.platform.storable._

import java.time.Instant
import scala.util.{Failure, Success, Try}
import optimus.platform.dal.EntityEventModule.EntityBitemporalSpaceImpl
import optimus.platform.internal.{ClassInfo, UnknownEntityReference}
import optimus.platform.internal.{ClassInfoBuilder, TemporalSource}
import optimus.platform.dsi.bitemporal._
import optimus.platform.temporalSurface.operations._
import optimus.platform.temporalSurface.impl.TemporalSurfaceCachedDataAccess
import optimus.platform.storable.EntityMetadata
import optimus.core.CoreHelpers
import optimus.platform.dsi.Feature
import msjava.slf4jutils.scalalog.getLogger
import optimus.platform.AsyncImplicits._
import optimus.core.CoreAPI
import optimus.platform.dsi.bitemporal.Query

import scala.reflect.ClassTag
import optimus.scalacompat.collection._

import scala.collection.immutable.Map.Map1

@entity class RawDataAccess(@transient val er: DSIResolver with EntityResolverReadImpl) {
  import EntityResolverReadImpl.mapResult

  @node @scenarioIndependent
  def findByReferenceAt(ref: EntityReference, at: DSIQueryTemporality.At): Option[PersistentEntity] = {
    log.debug(s"findByReferenceAt(ref = $ref, at = $at)")
    val cmd = Select(ReferenceQuery(ref), at)
    er.decodePersistentEntity(er.executor.executeQuery(er.dsi, cmd))
  }

  @node @scenarioIndependent
  def findByReferenceWithClassNameAt[T <: Entity](
      ref: EntityReference,
      at: DSIQueryTemporality.At,
      entityClass: Class[T]): Option[PersistentEntity] = {
    log.debug(s"findByReferenceWithClassNameAt(ref = $ref, at = $at, className = ${entityClass.getName})")
    val cmd = Select(ReferenceQuery(ref, entityClass.getName), at)
    er.decodePersistentEntity(er.executor.executeQuery(er.dsi, cmd))
  }

  @node @scenarioIndependent
  def findByIndexAt(
      key: SerializedKey,
      queryTime: DSIQueryTemporality.At,
      itemTime: DSIQueryTemporality.At,
      erefs: Set[EntityReference],
      entitledOnly: Boolean = false): Iterable[PersistentEntity] = {
    log.debug(s"findByIndexAt(key = $key, queryTime = $queryTime, itemTime = $itemTime)")
    fetchEntities(SerializedKeyQuery(key, erefs, entitledOnly), queryTime, itemTime)
  }

  @node @scenarioIndependent
  def findLinkedEntitiesAt[E <: Entity](
      targetClass: Class[E],
      lookupSrc: EntityReference,
      propName: String,
      queryTime: DSIQueryTemporality.At,
      itemTime: DSIQueryTemporality.At): Iterable[PersistentEntity] = {
    log.debug(
      s"findLinkedEntitiesAt(targetClass = $targetClass, lookupSrc = $lookupSrc, propName = $propName, queryTime = $queryTime, itemTime = $itemTime)")
    val query = LinkageQuery(targetClass.getName, propName, lookupSrc)
    fetchEntities(query, queryTime, itemTime)
  }

  @node @scenarioIndependent
  def findEntitiesByClassAt[S <: Entity](
      clazz: Class[S],
      queryTime: DSIQueryTemporality.At,
      itemTime: DSIQueryTemporality.At,
      entitledOnly: Boolean = false): Iterable[PersistentEntity] = {
    log.debug(
      s"findEntitiesByClassAt(clazz = $clazz, queryTime = $queryTime, itemTime = $itemTime, " +
        s"entitledOnly = $entitledOnly)")
    fetchEntities(EntityClassQuery(clazz.getName, entitledOnly), queryTime, itemTime)
  }

  @node @scenarioIndependent
  def findEntityByCmidAt[S <: Entity](
      clazz: Class[S],
      cmid: CmReference,
      at: DSIQueryTemporality.At): Option[PersistentEntity] = {
    log.debug(s"findEntitiesByCmidAt(cmid = $cmid, at = $at)")
    val cmd = Select(EntityCmReferenceQuery(cmid, clazz.getName), at)
    er.decodePersistentEntity(er.executor.executeQuery(er.dsi, cmd))
  }

  @node @scenarioIndependent
  def fetchEntities(
      query: Query,
      queryTime: DSIQueryTemporality.At,
      itemTime: DSIQueryTemporality.At): Iterable[PersistentEntity] = {
    val dsiResult = er.executor.executeQuery(er.dsi, Select(query, queryTime))
    log.debug(s"fetchEntities query = $query, queryTime = $queryTime, itemTime = $itemTime => ${CoreHelpers
        .safeToString(dsiResult, null, 1000)}")
    val result = mapResult(dsiResult) { case SelectResult(value) =>
      value
    } getOrElse Nil
    val tsResult =
      if (queryTime == itemTime) result
      else {
        log.debug("fetchEntities remapping to item time")
        result.apar.map { pe =>
          val remapped = findByReferenceAt(pe.entityRef, itemTime)
          remapped.getOrElse(throw new EntityNotMappedException(pe.entityRef, queryTime, itemTime, query))
        }
      }
    tsResult
  }

  // we will do the batch on the server side
  @node @scenarioIndependent
  private[optimus] def findEntityMetadata(entityReference: EntityReference): Option[EntityMetadata] = {
    val cmd = QueryEntityMetadata(entityReference)
    val dsiResult = er.executor.executeQuery(er.dsi, cmd)

    mapResult(dsiResult) { case QueryEntityMetadataResult(meta) =>
      Some(meta)
    }.flatten
  }

  @node @scenarioIndependent
  def findEntitiesByClass[S <: Entity](
      clazz: Class[S],
      when: DSIQueryTemporality,
      entitledOnly: Boolean): Iterable[PersistentEntity] = {
    val cmd = Select(EntityClassQuery(clazz.getName, entitledOnly), when)
    val dsiResult = er.executor.executeQuery(er.dsi, cmd)
    mapResult(dsiResult) { case SelectResult(value) =>
      value
    } getOrElse Nil
  }
}

object EntityResolverReadImpl {
  // These will hit the private def findEntityImpl cache
  findEntity.setCacheable(false)
  findEntityOption.setCacheable(false)

  // These will hit the private def findByReferenceImpl cache
  findByReferenceOption.setCacheable(false)
  findByReference.setCacheable(false)
  findByReferenceWithType.setCacheable(false)

  findEntities.setCacheable(false)
  findEntitiesByClass.setCacheable(false)
  getByKeyWithTemporality.setCacheable(false)
  getByRefWithTemporality.setCacheable(false)
  findEntitiesAuditInfo.setCacheable(false)
  findPersistentEntities.setCacheable(false)
  enumerateKeys.setCacheable(false)

  private[optimus /*platform*/ ] def mapResult[A](r: Result)(pf: PartialFunction[Result, A]) = r match {
    case ErrorResult(err, _)    => throw err
    case VoidResult             => None
    case _ if pf.isDefinedAt(r) => Some(pf(r))
    case o => throw new GeneralDALException(s"Unexpected DSI result type ${o.getClass.getName} :: $o")
  }
}

@entity
trait EntityResolverReadImpl { this: DSIResolver =>
  import EntityResolverReadImpl.mapResult

  @async private[optimus /*platform*/ ] def asyncMapResult[A](r: Result)(f: Result => Boolean)(
      nf: NodeFunction1[Result, A]) = r match {
    case ErrorResult(err, _) => throw err
    case VoidResult          => None
    case _ if f(r)           => Some(nf(r))
    case o                   => throw new GeneralDALException(s"Unexpected DSI result type ${o.getClass.getName} :: $o")
  }

  def translateMarkers(markers: java.util.IdentityHashMap[Marker[_], Tweak]): Seq[Tweak] =
    TemporalSource.translateMarkers(markers)

  private object temporalContextEntityResolver extends TemporalContextEntityResolver {
    private[this] val log = getLogger[temporalContextEntityResolver.type]

    override val toString = s"temporalContextEntityResolver: ${System.identityHashCode(this)}"
    private val classInfoBuilder = new ClassInfoBuilder()

    @node @scenarioIndependent
    override def loadClassInfo(entityRef: EntityReference): ClassInfo = {
      classInfoBuilder.fromRef(entityRef) match {
        case Some(res) => res
        case None =>
          rawDataAccess.findEntityMetadata(entityRef) map classInfoBuilder.fromMeta getOrElse UnknownEntityReference
      }
    }

    @node @scenarioIndependent
    override def getItemKeys(operation: TemporalSurfaceQuery)(
        sourceTemporality: operation.TemporalityType): Seq[operation.ItemKey] = {
      getItemData(operation)(sourceTemporality, sourceTemporality).keySet.toSeq
    }

    @node @scenarioIndependent
    override def getItemData(operation: TemporalSurfaceQuery)(
        sourceTemporality: operation.TemporalityType,
        itemTemporality: operation.TemporalityType): Map[operation.ItemKey, operation.ItemData] = {

      log.debug(
        s"getItemData(operation = $operation, sourceTemporality = $sourceTemporality, itemTemporality = $itemTemporality")
      // Typical node stack from here:
      // ~> getEntityItemData ~> RawDataAccess#findByIndexAt ~> RDA#fetchEntities ~> DALDSIEx#executeQuery ~>
      ///  NR ~> #doExecuteQuery ~> PrcBrokerMultiplexingProxy#executeReadOnlyCommands ~>
      // .. DSIClient#executeCommands ... DALProtoClient#request ~> RequestBatcher#send  Will suspend until we get everything
      operation match {

        case query @ QueryByKey(key) =>
          val (erefs, entitledOnly) = query match {
            case QueryByNonUniqueKey(_, _, _, erefs, entitledOnly_) => (erefs, entitledOnly_)
            case QueryByUniqueKey(_, _, _, entitledOnly_)           => (Set.empty[EntityReference], entitledOnly_)
          }
          val raw = rawDataAccess
            .findByIndexAt(
              key,
              toQueryTemporality(sourceTemporality, itemTemporality),
              toQueryTemporality(itemTemporality),
              erefs,
              entitledOnly)
            .toSeq
          val result: Map[operation.ItemKey, operation.ItemData] = raw.iterator.map { pe: PersistentEntity =>
            (pe.entityRef, pe)
          }.toMap
          if (key.unique) require(result.size < 2)
          log.debug(
            s"Loaded by key: ${result.map { case (ref, pe) => (ref, pe.className, pe.keys) }} [at: $sourceTemporality, $itemTemporality]")
          result

        case query: DataQueryByEntityReference[_] =>
          val pe =
            rawDataAccess.findByReferenceWithClassNameAt(
              query.eRef,
              toQueryTemporality(itemTemporality),
              query.targetClass)
          val result = new Map1(
            query.eRef,
            pe.getOrElse(
              throw new EntityNotMappedException(
                query.eRef,
                toQueryTemporality(sourceTemporality),
                toQueryTemporality(itemTemporality),
                ReferenceQuery(query.eRef)))
          )
          log.debug(s"Loaded by entity reference: ${result
              .map { case (ref, pe) => (ref, pe.className, pe.keys) }} [at: $sourceTemporality, $itemTemporality]")
          result

        case query @ QueryByClass(_, clazz, entitledOnly) =>
          val raw = rawDataAccess
            .findEntitiesByClassAt(
              clazz,
              toQueryTemporality(sourceTemporality, itemTemporality),
              toQueryTemporality(itemTemporality),
              entitledOnly)
            .toSeq
          val result: Map[operation.ItemKey, operation.ItemData] = raw.iterator.map { pe: PersistentEntity =>
            (pe.entityRef, pe)
          }.toMap
          log.debug(
            s"Loaded by class: ${result.map { case (ref, pe) => (ref, pe.className, pe.keys) }} [at: $sourceTemporality, $itemTemporality]")
          result

        case query @ QueryByLinkage(_, clazz, lookup, propertyName) =>
          val raw = rawDataAccess
            .findLinkedEntitiesAt(
              clazz,
              lookup,
              propertyName,
              toQueryTemporality(sourceTemporality, itemTemporality),
              toQueryTemporality(itemTemporality))
            .toSeq
          val result: Map[operation.ItemKey, operation.ItemData] = raw.iterator.map { pe: PersistentEntity =>
            (pe.entityRef, pe)
          }.toMap
          log.debug(s"Loaded by linkage: ${result
              .map { case (ref, pe) => (ref, pe.className, pe.keys) }} [at: $sourceTemporality, $itemTemporality]")
          result

      }
    }

    @node @scenarioIndependent
    override def getSingleItemData(operation: TemporalSurfaceQuery)(
        temporality: operation.TemporalityType,
        key: operation.ItemKey): operation.ItemData = {
      operation match {
        case eq: QueryByEntityReference[_] =>
          val eref = key.asInstanceOf[eq.ItemKey]
          val eqTemp = temporality.asInstanceOf[eq.TemporalityType]
          rawDataAccess
            .findByReferenceWithClassNameAt(eref, toQueryTemporality(eqTemp), eq.targetClass)
            .asInstanceOf[Option[operation.ItemData]]
            .getOrElse(throw new SingleItemDataNotFoundForEntityRefException(eref, toQueryTemporality(eqTemp)))

        case eq: TemporalSurfaceQuery =>
          val eref = key.asInstanceOf[eq.ItemKey]
          val eqTemp = temporality.asInstanceOf[eq.TemporalityType]
          rawDataAccess
            .findByReferenceAt(eref, toQueryTemporality(eqTemp))
            .asInstanceOf[Option[operation.ItemData]]
            .getOrElse(throw new SingleItemDataNotFoundForEntityRefException(eref, toQueryTemporality(eqTemp)))
      }
    }

    private def toQueryTemporality(qt: QueryTemporality.At) = DSIQueryTemporality.At(qt.validTime, qt.txTime)

    // TODO (OPTIMUS-13067): HACK until we always have a temporality
    private def toQueryTemporality(qt: QueryTemporality.At, default: QueryTemporality.At): DSIQueryTemporality.At =
      toQueryTemporality(if (qt == null) default else qt)

    private[optimus] val rawDataAccess = RawDataAccess(EntityResolverReadImpl.this)

  }
  private[optimus] val dataAccess = new TemporalSurfaceCachedDataAccess(temporalContextEntityResolver)

  val executor: DSIExecutor = DALDSIExecutor

  @node @scenarioIndependent
  override private[optimus] def getPersistentEntityByRef(
      entityRef: EntityReference,
      tc: TemporalContext): Option[PersistentEntity] = {
    getPersistentEntityByRefImpl(entityRef, tc)
  }

  @node @scenarioIndependent
  private[optimus] def getPersistentEntityByRefImpl(
      entityRef: EntityReference,
      tc: TemporalContext): Option[PersistentEntity] = {
    tc.dataAccess(QueryPersistentEntityByEntityReference(dataAccess, entityRef))
  }

  @node @scenarioIndependent
  def findByReference(ref: EntityReference, tc: TemporalContext): Entity = {
    findByReferenceWithTypeImpl(tc, EntityReferenceQuery(ref)).getOrElse { throw new LinkResolutionException(ref, tc) }
  }

  @node @scenarioIndependent
  def findByReferenceOption(ref: EntityReference, tc: TemporalContext): Option[Entity] = {
    findByReferenceWithTypeImpl(tc, EntityReferenceQuery(ref))
  }

  @node @scenarioIndependent
  private[optimus] def findByReferenceWithType[T <: Entity](
      ref: EntityReference,
      tc: TemporalContext,
      clazz: Class[T],
      clazzIsActualType: Boolean,
      reason: EntityReferenceQueryReason): T = {
    findByReferenceWithTypeImpl(tc, EntityReferenceQuery(ref, clazz, clazzIsActualType, reason)) getOrElse {
      throw new LinkResolutionException(ref, tc)
    }
  }

  @node @scenarioIndependent
  private[optimus] def findByReferenceWithTypeOption[T <: Entity](
      ref: EntityReference,
      tc: TemporalContext,
      clazz: Class[T],
      clazzIsActualType: Boolean,
      reason: EntityReferenceQueryReason): Option[T] = {
    findByReferenceWithTypeImpl(tc, EntityReferenceQuery(ref, clazz, clazzIsActualType, reason))
  }

  /**
   * implementation note - we wrap the non result affecting parameters in a queryDetails as this has identity based on
   * eref only
   */
  @node @scenarioIndependent
  private def findByReferenceWithTypeImpl[T <: Entity](
      tc: TemporalContext,
      queryDetails: EntityReferenceQuery[T]): Option[T] = {
    @nowarn("msg=10500 optimus.core.CoreAPI.nodeResultCurrentCS")
    val res = CoreAPI.nodeResultCurrentCS {
      tc.dataAccess(QueryByEntityReference(dataAccess, queryDetails))
    }
    wrapQueryDSIException {
      res.value
    }
  }

  @node @scenarioIndependent
  private def findEntityImpl[E <: Entity](key: UniqueKey[E], tc: TemporalContext): Option[E] = {
    @nowarn("msg=10500 optimus.core.CoreAPI.nodeResultCurrentCS")
    val res = CoreAPI.nodeResultCurrentCS {
      tc.dataAccess(QueryByUniqueKey(dataAccess, key, entitledOnly = false))
    }
    wrapQueryDSIException {
      res.value
    }
  }
  @node @scenarioIndependent
  override def findEntity[E <: Entity](key: Key[E], tc: TemporalContext): E = {
    key match {
      case u: UniqueKey[E] =>
        findEntityImpl(u, tc) getOrElse {
          throw new optimus.platform.dal.EntityNotFoundException(key, tc)
        }
      case _ => throw new IllegalStateException(s"key must be unique, not ${key.getClass}")
    }
  }

  @node @scenarioIndependent
  override def findEntityOption[E <: Entity](key: Key[E], tc: TemporalContext): Option[E] = {
    key match {
      case u: UniqueKey[E] => findEntityImpl(u, tc)
      case _               => throw new IllegalStateException(s"key must be unique, not ${key.getClass}")
    }
  }

  @node @scenarioIndependent def findEvent[E <: BusinessEvent](key: Key[E], tc: HasTTContext): E =
    findEventOption(key, tc) getOrElse { throw new EventNotFoundException(key, tc) }

  @node @scenarioIndependent
  def findEventOption[E <: BusinessEvent](key: Key[E], tc: HasTTContext): Option[E] = {
    findEventRaw(key.toSerializedKey, tc).map {
      EventSerializer.deserializeBusinessEvent(tc)(_).asInstanceOf[E]
    }
  }

  @node @scenarioIndependent
  def findEventRaw(serKey: SerializedKey, tc: HasTTContext): Option[SerializedBusinessEvent] = {
    val tt = resolveEventLoadTT(tc, serKey.typeName)
    val cmd = GetBusinessEventByKey(serKey, tt)
    val dsiResult = executor.executeQuery(dsi, cmd)

    mapResult[Option[SerializedBusinessEvent]](dsiResult) { case GetBusinessEventResult(result) =>
      assert(result.size <= 1, "findEventRaw: Wrong result size")
      result.headOption
    } getOrElse None
  }

  @node @scenarioIndependent
  def findEventsByIndexRaw(
      serKey: SerializedKey,
      tc: HasTTContext,
      entitledOnly: Boolean = false): Iterable[SerializedBusinessEvent] = {
    val tt = resolveEventLoadTT(tc, serKey.typeName)
    val cmd = Select(EventSerializedKeyQuery(serKey, entitledOnly), DSIQueryTemporality.TxTime(tt))
    val dsiResult = executor.executeQuery(dsi, cmd)

    mapResult[Iterable[SerializedBusinessEvent]](dsiResult) { case GetBusinessEventResult(result) =>
      result
    }.getOrElse(Iterable.empty)
  }

  @node @scenarioIndependent
  override def findByIndex[E <: Entity](
      key: Key[E],
      tc: TemporalContext,
      classInfo: Option[Class[E]] = None,
      entitledOnly: Boolean = false): Iterable[E] = {
    key match {
      case u: UniqueKey[E] =>
        val opt =
          if (classInfo.isDefined) QueryByUniqueKey(dataAccess, u.toSerializedKey, classInfo.get, entitledOnly)
          else QueryByUniqueKey(dataAccess, u, entitledOnly)
        tc.dataAccess(opt).toList
      case n: NonUniqueKey[E] =>
        val opt =
          if (classInfo.isDefined)
            QueryByNonUniqueKey(dataAccess, n.toSerializedKey, classInfo.get, Set.empty[EntityReference], entitledOnly)
          else QueryByNonUniqueKey(dataAccess, n.toSerializedKey, Set.empty[EntityReference], entitledOnly)
        tc.dataAccess(opt)
    }
  }

  @node @scenarioIndependent
  override def findByIndexWithEref[E <: Entity](
      key: Key[E],
      erefs: Iterable[EntityReference],
      tc: TemporalContext): Iterable[E] = {
    key match {
      case n: NonUniqueKey[E] =>
        require(
          dsi.serverFeatures().supports(Feature.SerializeKeyWithRefFilter),
          "Server is not supporting read of non unique indexed with entity references filter")
        if (erefs.isEmpty) {
          log.warn(s"Tried to find entities with empty eref filter by key ${key.toSerializedKey}. Is this intended ?")
          Iterable.empty
        } else tc.dataAccess(QueryByNonUniqueKey(dataAccess, n.toSerializedKey, erefs.toSet))
      case u: UniqueKey[E] =>
        throw new GeneralDALException(s"Tried to find entities with eref filter by unique index key $key")
    }
  }

  @node @scenarioIndependent
  override def findByIndexInRange[E <: Entity](
      key: Key[E],
      fromTemporalContext: TemporalContext,
      toTemporalContext: TemporalContext): Iterable[VersionHolder[E]] = {
    require(!key.toSerializedKey.refFilter, s"not supported for queryByEref=true index: ${key.toSerializedKey}")
    if (dsi.serverFeatures().supports(Feature.RangeQuery)) {
      val query = SerializedKeyQuery(key.toSerializedKey)
      val temporalQuery = key match {
        case u: UniqueKey[E] =>
          throw new IllegalStateException("Query allowed for findByIndexInRange is of type non - unique")
        case nu: NonUniqueKey[E] =>
          QueryByNonUniqueKey(dataAccess, nu, Set.empty[EntityReference])
      }

      val (vf, vt, tf, tt) = (
        findValidTime(temporalQuery, fromTemporalContext),
        findValidTime(temporalQuery, toTemporalContext),
        findTxTime(temporalQuery, fromTemporalContext),
        findTxTime(temporalQuery, toTemporalContext))
      val qt = DSIQueryTemporality.BitempRange(TimeInterval(vf, vt), TimeInterval(tf, tt), false)
      val data = getSpaceImplWithDSITemporality(query, qt, tt)
      val vrefHolders = data.groupBy(_.vref) map { case (v, rect) =>
        EntityVersionHolder(EntityBitemporalSpaceImpl[E](rect.toSet, toTemporalContext.unsafeTxTime))
      }

      // We create creationTime as flatTemporalContext so it is safe to call unsafeValidTime method here
      vrefHolders.toSeq.sortBy(v => (v.creationContext.unsafeValidTime, v.creationContext.unsafeTxTime))
    } else {
      throw new UnsupportedOperationException("Server doesn't support RangeQuery feature")
    }
  }

  @node @scenarioIndependent
  def findEventsByIndex[E <: BusinessEvent](
      key: Key[E],
      tc: TemporalContext,
      entitledOnly: Boolean = false): Iterable[E] = {
    val serializedKey = key.toSerializedKey
    if (!serializedKey.indexed || serializedKey.unique)
      throw new GeneralDALException(s"Tried to find events by index with a non index key $key")
    findEventsByIndexRaw(serializedKey, tc, entitledOnly).map {
      EventSerializer.deserializeBusinessEvent(tc)(_).asInstanceOf[E]
    }
  }

  private[dal] def decodePersistentEntity(result: Result): Option[PersistentEntity] = {
    mapResult(result) {
      // This is the expected result--we found some payload.
      case SelectResult(Seq(value)) => value
      // This is the result when we query outside of the bitemporal rectangles
      // and there's no payload.
      case SelectResult(Nil) => PersistentEntity.Null
    } orElse None // This is the result when we remap from queryTime to
    // itemTime via fetchEntities -> findByReferenceAt and do not find the
    // entity at itemTime.
  }

  protected def toQueryTemporality(qt: QueryTemporality.At, rtt: Instant) =
    DSIQueryTemporality.At(qt.validTime, qt.txTime)

  protected def toDSITemporality(when: QueryTemporality, rtt: Instant) = when match {
    case QueryTemporality.At(vt, tt)    => DSIQueryTemporality.At(vt, tt)
    case QueryTemporality.ValidTime(vt) => DSIQueryTemporality.ValidTime(vt, rtt)
    case QueryTemporality.TxTime(tt)    => DSIQueryTemporality.TxTime(tt)
    case QueryTemporality.BitempRange(vtInterval: ValidTimeInterval, ttInterval: TimeInterval, inRange) =>
      require(
        !ttInterval.to.isAfter(rtt),
        s"ttInterval.to should be no later than rtt, ttInterval.to: ${ttInterval.to}, rtt: $rtt")
      DSIQueryTemporality.BitempRange(TimeInterval(vtInterval.from, vtInterval.to), ttInterval, inRange)
    case QueryTemporality.All => DSIQueryTemporality.All(rtt)
  }

  // The readTxTime is used to ensure the underlying mongodb reaches this tt, but
  // it is not used to guarantee that the Grouping is persisted before this tt.
  // Thus this method is @impure
  @impure @async def countGroupings(query: Query, readTxTime: Instant): Long = {
    val cmd = query match {
      case q @ EventClassQuery(_, false)  => CountGroupings(q, readTxTime)
      case q @ EntityClassQuery(_, false) => CountGroupings(q, readTxTime)
      case q => throw new GeneralDALException(s"Not supported query type in countGroupings: $q")
    }
    // we do not put 'executeQuery' into the NodeFunction since we do not want to cache the result
    withFeatureCheck(Feature.CountGroupings)(asNode(() => true))
    val dsiResult = executor.executeQuery(dsi, cmd)
    mapResult(dsiResult) { case CountResult(num) =>
      num
    } getOrElse 0
  }

  @node @scenarioIndependent
  private def countImpl(query: Query, temporality: DSIQueryTemporality): Long = {
    val cmd = Count(query, temporality)
    val dsiResult = executor.executeQuery(dsi, cmd)
    mapResult(dsiResult) { case CountResult(num) =>
      num
    } getOrElse 0
  }

  @node @scenarioIndependent
  def count(query: Query, temporality: DSIQueryTemporality): Long = {

    query match {
      case EventClassQuery(_, false) | EntityClassQuery(_, false) => countImpl(query, temporality)
      case SerializedKeyQuery(key, _, false) if !key.unique       => countImpl(query, temporality)
      case q => throw new GeneralDALException(s"Not supported query type in count: $q")
    }

  }

  @node @scenarioIndependent
  private[optimus] def findEntitiesWithRtt[S <: Entity: ClassTag](
      qt: QueryTemporality,
      rtt: Instant,
      entitledOnly: Boolean = false): Iterable[S] = {
    findEntitiesByClassWithRtt[S](implicitly[ClassTag[S]].runtimeClass.asInstanceOf[Class[S]], qt, rtt, entitledOnly)
  }

  @node
  def findEntities[S <: Entity: ClassTag](qt: QueryTemporality, entitledOnly: Boolean = false): Iterable[S] = {
    val rtt = findTransactionTime(
      QueryByClass(dataAccess, implicitly[ClassTag[S]].runtimeClass.asInstanceOf[Class[S]], entitledOnly))
    findEntitiesWithRtt(qt, rtt, entitledOnly)
  }

  @node @scenarioIndependent
  def findEntitiesAt[S <: Entity: ClassTag](qt: QueryTemporality.At, entitledOnly: Boolean = false): Iterable[S] = {
    findEntitiesWithRtt(qt, qt.txTime, entitledOnly)
  }

  @node @scenarioIndependent
  def findEntitiesAtTc[S <: Entity: ClassTag](tc: TemporalContext, entitledOnly: Boolean = false): Iterable[S] = {
    val dsiQt = QueryTemporalityFinder.findQueryTemporality(
      tc,
      QueryByClass(dataAccess, implicitly[ClassTag[S]].runtimeClass.asInstanceOf[Class[S]], entitledOnly))
    val qt = QueryTemporality.At(dsiQt.validTime, dsiQt.txTime)
    findEntitiesWithRtt(qt, qt.txTime, entitledOnly)
  }

  @node @scenarioIndependent private def queryDeserializeLocal[S](
      e: PersistentEntity,
      when: DSIQueryTemporality,
      clazz: Class[S]): S = {
    val tc = when match {
      case DSIQueryTemporality.At(validTime: Instant, txTime: Instant) =>
        DALImpl.TemporalContext(validTime, txTime)
      case _ => DALImpl.TemporalContext(e.vtInterval.from, e.txInterval.from)
    }
    clazz.cast(tc.deserialize(e, DSIStorageInfo.fromPersistentEntity(e)))
  }

  @node @scenarioIndependent
  private[optimus] def findEntitiesByClassWithRtt[S <: Entity](
      clazz: Class[S],
      qt: QueryTemporality,
      rtt: Instant,
      entitledOnly: Boolean = false): Iterable[S] = {

    val when = toDSITemporality(qt, rtt)

    val blobs = temporalContextEntityResolver.rawDataAccess.findEntitiesByClass(clazz, when, entitledOnly)

    blobs.apar.map { queryDeserializeLocal(_, when, clazz) }
  }

  @node
  def findEntitiesByClass[S <: Entity](
      clazz: Class[S],
      qt: QueryTemporality,
      entitledOnly: Boolean = false): Iterable[S] = {
    val rtt = findTransactionTime(QueryByClass(dataAccess, clazz, entitledOnly))
    findEntitiesByClassWithRtt(clazz, qt, rtt, entitledOnly)
  }

  @node @scenarioIndependent
  def findEntitiesByClassAt[S <: Entity](
      clazz: Class[S],
      qt: QueryTemporality.At,
      entitledOnly: Boolean = false): Iterable[S] = {
    findEntitiesByClassWithRtt(clazz, qt, qt.txTime, entitledOnly)
  }

  @node @scenarioIndependent
  private[optimus] def findEntitiesByClassWithContext[S <: Entity](
      clazz: Class[S],
      tc: TemporalContext,
      entitledOnly: Boolean = false): Iterable[S] = {
    tc.dataAccess(QueryByClass(dataAccess, clazz, entitledOnly))
  }

  @node @scenarioIndependent
  def findEvents[B <: BusinessEvent: ClassTag](
      tc: HasTTContext = DALImpl.loadContext,
      entitledOnly: Boolean = false): Iterable[B] = {
    val typeName = implicitly[ClassTag[B]].runtimeClass.getName
    findEventsRaw(typeName, tc, entitledOnly) map {
      EventSerializer.deserializeBusinessEvent(tc)(_).asInstanceOf[B]
    }
  }

  @node @scenarioIndependent
  def findEventsRaw(
      typeName: String,
      tc: HasTTContext,
      entitledOnly: Boolean = false): Iterable[SerializedBusinessEvent] = {
    val tt = resolveEventLoadTT(tc, typeName)
    val cmd = GetBusinessEventsByClass(EventClassQuery(typeName, entitledOnly), tt)

    val dsiResult = executor.executeQuery(dsi, cmd)

    dsiResult match {
      case GetBusinessEventResult(events) => events
      case ErrorResult(error, _)          => throw error
      case o => throw new GeneralDALException(s"Unexpected DSI result type ${o.getClass.getName}")
    }
  }

  @node @scenarioIndependent
  override def enumerateKeysWithRtt[E <: Entity](
      indexInfo: IndexInfo[E, _],
      qt: QueryTemporality,
      rtt: Instant): Iterable[SortedPropertyValues] = {
    require(indexInfo.unique && !indexInfo.indexed)
    val when = toDSITemporality(qt, rtt)
    val cmd = {
      val typeName = indexInfo.storableClass.getName
      val sortedPropNames = indexInfo.propertyNames.sorted
      EnumerateKeys(typeName, sortedPropNames, when)
    }

    val dsiResult = executor.executeQuery(dsi, cmd)

    mapResult(dsiResult) { case EnumerateKeysResult(keys) =>
      keys map { _.properties }
    } getOrElse Nil
  }

  @node @scenarioIndependent
  def enumerateKeysAt[E <: Entity](
      indexInfo: IndexInfo[E, _],
      qt: QueryTemporality.At): Iterable[SortedPropertyValues] = {
    enumerateKeysWithRtt(indexInfo, qt, qt.txTime)
  }

  @node @scenarioIndependent
  def enumerateKeysAtTc[E <: Entity: ClassTag](
      indexInfo: IndexInfo[E, _],
      tc: TemporalContext): Iterable[SortedPropertyValues] = {
    val dsiQt: DSIQueryTemporality.At = QueryTemporalityFinder.findQueryTemporality(
      tc,
      QueryByClass(dataAccess, implicitly[ClassTag[E]].runtimeClass.asInstanceOf[Class[E]]))
    val qt = QueryTemporality.At(dsiQt.validTime, dsiQt.txTime)
    enumerateKeysWithRtt(indexInfo, qt, qt.txTime)
  }

  @node
  def enumerateKeys[E <: Entity](indexInfo: IndexInfo[E, _], qt: QueryTemporality): Iterable[SortedPropertyValues] = {
    val rtt = findTransactionTime(QueryByClass(dataAccess, indexInfo.storableClass))
    enumerateKeysWithRtt(indexInfo, qt, rtt)
  }

  @node @scenarioIndependent
  private[optimus] def getByKeyWithTemporalityWithRtt[E <: Entity](
      key: Key[E],
      qt: QueryTemporality,
      rtt: Instant,
      entitledOnly: Boolean): Iterable[PersistentEntity] = {
    val when = toDSITemporality(qt, rtt)
    val cmd = Select(Query(key).copy(entitledOnly = entitledOnly), when)

    val dsiResult = executor.executeQuery(dsi, cmd)

    mapResult(dsiResult) { case SelectResult(value) =>
      value
    } getOrElse Nil
  }

  @node
  def getByKeyWithTemporality[E <: Entity](
      key: Key[E],
      qt: QueryTemporality,
      entitledOnly: Boolean = false): Iterable[PersistentEntity] = {
    val temporalQuery = key match {
      case u: UniqueKey[E] => QueryByUniqueKey(dataAccess, u, entitledOnly)
      case nu: NonUniqueKey[E] =>
        QueryByNonUniqueKey(dataAccess, nu.toSerializedKey, Set.empty[EntityReference], entitledOnly)
    }
    val rtt = findTransactionTime(temporalQuery)
    getByKeyWithTemporalityWithRtt(key, qt, rtt, entitledOnly)
  }

  @node @scenarioIndependent
  def getByKeyWithTemporalityAt[E <: Entity](
      key: Key[E],
      qt: QueryTemporality.At,
      entitledOnly: Boolean = false): Iterable[PersistentEntity] = {
    getByKeyWithTemporalityWithRtt(key, qt, qt.txTime, entitledOnly)
  }

  @node @scenarioIndependent private[optimus] def getSpaceByRefWithTemporality(
      ref: EntityReference,
      qt: QueryTemporality,
      rtt: Instant): Iterable[SelectSpaceResult.Rectangle] = {
    getSpaceImpl(ReferenceQuery(ref), qt, rtt)
  }

  @node @scenarioIndependent private[optimus] def getSpaceByRefWithClassNameTemporality(
      ref: EntityReference,
      className: String,
      qt: QueryTemporality,
      rtt: Instant): Iterable[SelectSpaceResult.Rectangle] = {
    getSpaceImpl(ReferenceQuery(ref, className), qt, rtt)
  }

  @node @scenarioIndependent private[optimus] def getSpaceByKeyWithTemporality(
      key: SerializedKey,
      qt: QueryTemporality,
      rtt: Instant): Iterable[SelectSpaceResult.Rectangle] = {
    getSpaceImpl(SerializedKeyQuery(key), qt, rtt)
  }

  @node @scenarioIndependent private[optimus] def getSpaceByClassAppIdUserIdInRange(
      fromTemporalContext: TemporalContext,
      toTemporalContext: TemporalContext,
      typeName: String,
      rangeQueryOpts: RangeQueryOptions): Iterable[SelectSpaceResult.Rectangle] = {
    if (
      (rangeQueryOpts.inRange && dsi.serverFeatures().supports(Feature.RangeVersionQuery)) ||
      dsi.serverFeatures().supports(Feature.RangeQuery)
    ) {
      val query = EntityClassAppIdUserIdQuery(typeName, rangeQueryOpts.appId, rangeQueryOpts.userId)
      val (vf, vt, tf, tt) = (
        fromTemporalContext.unsafeValidTime,
        toTemporalContext.unsafeValidTime,
        fromTemporalContext.unsafeTxTime,
        toTemporalContext.unsafeTxTime)
      val qt = DSIQueryTemporality.BitempRange(TimeInterval(vf, vt), TimeInterval(tf, tt), rangeQueryOpts.inRange)
      getSpaceImplWithDSITemporality(query, qt, tt)
    } else {
      throw new UnsupportedOperationException("Server doesn't support RangeQuery feature")
    }
  }

  @node @scenarioIndependent private def getSpaceImpl(
      query: Query,
      qt: QueryTemporality,
      rtt: Instant): Iterable[SelectSpaceResult.Rectangle] = {
    val when = toDSITemporality(qt, rtt)
    getSpaceImplWithDSITemporality(query, when, rtt)
  }

  @node @scenarioIndependent private def getSpaceImplWithDSITemporality(
      query: Query,
      qt: DSIQueryTemporality,
      rtt: Instant): Iterable[SelectSpaceResult.Rectangle] = {
    val cmd = SelectSpace(query, qt)

    val dsiResult = executor.executeQuery(dsi, cmd)

    mapResult(dsiResult) { case SelectSpaceResult(value) =>
      value
    } getOrElse Nil
  }

  @node @scenarioIndependent
  def getByRefWithTemporalityWithRtt(
      entityRef: EntityReference,
      qt: QueryTemporality,
      rtt: Instant,
      entitledOnly: Boolean = false): Iterable[PersistentEntity] = {
    val when = toDSITemporality(qt, rtt)
    val cmd = Select(ReferenceQuery(entityRef, entitledOnly, None), when)

    val dsiResult = executor.executeQuery(dsi, cmd)

    mapResult(dsiResult) { case SelectResult(value) =>
      value
    } getOrElse Nil
  }

  @node
  private[optimus] def getByRefWithTemporality(
      entityRef: EntityReference,
      qt: QueryTemporality): Iterable[PersistentEntity] = {
    val rtt = findTransactionTime(QueryByEntityReference(dataAccess, EntityReferenceQuery(entityRef)))
    getByRefWithTemporalityWithRtt(entityRef, qt, rtt)
  }

  @node @scenarioIndependent
  def getByRefWithTemporalityAt(
      entityRef: EntityReference,
      qt: QueryTemporality.At,
      entitledOnly: Boolean = false): Iterable[PersistentEntity] = {
    getByRefWithTemporalityWithRtt(entityRef, qt, qt.txTime, entitledOnly)
  }

  @node @scenarioIndependent
  private[optimus] def getByCmReferenceWithTemporalityAt[S <: Entity](
      clazz: Class[S],
      cmid: CmReference,
      qt: QueryTemporality.At): Iterable[PersistentEntity] = {
    val when = toDSITemporality(qt, qt.txTime)
    val cmd = Select(EntityCmReferenceQuery(cmid, clazz.getName), when)

    val dsiResult = executor.executeQuery(dsi, cmd)

    mapResult(dsiResult) { case SelectResult(value) =>
      value
    } getOrElse Nil
  }

  @node @scenarioIndependent
  def getBusinessEvent(ref: BusinessEventReference, tc: TransactionTimeContext): Option[BusinessEvent] = {
    // BusinessEventReference doesn't have a class name, but in this case we should have a FixedTransactionTimeContext,
    // which can make a call to unsafeTxTime (if this isn't true we throw)
    val tt = resolveEventLoadTT(tc)
    val cmd = GetBusinessEvent(ref, tt)

    val dsiResult = executor.executeQuery(dsi, cmd)
    asyncMapResult(dsiResult) { result: Result => GetBusinessEventResult.unapply(result).isDefined } {
      asNode { result: Result =>
        val GetBusinessEventResult(serializedBusinessEvents) = result
        assert(serializedBusinessEvents.size <= 1)
        if (serializedBusinessEvents.isEmpty) None
        else Some(EventSerializer.deserializeBusinessEvent(tc)(serializedBusinessEvents.head))
      }
    } getOrElse None
  }

  @node @scenarioIndependent
  def getBusinessEventByCmReference[S <: BusinessEvent](
      clazz: Class[S],
      cmid: CmReference,
      tc: TemporalContext): Option[BusinessEvent] = {
    val tt = resolveEventLoadTT(tc, clazz.getName)
    val cmd = Select(EventCmReferenceQuery(cmid, clazz.getName), DSIQueryTemporality.TxTime(tt))

    val dsiResult = executor.executeQuery(dsi, cmd)

    mapResult(dsiResult) { case GetBusinessEventResult(evs) =>
      require(
        evs.isEmpty || evs.size == 1,
        s"Expected 0 or 1 events but got back ${evs.size}: ${evs.map(_.id).mkString(";")}")
      if (evs.isEmpty) None
      else Some(EventSerializer.deserializeBusinessEvent(tc)(evs.head))
    } getOrElse None
  }

  @node @scenarioIndependent
  private[dal] def getInitiatingEvent(
      ref: EntityReference,
      cls: Class[_],
      vt: Instant,
      tc: HasTTContext): Option[BusinessEvent] = {
    val tt = resolveEventLoadTT(tc, cls.getName)
    val cmd = GetInitiatingEvent(ref, vt, tt)

    val dsiResult = executor.executeQuery(dsi, cmd)

    mapResult(dsiResult) {
      case GetInitiatingEventResult(Some(event), _) => Some(EventSerializer.deserializeBusinessEvent(tc)(event))
      case _                                        => None
    } getOrElse None
  }

  @node @scenarioIndependent
  def getEntityEventTimeline(
      ref: EntityReference,
      cls: Class[_],
      tc: HasTTContext): Seq[(BusinessEvent, Option[PersistentEntity])] = {
    val tt = resolveEventLoadTT(tc, cls.getName)
    val cmd = GetEntityEventTimeline(ref, tt)

    val dsiResult = executor.executeQuery(dsi, cmd)

    mapResult(dsiResult) { case GetEntityEventTimelineResult(comb) =>
      comb map { k =>
        (EventSerializer.deserializeBusinessEvent(tc)(k._1), k._2)
      } toSeq
    } getOrElse Nil
  }

  @node @scenarioIndependent
  def getEntityEventValidTimeline(
      ref: EntityReference,
      tt: Instant,
      vtRange: Option[ValidTimeInterval]): Seq[(Option[SerializedBusinessEventWithTTTo], Instant)] = {
    val cmd = GetEntityEventValidTimeline(ref, tt, vtRange)

    val dsiResult = executor.executeQuery(dsi, cmd)

    mapResult(dsiResult) { case GetEntityEventValidTimelineLazyLoadResult(comb) =>
      comb
    } getOrElse Nil
  }

  @node @scenarioIndependent
  def getEventTimeline(eref: BusinessEventReference, tt: Instant): Seq[SerializedBusinessEventWithTTTo] = {
    val cmd = GetEventTimeline(eref, tt)

    val dsiResult = executor.executeQuery(dsi, cmd)

    mapResult(dsiResult) { case GetBusinessEventWithTTToResult(events) =>
      events.toSeq
    } getOrElse Nil
  }

  @node @scenarioIndependent // XXX Revisit semantics of TemporalContext: we're explicitly overriding the vt,
  // so we're getting the tt from the event, not any temporal surfaces related ot the entities themselves.
  def getAssociatedEntities(
      ref: BusinessEventReference,
      cls: Class[_],
      vt: Instant,
      tc: TransactionTimeContext,
      typeName: Option[String] = None): Set[Entity] = {
    val tt = resolveEventLoadTT(tc, cls.getName)
    if (typeName.isDefined)
      require(
        dsi.serverFeatures().supports(Feature.EventEntitiesWithType),
        "DAL broker doesn't support event.entitiesOfType")

    val cmd = GetAssociatedEntities(ref, vt, tt, typeName)

    val dsiResult = executor.executeQuery(dsi, cmd)

    mapResult(dsiResult) { case SelectResult(pes) =>
      pes.map(deserializeOne(DALImpl.TemporalContext(vt, tt))) toSet
    } getOrElse Set.empty[Entity]
  }

  @node @scenarioIndependent
  def getEventTransactions(
      ref: BusinessEventReference,
      cls: Class[_],
      tc: TransactionTimeContext): Seq[SelectSpaceResult.Rectangle] = {
    val result = DALImpl.resolver.withFeatureCheck(Feature.GetEventTransactionsApi) {
      asNode(() => { executor.executeQuery(dsi, GetEventTransactions(ref, resolveEventLoadTT(tc, cls.getName))) })
    }
    mapResult(result) { case SelectSpaceResult(value) =>
      value.toSeq
    } getOrElse Nil
  }

  @node @scenarioIndependent
  private[optimus /*platform*/ ] def getAssociatedPersistentEntities(
      ref: BusinessEventReference,
      cls: Class[_],
      vt: Instant,
      tc: TransactionTimeContext): Set[PersistentEntity] = {
    val tt = resolveEventLoadTT(tc, cls.getName)
    val cmd = GetAssociatedEntities(ref, vt, tt)

    val dsiResult = executor.executeQuery(dsi, cmd)

    mapResult(dsiResult) { case SelectResult(pes) =>
      pes toSet
    } getOrElse Set.empty[PersistentEntity]
  }

  @node @scenarioIndependent
  def getLinkedEntities[E <: Entity: ClassTag](
      lookupSrc: Entity,
      propName: String,
      tc: TemporalContext): Iterable[E] = {
    val eRef = lookupSrc.dal$entityRef
    require(eRef ne null, "Cannot get linked entities for a heap entity. lookupSrc needs to be a stored entity")
    val clazz = implicitly[ClassTag[E]].runtimeClass.asInstanceOf[Class[E]]

    // HACK: toSet, need to keep the result collection during the dataAccess transform result into Map
    @nowarn("msg=10500 optimus.core.CoreAPI.nodeResult")
    val res = CoreAPI.nodeResult {
      tc.dataAccess(QueryByLinkage[E](dataAccess, clazz, eRef, propName)).toSet
    }

    wrapQueryDSIException {
      res.value
    }
  }

  @node
  def getLinkedEntities2[E <: Entity: ClassTag](lookupSrc: Entity, propName: String): Iterable[E] = {
    getLinkedEntities(lookupSrc, propName, TemporalSource.loadContext)
  }

  /**
   * Tactical public API to provide information needed for reconciliation in a tt range.
   *
   * @param query
   *   name-space filter in terms of class name or package prefix
   * @param ttRange
   *   the range of transaction time
   * @return
   *   the affected persistent entities by events during the designated tt range
   *
   * Note: this is only a short-term tactical API for reconciling between object store and relational store in DSL
   * project. And it will soon be replaced by a proper one. Please don't use it otherwise.
   */
  @node @scenarioIndependent
  def findAffectedPersistentEntitesInRange(
      query: EntityClassQuery,
      ttRange: TimeInterval): Iterable[PersistentEntity] = {
    assert(ttRange ne null)
    assert(ttRange.from ne null)
    assert(ttRange.to ne null)

    val cmd = Select(query, DSIQueryTemporality.TxRange(ttRange))

    val dsiResult = executor.executeQuery(dsi, cmd)

    mapResult(dsiResult) { case SelectResult(pes) =>
      pes
    } getOrElse Nil
  }

  /*
   * Optimus internal query APIs that return PersistentEntities instead of deserialized Entities.
   * These interfaces are currently used for replication and the JavaScript API (for DAL - JSServer communication)
   */

  /*
   * queries PersistentEntities for a transaction time interval
   * return:
   *   - if the query succeeds, returns a Map from entity reference to a sequence of persistent entities sorted in increasing ttInterval.from time stamps
   *     wrapped in the Success interface
   *   - if the query fails, returns the error wrapped in the sacal.util.Failure interface
   * Note: does not throw for NonFatal exceptions.
   *
   * This range query is currently not exposed as an public API and only the 'raw' version is provided. It is used for replication.
   * Currently, only EntityClassQueries are supported.
   */
  @node @scenarioIndependent
  private[optimus] def rangeQuery(
      query: Query,
      ttRange: TimeInterval): Try[Map[EntityReference, Seq[PersistentEntity]]] = {
    assert(ttRange ne null)
    assert(ttRange.from ne null)
    assert(ttRange.to ne null)

    val cmd = Select(query, DSIQueryTemporality.TxRange(ttRange))

    val dsiResult = executor.executeQuery(dsi, cmd)

    mapTryResult(dsiResult) { case SelectResult(pes) =>
      Success(pes.groupBy(_.entityRef).mapValuesNow { s =>
        s.toSeq.sortBy(x => x.txInterval.from)
      })
    }
  }

  /*
   * queries the DAL using a optimus.platform.dsi.bitemporal.Query, i.e. either using a
   * className (EntityClassQuery), a serialized key (SerializedKeyQuery), or a entity reference (ReferenceQuery)
   *
   * returns:
   *   - Success[Iterable[PersistentEntity]] in case the query executes successfully (empty iterable if the query doesn't match any entities)
   *   - Failure[Exception]
   */
  @node @scenarioIndependent
  private[optimus] def findPersistentEntitiesWithRtt(
      query: Query,
      qt: QueryTemporality,
      rtt: Instant): Try[Iterable[PersistentEntity]] = {
    val when = toDSITemporality(qt, rtt)
    val cmd = Select(query, when)

    val dsiResult = executor.executeQuery(dsi, cmd)

    mapTryResult(dsiResult) { case SelectResult(pes) =>
      Success(pes)
    }
  }

  @node
  private[optimus] def findPersistentEntities(query: Query, qt: QueryTemporality): Try[Iterable[PersistentEntity]] = {
    val temporalQuery = query match {
      case SerializedKeyQuery(key, _, false) =>
        if (key.unique) QueryByUniqueKey(dataAccess, key, entitledOnly = false)
        else QueryByNonUniqueKey(dataAccess, key, Set.empty, entitledOnly = false)
      case EntityClassQuery(clazz, entitledOnly) =>
        QueryByClass(dataAccess, Class.forName(clazz).asInstanceOf[Class[Entity]], entitledOnly)
      case ReferenceQuery(ref, _, _) => QueryPersistentEntityByEntityReference(dataAccess, ref)
      case other                     => throw new Exception(s"Not able to create TemporalSurfaceQuery from $other")
    }
    val rtt = findTransactionTime(temporalQuery)
    findPersistentEntitiesWithRtt(query, qt, rtt)
  }

  @node
  private[optimus] def findPersistentEntitiesAt(
      query: Query,
      qt: QueryTemporality.At): Try[Iterable[PersistentEntity]] = {
    findPersistentEntitiesWithRtt(query, qt, qt.txTime)
  }

  @node @scenarioIndependent
  private[optimus] def findEntitiesAuditInfoWithRtt(
      entityRef: EntityReference,
      className: String,
      qt: QueryTemporality,
      rtt: Instant): Try[Iterable[EntityAuditInfo]] = {
    val when = toDSITemporality(qt, rtt)
    val cmd = GetAuditInfo(entityRef, className, when)

    val dsiResult = executor.executeQuery(dsi, cmd)

    mapTryResult(dsiResult) { case GetAuditInfoResult(r) =>
      Success(r)
    }
  }

  @node @scenarioIndependent
  private[optimus] def findEntitiesAuditInfo(
      entityRef: EntityReference,
      className: String,
      qt: QueryTemporality.At): Try[Option[EntityAuditInfo]] = {
    findEntitiesAuditInfoWithRtt(entityRef, className, qt, qt.txTime).flatMap(_.toList match {
      case auditInfo :: Nil => Success(Some(auditInfo))
      case Nil              => Success(None)
      case _ =>
        Failure(
          new IllegalStateException(s"Too many elements in the result " +
            s"- eref: ${entityRef}, class: ${className}, vt: ${qt.validTime}, tt: ${qt.txTime}"))
    })
  }

  @node @scenarioIndependent
  private[optimus] def findEntitiesAuditInfoAt(
      entityRef: EntityReference,
      className: String,
      qt: QueryTemporality.At): Try[Iterable[EntityAuditInfo]] = {
    findEntitiesAuditInfoWithRtt(entityRef, className, qt, qt.txTime)
  }

  private[optimus] def findEntitiesNotAtSlot(
      query: Query,
      slot: Int,
      className: String,
      rtt: Instant): Try[Iterable[EntityReference]] = {
    val when = toDSITemporality(QueryTemporality.All, rtt)
    val cmd = GetRefsNotAtSlot(query, className, slot, when)

    val dsiResult = executor.executeQuery(dsi, cmd)

    mapTryResult(dsiResult) { case GetRefsNotAtSlotResult(refs) =>
      Success(refs.map(sr => EntityReference(sr.data)))
    }
  }

  private[this] def mapTryResult[A](r: Result)(pf: PartialFunction[Result, Try[A]]): Try[A] = r match {
    case ErrorResult(err, _)    => Failure(err)
    case _ if pf.isDefinedAt(r) => pf(r)
    case o                      => Failure(new GeneralDALException(s"Unexpected DSI result type ${o.getClass.getName}"))
  }

  private[optimus] def deserializeOne(tc: TemporalContext)(e: PersistentEntity): Entity = {
    val storageInfo = DSIStorageInfo.fromPersistentEntity(e)
    val entity = tc.deserialize(e, storageInfo)
    entity
  }

  private def resolveEventLoadTT(tc: HasTTContext) = tc.ttContext.unsafeTxTime
  private def resolveEventLoadTT(tc: HasTTContext, eventClassName: String) = tc.ttContext.getTTForEvent(eventClassName)

  @nowarn("msg=10500 optimus.core.CoreAPI.nodeResult")
  @node private def findTransactionTime(tq: TemporalSurfaceQuery): Instant = {
    CoreAPI.nodeResult(DALImpl.transactionTime) match {
      case NodeSuccess(res) =>
        res.asInstanceOf[Instant]
      case NodeFailure(exp) =>
        val lc = DALImpl.loadContext
        QueryTemporalityFinder.findQueryTemporality(lc, tq).readTxTime
    }
  }

  @node @scenarioIndependent
  private def findValidTime(tq: TemporalSurfaceQuery, temporalContext: TemporalContext): Instant =
    QueryTemporalityFinder.findQueryTemporality(temporalContext, tq).validTime

  @node @scenarioIndependent
  private def findTxTime(tq: TemporalSurfaceQuery, temporalContext: TemporalContext): Instant =
    QueryTemporalityFinder.findQueryTemporality(temporalContext, tq).txTime

  @node @scenarioIndependent
  private[dal] def canPerformAction[E <: Entity](action: EntityAction)(implicit m: ClassTag[E]): Boolean = {
    canPerformActionOnEntity(m.runtimeClass.getName, action)
  }

  @node @scenarioIndependent
  private[dal] def canPerformActionOnEntity(entityName: String, action: EntityAction): Boolean = {
    withFeatureCheck(Feature.DalActionCheck) {
      asNode { () =>
        val result = executor.executeQuery(dsi, CanPerformAction(entityName, action))
        result match {
          case BooleanResult(value) => value
          case ErrorResult(ex, _)   => throw ex
          case o                    => throw new GeneralDALException(s"Unexpected result type: $o")
        }
      }
    }
  }

  @node @scenarioIndependent private[optimus /*platform*/ ] def withFeatureCheck[T, F <: Feature](feature: F)(
      block: NodeFunction0[T]): T = {
    if (dsi.serverFeatures().supports(feature)) {
      block.apply()
    } else {
      throw new UnsupportedOperationException(s"Server doesn't support ${feature.getClass.getSimpleName} feature")
    }
  }

  /**
   * This method is ScenarioIndependent since DSIQueryTemporality is stored in each Entity expression.
   */
  @node @scenarioIndependent
  private[optimus] def findByExpressionCommand(command: ExpressionQueryCommand): QueryResultSet

  /**
   * We have this method because we don't want to cache above findByExpressionCommand node, because the result of that
   * node may contain lots of PersistentEntity which consume lots of memory. For this node, we will deserialize the
   * PersistentEntity into Entity, which costs less memory when we cache them
   */
  @node @scenarioIndependent
  private[optimus /*platform*/ ] def findByExpressionCommandWithTemporalContext(
      command: ExpressionQueryCommand,
      loadContext: TemporalContext): QueryResultSet
}

final case class LoadTo(at: DSIQueryTemporality.At, loadContext: TemporalContext)
