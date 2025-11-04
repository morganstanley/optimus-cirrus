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

import java.time.DateTimeException
import java.time.Instant
import msjava.base.util.uuid.MSUuid
import msjava.slf4jutils.scalalog.getLogger
import optimus.graph.DiagnosticSettings
import optimus.graph.PropertyInfo
import optimus.platform._
import optimus.platform.dsi.bitemporal.DateTimeSerialization
import optimus.platform.dsi.bitemporal.WriteCommand
import optimus.platform.pickling.PropertyMapOutputStream.PickleSeq
import optimus.platform.pickling.{AbstractPickledOutputStream, TemporaryEntityException}
import optimus.platform.storable._

import scala.collection.{concurrent, immutable, mutable}

/**
 * Trait representing things that can create persist blocks.
 */
trait PersistBlockComponent[D] { resolver: EntityResolverWriteImpl =>
  protected[optimus /*platform*/ ] def createPersistBlock: PluginTagKeyValue[BasicPersistBlock]
  protected[optimus /*platform*/ ] def createScenario(tweaks: Iterable[Tweak]): Scenario
}

/**
 * A common superclass for PersistBlock instances, providing basic functionality.
 *
 * The key functionality in this class is a cache (which is really more of a fancy buffer) that stores pending
 * operations, indexed by Entities. It also stores temporary entities in place of real ones, and sometimes has to update
 * those entries with the permanent versions.
 *
 * @param resolver
 *   The DAL resolver to use.
 * @tparam A
 *   The type used to represent events in this PersistBlock.
 */
abstract class AbstractPersistBlock[A](resolver: EntityResolverWriteImpl) extends PersistBlock {
  import AbstractPersistBlock.log

  private def checkVtPrecisionAndLogIfNano(vt: Instant, msg: => String): Unit = {
    if (vt != null) {
      try {
        DateTimeSerialization.fromInstant(vt)
      } catch {
        case ex: DateTimeException if (ex.getMessage.contains("Invalid value for EpochDay")) =>
          throw new GeneralDALException(
            s"valid time: ${vt} is not within supported range ${TimeInterval.NegInfinity} to ${TimeInterval.Infinity}")
      }
      if (vt.getNano % 1000 != 0)
        log.warn(s"DAL broker supports precision upto micro. Nano's will be stripped from vt=${vt}. ${msg}")
    }
  }

  protected def checkVtPrecisionAndLogIfNano(vt: Instant, event: BusinessEvent): Unit = {
    checkVtPrecisionAndLogIfNano(vt, s"Event: ${event}")
  }

  protected def checkVtPrecisionAndLogIfNano(vt: Instant, entity: Entity): Unit = {
    checkVtPrecisionAndLogIfNano(vt, s"Entity: ${entity}")
  }

  protected def checkVtPrecisionAndLogIfNano[E <: Entity](vt: Instant, holder: EntityVersionHolder[E]): Unit = {
    checkVtPrecisionAndLogIfNano(vt, s"Entity with ref: ${holder.bitempSpace.eref}")
  }

  protected def checkVtPrecisionAndLogIfNano[E <: Entity](vt: Instant, holder: EntityReferenceHolder[E]): Unit = {
    checkVtPrecisionAndLogIfNano(vt, s"Entity with ref: ${holder.ref}")
  }

  private[this] final def throwConflictException(prior: Op, given: Op): Nothing = {
    val msg =
      s"Cannot ${given.opString} ${given.entity} at ${given.vt} due to conflicting request to ${prior.opString} ${prior.entity} at ${prior.vt}."
    throw new DALTransactionConflictException(given.entity, msg)
  }

  val entityMutationAllowed: Boolean =
    !DiagnosticSettings.getBoolProperty(AbstractPersistBlock.UseImmutableEntitiesProperty, false)

  val generateIgnoreListForReferenceResolutionEnabled: Boolean =
    DiagnosticSettings.getBoolProperty(AbstractPersistBlock.generateIgnoreListForReferenceResolution, true)
  val writeRetryAllowed = false

  private[this] var deferredValidations: List[() => Unit] = Nil
  private[this] def deferredValidate(f: => Unit) = {
    deferredValidations ::= (() => f)
  }

  protected def storeValidTimeOf(a: A): Instant

  @async def commit(
      asserts: Set[(EntityReference, A)],
      cache: Iterable[PersistCacheEntry],
      ignoreListForReferenceResolution: Set[EntityReference]): PersistResult

  def createCommandsInternal(
      asserts: Set[(EntityReference, A)],
      cache: Iterable[PersistCacheEntry],
      ignoreListForReferenceResolution: Set[EntityReference]): Seq[WriteCommand]

  sealed trait CacheKey
  case class EntityKey(entity: Entity) extends CacheKey
  case class ReferenceKey(ref: EntityReference) extends CacheKey

  /**
   * A representation of a pending operation. These are used in the cache.
   */
  sealed trait Op {
    def vt: A
    def opString: String
    def entity: Entity
  }

  /**
   * An assert operation.
   *
   * @param entity
   *   The entity to check.
   * @param vt
   *   The event at which this assert took place.
   * @param propertyInfo
   *   The property to check.
   */
  case class AssertEntry(entity: Entity, vt: A, propertyInfo: PropertyInfo[_]) extends Op {
    def opString = s"persist link from val ${propertyInfo.name} to entity"
  }

  sealed trait PersistCacheEntry extends Op
  sealed trait PersistEntry extends PersistCacheEntry
  object PersistEntry {
    def apply(entity: Entity, vt: A, upsert: Boolean, cmid: Option[MSUuid]) = {
      if (upsert) UpsertEntry(entity, vt, cmid)
      else PutEntry(entity, vt, cmid)
    }
    def unapply(entry: PersistEntry): Some[(Entity, A, Boolean, Option[MSUuid])] = entry match {
      case PutEntry(entity, vt, cmid)    => Some((entity, vt, false, cmid))
      case UpsertEntry(entity, vt, cmid) => Some((entity, vt, true, cmid))
    }
  }

  /**
   * Represents a put operation in the cache.
   *
   * @param entity
   *   Entity to insert.
   * @param vt
   *   Time event at which to insert it.
   * @param cmid
   *   The ID at which to insert it.
   */
  case class PutEntry(entity: Entity, vt: A, cmid: Option[MSUuid]) extends PersistEntry {
    def opString = "put"
  }

  /**
   * Represents an upsert operation in the cache.
   *
   * @param entity
   *   Entity to upsert.
   * @param vt
   *   Time event at which to upsert it.
   * @param cmid
   *   The ID at which to upsert it.
   */
  case class UpsertEntry(entity: Entity, vt: A, cmid: Option[MSUuid]) extends PersistEntry {
    def opString = "upsert"
  }

  /**
   * Represents a cache invalidation in the cache.
   *
   * @param entity
   *   Entity to invalidate.
   * @param vt
   *   Time event at which to do the invalidation.
   */
  case class InvalidateEntry(entity: Entity, vt: A) extends PersistCacheEntry {
    def opString = "invalidate"
  }

  case class InvalidateEntryByRef(ref: EntityReference, vt: A, className: String) extends PersistCacheEntry {
    override def entity: Entity =
      throw new UnsupportedOperationException(s"InvalidateEntryByRef not expected to dereference $ref of $className")
    override def opString = "invalidateByRef"
  }

  /**
   * Represents a reversion in the cache.
   *
   * @param entity
   *   Entity to revert.
   * @param vt
   *   Time event at which to do the reversion.
   */
  case class RevertEntry(entity: Entity, vt: A) extends PersistCacheEntry {
    def opString = "revert"
  }

  /**
   * Check if an entity is transient, and throw an exception if it is. This is done by calling isStorable on its
   * EntityInfo object.
   *
   * @param e
   *   Entity to check.
   * @throws PersistTransientEntityException
   *   If e is transient.
   */
  private[this] def checkTransient(e: Entity): Unit = {
    if (!e.$info.isStorable)
      throw new PersistTransientEntityException(e)
  }

  /**
   * Map from CacheKeys that identify entities to their pending operations.
   */
  protected[this] val cache = concurrent.TrieMap.empty[CacheKey, List[PersistCacheEntry]]
  protected[this] val entityRefs = concurrent.TrieMap.empty[Entity, EntityReference]
  protected[this] val lockTokens = concurrent.TrieMap.empty[Entity, Long]
  protected[this] val events = concurrent.TrieMap.empty[BusinessEvent, BusinessEventBlock]
  protected var minAssignableTtOpt: Option[Instant] = None

  case class PersistBlockSavedState(
      cache: Map[CacheKey, List[PersistCacheEntry]],
      entityRefs: Map[Entity, EntityReference],
      lockTokens: Map[Entity, Long],
      events: Map[BusinessEvent, BusinessEventBlock])
  private var savedState: Option[PersistBlockSavedState] = None

  protected[this] def isStateSaved: Boolean = {
    // set to null once transaction is disposed - see optimus.platform.dal.AbstractPersistBlock.dispose()
    // added an explicit require with proper message - otherwise would hit general NPE on savedState access
    require(savedState != null, "persistBlock is already in disposed state - thus can't reexecute, etc")
    savedState.isDefined
  }

  protected[this] def saveState(): Unit = {
    require(savedState.isEmpty, "You may only call this method once on a persist block")
    savedState = Some(
      PersistBlockSavedState(Map.empty ++ cache, Map.empty ++ entityRefs, Map.empty ++ lockTokens, Map.empty ++ events))
  }

  protected[this] def restoreState(): Unit = {
    require(savedState.isDefined, "Nothing to restore.")
    cache.clear()
    entityRefs.clear()
    lockTokens.clear()
    events.clear()
    cache ++= savedState.get.cache
    entityRefs ++= savedState.get.entityRefs
    lockTokens ++= savedState.get.lockTokens
    events ++= savedState.get.events
  }

  protected[this] def cacheValues = cache.values

  protected[this] def getEntityRef(entity: Entity) =
    if (entity.dal$entityRef ne null) entity.dal$entityRef else entityRefs.get(entity).getOrElse(null)

  private[this] def keyOf(entity: Entity): CacheKey = getEntityRef(entity) match {
    case er: EntityReference => ReferenceKey(er)
    case null                => EntityKey(entity)
  }

  /**
   * Manipulate the cache entry for a given entity. If no entry exists, then the given entry will be added. If an entry
   * already exists, then it will be upgraded if possible, or else a conflict exception will be thrown. The cache key
   * will be obtained from the entity.
   *
   * @param entity
   *   The entity (ignored).
   * @param newOp
   *   The new operation entry.
   * @return
   *   Whether or not an entry already existed.
   */
  private def putOrUpgradeIfCompatible(entity: Entity, newOp: PersistCacheEntry): Boolean = {
    val key = keyOf(entity)
    putOrUpgradeIfCompatible(key, newOp)
  }

  private def putOrUpgradeIfCompatible(eref: EntityReference, newOp: InvalidateEntryByRef): Boolean = {
    putOrUpgradeIfCompatible(ReferenceKey(eref), newOp)
  }

  /**
   * Manipulate the cache entry for a given entity. If no entry exists, then the given entry will be added. If an entry
   * already exists, then it will be upgraded if possible, or else a conflict exception will be thrown.
   *
   * @param key
   *   The CacheKey to use.
   * @param newOp
   *   The new operation entry.
   * @return
   *   Whether or not an entry already existed.
   */
  private def putOrUpgradeIfCompatible(key: CacheKey, newOp: PersistCacheEntry): Boolean = {
    var finished: Boolean = false
    var exists: Boolean = false
    while (!finished) {
      cache.putIfAbsent(key, newOp :: Nil) match {
        case None =>
          finished = true
          exists = false
        case Some(oldOps) =>
          exists = oldOps exists { oldOp =>
            if (oldOp == newOp)
              true
            else if ((oldOp.entity != newOp.entity || oldOp.getClass != newOp.getClass) && oldOp.vt == newOp.vt) {
              throwConflictException(oldOp, newOp)
              false
            } else
              false
          }
          if (!exists)
            finished = cache.replace(key, oldOps, newOp :: oldOps)
          else
            finished = true
      }
    }
    !exists
  }

  /**
   * Add a cache entry to persist an entity to the DAL, checking that it's a @stored @entity class (not module) first.
   *
   * @param entity
   *   The entity to add.
   * @param vt
   *   Time event at which to add the entity.
   * @param upsert
   *   Whether to upsert.
   * @param cmid
   *   Command ID to use.
   * @tparam T
   *   Entity type.
   * @return
   *   The inserted entity.
   */
  def addEntity[T <: Entity](entity: T, vt: A, upsert: Boolean, cmid: Option[MSUuid]): T = {
    checkTransient(entity)
    if (entity.$isModule)
      throw new PersistModuleEntityException(entity)
    putOrUpgradeIfCompatible(entity, PersistEntry(entity, vt, upsert, cmid))
    entity
  }

  /**
   * Add an invalidation entry to the cache for an entity.
   *
   * @param entity
   *   Entity for which to add an invalidation entry.
   * @param vt
   *   Time event at which to invalidate.
   */
  def addInvalidate(entity: Entity, vt: A): Unit = {
    putOrUpgradeIfCompatible(entity, InvalidateEntry(entity, vt))
  }

  def addInvalidate(eref: EntityReference, vt: A, className: String): Unit = {
    putOrUpgradeIfCompatible(eref, InvalidateEntryByRef(eref, vt, className))
  }

  /**
   * Add a revert entry to the cache for an entity.
   *
   * @param entity
   *   Entity for which to add a revert entry.
   * @param vt
   *   Time event at which to revert.
   */
  def addRevert(entity: Entity, vt: A): Unit = {
    putOrUpgradeIfCompatible(entity, RevertEntry(entity, vt))
  }

  private[this] def equalKeys(l: Entity, r: Entity): Boolean = {
    assert(
      l.$info eq r.$info,
      s"Can't compare keys on entities of different classes (${l.$info.runtimeClass.getName},${r.$info.runtimeClass.getName})")
    val keys = l.allKeyInfos[Entity]
    keys
      .filter { k =>
        k.unique && !k.indexed
      }
      .forall(k => k.entityToSerializedKey(l, entityRefs) == k.entityToSerializedKey(r, entityRefs))
  }

  /**
   * Replace a temporary entity to point to another non-temporary entity.
   *
   * @param existing
   *   The non-temporary entity to replace updated.
   * @param updated
   *   The temporary entity to be updated.
   * @param vt
   *   Time event at which to do the replacement.
   * @tparam E
   *   The Entity type.
   */
  def replaceEntity[E <: Entity](
      existing: E,
      updated: E,
      vt: A
  ): Unit = {
    require(updated.dal$isTemporary, "[Replace] Updated entity state must be temporary/heap, non-persisted")
    require(!existing.dal$isTemporary, "[Replace] Existing entity must be non-temporary/non-heap, persisted")

    deferredValidate {
      if (!equalKeys(existing, updated))
        throw new ReplaceKeyConflictException(existing, updated)
    }

    entityRefs += updated -> existing.dal$entityRef
    existing.dal$storageInfo.lockToken.foreach(token => lockTokens += updated -> token)

    addEntity(updated, vt, false, None)
  }

  /**
   * Creates asserts and upgrades temporary entities, then commits the asserts using commit.
   *
   * @return
   *   The result of committing the created asserts.
   */
  @async final def flush(): PersistResult = {
    val (asserts, ignoreListForReferenceResolution) = prepareFlush()
    val r = commit(asserts, cache.values.flatten, ignoreListForReferenceResolution)
    completeFlush()
    r
  }

  /**
   * Create asserts and upgrade temporary entities.
   *
   * @return
   *   The created asserts.
   */
  final def prepareFlush(): (Set[(EntityReference, A)], Set[EntityReference]) = {
    val (asserts, ignoreListForReferenceResolution, needsResolution) = closeAndCreateAsserts()
    // Assigns references to temporary entities and replaces their 'cache' entries with reference entries.
    // This will throw if a temporary entity resolves to a reference that's already in the transaction with a conflicting operation.
    upgradeTemporaryEntities()
    val ignoredRefs = ignoreListForReferenceResolution.map { ent =>
      Option(ent.dal$entityRef).getOrElse(entityRefs(ent))
    }.toSet
    val unIgnoredRefs = needsResolution.map { ent => Option(ent.dal$entityRef).getOrElse(entityRefs(ent)) }.toSet
    log.debug(s"Ignored Refs - ${ignoredRefs.mkString(",")}")
    (asserts, ignoredRefs -- unIgnoredRefs)
  }

  /**
   * Currently does nothing.
   */
  def completeFlush() = {}

  /**
   * Resets the cache, entityRefs and lockTokens and delete savedState.
   */
  private[optimus /*dal*/ ] override def dispose() = {
    // needed as Persist blocks are Scenarios and reachable from ScenarioStacks, which are referencable from any nodes that may be cached
    // e.g. as a parent scenario.
    cache.clear()
    entityRefs.clear()
    lockTokens.clear()
    // intentionally using null here rather than None as None indicates that nothing is yet saved
    // null should cause a failure if accessed after disposal
    savedState = null

  }

  // TODO (OPTIMUS-11591): Only used by tests. Delete?
  /**
   * This creates asserts as per prepareFlush (which also upgrades temporary entities), but does not commit them.
   * Instead, it filters them through createCommandsInternal, then clears the cache.
   *
   * @return
   *   Result of filtering created asserts through createCommandsInternal.
   */
  final def createCommands(): Seq[WriteCommand] = {
    val (asserts, ignoreListForReferenceResolution) = prepareFlush()
    val ret = createCommandsInternal(asserts, cache.values.flatten, ignoreListForReferenceResolution)

    // Probably unneeded, we don't reuse PersistBlocks afaik so these will be eligible for GC immediately anyway.
    cache.clear()
    ret
  }

  /**
   * Assign references to temporary entities and replaces their 'cache' entries with reference entries. This will throw
   * if a temporary entity resolves to a reference that's already in the transaction with a conflicting operation.
   *
   * @return
   *   A map containing just the replaced temporaries.
   */
  private[this] def upgradeTemporaryEntities(): Map[Entity, EntityReference] = {
    val temporaries: Map[Entity, (Boolean, Option[MSUuid])] = cache.iterator.flatMap {
      case (ek: EntityKey, ops) =>
        ops collect { case PersistEntry(_, _, upsert, cmid) =>
          assert(getEntityRef(ek.entity) eq null, "found supposedly temporary entity with valid entity reference")
          (ek.entity, (upsert, cmid))
        }
      case _ => Nil
    }.toMap

    val refMap = resolver.assignTemporaryReferences(temporaries, entityRefs)

    temporaries foreach { case (ek, _) =>
      // Remove existing key from cache, replace with ReferenceKey (may conflict with existing operation and throw)
      val key = EntityKey(ek)
      val ops = cache(key)
      cache.remove(key)
      ops foreach { op =>
        putOrUpgradeIfCompatible(ReferenceKey(refMap(ek)), op)
      }
    }

    entityRefs ++= refMap
    refMap
  }

  /**
   * Run through a snapshot of the cache, pull out all the assertions. The cache will be reconstituted in the process.
   * The assertions will then have all entity references resolved, and will be returned.
   *
   * @return
   *   The AssertEntries extracted from the cache.
   */
  private[this] def closeAndCreateAsserts(): (Set[(EntityReference, A)], Seq[Entity], Seq[Entity]) = {
    // snapshot cache on entry as it will be changing from underneath us
    val ops = cache.toMap

    val assertOps = immutable.HashSet.newBuilder[AssertEntry]

    // Keeping 2 sequences of Entities - One for entities which refer to only FinalTypedReferences and once other.
    // For the multi-vt write case there could be an overlap in these collections - i.e one version points only to
    // FinalTypedReferences and other version doesn't. In such a case, we cannot ignore this entity reference. Hence,
    // we will remove the overlapping ones from ignoreList at the call site of this method.
    val ignoreListForReferenceResolution = immutable.Seq.newBuilder[Entity]
    val needsReferenceResolution = immutable.Seq.newBuilder[Entity]

    class PersistVisitor(vt: A, upsert: Boolean, cmid: Option[MSUuid]) extends AbstractPickledOutputStream {
      private[this] var currentPropertyInfo: PropertyInfo[_] = _
      private[this] var outerMonoTemporal: Boolean = _
      private[this] var containsFinalTypedReferencesOnly: Boolean = true

      private def tryPersist(entity: Entity): Boolean = {
        checkTransient(entity)
        putOrUpgradeIfCompatible(entity, PersistEntry(entity, vt, upsert, cmid))
      }

      def apply(entity: Entity): Unit = {
        tryPersist(entity)
        outerMonoTemporal = entity.$info.monoTemporal
        // Pickle call here is used to build assert operations through writePropertyInfo and writeEntity
        entity.pickle(this)
        if (containsFinalTypedReferencesOnly && generateIgnoreListForReferenceResolutionEnabled) {
          ignoreListForReferenceResolution += entity
        } else {
          needsReferenceResolution += entity
        }
      }

      private def traversePropToFindReferences(data: Any): Unit = if (containsFinalTypedReferencesOnly) {
        data match {
          // if we encounter a non FinalTypedReference being pickled, don't add this into ignoreList.
          case e: FinalTypedReference =>
          case e: FinalReference      => containsFinalTypedReferencesOnly = false
          case e: TemporaryReference  => containsFinalTypedReferencesOnly = false
          // Using forall to exit the iteration on first encounter of non-finalTyped reference.
          case col: PickleSeq[_] @unchecked =>
            col.forall { x =>
              traversePropToFindReferences(x)
              containsFinalTypedReferencesOnly
            }
          case col: collection.Set[_] @unchecked =>
            col.forall { x =>
              traversePropToFindReferences(x)
              containsFinalTypedReferencesOnly
            }
          case m: Map[_, _] @unchecked =>
            m.forall { case (k, v) =>
              traversePropToFindReferences(k)
              traversePropToFindReferences(v)
              containsFinalTypedReferencesOnly
            }
          case x: Any =>
        }
      }

      override def writeRawObject(data: AnyRef): Unit = if (containsFinalTypedReferencesOnly) {
        traversePropToFindReferences(data)
      }

      override def writeEntity(entity: Entity): Unit = {
        if (entity.$isModule) checkTransient(entity)
        // do not generate AssertEntry for monoTemporal entities
        else if (!entity.$inline && !outerMonoTemporal) assertOps += AssertEntry(entity, vt, currentPropertyInfo)
        // if dal$entityRef is null or isTemporary then it is a heap entity, or if it is a finalReference
        if (
          containsFinalTypedReferencesOnly && Option(getEntityRef(entity)).forall { r =>
            r.isTemporary || r.getTypeId.isEmpty
          }
        ) {
          containsFinalTypedReferencesOnly = false
        }
      }

      override def writePropertyInfo(info: PropertyInfo[_]): Unit = {
        super.writePropertyInfo(info)
        currentPropertyInfo = info
      }
    }

    def tryInvalidate(vt: A, entity: Entity, op: (Entity, A) => PersistCacheEntry) = {
      val newOp = op(entity, vt)
      putOrUpgradeIfCompatible(entity, newOp)
    }

    ops foreach {
      _._2 foreach {
        case PersistEntry(entity, vt, upsert, cmid) => new PersistVisitor(vt, upsert, cmid).apply(entity)
        case InvalidateEntry(entity, vt)            => tryInvalidate(vt, entity, InvalidateEntry.apply)
        case RevertEntry(entity, vt)                => tryInvalidate(vt, entity, RevertEntry.apply)
        case _: InvalidateEntryByRef                =>
      }
    }

    deferredValidations foreach { _() }

    val assertResult = assertOps.result()
    val dalEventsTt = events.keySet.filterNot(_.dal$isTemporary).flatMap(_.loadTransactionTime)
    val maxDalEventTt = if (dalEventsTt.nonEmpty) {
      val maxTt = dalEventsTt.max
      log.info(s"minAssignableTtOpt will consider max tt of reused business events: ${maxTt}")
      Set(maxTt)
    } else Set.empty

    val refTxTimes = assertResult.flatMap(a => Option(a.entity.dal$storageInfo.txTime)) ++ maxDalEventTt
    if (refTxTimes.nonEmpty) minAssignableTtOpt = Some(refTxTimes.max)

    val asserts = assertResult flatMap { case ae @ AssertEntry(entity, vt, propertyInfo) =>
      cache.get(keyOf(entity)) match {
        // If we're persisting the entity before or at the same VT (as the assertion), let it through.
        // This is not always correct in the case where the entity has been previously invalidated at a later VT,
        // but we don't have complete checking anyway.
        //
        // Strategically this check should be an interval check on the server after the grouping changes
        // are computed.  i.e. assert that the target is valid during the entire interval of the parent entity version.
        case Some(PersistEntry(_, persistVt, _, _) :: Nil)
            if !(storeValidTimeOf(persistVt) isAfter storeValidTimeOf(vt)) =>
          None

        // There could be multi-vt update for the entity, if there exists any entry corresponding to the entity which is before or at the same VT (as the assertion), let it through.
        // if there are heap entities with same key but with different content for multi-vt update, it will not come to this case, since CacheKey will be different for those heap entities
        case Some(PersistEntry(_, persistVt, _, _) :: listOfPersistEntries)
            if (!((storeValidTimeOf(persistVt) isAfter storeValidTimeOf(vt))
              &&
                listOfPersistEntries
                  .forall(persistEntry => { storeValidTimeOf(persistEntry.vt) isAfter storeValidTimeOf(vt) }))) =>
          None

        // We are being conservative here by ensuring revert vt and AssertValid vt to be same
        // as that is the use case we have at the moment. In future, we can open up this check
        // as needed. As revert could cause invalidate so there is a check in server side to make
        // sure that doesn't happen
        case Some(RevertEntry(revertEntity, revertVt) :: Nil)
            if (storeValidTimeOf(revertVt).equals(storeValidTimeOf(vt))) =>
          Some((revertEntity.dal$entityRef, vt))
        // If we're invalidating or reverting (or multiple ops), assume conflict.
        // There are a lot of potential false positives here (including multi-VT insert)
        // This needs to be strategically handled on the server-side, so not attempting to address here.
        case Some(ex :: _) =>
          throwConflictException(ex, ae)
        case e @ Some(Nil)                        => throw new MatchError(e)
        case None if getEntityRef(entity) eq null =>
// unexpected temporary entity found.
          throw new TemporaryEntityException(entity, propertyInfo)
        case None => Some((getEntityRef(entity), vt))
      }
    }
    // return the seq of entities that do not point to temporary entities. The entities in this skiplist could be temporary or final themselves - so no assumptions on the type of containing entity.
    val ignoredEntities = ignoreListForReferenceResolution.result()
    val entitiesNeedingResolution = needsReferenceResolution.result()
    log.debug(s"This is the ignoreList - ${ignoredEntities.mkString(
        ", ")}. This is the needsResolution List - ${entitiesNeedingResolution.mkString(", ")}")
    (asserts, ignoredEntities, entitiesNeedingResolution)
  }
}

object AbstractPersistBlock {
  val UseImmutableEntitiesProperty = "optimus.dsi.transaction.immutable"
  val generateIgnoreListForReferenceResolution = "optimus.dsi.transaction.generateIgnoreListForReferenceResolution"
  private val log = getLogger[AbstractPersistBlock[_]]

  def scenario(twks: Iterable[Tweak]): Scenario = PersistBlock.scenario(twks)
}

object BasicPersistBlock extends NonForwardingPluginTagKey[BasicPersistBlock] {
  def scenario(twks: Iterable[Tweak]): Scenario = AbstractPersistBlock.scenario(twks)
}

/**
 * An implementation of PersistBlock that uses Instant as the event type. This means everything happens "now", as
 * opposed to behavior with more complex notions of time.
 *
 * @param resolver
 *   The DAL resolver to use.
 */
class BasicPersistBlock(resolver: EntityResolverWriteImpl) extends AbstractPersistBlock[Instant](resolver) {
  override def persist[T <: Entity](entity: T, vt: Instant): T = {
    checkVtPrecisionAndLogIfNano(vt, entity)
    val upsert = entity.dal$storageInfo.lockToken.isEmpty
    addEntity(entity, vt, upsert, None)
  }
  override def persist[T <: Entity](entity: T): T = persist(entity, DALImpl.storeContext)

  def put(entity: Entity): Unit = put(entity, DALImpl.storeContext)
  def put(entity: Entity, vt: Instant): Unit = {
    checkVtPrecisionAndLogIfNano(vt, entity)
    addEntity(entity, vt, false, None)
  }
  def put(entity: Entity, cmid: MSUuid) =
    throw new UnsupportedOperationException(
      "Cannot use CMIDs with persist block. Use an immutable event block instead.")
  def upsert(entity: Entity): Unit = upsert(entity, DALImpl.storeContext)
  def upsert(entity: Entity, vt: Instant): Unit = {
    checkVtPrecisionAndLogIfNano(vt, entity)
    addEntity(entity, vt, true, None)
  }
  def upsert(entity: Entity, cmid: MSUuid) =
    throw new UnsupportedOperationException(
      "Cannot use CMIDs with persist block. Use an immutable event block instead.")

  override def invalidate(entity: Entity) = {
    val vt = DALImpl.storeContext
    checkVtPrecisionAndLogIfNano(vt, entity)
    addInvalidate(entity, vt)
  }
  override def invalidate(entity: Entity, vt: Instant) = {
    checkVtPrecisionAndLogIfNano(vt, entity)
    addInvalidate(entity, vt)
  }

  override def unsafeInvalidate[E <: Entity: Manifest](holder: EntityVersionHolder[E]): Unit = {
    val vt = DALImpl.storeContext
    val clazz = implicitly[Manifest[E]].runtimeClass
    checkVtPrecisionAndLogIfNano(vt, holder)
    addInvalidate(holder.bitempSpace.eref, vt, clazz.getName)
  }

  override def unsafeInvalidate[E <: Entity: Manifest](holder: EntityReferenceHolder[E]): Unit = {
    val vt = DALImpl.storeContext
    val clazz = implicitly[Manifest[E]].runtimeClass
    checkVtPrecisionAndLogIfNano(vt, holder)
    addInvalidate(holder.ref, vt, clazz.getName)
  }

  override def replace[E <: Entity](existing: E, updated: E) = replaceEntity(existing, updated, DALImpl.storeContext)

  /**
   * Create a commit for a set of entities in a cache. This goes through the set of entity references, pulls out the
   * cache entries, converts them to
   *
   * @param as
   *   The set of entities to commit.
   * @param cache
   *   The cache.
   * @return
   *   The commands to execute for the given entities.
   */
  protected[this] def createCommit(
      as: Set[(EntityReference, Instant)],
      cache: Iterable[PersistCacheEntry],
      ignoreListForReferenceResolution: Set[EntityReference]): Seq[(Option[Entity], WriteCommand)] = {

    val persistBldr = Vector.newBuilder[(Entity, Instant, Boolean, Option[MSUuid])]
    val invalidateOps = mutable.HashMap[Entity, Instant]()
    val invalidateByRefOps = mutable.HashMap[EntityReference, (Instant, String)]()
    val revertOps = mutable.HashMap[Entity, Instant]()

    cache foreach {
      case InvalidateEntryByRef(ref, vt, className) =>
        invalidateByRefOps(ref) = (vt, className)
      case e if e.entity.$inline =>
      // skip
      case PersistEntry(entity, vt, upsert, cmid) =>
        require(
          vt != TimeInterval.Infinity,
          s"Valid time cannot be equal to infinity. Valid Time as infinity found for persistent entry ${PersistEntry(entity, vt, upsert, cmid)}"
        )
        persistBldr += ((entity, vt, upsert, cmid))
      case InvalidateEntry(entity, vt) =>
        invalidateOps(entity) = vt
      case RevertEntry(entity, vt) =>
        revertOps(entity) = vt
      case e => throw new MatchError(e)
    }

    val persistOps = persistBldr.result()

    assert(revertOps.isEmpty, "Cannot revert entities in legacy persist block.")
    if (persistOps.nonEmpty || invalidateOps.nonEmpty || invalidateByRefOps.nonEmpty || revertOps.nonEmpty)
      resolver.createPersistCommands(
        as,
        persistOps,
        invalidateOps,
        invalidateByRefOps,
        entityRefs,
        lockTokens,
        minAssignableTtOpt,
        ignoreListForReferenceResolution)
    else
      Nil
  }

  /**
   * Convert a set of entities to a set of write commands, then execute them.
   *
   * @param as
   *   The set of entities to commit.
   * @param cache
   *   The cache.
   * @return
   *   The result of the commit operation.
   */
  @async final def commit(
      as: Set[(EntityReference, Instant)],
      cache: Iterable[PersistCacheEntry],
      ignoreListForReferenceResolution: Set[EntityReference]): PersistResult = {
    val cmds = createCommit(as, cache, ignoreListForReferenceResolution)
    if (cmds.nonEmpty)
      resolver.commitPersistBlock(cmds, entityMutationAllowed)
    else
      PersistResult(TimeInterval.NegInfinity, isEmptyTransaction = true)
  }

  /**
   * Convert a set of entities to a set of write commands, but return them rather than executing them.
   *
   * @param as
   *   The set of entities to commit.
   * @param cache
   *   The cache.
   * @return
   *   The commands.
   */
  override def createCommandsInternal(
      as: Set[(EntityReference, Instant)],
      cache: Iterable[PersistCacheEntry],
      ignoreListForReferenceResolution: Set[EntityReference]): Seq[WriteCommand] = {
    createCommit(as, cache, ignoreListForReferenceResolution).map(_._2)
  }

  override protected def storeValidTimeOf(i: Instant) = i
}

trait BasicPersistBlockComponent extends PersistBlockComponent[BasicPersistBlock] { resolver: EntityResolverWriteImpl =>
  protected[optimus] def createPersistBlock: PluginTagKeyValue[BasicPersistBlock] =
    PluginTagKeyValue(BasicPersistBlock, new BasicPersistBlock(resolver))

  protected[optimus] def createScenario(tweaks: Iterable[Tweak]): Scenario =
    BasicPersistBlock.scenario(tweaks)

  /**
   * To be precise, it is not entirely side-effect free, but tries its best. Currently implemented:
   *   - Put operations don't modify dal$entityRef and dal$storageInfo
   */
  protected[optimus] def createPersistBlockNoSideEffect: PluginTagKeyValue[BasicPersistBlock] =
    PluginTagKeyValue(BasicPersistBlock, new BasicPersistBlock(resolver) { override val entityMutationAllowed = false })
}
