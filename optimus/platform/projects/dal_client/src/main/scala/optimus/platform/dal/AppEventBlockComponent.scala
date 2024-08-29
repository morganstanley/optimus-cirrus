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

import msjava.base.util.uuid.MSUuid
import optimus.entity.IndexInfo
import optimus.platform.AsyncImplicits._
import optimus.platform._
import optimus.platform.dsi.bitemporal.PutApplicationEvent
import optimus.platform.dsi.bitemporal.WriteBusinessEvent
import optimus.platform.dsi.bitemporal.WriteCommand
import optimus.platform.storable.BusinessEventReference
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReference
import optimus.platform.storable.EntityReferenceHolder
import optimus.platform.storable.EntityVersionHolder
import optimus.platform.storable.SerializedKey

import scala.collection.mutable
import optimus.scalacompat.collection._

class PersistEventConflictException(val entity: Entity, val existing: BusinessEvent, val given: BusinessEvent)
    extends RuntimeException

object AppEventBlock extends NonForwardingPluginTagKey[AppEventBlock] {
  def scenario: Scenario = AbstractPersistBlock.scenario(Nil)
}

object BusinessEventBlock extends NonForwardingPluginTagKey[BusinessEventBlock] {
  def scenario: Scenario = Scenario.empty
}

class BusinessEventBlock(val event: BusinessEvent, val cmid: Option[MSUuid]) {
  def validTime: Instant = event.validTime
  override def toString: String =
    s"BusinessEventBlock(event.validTime=${event.validTime}, event.toMap=${event.toMap}, cmid=$cmid)"
}

/**
 * This is a persist block which uses DAL events as its time, meaning it has the full bitemporal notion of time.
 *
 * @param resolver
 *   The DAL resolver to use.
 * @param clientTxTime
 *   The client transaction time.
 * @param elevatedForUser
 *   The user that ran this block.
 */
class AppEventBlock(
    resolver: EntityResolverWriteImpl,
    clientTxTime: Option[Instant] = None,
    elevatedForUser: Option[String] = None)
    extends AbstractPersistBlock[BusinessEventBlock](resolver) {
  override def persist[T <: Entity](entity: T, vt: Instant): T = throw new UnsupportedOperationException
  override def persist[T <: Entity](entity: T): T = {
    val upsert = entity.dal$storageInfo.lockToken.isEmpty
    addEntity(entity, currEvent, upsert, None)
  }

  def put(entity: Entity): Unit = addEntity(entity, currEvent, false, None)
  def put(entity: Entity, cmid: MSUuid): Unit = addEntity(entity, currEvent, false, Some(cmid))
  def put(entity: Entity, vt: Instant): Unit = throw new UnsupportedOperationException
  def upsert(entity: Entity): Unit = addEntity(entity, currEvent, true, None)
  def upsert(entity: Entity, cmid: MSUuid): Unit = addEntity(entity, currEvent, true, Some(cmid))
  def upsert(entity: Entity, vt: Instant): Unit = throw new UnsupportedOperationException

  override def invalidate(entity: Entity) = addInvalidate(entity, currEvent)
  override def invalidate(entity: Entity, vt: Instant) = throw new UnsupportedOperationException
  override def unsafeInvalidate[E <: Entity: Manifest](holder: EntityVersionHolder[E]) = {
    val clazz = implicitly[Manifest[E]].runtimeClass
    addInvalidate(holder.bitempSpace.eref, currEvent, clazz.getName)
  }
  override def unsafeInvalidate[E <: Entity: Manifest](holder: EntityReferenceHolder[E]): Unit = {
    val clazz = implicitly[Manifest[E]].runtimeClass
    addInvalidate(holder.ref, currEvent, clazz.getName)
  }

  override def replace[T <: Entity](existing: T, updated: T) = replaceEntity(existing, updated, currEvent)

  override def revert(entity: Entity) = addRevert(entity, currEvent)

  @async def createCommit(
      asserts: Set[(EntityReference, BusinessEventBlock)],
      ops: Iterable[PersistCacheEntry],
      events: Iterable[CacheValue]
  ): (Map[WriteBusinessEvent.Put, Entity], List[PutApplicationEvent]) = {
    val assertMap = asserts groupBy { _._2 }
    val opsMap = ops.groupBy { _.vt }

    val bcmds
        : Seq[(Map[WriteBusinessEvent.Put, Entity], Map[WriteBusinessEvent.PutSlots, Entity], WriteBusinessEvent)] =
      events.aseq.map { evt =>
        require(
          evt.validTime != TimeInterval.Infinity,
          s"Valid time set as infinity not allowed. Valid time found to be infinity for WriteBusinessEvent ${evt}")
        checkVtPrecisionAndLogIfNano(evt.validTime, evt.event)
        val invalidates = Vector.newBuilder[Entity]
        val invalidateByRefs = Vector.newBuilder[(EntityReference, String)]
        val reverts = Vector.newBuilder[Entity]
        val persists = Vector.newBuilder[(Entity, Boolean, Option[MSUuid])]

        opsMap.getOrElse(evt, Nil) foreach {
          case InvalidateEntryByRef(ref, _, className) =>
            invalidateByRefs += (ref -> className)
          case e if e.entity.$inline =>
          // skip
          case PersistEntry(entity, _, upsert, cmid) =>
            persists += ((entity, upsert, cmid))
          case InvalidateEntry(entity, _) =>
            invalidates += entity
          case RevertEntry(entity, _) =>
            reverts += entity
        }

        val as = assertMap.getOrElse(evt, Nil) map { _._1 }

        resolver.createBusinessEventCommand(
          evt.event,
          evt.cmid,
          as,
          persists.result(),
          invalidates.result(),
          invalidateByRefs.result(),
          reverts.result(),
          entityRefs,
          lockTokens)
      }(collection.Seq.breakOut)

    val entityToPutMap =
      bcmds.map(_._1).foldLeft[Map[WriteBusinessEvent.Put, Entity]](Map.empty)((acc, item) => acc ++ item).toMap

    // EVENTS APPEVENT
    (
      entityToPutMap,
      PutApplicationEvent(
        bcmds.map(_._3),
        -1,
        -1,
        clientTxTime,
        elevatedForUser,
        minAssignableTtOpt = minAssignableTtOpt) :: Nil)
  }

  override def createCommandsInternal(
      asserts: Set[(EntityReference, BusinessEventBlock)],
      cache: Iterable[PersistCacheEntry]): Seq[WriteCommand] =
    createCommit(asserts, cache, validateAndGetBusinessEventBlocks(cache))._2

  @async final def commit(
      asserts: Set[(EntityReference, BusinessEventBlock)],
      ops: Iterable[PersistCacheEntry]): PersistResult = {
    val eventsCache = validateAndGetBusinessEventBlocks(ops)

    val (cmdToEntity, cmds) = createCommit(asserts, ops, eventsCache)

    def extractRef(ref: EntityReference): Entity = {
      Option(ref) flatMap { r =>
        cache.get(ReferenceKey(r))
      } flatMap { _.headOption } map { _.entity } orNull
    }

    if (cmds.nonEmpty)
      resolver.executeAppEvent(cmds, cmdToEntity, entityMutationAllowed, writeRetryAllowed, extractRef)
    else
      PersistResult(TimeInterval.NegInfinity)
  }

  protected final def validateAndGetBusinessEventBlocks(ops: Iterable[PersistCacheEntry]): Iterable[CacheValue] = {
    // Some basic checks on events are needed. In this case we are validating that there aren't multiple
    // conflicting business events within this single AppEventBlockComponent (i.e., within a logical DAL transaction).
    //
    // It's tempting to try to perform such validation earlier, but this isn't actually possible. The reason is that in
    // order to perform this validation we need to compare events and ensure that the events used in each event block
    // are different. For events with an @key, this entails serializing the key value, and if that contains any entity
    // then serialization requires knowing the entity reference of that entity. Since we only perform reference
    // assignment in prepareFlush, we have to do this validation afterwards.
    val eventsCache = mutable.Map.empty[CacheEventKey, CacheValue]
    events.values.foreach { businessEventBlock =>
      val key = keyOf(businessEventBlock.event)
      val existingOpt = eventsCache.get(key)
      existingOpt.foreach { existing =>
        if (businessEventBlock.event != existing.event)
          throwConflictException(businessEventBlock.event, businessEventBlock.event, key)
      }
      eventsCache(key) = businessEventBlock
    }
    events.values.foreach { evblk =>
      if (evblk.event.dal$eventRef eq null)
        evblk.event.dal$eventRef = BusinessEventReference.fresh
    }
    eventsCache.values
  }

  type CacheValue = BusinessEventBlock
  sealed trait CacheEventKey
  case class EventKey(key: BusinessEvent) extends CacheEventKey
  case class EventSerializedKey(key: SerializedKey) extends CacheEventKey
  case class EventReferenceKey(key: BusinessEventReference) extends CacheEventKey

  private[this] def keyOf(event: BusinessEvent) =
    if (event.dal$eventRef ne null)
      EventReferenceKey(event.dal$eventRef)
    else {
      // NB since !indexed implies unique, this is sufficient (even though it's not consistent with other places where
      // we explicitly look for !indexed && unique
      val keys = event.$info.keys.filter(k => !k.indexed)
      if (keys.length > 1)
        throw new UnsupportedOperationException("business event %s can not have two keys".format(event))
      else if (keys.length == 1) {
        val key = keys.head.asInstanceOf[IndexInfo[BusinessEvent, _]]
        EventSerializedKey(key.entityToSerializedKey(event, entityRefs).head)
      } else EventKey(event)
    }
  final def throwConflictException(be1: BusinessEvent, be2: BusinessEvent, key: CacheEventKey): Nothing = {
    val msg = s"found conflicting BusinessEvents evt1:${be1} evt2:${be2} using same key:${key}"
    throw new DALEventConflictException(msg)
  }

  @async def businessEvent[T](evt: BusinessEvent, cmid: Option[MSUuid])(f: AsyncFunction0[T]): T = {
    assert(evt ne null)

    if (currentBusinessEvent.nonEmpty)
      throw new UnsupportedOperationException("Cannot nest businessEvent blocks")

    // The BusinessEventBlock we are executing against is there to capture the event being persisted along with its CMID
    // (if applicable) and the valid time of the event. It is added to the scenario stack, marked by a plugin tag, so
    // that any commands within the block (DAL.put etc.) can use that tag to resolve which event they are executing
    // within. As those commands execute, they populate local cache state in the AppEventBlockComponent, building the
    // set of commands which are ultimately going to be sent to the server for execution.
    val businessEventBlock = events.getOrElseUpdate(evt, new BusinessEventBlock(evt, cmid))
    AdvancedUtils.givenWithPluginTag(BusinessEventBlock, businessEventBlock, BusinessEventBlock.scenario) {
      f()
    }
  }

  override protected def storeValidTimeOf(a: BusinessEventBlock) = a.validTime

  private[this] def currentBusinessEvent: Option[BusinessEventBlock] =
    EvaluationContext.scenarioStack.findPluginTag(BusinessEventBlock)

  private[this] def currEvent: BusinessEventBlock =
    currentBusinessEvent.getOrElse(throw new IllegalStateException("Cannot persist without event in transaction block"))

  override protected def saveState(): Unit = {
    super.saveState()
  }

  override protected def restoreState(): Unit = {
    super.restoreState()
  }

  private[optimus /*dal*/ ] override def dispose(): Unit = {
    super.dispose()
    entityRefs.clear()
  }
}

class DelayedAppEventBlock(
    resolver: EntityResolverWriteImpl,
    clientTxTime: Option[Instant] = None,
    elevatedForUser: Option[String] = None)
    extends AppEventBlock(resolver, clientTxTime, elevatedForUser)
    with DelayedDALScenario {
  override val entityMutationAllowed = false
  override val writeRetryAllowed = true

  final def flushNoSave(): Transaction = {
    val commandCreator = asAsync(() => {
      if (!isStateSaved) // This because we want to re-resolve keys
        saveState()
      else
        restoreState()
      val asserts = prepareFlush()
      val cmdsCache = cacheValues.flatten
      val (cmdToEnt, cmds) = createCommit(asserts, cmdsCache, validateAndGetBusinessEventBlocks(cmdsCache))
      completeFlush()
      Transaction.CommandCreatorResult(cmds.head, cmdToEnt)
    })
    new Transaction(commandCreator, dispose _, cacheValues.size, clientTxTime)
  }
}

trait AppEventBlockComponent { resolver: EntityResolverWriteImpl =>
  final protected[optimus /*platform*/ ] def inNewAppEvent[T](
      f: => T,
      txTime: Option[Instant] = None): (T, PersistResult) =
    inNewAppEvent$NF(asAsync(() => f), txTime)
  @async final protected[optimus /*platform*/ ] def inNewAppEvent$NF[T](
      f: AsyncFunction0[T],
      txTime: Option[Instant] = None): (T, PersistResult) = {
    if (currentAppEvent.nonEmpty)
      throw new UnsupportedOperationException("Cannot nest appEvent blocks")
    val newBlock = new AppEventBlock(resolver, txTime)
    DALBlock.execute(AppEventBlock.scenario, PluginTagKeyValue(AppEventBlock, newBlock), f)
  }

  @async protected[optimus /*platform*/ ] def inNewAppEventNoEntityMutation[T](
      f: AsyncFunction0[T],
      elevatedForUser: Option[String] = None): (T, PersistResult) = {
    if (currentAppEvent.nonEmpty)
      throw new UnsupportedOperationException("Cannot nest appEvent blocks")
    val newBlock = new AppEventBlock(resolver, None, elevatedForUser) {
      override val entityMutationAllowed = false
      override val writeRetryAllowed = true
    }
    DALBlock.execute(AppEventBlock.scenario, PluginTagKeyValue(AppEventBlock, newBlock), f)
  }

  @async protected[optimus /*platform*/ ] def inDelayedAppEvent[T](
      f: AsyncFunction0[T],
      clientTxTime: Option[Instant] = None,
      elevatedForUser: Option[String] = None): Transaction = {
    if (currentAppEvent.nonEmpty)
      throw new UnsupportedOperationException("Cannot nest appEvent blocks")
    val newBlock = new DelayedAppEventBlock(resolver, clientTxTime, elevatedForUser)
    DALBlock.executeDelayed(AppEventBlock.scenario, newBlock, f)
  }

  private[this] def currentAppEvent: Option[AppEventBlock] =
    EvaluationContext.scenarioStack.findPluginTag(AppEventBlock)

  private[dal] final def inAppEvent[T](f: => T): T = inAppEvent$NF(asAsync(() => f))
  @async private[dal] final def inAppEvent$NF[T](f: AsyncFunction0[T]): T = {
    val existing = currentAppEvent
    if (existing.isEmpty) {
      val newBlock = new AppEventBlock(resolver)
      DALBlock.execute(AppEventBlock.scenario, PluginTagKeyValue(AppEventBlock, newBlock), f)._1
    } else {
      f()
    }
  }

  private[dal] final def event[T](e: BusinessEvent)(f: => T): T = event$NF(e)(asAsync(() => f))
  @async private[dal] final def event$NF[T](e: BusinessEvent)(f: AsyncFunction0[T]): T = inAppEvent {
    currentAppEvent
      .getOrElse(throw new IllegalStateException("Cannot run outside AppEventBlock"))
      .businessEvent(e, None)(f)
  }

  @async private final def inAppEventNoEntityMutation[T](elevatedForUser: Option[String])(
      f: AsyncFunction0[T]): Option[PersistResult] = {
    val existing = currentAppEvent
    if (existing.isEmpty) {
      val newBlock = new AppEventBlock(resolver, elevatedForUser = elevatedForUser) {
        override val entityMutationAllowed = false
        override val writeRetryAllowed = true
      }
      Some(DALBlock.execute(AppEventBlock.scenario, PluginTagKeyValue(AppEventBlock, newBlock), f)._2)
    } else {
      f()
      None
    }
  }

  @async def eventNoEntityMutation[T](e: BusinessEvent, cmid: Option[MSUuid], elevatedFor: Option[String] = None)(
      f: AsyncFunction0[T]): Option[PersistResult] =
    inAppEventNoEntityMutation(elevatedFor)(asAsync(() => {
      currentAppEvent
        .getOrElse(throw new IllegalStateException("Cannot run outside AppEventBlock"))
        .businessEvent(e, cmid)(f)
    }))
}
