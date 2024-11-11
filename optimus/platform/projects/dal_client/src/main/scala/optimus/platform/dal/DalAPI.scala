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

import msjava.base.util.uuid.MSUuid
import optimus.config.OptimusConfigurationException
import optimus.core.CoreAPI
import optimus.core.NodeAPI
import optimus.dsi.partitioning.Partition
import optimus.dsi.partitioning.PartitionMap
import optimus.graph.Node
import optimus.graph.NodeKey
import optimus.graph.NodeFuture
import optimus.graph.PropertyNode
import optimus.platform.AsyncImplicits._
import optimus.platform.PluginHelpers.toNode
import optimus.platform._
import optimus.platform.annotations._
import optimus.platform.dal.messages.StreamsACLs
import optimus.platform.dal.messages.MessagesOperations
import optimus.platform.dal.pubsub.PubSubOperations
import optimus.platform.dal.restricted.ThinUntweakedScenarioState
import optimus.platform.dal.session.RolesetMode.SpecificRoleset
import optimus.platform.dsi.bitemporal.MessagesErrorResult
import optimus.platform.dsi.bitemporal.MessagesPublishCommandBase
import optimus.platform.dsi.bitemporal.PutApplicationEvent
import optimus.platform.internal.TemporalSource
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityCompanionBase
import optimus.platform.storable.EntityReference
import optimus.platform.storable.EntityReferenceHolder
import optimus.platform.storable.EntityVersionHolder
import optimus.platform.temporalSurface.impl.FlatTemporalContext
import optimus.platform.util.ElevatedUtils
import optimus.utils.MacroUtils.SourceLocation

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait TemporalContextAPI {
  @tweakable
  @nodeSync
  def validTime: Instant = TemporalSource.validTimeAsNode
  def validTime$queued: NodeFuture[Instant] = NodeAPI.queuedNodeOf { TemporalSource.validTimeAsNode }
  def validTime$newNode: PropertyNode[Instant] = TemporalSource.validTimeMarker

  @tweakable
  @nodeSync
  def transactionTime: Instant = TemporalSource.transactionTimeAsNode
  def transactionTime$queued: NodeFuture[Instant] = NodeAPI.queuedNodeOf { TemporalSource.transactionTimeAsNode }
  def transactionTime$newNode: PropertyNode[Instant] = TemporalSource.transactionTimeMarker

  @tweakable
  @nodeSync // TODO (OPTIMUS-11594): revisit name of public API
  def loadContext: TemporalContext = TemporalSource.loadContext
  def loadContext$queued: NodeFuture[TemporalContext] = NodeAPI.queuedNodeOf { TemporalSource.loadContext }
  def loadContext$newNode: PropertyNode[Nothing] = TemporalSource.loadContextMarker

  @tweakable
  @nodeSync // TODO (OPTIMUS-11594): revisit name of public API
  def storeContext: Instant = TemporalSource.validTimeStore
  def storeContext$queued: NodeFuture[Instant] = NodeAPI.queuedNodeOf { TemporalSource.validTimeStore }
  def storeContext$newNode: PropertyNode[Instant] = TemporalSource.validTimeStoreMarker

  @tweakable
  @nodeSync // TODO (OPTIMUS-11594): revisit name of public API
  def temporalContextFactory: TemporalContextFactory = TemporalSource.temporalContextFactory
  def temporalContextFactory$queued: NodeFuture[TemporalContextFactory] = NodeAPI.queuedNodeOf {
    TemporalSource.temporalContextFactory
  }
  def temporalContextFactory$newNode: NodeKey[TemporalContextFactory] = NodeAPI.nodeKeyOf {
    TemporalSource.temporalContextFactory
  }

  def TemporalContext(validTime: Instant, txTime: Instant, tag: Option[String] = None)(implicit
      sourceLocation: SourceLocation): TemporalContext = {
    FlatTemporalContext(validTime, txTime, tag)
  }
  // This gives the immediately before validTime TemporalContext using the minus 1 microsecond implementation.
  def TemporalContextImmediatelyBeforeValidTime(
      validTime: Instant,
      txTime: Instant,
      tag: Option[String] = None): TemporalContext = {
    FlatTemporalContext(validTime.minus(1, ChronoUnit.MICROS), txTime, tag)
  }

  // This gives the immediately after validTime TemporalContext using the plus 1 microsecond implementation.
  def TemporalContextImmediatelyAfterValidTime(
      validTime: Instant,
      txTime: Instant,
      tag: Option[String] = None): TemporalContext = {
    FlatTemporalContext(validTime.plus(1, ChronoUnit.MICROS), txTime, tag)
  }

}

trait PartitionMapAPI {
  private[optimus] def partitionMapForNotification = PartitionMapAPI.getPartitionMapFromRuntimeEnv
}

object PartitionMapAPI {
  // If using MockRuntimeConfiguration, please add the override
  // rc.withOverride(PartitionMap.PartitionMapProperty, PartitionMap())
  private[optimus] def getPartitionMapFromRuntimeEnv: PartitionMap = {
    getPartitionMapFromRuntimeEnv(EvaluationContext.env)
  }

  private[optimus] def getPartitionMapFromRuntimeEnv(runtime: RuntimeEnvironment): PartitionMap = {
    runtime.entityResolver match {
      case r: HasPartitionMap => r.partitionMap
      case _ =>
        val config = runtime.config.runtimeConfig
        config
          .get(PartitionMap.PartitionMapProperty)
          .getOrElse(
            throw new OptimusConfigurationException("Couldn't find the PartitionMap in the RuntimeEnvironment."))
          .asInstanceOf[PartitionMap]
    }
  }
}

private[optimus] trait DalAPI extends TemporalContextAPI with PartitionMapAPI {
  @async final def newTransaction[T](f: => T): PersistResult = newTransactionImpl(asAsync(() => f), None)
  @async final def newTransaction[T](f: AsyncFunction0[T]): PersistResult = newTransactionImpl(f, None)

  @async private[optimus] final def newTransaction[T](elevatedFor: String)(f: => T): PersistResult =
    newTransactionImpl(asAsync(() => f), Some(elevatedFor))
  @async private[optimus] final def newTransaction$NF[T](elevatedFor: String)(f: AsyncFunction0[T]): PersistResult =
    newTransactionImpl(f, Some(elevatedFor))

  @async final def delayedTransaction[T](f: => T): Transaction =
    delayedTransactionImpl(asAsync(() => f), ElevatedUtils.currentElevatedForUser)
  @async final def delayedTransaction$NF[T](f: AsyncFunction0[T]): Transaction =
    delayedTransactionImpl(f, ElevatedUtils.currentElevatedForUser)

  @async final def delayedTransaction[T](elevatedFor: String)(f: => T): Transaction =
    delayedTransactionImpl(asAsync(() => f), Some(elevatedFor))
  @async def delayedTransaction$NF[T](elevatedFor: String)(f: AsyncFunction0[T]): Transaction =
    delayedTransactionImpl(f, Some(elevatedFor))

  // Ideally there ought to be two separate APIs here, one which takes a cmid argument and one which doesn't. Then they
  // should both call through to newEventImpl, one with Some(cmid) and one with None. However, in order for auto-asAsync
  // to work, that lands us in trouble because the compiler "gets confused" (technical term) and tries to call the
  // sync-ish version of newEvent rather than the async-ish version if there is no async call in the body. The result is
  // that code like this fails to work:
  // newTransaction {
  //   newEvent(...) {
  //     DAL.put(whatever)
  //   }
  // }
  // ...because the newEvent call is made with an AsyncFunction wrapped in an asAsync(), which means that we don't end
  // up having an event block in scope by the time we execute DAL.put. This manifests as an exception along the line of
  // "cannot persist outside of event block in transaction". Once there is a fix to auto-asAsync, we can split this API
  // back into two different calls and remove the default-null argument here.
  @async final def newEvent[T](e: BusinessEvent, cmid: MSUuid = null)(f: => T): Option[PersistResult] = {
    // identity(...) is required for Loom: without it, Scala optimizes this by calling f rather than creating () => f
    newEventImpl(e, asAsync(() => identity(f)), Option(cmid), ElevatedUtils.currentElevatedForUser)
  }
  @async final def newEvent$NF[T](e: BusinessEvent, cmid: MSUuid = null)(f: AsyncFunction0[T]): Option[PersistResult] =
    newEventImpl(e, f, Option(cmid), ElevatedUtils.currentElevatedForUser)

  @async private final def newTransactionImpl[T](f: AsyncFunction0[T], elevatedFor: Option[String]): PersistResult =
    resolver.inNewAppEventNoEntityMutation(f, elevatedFor)._2

  @async private final def delayedTransactionImpl[T](f: AsyncFunction0[T], elevatedFor: Option[String]): Transaction =
    resolver.inDelayedAppEvent(f, clientTxTime = None, elevatedForUser = elevatedFor)

  @async private final def newEventImpl[T](
      ev: BusinessEvent,
      f: AsyncFunction0[T],
      cmid: Option[MSUuid],
      elevatedFor: Option[String]): Option[PersistResult] =
    resolver.eventNoEntityMutation(ev, cmid, elevatedFor)(f)

  private[this] def resolver: ResolverImpl = EvaluationContext.env.entityResolver.asInstanceOf[ResolverImpl]

  def sequence(txn1: Transaction, txns: Transaction*): TransactionSequence =
    new TransactionSequence(txn1 :: txns.toList)

  def sequence(txns: Seq[Transaction]): TransactionSequence = new TransactionSequence(txns)

  @async def batchExecute(
      batchSize: Int,
      txn1: BatchExecutable,
      txns: BatchExecutable*): Map[Transaction, Try[PersistResult]] = batchExecute(txn1 :: txns.toList)

  @async def batchExecute(txn1: BatchExecutable, txns: BatchExecutable*): Map[Transaction, Try[PersistResult]] =
    batchExecute(txn1 :: txns.toList)

  @async def batchExecute(transactions: Seq[BatchExecutable]): Map[Transaction, Try[PersistResult]] = {
    executeInBatches(transactions, None)
  }

  @async def batchExecute(batchSize: Int, transactions: Seq[BatchExecutable]): Map[Transaction, Try[PersistResult]] =
    executeInBatches(transactions, Some(batchSize))

  implicit def entityEventView[E <: Entity](e: E): EntityEventModule.EntityOps[E] = new EntityEventModule.EntityOps(e)
  implicit def entityCompanionOpsView[E <: Entity](
      ec: EntityCompanionBase[E]): EntityEventModule.EntityCompanionOps[E] =
    new EntityEventModule.EntityCompanionOps[E](ec)

  @async private[this] def getPartitionForTransaction(t: Transaction): Try[Partition] = {
    val pae = CoreAPI.asyncResult { t.createCommand().pae }.toTry
    pae match {
      case Success(paes) => Success(getPartitionForPutApplicationEvent(paes))
      case Failure(ex)   => Failure[Partition](ex)
    }
  }

  def getPartitionForPutApplicationEvent(
      pae: PutApplicationEvent
  ): Partition = DALImpl.getPartitionForPutApplicationEvent(pae)

  @async private def executeInBatches(
      transactions: Seq[BatchExecutable],
      batchSize: Option[Int]
  ): Map[Transaction, Try[PersistResult]] = {
    if (resolver.partitionMap.isEmpty)
      new BatchExecutor(resolver, batchSize).batchExecute(transactions)
    else {
      val txnsSeq = transactions.flatMap {
        case txn: Transaction         => List(txn)
        case seq: TransactionSequence => seq.transactions
      }

      val txnToPartition = txnsSeq.aseq.map(txn => (txn, getPartitionForTransaction(txn)))

      val failedTxns = txnToPartition.collect { case (txn, failedPartitionLookup: Failure[Partition]) =>
        (txn, Failure[PersistResult](failedPartitionLookup.exception))
      }.toMap
      val passedTxns = txnToPartition.collect { case (txn, successPartitionLookup: Success[Partition]) =>
        (successPartitionLookup.value, txn)
      }

      val partitionWithGroupedCommands = passedTxns.groupBy { case (k, v) => k } map { case (k, v) =>
        v.map { case (part, command) => command }
      }

      partitionWithGroupedCommands.aseq
        .flatMap(txns => new BatchExecutor(resolver, batchSize).batchExecute(txns))
        .toMap ++ failedTxns
    }
  }

  // The withDalRoles API takes a roleset, create a UntweakedScenarioState, and then rewire the untweaked scenario state to
  // point to that of the new environment.
  private[optimus /*platform*/ ] def untweakedScenarioStateGivenRoles(
      roles: SpecificRoleset): UntweakedScenarioState = {
    EvaluationContext.entityResolver match {
      case sf: SessionFetcher =>
        val dsi = sf.dsi
        val ecs = dsi.createNewSession(roles.underlying)
        val unrestrictedResolver = sf match {
          case rer: RestrictedEntityResolver => rer.unrestricted
          case er: EntityResolver            => er
        }
        val resolver = RestrictedEntityResolver(dsi, ecs, unrestrictedResolver)
        ThinUntweakedScenarioState(roles, resolver)
      case _ =>
        throw new IllegalArgumentException(
          s"Cannot use instance of entity resolver ${EvaluationContext.entityResolver} without " +
            s"SessionFetcher for thin session establishment")
    }
  }

  @nodeSync
  @nodeSyncLift
  def withDalRoles[T](roles: String*)(@nodeLift @nodeLiftByName f: => T): T =
    withDalRoles$withNode(roles: _*)(toNode(f _))
  // noinspection ScalaUnusedSymbol
  def withDalRoles$withNode[T](roles: String*)(f: Node[T]): T = withDalRoles$newNode(roles)(f).get
  // noinspection ScalaUnusedSymbol
  def withDalRoles$queued[T](roles: String*)(f: Node[T]): Node[T] = withDalRoles$newNode(roles)(f).enqueueAttached

  private def withDalRoles$newNode[T](roles: Seq[String])(f: Node[T]): Node[T] = {
    val uss = untweakedScenarioStateGivenRoles(SpecificRoleset(roles.toSet))
    val ss = EvaluationContext.scenarioStack.withNewRoot(uss.environment, f)
    f.attach(ss)
    f
  }
}

abstract class DALModuleAPI {

  /**
   * Adds an entity instance to the persist block.
   *
   * When the persist block completes, the given entity will be persisted to the datastore.
   *
   * Properties on the given entity that are not marked with cascade are not persisted, though the DAL will validate
   * that they are persisted.
   *
   * @param entity
   *   an entity to be persisted to the datastore
   * @return
   *   the entity that will be persisted (the same entity that was passed in)
   * @throws UnsupportedOperationException
   *   if used outside persist block
   */
  def persist[E <: Entity](entity: E): E =
    persistBlock("persist").persist(entity)

  def persist[E <: Entity](entity: E, vt: Instant): E =
    persistBlock("persist").persist(entity, vt)

  /**
   * Save a new version of this entity. If the supplied instance is fresh (i.e. not loaded from the DAL), the operation
   * will fail if an entity with the same key already exists in the datastore. Otherwise perform "bisection update" of a
   * previously-loaded entity at the current valid time, as long as no concurrent update to the entity has occurred.
   */
  def put(entity: Entity): Unit =
    persistBlock("persist").put(entity)

  def put(entity: Entity, cmid: MSUuid): Unit =
    persistBlock("persist").put(entity, cmid)

  def put(entity: Entity, vt: Instant): Unit =
    persistBlock("persist").put(entity, vt)

  /**
   * Unconditionally save a new version of this entity.
   *
   * If any previous entity exists with the same key, then perform a bisection update. Otherwise insert a new entity
   * into the datastore. This operation performs no checking for concurrent updates or conflicts with existing entities.
   */
  def upsert(entity: Entity): Unit =
    persistBlock("persist").upsert(entity)

  def upsert(entity: Entity, cmid: MSUuid): Unit =
    persistBlock("persist").upsert(entity, cmid)

  def upsert(entity: Entity, vt: Instant): Unit =
    persistBlock("persist").upsert(entity, vt)

  /**
   * Invalidates an existing entity after the current valid time until the next point on its valid timeline, if any.
   *
   * If an entity is valid before the given/current valid time, it will remain so up to the valid time. If the
   * given/current valid time is the same as the entity's valid time, the entity will be completely invalidated in the
   * datastore.
   *
   * @param entity
   *   the entity to invalidate
   * @throws UnsupportedOperationException
   *   if used outside persist block
   */
  def invalidate(entity: Entity): Unit = {
    persistBlock("invalidate").invalidate(entity)
  }

  def revert(entity: Entity): Unit = {
    persistBlock("revert").revert(entity)
  }

  def invalidate(entity: Entity, vt: Instant): Unit = {
    persistBlock("invalidate").invalidate(entity, vt)
  }

  private[optimus /*platform*/ ] def unsafeInvalidate[E <: Entity: Manifest](holder: EntityVersionHolder[E]): Unit = {
    persistBlock("unsafeInvalidate").unsafeInvalidate(holder)
  }

  private[optimus /*platform*/ ] def unsafeInvalidate[E <: Entity: Manifest](holder: EntityReferenceHolder[E]): Unit = {
    persistBlock("unsafeInvalidate").unsafeInvalidate(holder)
  }

  /**
   * Replace entity data at current valid time.
   *
   * Given existing and updated entities, replace the non-key properties of the existing entity with the values provided
   * in the updated.
   *
   * @param existing
   *   Existing entity, loaded from data store
   * @param updated
   *   Temporary entity containing updated values
   * @throws IllegalArgumentException
   *   if keys of existing and updated entities do not match
   */
  def replace[E <: Entity](existing: E, updated: E): Unit = {
    persistBlock("replace").replace(existing, updated)
  }

  /**
   * Publish the Self Contained Event to Messaging System
   *
   * @param evt
   *   Self contained Event that will be published
   * @param waitForAck
   *   If false, it will behave as "fire-and-forget", i.e. there will be no guarantee of message reaching messaging
   *   system. And so, it's faster than the alternative.
   * @throws MessagesErrorResult
   *   if publishing of event fails for any reason
   */
  @async def publishEvent[T <: BusinessEvent with ContainedEvent](evt: T, waitForAck: Boolean = true): Unit =
    if (waitForAck) resolver.publishEvent(evt, MessagesPublishCommandBase.Option.Ack)
    else resolver.publishEvent(evt, MessagesPublishCommandBase.Option.IgnoreAck)

  /**
   * Publish the "Delayed Transaction" to Messaging System
   *
   * @param transaction
   *   "Delayed Transaction" that will be published
   * @throws MessagesErrorResult
   *   if publishing of transaction fails for any reason
   */
  @async def publishTransaction[T <: Transaction](
      transaction: T
  ): Unit = resolver.publishTransaction(transaction)

  @async private[platform] def checkEntitlementAndSetACLs(appId: String, acls: Seq[StreamsACLs]) =
    resolver.checkEntitlementAndSetACLs(appId, acls)

  @parallelizable
  def getTemporalCoordinates(e: Entity): TemporalCoordinates = {
    // later we will pull this from DSI, for now we are using the internal member of entity
    TemporalCoordinates(e.validTime, e.txTime)
  }

  def getPartitionForPutApplicationEvent(
      pae: PutApplicationEvent
  ): Partition = PartitionedDsiProxy.validateAndGetSinglePartitionForCommands(resolver.partitionMap, Seq(pae))

  /**
   * Invalidates all currently valid instances of an entity by type.
   *
   * This method is intended to be used until proper entity schema versioning is introduced. Since the DAL can't
   * currently load entities with properties that don't match the current entity definition,
   * DAL.invalidateCurrentInstances() can be used as a rough tool to clear out existing instances of a type.
   *
   * No existing entities are purged from the store. They will still be accessible at a transaction time prior the
   * invocation of invalidateCurrentInstances.
   */
  @async def invalidateCurrentInstances[E <: Entity: Manifest](): Int =
    resolver.invalidateCurrentInstances[E]()

  /**
   * Similar to invalidateCurrentInstances[E]() but only with a batch of given entity references. When there are too
   * many instances for a class, this function should be used to invalidate by batches.
   *
   * @return
   *   number of instances invalidated
   */
  def invalidateCurrentInstances[E <: Entity: Manifest](entityReferences: Seq[EntityReference]): Int =
    resolver.invalidateCurrentInstances[E](entityReferences)

  def invalidateCurrentInstances(entityReferences: Seq[EntityReference], className: String): Int =
    resolver.invalidateCurrentInstances(entityReferences, className)

  /**
   * Completely purges all data from the current database. Use with caution This works only on temporary unnamed
   * databases, or named current user database
   */
  def purgePrivateContext(): Unit = {
    resolver.purgePrivateContext()
  }

  /**
   * Get the current persist block.
   *
   * @param op
   *   The name of the operation (used to construct an exception)
   * @return
   *   The persist block data.
   */
  private[this] def persistBlock(op: String): PersistBlock =
    EvaluationContext.scenarioStack.findPluginTag(BasicPersistBlock) match {
      case Some(ds) => ds
      case _ =>
        EvaluationContext.scenarioStack.findPluginTag(AppEventBlock) match {
          case Some(ds) => ds
          case _        => throw new UnsupportedOperationException("cannot call " + op + " outside of a persist block")
        }
    }

  def resolver: ResolverImpl = EvaluationContext.env.entityResolver.asInstanceOf[ResolverImpl]
}

@entity
private[optimus /*dal*/ ] abstract class ResolverImpl
    extends DSIResolver
    with EntityResolverWriteImpl
    with EntityResolverReadImpl
    with BasicPersistBlockComponent
    with AppEventBlockComponent
    with AsyncDALOperations
    with PubSubOperations
    with MessagesOperations

object DALImpl extends DALModuleAPI with CoreAPI with TemporalContextAPI with PartitionMapAPI
