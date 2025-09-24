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
import msjava.protobufutils.server.BackendException
import msjava.slf4jutils.scalalog.getLogger
import optimus.config.RuntimeEnvironmentEnum
import optimus.core.Collections
import optimus.core.CoreAPI
import optimus.dsi.base.DSIHaltException
import optimus.dsi.partitioning.DefaultPartition
import optimus.dsi.partitioning.Partition
import optimus.dsi.partitioning.PartitionMap
import optimus.entity._
import optimus.graph.diagnostics.ObservationRegistry
import optimus.graph.diagnostics.gridprofiler.GridProfiler
import optimus.platform._
import optimus.platform.annotations.deprecating
import optimus.platform.AsyncImplicits._
import optimus.platform.dal.RequestGenerator.checkMonoTemporalSupported
import optimus.platform.dal.config.DALEnvs
import optimus.platform.dal.config.DalEnv
import optimus.platform.dsi.Feature
import optimus.platform.dsi.bitemporal
import optimus.platform.dsi.bitemporal.Query
import optimus.platform.dsi.bitemporal.{
  DuplicateStoredKeyException => _,
  DuplicateUniqueIndexException => _,
  OutdatedVersionException => _,
  SecureEntityException => _,
  _
}
import optimus.platform.storable._
import optimus.platform.temporalSurface.DataFreeTemporalSurfaceMatchers
import optimus.platform.temporalSurface.impl.FlatTemporalContext
import optimus.scalacompat.collection._
import optimus.platform.dsi.bitemporal.EventQuery

import scala.collection.mutable
import scala.util.control.NonFatal
import scala.{collection => sc}

object EntityResolverWriteImpl {
  private def resolvePartitionForCmds(writeRequests: Seq[WriteCommand], partitionMap: PartitionMap): Partition = {
    partitionMap.partitions.toSeq match {
      case Seq() => DefaultPartition
      case _ =>
        if (PartitionedDsiProxy.checkEmptyTransaction(writeRequests)) DefaultPartition
        else PartitionedDsiProxy.getPartitionForCommands(writeRequests, partitionMap)
    }
  }
}

trait EntityResolverWriteImpl extends EntityResolverWriteOps { this: DSIResolver =>
  import EntityResolverWriteImpl._

  val requestGenerator = new RequestGenerator(dsi)

  val retryAttempts: Int = DSIClientCommon.getMaxRetryAttempts
  val retryBackoffTime: Long = DSIClientCommon.getBackoffTimeInterval

  private[this] val log = getLogger[EntityResolverWriteImpl]

  private val executor: DSIExecutor = DALDSIExecutor

  private def executeSaveCommands(requests: Iterable[(Option[Entity], WriteCommand)], mutateEntities: Boolean) = {
    val writeRequests = requests.iterator.map { _._2 }.toIndexedSeq
    val results = executor.executeLeadWriterCommands(dsi, writeRequests)
    if (GridProfiler.DALProfilingOn || ObservationRegistry.active) {
      // only counts PutResult
      val n = results.count(_.isInstanceOf[PutResult])
      if (GridProfiler.DALProfilingOn) GridProfiler.recordDALWrite(n)
      if (ObservationRegistry.active) DalCounters.dalWriteCounter.add(n)
    }
    val resolvedPartition: Partition = resolvePartitionForCmds(writeRequests, partitionMap)
    val erefs = postSaveProcessing(requests, results, mutateEntities, resolvedPartition)

    PersistResult(lastWitnessedTime(resolvedPartition), erefs)
  }

  private[dal] def throwErrorResult(err: ErrorResult, partition: Partition) = {
    if (err.readAfter != null)
      updateLastWitnessedTime(Map(partition -> err.readAfter))
    throw err.error
  }

  private def throwErrorResults(errs: Seq[ErrorResult], partition: Partition): Unit = {
    if (errs.nonEmpty) {
      if (errs.size == 1) throwErrorResult(errs.head, partition)
      else {
        var latestReadAfter: Instant = null
        val error = new GeneralDALException(s"$errs.size} DAL errors, see suppressed exceptions for causes")
        errs.foreach { err =>
          error.addSuppressed(err.error)
          if (err.readAfter ne null) {
            if ((latestReadAfter eq null) || err.readAfter.isAfter(latestReadAfter))
              latestReadAfter = err.readAfter
          }
        }
        if (latestReadAfter ne null)
          updateLastWitnessedTime(Map(partition -> latestReadAfter))
        throw error
      }
    }
  }

  def serverFeatures = dsi.serverFeatures()

  @async final override def executeAppEvent(
      appEvtCmds: Seq[PutApplicationEvent],
      cmdToEntity: Map[WriteBusinessEvent.Put, Entity],
      mutateEntities: Boolean,
      retryWrite: Boolean,
      refMap: EntityReference => Entity): PersistResult = {

    require(appEvtCmds.nonEmpty, "This API must be called with at least one PutApplicationEvent")
    require(
      appEvtCmds.map(_.extractClientInfo).distinct.size == 1,
      "Cannot send non-duplicate PutApplicationEvents. They should be resent as separate requests.")

    val appEvtId = Some(AppEventReference.fresh)
    val allBes = appEvtCmds flatMap (_.bes)
    val finalCmd = appEvtCmds.head.copy(bes = allBes, appEvtId = appEvtId)
    executeAndRetryAppEvent(finalCmd, cmdToEntity, refMap, mutateEntities, retryWrite, retryAttempts, retryBackoffTime)
  }

  @async private[this] def executeAndRetryAppEvent(
      cmd: PutApplicationEvent,
      cmdToEntity: Map[WriteBusinessEvent.Put, Entity],
      refMap: EntityReference => Entity,
      mutateEntities: Boolean,
      retryWrite: Boolean,
      attempts: Int,
      backoff: Long): PersistResult = {
    val res = CoreAPI.asyncResult {
      val cmds = Seq(cmd)
      val result = executor.executeLeadWriterCommands(dsi, cmds)
      if (GridProfiler.DALProfilingOn || ObservationRegistry.active) {
        // extracts counts of puts, invalidates and reverts from PutApplicationEventResult
        val n = result.collect { case r: PutApplicationEventResult =>
          r.beResults.map(r => r.putResults.size + r.invResults.size + r.revertResults.size).sum
        }.sum
        if (GridProfiler.DALProfilingOn) GridProfiler.recordDALWrite(n)
        if (ObservationRegistry.active) DalCounters.dalWriteCounter.add(n)
      }
      val resolvedPartition = resolvePartitionForCmds(cmds, partitionMap)
      processAppEventResults(cmd, result, cmdToEntity, refMap, mutateEntities, resolvedPartition)
    }
    def appEvtId = cmd.appEvtId
    res match {
      case NodeSuccess(r) => r
      case NodeFailure(NonFatal(e)) if isRetryableInAppEvt(e) =>
        if (attempts <= 1) {
          log.error(s"Out of retries for application event id $appEvtId. attempts is $attempts")
          throw convertDSIException(null, e)
        } else {
          log.warn(
            s"DAL client got exception ${e.getClass.getName} - ${e.getMessage} while putting application event with id $appEvtId, retrying after ${backoff}ms, retry attempts left $attempts")
          log.debug("Stack trace:", e)
          if (serverFeatures.supports(Feature.ClientSideAppEventReferenceAssignment) && retryWrite) {
            val retryCmd = getRetryAppEvent(cmd, e)
            executeAndRetryAppEvent(
              retryCmd,
              cmdToEntity,
              refMap,
              mutateEntities,
              retryWrite,
              attempts - 1,
              DSIClientCommon.boundedBackOffTime(backoff * 2))
          } else throw e
        }
      case NodeFailure(e) =>
        log.error(e.getMessage, e)
        throw e
    }
  }

  private def isRetryableInAppEvt(ex: Throwable) = ex match {
    case be @ (_: BackendException | _: DalTimedoutException | _: WriteLeaderHaltException |
        _: UnexpectedResultSizeException | _: DuplicateAppEventReferenceException) =>
      true
    case _ => false
  }

  private def getRetryAppEvent(appEvt: PutApplicationEvent, ex: Throwable) = {
    val retryAppEvent = ex match {
      case _: DuplicateAppEventReferenceException =>
        appEvt.copy(appEvtId = Some(AppEventReference.fresh))

      case _ => appEvt
    }

    if (!retryAppEvent.retry) retryAppEvent.copy(retry = true) else retryAppEvent
  }

  @async private def processAppEventResults(
      appEvtCmd: PutApplicationEvent,
      result: Seq[Result],
      cmdToEntity: Map[WriteBusinessEvent.Put, Entity],
      refMap: EntityReference => Entity,
      mutateEntities: Boolean,
      partition: Partition
  ): PersistResult = {
    def extractEntityRef(t: Throwable): Entity = t match {
      case ex: bitemporal.OutdatedVersionException => refMap(ex.ref)
      case dk: bitemporal.DuplicateKeyException    => dk.inputEntityRef.map(refMap).orNull
      case _                                       => null
    }

    if (result.size != 1) {
      log.error(
        s"There are multiple results , the entities contained in these commands "
          + "will not be fully mapped to the persistent entities returned in the result")
      throw new UnexpectedResultSizeException(
        s"${result.size} results cannot be mapped to a single PutApplicationEvent command")
    }

    // We can only correctly match the first PutApplicationEvent as on the server side we merge the sequence and return only one result
    val (cmdsToResults, isEmptyTransaction) = result.head match {
      case err: ErrorResult =>
        if (err.readAfter != null)
          updateLastWitnessedTime(Map(partition -> err.readAfter))
        throw convertDSIException(extractEntityRef(err.error), err.error)
      case VoidResult => Seq.empty -> false // can be returned if some other business event failed with exception
      case r: PutApplicationEventResult => {
        updateLastWitnessedTime(Map(partition -> r.txTime))
        (appEvtCmd.bes zip r.beResults).flatMap { case (busEvt, busEvtResult) =>
          busEvt.puts zip busEvtResult.putResults
        } -> (r.txTime == TimeInterval.NegInfinity)
      }
      case tr: TimestampedResult =>
        updateLastWitnessedTime(Map(partition -> tr.txTime))
        Seq.empty -> false
      // NB: Updating in-memory entity state was a mistake.
      // Let's try to get away with not doing it in the new event API.
      //            putResults foreach { pr =>
      //              val ref = pr.permRef
      //              processPutResult(entityMap(ref), pr)
      //            }
      case _ => throw new IllegalStateException(result.toString)
    }

    if (mutateEntities) {
      mutateEntiesWithAppEventResult(cmdsToResults, cmdToEntity)
      PersistResult(lastWitnessedTime(partition), Map.empty, isEmptyTransaction)
    } else {
      val entityRefs = getEntityToReferenceHolderMap(cmdsToResults, cmdToEntity)
      PersistResult(lastWitnessedTime(partition), entityRefs, isEmptyTransaction)
    }
  }

  def mutateEntiesWithAppEventResult(
      cmdToResult: Seq[(WriteBusinessEvent.Put, PutResult)],
      cmdToEntity: Map[WriteBusinessEvent.Put, Entity]): Unit = {
    cmdToResult.foreach { case (cmd, result) =>
      val entity = cmdToEntity(cmd)
      if (entity.dal$entityRef == null) entity.dal$entityRef = result.permRef
      else assert(entity.dal$entityRef == result.permRef)
      entity.dal$storageInfoUpdate(
        new DSIStorageInfo(result.lockToken, result.vtInterval, result.txTime, result.versionedRef))
    }
  }

  @async private def getEntityToReferenceHolderMap(
      cmdToResult: Seq[(WriteBusinessEvent.Put, PutResult)],
      cmdToEntity: Map[WriteBusinessEvent.Put, Entity]): Map[Entity, EntityReferenceHolder[Entity]] = {
    cmdToResult.apar.flatMap { case (cmd, result) =>
      val entityOpt = cmdToEntity.get(cmd)
      entityOpt.map { entity =>
        if (entity.dal$entityRef != null) assert(entity.dal$entityRef == result.permRef)
        entity -> getReferenceHolderFromResult(entity, result)
      }
    }.toMap
  }

  @async private def getReferenceHolderFromResult(entity: Entity, result: PutResult): EntityReferenceHolder[Entity] = {
    // TODO (OPTIMUS-24843): remove PutResult.txTime dependency
    val temporalContext =
      FlatTemporalContext(DataFreeTemporalSurfaceMatchers.all, result.vtInterval.from, result.txTime, None)
    EntityReferenceHolder.fromJustStoredEntity(
      entity,
      result.permRef,
      temporalContext,
      result.txTime,
      FullVersionInfo(result.versionedRef, result.vtInterval, TimeInterval(result.txTime), result.lockToken)
    )
  }

  // This entire API needs to be transposed to compute all effects to a single entity at one time
  // so that it doesn't have to be done on the DSI side.
  @async private[dal] def createBusinessEventCommand(
      evt: BusinessEvent,
      evtCmid: Option[MSUuid],
      assertValid: Iterable[EntityReference],
      persistedEntities: Iterable[(Entity, Boolean, Option[MSUuid])],
      invalidatedEntities: Iterable[Entity],
      invalidateByRefs: Iterable[(EntityReference, String)],
      revertedEntities: Iterable[Entity],
      entityReferences: mutable.Map[Entity, EntityReference],
      lockTokens: collection.Map[Entity, Long]
  ): (Map[WriteBusinessEvent.Put, Entity], Map[WriteBusinessEvent.PutSlots, Entity], WriteBusinessEvent) = {
    val keyMap = new java.util.HashMap[SerializedKey, (Entity, SerializedEntity)]()

    def checkSerializedKey(entity: Entity, se: SerializedEntity, k: SerializedKey): Unit = {
      if (k.unique) {
        val existing = keyMap.put(k, (entity, se))
        if ((existing ne null) && existing._2.entityRef != se.entityRef) {
          throw new KeyConflictException(existing._1, entity)
        }
      }
      if (k.refFilter)
        require(
          serverFeatures.supports(Feature.SerializeKeyWithRefFilter),
          "Server is not supporting write of non unique indexed with refFilter entity")
    }
    def getLockToken(entity: Entity, upsert: Boolean): Option[Long] =
      if (upsert) None
      else
        entity.dal$storageInfo.lockToken match {
          case lt: Some[Long] => lt
          case None           => lockTokens.get(entity) orElse Some(0)
        }

    val serializedEntities = persistedEntities.aseq.map { case (entity, upsert, cmid) =>
      val se = EntitySerializer.serialize(entity, entityReferences, cmid, true)
      require(se.entityRef ne null, "null entity ref when building WBE command")
      se.keys foreach { key =>
        checkSerializedKey(entity, se.someSlot, key)
      }
      (se, entity, upsert)
    }

    val (persists, multiSlotPersists) = serializedEntities.partition { case (se, _, _) => se.entities.size == 1 }

    val persistRequest = persists.map { case (ser, entity, upsert) =>
      val se = ser.someSlot
      val lockToken = getLockToken(entity, upsert)
      val monoTemporal = entity.$info.monoTemporal
      if (monoTemporal) checkMonoTemporalSupported(serverFeatures, entity.$info.runtimeClass.getName)
      WriteBusinessEvent.Put(se, lockToken, monoTemporal) -> entity
    }.toMap

    val multiSlotPersistRequest = multiSlotPersists.map { case (ses, entity, upsert) =>
      val lockToken = getLockToken(entity, upsert)
      WriteBusinessEvent.PutSlots(ses, lockToken) -> entity
    }.toMap

    val invalidateRequest: Seq[InvalidateRevertProps] = invalidatedEntities.iterator.map { entity =>
      val lt = entity.dal$storageInfo.lockToken getOrElse { throw new UnsavedEntityUpdateException(entity) }
      val monoTemporal = entity.$info.monoTemporal
      if (monoTemporal) checkMonoTemporalSupported(serverFeatures, entity.$info.runtimeClass.getName)
      InvalidateRevertProps(entity.dal$entityRef, lt, entity.getClass.getName, monoTemporal = monoTemporal)
    }.toIndexedSeq

    val invalidateByRefRequest: Seq[InvalidateRevertProps] = invalidateByRefs.iterator.map { case (eref, className) =>
      val info = EntityInfoRegistry.getClassInfo(className)
      val monoTemporal = info.monoTemporal
      if (monoTemporal) checkMonoTemporalSupported(serverFeatures, className)
      InvalidateRevertProps(eref, -1, className, monoTemporal)
    }.toIndexedSeq

    val revertRequest: Seq[InvalidateRevertProps] = revertedEntities.iterator.map { entity =>
      require(!entity.$info.monoTemporal, "Cannot revert a monotemporal entity")
      val lt = entity.dal$storageInfo.lockToken getOrElse { throw new UnsavedEntityUpdateException(entity) }
      InvalidateRevertProps(entity.dal$entityRef, lt, entity.getClass.getName)
    }.toIndexedSeq
    if (revertRequest.nonEmpty)
      require(dsi.serverFeatures().supports(Feature.SupportsRevertOp), "DAL broker doesn't support revert operation")

    val serializedEvt = EventSerializer.serializeBusinessEvent(evt, entityReferences, evtCmid)

    val stateFlag = evt.dal$eventInfo match {
      case LocalDALEventInfo =>
        EventStateFlag.NEW_EVENT
      case DSIAmendEventInfo(_, _) =>
        EventStateFlag.AMEND
      case _ =>
        EventStateFlag.RESTATE
    }

    (
      persistRequest,
      multiSlotPersistRequest,
      WriteBusinessEvent(
        evt = serializedEvt,
        state = stateFlag,
        asserts = assertValid,
        puts = persistRequest.keys.toSeq,
        putSlots = multiSlotPersistRequest.keys.toSeq,
        invalidates = invalidateRequest ++ invalidateByRefRequest,
        reverts = revertRequest
      ))
  }

  def createPersistCommands(
      as: Iterable[(EntityReference, Instant)],
      persists: Iterable[(Entity, Instant, Boolean, Option[MSUuid])],
      invalidates: Iterable[(Entity, Instant)],
      invalidateByRefs: Iterable[(EntityReference, (Instant, String))],
      entityReferences: mutable.Map[Entity, EntityReference],
      lockTokens: collection.Map[Entity, Long],
      minAssignableTtOpt: Option[Instant],
      ignoreListForReferenceResolution: Set[EntityReference]) = {
    val keyMap = new java.util.HashMap[(SerializedKey, Instant), Entity]()
    val requestBuilder = Seq.newBuilder[(Option[Entity], WriteCommand)]

    def checkSerializedKey(entity: Entity, k: SerializedKey, vt: Instant): Unit = {
      if (k.unique) {
        val existing = keyMap.put((k, vt), entity)
        if (existing ne null) {
          throw new KeyConflictException(entity, existing)
        }
      }
      if (k.refFilter)
        require(
          serverFeatures.supports(Feature.SerializeKeyWithRefFilter),
          "Server is not supporting write of non unique indexed with refFilter entity")
    }

    as.groupBy(_._2) foreach { case (vt, es) =>
      requestBuilder += ((None, AssertValid(es map { _._1 }, vt)))
    }

    persists foreach { case (entity, vt, upsert, cmid) =>
      val cmd =
        requestGenerator
          .nonRecursiveGenerateRequestsForPut(
            entity,
            entityReferences,
            lockTokens,
            vt,
            upsert,
            cmid,
            minAssignableTtOpt,
            ignoreListForReferenceResolution.contains(Option(entity.dal$entityRef).getOrElse(entityReferences(entity)))
          )
      cmd.value.keys foreach { key =>
        checkSerializedKey(entity, key, vt)
      }
      requestBuilder += ((Option(entity), cmd))
    }

    invalidates foreach { case (entity, vt) =>
      val cmd = requestGenerator.generateInvalidateAfterRequest(entity, vt)
      requestBuilder += ((Option(entity), cmd))
    }

    invalidateByRefs foreach { case (ref, (vt, className)) =>
      val cmd = requestGenerator.generateInvalidateAfterRequest(ref, vt, className)
      requestBuilder += ((None, cmd))
    }

    requestBuilder.result()
  }

  def commitPersistBlock(cmds: Seq[(Option[Entity], WriteCommand)], mutateEntities: Boolean) = {
    executeSaveCommands(cmds, mutateEntities)
  }

  private def mutateEntityWithPutResult(entity: Entity, pr: PutResult): Unit = {
    // Must be assigned prior to dsi operation as of ages ago.
    if (entity.dal$entityRef == null)
      entity.dal$entityRef = pr.permRef // the eref assignment is changed into post save phase instead of pre save phase
    else assert(entity.dal$entityRef == pr.permRef)
    entity.dal$storageInfoUpdate(new DSIStorageInfo(pr.lockToken, pr.vtInterval, pr.txTime, pr.versionedRef))
  }

  private[this] def convertDSIException[T](entity: Entity, ex: Throwable) = {
    ex match {
      case e: DSIHaltException =>
        new WriteLeaderHaltException(e.getMessage)
      case e: AssertValidException =>
        new EntityNotValidAtValidTimeException(e.getMessage)
      case e: bitemporal.DSISpecificError =>
        new BoundDSIException(entity, e)
      case e: bitemporal.DuplicateStoredKeyException =>
        new DuplicateStoredKeyException(entity, e)
      case e: bitemporal.SecureEntityException =>
        new SecureEntityException(entity, e)
      case e: bitemporal.DuplicateUniqueIndexException =>
        new DuplicateUniqueIndexException(entity, e)
      case e: bitemporal.OutdatedVersionException =>
        log.error("OutdatedVersionException: entity reference from DSI is {}, found entity {}", e.ref, entity)
        throw new OutdatedVersionException(entity, e)
      // If EntityNotFoundException is thrown from the DSI, it really means
      // there was an problem finding an entity that internally was expected to exist
      /*case e: bitemporal.EntityNotFoundException =>
        throw new EntityNotFoundException(e)*/
      case e: bitemporal.DuplicateAppEventReferenceException =>
        log.warn(s"Got duplicate appevent exception with message ${e.getMessage}")
        e
      case e: bitemporal.DSIException =>
        new BoundDSIException(entity, e)
      case e => e
    }
  }

  private def postSaveProcessing(
      requests: Iterable[(Option[Entity], Command)],
      results: Iterable[Result],
      mutateEntities: Boolean,
      partition: Partition): Map[Entity, EntityReferenceHolder[Entity]] = {
    val entityToRef = Map.newBuilder[Entity, EntityReferenceHolder[Entity]]
    val errors = Collections.flatMap2(requests, results) { case ((entity, _), res) =>
      res match {
        case AssertValidResult() => None
        // TODO (OPTIMUS-24843): remove PutResult.txTime dependency
        case pr: PutResult =>
          entity.foreach { e =>
            if (mutateEntities) mutateEntityWithPutResult(e, pr)
            else entityToRef.addOne(e, getReferenceHolderFromResult(e, pr))
          }
          updateLastWitnessedTime(Map(partition -> pr.txTime))
          None
        case tsr: TimestampedResult =>
          updateLastWitnessedTime(Map(partition -> tsr.txTime))
          None
        case ErrorResult(error, readAfter) =>
          if (readAfter != null)
            updateLastWitnessedTime(Map(partition -> readAfter))
          Some((entity, error))
        case VoidResult => None
        case other      => Some((entity, new GeneralDALException("Unexpected result type " + other)))
      }
    }

    // We may get back any number of ErrorResults from the DSI.  Throw the first one arbitrarily.
    errors.headOption foreach { case (entity, ex) => throw convertDSIException(entity.orNull, ex) }
    entityToRef.result()
  }

  @async def invalidateCurrentInstances[E <: Entity: Manifest](): Int = {
    val className = implicitly[Manifest[E]].runtimeClass.getName
    invalidateCurrentInstances(className)
  }

  @async def invalidateCurrentInstances(className: String): Int = {
    val partition = resolvePartitionForCmds(Seq(InvalidateAllCurrent(className, None)), partitionMap)
    val now = atNow(Set(partition), false).get(partition).get
    val c = count(EntityClassQuery(className), DSIQueryTemporality.TxTime(now))
    val cmd = InvalidateAllCurrent(className, Some(c))
    wrapQueryDSIException {
      dsi.execute(cmd) match {
        case Seq(InvalidateAllCurrentResult(count, txTimeOpt)) =>
          txTimeOpt.foreach(tx => updateLastWitnessedTime(Map(partition -> tx))); count
        case Seq(err: ErrorResult, _*) => throwErrorResult(err, partition)
        case Seq(other, _*)            => throw new GeneralDALException("Unexpected result type " + other)
      }
    }
  }

  def invalidateCurrentInstances[E <: Entity: Manifest](refs: Seq[EntityReference]): Int =
    invalidateCurrentInstances(refs, implicitly[Manifest[E]].runtimeClass.getName)

  def invalidateCurrentInstances(refs: Seq[EntityReference], className: String): Int = {
    val cmd = InvalidateAllCurrentByRefs(refs, className)
    val partition = resolvePartitionForCmds(Seq(cmd), partitionMap)
    wrapQueryDSIException {
      dsi.execute(cmd) match {
        case Seq(InvalidateAllCurrentResult(count, Some(txTime))) =>
          updateLastWitnessedTime(Map(partition -> txTime)); count
        case Seq(InvalidateAllCurrentResult(count, None)) =>
          log.warn("InvalidateAllCurrentResult txTime is None. Not updating last witnessed time")
          count
        case Seq(err: ErrorResult, _*) => throwErrorResult(err, partition)
        case Seq(other, _*)            => throw new GeneralDALException("Unexpected result type " + other)
      }
    }
  }

  def prepareMonoTemporal[E <: Entity: Manifest](refs: Seq[EntityReference]): Int = {
    prepareMonoTemporal(refs, implicitly[Manifest[E]].runtimeClass.getName)
  }

  def prepareMonoTemporal(refs: Seq[EntityReference], className: String): Int = {
    checkMonoTemporalSupported(serverFeatures, className)

    val cmd = PrepareMonoTemporal(refs, className)
    val partition = resolvePartitionForCmds(Seq(cmd), partitionMap)
    wrapQueryDSIException {
      dsi.execute(cmd) match {
        case Seq(PrepareMonoTemporalResult(count, Some(txTime))) =>
          updateLastWitnessedTime(Map(partition -> txTime)); count
        case Seq(PrepareMonoTemporalResult(count, None)) =>
          log.warn("PrepareMonoTemporalResult txTime is None. Not updating last witnessed time")
          count
        case Seq(err: ErrorResult, _*) => throwErrorResult(err, partition)
        case Seq(other, _*)            => throw new GeneralDALException("Unexpected result type " + other)
      }
    }
  }

  private def validatePurge() = {
    val env = DalEnv(EvaluationContext.env.config.runtimeConfig.env)
    val runtimeEnv = DALEnvs.getRuntimeEnvforDALEnv(env)
    runtimeEnv match {
      case RuntimeEnvironmentEnum.dev | RuntimeEnvironmentEnum.test =>
      case _ =>
        throw new InvalidPurgeOperationException(s"Cannot purge context in ${DALEnvs.getRuntimeEnvforDALEnv(
            env)}. Can purge only in ${RuntimeEnvironmentEnum.dev} or ${RuntimeEnvironmentEnum.test}")
    }
    val isInMemoryDAL = EvaluationContext.env.config.runtimeConfig
      .getString(RuntimeProperties.DsiUriProperty)
      .map(_.startsWith("memory://"))
      .getOrElse(false)
    if (!isInMemoryDAL) {
      dsi.baseContext match {
        case n: NamedContext  =>
        case u: UniqueContext =>
        case ctx              => throw new InvalidPurgeOperationException(s"Cannot purge context in ${ctx}")
      }
    }
  }

  def purgePrivateContext(): Unit = {
    validatePurge()
    val errorMessages = wrapQueryDSIException { dsi.execute(SystemCommand("delete_all")) } flatMap { result =>
      result match {
        case SystemCommandResult(message) =>
          log.warn("Purge: not invalidating caches, loading previously-loaded entities will still return results.")
          initLastWitnessedTimeMap() // Clear witnessed times on dal client
          if (!message.contains("OK")) {
            log.error(s"Database purge returned unexpected result ${message}")
            Some(message)
          } else None

        case err: ErrorResult =>
          log.error(s"Database purge returned error result ${err}")
          Some(err.error.getMessage)

        case other =>
          log.error(s"Database purged returned unknown error ${other}")
          Some(s"Unexpected error found ${other}")
      }
    }

    if (errorMessages.nonEmpty) throw new GeneralDALException(errorMessages.mkString("\n"))
  }

  def executeAccMetadataCommand(cmd: AccMetadataCommand) = {
    val cmds = Seq(cmd)
    val partition = resolvePartitionForCmds(cmds, partitionMap)
    wrapQueryDSIException {
      executor.executeLeadWriterCommands(dsi, cmds)
    } match {
      case Seq(AccTableResult(clsName, slot)) =>
        log.info(s"successfully ${cmd.action} table for $clsName $slot")
      case Seq(err: ErrorResult, _*) => throwErrorResult(err, partition)
      case Seq(other, _*)            => throw new GeneralDALException("Unexpected result type " + other)
    }
  }

  def assignTemporaryReferences(
      entities: Map[Entity, (Boolean, Option[MSUuid])],
      initialRefs: sc.Map[Entity, EntityReference]): Map[Entity, EntityReference] = {
    def hasKey(e: Entity) = e.$info.asInstanceOf[ClassEntityInfo].indexes.exists { idx =>
      idx.default && idx.unique && !idx.indexed
    }

    entities.map { case (entity, (upsert, cmid)) =>
      val ref =
        if (entity.dal$entityRef ne null) entity.dal$entityRef
        else
          initialRefs.getOrElse(entity, EntityReference.freshTemporary)
      (entity, ref)
    }
  }

  private def executeObliterate(q: Query): Unit =
    executeObliterates(Seq(Obliterate(q)))

  private def executeObliterates(cmds: Seq[Obliterate]): Unit = {
    val partition = resolvePartitionForCmds(cmds, partitionMap)
    val res = dsi.executeLeadWriterCommands(cmds)
    val errors = res.flatMap {
      case VoidResult     => None
      case e: ErrorResult => Some(e)
      case o => Some(ErrorResult(new GeneralDALException(s"Unexpected DSI result type ${o.getClass.getName}")))
    }
    if (errors.nonEmpty) throwErrorResults(errors, partition)
  }

  private[optimus] def obliterateEntityByClassName(className: String): Unit = {
    val q = EntityClassQuery(className)
    executeObliterate(q)
  }

  override def obliterateClass[E <: Entity: Manifest](): Unit = {
    val klass = implicitly[Manifest[E]].runtimeClass
    obliterateEntityByClassName(klass.getName)
  }

  override def obliterateEventClass[E <: BusinessEvent: Manifest](): Unit = {
    val klass = implicitly[Manifest[E]].runtimeClass
    val q = new EventClassQuery(klass.getName)
    executeObliterate(q)
  }

  override def obliterateEntity(key: Key[_]): Unit = {
    val q = new SerializedKeyQuery(key.toSerializedKey)
    executeObliterate(q)
  }

  override def obliterateEntities(refs: Seq[EntityReference], classNameOpt: Option[String] = None): Unit = {
    val qs = classNameOpt map { cn =>
      refs.map(ReferenceQuery.apply(_, cn))
    } getOrElse (refs.map(ReferenceQuery.apply))
    val os = qs.map(Obliterate.apply)
    val oblQuery = Obliterate.lift(os)
    executeObliterates(Seq(oblQuery))
  }

  override def obliterateEntityForClass(ref: EntityReference, clazzName: String): Unit = {
    require(
      clazzName != null && clazzName.nonEmpty,
      s"Classname is expected to be defined when calling obliterateEntityForClass API. Got classname ${clazzName}")
    val q = new ReferenceQuery(ref, false, Some(clazzName))
    executeObliterate(q)
  }

  override def obliterateEvent(key: Key[_]): Unit = {
    val q = new EventSerializedKeyQuery(key.toSerializedKey)
    executeObliterate(q)
  }

  override def obliterateEvents(refs: Seq[BusinessEventReference], classNameOpt: Option[String] = None): Unit = {
    val qs = refs.map(r =>
      new EventReferenceQuery(r) {
        override val className: Option[String] = classNameOpt
      })
    val os = qs.map(Obliterate.apply)
    val oblQuery = Obliterate.lift(os)
    executeObliterates(Seq(oblQuery))
  }

  override def obliterateEvent(ref: BusinessEventReference): Unit = {
    val q = new EventReferenceQuery(ref)
    executeObliterate(q)
  }

  override def obliterateEventForClass(ref: BusinessEventReference, clazzName: String): Unit = {
    require(ref.isInstanceOf[TypedReference], "require typed reference")
    require(
      clazzName != null && clazzName.nonEmpty,
      s"Classname is expected to be defined when calling obliterateEventForClass API. Got classname ${clazzName}")
    val q = new EventReferenceQuery(ref) {
      override val className: Option[String] = Some(clazzName)
    }
    executeObliterate(q)
  }

  @deprecating("To be replaced by classpath registration")
  private[optimus] def createSchemaVersions(className: SerializedEntity.TypeRef, schemaVersions: Set[Int]) = {
    val cmd = CreateSlots(className, schemaVersions)
    val res = dsi.execute(cmd)
    val partition = resolvePartitionForCmds(Seq(cmd), partitionMap)
    res match {
      case Seq(err: ErrorResult, _*)                    => throwErrorResult(err, partition)
      case Seq(CreateSlotsResult(alreadyExistingSlots)) => alreadyExistingSlots
      case o => throw new GeneralDALException("Unexpected DSI result type " + o.getClass.getName)
    }
  }

  private[optimus] def populateSlot(
      className: SerializedEntity.TypeRef,
      entityReference: EntityReference,
      newVersions: Seq[(VersionedReference, SerializedEntity)]) = {
    val cmd = FillSlot(className, entityReference, newVersions)
    val res = dsi.execute(cmd)
    val partition = resolvePartitionForCmds(Seq(cmd), partitionMap)
    res match {
      case Seq(err: ErrorResult, _*) => throwErrorResult(err, partition)
      case Seq(VoidResult)           =>
      case o => throw new GeneralDALException("Unexpected DSI result type " + o.getClass.getName)
    }
  }
}

private class InvalidPurgeOperationException(val msg: String) extends Exception(msg)
private class UnexpectedResultSizeException(val msg: String) extends Exception(msg)
