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

import java.net.URI
import java.util.concurrent.atomic.AtomicReference
import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicBoolean
import msjava.protobufutils.server.BackendException
import optimus.dsi.session.{EstablishSession, EstablishedClientSession}
import optimus.platform._
import optimus.platform.dsi.protobufutils.VersioningServerManager
import optimus.platform.dsi.versioning.VersioningRedirectionInfo
import optimus.platform.dsi.{DSIResponse, Response}
import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.protobufutils.DalBrokerClient
import optimus.platform.dsi.bitemporal.proto.Dsi.DSIRequestProto

import scala.util.Random
import optimus.platform.dal.session.ClientSessionContext.SessionData

import scala.annotation.tailrec
import optimus.dsi.base.extractReadTxTime
import optimus.dsi.session.EstablishSessionFailure
import optimus.dsi.session.EstablishSessionSuccess
import optimus.graph.DiagnosticSettings
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.pubsub.AbstractPubSubStreamRetryManager
import optimus.platform.dal.pubsub.HandlePubSub
import optimus.platform.dal.pubsub.PubSubClientRequest
import optimus.platform.dal.pubsub.PubSubClientResponse
import optimus.platform.dal.session.ClientSessionContext
import optimus.platform.dal.session.RolesetMode.SpecificRoleset
import optimus.platform.dal.session.RolesetMode.UseLegacyEntitlements
import optimus.platform.dsi.Feature.SupportCreateNewSession
import optimus.platform.dsi.protobufutils.BatchContext
import optimus.platform.dsi.protobufutils.BatchRetryManager
import optimus.platform.dsi.protobufutils.DebugZeroResult
import optimus.platform.dsi.protobufutils.SharedRequestLimiter
import optimus.platform.runtime.BrokerProviderResolver
import optimus.platform.runtime.OptimusCompositeLeaderElectorClient
import optimus.platform.runtime.UriUtils

import scala.util.Try
import scala.util.control.NonFatal

object DSIClient {
  private sealed trait RetryBehaviour
  private final case class DoNotRetry(t: Throwable) extends RetryBehaviour
  private final case class ConsiderRetry(ex: Exception, client: Option[DalBrokerClient]) extends RetryBehaviour
  private final case class NoRetryNeeded(res: Seq[Result]) extends RetryBehaviour

  private val defaultSupportUnavailablePartitions =
    DiagnosticSettings.getBoolProperty("optimus.platform.dal.client.supportUnavailablePartitions", true)
  private val _supportUnavailablePartitions = new AtomicBoolean(defaultSupportUnavailablePartitions)
  private[optimus] def setSupportUnavailablePartitions(value: Boolean) = _supportUnavailablePartitions.set(value)
  private[optimus] def resetSupportUnavailablePartitions =
    _supportUnavailablePartitions.set(defaultSupportUnavailablePartitions)
  def supportUnavailablePartitions = _supportUnavailablePartitions.get

  private[optimus] def getUnavailablePartitionsFromReplica(
      brokerProviderResolver: BrokerProviderResolver,
      replica: String): Set[String] = {
    if (supportUnavailablePartitions) {
      // we get the leader read broker's uriString
      val uriString = Some(brokerProviderResolver.resolve(replica))
        .collect({ case client: OptimusCompositeLeaderElectorClient => client.getWriteBrokerRaw })
        .flatten
      val uri = new URI(uriString.getOrElse(throw new DALRetryableActionException("no broker registered!", None)))
      UriUtils.getQueryMap(uri).get("unavailablePartitions").map(_.split('+').toSet).getOrElse(Set.empty)
    } else {
      Set.empty
    }
  }
}

abstract class DSIClient(
    brokerCtx: ClientBrokerContext,
    toleranceToNowMinutes: Int = DSIClientCommon.toleranceToNowMinutes,
    failReadsTooCloseToAtNow: Boolean = DSIClientCommon.failReadsTooCloseToAtNow,
    disableBatchRetry: Boolean = DSIClientCommon.DisableBatchRetry,
    protected val writeRequestLimiter: Option[SharedRequestLimiter] = SharedRequestLimiter.write)
    extends ClientSideDSI { outer =>
  import DSIClientCommon._

  protected val requestLimiter = new ClientRequestLimiter(brokerCtx.cmdLimit)

  protected val retryAttempts: Int = getRetryAttempts
  protected val retryBackoffTime: Long = getRetryBackoffTime
  protected def getRetryAttempts: Int = DSIClientCommon.getMaxRetryAttempts
  protected def getRetryBackoffTime: Long = DSIClientCommon.getBackoffTimeInterval
  protected def getRetryCheckSessionState: Boolean = DSIClientCommon.getRetryCheckSessionState

  // lazy to override in tests
  protected lazy val batchRetryManager: Option[BatchRetryManager] =
    if (disableBatchRetry) None else Some(new DsiClientBatchRetryManager(retryAttempts, retryBackoffTime))
  protected[optimus] val sessionCtx = createSessionContext
  protected val pubSubRetryManager = getPubSubRetryManager(retryAttempts, retryBackoffTime)
  protected def createSessionContext: ClientSessionContext = new ClientSessionContext
  protected[optimus] def serverFeatures() = {
    val existingSession = sessionCtx.sessionData.existingSession.orElse {
      // Call GetInfo to establish a new session
      execute(GetInfo())
      sessionCtx.sessionData.existingSession
    }
    require(existingSession.isDefined)
    existingSession.get.serverFeatures
  }

  private[this] val recentCauseOfException: AtomicReference[Throwable] = new AtomicReference()

  override def baseContext: Context = brokerCtx.context
  override def sessionData: SessionData = sessionCtx.sessionData

  // TODO (OPTIMUS-13424): should be protected, however dal_browser UserBoundDSI is massively violating encapsulation here
  private[optimus] def getSender(): DalBrokerClient

  protected def getPubSubRetryManager(
      retryAttempts: Int,
      retryBackoffTime: Long): Option[DsiClientPubSubStreamRetryManager] = None

  /* This is part of the state of the client. It must be guarded against concurrent modification, that's why
  we have all this hassle with the lifecycle methods (see below where this guy is modified). */
  private[this] val senderRef: AtomicReference[DalBrokerClient] = new AtomicReference()

  @tailrec
  private[this] def getDSIRequestSender(attempt: Int = 0): DalBrokerClient = {
    val sender = senderRef.get()
    if (sender ne null) {
      sender
    } else {
      if (canSessionCtxBeUsedWithNewSender) {
        val newSender = getSender()
        if (senderRef.compareAndSet(null, newSender)) {
          newSender
        } else {
          newSender.shutdown(Some(DalBrokerClient.ShutdownCause.DuplicateCreation))
          getDSIRequestSender()
        }
      } else {
        val sleep = 100L << Math.min(8, attempt) // max wait 25600ms
        log.info(s"Session context cannot be used by new sender. Will retry after $sleep ms.")
        onRetryGetDsiRequestSender(attempt + 1)
        Thread.sleep(sleep)
        getDSIRequestSender(attempt + 1)
      }
    }
  }

  protected def onRetryGetDsiRequestSender(attempt: Int): Unit = {}

  private[optimus] def getCurrentSender(): DalBrokerClient = {
    getDSIRequestSender()
  }

  protected def canSessionCtxBeUsedWithNewSender: Boolean = {
    !getRetryCheckSessionState || sessionCtx.isNotInitialized
  }

  private[this] val versioningServerManager = new VersioningServerManager(brokerCtx)

  protected def beforeProtoShutdown(): Unit = {} // test hook

  /*
   * One request threw, need to do some closing. Definitely close the protoclient that threw, but cleanup this DSIClient only
   * if it hasn't been done already
   */
  private[platform] final def close(failedProtoClient: Option[DalBrokerClient]): Unit =
    this.synchronized {
      val prefix = s"close $failedProtoClient -"
      if (senderRef.compareAndSet(failedProtoClient getOrElse null, null)) {
        log.info(s"$prefix this is the current protoclient, resetting so it can be recreated")
        beforeProtoShutdown()
        failedProtoClient.foreach(_.shutdown(None))
        sessionCtx.connectionClosed()
      } else {
        log.debug(s"$prefix there's already a new protoclient (${senderRef.get}), nothing to do")
      }
    }

  /*
   * A regular close triggered from the outside. Close the proto if we have one (or if it's being created), then nullify.
   */
  override def close(shutdown: Boolean): Unit = this.synchronized {
    val sender = senderRef.getAndSet(null)
    if (sender != null) {
      log.info(s"close - closing $sender")
      try {
        sender.shutdown(None)
      } catch {
        case NonFatal(ex) =>
          log.error(s"Error when closing $sender.", ex)
      }
    } else {
      log.info("close - no underlying protoclient")
    }
    sessionCtx.connectionClosed()
    // should only be done when the DSIClient will no longer be used
    if (shutdown) {
      versioningServerManager.shutdown()
      batchRetryManager foreach { _.shutdown() }
      pubSubRetryManager.foreach { _.closeStreamManager() }
    }
  }

  // @tailrec
  private[this] def shouldRetry(t: Throwable, requestType: DSIRequestProto.Type): Boolean = ??? /* {
    val isRetryable = t.isInstanceOf[DALRetryableActionException] ||
      (requestType == DSIRequestProto.Type.WRITE && t.isInstanceOf[NonLeaderBrokerException]) ||
      (requestType == DSIRequestProto.Type.READ_ONLY && t.isInstanceOf[DsiSpecificTransientError])
    // has to be written like this so the recursive call is in tail position
    if (isRetryable) true
    else (t ne null) && shouldRetry(t.getCause, requestType)
  } */

  private[this] def checkErrorResults(results: Seq[Result]): Unit = {
    import optimus.platform.dsi.bitemporal.ErrorResult
    results foreach {
      case ErrorResult(error: NonLeaderBrokerException, _)  => throw new NonLeaderBrokerException(error.getMessage)
      case ErrorResult(error: NonReaderBrokerException, _)  => throw new NonReaderBrokerException(error.getMessage)
      case ErrorResult(error: DsiSpecificTransientError, _) => throw new DsiSpecificTransientError(error.getMessage)
      case _                                                => // do nothing
    }
  }

  @async private final def executeCommandsWithRetry(
      cmds: Seq[Command],
      requestType: DSIRequestProto.Type,
      hasRetried: Boolean,
      attempts: Int,
      backoff: Long,
      separateWriteBatcher: Boolean): Seq[Result] = ??? /* {
    import DSIClient._

    var response: Response = null

    val res = asyncResult(EvaluationContext.cancelScope) {
      val currentSender = getDSIRequestSender()
      response = currentSender.request(cmds, requestType, sessionCtx, separateWriteBatcher = separateWriteBatcher)
      checkErrorResults(response.results)
      if (DebugZeroResult.value) {
        log.debug(
          s"debug 0 result-3 response.results.size:${response.results.size} type:${response.results.map(_.getClass.getCanonicalName).toSet.mkString(":")}")
      }

      if (ServerSideVersioning) {
        response = {
          val versionedCmds = response.results.flatMap {
            case errorResult: ErrorResult if errorResult.error.isInstanceOf[IncompleteWriteRequestException] =>
              val error = errorResult.error.asInstanceOf[IncompleteWriteRequestException]

              val impactedCommands = computeRedirectionImpact(error.redirectionInfo, cmds)
              if (impactedCommands.isEmpty)
                throw new InvalidRedirectionInfoException

              versioningServerManager.version(sessionCtx, impactedCommands, requestType, error.redirectionInfo)

            case validResult => Seq.empty[Command]
          }

          if (versionedCmds.isEmpty)
            response
          else {
            val nonVersionableResults = response.results.filterNot {
              case errorResult: ErrorResult if errorResult.error.isInstanceOf[IncompleteWriteRequestException] =>
                true
              case _ => false
            }
            val versionedResults =
              currentSender
                .request(versionedCmds, requestType, sessionCtx, separateWriteBatcher = separateWriteBatcher)
                .results
            DSIResponse(nonVersionableResults ++ versionedResults, response.establishSessionResult)
          }
        }
      }

      if (hasRetried) DALEventMulticaster.dalRecoveryCallback(createDALRecoveryEvent(response.client))
    }

    val retryBehaviour = if (res.hasException) res.exception match {
      case ex: DALRetryableActionException    => ConsiderRetry(ex, ex.client)
      case ex: DALNonRetryableActionException =>
        // we check for cause first, in case this was wrapped by BatchRetryManager
        val t = Option(ex.getCause) getOrElse ex
        DoNotRetry(t)
      case ex: Exception => ConsiderRetry(ex, Option(response) flatMap (_.client))
      case t             => DoNotRetry(t)
    }
    else NoRetryNeeded(DSIClientCommon.collate(response.results))

    retryBehaviour match {
      case DoNotRetry(t)      => throw t
      case NoRetryNeeded(ret) => ret
      case ConsiderRetry(ex, currentSender) =>
        val duplicateCause = seenSameCause(ex)
        if (!duplicateCause) {
          log.warn(s"DAL client got exception: ${ex.getClass.getName} - ${ex.getMessage}")
          log.debug("Stack trace:", ex)
        }
        if (shouldRetry(ex, requestType)) {
          if (attempts > 0) {
            if (!duplicateCause) log.warn(s"DAL client retry, attempts left = $attempts, retrying in: ${backoff}ms")
            DALEventMulticaster.dalRetryCallback(createDALRetryEvent(ex))
            close(currentSender)
            delay(backoff)
            executeCommandsWithRetry(
              cmds,
              requestType,
              hasRetried = true,
              attempts = attempts - 1,
              backoff = boundedBackOffTime(backoff * 2),
              separateWriteBatcher = separateWriteBatcher)
          } else {
            if (!duplicateCause) log.warn("Out of retries", ex)
            throw ex
          }
        } else {
          if (!duplicateCause) ex match {
            case _: BackendException | _: DalTimedoutException if requestType == DSIRequestProto.Type.WRITE =>
              log.warn(
                "This exception is not retryable at this point. It might be retried later for PutApplicationEvents")
            case _ => log.warn("This exception is not retryable", ex)
          }
          throw ex
        }

    }
  } */

  @async private final def executeCommands(
      cmds: Seq[Command],
      requestType: DSIRequestProto.Type,
      separateWriteBatcher: Boolean = true): Seq[Result] = {
    val randomNum = new Random(System.currentTimeMillis)
    val backoff = retryBackoffTime + math.abs(randomNum.nextLong() % retryBackoffTime)
    executeCommandsWithRetry(
      cmds,
      requestType,
      hasRetried = false,
      attempts = retryAttempts,
      backoff = backoff,
      separateWriteBatcher = separateWriteBatcher)
  }

  private def computeRedirectionImpact(
      redirectionInfo: VersioningRedirectionInfo,
      commands: Seq[Command]): Seq[LeadWriterCommand] = {
    val impactedCommands: Seq[LeadWriterCommand] = commands.flatMap {
      case put: Put => throw new UnversionableCommandException(put)

      case pae: PutApplicationEvent =>
        val impactedBusinessEvents = pae.bes.flatMap { be =>
          val impactedPuts = be.puts.filter(p => redirectionInfo.impacts(p.ent.className))
          val impactedPutSlots = be.putSlots.filter(ps => redirectionInfo.impacts(ps.ents.className))
          if (impactedPuts.nonEmpty || impactedPutSlots.nonEmpty) {
            val copyPuts = impactedPuts.nonEmpty && impactedPuts.size != be.puts.size
            val copyPutSlots = impactedPutSlots.nonEmpty && impactedPutSlots.size != be.putSlots.size
            (copyPuts, copyPutSlots) match {
              case (false, false) => Some(be)
              case (false, true)  => Some(be.copy(putSlots = impactedPutSlots))
              case (true, false)  => Some(be.copy(puts = impactedPuts))
              case (true, true)   => Some(be.copy(puts = impactedPuts, putSlots = impactedPutSlots))
            }
          } else if (redirectionInfo.impacts(be.evt.className))
            Some(be)
          else
            None
        }

        if (impactedBusinessEvents.nonEmpty) {
          if (impactedBusinessEvents.size != pae.bes.size) Some(pae.copy(bes = impactedBusinessEvents)) else Some(pae)
        } else None

      case gae: GeneratedAppEvent =>
        val impactedPuts = gae.puts.filter(p =>
          redirectionInfo.impacts(p.className().getOrElse(throw new UnversionableCommandException(p))))
        impactedPuts

      case cmd => None
    }

    impactedCommands
  }

  @tailrec
  private[this] def seenSameCause(exception: Throwable): Boolean = {
    val cause = exception.getCause
    if ((cause eq null) || (cause eq exception)) {
      val pastCause = recentCauseOfException.getAndSet(exception)
      exception eq pastCause
    } else {
      seenSameCause(cause)
    }
  }

  @async final override def executeLeadWriterCommands(cmds: Seq[LeadWriterCommand]): Seq[Result] =
    ??? // executeCommands(cmds, DSIRequestProto.Type.WRITE)

  @async final override def executeServiceDiscoveryCommands(sds: Seq[ServiceDiscoveryCommand]): Seq[Result] =
    ??? // executeCommands(sds, DSIRequestProto.Type.SERVICE_DISCOVERY)

  @async final override def executeReadOnlyCommands(cmds: Seq[ReadOnlyCommand]): Seq[Result] = ??? /* {

    // Use wall-clock time to estimate At.now, without actually calling DAL broker.
    val wallClock = patch.MilliInstant.now
    val toleranceToNow: Option[Duration] =
      if (toleranceToNowMinutes > 0) Some(Duration.ofMinutes(toleranceToNowMinutes)) else None
    val latest = toleranceToNow map { wallClock.minus }

    def reject(cmd: ReadOnlyCommand, readTT: Instant, f: String => _) = {
      val nodeStack = EvaluationContext.currentNode.waitersToFullMultilineNodeStack()
      val msg = s"Read command txtime too close to now (${wallClock}): TxTime = ${readTT} Command: ${cmd}"
      f(s"${msg}, see log for node stack")
      log.info(s"${msg} - node stack :\n$nodeStack")
    }
    val currEnvConfig = EvaluationContext.env.config
    val isMockDal = currEnvConfig != null &&
      (currEnvConfig.runtimeConfig.env == "mock" && currEnvConfig.runtimeConfig.mode == "mock")

    val toExecute = cmds filter { cmd =>
      Try(extractReadTxTime(cmd)).map(tt => (tt, latest)) map {
        case (TimeInterval.Infinity, _) => false // never allow to read at +INF
        case (_, None)                  => true // no limit, allow all reads
        case (finiteTime, Some(latestAllowed)) if finiteTime.isBefore(latestAllowed) =>
          true // read far enough from now, ok
        case (timeTooCloseToAtNow, _) if !failReadsTooCloseToAtNow =>
          if (!isMockDal)
            reject(cmd, timeTooCloseToAtNow, println) // stdout usage intended, works with CI stdout plugin
          true // too close but we're just logging so allow execution
        case (timeTooCloseToAtNow, _) =>
          isMockDal // too close and we reject unless this is mock DAL
      } getOrElse true // fail-safe: could not determine read tt
    }

    val results = if (toExecute.nonEmpty) executeCommands(toExecute, DSIRequestProto.Type.READ_ONLY) else Nil

    (results.size - toExecute.size) match {
      case a if a > 0 => {
        log.error(s"The DAL broker returned ${a} more results than expected")
        throw new UnexpectedResultException
      }
      case b if b < 0 => log.error(s"The DAL broker returned ${-b} fewer results than expected")
      case _          => log.debug(s"The DAL broker returned ${results.size} results, as expected")
    }

    val correlation = (toExecute zipAll (results, VoidCommand, ErrorResult(new MissingResultException))).toMap
    cmds map { cmd =>
      correlation.getOrElse(
        cmd, {
          val tt = extractReadTxTime(cmd)
          reject(cmd, tt, log.error(_: String))
          toleranceToNow map { tolerance =>
            ErrorResult(new ReadAtApproximateNowException(cmd, tt, wallClock, tolerance))
          } getOrElse {
            ErrorResult(
              new ReadFarAheadOfLsqtException(
                "Unable to read data as max requested time is +infinity, too far ahead of latest safe query time."))
          }
        }
      )
    }
  } */

  override def executePubSubRequest(request: PubSubClientRequest): PubSubClientResponse = {
    require(pubSubRetryManager.isDefined, s"pubSubRetryMgr is not defined, but received: $request")
    pubSubRetryManager.get.executePubSubRequest(request)
  }

  protected def createDALRetryEvent(t: Exception): DALRetryEvent = DALClientRetryEvent(t)
  protected def createDALRecoveryEvent(client: Option[DalBrokerClient]): DALRecoveryEvent =
    DALRecoveryEvent("Connect to " + client.map(_.connectionString).getOrElse("<unknown>"))

  private[optimus] override def setEstablishSession(command: => EstablishSession): Unit =
    sessionCtx.setEstablishSession(command)
  private[optimus] override def bindSessionFetcher(sessionFetcher: SessionFetcher): Unit = {
    sessionCtx.bindSessionFetcher(sessionFetcher)
  }

  private[optimus] def connectionSession(): Option[EstablishedClientSession] = sessionCtx.connectionSession()

  protected class DsiClientBatchRetryManager(override val maxRetryAttempts: Int, override val retryBackoffTime: Long)
      extends BatchRetryManager {
    override protected def shouldRetry(t: Throwable, requestType: DSIRequestProto.Type): Boolean =
      outer.shouldRetry(t, requestType)
    override protected def sendRetryBatch(newBatch: BatchContext): Unit = {
      val currentSender = outer.getDSIRequestSender()
      currentSender.sendBatch(newBatch)
    }
    override def raiseDALRetryEvent(ex: Exception): Unit =
      DALEventMulticaster.dalRetryCallback(outer.createDALRetryEvent(ex))
    override def raiseDALRecoveryEvent(client: Option[DalBrokerClient]): Unit =
      DALEventMulticaster.dalRecoveryCallback(outer.createDALRecoveryEvent(client))
    override protected def close(client: Option[DalBrokerClient], batch: BatchContext): Unit = outer.close(client)
  }

  protected[optimus] class DsiClientPubSubStreamRetryManager(maxRetryAttempts: Int, retryBackoffTime: Long)
      extends AbstractPubSubStreamRetryManager(maxRetryAttempts, retryBackoffTime, outer.sessionCtx) {
    override def closeSender(failed: Option[DalBrokerClient]): Unit = outer.close(failed)
    override def closeCurrentSender(): Unit = outer.close(shutdown = false)
    override def getCurrentSender: DalBrokerClient = outer.getDSIRequestSender()
  }
}

trait ClientSideDSI extends DSI with HandlePubSub {
  import DSIClientCommon.log
  @async override def executeReadOnlyCommands(reads: Seq[ReadOnlyCommand]): Seq[Result]
  @async override def executeLeadWriterCommands(cmds: Seq[LeadWriterCommand]): Seq[Result]

  @async override def executeMessagesCommands(cmds: Seq[MessagesCommand]): Seq[MessagesResult] =
    throw new UnsupportedOperationException(s"Messages commands are not supported!")

  @async def executeServiceDiscoveryCommands(sds: Seq[ServiceDiscoveryCommand]): Seq[Result] =
    throw new ServiceDiscoveryUnsupportedException("Cannot execute service discovery commands against this DSI")

  override def executePubSubRequest(request: PubSubClientRequest): PubSubClientResponse = {
    val streamId = request.cmd.streamId
    throw new PubSubCommandUnsupportedException(streamId, request.toString)
  }

  @async def executeAccCommands(reads: Seq[ReadOnlyCommand]): Seq[Result] = {
    executeReadOnlyCommands(reads)
  }

  def sessionData: SessionData
  private[optimus] def bindSessionFetcher(sessionFetcher: SessionFetcher): Unit

  @async override private[optimus] final def getSession(tryEstablish: Boolean = true): EstablishedClientSession = {

    sessionData match {
      // uninitialized
      case SessionData(None, None, _) => throw new IllegalStateException("DAL session has not been initialized")

      // initialized
      case SessionData(Some(session), None, _) => session

      // pending establish or pending reestablish
      case SessionData(_, Some(_), _) =>
        if (!tryEstablish)
          throw new IllegalStateException("Could not establish a pending establish or pending reestablish DAL session")

        // issue read query to DAL to establish or reestablish a session
        executeReadOnlyCommands(GetInfo() :: Nil)
        // retry
        getSession(tryEstablish = false)
    }
  }

  @async override private[optimus] final def createNewSession(roles: Set[String]): EstablishedClientSession = {
    val createNewSessionSupported = serverFeatures().supports(SupportCreateNewSession)
    if (createNewSessionSupported) {
      val existingSession = getSession()
      val establishedSessionRoles = existingSession.rolesetMode match {
        case SpecificRoleset(rs) => rs
        case UseLegacyEntitlements =>
          throw new IllegalStateException("Existing dal session should be established using new role base entitlement.")
        case _ =>
          throw new DSISpecificError(s"RoleSetMode ${existingSession.rolesetMode} not supported with createNewSession")
      }
      require(
        roles.subsetOf(establishedSessionRoles),
        s"roles $roles should be subset of established session roles $establishedSessionRoles")
      val existingTokenType = existingSession.establishingCommand.onBehalfTokenType
      val existingToken =
        if (existingTokenType == OnBehalfTokenType.Default) None
        else existingSession.establishingCommand.onBehalfSessionToken
      val newSession = EstablishSession(
        existingSession.establishingCommand.applicationIdentifier,
        existingSession.clientFeatures,
        existingSession.establishingCommand.context,
        existingSession.establishingCommand.realId,
        existingSession.establishingCommand.effectiveId,
        rolesets = Seq(roles),
        Some(existingSession.establishmentTime),
        existingSession.establishingCommand.classpathHash,
        existingSession.establishingCommand.fullClasspath,
        existingSession.establishingCommand.pid,
        existingSession.establishingCommand.slotMapOverrides,
        onBehalfTokenType = existingTokenType,
        onBehalfSessionToken = existingToken,
        existingSession.establishingCommand.clientPath,
        existingSession.establishingCommand.sessionInfo.clientMachineIdentifier,
        shouldEstablishRoles = false
      )
      val res = executeReadOnlyCommands(Seq(CreateNewSession(newSession)))
      require(res.nonEmpty, s"CreateNewSession request for session ${newSession} returned empty results, not expected")
      val expectedRes = res.head
      expectedRes match {
        case CreateNewSessionResult(EstablishSessionSuccess(aid, acf, si, ssf, rs, et, est, etk, hash)) =>
          EstablishedClientSession(aid, acf, si, ssf, rs, et, newSession, est, etk, hash)
        case CreateNewSessionResult(EstablishSessionFailure(_, message)) => throw new DSISpecificError(message)
        case _ =>
          throw new DSISpecificError(
            s"received unexpected result type ${expectedRes.getClass}, actual result: ${expectedRes}")
      }
    } else {
      log.warn(
        s"new session creation for sub set of roles $roles is not supported by broker. returning the existing client session back")
      // falling back to older behaviour
      getSession()
    }
  }

  final def supportsImpersonation: Boolean = true

  protected final def brokerVirtualHostname(env: DalEnv): Option[String] =
    ClientBrokerContext.brokerVirtualHostname(env)
}
