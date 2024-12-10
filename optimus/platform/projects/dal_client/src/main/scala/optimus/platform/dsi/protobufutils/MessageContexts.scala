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
package optimus.platform.dsi.protobufutils

import java.util.UUID
import java.time.Instant
import com.google.protobuf.MessageLite
import msjava.protobufutils.server.BackendException
import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.ChainedID
import optimus.dsi.session.EstablishSessionResult
import optimus.graph.CancellationScope
import optimus.graph.GraphInInvalidState
import optimus.graph.CompletableNode
import optimus.graph.Scheduler
import optimus.platform.dal.DALCache
import optimus.platform.NonForwardingPluginTagKey
import optimus.platform.ScenarioStack
import optimus.platform.RuntimeEnvironment
import optimus.platform.dal.session.ClientSessionContext
import optimus.platform.dsi.bitemporal.CmdLogLevelHelper
import optimus.platform.dsi.bitemporal.proto.CommandAppNameTag
import optimus.platform.dsi.bitemporal.proto.CommandLocation
import optimus.platform.dsi.bitemporal.proto.CommandProtoSerialization
import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform.dsi.bitemporal.Command
import optimus.platform.dsi.bitemporal.{DALNonRetryableActionException, DALRetryableActionException}
import optimus.platform.dsi.bitemporal.Result
import optimus.platform.dsi.bitemporal.UnentitledCommand
import optimus.platform.dsi.versioning.VersioningRedirectionInfo
import optimus.platform.dsi.DSIResponse
import optimus.platform.dsi.Response
import optimus.platform.dsi.bitemporal.AbstractPartialResult
import optimus.platform.dsi.bitemporal.GetInfo
import optimus.platform.dsi.bitemporal.HasVtTime
import optimus.platform.dsi.bitemporal.MessagesPublishCommandBase
import optimus.platform.dsi.bitemporal.Put
import optimus.platform.dsi.bitemporal.PutApplicationEvent
import optimus.platform.dsi.bitemporal.ReadOnlyCommandWithTT
import optimus.platform.dsi.bitemporal.ReadOnlyCommandWithTTRange
import optimus.platform.dsi.bitemporal.ReadOnlyCommandWithTemporality
import optimus.platform.relational.CallerDetail
import optimus.platform.relational.ExtraCallerDetail

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Promise

trait BatchContext {
  def requestUuid: String
  def chainedId: ChainedID
  def requestType: DSIRequestProto.Type
  final def readOnly: Boolean = ??? /* requestType == DSIRequestProto.Type.READ_ONLY */
  def clientSessionContext: ClientSessionContext
  def clientRequests: Vector[ClientRequest]
  def token: Option[InFlightRequestToken]
  def redirectionInfo: Option[VersioningRedirectionInfo]
  def started: Long
  def numResults: Int

  def completeAllWithException(client: Option[DalBrokerClient], ex: Throwable): Unit
  final def completeDueToShutdown(client: Option[DalBrokerClient]): Unit = {
    val ex =
      if (readOnly)
        new DALRetryableActionException("Connection shutdown", client)
      else
        new BackendException("Connection shutdown")
    completeAllWithException(client, ex)
  }
  def completeResponses(
      client: DalBrokerClient,
      seqId: Int,
      now: Long,
      partial: Boolean,
      enableOutOfOrderCompletion: Boolean): Boolean
  def expired(timeout: Int): Boolean
  def commandIdToClientRequestContext(i: Int): BatchContext.ClientRequestContext

  def cmdDetails(logLevel: Int): Iterable[String] = {
    val commands = clientRequests.flatMap(_.commands)
    val totCmdCnt = commands.size
    for (((commandCls, entityClsOpt), group) <- commands.groupBy(c => (c.getClass.getSimpleName, c.className())))
      yield {
        val sz = group.size

        val commandType = Some(s"C: ${commandCls}")
        val entityType = entityClsOpt.map(entityCls => s"(${entityCls},${sz})")
        val partOfWhole = Some(s"(${sz} of ${totCmdCnt})")
        val commandListing =
          if (logLevel == CmdLogLevelHelper.DebugLevel) {
            Some(CmdLogLevelHelper.iterDisplay("Cmds:", group.toSeq, (c: Command) => c.logDisplay(logLevel), 3))
          } else {
            None
          }
        val temporalInfo =
          if (BatchContext.hasTemporalInfoToLog(group.head)) Some(BatchContext.temporalLogString(group)) else None

        List(commandType, entityType, temporalInfo, partOfWhole, commandListing).flatten.mkString(" ")
      }
  }

  lazy val minAssignableTtOpt: Option[Instant] = ??? /* {
    if (requestType == DSIRequestProto.Type.WRITE) {
      val mats = clientRequests.flatMap(_.commands.collect {
        case pae: PutApplicationEvent if pae.minAssignableTtOpt.isDefined => pae.minAssignableTtOpt.get
        case p: Put if p.minAssignableTtOpt.isDefined                     => p.minAssignableTtOpt.get
      })
      if (mats.nonEmpty) Some(mats.max) else None
    } else None
  } */

  val lastWitnessedTxTimeOpt: Option[Instant] = ??? /* {
    if (requestType == DSIRequestProto.Type.WRITE) {
      val lwts = clientRequests.collect { case wr: WriteClientRequest => wr.lastWitnessedTxTimeOpt }.flatten
      if (lwts.nonEmpty) Some(lwts.max) else None
    } else None
  } */
}

object BatchContext {
  val log = getLogger[BatchContext]

  final class ClientRequestHelper(
      val commands: Vector[Command],
      val commandProtos: Vector[CommandProto],
      val callerDetail: Option[CallerDetail],
      val extraCallerDetail: Option[ExtraCallerDetail]
  )

  // TODO (OPTIMUS-13333): Privatize (need to encapsulate logic from RequestSender)
  final class ClientRequestContext(
      val completable: Completable,
      val base: Int,
      size: Int,
      helperOpt: Option[ClientRequestHelper],
      appNameTag: Option[String],
      var establishSessionResult: Option[EstablishSessionResult] = None) {
    // no override for hash and equals: we need identity here for efficiency

    private[this] val commandContexts: Seq[ListBuffer[Result]] =
      IndexedSeq.fill(size)(mutable.ListBuffer.empty[Result])

    /**
     * lazy because only used if dalCacheEnabled
     */
    private[this] lazy val commandContextProtos: Seq[ListBuffer[MessageLite]] =
      IndexedSeq.fill(size)(mutable.ListBuffer.empty[MessageLite])

    // we'll remove command IDs as they are completed, this is how we know that the request itself is completed
    private[this] val incompleteCommandIds = mutable.BitSet((0 until size): _*)

    private[this] def commandContext(i: Int) = commandContexts(i - base)
    private[this] def commandContextProto(i: Int) = commandContextProtos(i - base)

    def isComplete = incompleteCommandIds.size == 0 // don't replace this with .isEmpty until Scala fixes perf issue

    private[BatchContext] def completeWithException(ex: Throwable) = {
      completable.completeWithException(ex)
    }

    private[BatchContext] def complete(client: DalBrokerClient) = {
      val allResults = commandContexts.flatten
      completable.completeWithResult(DSIResponse(allResults, establishSessionResult, Option(client)))
      allResults.size
    }

    def addCommandResults(
        requestUuid: String,
        seqId: Int,
        commandIndex: Int,
        partResults: Seq[Result],
        partResultProtos: Seq[MessageLite],
        isPartial: Boolean): Unit = {
      commandContext(commandIndex) ++= partResults

      // only record proto if DALCache is enabled
      if (DALCache.Global.dalCacheEnabled) commandContextProto(commandIndex) ++= partResultProtos

      if (!isPartial) {
        val removed = incompleteCommandIds.remove(commandIndex - base)
        if (!removed) {
          log.error(
            "{} can't find index of completed command ({}) in my list, something may be wrong here",
            logPrefix(requestUuid, seqId),
            commandIndex)
          throw new DALNonRetryableActionException(
            s"can't find index of completed command ${commandIndex}, duplicate results may have been sent by the broker",
            None)
        } else {
          if (DALCache.Global.dalCacheEnabled) {
            // helperOpt will be None if !readOnly
            helperOpt.foreach(helper => {
              val command = helper.commands(commandIndex - base)
              if (DALCache.Global.shouldCache(command)) {
                val commandProto = helper.commandProtos(commandIndex - base)
                val results = commandContext(commandIndex)
                val resultProtos = commandContextProto(commandIndex)
                if (DALCache.shouldCacheCommandGetInfo && command.isInstanceOf[GetInfo]) {
                  // DAL uses initial GetInfo to establish DAL session, and get server SupportedFeatures
                  // this late replace allows GetInfo to happen, but still use cached version
                  val alt = DALCache.Global.load(Seq(command), Seq(commandProto), None)
                  alt.foreach(seq => results.update(0, seq.head))
                }
                // doesn't overwrite if file exists
                DALCache.Global.save(command, commandProto, results, resultProtos, helper.extraCallerDetail)
              }
            })
          }
        }
      }
    }
  }

  def apply(
      requestUuid: String,
      chainedId: ChainedID,
      requestType: DSIRequestProto.Type,
      clientSessionContext: ClientSessionContext,
      clientRequests: Vector[ClientRequest],
      token: Option[InFlightRequestToken],
      redirectionInfo: Option[VersioningRedirectionInfo],
      retryManager: Option[BatchRetryManager]): BatchContext = {
    retryManager map { rm =>
      RetryBatchContextImpl(
        requestUuid,
        chainedId,
        requestType,
        clientSessionContext,
        clientRequests,
        token,
        redirectionInfo,
        rm.maxRetryAttempts,
        rm.retryBackoffTime,
        rm)
    } getOrElse {
      BatchContextImpl(
        requestUuid,
        chainedId,
        requestType,
        clientSessionContext,
        clientRequests,
        token,
        redirectionInfo)
    }
  }

  def unapply(batchContext: BatchContext) =
    Some((batchContext.requestUuid, batchContext.chainedId, batchContext.started))

  object BatchContextImpl {
    def apply(
        requestUuid: String,
        chainedId: ChainedID,
        requestType: DSIRequestProto.Type,
        clientSessionContext: ClientSessionContext,
        clientRequests: Vector[ClientRequest],
        token: Option[InFlightRequestToken],
        redirectionInfo: Option[VersioningRedirectionInfo]): BatchContextImpl = {
      new BatchContextImpl(
        requestUuid,
        chainedId,
        requestType,
        System.currentTimeMillis,
        clientSessionContext,
        clientRequests,
        token,
        redirectionInfo)
    }
  }

  class BatchContextImpl protected (
      val requestUuid: String,
      val chainedId: ChainedID,
      val requestType: DSIRequestProto.Type,
      val started: Long,
      val clientSessionContext: ClientSessionContext,
      val clientRequests: Vector[ClientRequest],
      val token: Option[InFlightRequestToken],
      val redirectionInfo: Option[VersioningRedirectionInfo])
      extends BatchContext {
    // no override for hash and equals: we need identity here for efficiency

    private[this] val clientRequestContextMap = {
      val map = new java.util.TreeMap[Int, ClientRequestContext]()
      var currentCmdIndex = 0

      clientRequests foreach { cr =>
        val base = currentCmdIndex

        val count = cr.commandProtos.size

        val helper =
          if (DALCache.Global.dalCacheEnabled && readOnly)
            Some(
              new ClientRequestHelper(
                cr.commands.toVector,
                cr.commandProtos.toVector,
                cr.callerDetail,
                cr.extraCallerDetail))
          else
            None

        val crc = new ClientRequestContext(cr.completable, base, count, helper, cr.appNameTag)
        currentCmdIndex += count
        map.put(crc.base, crc)
      }
      map
    }

    private def clientRequestContexts = clientRequestContextMap.values().asScala

    // TODO (OPTIMUS-13333): Privatize.
    def commandIdToClientRequestContext(i: Int) = synchronized {
      val e = clientRequestContextMap.floorEntry(i)
      e.getValue()
    }

    // we'll drop client request contexts as they're no longer needed so we need to keep track of numResults separately for logging at the end
    private var _numResults = 0

    def numResults = _numResults

    def completeAllWithException(client: Option[DalBrokerClient], ex: Throwable): Unit = synchronized {
      clientRequestContexts.foreach { _.completeWithException(ex) }
      token.foreach(_.complete)
    }

    private[this] def completeClientRequest(client: DalBrokerClient, clientRequestContext: ClientRequestContext) = {
      val results = clientRequestContext.complete(client)
      _numResults += results
    }

    // Returns true if batch is complete and can be removed from MessageReceiver tracking data structures.
    def completeResponses(
        client: DalBrokerClient,
        seqId: Int,
        now: Long,
        partial: Boolean,
        enableOutOfOrderCompletion: Boolean): Boolean = synchronized {
      if (partial) {
        if (enableOutOfOrderCompletion) {
          val newlyCompletedRequests = clientRequestContexts.filter(_.isComplete)

          newlyCompletedRequests.foreach { crc =>
            completeClientRequest(client, crc)
            clientRequestContextMap.remove(crc.base)
          }

          val numCompletedAheadOfTime = newlyCompletedRequests.size

          if (numCompletedAheadOfTime > 0)
            log.debug(
              "{} {} requests completed ahead of time, in {}ms",
              logPrefix(requestUuid, seqId),
              numCompletedAheadOfTime,
              now - started)

          if (clientRequestContextMap.isEmpty) {
            log.warn(
              "{} partial response and yet all client requests are already completed ... Completing batch anyway, look into this!",
              logPrefix(requestUuid, seqId))
            true
          } else {
            false
          }
        } else {
          false
        }
      } else {
        clientRequestContexts.foreach(completeClientRequest(client, _))
        clientRequestContextMap.clear()
        token.foreach(_.complete)
        true
      }
    }

    def expired(timeout: Int) = System.currentTimeMillis > started + timeout

    protected def inCompleteClientRequests(allClientRequests: Vector[ClientRequest]): Vector[ClientRequest] =
      synchronized {
        val completables = clientRequestContexts map { _.completable } toSet

        allClientRequests filter { cr =>
          completables.contains(cr.completable)
        }
      }
  }

  object RetryBatchContextImpl {
    def apply(
        requestUuid: String,
        chainedId: ChainedID,
        requestType: DSIRequestProto.Type,
        clientSessionContext: ClientSessionContext,
        clientRequests: Vector[ClientRequest],
        token: Option[InFlightRequestToken],
        redirectionInfo: Option[VersioningRedirectionInfo],
        attempts: Int,
        backoff: Long,
        retryManager: BatchRetryManager): RetryBatchContextImpl = {
      new RetryBatchContextImpl(
        requestUuid,
        chainedId,
        requestType,
        System.currentTimeMillis(),
        clientSessionContext,
        clientRequests,
        token,
        redirectionInfo,
        attempts,
        backoff,
        retryManager)
    }
  }

  final class RetryBatchContextImpl private (
      rid: String,
      cid: ChainedID,
      rType: DSIRequestProto.Type,
      startTime: Long,
      sessionCtx: ClientSessionContext,
      requests: Vector[ClientRequest],
      reqToken: Option[InFlightRequestToken],
      verRedirectionInfo: Option[VersioningRedirectionInfo],
      val attempts: Int,
      val backoff: Long,
      retryManager: BatchRetryManager)
      extends BatchContextImpl(rid, cid, rType, startTime, sessionCtx, requests, reqToken, verRedirectionInfo) {

    override def completeAllWithException(client: Option[DalBrokerClient], ex: Throwable): Unit =
      retryManager.retry(client, this, ex)
    override def completeResponses(
        client: DalBrokerClient,
        seqId: Int,
        now: Long,
        partial: Boolean,
        enableOutOfOrderCompletion: Boolean): Boolean = {
      val ret = super.completeResponses(client, seqId, now, partial, enableOutOfOrderCompletion)
      // if the batch is successfully completed after retry (attempts have been used), we raise the recovery event
      if (attempts < retryManager.maxRetryAttempts) {
        retryManager.raiseDALRecoveryEvent(Option(client))
      }
      ret
    }

    def finallyCompleteAllWithException(client: Option[DalBrokerClient], ex: Throwable): Unit = {
      super.completeAllWithException(client, ex)
    }

    def copy(uuid: UUID, remainingAttempts: Int, newBackOff: Long): RetryBatchContextImpl = {
      // if the original batch's chainedId was created from its requestUuid
      // (see RequestSender where requestChainedId is generated)
      // then we also create a new chainedId for the new uuid received
      val batchChainedId = if (chainedId.repr == requestUuid) new ChainedID(uuid) else chainedId
      val pendingRequests = inCompleteClientRequests(clientRequests) // possible some clientRequests were completed
      new RetryBatchContextImpl(
        uuid.toString,
        batchChainedId,
        requestType,
        started,
        clientSessionContext,
        pendingRequests,
        token,
        redirectionInfo,
        remainingAttempts,
        newBackOff,
        retryManager
      )
    }
  }

  def hasTemporalInfoToLog(c: Command): Boolean = c match {
    case _: ReadOnlyCommandWithTT          => true
    case _: ReadOnlyCommandWithTTRange     => true
    case _: ReadOnlyCommandWithTemporality => true
    case _                                 => false
  }

  def temporalLogString(commands: Seq[Command]): String = {
    val ttsInterval = interval(collectTts(commands))
    val vtsInterval = interval(collectVts(commands))

    s"${formatInterval("ttRange", ttsInterval)} ${formatInterval("vtRange", vtsInterval)}"
  }

  def interval(times: Seq[Instant]): (Int, Option[(Instant, Instant)]) = {
    val distinctTimes = times.distinct
    if (distinctTimes.isEmpty) {
      (0, None)
    } else {
      (distinctTimes.size, Some(distinctTimes.min, distinctTimes.max))
    }
  }

  def formatInterval(name: String, interval: (Int, Option[(Instant, Instant)])): String = interval match {
    case (0, _)                  => ""
    case (1, Some((instant, _))) => s"$name(uniq: 1, val: $instant)"
    case (uniqInstants, None)    => s"$name(uniq: $uniqInstants)" // unexpected case, it should provide min/max
    case (uniqInstants, Some((minInstant, maxInstant))) =>
      s"$name(uniq: $uniqInstants, min: $minInstant, max: $maxInstant)"
  }

  def collectTts(commands: Seq[Command]): Seq[Instant] = {
    commands.collect {
      case rc: ReadOnlyCommandWithTT          => rc.tt
      case rc: ReadOnlyCommandWithTTRange     => rc.ttRange.to
      case rc: ReadOnlyCommandWithTemporality => rc.temporality.readTxTime
    }
  }

  def collectVts(commands: Seq[Command]): Seq[Instant] = {
    commands.collect { case rc: ReadOnlyCommandWithTemporality =>
      rc.temporality match {
        case c: HasVtTime => Some(c.readValidTime)
        case _            => None
      }
    }.flatten
  }
}

trait Completable {
  def completeWithException(t: Throwable): Unit
  def completeWithResult(result: Response): Unit
  def isCancelled: Boolean
  def env: Option[RuntimeEnvironment]
}

trait PreCalc {
  def launch(pr: AbstractPartialResult[_, _]): Unit
  def mungeResults(rs: Seq[Result]): Seq[Result]
}
object NoPreCalc extends PreCalc {
  override def launch(pr: AbstractPartialResult[_, _]): Unit = throw new AssertionError("Should never be called.")
  override def mungeResults(rs: Seq[Result]): Seq[Result] = throw new AssertionError("Should never be called.")
}
object PreCalc {
  def unapply(arg: PreCalc): Option[PreCalc] = if (arg ne NoPreCalc) Some(arg) else None
}
object PreCalcTag extends NonForwardingPluginTagKey[PreCalc]

/**
 * adapts dal client's Completable API to graph Nodes, taking care of completion, failure and cancellation
 */
final case class NodeWithScheduler(
    scheduler: Scheduler,
    node: CompletableNode[Response],
    seqOpId: Long,
    preCalc: PreCalc)
    extends Completable {
  private val csListener: CancellationScope => Unit =
    (cs: CancellationScope) => completeWithException(cs.cancellationCause): Unit

  // keep a copy of the node's ss, as it may change when the node completes
  private val ss: ScenarioStack = node.scenarioStack()

  override def env = Some(ss.env)

  // if our CancelScope gets cancelled then abort the node so that we don't sit around waiting for the response
  // TODO (OPTIMUS-16988): we could also cancel the request on the server-side if it
  // was supported
  ss.cancelScope.addListener(csListener)

  def completeWithException(t: Throwable): Unit = {
    ss.cancelScope.removeListener(csListener)
    if (node.tryCompleteWithException(t, scheduler)) {
      // we complete the waiting node first because it may schedule more nodes, and we want to do that before notifying
      // that all DSI calls are done, because if the graph is idle at that point batchers will run and we want to
      // defer that for as long as possible
      // NB we entered DSI batch scope in the RequestBatcher
      ss.batchScope.exitDSI(scheduler, node)
    } else assertCancelled()
  }

  def completeWithResult(result: Response): Unit = {
    ss.cancelScope.removeListener(csListener)
    if (node.tryCompleteWithResult(result, scheduler)) {
      // we complete the waiting node first because it may schedule more nodes, and we want to do that before notifying
      // that all DSI calls are done, because if the graph is idle at that point batchers will run and we want to
      // defer that for as long as possible
      // NB we entered DSI batch scope in the RequestBatcher
      ss.batchScope.exitDSI(scheduler, node)
    } else assertCancelled()
  }

  private def assertCancelled(): Unit = {
    // we should only be getting completed twice if we've been cancelled (once is the early abort due to the
    // client-side cancellation, and once is due to the uncancelled server-side DAL query completing).
    if (!isCancelled) {
      throw new GraphInInvalidState("Unexpected duplicate completion in a non-cancellation situation")
    }
  }

  def isCancelled: Boolean = ss.cancelScope.isCancelled
}

final case class CompletablePromise(promise: Promise[Response]) extends Completable {
  def completeWithException(t: Throwable): Unit = {
    promise.failure(t)
  }
  def completeWithResult(result: Response): Unit = {
    promise.success(result)
  }
  def isCancelled: Boolean = false
  override def env = None
}

final case class InterceptedCompletable(underlying: Completable, handler: Either[Throwable, Response] => Unit)
    extends Completable {
  def completeWithException(t: Throwable): Unit = {
    handler(Left(t))
    underlying.completeWithException(t)
  }
  def completeWithResult(result: Response): Unit = {
    handler(Right(result))
    underlying.completeWithResult(result)
  }
  def isCancelled: Boolean = underlying.isCancelled
  override def env: Option[RuntimeEnvironment] = underlying.env
}

sealed abstract class ClientRequest {
  def completable: Completable
  def nodeID: ChainedID
  def clientSessionCtx: ClientSessionContext
  def commands: Seq[Command]
  def commandProtos: Seq[CommandProto]
  def callerDetail: Option[CallerDetail]
  def extraCallerDetail: Option[ExtraCallerDetail]
  def appNameTag: Option[String]
  def hasUnentitled: Boolean
  def versioningRedirectionInfo: Option[VersioningRedirectionInfo]

  def withCompletable(completable: Completable): ClientRequest
}

final case class ReadClientRequest private (
    completable: Completable,
    nodeID: ChainedID,
    clientSessionCtx: ClientSessionContext,
    commands: Seq[Command],
    commandProtos: Seq[CommandProto],
    callerDetail: Option[CallerDetail],
    extraCallerDetail: Option[ExtraCallerDetail],
    appNameTag: Option[String])
    extends ClientRequest {
  override def versioningRedirectionInfo: Option[VersioningRedirectionInfo] = None
  lazy val hasUnentitled = commands exists { _.isInstanceOf[UnentitledCommand] }

  def withCompletable(completable: Completable): ReadClientRequest = this.copy(completable = completable)
}

final case class WriteClientRequest private (
    completable: Completable,
    nodeID: ChainedID,
    clientSessionCtx: ClientSessionContext,
    commands: Seq[Command],
    commandProtos: Seq[CommandProto],
    callerDetail: Option[CallerDetail],
    extraCallerDetail: Option[ExtraCallerDetail],
    appNameTag: Option[String],
    versioningRedirectionInfo: Option[VersioningRedirectionInfo],
    lastWitnessedTxTimeOpt: Option[Instant]
) extends ClientRequest {
  override def hasUnentitled: Boolean = false
  def hasMessagesCommands: Boolean = commands.exists(_.isInstanceOf[MessagesPublishCommandBase])

  def withCompletable(completable: Completable): WriteClientRequest = this.copy(completable = completable)
}

object ClientRequest extends CommandProtoSerialization {
  def read(
      completable: Completable,
      nodeID: ChainedID,
      clientSessionCtx: ClientSessionContext,
      commands: Seq[Command],
      callerDetail: Option[CallerDetail] = None,
      extraCallerDetail: Option[ExtraCallerDetail] = None,
      appNameTag: Option[String] = None): ReadClientRequest = {
    val commandProtos = commands.map(toProto)
    ReadClientRequest(
      completable,
      nodeID,
      clientSessionCtx,
      commands,
      commandProtos,
      callerDetail,
      extraCallerDetail,
      appNameTag)
  }

  def write(
      completable: Completable,
      nodeID: ChainedID,
      clientSessionCtx: ClientSessionContext,
      commands: Seq[Command],
      redirectionInfo: Option[VersioningRedirectionInfo],
      lastWitnessedTxTimeOpt: Option[Instant],
      callerDetail: Option[CallerDetail] = None,
      extraCallerDetail: Option[ExtraCallerDetail] = None,
      appNameTag: Option[String] = None
  ): WriteClientRequest = {
    val commandProtos = commands.map(toProto)
    WriteClientRequest(
      completable = completable,
      nodeID = nodeID,
      clientSessionCtx = clientSessionCtx,
      commands = commands,
      commandProtos = commandProtos,
      callerDetail = callerDetail,
      extraCallerDetail = extraCallerDetail,
      appNameTag = appNameTag,
      versioningRedirectionInfo = redirectionInfo,
      lastWitnessedTxTimeOpt = lastWitnessedTxTimeOpt
    )
  }
}

object CommandLocations {
  def generateSummary(commandLocations: Seq[CommandLocation]): String =
    commandLocations.map { _.location }.distinct.mkString("\n")
  def extract(clientRequests: Seq[ClientRequest]): Seq[CommandLocation] = {
    var (start, end) = (0, 0)
    clientRequests.flatMap { request =>
      end = start + request.commands.size
      val ret = request.callerDetail match {
        case Some(callerDetail) if start != end =>
          Some((CommandLocation(callerDetail.linkFormatLocation, (start until end))))
        case _ => None
      }
      start = end
      ret
    }
  }
}

object CommandAppNameTags {
  def extract(clientRequests: Seq[ClientRequest]): Seq[CommandAppNameTag] = {
    var (start, end) = (0, 0)
    val reqs = clientRequests.flatMap { request =>
      end = start + request.commands.size
      val ret = request.appNameTag match {
        case Some(tag) if start != end => Some(CommandAppNameTag(tag, (start until end)))
        case _                         => None
      }
      start = end
      ret
    }
    reqs
      .groupBy(_.appNameTag)
      .map { case (tag, requests) =>
        CommandAppNameTag(tag, requests.foldLeft(Seq.empty[Int]) { case (acc, cur) => acc ++ cur.commandIndices })
      }
      .toSeq
      .sortBy(_.appNameTag)
  }
}
