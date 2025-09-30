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
package optimus.platform.dal.messages

import msjava.slf4jutils.scalalog.getLogger
import optimus.core.CoreAPI
import optimus.dsi.partitioning.PartitionMap
import optimus.dsi.session.EstablishSessionResult
import optimus.graph.DiagnosticSettings
import optimus.platform.AdvancedUtils
import optimus.platform.async
import optimus.platform.dal.AbstractRemoteDSIProxy
import optimus.platform.dal.ClientBrokerContext
import optimus.platform.dal.MissingResultException
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.messages.MessagesEvent.StreamCreationFailed
import optimus.platform.dal.messages.MessagesEvent.SubscriptionChangeFailed
import optimus.platform.dal.session.ClientSessionContext
import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.protobufutils.DalBrokerClient

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import scala.util.control.NonFatal

import scala.jdk.CollectionConverters._
import optimus.utils.CollectionUtils._

object MessagesDsiProxyBase {
  private val log = getLogger(this)

  // This isn't ideal however, we don't want to create another hierarchy of request types just for MessagesClient
  // interface. So keeping this simple for now.
  val NewStreamChangeId: Int = -500
  val CloseStreamChangeId: Int = -1000

  private val operationRetryAttempts: Int =
    DiagnosticSettings.getIntProperty("optimus.platform.messages.retryAttempts", 10)
  private val operationRetryBackoffTime: Long =
    DiagnosticSettings.getLongProperty("optimus.platform.messages.retryBackoffTimeMs", 1000L)
  private val disablePublishRequestLogging: Boolean =
    DiagnosticSettings.getBoolProperty("optimus.platform.messages.disablePublishRequestLogging", false)
  private val operationMaxRetryBackoffTimeMs: AtomicLong =
    new AtomicLong(DiagnosticSettings.getLongProperty("optimus.platform.messages.maxRetryBackoffTimeMs", 30000L))

  def setOperationMaxRetryBackoffTimeMs(timeMs: Long) = operationMaxRetryBackoffTimeMs.set(timeMs)

  def getOperationMaxRetryBackoffTimeMs() = operationMaxRetryBackoffTimeMs.get()

  def shouldCloseIfConnStrIsNoLongerInLeaderOrFollower(
      connStr: Option[String],
      leader: Option[String],
      followers: List[String]): Boolean = {
    connStr exists { s =>
      !leader.contains(s) && !followers.contains(s)
    }
  }
}

trait MessagesDsiProxyBase extends MessagesRetryHandler with CoreAPI {
  remoteProxy: AbstractRemoteDSIProxy =>
  import MessagesDsiProxyBase._

  override def partitionMap: PartitionMap = throw new UnsupportedOperationException

  protected[messages] val streamMap = new ConcurrentHashMap[String, MessagesNotificationStream]()

  private[optimus] def TEST_ONLY_currentSubscriptions: Set[MessagesSubscription] = {
    // Better to get the stream ID referring to the react block but sufficient for tests
    require(streamMap.size == 1, "Only supported for one react() block")
    val notificationStream = streamMap.values.asScala.single
    notificationStream.currentSubscriptions
  }

  private[optimus] def checkStreamExists(streamId: String) = streamMap.containsKey(streamId)

  protected def calculateBackoffTimeMs(seedBackoffTime: Long, retryAttempt: Int): Long = {
    val backoffTime = Math.round(seedBackoffTime * Math.pow(2, retryAttempt - 1))
    if (backoffTime > getOperationMaxRetryBackoffTimeMs()) getOperationMaxRetryBackoffTimeMs()
    else backoffTime
  }

  private[messages] val ctx: ClientBrokerContext

  private[optimus] val callback: MessagesClientEventsListener = new MessagesClientEventsListener {

    override def handleResults(
        results: Seq[Result]
    ): Unit = {
      val (strmRes, others) = results.partition(_.isInstanceOf[MessagesStreamResult])
      strmRes
        .collect { case r: MessagesStreamResult => r }
        .groupBy(_.streamId)
        .foreach { case (strmId, res) =>
          getStream(strmId).foreach(strm => handleStreamResults(res, strm))
        }
      others foreach {
        case ErrorResult(error, _)      => handleErrorResult(error)
        case MessagesErrorResult(error) => handleErrorResult(error)
        case other                      => log.error(s"Do not expect result: $other")
      }
    }

    private def handleStreamResults(
        results: Seq[MessagesStreamResult],
        stream: MessagesNotificationStream
    ): Unit = {
      val (notificationResults, others) = results.partition(_.isInstanceOf[DirectMessagesNotificationResult])
      others.foreach {
        case streamChange: MessagesChangeSubscriptionSuccessResult =>
          stream.onSubscriptionUpdated(streamChange.changeRequestId)
        case _: MessagesCreateStreamSuccessResult => stream.onStreamCreationSuccess()
        case _: MessagesCloseStreamSuccessResult =>
          stream.onStreamClosed()
          streamMap.remove(stream.id)
        case _: MessagesBrokerConnect                   => onConnect()
        case _: MessagesBrokerDisconnect                => onDisconnect()
        case r: MessagesCreateClientStreamSuccessResult => throw new NotImplementedError(s"Unexpected result $r")
        case r: DirectMessagesNotificationResult        => throw new IllegalArgumentException(s"Unexpected result $r")
      }
      if (notificationResults.nonEmpty)
        stream.onMessageReceived(notificationResults.collect { case r: DirectMessagesNotificationResult => r })
    }

    override def handleError(
        failedClient: Option[DalBrokerClient],
        th: Throwable
    ): Unit = handleMessagesFailure(failedClient, th)

    override def consumeEstablishSessionResult(
        result: EstablishSessionResult,
        clientSessionCtx: ClientSessionContext
    ): Unit = clientSessionCtx.consumeEstablishSessionResult(result)

    private def getStream(streamId: String): Option[MessagesNotificationStream] = {
      val res = Option(streamMap.get(streamId))
      if (res.isEmpty) log.info(s"Stream id does not exist in the stream map, might be closed $streamId")
      res
    }

    private[messages] def handleStreamErrorResults(
        strmException: MessagesStreamException,
        stream: MessagesNotificationStream
    ): Unit = {
      strmException match {
        case msce: MessagesStreamCreationException =>
          stream.onSubscriptionError(StreamCreationFailed(msce.streamId, msce))
        case msce: MessagesSubscriptionChangeException =>
          stream.onSubscriptionError(SubscriptionChangeFailed(msce.changeRequestId, msce.streamId, msce))
        case _: MessagesStreamClosureException =>
          stream.onStreamClosed()
          streamMap.remove(stream.id)
        case ex: MessagesTransientException =>
          stream.onStreamError(MessagesEvent.MessagesStreamError(stream.id, ex))
      }
    }

    private[messages] def handleErrorResult(error: Throwable): Unit = {
      error match {
        case strmException: MessagesStreamException =>
          getStream(strmException.streamId).foreach(handleStreamErrorResults(strmException, _))
        case ex => log.error("Unknown error received, going to ignore..!", ex)
      }
    }
  }

  protected def createBrokerClient(): MessagesClient

  protected[messages] val dalEnv: DalEnv

  @async override final def executeMessagesCommands(
      cmds: Seq[MessagesCommand]
  ): Seq[MessagesResult] = {
    // multiple publishEvent command is batched into single request
    val events = cmds.collect { case pc: MessagesPublishCommand => pc }
    val eventsResult: Map[MessagesCommand, MessagesResult] =
      if (events.nonEmpty) (events zip executePublishCommands(events, 1)).toMap
      else Map.empty

    // multiple publishTransaction command is batched into single request
    val transactions = cmds.collect { case pc: MessagesPublishTransactionCommand => pc }
    val transactionsResult: Map[MessagesCommand, MessagesResult] =
      if (transactions.nonEmpty) (transactions zip executePublishCommands(transactions, 1)).toMap
      else Map.empty

    val alcsCmd = cmds.collect { case ec: StreamsACLsCommand => ec }
    val aclsResult: Map[MessagesCommand, MessagesResult] =
      alcsCmd.map(c => c -> checkEntitlementAndSetACLs(c)).toMap

    val createStreams = cmds.collect { case sc: CreateMessagesClientStream => sc }
    val createStreamRes: Map[MessagesCommand, MessagesResult] =
      createStreams.map(c => c -> executeCreateStream(c)).toMap

    cmds.map { c =>
      eventsResult.getOrElse(
        c,
        transactionsResult.getOrElse(
          c,
          createStreamRes.getOrElse(c, aclsResult.getOrElse(c, MessagesErrorResult(new MissingResultException)))))
    }
  }

  private def checkEntitlementAndSetACLs(cmd: StreamsACLsCommand, attempt: Int = 0): MessagesResult = {
    val client = getClient
    try {
      client.checkEntitlementAndSetACLs(cmd)
    } catch {
      case e: DALRetryableActionException if attempt < getRetryAttempts =>
        val backoffTime = calculateBackoffTimeMs(getRetryBackoffTime, attempt + 1)
        closeCurrentSender()
        Thread.sleep(backoffTime)

        log.error(
          s"${cmd.logDisplay(CmdLogLevelHelper.InfoLevel)} failed, attempted ${attempt + 1} times, retrying again",
          e)
        checkEntitlementAndSetACLs(cmd, attempt + 1)

      case NonFatal(ex) =>
        log.error(s"${cmd.logDisplay(CmdLogLevelHelper.InfoLevel)} failed", ex)
        MessagesErrorResult(new StreamsACLsCommandException(ex.getMessage))
    }
  }

  @async private def executePublishCommands(
      cmds: Seq[MessagesPublishCommandBase],
      attempt: Int
  ): Seq[MessagesResult] = {
    val res = asyncResult {
      val client = getClient
      val (publishTime, res) = AdvancedUtils.timed {
        client.publish(cmds)
      }
      if (!disablePublishRequestLogging)
        cmds.foreach(c =>
          log.info(
            s"publish request ${c.logDisplay(CmdLogLevelHelper.InfoLevel)} completed in ${TimeUnit.NANOSECONDS.toMillis(publishTime)} ms."))
      res
    }
    if (res.hasException) {
      res.exception match {
        case _: DALRetryableActionException if attempt < getRetryAttempts =>
          val backoffTime = calculateBackoffTimeMs(getRetryBackoffTime, attempt + 1)
          cmds.foreach(c =>
            log.info(
              s"request ${c.logDisplay(CmdLogLevelHelper.InfoLevel)} failed. Retrying in $backoffTime ms. Attempt $attempt"))
          delay(backoffTime)
          closeCurrentSender()
          executePublishCommands(cmds, attempt + 1)
        case ex =>
          cmds.map { c =>
            log.error(s"request ${c.logDisplay(CmdLogLevelHelper.InfoLevel)} failed!", ex)
            MessagesErrorResult(new MessagesPublishException(ex.getMessage, ex))
          }
      }
    } else res.value
  }

  private def executeCreateStream(
      cmd: CreateMessagesClientStream
  ): MessagesResult =
    Option(streamMap.get(cmd.streamId)) match {
      case Some(_) =>
        MessagesErrorResult(
          new MessagesStreamCreationException(
            cmd.streamId,
            s"Messages Stream already exists with same id=${cmd.streamId}!"))
      case None =>
        val stream = new MessagesNotificationStream(cmd, this)
        streamMap.put(cmd.streamId, stream)
        // TODO (OPTIMUS-49663): Add client-side filter logic when notifying upstream.
        val classSubs = cmd.createStream.subs.map(_.eventClassName)
        createNewStream(cmd.createStream, classSubs)
        MessagesCreateClientStreamSuccessResult(cmd.streamId, stream)
    }

  private[messages] def createNewStream(
      createStrm: CreateMessagesStream,
      subs: Set[MessagesSubscription.EventClassName]
  ): Unit = {
    withRetry(Seq(createStrm)) { client =>
      client.createStream(createStrm)
      log.info(s"New messages stream initialized with id: ${createStrm.streamId} with Subscriptions: $subs")
    }
  }

  private[messages] def changeStreamSubscription(
      streamId: String,
      changeReqId: Int,
      newSubs: Set[MessagesSubscription],
      removeSubs: Set[MessagesSubscription]
  ): Unit = {
    Option(streamMap.get(streamId)) match {
      case None => throw new MessagesSubscriptionChangeException(streamId, changeReqId, s"Stream $streamId not found!")
      case Some(_) =>
        val cmd = ChangeMessagesSubscription(streamId, changeReqId, newSubs, removeSubs.map(_.subId))
        withRetry(Seq(cmd)) { client =>
          client.updateSubscription(cmd, newSubs.map(_.eventClassName), removeSubs.map(_.eventClassName))
        }
    }
  }

  private[messages] def closeStream(streamId: String): Unit = {
    Option(streamMap.get(streamId)) match {
      case None => log.warn(s"Stream $streamId does not exist. Ignoring close request.")
      case Some(_) =>
        val cmd = CloseMessagesStream(streamId)
        withRetry(Seq(cmd)) { client =>
          client.closeStream(cmd)
        }
    }
  }

  private[messages] def commitStream(
      streamId: String,
      commitIds: Seq[Long]
  ): Unit = {
    Option(streamMap.get(streamId)) match {
      case None => log.warn(s"Stream $streamId does not exist. Ignoring commit request.")
      case Some(_) =>
        try {
          val cmd = CommitMessagesStream(streamId, commitIds)
          getClient.commitStream(cmd)
        } catch {
          case NonFatal(ex) =>
            // This command is on best effort basis, so we ignore any failures and do not retry.
            log.warn(
              s"Commit command failed for stream $streamId. Will be re-attempted later! Reason: ${ex.getMessage}")
        }
    }
  }

  private[messages] def getClient: MessagesClient = getCurrentSender().asInstanceOf[MessagesClient]

  private[messages] def closeCurrentSender(): Unit = close(shutdown = false)
  private[messages] def closeSender(failed: Option[DalBrokerClient]): Unit = remoteProxy.close(failed)

  override def getRetryAttempts: Int = MessagesDsiProxyBase.operationRetryAttempts
  override def getRetryBackoffTime: Long = MessagesDsiProxyBase.operationRetryBackoffTime
  def getMaxRetryBackoffTime: Long = getOperationMaxRetryBackoffTimeMs()
}
