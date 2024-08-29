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
package optimus.platform.dal.pubsub

import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import msjava.slf4jutils.scalalog.getLogger
import optimus.dsi.pubsub.CustomThreadGroup
import optimus.platform.dal.DSIClientCommon
import optimus.platform.dal.session.ClientSessionContext
import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.protobufutils._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

abstract class AbstractPubSubStreamRetryManager(
    maxRetryCount: Int,
    retryBackoffTime: Long,
    clientSessionContext: ClientSessionContext)
    extends PubSubStreamManager {
  import AbstractPubSubStreamRetryManager._

  private[this] val retryCount = new AtomicInteger(0)

  private[this] val retryHandlerThreadPool = {
    val factory = new ThreadFactory {
      override def newThread(runnable: Runnable): Thread = {
        val thread =
          new Thread(
            retryThreadGroup,
            runnable,
            s"AbstractPubSubStreamManager-${retryHandlerThreadPoolCount.incrementAndGet}")
        thread.setDaemon(true)
        thread
      }
    }
    Executors.newSingleThreadExecutor(factory)
  }

  private[this] val retryHandlerExecutionContext = ExecutionContext.fromExecutorService(retryHandlerThreadPool)
  // necessary for overriding in tests
  protected def getExecutionContext: ExecutionContext = retryHandlerExecutionContext

  // We keep an instance of last known good client to make sure that commands like ChangeSubscription/ClosePubSubStream
  // are sent using the same "client" (i.e., to same broker host) which created those streams. This way we avoid the
  // edge condition where due to some transient issue streams are getting reconnected, but before that happens
  // ChangeSubscription/ClosePubSubStream commands are sent to another broker, which doesn't have corresponding streams
  // yet and fails.
  private[this] val lastKnownGoodBrokerClient = new AtomicReference[DalBrokerClient]()

  override protected def executePubSubCommands(cmds: Seq[PubSubCommand]): Unit = {

    def doExecute(brokerClient: DalBrokerClient): Unit = {
      val metadata = batchCtxMetadata.get
      val pubSubBatchCtx = new PubSubBatchContext(
        cmds,
        UUID.randomUUID.toString,
        metadata.get.chainedID,
        clientSessionContext,
        metadata.get.appNameTag)
      brokerClient.sendBatch(pubSubBatchCtx)
    }

    val brokerClient =
      try {
        Some(getCurrentSender)
      } catch {
        case th: Throwable =>
          handlePubSubSendFailure(None, th)
          None
      }
    try {
      brokerClient.foreach { b =>
        val areAllCreateCmds = cmds.forall(_.isInstanceOf[CreatePubSubStream])
        if (areAllCreateCmds) {
          doExecute(b)
          lastKnownGoodBrokerClient.set(b)
        } else {
          // Means ChangeSubscription/ClosePubSubStream commands are present. Make sure that
          // we execute those commands only if client is the same that created those streams.
          // Else it is possible that retry is happening due to a transient issue, and we
          // simply trigger yet another retry.
          Option(lastKnownGoodBrokerClient.get) match {
            case Some(previousClient) if previousClient == b => doExecute(b)
            case mayBepreviousClient =>
              handlePubSubSendFailure(
                mayBepreviousClient,
                new DALRetryableActionException(
                  s"Retrying ${cmds.size} non-create command(s) because previous client $mayBepreviousClient is not the same as current client $b",
                  mayBepreviousClient)
              )
          }
        }
      }
    } catch {
      case retryable: DALRetryableActionException =>
        handlePubSubSendFailure(brokerClient, retryable)
      case th: Throwable =>
        // we are making sure that ongoing streams aren't affected by non-retryable errors for a single stream
        cmds foreach { pubSubCmd =>
          val error = pubSubCmd match {
            case _: CreatePubSubStream => new PubSubStreamCreationException(pubSubCmd.streamId, th.getMessage)
            case _: ClosePubSubStream  => new PubSubStreamClosureException(pubSubCmd.streamId, th.getMessage)
            case ChangeSubscription(_, changeId, _, _) =>
              new PubSubSubscriptionChangeException(changeId, pubSubCmd.streamId, th.getMessage)
          }
          // given the exception is not retryable, passing brokerClient as None, just to make it explicit that
          // the current client shouldn't be closed in any way
          handlePubSubSendFailure(None, error)
        }
    }
  }

  private def doRetry(rt: DALRetryableActionException): Unit = {
    if (retryCount.incrementAndGet() < maxRetryCount) {

      // We close current proto client, if any, and start afresh. This might incur unnecessary overhead in some
      // scenarios, but those are very rare, so, simplifying the logic. Note that it could retry recursively which
      // need to be avoided.
      allowRetry.set(false)
      closeCurrentSender()
      allowRetry.set(true)

      val backoff = calculateBackoffTime(retryBackoffTime, retryCount.get)
      log.info(s"Retrying (count#${retryCount.get}) in $backoff millis due to cause: ${extractRtCause(rt)}")
      Thread.sleep(backoff)

      val cmdsToBeRetried = generateStreamRetryCmds(rt)
      notifyRetryCommandsGenerated(cmdsToBeRetried)

      if (cmdsToBeRetried.nonEmpty) {
        executePubSubCommands(cmdsToBeRetried)
      } else {
        // there are no streams to be retried, so send the error upstream
        val cause = extractRtCause(rt)
        sendExceptionUpstream(cause)
      }
    } else if (retryCount.getAndIncrement() == maxRetryCount) {
      log.warn(s"Run out of retries, giving up: $rt")
      val cause = extractRtCause(rt)
      sendExceptionUpstream(cause)
    }
  }

  // Keeping the state to avoid recursive retry..
  private[this] val allowRetry = new AtomicBoolean(true)
  private def handlePubSubRetry(failedClient: Option[DalBrokerClient], rt: DALRetryableActionException): Unit = {
    if (allowRetry.get) {
      Future {
        try {
          closeSender(failedClient)
          doRetry(rt)
        } catch {
          case _: InterruptedException =>
            log.info("Retry interrupted, probably runtime is shutting down")
          case ex: Throwable =>
            log.error(s"retry failed.", ex)
            sendExceptionUpstream(ex)
        }
      }(getExecutionContext)
    }
  }

  /**
   * This public API will be called due to following reasons:
   *   1. pubSubProtoClient receives some ErrorResult 2. stream disconnect received 3. Zk disconnect received For 2 & 3,
   *      the invocation is from PubSubProtoClient.shutdown()
   */
  def handlePubSubException(failedClient: DalBrokerClient, th: Throwable): Unit = {
    handlePubSubSendFailure(Some(failedClient), th)
  }

  private[optimus] def remoteStreamCleanup(streamId: String): Unit = {
    if (!super.checkStreamExists(streamId)) {
      Future {
        log.debug(s"remoteStreamCleanup called for Stream#$streamId")
        executePubSubCommands(Seq(ClosePubSubStream(streamId)))
      }(getExecutionContext)
    } else {
      log.warn(s"remoteStreamCleanup called for existing Stream#$streamId")
    }
  }

  private def handlePubSubSendFailure(failedClient: Option[DalBrokerClient], th: Throwable): Unit = {
    if (th.getCause.isInstanceOf[PubSubStreamDisconnectedException])
      onDisconnect()
    th match {
      case rt: DALRetryableActionException =>
        handlePubSubRetry(failedClient, rt)
      case _: Throwable =>
        // we won't retry it here
        sendExceptionUpstream(th)
    }
  }

  private def sendExceptionUpstream(th: Throwable): Unit = {
    val err = ErrorResult(th)
    val errorResults = th match {
      case pse: PubSubException if !pse.isInstanceOf[PubSubTransientException] =>
        // we will broadcast a PubSubTransientException to all the streams, since, we have run out of retries
        Seq(pse.streamId -> err)
      case _ =>
        super.getCurrentStreamObjs.map { streamObj =>
          streamObj.serverSideStreamId -> err
        }
    }
    super.handlePubSubResult(errorResults)
  }

  override def executePubSubRequest(request: PubSubClientRequest): PubSubClientResponse =
    executePubSubRequestImpl(Seq(request)).head

  override def executePubSubRequestBatch(requests: Seq[PubSubClientRequest]): Seq[PubSubClientResponse] =
    executePubSubRequestImpl(requests)

  def closeStreamManager(): Unit = {
    retryHandlerThreadPool.shutdownNow()
    super.shutdownAllStreams()
  }

  def generateStreamRetryCmds(rt: DALRetryableActionException): Seq[CreatePubSubStream] = {
    super.getCurrentStreamObjs.flatMap { streamObj =>
      if (streamObj.getState != NotificationStreamState.OngoingRefresh && retryCount.get > 1)
        retryCount.set(0)
      val cmdOpt = streamObj.refreshStream(extractRtCause(rt))
      if (cmdOpt.isEmpty) {
        // it means that we don't want to retry on this stream (either pending closure state or run out of retries)
        super.sendErrorAndCloseStream(streamObj.serverSideStreamId, extractRtCause(rt))
        None
      } else cmdOpt
    }
  }

  private def extractRtCause(rt: DALRetryableActionException) = Option(rt.getCause).getOrElse(rt)

  def closeSender(failed: Option[DalBrokerClient]): Unit
  def closeCurrentSender(): Unit
  def getCurrentSender: DalBrokerClient

  // Testing hooks.
  protected def notifyRetryCommandsGenerated(retryCmds: Seq[PubSubCommand]): Unit = {}
}

object AbstractPubSubStreamRetryManager {
  private val log = getLogger(this)
  private val retryHandlerThreadPoolCount = new AtomicInteger(0)
  private lazy val retryThreadGroup = new CustomThreadGroup("PubSubStreamRetry")

  def calculateBackoffTime(seedBackoffTime: Long, retryAttempt: Int): Long =
    DSIClientCommon.randomizedBackOffTime(
      DSIClientCommon.boundedBackOffTime(Math.round(seedBackoffTime * Math.pow(2, retryAttempt - 1))))
}
