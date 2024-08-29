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

import java.util.concurrent.atomic.AtomicReference
import java.time.Instant

import msjava.slf4jutils.scalalog.getLogger
import optimus.dsi.notification.NotificationEntry
import optimus.dsi.partitioning.Partition
import optimus.dsi.partitioning.PartitionMap
import optimus.dsi.pubsub._
import optimus.platform.RuntimeEnvironment
import optimus.platform.TimeInterval
import optimus.platform.dal.DALPubSub._
import optimus.platform.dal.PartitionMapAPI
import optimus.platform.dal.pubsub.PubSubClientRequest.ChangeSubRequest
import optimus.platform.dal.pubsub.PubSubClientRequest.CloseStream
import optimus.platform.dsi.bitemporal._

import scala.jdk.CollectionConverters._
import scala.collection.mutable

object NotificationStreamImpl {
  private val log = getLogger(this)

  sealed trait Action
  final case class UnlinkAction(stream: NotificationStreamImpl) extends Action
}

private[optimus] class NotificationStreamImpl(
    override val id: ClientStreamId,
    addCmd: CreatePubSubStream,
    notificationStreamCallback: NotificationStreamCallback,
    partition: Partition,
    private[optimus] val pubSubRequestHandler: HandlePubSub,
    env: RuntimeEnvironment
) extends NotificationStream {
  import NotificationStreamImpl._

  private[this] val subscriptionStateManager = new SubscriptionStateManager(addCmd.subs)

  private[optimus] val serverSideStreamId: String = addCmd.streamId
  private[this] val lastSeenTxTime = new AtomicReference[Instant](TimeInterval.NegInfinity)
  private[this] val streamState = new AtomicReference[NotificationStreamState](NotificationStreamState.Uninitialized)
  private[this] val retryCmd = new AtomicReference[Option[CreatePubSubStream]](None)

  private[this] val clientStreamLogHeader = s"ClientStreamId: $id -"
  private[this] def clientAndServerStreamLogHeader(svid: String = serverSideStreamId) =
    s"$clientStreamLogHeader ServerStreamId: $svid -"

  private implicit val partitionMap: PartitionMap = PartitionMapAPI.getPartitionMapFromRuntimeEnv(env)

  // To hold client requests in case of stream getting refreshed or waiting for successful creation ack.
  private[this] val pendingClientRequests = new mutable.ListBuffer[PubSubClientRequest]

  // For unit tests..
  def sizeOfPendingClientRequests: Int = synchronized { pendingClientRequests.size }

  private[this] val msgHandler = createNotificationMessageHandler
  protected def createNotificationMessageHandler: NotificationMessageHandler =
    new AsyncNotificationMessageHandler(serverSideStreamId, id, notificationStreamCallback, () => close())

  private[optimus] def getLastSeenTt: Instant = lastSeenTxTime.get

  private[this] def updateTt(tt: Instant): Unit = lastSeenTxTime.set(tt)

  def isInitialized: Boolean = getState == NotificationStreamState.Initialized

  private[dal] def getState = streamState.get
  private[this] def updateState(state: NotificationStreamState): Unit = streamState.set(state)

  override def currentSubscriptions: Set[Subscription] = synchronized {
    subscriptionStateManager.currentSubscriptions()
  }

  override def changeSubscription(
      changeId: Int,
      subscriptionsToAdd: Set[Subscription],
      subscriptionsToRemove: Set[Subscription.Id]): Unit = {
    import PubSubHelper._
    require(
      subscriptionsToAdd.isEmpty || subscriptionsToAdd.inferredPartition == partition,
      "The subscriptions to add must be of the same partition with the stream"
    )

    pubSubRequestHandler.executePubSubRequest(
      ChangeSubRequest(
        ChangeSubscription(serverSideStreamId, changeId, subscriptionsToAdd.toSeq, subscriptionsToRemove.toSeq)))
  }

  private[pubsub] def subscriptionChangeRequest(req: PubSubClientRequest.ChangeSubRequest): Boolean =
    synchronized {
      getState match {
        case NotificationStreamState.OngoingRefresh | NotificationStreamState.Uninitialized =>
          log.info(
            s"${clientAndServerStreamLogHeader()} Stream is either undergoing refresh or not initialized yet, adding change subscription req: ${req.cmd.changeRequestId} in pending queue.")
          pendingClientRequests += req
          false
        case NotificationStreamState.PendingClosure | NotificationStreamState.Closed =>
          val err = new PubSubSubscriptionChangeException(
            req.cmd.changeRequestId,
            serverSideStreamId,
            "Stream has been closed and pending for ack.")
          handleErrorResult(ErrorResult(err))
          false
        case NotificationStreamState.Initialized =>
          subscriptionStateManager.subscriptionChangeRequest(req.cmd)
          true
      }
    }

  private[pubsub] def closeStreamRequest(req: PubSubClientRequest.CloseStream): Boolean = synchronized {
    getState match {
      case NotificationStreamState.OngoingRefresh | NotificationStreamState.Uninitialized =>
        log.info(
          s"${clientAndServerStreamLogHeader()} Stream is either undergoing refresh or not initialized yet, adding close req in pending queue.")
        pendingClientRequests.clear() // Other pending requests doesn't matter if stream is being closed.
        pendingClientRequests += req
        false
      case NotificationStreamState.PendingClosure | NotificationStreamState.Closed =>
        log.info(s"${clientAndServerStreamLogHeader()} Stream has already been closed and pending for ack.")
        false
      case NotificationStreamState.Initialized =>
        updateState(NotificationStreamState.PendingClosure)
        true
    }
  }

  /**
   * This method is invoked when there is a retry required. Depending on the current state of the stream, it will return
   * the command necessary to register the stream on a new pubsub broker.
   */
  protected[pubsub] def refreshStream(cause: Throwable): Option[CreatePubSubStream] = synchronized {
    getState match {
      case NotificationStreamState.OngoingRefresh =>
        // this means that there is an error while trying to refresh the stream
        retryCmd.get()
      case NotificationStreamState.PendingClosure | NotificationStreamState.Closed =>
        // PendingClosure means that the stream has been marked for closure. So we directly close it, without retrying.

        // Closed means this stream has been closed successfully but yet to be removed from streamMap of stream manager
        // or a stack variable. See also:
        //   optimus.platform.dal.pubsub.AbstractPubSubStreamRetryManager.generateStreamRetryCmds
        None
      case state =>
        // this means that the stream needs to be refreshed on a new pubsub broker
        log.warn(s"$clientStreamLogHeader Stream in state: $state is undergoing refresh due to: $cause")
        val addCmd = Some(generateCreatePubSubCmd())
        updateState(NotificationStreamState.OngoingRefresh)
        msgHandler.handleMessages(StreamDisconnectedMessage(cause) :: Nil)
        retryCmd.set(addCmd)
        addCmd
    }
  }

  /**
   * This method generates the CreatePubSubStream cmd by merging all the existing subs with the pending subChanges
   */
  protected[pubsub] def generateCreatePubSubCmd(): CreatePubSubStream = synchronized {
    val startTime = getState match {
      case NotificationStreamState.Uninitialized => addCmd.startTime
      case NotificationStreamState.Initialized   => Some(getLastSeenTt)
      case state =>
        throw new IllegalStateException(
          s"$clientStreamLogHeader Stream cannot be refreshed since it's in state: $state")
    }
    subscriptionStateManager.createRetryAddPubSubCmd(
      streamId = addCmd.streamId,
      startTime = startTime,
      endTime = addCmd.endTime,
      streamInitialized = getState != NotificationStreamState.Uninitialized,
      vtFilterInterval = addCmd.vtFilterInterval
    )
  }

  // This is getting invoked within synchronized block.
  private def executePendingClientRequests(): Unit = {
    val reqs = pendingClientRequests.result()
    pendingClientRequests.clear()
    // The following statement may eventually call into 'handleErrorResult(...)'. The messages generated by it will
    // reach the callback before the messages in 'notificationMessages' of 'handlePubSubResult(...)'. For example,
    // 'SubscriptionChangeFailed' may arrive before 'StreamCreationSucceeded'. Therefore, the callback cannot rely on
    // the order of these messages.
    pubSubRequestHandler.executePubSubRequestBatch(reqs)
  }

  /*
   * This method creates the corresponding NotificationMessage from the PuSubResult and passes it to the cb of stream.
   * It is also responsible for the state change after receiving the responses accordingly:
   *
   * Result                          | Current_State   | New_State
   * --------------------------------|-----------------|------------------------------------------------------------
   * CreatePubSubStreamSuccessResult | Uninitialized   | if(sowComplete) then RealTime; else Initialized
   * CreatePubSubStreamSuccessResult | OngoingRefresh  | if(sowComplete) then RealTime; else Initialized
   * ChangeSubscriptionSuccessResult | Initialized     | if(sowComplete) then RealTime;
   * PubSubSowResult                 | Initialized     | if(sowComplete) then RealTime;
   * PubSubNotificationResult        | RealTime        | ***Do NOT do any state change operations here (slowness)***
   * ClosePubSubStreamSuccessResult  | PendingClosure  | N/A
   */
  protected[pubsub] def handlePubSubResult(result: PubSubResult): Iterator[Action] = synchronized {
    val actionSeqBuilder = Seq.newBuilder[Action]
    log.trace(
      s"${clientAndServerStreamLogHeader(result.streamId)} Begin to handle PubSubResult: $result in state: ${streamState.get}")

    def handleDataNotification(
        serverStreamId: String,
        writeReqId: Option[String],
        txTime: Instant,
        data: Map[Subscription, Seq[NotificationEntry]]): Seq[NotificationMessage] = {
      log.trace(s"${clientAndServerStreamLogHeader(serverStreamId)} Received full data message at tt: $txTime")
      // Our de-dup logic here is simply to ignore messages with tt <= lastSeenTxTime.
      if (txTime.isAfter(getLastSeenTt)) {
        updateTt(txTime)
        Seq(DataNotificationMessage(txTime, writeReqId, data))
      } else Seq.empty
    }

    val notificationMessages = result match {
      case res @ CreatePubSubStreamSuccessResult(svid, tt) if getState == NotificationStreamState.OngoingRefresh =>
        require(
          retryCmd.get.isDefined,
          s"${clientAndServerStreamLogHeader(svid)} Since this is post-refresh, we expect the retryCmd to be defined!")
        val pendingChangeIds = subscriptionStateManager.pendingChanges.asScala.map { case (k, _) => k }
        val subChangeResponses = pendingChangeIds.map(SubscriptionChangeSucceeded(_, tt)).toSeq
        subscriptionStateManager.resetStateManager(postRefresh = true)
        subscriptionStateManager.subscriptionChangeResponse(res, retryCmd.get)
        // we are also acknowledging all the pending subChange requests, since they have been merged in the new request
        val allResponses = Seq(StreamCreationSucceeded(tt)) ++ subChangeResponses
        updateTt(tt)
        updateState(NotificationStreamState.Initialized)
        log.info(s"${clientAndServerStreamLogHeader(svid)} Stream re-established at tt: $tt")
        executePendingClientRequests()
        allResponses
      case res @ CreatePubSubStreamSuccessResult(svid, tt) =>
        subscriptionStateManager.subscriptionChangeResponse(res)
        updateTt(tt)
        updateState(NotificationStreamState.Initialized)
        log.info(s"${clientAndServerStreamLogHeader(svid)} Stream creation ack received at tt: $tt")
        executePendingClientRequests()
        Seq(StreamCreationSucceeded(tt))
      case res @ ChangeSubscriptionSuccessResult(svid, changeId, tt) =>
        subscriptionStateManager.subscriptionChangeResponse(res)
        updateTt(tt)
        log.info(
          s"${clientAndServerStreamLogHeader(svid)} Subscriptions successfully changed for changeRequestId: $changeId at tt: $tt")
        Seq(SubscriptionChangeSucceeded(changeId, getLastSeenTt))
      case sowResult: PubSubSowResult =>
        val data = subscriptionStateManager.sowResponse(sowResult)
        updateTt(sowResult.txTime)
        if (data.nonEmpty) {
          log.info(s"${clientAndServerStreamLogHeader(sowResult.streamId)} SOW completed at tt: ${sowResult.txTime}")
          Seq(SowNotificationMessage(sowResult.txTime, data))
        } else Seq.empty
      case res @ DirectPubSubNotificationPartialResult(svid, writeReqId, txTime, _, _) =>
        val mayBeFullResponse = subscriptionStateManager.partialDataResponse(res)
        log.trace(s"${clientAndServerStreamLogHeader(svid)} Received partial data message at tt: $txTime")
        mayBeFullResponse match {
          case Some(data) => handleDataNotification(svid, writeReqId, txTime, data)
          case None       => Seq.empty // We still haven't received full response, so do nothing..
        }
      case res @ DirectPubSubNotificationResult(svid, writeReqId, txTime, _) =>
        val data = subscriptionStateManager.dataResponse(res)
        handleDataNotification(svid, writeReqId, txTime, data)
      case x: ReferentialPubSubNotificationPartialResult => throw new MatchError(x)
      case x: ReferentialPubSubNotificationResult        => throw new MatchError(x)
      case PubSubHeartbeatResult(svid, txTime) =>
        log.trace(s"${clientAndServerStreamLogHeader(svid)} Received heartbeat message at tt: $txTime")
        updateTt(txTime)
        Seq(HeartbeatMessage(txTime))
      case sowStartResult: PubSubSowStartResult =>
        log.info(
          s"${clientAndServerStreamLogHeader(sowStartResult.streamId)} SOW started on server at tt: ${sowStartResult.txTime}")
        Seq(SowStartMessage(sowStartResult.txTime))
      case sowEndResult: PubSubSowEndResult =>
        log.info(
          s"${clientAndServerStreamLogHeader(sowEndResult.streamId)} SOW completed on server at tt: ${sowEndResult.txTime}")
        Seq(SowEndMessage(sowEndResult.txTime))
      case PubSubCatchupStartResult(svid, txTime) =>
        log.info(s"${clientAndServerStreamLogHeader(svid)} Background catchup started")
        Seq(CatchupStartMessage(txTime))
      case PubSubCatchupCompleteResult(svid, txTime) =>
        log.info(s"${clientAndServerStreamLogHeader(svid)} Background catchup completed ")
        Seq(CatchupCompleteMessage(txTime))
      case PubSubRepLagOverlimitResult(svid, lagTime) =>
        log.info(
          s"${clientAndServerStreamLogHeader(svid)} Replication lag above limit threshold, $lagTime milliseconds")
        Seq(ReplicationLagIncreaseEvent(lagTime))
      case PubSubRepLagUnderlimitResult(svid, lagTime) =>
        log.info(
          s"${clientAndServerStreamLogHeader(svid)} Replication lag below limit threshold, $lagTime milliseconds")
        Seq(ReplicationLagDecreaseEvent(lagTime))
      case PubSubUpstreamChangedResult(svid) =>
        log.info(s"${clientAndServerStreamLogHeader(svid)} Upstream connection changed")
        Seq(UpstreamChangedEvent)
      case PubSubBrokerConnect(svid) =>
        getState match {
          case NotificationStreamState.Closed =>
            Seq.empty
          case _ =>
            log.info(s"${clientAndServerStreamLogHeader(svid)} Connected to pubsub broker")
            Seq(PubSubBrokerConnectEvent)
        }
      case PubSubBrokerDisconnect(svid) =>
        getState match {
          case NotificationStreamState.Closed =>
            Seq.empty
          case _ =>
            log.info(s"${clientAndServerStreamLogHeader(svid)} Disconnected from pubsub broker")
            Seq(PubSubBrokerDisconnectEvent)
        }
      case ClosePubSubStreamSuccessResult(svid) =>
        log.info(s"${clientAndServerStreamLogHeader(svid)} Stream closed")
        shutdown().foreach(actionSeqBuilder += _)
        Seq.empty // EndOfStreamMessage is sent from 'doShutdown(...)'
    }
    msgHandler.handleMessages(notificationMessages)
    actionSeqBuilder.result().iterator
  }

  protected[pubsub] def handleErrorResult(result: ErrorResult): Unit = synchronized {
    getState match {
      case NotificationStreamState.Closed =>
      // Some of the error results are generated at the client level, then are broadcast to all streams. They should not
      // be delivered to the callback if this stream has been closed successfully.
      // See also: optimus.platform.dal.pubsub.AbstractPubSubStreamRetryManager.sendExceptionUpstream
      case _ =>
        log.info(s"$clientStreamLogHeader PubSubError! ${result.error}", result.error)
        val controlMessage = result.error match {
          case ex: PubSubStreamCreationException => StreamCreationFailed(ex)
          case ex: CreatePubSubStreamMultiPartitionException =>
            MultiPartitionStreamCreationFailed(ex.partitionToSubMap)
          case ex: PubSubStreamClosureException => StreamErrorMessage(ex)
          case ex: PubSubSubscriptionChangeException =>
            subscriptionStateManager.subscriptionChangeResponse(result)
            SubscriptionChangeFailed(ex.changeRequestId, getLastSeenTt)
          case ex: ChangeSubscriptionMultiPartitionException =>
            subscriptionStateManager.subscriptionChangeResponse(result)
            MultiPartitionSubscriptionChangeFailed(ex.changeRequestId, getLastSeenTt, ex.partitionToSubMap)
          case t =>
            StreamErrorMessage(t)
        }
        msgHandler.handleMessages(controlMessage :: Nil)
    }
  }

  override def close(): Unit =
    pubSubRequestHandler.executePubSubRequest(CloseStream(ClosePubSubStream(serverSideStreamId)))

  protected[pubsub] def shutdown(maybeThrowable: => Option[Throwable] = None): Iterator[Action] = synchronized {
    getState match {
      case NotificationStreamState.Closed => Iterator.empty
      case _ =>
        maybeThrowable match {
          case None =>
            msgHandler.handleMessages(EndOfStreamMessage :: Nil)
          case Some(th) =>
            log.info(s"$clientStreamLogHeader Closing stream due to $th")
            handleErrorResult(ErrorResult(th))
            msgHandler.handleMessages(EndOfStreamMessage :: Nil)
        }

        subscriptionStateManager.resetStateManager(postRefresh = false)
        msgHandler.shutdown()

        updateState(NotificationStreamState.Closed)
        Iterator(UnlinkAction(this))
    }
  }
}

sealed trait NotificationStreamState

object NotificationStreamState {
  case object Uninitialized extends NotificationStreamState
  case object Initialized extends NotificationStreamState
  case object OngoingRefresh extends NotificationStreamState
  case object PendingClosure extends NotificationStreamState
  case object Closed extends NotificationStreamState
}
