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
package optimus.platform.reactive.pubsub

import java.util.concurrent.atomic._
import java.time.Instant
import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.dsi.partitioning._
import optimus.dsi.pubsub._
import optimus.graph.tracking.EventCause
import optimus.graph.tracking.NoEventCause
import optimus.platform._
import optimus.platform.dal.DALImpl
import optimus.platform.dal.DALPubSub._
import optimus.platform.dal.pubsub.DalPubSubImpl
import optimus.platform.reactive.handlers.GlobalStatusSource
import optimus.platform.reactive.handlers.InputFailedEvent
import optimus.platform.reactive.handlers.InputFailedStatusEvent
import optimus.platform.reactive.handlers.PubSubStreamFailedEvent
import optimus.platform.reactive.handlers.PubSubTickingTxTimeFailedEvent
import optimus.platform.reactive.handlers.StateChangeEvent.StatusError

import scala.util.control.NonFatal

private[reactive] object TxTimeProvider {
  val log: Logger = getLogger(this)
}

// TODO (OPTIMUS-25792): clean-up this logic after external pubsub deco
private[reactive] class TxTimeProvider(val partition: Partition) {
  import TxTimeProvider.log

  val env = EvaluationContext.env

  class RegionalTxTimeCallback(streamId: Int) extends NotificationStreamCallback {

    override def notifyStreamEvent(id: ClientStreamId, msg: StreamEvent): Unit = {
      require(id == streamId, s"stream id should be $streamId for regional tx time provider for partition ${partition}")
      if (currentStreamId != streamId) {
        // This can happen if there was a race to create the stream, or if we receive notifications for a stream
        // that we have requested to close but which hasn't actually closed yet. The time provider should recover
        // from this state automatically
        log.info(
          s"ignoring message $msg - streamId($streamId) not currentStreamId($currentStreamId) for partition ${partition}")
      } else
        msg match {
          case HeartbeatMessage(tt) =>
            tryUpdateTT(tt)
          case StreamCreationSucceeded(tt) =>
            log.info(s"streamId($streamId): ticking now stream connected for partition ${partition}")
            tryUpdateTT(tt)

          // errors
          case StreamCreationFailed(exception) =>
            handleError("ticking now creation failed", exception)
          case MultiPartitionSubscriptionChangeFailed(changeRequestId, tt, partitionedSubs) =>
            handleError(
              "ticking now creation failed",
              new RuntimeException(s"MultiPartitionSubscriptionChangeFailed($changeRequestId, $tt, $partitionedSubs)"))
          case EndOfStreamMessage =>
            log.info(s"streamId($streamId): ticking now got end of stream $msg for partition ${partition}")
          case StreamErrorMessage(th) =>
            handleError("ticking now creation received error", th)
          case StreamDisconnectedMessage(th) =>
            // This is an informational message. The DAL client will attempt reconnection and if successful a
            // StreamCreationSucceeded will be published, and if unsuccessful a StreamCreationFailed, so we don't
            // propagate this one to the global status handler
            log.info(
              s"streamId($streamId): ticking now stream disconnected for partition ${partition}" +
                s" - connection retry will happen in the background",
              th)

          // unexpected messages
          case _: DataMessage =>
            unexpected(msg)
          case MultiPartitionStreamCreationFailed(partitionedSubs) =>
            unexpected(msg)
          case SubscriptionChangeSucceeded(changeRequestId, tt) =>
            unexpected(msg)
          case SubscriptionChangeFailed(changeRequestId, tt) =>
            unexpected(msg)
          case SowStartMessage(tt) =>
            unexpected(msg)
          case SowEndMessage(tt) =>
            unexpected(msg)
          case CatchupStartMessage(tt) =>
            unexpected(msg)
          case CatchupCompleteMessage(tt) =>
            unexpected(msg)
        }
    }

    private def handleError(detail: String, throwable: Throwable, notifyGlobalStatus: Boolean = true): Unit = {
      val msg = s"$detail for partition ${partition}"
      log.error(s"streamId($streamId): $msg", throwable)
      try
        if (notifyGlobalStatus) {
          val failedEvent = InputFailedEvent(Some(streamId), msg, StatusError(throwable, NoEventCause))
          GlobalStatusSource.publish(PubSubTickingTxTimeFailedEvent(failedEvent))
        }
      finally destroy(streamId)
    }

    private def tryUpdateTT(tt: Instant): Unit = {
      val currentTime = txTime
      if (currentTime == null || tt.isAfter(currentTime)) {
        log.trace(
          s"heartbeat time $tt is after current time $currentTime for partition ${partition}, going to update currentTime")
        val success = txTimeRef.compareAndSet(currentTime, tt)
        if (success) {
          log.trace(
            s"heartbeat time $tt is after current time $currentTime for partition ${partition}, done update currentTime")
        } else {
          log.error(
            s"streamId($streamId): heartbeat time $tt is after current time $currentTime for partition ${partition}, " +
              s"but someone others updated currentTime to $txTime")
        }
      } else if (tt == currentTime) {
        log.trace(s"heartbeat time is same with current time for partition ${partition}, ignore: $currentTime")
      } else {
        throw new IllegalStateException(
          s"heartbeat time $tt is before current time $currentTime for partition ${partition}!")
      }
    }

    def unexpected(msg: StreamEvent): Unit =
      log.info(s"streamId($streamId): ticking now discarding unexpected message - $msg")

    override def notifyGlobalEvent(id: ClientStreamId, msg: GlobalEvent): Unit = {
      if (currentStreamId != streamId)
        log.warn(s"ignoring message $msg - streamId($streamId) not currentStreamId($currentStreamId)")
      else
        msg match {
          case ReplicationLagIncreaseEvent(lagTime) =>
            log.warn(s"streamId($streamId): ticking now delays - $msg")
          case ReplicationLagDecreaseEvent(lagTime) =>
            log.info(s"streamId($streamId): ticking now delays - $msg")
          case UpstreamChangedEvent =>
            log.info(s"streamId($streamId): ticking now upstream changed - $msg")
          case PubSubBrokerConnectEvent =>
            log.info(s"streamId($streamId): ticking now broker connected - $msg")
          case PubSubBrokerDisconnectEvent =>
            log.warn(s"streamId($streamId): ticking now broker disconnected  - $msg")
          case PubSubDelayEvent(types) =>
            log.warn(s"streamId($streamId): ticking delay for types $types")
          case PubSubDelayOverEvent(types) =>
            log.info(s"streamId($streamId): ticking delay recovered for types $types")
        }
    }
  }

  private val txTimeRef = new AtomicReference[Instant](null)
  private[pubsub] def txTime: Instant = txTimeRef.get()

  private val dalPsRef = new AtomicReference[DalPubSubImpl]()
  private[pubsub] def dalPs: DalPubSubImpl = dalPsRef.get()

  private val streamRef = new AtomicReference[NotificationStream]
  private[pubsub] def stream: NotificationStream = streamRef.get()
  def currentStreamId: Int =
    Option(stream)
      .map(_.id)
      .getOrElse(
        -1
      ) // primarily integrity checks since a different callback object is created and used for each TxTimeProvider

  private val streamIdGen = new AtomicInteger()

  def getTxTime(timeoutMillis: Long): Instant = {
    val timeout = System.currentTimeMillis() + timeoutMillis
    while (txTime == null && System.currentTimeMillis() < timeout) {
      delay(100)
      log.trace(s"waiting to get first TT for partition ${partition}")
    }
    if (txTime eq null)
      log.warn(s"failed to get first TT for partition ${partition} after $timeoutMillis ms")
    txTime
  }

  def ensureConnected(): Unit = Option(stream).getOrElse(reconnect())

  private def reconnect(): Unit = {
    val id = streamIdGen.incrementAndGet()
    log.info(s"Reconnecting: incremented stream id gen from ${id - 1} to $id")

    if (dalPs == null) {
      log.info("Initialising DalPS")
      val newDalPs = new DalPubSubImpl(DALImpl.resolver, env)

      if (dalPsRef.compareAndSet(null, newDalPs)) {
        log.info("DalPS initialised and set successfully")
        // Close the stream and null out the DalPS if the environment is shutdown
        env.addShutdownAction {
          log.info("RuntimeEnvironment was shutdown: closing stream and clearing DalPS")
          destroy()
          dalPsRef.set(null)
        }
      } else {
        log.info("DalPS was initialised but someone beat us to setting it")
      }
    }

    if (stream == null) {
      val newStream = createStream(dalPs, id)
      if (!streamRef.compareAndSet(null, newStream)) {
        log.info(
          s"TickingNotificationStream with id $currentStreamId for partition ${partition} was initialised but someone beat us to setting it. Will now be closed")
        // Close the created stream if we lost the race
        closeStream(newStream)
      } else {
        log.info(
          s"TickingNotificationStream with id $currentStreamId for partition ${partition} initialised and set successfully")
      }
    }
  }

  private def createStream(curDalPs: DalPubSubImpl, id: Int): NotificationStream = {
    // TickingTxTime was requested and then environment was shutdown so the request must fail
    if (curDalPs == null) throw new IllegalStateException("TickingTxTime requested for a shutdown environment")

    curDalPs.createTickingNotificationStream(
      streamId = id,
      subscriptions = Set(Subscription(partition)),
      cb = new RegionalTxTimeCallback(id),
      startTime = None,
      endTime = TimeInterval.Infinity,
      partition = partition
    )
  }

  def destroy(): Unit = {
    log.info(
      s"Destroying TickingNotificationStream with id $currentStreamId for partition ${partition} due to env shutdown or reset")
    // Clear the current stream and shut it down.
    // Clearing the stream makes currentStreamId (effectively) -1 so future messages received on that stream will be ignored
    Option(streamRef.getAndSet(null)).foreach(closeStream)
  }

  private def destroy(streamId: Int): Unit = {
    val curStream = stream
    if (curStream.id == streamId) {
      if (streamRef.compareAndSet(curStream, null)) {
        log.info(
          s"Close TickingNotificationStream with id $currentStreamId for partition ${partition} requested from callback and successfully cleared stream")
        closeStream(curStream)
      } else {
        log.info(
          s"Close TickingNotificationStream with id $currentStreamId for partition ${partition} requested from callback but somebody beat us to setting a new one")
      }
    } else if (streamId < curStream.id) {
      log.info(
        s"Close TickingNotificationStream requested with id $currentStreamId for partition ${partition} less than current stream id: ${curStream.id}")
    } else {
      log.info(
        s"Close TickingNotificationStream requested with id $currentStreamId for partition ${partition} greater than current stream id: ${curStream.id}")
    }
  }

  private def closeStream(toClose: NotificationStream): Unit = {
    log.info(s"Closing TickingNotificationStream for partition ${partition} with id: ${toClose.id}")

    try toClose.close()
    catch {
      case NonFatal(e) => log.warn(s"Stream close failed with exception: $e")
    }
  }
  log.info(s"txtimeprovider created for partition : ${partition}")
}
