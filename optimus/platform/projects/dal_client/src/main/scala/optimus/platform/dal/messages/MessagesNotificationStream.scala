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
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.dsi.pubsub.CustomThreadGroup
import optimus.graph.DiagnosticSettings
import optimus.logging.LoggingInfo
import optimus.platform.dal.NotificationStream
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.Host
import optimus.platform.dal.messages.MessagesEvent._
import optimus.platform.dal.pubsub.NotificationStreamState
import optimus.platform.dsi.DalMessagesClientCrumbSource
import optimus.platform.dsi.bitemporal.CreateMessagesClientStream
import optimus.platform.dsi.bitemporal.CreateMessagesStream
import optimus.platform.dsi.bitemporal.DirectMessagesNotificationResult
import optimus.platform.dsi.bitemporal.MessagesNotificationResult
import optimus.platform.dsi.bitemporal.MessagesStreamCreationException
import optimus.platform.dsi.bitemporal.MessagesStreamDisconnectException
import optimus.platform.dsi.bitemporal.MessagesSubscriptionChangeException
import spray.json.DefaultJsonProtocol._

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

object MessagesNotificationStream {

  private val log = getLogger(this)
  private val useAsyncCallbackInvoker =
    DiagnosticSettings.getBoolProperty("optimus.dal.messages.useAsyncCallbackInvoker", true)

  sealed trait MessagesStreamRequest

  final case class SubscriptionChangeRequest(
      changeId: Int,
      toAdd: Set[MessagesSubscription],
      toRemove: Set[MessagesSubscription.Id])
      extends MessagesStreamRequest
  private case object CloseRequest extends MessagesStreamRequest
}

class MessagesNotificationStream(
    cmd: CreateMessagesClientStream,
    proxy: MessagesDsiProxyBase
) extends DalMessages.MessagesNotificationStream {
  import MessagesNotificationStream._

  override val id = cmd.streamId
  private val streamId = cmd.streamId
  private val logHeader = s"StreamId: $streamId -"

  protected val presentSubs =
    new ConcurrentHashMap[MessagesSubscription.Id, MessagesSubscription](cmd.createStream.subs.size)
  // For dynamic subscription changes.
  protected val pendingSubscriptionChanges = new ConcurrentHashMap[Int, SubscriptionChangeRequest]()
  // When stream is in uninitialized state and waiting for successful creation ack.
  protected val pendingStreamRequests = mutable.ListBuffer[MessagesStreamRequest]()

  protected val currentState = new AtomicReference[NotificationStreamState](NotificationStreamState.Uninitialized)
  private[optimus] def getState = currentState.get

  protected def createNewAsyncMessagesCallbackInvoker(
      streamId: String,
      callback: MessagesNotificationCallback,
      close: () => Unit,
      commitStream: Seq[Long] => Unit,
      dalEnv: DalEnv
  ) = new AsyncMessagesCallbackInvoker(streamId, callback, close, commitStream, dalEnv)

  private val callbackInvoker =
    if (useAsyncCallbackInvoker)
      createNewAsyncMessagesCallbackInvoker(
        streamId = streamId,
        callback = cmd.callback,
        close = close,
        commitStream = proxy.commitStream(streamId, _),
        dalEnv = proxy.dalEnv
      )
    else new SyncMessagesCallbackInvoker(streamId, cmd.callback, close, proxy.commitStream(streamId, _), proxy.dalEnv)

  def onDisconnect(): Unit =
    callbackInvoker.notifyMessage(MessagesBrokerDisconnectEvent)

  def onConnect(): Unit =
    callbackInvoker.notifyMessage(MessagesBrokerConnectEvent)

  override def changeSubscription(
      changeId: Int,
      subscriptionsToAdd: Set[MessagesSubscription],
      subscriptionsToRemove: Set[NotificationStream.SubscriptionIdType] = Set.empty
  ): Unit = synchronized {
    val state = currentState.get
    val changeInfo = SubscriptionChangeRequest(changeId, subscriptionsToAdd, subscriptionsToRemove)
    state match {
      case NotificationStreamState.Uninitialized => pendingStreamRequests += changeInfo
      case NotificationStreamState.Initialized =>
        log.info(
          s"$logHeader change subscription request received - add: ${subscriptionsToAdd.size}, remove: ${subscriptionsToRemove.size}")
        if (pendingSubscriptionChanges.containsKey(changeId))
          onStreamError(
            SubscriptionChangeFailed(
              changeId,
              streamId,
              new MessagesSubscriptionChangeException(
                streamId,
                changeId,
                s"$logHeader Duplicate subscription change request received!")))
        else {
          val (oldSubs, missingSubs) =
            subscriptionsToRemove.map(sid => sid -> findExistingSubscription(sid)).partition(_._2.nonEmpty)
          if (missingSubs.nonEmpty)
            onStreamError(
              SubscriptionChangeFailed(
                changeId,
                streamId,
                new MessagesSubscriptionChangeException(
                  streamId,
                  changeId,
                  s"$logHeader Existing subscription(s) ${missingSubs.map(_._1)} not found")))
          else {
            pendingSubscriptionChanges.putIfAbsent(changeId, changeInfo)
            proxy.changeStreamSubscription(streamId, changeId, subscriptionsToAdd, oldSubs.flatMap(_._2))
          }
        }
      case NotificationStreamState.Closed | NotificationStreamState.PendingClosure =>
        log.warn(s"$logHeader Stream already in close / closing state, ignoring subscription request $changeId.")
        onStreamError(
          SubscriptionChangeFailed(
            changeId,
            streamId,
            new MessagesSubscriptionChangeException(streamId, changeId, s"$logHeader Stream is in: $state state")))
      case NotificationStreamState.OngoingRefresh =>
        pendingSubscriptionChanges.putIfAbsent(changeId, changeInfo)
    }
  }

  override def close(): Unit = synchronized {
    val state = currentState.get
    state match {
      case NotificationStreamState.Uninitialized => pendingStreamRequests += CloseRequest
      case curState @ NotificationStreamState.Initialized =>
        callbackInvoker.forceCommitOutstandingIds()
        proxy.closeStream(streamId)
        updateState(curState, NotificationStreamState.PendingClosure)
      case NotificationStreamState.PendingClosure | NotificationStreamState.Closed =>
        log.warn(s"$logHeader Already closing / closed, ignoring close request!")
      case NotificationStreamState.OngoingRefresh =>
        pendingStreamRequests += CloseRequest
    }
  }

  override def currentSubscriptions: Set[MessagesSubscription] = presentSubs.asScala.values.toSet

  private def findExistingSubscription(id: MessagesSubscription.Id): Option[MessagesSubscription] = {
    Option(presentSubs.get(id))
      .orElse {
        pendingSubscriptionChanges.asScala.flatMap { case (_, info) => info.toAdd.find(_.subId == id) }.headOption
      }
  }

  private def updateState(curState: NotificationStreamState, newState: NotificationStreamState): Unit = {
    currentState.compareAndSet(curState, newState)
    log.info(s"$logHeader State changed from $curState to $newState")
  }

  private[optimus] def commitStream(commitIds: Seq[Long]): Unit = callbackInvoker.putIdsForCommit(commitIds)

  private[messages] def onStreamError(errorEvent: StreamErrorEvent): Unit = {
    log.error(errorEvent.error.getMessage, errorEvent.error)
    callbackInvoker.notifyMessage(errorEvent)
  }

  private def executePendingRequests(): Unit = {
    val results = pendingStreamRequests.result() ++ pendingSubscriptionChanges.values().asScala.toList
    pendingStreamRequests.clear()
    pendingSubscriptionChanges.clear()
    results.foreach {
      case SubscriptionChangeRequest(changeId, toAdd, toRemove) => changeSubscription(changeId, toAdd, toRemove)
      case CloseRequest                                         => close()
    }
  }

  def onMessageReceived(
      msgs: Seq[DirectMessagesNotificationResult]
  ): Unit = {
    val currentSubs = currentSubscriptions.map(_.eventClassName)
    msgs.foreach { msg =>
      msg.entries.foreach {
        case eventEntry: MessagesNotificationResult.SimpleEntry =>
          val className = eventEntry.className
          if (currentSubs.contains(className))
            callbackInvoker.notifyMessage(
              MessagesDataNotification(
                streamId = streamId,
                eventClassName = className,
                payload = eventEntry.serializedMsg,
                commitId = msg.commitId,
                pubReqId = msg.publishReqId
              )
            )
        case transactionEntry: MessagesNotificationResult.TransactionEntry =>
          val className = transactionEntry.className
          if (currentSubs.contains(className))
            callbackInvoker.notifyMessage(
              MessagesTransactionNotification(
                streamId = streamId,
                className = className,
                payload = transactionEntry.serializedMsg,
                commitId = msg.commitId,
                pubReqId = msg.publishReqId
              )
            )
      }
    }
  }

  private def resetStreamSubs() = presentSubs.clear()

  def onStreamCreationSuccess(): Unit = synchronized {
    getState match {
      case NotificationStreamState.OngoingRefresh =>
        updateState(NotificationStreamState.OngoingRefresh, NotificationStreamState.Initialized)
        require(retryCmd.get.isDefined, s" Since this is post-refresh, we expect the retryCmd to be defined!")
        resetStreamSubs()
        retryCmd.get().get.subs.foreach(s => presentSubs.put(s.subId, s))
        retryCmd.set(None)
        callbackInvoker.notifyMessage(StreamCreationSucceeded(streamId))
        executePendingRequests()
      case NotificationStreamState.Uninitialized =>
        cmd.createStream.subs.foreach(s => presentSubs.put(s.subId, s))
        updateState(NotificationStreamState.Uninitialized, NotificationStreamState.Initialized)
        executePendingRequests()
        callbackInvoker.notifyMessage(StreamCreationSucceeded(streamId))
      case state =>
        onStreamError(
          MessagesStreamError(
            streamId,
            new MessagesStreamCreationException(streamId, s"$logHeader Stream cannot be in state: $state")))
    }
  }

  def onSubscriptionUpdated(changeId: Int): Unit = synchronized {
    Option(pendingSubscriptionChanges.get(changeId)) match {
      case None =>
        log.warn(s"Subscription change request $changeId not found in pending changes. Perhaps stream got closed!")
      case Some(info) =>
        info.toAdd.foreach(s => presentSubs.put(s.subId, s))
        info.toRemove.foreach(sid => presentSubs.remove(sid))
        pendingSubscriptionChanges.remove(changeId)
        callbackInvoker.notifyMessage(SubscriptionChangeSucceeded(changeId, streamId))
    }
  }

  def onSubscriptionError(streamError: StreamErrorEvent): Unit = {
    streamError match {
      case sub: SubscriptionChangeFailed                          => pendingSubscriptionChanges.remove(sub.changeId)
      case StreamCreationFailed(_, _) | MessagesStreamError(_, _) =>
    }
    callbackInvoker.notifyMessage(streamError)
  }

  def onStreamClosed(): Unit = synchronized {
    callbackInvoker.notifyMessage(StreamCloseSucceeded(id))
    shutdown()
  }

  private[this] val retryCmd = new AtomicReference[Option[CreateMessagesStream]](None)

  /**
   * This method is invoked when there is a retry required. Depending on the current state of the stream, it will return
   * the command necessary to register the stream on a new messages broker.
   */
  protected[messages] def refreshStream(cause: Throwable): Option[CreateMessagesStream] = synchronized {
    getState match {
      case NotificationStreamState.OngoingRefresh =>
        // this means that there is an error while trying to refresh the stream
        retryCmd.get()
      case NotificationStreamState.PendingClosure | NotificationStreamState.Closed =>
        // PendingClosure means that the stream has been marked for closure. So we directly close it, without retrying.
        // Closed means this stream has been closed successfully but yet to be removed from streamMap
        // or a stack variable.
        None
      case state =>
        // this means that the stream needs to be refreshed on a new messages broker
        log.warn(s"$logHeader Stream in state: $state is undergoing refresh due to: $cause")
        val addCmd = Some(generateMessagesCmd())
        updateState(state, NotificationStreamState.OngoingRefresh)
        callbackInvoker.notifyMessage(
          MessagesStreamDisconnect(streamId, new MessagesStreamDisconnectException(cause.getMessage)))
        retryCmd.set(addCmd)
        addCmd
    }
  }

  /**
   * This method generates the CreateMessagesStream cmd by merging all the existing subs with the pending subChanges
   */
  protected[messages] def generateMessagesCmd(): CreateMessagesStream = synchronized {
    val state = getState
    if (state != NotificationStreamState.Initialized && state != NotificationStreamState.Uninitialized)
      throw new IllegalStateException(s"$logHeader Stream cannot be refreshed since it's in state: $state")
    val subs =
      if (state == NotificationStreamState.Initialized) currentSubscriptions
      else cmd.createStream.subs
    cmd.createStream.copy(subs = subs)
  }

  private[messages] def shutdown(maybeThrowable: => Option[Throwable] = None): Unit = synchronized {
    getState match {
      case NotificationStreamState.Closed => Iterator.empty
      case _ =>
        maybeThrowable match {
          case None =>
          case Some(th) =>
            log.info(s"$logHeader Closing stream due to $th")
            callbackInvoker.notifyMessage(MessagesStreamError(streamId, th))
        }
        pendingSubscriptionChanges.clear()
        presentSubs.clear()
        callbackInvoker.shutdown()
        updateState(getState, NotificationStreamState.Closed)
    }
  }
}

object MessagesCallbackInvoker {
  private val log = getLogger(this)
}

sealed trait MessagesCallbackInvoker {
  import MessagesCallbackInvoker._
  import MessagesEvent._

  val streamId: String
  val callback: MessagesNotificationCallback
  protected val closeStream: () => Unit
  protected val commitStream: Seq[Long] => Unit
  val dalEnv: DalEnv

  private[messages] def sendMessageNotificationCrumb(
      notification: StreamNotificationEvent,
      clientReceiveTime: Long
  ): Unit = {
    import spray.json._
    val dataMap = notification match {
      case event: MessagesDataNotification =>
        Map(
          "env" -> dalEnv.mode,
          "type" -> "notification",
          "streamId" -> notification.streamId,
          "reqId" -> event.pubReqId,
          "className" -> event.eventClassName,
          "clientReceiveTime" -> clientReceiveTime.toString
        )
      case transaction: MessagesTransactionNotification =>
        Map(
          "env" -> dalEnv.mode,
          "type" -> "notification",
          "streamId" -> notification.streamId,
          "reqId" -> transaction.pubReqId,
          "className" -> transaction.className,
          "clientReceiveTime" -> clientReceiveTime.toString
        )
    }

    val clientProps = Map(
      "host" -> Host(LoggingInfo.getHost).toStringNoSuffix(),
      "pid" -> LoggingInfo.pid.toString
    )
    Breadcrumbs.info(
      ChainedID.root,
      PropertiesCrumb(_, DalMessagesClientCrumbSource, dataMap, Map("clientSummary" -> clientProps.toJson))
    )
  }

  final def notifyMessage(msg: MessagesEvent): Unit = msg match {
    case g: GlobalEvent => notifyGlobalEvent(g)
    case s: StreamEvent => notifyStreamEvent(s)
  }

  protected def notifyGlobalEvent(evt: GlobalEvent): Unit =
    invokeSafely[GlobalEvent](evt, ev => callback.notifyGlobalEvent(streamId, ev))

  protected def notifyStreamEvent(evt: StreamEvent): Unit

  final protected def safelyDeliverStreamErrorMessage(th: Throwable): Unit = { // noexcept
    try {
      callback.notifyStreamEvent(streamId, MessagesStreamError(streamId, th))
    } catch {
      case ex: Exception =>
        log.error("Error while delivering StreamErrorMessage", ex)
    }
  }

  final protected def invokeSafely[ME <: MessagesEvent](evt: ME, f: ME => Unit): Unit = {
    try {
      f(evt)
    } catch {
      case NonFatal(th) =>
        log.error(
          s"Uncaught exception occurred while invoking messages stream ($streamId) callback for evt: $evt. Stream will be closed.",
          th)
        invokeStreamErrorSafely(th)
        closeStream()
    }
  }

  final protected def invokeStreamErrorSafely(th: Throwable): Unit = {
    try {
      callback.notifyStreamEvent(streamId, MessagesStreamError(streamId, th))
    } catch {
      case NonFatal(ex) =>
        log.error(s"Error while delivering StreamErrorEvent for stream $streamId", ex)
    }
  }

  def shutdown(): Unit

  def putIdsForCommit(ids: Seq[Long]): Unit

  def forceCommitOutstandingIds(): Unit
}

class SyncMessagesCallbackInvoker(
    override val streamId: String,
    override val callback: MessagesNotificationCallback,
    override val closeStream: () => Unit,
    override val commitStream: Seq[Long] => Unit,
    override val dalEnv: DalEnv
) extends MessagesCallbackInvoker {
  import MessagesEvent._

  override protected def notifyStreamEvent(evt: StreamEvent): Unit =
    invokeSafely[StreamEvent](evt, ev => callback.notifyStreamEvent(streamId, ev))

  override def shutdown(): Unit = {}

  override def putIdsForCommit(ids: Seq[Long]): Unit = commitStream(ids)

  override def forceCommitOutstandingIds(): Unit = {}
}

object AsyncMessagesCallbackInvoker {
  sealed trait Status
  case object InitializingStatus extends Status
  case object ReadyStatus extends Status
  case object ErrorStatus extends Status
  case object StoppingStatus extends Status
  case object CompletedStatus extends Status
  case object FailedStatus extends Status

  private[messages] var batchsize = new AtomicInteger(
    DiagnosticSettings.getIntProperty("optimus.dal.messages.commit.batch.size", 500))

  private[messages] var batchBuildTimeMs = new AtomicLong(
    DiagnosticSettings.getLongProperty("optimus.dal.messages.commit.batch.buildTimeMs", 500L))

  private[optimus] def setBatchSize(size: Int): Unit = batchsize.set(size)
  private[messages] def getBatchSize(): Int = batchsize.get()

  private[optimus] def setBatchBuildTimeMs(batchTimeMs: Long): Unit = batchBuildTimeMs.set(batchTimeMs)
  private[messages] def getBatchBuildTimeMs(): Long = batchBuildTimeMs.get()
}

// This implementation invokes client notification callback in a separate thread.
class AsyncMessagesCallbackInvoker(
    override val streamId: String,
    override val callback: MessagesNotificationCallback,
    override val closeStream: () => Unit,
    override val commitStream: Seq[Long] => Unit,
    override val dalEnv: DalEnv
) extends MessagesCallbackInvoker {
  import AsyncMessagesCallbackInvoker._

  sealed trait Task
  private case class StreamEventTask(event: StreamEvent) extends Task
  private case object StopTask extends Task

  @volatile
  private[this] var status: Status = InitializingStatus
  private[this] val taskQueue = new LinkedBlockingQueue[Task]()
  @volatile
  private[this] var stoppingDeadline: Long = -1
  private val log = getLogger(this)
  private val logHeader = s"StreamId: $streamId -"

  val msgHandlerThreadGroup = new CustomThreadGroup("MessagesCallbackInvoker")

  protected var commitIds = new mutable.ListBuffer[Long]()

  // This is accessed only by this invoker thread, so it's thread-safe.
  private var lastBatchBuildStartTime = System.currentTimeMillis()

  private def fetchIdsToCommit(): Option[Seq[Long]] = commitIds.synchronized {
    if (
      (commitIds.size > batchsize.get() ||
        (System.currentTimeMillis() > lastBatchBuildStartTime + batchBuildTimeMs.get())) && commitIds.nonEmpty
    ) {
      lastBatchBuildStartTime = System.currentTimeMillis()
      val size = Math.min(commitIds.size, batchsize.get())
      val ids = commitIds.take(size)
      commitIds.remove(0, size)
      Some(ids)
    } else None
  }

  override def putIdsForCommit(ids: Seq[Long]): Unit = commitIds.synchronized { commitIds.appendAll(ids) }

  private[this] val msgHandlerThread = new Thread(msgHandlerThreadGroup, s"MessagesCallbackInvoker-$streamId") {

    override def run(): Unit = {
      try {
        try {
          log.info(s"$logHeader Messages callback invoker thread started..")
          notifyMsgHandlerThreadSetup()
          callback.setupThread(streamId)
          log.info(s"$logHeader Messages callback invoker setup is successful.")
        } catch {
          case ex: Exception =>
            // Don't rethrow. Otherwise, EndOfStreamMessage won't be delivered.
            val enteringErrorStatus = updateStatus(ErrorStatus, InitializingStatus)
            log.error(s"$logHeader Uncaught exception occurred while invoking thread setup callback", ex)
            safelyDeliverStreamErrorMessage(ex)
            if (enteringErrorStatus) closeStream()
        }

        updateStatus(ReadyStatus, InitializingStatus)

        while (
          status match {
            case ReadyStatus | ErrorStatus => true
            case StoppingStatus            => val d = stoppingDeadline; d < 0 || d >= System.nanoTime()
            case _                         => false
          }
        ) {
          try {
            Option(taskQueue.poll(10000, TimeUnit.MILLISECONDS)).foreach {
              case StreamEventTask(event) =>
                try {
                  val clientReceiveTime = System.currentTimeMillis()
                  callback.notifyStreamEvent(streamId, event)
                  event match {
                    case messageNotif: StreamNotificationEvent =>
                      sendMessageNotificationCrumb(messageNotif, clientReceiveTime)
                    case _ => // Do not send crumbs for other event types
                  }
                } catch {
                  case ex: Exception =>
                    // Don't rethrow. Otherwise, EndOfStreamMessage won't be delivered.
                    val enteringErrorStatus = updateStatus(ErrorStatus, ReadyStatus)
                    log.error(
                      s"$logHeader Uncaught exception occurred while invoking stream notify callback for stream event: $event.",
                      ex)
                    safelyDeliverStreamErrorMessage(ex)
                    // Check status to avoid endless looping if a user failed to handle EndOfStreamMessage.
                    if (enteringErrorStatus) closeStream()
                }
              case StopTask =>
                // Consume the interruption flag avoiding it's caught by code in 'finally' block.
                Thread.interrupted()
                updateStatus(CompletedStatus, ReadyStatus, StoppingStatus)
                if (updateStatus(FailedStatus, ErrorStatus)) {
                  // It's not possible to reach here unless there is a programmatic mistake.
                  log.error("Encountered StopTask with ErrorStatus")
                }
            }
            fetchIdsToCommit().foreach(commitStream)
          } catch { case _: InterruptedException => }
        }
      } catch {
        // In case that a default handler wants to terminate JVM for some special types of Errors, it shall be
        // able to get the error. The handler at
        // optimus.platform.dal.messages.AsyncNotificationMessageHandler.msgHandlerThreadGroup will log uncaught Throwables.
        case ex: Exception =>
          log.error(
            "{} Uncaught exception in Messages notification handler thread, current status: {}",
            logHeader,
            status,
            ex)
      } finally {
        // Since the control flow may get here during handling an Error, further Errors may be raised anytime.
        updateStatus(FailedStatus, InitializingStatus, ReadyStatus, ErrorStatus, StoppingStatus)

        try {
          if (status == FailedStatus) {
            // The control flow may get here only when shutting down is timed out, or when it's failed to close a stream
            // during handling exceptions, or when an Error is thrown. It's generally not recoverable.
            // EndOfStreamMessage cannot be delivered in this case, because we don't know how long the callback will
            // take to handle it, or whether a new Error would be thrown during handling it.
            log.error(
              "{} Failed to shutdown notification message handler gracefully. " +
                "EndOfStreamMessage may not reach the callback.",
              logHeader)
          }

          log.info(s"$logHeader Message handler thread is stopping..")
          notifyMsgHandlerThreadTeardown()
          callback.teardownThread(streamId)
          log.info(s"$logHeader Message handler thread teardown successful..")
        } catch {
          // Don't throw it if it's an Exception so that it won't override the original Error. The handler at
          // optimus.platform.dal.messages.AsyncNotificationMessageHandler.msgHandlerThreadGroup will log uncaught Throwables.
          case ex: Exception =>
            log.error("{} Error while notifying stream closing", logHeader, ex)
        }
      }
    }
  }

  msgHandlerThread.setDaemon(true)
  msgHandlerThread.start()

  override protected def notifyStreamEvent(evt: StreamEvent): Unit = taskQueue.put(StreamEventTask(evt))

  private def updateStatus(to: Status, from: Status*): Boolean = {
    if (from.contains(status)) {
      synchronized {
        if (from.contains(status)) {
          status = to
          true
        } else false
      }
    } else false
  }

  override def shutdown(): Unit = {
    // This method is supposed to be called from a synchronized block. Don't block the control flow here.
    if (updateStatus(StoppingStatus, InitializingStatus, ReadyStatus, ErrorStatus)) {
      stoppingDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(30000)
      taskQueue.put(StopTask)
    }
  }

  override def forceCommitOutstandingIds(): Unit = {
    val ids =
      commitIds.synchronized {
        val res = commitIds.toList
        commitIds.clear()
        res
      }
    if (ids.nonEmpty) commitStream(ids)
  }

  // Testing hooks..
  protected def notifyMsgHandlerThreadSetup(): Unit = ()
  protected def notifyMsgHandlerThreadTeardown(): Unit = ()
}
