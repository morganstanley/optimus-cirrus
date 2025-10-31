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

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.core.ChainedNodeID
import optimus.core.CoreAPI.asyncResult
import optimus.dsi.notification.NotificationEntry
import optimus.dsi.notification.NotificationEntry.ReferenceOnlyNotificationEntry
import optimus.dsi.pubsub.CustomThreadGroup
import optimus.dsi.pubsub.DataNotificationMessage
import optimus.dsi.pubsub.GlobalEvent
import optimus.dsi.pubsub.NotificationMessage
import optimus.dsi.pubsub.SowNotificationMessage
import optimus.dsi.pubsub.StreamErrorMessage
import optimus.dsi.pubsub.StreamEvent
import optimus.dsi.pubsub.Subscription
import optimus.graph.DiagnosticSettings
import optimus.platform.AsyncImplicits.collToAsyncMarker
import optimus.platform._
import optimus.platform.dal.DALPubSub.NotificationStreamCallback
import optimus.platform.dal.EntitySerializer
import optimus.platform.dal.config.DalAsyncBatchingConfig
import optimus.platform.dal.pubsub.NotificationMessageHandler.FilteredStreamEvent
import optimus.platform.temporalSurface.DataFreeTemporalSurfaceMatchers
import optimus.platform.temporalSurface.impl.FlatTemporalContext
import optimus.platform.dsi.DalPubSubClientCrumbSource
import optimus.platform.dsi.bitemporal.DSIQueryTemporality
import optimus.platform.dsi.bitemporal.Result
import optimus.platform.dsi.bitemporal.Select
import optimus.platform.dsi.bitemporal.SelectResult
import optimus.platform.storable.Entity
import optimus.platform.storable.PersistentEntity
import optimus.platform.dsi.Feature
import optimus.platform.dsi.bitemporal.DSI
import optimus.platform.dsi.bitemporal.EntityQuery
import optimus.platform.dsi.bitemporal.ErrorResult
import optimus.platform.dsi.bitemporal.PubSubException
import optimus.platform.dsi.bitemporal.ReferenceQuery
import optimus.platform.dsi.bitemporal.VersionedReferenceQuery
import optimus.platform.storable.EntityReference
import optimus.platform.storable.VersionedReference
import optimus.utils.CollectionUtils._

import java.util
import scala.jdk.CollectionConverters._
import scala.concurrent.duration.DurationInt

object NotificationMessageHandler {
  val log: Logger = getLogger(this)
  val msgHandlerThreadGroup = new CustomThreadGroup("NotificationMessageHandler")
  val rawEventsHandlerThreadGroup = new CustomThreadGroup("RawEventsHandler")
  private val temporalSurfaceTag = Some("NotificationMessageHandler")

  @node
  def hydrate(pe: PersistentEntity): Entity = {
    EntitySerializer.hydrate[Entity](
      pe,
      FlatTemporalContext(
        DataFreeTemporalSurfaceMatchers.all,
        pe.vtInterval.from,
        pe.txInterval.from,
        temporalSurfaceTag))
  }

  @node
  def filter(nes: Seq[NotificationEntry], clientFunc: NodeFunction1[Any, Boolean]): Seq[NotificationEntry] = {
    val newNes = nes.apar.filter { ne =>
      val pe =
        ne.segment.data.toPersistentEntity(
          ne.slotRef.vref,
          ne.lockToken,
          ne.segment.vtInterval,
          TimeInterval(ne.txTime),
          None)
      val v = hydrate(pe)
      clientFunc(v)
    }
    newNes
  }

  @node
  def sowFilter(nes: Seq[PersistentEntity], clientFunc: NodeFunction1[Any, Boolean]): Seq[PersistentEntity] = {
    val newNes = nes.apar.filter { pe =>
      val v = hydrate(pe)
      clientFunc(v)
    }
    newNes
  }

  class FilteredStreamEvent(event: StreamEvent) {
    @entersGraph
    def getFilteredEvent: Option[StreamEvent] = event match {
      case dm: DataNotificationMessage if dm.needsClientSideFiltering =>
        val newData = dm.fullData.apar.flatMap { case kv @ (s, nes) =>
          s.clientFilter match {
            case Some(cf) =>
              val fnes = filter(nes, cf)
              if (fnes.isEmpty) None
              else
                Some(s -> fnes)
            case None => Some(kv)
          }
        }
        if (newData.isEmpty) None
        else Some(DataNotificationMessage(dm.tt, dm.writeReqId, newData))
      case sow: SowNotificationMessage if sow.needsClientSideFiltering =>
        val newData = sow.fullData.apar.flatMap { case kv @ (s, pes) =>
          s.clientFilter match {
            case Some(cf) =>
              val fpes = sowFilter(pes, cf)
              if (fpes.isEmpty) None
              else
                Some(s -> fpes)
            case None => Some(kv)
          }
        }
        if (newData.isEmpty) None
        else Some(SowNotificationMessage(sow.tt, newData))
      case _ => Option(event)
    }
  }
}

final case class VrefQueryHelper(readDSI: DSI) {
  val isClientVrefEnabled: Boolean = Feature.VersionedReferenceQuery.enabled
  val isServerVrefEnabled: Boolean = readDSI.serverFeatures().supports(Feature.VersionedReferenceQuery)
  val isVrefQuerySupported = isClientVrefEnabled && isServerVrefEnabled
  def getEntityQuery(
      versionedRef: VersionedReference,
      ref: EntityReference,
      validTimeInterval: ValidTimeInterval,
      txTimeInterval: TimeInterval,
      cn: String,
      entitledOnly: Boolean): EntityQuery = if (isVrefQuerySupported)
    VersionedReferenceQuery(
      versionedRef,
      validTimeInterval,
      txTimeInterval,
      cn,
      entitledOnly
    )
  else
    ReferenceQuery(
      ref,
      entitledOnly,
      Some(cn)
    )
}

sealed trait NotificationMessageHandler {
  import NotificationMessageHandler._

  val isPubSubReferenceNotificationEnabled: Boolean = Feature.PubSubReferenceNotification.enabled

  val maxLatencyForRawEventsQueue =
    DiagnosticSettings.getLongProperty("optimus.dal.pubsub.maxLatencyForRawEventsQueue", 5.seconds.toMillis)

  lazy val vrefQueryHelper: VrefQueryHelper =
    VrefQueryHelper(readDSI.getOrElse(throw new Exception("DSI is missing.")))

  def serverStreamId: String
  val clientStreamId: Int
  val notificationStreamCallback: NotificationStreamCallback
  protected val closeStream: () => Unit
  protected val removeSubscription: (Set[Subscription.Id]) => Unit
  val readDSI: Option[DSI] = None

  protected def putRawEvents(event: StreamEvent): Unit

  protected val clientStreamLogHeader = s"ClientStreamId: $clientStreamId -"

  def shutdown(): Unit

  def collectReferenceOnlyNotificationEntry(
      msgs: Seq[StreamEvent]): Seq[(Subscription, ReferenceOnlyNotificationEntry)] = {
    msgs.flatMap {
      case dm: DataNotificationMessage =>
        dm.fullData.toSeq.flatMap { case (subscription, notificationEntries) =>
          notificationEntries.collect { case referenceOnlyNE: ReferenceOnlyNotificationEntry =>
            (subscription, referenceOnlyNE)
          }
        }
      case _ => Seq.empty
    }
  }

  @entersGraph
  def loadEntities(notificationEntrySeq: Seq[(Subscription, ReferenceOnlyNotificationEntry)])
      : Map[ReferenceOnlyNotificationEntry, Result] = {
    if (notificationEntrySeq.size > 0) {
      val results =
        asyncResult {
          val cmdSeq = notificationEntrySeq.map { case (subscription, notificationEntry) =>
            Select(
              vrefQueryHelper.getEntityQuery(
                notificationEntry.slotRef.vref,
                notificationEntry.segment.data.entityRef,
                notificationEntry.segment.vtInterval,
                TimeInterval(notificationEntry.txTime),
                notificationEntry.segment.data.className,
                subscription.entitledOnly
              ),
              DSIQueryTemporality.At(notificationEntry.segment.vtInterval.from, notificationEntry.txTime)
            )
          }

          readDSI
            .getOrElse(throw new Exception("DSI is missing."))
            .executeReadOnlyCommands(cmdSeq.toSeq)
        } match {
          case NodeSuccess(value) =>
            value
          case NodeFailure(ex) =>
            // If it reaches here, it means either DSI is missing, which should not be the case or DAL read side has issues, for which we are sending a StreamErrorMessage.
            safelyDeliverStreamErrorMessage(new PubSubException(serverStreamId, ex.getMessage))
            Seq()
        }
      notificationEntrySeq.map { case (_, notificationEntry) => notificationEntry }.zip(results).toMap
    } else Map()
  }

  def replaceReferenceOnlyNotifications(
      msgs: Seq[StreamEvent],
      notificationEntryMap: Map[ReferenceOnlyNotificationEntry, Result]): Seq[StreamEvent] = {
    if (notificationEntryMap.nonEmpty) {
      msgs map { msg =>
        msg match {
          case dm: DataNotificationMessage =>
            val newData = dm.fullData.flatMap {
              case (sub, data) => {
                try {
                  val notificationEntries = data.flatMap {
                    case referenceOnlyNE: ReferenceOnlyNotificationEntry =>
                      // if it is not a SelectResult, ie if it is an ErrorResult
                      // or if it isEmpty (in case of an unentitled read for a subscription where entitled only is true),
                      // exception will be generated, which will be caught and subscription will be closed
                      val dalEntity = notificationEntryMap
                        .getOrElse(
                          referenceOnlyNE,
                          throw new PubSubException(
                            serverStreamId,
                            s"Got unexpected error for referenceOnlyNE - ${referenceOnlyNE}")) match {
                        case SelectResult(value) => value
                        case ErrorResult(error, _) =>
                          throw new PubSubException(
                            serverStreamId,
                            s"Got unexpected error for referenceOnlyNE ${error.getMessage}")
                      }

                      // throwing Exception to give meaningful message at the client side, in cases of dal read failure
                      if (dalEntity.size == 0 && !sub.entitledOnly)
                        throw new PubSubException(
                          serverStreamId,
                          s"Couldn't load the entity from dal for referenceOnlyNE - ${referenceOnlyNE}")

                      // convert to NotificationEntryImpl by loading data from DAL
                      dalEntity.singleOption.map(e => referenceOnlyNE.toEntry(e.serialized))
                    case x: NotificationEntry => Some(x)
                  }
                  Some(sub, notificationEntries)
                } catch {
                  case ex: Exception =>
                    safelyDeliverStreamErrorMessage(new PubSubException(serverStreamId, ex.getMessage))
                    log.warn(
                      s"$clientStreamLogHeader Closing subscription - ${sub.subId} due to post check failure on message")
                    // close Subscription
                    removeSubscription(Set(sub.subId))
                    None
                }
              }
            }
            DataNotificationMessage(dm.tt, dm.writeReqId, newData)

          case se => se
        }
      }
    } else msgs
  }

  def handleReferenceOnlyNotifications(streamEventMsgs: Seq[StreamEvent]): Unit = {

    // call handleStreamEvent after loading data from DAL for ReferenceOnlyNotificationEntry
    val (timeTaken, streamEvents) =
      AdvancedUtils.timed {
        // checks for all the ReferenceOnlyNotificationEntries present in the batch, stores them in notificationEntrySeq along with Subscription as a tuple
        val referenceOnlyNotificationEntrySeq = collectReferenceOnlyNotificationEntry(streamEventMsgs)

        // for all the referenceOnlyNotificationEntrySeq entries, DAL read command is called to fetch the entities in a batch and store them in a map notificationEntryMap
        val notificationEntryMap = loadEntities(referenceOnlyNotificationEntrySeq)

        // prepare final streamEvents by replacing all the ReferenceOnlyNotificationEntry objects to NotificationEntryImpl objects
        replaceReferenceOnlyNotifications(streamEventMsgs, notificationEntryMap)
      }

    val timeInMillis = TimeUnit.NANOSECONDS.toMillis(timeTaken)
    if (timeInMillis >= maxLatencyForRawEventsQueue)
      log.warn(s"$clientStreamLogHeader handle reference-only notification took ${timeInMillis} ms.")
    streamEvents foreach { streamEvent =>
      handleStreamEvent(new FilteredStreamEvent(streamEvent))
    }
  }

  final def handleMessages(msgs: Seq[NotificationMessage]): Unit = {
    def filter(s: StreamEvent): Option[StreamEvent] =
      s match {
        case dm: DataNotificationMessage =>
          val newData = dm.fullData filter { case (_, nes) =>
            nes.nonEmpty
          }
          Some(DataNotificationMessage(dm.tt, dm.writeReqId, newData))
        case _ => Some(s)
      }

    msgs foreach {
      case g: GlobalEvent => handleGlobalEvent(g)
      case s: StreamEvent =>
        filter(s) foreach {
          case dm: DataNotificationMessage if dm.fullData.isEmpty =>
            log.info(s"$clientStreamLogHeader Filtering out empty data notification message at ${dm.tt}")
          case fs =>
            if (isPubSubReferenceNotificationEnabled)
              putRawEvents(fs)
            else
              handleStreamEvent(new FilteredStreamEvent(fs))
        }
    }
  }
  protected def handleStreamEvent(event: FilteredStreamEvent): Unit

  private def handleGlobalEvent(event: GlobalEvent): Unit =
    invokeCallbackSafely[GlobalEvent](event, ev => notificationStreamCallback.notifyGlobalEvent(clientStreamId, ev))

  final protected def safelyDeliverStreamErrorMessage(th: Throwable): Unit = { // noexcept
    try {
      notificationStreamCallback.notifyStreamEvent(clientStreamId, StreamErrorMessage(th))
    } catch {
      case ex: Exception =>
        log.error(s"$clientStreamLogHeader Error while delivering StreamErrorMessage", ex)
    }
  }

  final protected def invokeCallbackSafely[T <: NotificationMessage](event: T, callback: T => Unit): Unit = {
    try {
      callback(event)
    } catch {
      case th: Throwable =>
        log.error(
          s"$clientStreamLogHeader Uncaught exception occurred while invoking stream notify callback for stream event: $event.",
          th)
        safelyDeliverStreamErrorMessage(th)
        closeStream()
    }
  }
}

object AsyncNotificationMessageHandler {
  sealed trait Status
  case object InitializingStatus extends Status
  case object ReadyStatus extends Status
  case object ErrorStatus extends Status
  case object StoppingStatus extends Status
  case object CompletedStatus extends Status
  case object FailedStatus extends Status

  def defaultBatchSize = DalAsyncBatchingConfig.maxBatchSize

  sealed trait Task
  final case class StreamEventTask(event: FilteredStreamEvent, createTime: Long = System.currentTimeMillis())
      extends Task
  final case class StopTask(createTime: Long) extends Task
}
// This implementation invokes client notification callback in a separate thread.
class AsyncNotificationMessageHandler(
    override val serverStreamId: String,
    override val clientStreamId: Int,
    override val notificationStreamCallback: NotificationStreamCallback,
    override protected val closeStream: () => Unit,
    override val readDSI: Option[DSI] = None,
    override protected val removeSubscription: (Set[Subscription.Id]) => Unit = (_) => ()
) extends NotificationMessageHandler {
  import AsyncNotificationMessageHandler._
  import NotificationMessageHandler._

  private[this] val taskQueue = new LinkedBlockingQueue[Task]()
  private[this] val rawEventsQueue = new LinkedBlockingQueue[Task]

  override protected def putRawEvents(event: StreamEvent): Unit = {
    rawEventsQueue.put(StreamEventTask(new FilteredStreamEvent(event)))
  }

  private val taskThreadName = "Task"

  private val rawEventsThreadName = "RawEvents"

  private def publishCrumb(evt: StreamEvent): Unit = evt match {
    case dm: DataNotificationMessage =>
      // This event is generated per write request.
      val maybeReqId = dm.writeReqId
      val env =
        if (EvaluationContext.isInitialised) EvaluationContext.env.config.runtimeConfig.mode
        else ""
      maybeReqId foreach { reqId =>
        val props = Map("type" -> "strmNotified", "strmId" -> serverStreamId, "reqId" -> reqId, "env" -> env)
        Breadcrumbs.info(
          ChainedNodeID.nodeID,
          PropertiesCrumb(
            _,
            DalPubSubClientCrumbSource,
            props
          )
        )
      }
    case _ =>
  }

  class StatusHolder {
    @volatile var status: Status = InitializingStatus
    @volatile var stoppingDeadline: Long = -1

    def updateStatus(to: Status, from: Status*) = {
      if (from.contains(status)) {
        synchronized {
          if (from.contains(status)) {
            status = to
            true
          } else false
        }
      } else false
    }

  }

  abstract class StatusControlThread(threadGroup: ThreadGroup, threadName: String)
      extends Thread(threadGroup, threadName) {
    val threadPurpose: String

    val statusHolder: StatusHolder

    override def run(): Unit = {
      try {
        setupMessageHandlerThread(threadPurpose)
        while (checkThreadStatus(statusHolder.status, statusHolder.stoppingDeadline)) { runThread() }
      } catch {
        // In case that a default handler wants to terminate JVM for some special types of Errors, it shall be
        // able to get the error. The handler at
        // optimus.platform.dal.pubsub.NotificationMessageHandler#msgHandlerThreadGroup will log uncaught Throwables.
        case ex: Exception =>
          log.error(
            s"${clientStreamLogHeader} Uncaught exception in PubSub ${threadPurpose} message handler thread, current status: ${statusHolder.status}",
            ex)
      } finally {
        closeMessageHandlerThread(threadPurpose)
      }
    }

    protected def runThread(): Unit

    def setupMessageHandlerThread(threadName: String): Unit = {
      try {
        log.info(s"$clientStreamLogHeader $threadName Message handler thread started..")
        notifyMsgHandlerThreadSetup()
        // TODO (OPTIMUS-80295): We only initialize one of the thread, because the other one gets initialized in a
        // roundabout way by DALPSStreamManager.
        if (threadName.equals(rawEventsThreadName) && !EvaluationContext.isInitialised)
          EvaluationContext.initializeWithoutRuntime()
        notificationStreamCallback.setupThread(clientStreamId)
        log.info(s"$clientStreamLogHeader $threadName Message handler thread setup is successful.")
      } catch {
        case ex: Exception =>
          // Don't rethrow. Otherwise, EndOfStreamMessage won't be delivered.
          val enteringErrorStatus = statusHolder.updateStatus(ErrorStatus, InitializingStatus)

          log.error(s"$clientStreamLogHeader Uncaught exception occurred while invoking thread setup callback", ex)
          safelyDeliverStreamErrorMessage(ex)

          if (enteringErrorStatus) closeStream()
      }

      statusHolder.updateStatus(ReadyStatus, InitializingStatus)
    }

    def closeMessageHandlerThread(threadName: String): Unit = {
      // Since the control flow may get here during handling an Error, further Errors may be raised anytime.
      statusHolder.updateStatus(FailedStatus, InitializingStatus, ReadyStatus, ErrorStatus, StoppingStatus)

      try {
        if (statusHolder.status == FailedStatus) {
          // The control flow may get here only when shutting down is timed out, or when it's failed to close a stream
          // during handling exceptions, or when an Error is thrown. It's generally not recoverable.
          // EndOfStreamMessage cannot be delivered in this case, because we don't know how long the callback will
          // take to handle it, or whether a new Error would be thrown during handling it.
          log.error(
            s"{} Failed to shutdown ${threadName} notification message handler gracefully. " +
              "EndOfStreamMessage may not reach the callback.",
            clientStreamLogHeader
          )
        }

        log.info(s"$clientStreamLogHeader ${threadName} Message handler thread is stopping..")
        notifyMsgHandlerThreadTeardown()
        notificationStreamCallback.teardownThread(clientStreamId)
        log.info(s"$clientStreamLogHeader ${threadName} Message handler thread teardown successful..")
      } catch {
        // Don't throw it if it's an Exception so that it won't override the original Error. The handler at
        // optimus.platform.dal.pubsub.NotificationMessageHandler#msgHandlerThreadGroup will log uncaught Throwables.
        case ex: Exception =>
          log.error("{} Error while notifying stream closing", clientStreamLogHeader, ex)
      }
    }

    def checkThreadStatus(status: Status, stoppingDeadline: Long) = {
      if (ReadyStatus != status && ErrorStatus != status)
        log.info(s"$threadName status = $status stoppingDeadline = $stoppingDeadline now = ${System.nanoTime()}")
      status match {
        case ReadyStatus | ErrorStatus => true
        case StoppingStatus            => val d = stoppingDeadline; d < 0 || d >= System.nanoTime()
        case _                         => false
      }
    }

    def handleStopTask() = {
      statusHolder.updateStatus(CompletedStatus, ReadyStatus, StoppingStatus)

      if (statusHolder.updateStatus(FailedStatus, ErrorStatus)) {
        // It's not possible to reach here unless there is a programmatic mistake.
        log.error(s"$clientStreamLogHeader Encountered StopTask for ${threadPurpose} thread with ErrorStatus")
      }
    }

  }

  private[this] val msgHandlerThread = new StatusControlThread(msgHandlerThreadGroup, s"PubSubStream-$clientStreamId") {

    override val statusHolder: StatusHolder = new StatusHolder()
    override val threadPurpose = taskThreadName
    override def runThread(): Unit = {
      try {
        Option(taskQueue.poll(10000, TimeUnit.MILLISECONDS)).foreach({
          case StreamEventTask(event, createTime) =>
            try {
              event.getFilteredEvent foreach { evt =>
                publishCrumb(evt)
                notificationStreamCallback.notifyStreamEvent(clientStreamId, evt)
              }
            } catch {
              case ex: Exception =>
                // Don't rethrow. Otherwise, EndOfStreamMessage won't be delivered.
                val enteringErrorStatus = statusHolder.updateStatus(ErrorStatus, ReadyStatus)

                log.error(
                  s"$clientStreamLogHeader Uncaught exception occurred while invoking stream notify callback for stream event: $event.",
                  ex)
                safelyDeliverStreamErrorMessage(ex)

                // Check status to avoid endless looping if a user failed to handle EndOfStreamMessage.
                if (enteringErrorStatus) closeStream()
            }
          case StopTask(createTime) =>
            // Consume the interruption flag avoiding it's caught by code in 'finally' block.
            Thread.interrupted()
            handleStopTask()
        })
      } catch { case _: InterruptedException => }
    }
  }

  msgHandlerThread.setDaemon(true)
  msgHandlerThread.start()

  private[this] val rawEventsHandlerThread =
    new StatusControlThread(rawEventsHandlerThreadGroup, s"PubSubRawEvents-$clientStreamId") {
      override val statusHolder: StatusHolder = new StatusHolder()
      override val threadPurpose = rawEventsThreadName
      override def runThread(): Unit = {
        try {
          // read from rawEventsQueue and call handleStreamEvent to put in TaskQueue
          val streamEventTasks = new util.ArrayList[Task]()
          // getFromQueue
          // Block until at least one message in queue
          val streamEventTask = rawEventsQueue.take()
          streamEventTasks.add(streamEventTask)
          val eventFetchTime = System.currentTimeMillis()

          val eventCreateTime = streamEventTask match {
            case StreamEventTask(event, createTime) => createTime
            case StopTask(createTime)               => createTime
          }

          val queueLatency = eventFetchTime - eventCreateTime

          if (queueLatency >= maxLatencyForRawEventsQueue)
            log.warn(
              s"$clientStreamLogHeader rawEventsQueue latency: ${queueLatency} ms, exceeds the maxLatency: ${maxLatencyForRawEventsQueue} ms")

          // Try to drain as many messages upto a max defined
          rawEventsQueue.drainTo(streamEventTasks, defaultBatchSize)

          val (streamEventTsks, stopTask) = streamEventTasks.asScala.toSeq.partition(_.isInstanceOf[StreamEventTask])

          val streamEventMsgs = streamEventTsks.flatMap {
            case StreamEventTask(event, createTime) =>
              event.getFilteredEvent
            case _ => None
          }

          val stopTsk = stopTask.flatMap {
            case task @ StopTask(createTime) =>
              Some(task)
            case _ => None
          }.singleOption
          handleReferenceOnlyNotifications(streamEventMsgs)
          stopTsk.foreach { task =>
            Thread.interrupted()
            handleStopTask()
            taskQueue.put(task)
          }
        } catch { case _: InterruptedException => }
      }
    }

  if (isPubSubReferenceNotificationEnabled) {
    rawEventsHandlerThread.setDaemon(true)
    rawEventsHandlerThread.start()
  }

  override protected def handleStreamEvent(event: FilteredStreamEvent): Unit =
    taskQueue.put(StreamEventTask(event))

  override def shutdown(): Unit = {
    // This method is supposed to be called from a synchronized block. Don't block the control flow here.
    val rawEventStatus = rawEventsHandlerThread.statusHolder
    val msgEventStatus = msgHandlerThread.statusHolder
    if (
      rawEventStatus.updateStatus(StoppingStatus, InitializingStatus, ReadyStatus, ErrorStatus) && msgEventStatus
        .updateStatus(StoppingStatus, InitializingStatus, ReadyStatus, ErrorStatus)
    ) {
      val stoppingDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(30000)
      rawEventStatus.stoppingDeadline = stoppingDeadline
      msgEventStatus.stoppingDeadline = stoppingDeadline
      val stopTaskCreateTime = System.currentTimeMillis()
      if (isPubSubReferenceNotificationEnabled) {
        rawEventsQueue.put(StopTask(stopTaskCreateTime))
      } else {
        taskQueue.put(StopTask(stopTaskCreateTime))
      }
    }
  }

  // Testing hooks..
  protected def notifyMsgHandlerThreadSetup(): Unit = {}
  protected def notifyMsgHandlerThreadTeardown(): Unit = {}
}

// For unit tests, this simply invokes client notification callback in the same thread.
class SyncNotificationMessageHandler(
    override val serverStreamId: String,
    override val clientStreamId: Int,
    override val notificationStreamCallback: NotificationStreamCallback,
    override protected val closeStream: () => Unit,
    override val readDSI: Option[DSI] = None,
    override protected val removeSubscription: (Set[Subscription.Id]) => Unit = (_) => ()
) extends NotificationMessageHandler {
  import NotificationMessageHandler._

  override def shutdown(): Unit = {}
  override protected def handleStreamEvent(event: FilteredStreamEvent): Unit = {
    event.getFilteredEvent foreach { evt =>
      invokeCallbackSafely[StreamEvent](evt, ev => notificationStreamCallback.notifyStreamEvent(clientStreamId, ev))
    }
  }
  override protected def putRawEvents(event: StreamEvent): Unit =
    handleReferenceOnlyNotifications(Seq(event))

}
