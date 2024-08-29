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
import optimus.dsi.notification.NotificationEntry
import optimus.dsi.pubsub.CustomThreadGroup
import optimus.dsi.pubsub.DataNotificationMessage
import optimus.dsi.pubsub.GlobalEvent
import optimus.dsi.pubsub.NotificationMessage
import optimus.dsi.pubsub.SowNotificationMessage
import optimus.dsi.pubsub.StreamErrorMessage
import optimus.dsi.pubsub.StreamEvent
import optimus.graph.DiagnosticSettings
import optimus.platform.AsyncImplicits.collToAsyncMarker
import optimus.platform._
import optimus.platform.dal.DALPubSub.NotificationStreamCallback
import optimus.platform.dal.EntitySerializer
import optimus.platform.temporalSurface.DataFreeTemporalSurfaceMatchers
import optimus.platform.temporalSurface.impl.FlatTemporalContext
import optimus.platform.dsi.DalPubSubClientCrumbSource
import optimus.platform.storable.Entity
import optimus.platform.storable.PersistentEntity

object NotificationMessageHandler {
  val log: Logger = getLogger(this)
  private[pubsub] val disableVtFilter =
    DiagnosticSettings.getBoolProperty("optimus.platform.dal.pubsub.disableVtFilter", false)
  private val filterEmptyDataMessage =
    DiagnosticSettings.getBoolProperty("optimus.platform.dal.pubsub.filterEmptyDataMessage", true)
  val msgHandlerThreadGroup = new CustomThreadGroup("NotificationMessageHandler")
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

sealed trait NotificationMessageHandler {
  import NotificationMessageHandler._

  val clientStreamId: Int
  val notificationStreamCallback: NotificationStreamCallback
  protected val closeStream: () => Unit

  protected val clientStreamLogHeader = s"ClientStreamId: $clientStreamId -"

  def shutdown(): Unit

  final def handleMessages(msgs: Seq[NotificationMessage]): Unit = {
    def filter(s: StreamEvent): Option[StreamEvent] = if (disableVtFilter) Some(s)
    else
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
          case dm: DataNotificationMessage if filterEmptyDataMessage && dm.fullData.isEmpty =>
            log.info(s"Filtering out empty data notification message at ${dm.tt}")
          case fs => handleStreamEvent(new FilteredStreamEvent(fs))
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
        log.error("Error while delivering StreamErrorMessage", ex)
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
}
// This implementation invokes client notification callback in a separate thread.
class AsyncNotificationMessageHandler(
    val serverStreamId: String,
    override val clientStreamId: Int,
    override val notificationStreamCallback: NotificationStreamCallback,
    override protected val closeStream: () => Unit)
    extends NotificationMessageHandler {
  import AsyncNotificationMessageHandler._
  import NotificationMessageHandler._

  sealed trait Task
  case class StreamEventTask(event: FilteredStreamEvent) extends Task
  case object StopTask extends Task

  @volatile
  private[this] var status: Status = InitializingStatus
  private[this] val taskQueue = new LinkedBlockingQueue[Task]()
  @volatile
  private[this] var stoppingDeadline: Long = -1

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

  private[this] val msgHandlerThread = new Thread(msgHandlerThreadGroup, s"PubSubStream-$clientStreamId") {

    override def run(): Unit = {
      try {
        try {
          log.info(s"$clientStreamLogHeader Message handler thread started..")
          notifyMsgHandlerThreadSetup()
          notificationStreamCallback.setupThread(clientStreamId)
          log.info(s"$clientStreamLogHeader Message handler thread setup is successful.")
        } catch {
          case ex: Exception =>
            // Don't rethrow. Otherwise, EndOfStreamMessage won't be delivered.
            val enteringErrorStatus = updateStatus(ErrorStatus, InitializingStatus)

            log.error(s"$clientStreamLogHeader Uncaught exception occurred while invoking thread setup callback", ex)
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
            Option(taskQueue.poll(10000, TimeUnit.MILLISECONDS)).foreach({
              case StreamEventTask(event) =>
                try {
                  event.getFilteredEvent foreach { evt =>
                    publishCrumb(evt)
                    notificationStreamCallback.notifyStreamEvent(clientStreamId, evt)
                  }
                } catch {
                  case ex: Exception =>
                    // Don't rethrow. Otherwise, EndOfStreamMessage won't be delivered.
                    val enteringErrorStatus = updateStatus(ErrorStatus, ReadyStatus)

                    log.error(
                      s"$clientStreamLogHeader Uncaught exception occurred while invoking stream notify callback for stream event: $event.",
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
            })
          } catch { case _: InterruptedException => }
        }
      } catch {
        // In case that a default handler wants to terminate JVM for some special types of Errors, it shall be
        // able to get the error. The handler at
        // optimus.platform.dal.pubsub.NotificationMessageHandler#msgHandlerThreadGroup will log uncaught Throwables.
        case ex: Exception =>
          log.error(
            "{} Uncaught exception in PubSub message handler thread, current status: {}",
            clientStreamLogHeader,
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
              clientStreamLogHeader)
          }

          log.info(s"$clientStreamLogHeader Message handler thread is stopping..")
          notifyMsgHandlerThreadTeardown()
          notificationStreamCallback.teardownThread(clientStreamId)
          log.info(s"$clientStreamLogHeader Message handler thread teardown successful..")
        } catch {
          // Don't throw it if it's an Exception so that it won't override the original Error. The handler at
          // optimus.platform.dal.pubsub.NotificationMessageHandler#msgHandlerThreadGroup will log uncaught Throwables.
          case ex: Exception =>
            log.error("{} Error while notifying stream closing", clientStreamLogHeader, ex)
        }
      }
    }
  }

  msgHandlerThread.setDaemon(true)
  msgHandlerThread.start()

  override protected def handleStreamEvent(event: FilteredStreamEvent): Unit = taskQueue.put(StreamEventTask(event))
  private def updateStatus(to: Status, from: Status*) = {
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

  // Testing hooks..
  protected def notifyMsgHandlerThreadSetup(): Unit = {}
  protected def notifyMsgHandlerThreadTeardown(): Unit = {}
}

// For unit tests, this simply invokes client notification callback in the same thread.
class SyncNotificationMessageHandler(
    override val clientStreamId: Int,
    override val notificationStreamCallback: NotificationStreamCallback,
    override protected val closeStream: () => Unit)
    extends NotificationMessageHandler {
  import NotificationMessageHandler._

  override def shutdown(): Unit = {}
  override protected def handleStreamEvent(event: FilteredStreamEvent): Unit = {
    event.getFilteredEvent foreach { evt =>
      invokeCallbackSafely[StreamEvent](evt, ev => notificationStreamCallback.notifyStreamEvent(clientStreamId, ev))
    }
  }
}
