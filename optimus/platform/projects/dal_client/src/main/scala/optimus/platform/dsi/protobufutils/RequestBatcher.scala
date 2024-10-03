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

import java.time.Instant
import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.crumbs.RequestsStallInfo
import optimus.breadcrumbs.crumbs.StallPlugin
import optimus.core.needsPlugin
import optimus.graph._
import optimus.platform._
import optimus.platform.dal.DALCache
import optimus.platform.relational.CallerDetailKey
import optimus.platform.dsi.Response
import optimus.platform.dsi.bitemporal.Command
import optimus.platform.dsi.bitemporal.proto.Dsi.DSIRequestProto
import optimus.platform.temporalSurface.impl.TemporalSurfaceDALAccessTracePlugin

import scala.collection.mutable
import scala.util.control.NonFatal
import optimus.platform.dal.session.ClientSessionContext
import optimus.platform.dsi.DSIResponse
import optimus.platform.dsi.bitemporal.proto.CommandProtoSerialization
import optimus.platform.dsi.versioning.VersioningRedirectionInfo
import optimus.platform.relational.ExtraCallerDetail
import optimus.platform.util.Log

object RequestBatcher extends CommandProtoSerialization {

  private val log = getLogger(this)

  final case class Configuration(clientName: String, async: Boolean, largeBatchBreakdownCallback: () => Unit)
}

abstract class RequestBatcher[A, B, C](batchingQueue: BatchingQueue[A], config: RequestBatcher.Configuration) {

  import RequestBatcher.log
  import config._

  private val name = s"Request batcher $clientName"

  def start(): Unit = {
    log.info(s"Starting $name thread")
    batchingThread.start()
  }

  def stop(): Unit = stopThread(batchingThread)

  def offer(request: A): Boolean = {
    batchingQueue.offer(request)
  }

  private val batchingThread = {
    val res = new Thread {

      override def run(): Unit = {
        log.info(s"$name thread started")
        batchingQueue.printParams()

        try {
          while (!Thread.interrupted()) {
            try {
              val batchesToSend = filterRequests(batchingQueue.getNextBatch())

              val it = batchesToSend.iterator
              while (!Thread.currentThread.isInterrupted && it.hasNext) {
                try {
                  val (context, batch) = it.next()
                  sendBatch(context, batch)
                } catch {
                  case _: InterruptedException =>
                    Thread.currentThread().interrupt()
                }
              }

              if (Thread.currentThread.isInterrupted && it.hasNext) {
                val remaining = it.toArray
                handleInterrupt(remaining)
                log.info(s"batchingThread had to fail ${remaining.length} requests due to interruption")
              }
            } catch {
              case ex: InterruptedException =>
                throw ex
              case NonFatal(t) =>
                // not really expecting any exceptions at this point as sendBatch() does its own handling
                // should something go wrong though we need to be loud about it and quit this thread
                log.error("No exception is expected at this location, quitting now", t)
                throw t
            }
          }
        } catch {
          case _: InterruptedException =>
            log.info(s"batchingThread interrupted")
            ()
        } finally {
          log.info(s"$name thread is now stopped")
          shutdown()
        }

      }
    }
    res.setDaemon(true)
    res.setName(name)
    res
  }

  protected def filterRequests(as: Seq[A]): Seq[(B, C)]

  protected def sendBatch(context: B, batch: C): Unit

  protected def handleInterrupt(remaining: Seq[(B, C)]): Unit

  protected def shutdown(): Unit

}

object ClientRequestBatcher extends Log {

  sealed abstract class Batch {
    def requestType: DSIRequestProto.Type
    def requests: Vector[ClientRequest]
  }
  final case class ReadBatch(requests: Vector[ClientRequest]) extends Batch {
    override def requestType: DSIRequestProto.Type = DSIRequestProto.Type.READ_ONLY
  }
  final case class WriteBatch(requests: Vector[ClientRequest]) extends Batch {
    override def requestType: DSIRequestProto.Type = DSIRequestProto.Type.WRITE
  }

  def splitIntoBatchesConsideringEntitlements(requests: Seq[ClientRequest]): Seq[Batch] = {
    val singleReadBatch = mutable.ArrayBuffer.empty[ReadClientRequest]
    val otherBatches = List.newBuilder[Batch]
    val singleMsgsBatch = mutable.ArrayBuffer.empty[WriteClientRequest]
    requests.foreach {
      case r: ReadClientRequest if r.hasUnentitled        => otherBatches += ReadBatch(Vector(r))
      case r: ReadClientRequest                           => singleReadBatch += r
      case w: WriteClientRequest if w.hasMessagesCommands => singleMsgsBatch += w
      case w: WriteClientRequest                          => otherBatches += WriteBatch(Vector(w))
    }
    val readBatches = if (singleReadBatch.nonEmpty) ReadBatch(singleReadBatch.toVector) :: Nil else Nil
    val msgsBatches = if (singleMsgsBatch.nonEmpty) WriteBatch(singleMsgsBatch.toVector) :: Nil else Nil
    readBatches ++ msgsBatches ++ otherBatches.result()
  }

  def filterNotCancelledRequests(batch: Seq[ClientRequest]): Seq[(ClientSessionContext, Batch)] = {
    val filteredBatch = batch.filterNot(_.completable.isCancelled)
    filteredBatch.groupBy(_.clientSessionCtx).toList flatMap { case (clientSessionContext, clientNextBatch) =>
      ClientRequestBatcher
        .splitIntoBatchesConsideringEntitlements(clientNextBatch)
        .map(b => (clientSessionContext, b))
    }
  }

  @async(exposeArgTypes = true) def send(
      requestBatcher: ClientRequestBatcher,
      commands: Seq[Command],
      clientSessionCtx: ClientSessionContext): Response = needsPlugin

  @async(exposeArgTypes = true) def sendWrites(
      requestBatcher: ClientRequestBatcher,
      commands: Seq[Command],
      clientSessionCtx: ClientSessionContext,
      redirectionInfo: Option[VersioningRedirectionInfo],
      lastWitnessedTxTimeOpt: Option[Instant]): Response = needsPlugin

  val batchSenderPlugin: SchedulerPlugin = new SchedulerPlugin {
    override val pluginType: PluginType = PluginType.DAL
    override val stallReason = "waiting for command execution on DAL broker"
    override def stallRequests(node: NodeTaskInfo): Option[RequestsStallInfo] = {
      val requests = ClientRequestTracker.getRequestsInFlight.toSeq
      Some(RequestsStallInfo(StallPlugin.DAL, requests.length, requests))
    }

    override def adapt(v: NodeTask, ec: OGSchedulerContext): Boolean = {
      v match {
        case node: ClientRequestBatcher.send$node =>
          // batchScope exit happens in MessageContexts when we completeWithException/completeWithResult
          val preCalc = v.scenarioStack().findPluginTag(PreCalcTag).getOrElse(NoPreCalc)
          node.scenarioStack.batchScope.enterDSI(node)
          val callerDetail = node.scenarioStack.findPluginTag(CallerDetailKey)
          val extraCallerDetail = new ExtraCallerDetail(v)
          val appNameTag = node.scenarioStack.findPluginTag[String](AppNameTag)
          val seqOpId = node.scenarioStack().seqOpId
          // try-catch is required so that any error in serialization is correctly propagated
          // otherwise graph scheduler will simply shutdown the app as it doesn't expect adapt call to fail
          try {
            val clientRequest = ClientRequest.read(
              NodeWithScheduler(ec.scheduler, node, seqOpId, preCalc),
              node.ID,
              node.clientSessionCtx,
              node.commands,
              callerDetail,
              Some(extraCallerDetail),
              appNameTag)
            // make very clear about when cache will be used (at cost of a tiny bit of code duplication)
            if (DALCache.Global.dalCacheEnabled) {
              val cachedResultsOpt =
                DALCache.Global.load(clientRequest.commands, clientRequest.commandProtos, Some(extraCallerDetail))
              cachedResultsOpt match {
                case Some(cachedResults) => node.completeWithResult(DSIResponse(cachedResults, None), ec)
                case None                => node.requestBatcher.offer(clientRequest)
              }
            } else {
              node.requestBatcher.offer(clientRequest)
            }
          } catch {
            case ex: Throwable =>
              node.completeWithException(ex, ec)
          }
          true
        case node: ClientRequestBatcher.sendWrites$node =>
          // batchScope exit happens in MessageContexts when we completeWithException/completeWithResult
          node.scenarioStack.batchScope.enterDSI(node)
          val appNameTag = node.scenarioStack.findPluginTag[String](AppNameTag)
          // try-catch is required so that any error in seralization is correctly propogated
          // otherwise graph scheduler will simply shutdown the app as it doesn't expect adapt call to fail
          try {
            val clientRequest = ClientRequest.write(
              NodeWithScheduler(ec.scheduler, node, 0L, NoPreCalc),
              node.ID,
              node.clientSessionCtx,
              node.commands,
              node.redirectionInfo,
              node.lastWitnessedTxTimeOpt,
              appNameTag = appNameTag
            )
            node.requestBatcher.offer(clientRequest)
          } catch {
            case ex: Throwable =>
              node.completeWithException(ex, ec)
          }
          true
        case _ =>
          log.warn("Received unsupported task type " + v)
          false
      }
    }
  }

  send_info.setPlugin(batchSenderPlugin)
  sendWrites_info.setPlugin(batchSenderPlugin)
  if (Settings.traceTemporalSurfaceCommands) {
    send_info.setPlugin(new TemporalSurfaceDALAccessTracePlugin(batchSenderPlugin))
    sendWrites_info.setPlugin(new TemporalSurfaceDALAccessTracePlugin(batchSenderPlugin))
  }

}

class ClientRequestBatcher(
    requestSender: RequestSender,
    batchingQueue: BatchingQueue[ClientRequest] with BatchingQueueShutdown[ClientRequest],
    config: RequestBatcher.Configuration,
    memoryManager: Option[SharedRequestLimiter]
) extends RequestBatcher[ClientRequest, ClientSessionContext, ClientRequestBatcher.Batch](batchingQueue, config) {

  import config._
  private[this] val log = getLogger[ClientRequestBatcher]

  override protected def filterRequests(
      as: Seq[ClientRequest]): Seq[(ClientSessionContext, ClientRequestBatcher.Batch)] =
    ClientRequestBatcher.filterNotCancelledRequests(as)

  override protected def sendBatch(context: ClientSessionContext, batch: ClientRequestBatcher.Batch): Unit = {
    val requests = batch.requests
    val liveRequests = requests.filterNot(_.completable.isCancelled)
    if (liveRequests.nonEmpty) {
      try {
        if (async) {
          requestSender.sendBatch(context, liveRequests, batch.requestType)
        } else {
          val token = new InFlightRequestToken()
          requestSender.sendBatch(context, liveRequests, batch.requestType, Some(token))
          token.waitForResponse
        }
      } catch {
        case mtl: DALClientMessageTooLargeException =>
          def failRequests(): Unit = liveRequests.foreach(_.completable.completeWithException(mtl))
          batch match {
            case _: ClientRequestBatcher.WriteBatch => failRequests()
            case _: ClientRequestBatcher.ReadBatch if liveRequests.size == 1 =>
              log.error(s"Command itself is too large, can't split batch further: ${liveRequests.head}", mtl)
              failRequests()
            case _: ClientRequestBatcher.ReadBatch =>
              largeBatchBreakdownCallback()
              log.info(s"batch of ${liveRequests.size} requests too large, splitting into two and retrying")
              val (firstHalfBatch, secondHalfBatch) =
                liveRequests.splitAt(liveRequests.size / 2)
              sendBatch(context, ClientRequestBatcher.ReadBatch(firstHalfBatch))
              sendBatch(context, ClientRequestBatcher.ReadBatch(secondHalfBatch))
          }
      }
    }
  }

  override protected def handleInterrupt(remaining: Seq[(ClientSessionContext, ClientRequestBatcher.Batch)]): Unit = {
    remaining.foreach {
      case (_, rb: ClientRequestBatcher.ReadBatch) =>
        rb.requests.foreach(req => completeRequestDueToShutdown(req.completable, readOnly = true))
      case (_, wb: ClientRequestBatcher.WriteBatch) =>
        wb.requests.foreach(req => completeRequestDueToShutdown(req.completable, readOnly = false))
    }
  }

  override def offer(request: ClientRequest): Boolean = {
    super.offer(memoryManager.map(_.push(request)).getOrElse(request))
  }

  override protected def shutdown(): Unit = batchingQueue.shutdown()
}
