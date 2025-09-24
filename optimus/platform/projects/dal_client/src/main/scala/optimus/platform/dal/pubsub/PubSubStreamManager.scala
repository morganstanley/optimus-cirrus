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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.atomic.AtomicReference
import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.ChainedID
import optimus.dsi.partitioning.Partition
import optimus.graph.DiagnosticSettings
import optimus.platform.dal.DALPubSub
import optimus.platform.dal.DALPubSub._
import optimus.platform.RuntimeEnvironment
import optimus.platform.dal.ClientSideDSI
import optimus.platform.dsi.bitemporal._

import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

trait PubSubStreamManager extends HandlePubSub {
  import PubSubStreamManager._

  private[this] val streamMap = new ConcurrentHashMap[String, NotificationStreamImpl]()
  private[this] val closedStreams = new ConcurrentSkipListSet[String]
  protected val batchCtxMetadata = new AtomicReference[Option[PubSubContextMetadata]](None)

  protected val readDSI: Option[ClientSideDSI] = None

  /**
   * As of now, this method only ensures that the streamId is in the streamMap, however, in the future we can decide to
   * hibernate a particular stream and prevent it from receiving updates through a state check of the stream.
   */
  private[optimus] def checkStreamExists(streamId: String) = streamMap.containsKey(streamId)

  // the below api is exposed only for use in tests
  private[optimus] def getServerSideIdFromClientId(clientId: ClientStreamId): String =
    streamMap.asScala.find { case (_, v) => v.id == clientId }.map(_._1).getOrElse("")

  protected def executePubSubCommands(cmds: Seq[PubSubCommand]): Unit

  // This method should be re-entrant and should not lock any shared resources other than the operated stream object.
  // See also:
  //   optimus.platform.dal.pubsub.NotificationStreamImpl#executePendingClientRequests
  //     calls pubSubRequestHandler.executePubSubRequestBatch(reqs)
  //   optimus.platform.dal.pubsub.NotificationMessageHandler#invokeCallbackSafely
  //     calls closeStream
  protected def executePubSubRequestImpl(requests: Seq[PubSubClientRequest]): Seq[PubSubClientResponse] = {
    val reqsWithResult = requests.map(req => req -> preprocessClientRequest(req))
    val reqsToExecute = reqsWithResult.collect { case (req, result) if result.executeRequest => req }
    if (reqsToExecute.nonEmpty) {
      if (batchCtxMetadata.get.isEmpty) {
        val chainedID = ChainedID.root
        batchCtxMetadata.set(Some(PubSubContextMetadata(chainedID, None)))
        log.debug(s"Setting batchCtxMetadata: $batchCtxMetadata")
      }
      executePubSubCommands(reqsToExecute.map(_.cmd))
    }
    reqsWithResult.map { case (_, result) => result.clientResponse }
  }

  case class PreprocessRequestResult(executeRequest: Boolean, clientResponse: PubSubClientResponse)

  def preprocessClientRequest(request: PubSubClientRequest): PreprocessRequestResult = {
    val streamId = request.cmd.streamId
    val streamOpt = Option(streamMap.get(streamId))
    request match {
      case PubSubClientRequest.CreateStream(addCmd, clientId, cb, partition, env) if streamOpt.isEmpty =>
        val response =
          PubSubClientResponse.StreamCreationRequested(
            addPubSubNotificationStream(addCmd, clientId, cb, partition, env, readDSI))
        PreprocessRequestResult(executeRequest = true, response)
      case _: PubSubClientRequest.CreateStream =>
        throw new IllegalStateException(s"Duplicate Stream creation request received $streamId")
      case req: PubSubClientRequest.ChangeSubRequest =>
        val executeRequest = streamOpt
          .getOrElse(throw new IllegalStateException(s"Stream#$streamId must exist for change subscription request!"))
          .subscriptionChangeRequest(req)
        PreprocessRequestResult(executeRequest, PubSubClientResponse.VoidResponse)
      case req: PubSubClientRequest.CloseStream =>
        val executeRequest = streamOpt
          .getOrElse(throw new IllegalStateException(s"Stream#$streamId must exist for close request!"))
          .closeStreamRequest(req)
        PreprocessRequestResult(executeRequest, PubSubClientResponse.VoidResponse)
    }
  }

  protected def createNewNotificationStream(
      clientStreamId: ClientStreamId,
      addCmd: CreatePubSubStream,
      cb: NotificationStreamCallback,
      partition: Partition,
      env: RuntimeEnvironment,
      readDSI: Option[ClientSideDSI]
  ): NotificationStreamImpl = new NotificationStreamImpl(clientStreamId, addCmd, cb, partition, this, env, readDSI)

  private def addPubSubNotificationStream(
      addCmd: CreatePubSubStream,
      clientId: ClientStreamId,
      callback: DALPubSub.NotificationStreamCallback,
      partition: Partition,
      env: RuntimeEnvironment,
      readDSI: Option[ClientSideDSI]
  ): NotificationStream = {
    val newNotificationStream = createNewNotificationStream(clientId, addCmd, callback, partition, env, readDSI)
    if (streamMap.putIfAbsent(addCmd.streamId, newNotificationStream) != null) {
      throw new IllegalStateException(s"Duplicate Stream creation request received ${addCmd.streamId}")
    }
    log.info(s"Initialized new NotificationStream with id: ${newNotificationStream.id}")
    newNotificationStream
  }

  private def ignoreResultIfStreamAlreadyClosed(streamId: String, res: Result): Unit = {
    if (closedStreams.contains(streamId))
      log.info(s"Ignoring $res as Stream#$streamId has already been closed")
    else
      log.error(s"Received $res for Stream#$streamId, but cannot find such a stream")
  }

  def handlePubSubResult(streamIdWithResult: Seq[(String, Result)]): Unit = {
    streamIdWithResult.foreach {
      case (streamId, csr: ClosePubSubStreamSuccessResult) =>
        Option(streamMap.get(streamId)) match {
          case Some(s) => s.handlePubSubResult(csr) foreach performStreamAction
          case None    => ignoreResultIfStreamAlreadyClosed(streamId, csr)
        }
      case (_, _: PubSubBrokerConnect) =>
        onConnect()
      case (streamId, psr: PubSubResult) =>
        Option(streamMap.get(streamId)) match {
          case Some(s) => s.handlePubSubResult(psr) foreach performStreamAction
          case None    => ignoreResultIfStreamAlreadyClosed(streamId, psr)
        }
      case (streamId, err @ ErrorResult(_: PubSubSubscriptionChangeException, _)) =>
        Option(streamMap.get(streamId)) match {
          case Some(s) => s.handleErrorResult(err)
          case None    => ignoreResultIfStreamAlreadyClosed(streamId, err)
        }
      case (streamId, err @ ErrorResult(_: ChangeSubscriptionMultiPartitionException, _)) =>
        Option(streamMap.get(streamId)) match {
          case Some(s) => s.handleErrorResult(err)
          case None    => ignoreResultIfStreamAlreadyClosed(streamId, err)
        }
      case (streamId, err @ ErrorResult(_: PubSubEntitlementException, _)) =>
        Option(streamMap.get(streamId)) match {
          case Some(s) => s.handleErrorResult(err)
          case None    => ignoreResultIfStreamAlreadyClosed(streamId, err)
        }
      case (streamId, err @ ErrorResult(th, _)) =>
        Option(streamMap.get(streamId)) match {
          case Some(s) => s.shutdown(Some(th)) foreach performStreamAction
          case None    => ignoreResultIfStreamAlreadyClosed(streamId, err)
        }
      case (streamId, res) =>
        log.error(s"Unexpected Result received: $res, streamId: $streamId")
    }
  }

  def onConnect(): Unit = {
    streamMap.asScala.foreach { case (id, stream) =>
      stream.handlePubSubResult(PubSubBrokerConnect(id)) foreach performStreamAction
    }
  }

  def onDisconnect(): Unit = {
    cleanUpStaleStreams()
    streamMap.asScala.foreach { case (id, stream) =>
      stream.handlePubSubResult(PubSubBrokerDisconnect(id)) foreach performStreamAction
    }
  }

  def shutdownAllStreams(): Unit = {
    cleanUpStaleStreams()
    streamMap.asScala.foreach { case (id, stream) =>
      stream.shutdown() foreach performStreamAction
    }
    streamMap.clear()
  }

  protected def getCurrentStreamObjs: Seq[NotificationStreamImpl] =
    streamMap.asScala.map { case (_, streamObj) => streamObj }.toSeq

  protected def sendErrorAndCloseStream(streamId: String, th: Throwable): Unit = {
    val streamObj = streamMap.get(streamId)
    streamObj.shutdown(Some(th)) foreach performStreamAction
  }

  private def performStreamAction(action: NotificationStreamImpl.Action): Unit = {
    action match {
      case NotificationStreamImpl.UnlinkAction(streamObj) =>
        streamMap.remove(streamObj.serverSideStreamId, streamObj)
        closedStreams.add(streamObj.serverSideStreamId)
    }
  }

  private val staleStreamStatusByPartition = new ConcurrentHashMap[Partition, StaleStreamState]()
  def notifyStaleStreams(): Unit = {
    log.trace("Checking for stale streams")
    val earliestUpdateTime = patch.MilliInstant.now.minusSeconds(pubsubStaleLimitInSec)
    val staleStreams = streamMap.asScala.collect {
      case (_, stream) if stream.getLastUpdateTime.isBefore(earliestUpdateTime) => stream
    } toSet

    if (staleStreams.nonEmpty) {
      staleStreams.groupBy(_.partition).foreach { case (ptn, strms) =>
        staleStreamStatusByPartition.compute(
          ptn,
          (_, oldVal) => {
            if (oldVal == null || oldVal.streams != strms) {
              val state = StaleStreamState(streamStateCounter.getAndIncrement(), strms)
              log.warn(s"Stale streams found in $ptn partition: $state")
              val evt = PubSubTickDelay(state.entityTypes)
              strms foreach { stream =>
                stream.handlePubSubResult(evt)
              }
              state
            } else {
              oldVal
            }
          }
        )
      }
    } else {
      cleanUpStaleStreams()
    }
  }

  private def cleanUpStaleStreams(): Unit = {
    staleStreamStatusByPartition.forEach((ptn, state) => {
      log.warn(s"Stale streams recovered in $ptn partition: $state")
      val evt = PubSubTickDelayOver(state.entityTypes)
      state.streams foreach { stream =>
        stream.handlePubSubResult(evt)
      }
      staleStreamStatusByPartition.remove(ptn)
    })
  }
}

object PubSubStreamManager {
  private val log = getLogger(this)

  private val pubsubStaleMinLimitInSec: Int =
    DiagnosticSettings.getIntProperty("optimus.dal.pubsub.staleMinLimitInSec", 5)
  private val pubsubStaleLimitInSec: Int = {
    val p = DiagnosticSettings.getIntProperty("optimus.dal.pubsub.staleLimitInSec", 10)
    if (p <= pubsubStaleMinLimitInSec) {
      throw new IllegalArgumentException(
        s"Invalid value for optimus.dal.pubsub.staleLimitInSec: $p. It should be greater than $pubsubStaleMinLimitInSec.")
    }
    p
  }

  private val streamStateCounter = new AtomicInteger(0)
  private final case class StaleStreamState(counter: Int, streams: Set[NotificationStreamImpl]) {
    def entityTypes: Set[String] = streams.flatMap(_.currentSubscriptions.flatMap(_.className)).toSet

    private lazy val description =
      s"DalPS Stale Streams($counter, ${streams
          .map { s => (s.serverSideStreamId, s.getLastUpdateTime, s.currentSubscriptions.flatMap(_.className)) }
          .mkString(",")})"
    override def toString: String = description
  }
}
