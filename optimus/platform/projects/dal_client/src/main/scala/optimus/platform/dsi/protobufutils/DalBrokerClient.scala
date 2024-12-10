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

// import msjava.base.spring.lifecycle.BeanState
import msjava.slf4jutils.scalalog.getLogger
import optimus.dsi.partitioning.DefaultPartition
import optimus.dsi.partitioning.Partition
import optimus.platform._
import optimus.platform.dal.ClientContext
import optimus.platform.dal.ClientRequestLimiter
import optimus.platform.dal.HasLastWitnessedTime
import optimus.platform.dal.client.DalClientBatchTracker
import optimus.platform.dal.session.ClientSessionContext
import optimus.platform.dsi.Response
import optimus.platform.dsi.bitemporal.Command
import optimus.platform.dsi.bitemporal.proto.Dsi.DSIRequestProto
import optimus.platform.dsi.versioning.VersioningRedirectionInfo

import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.util.control.NonFatal

object DalBrokerClient {
  sealed trait ShutdownCause
  object ShutdownCause {
    final case object DuplicateCreation extends ShutdownCause
  }
}

trait DalBrokerClient {
  def connectionString: String
  def clientName: String
  @async def request(
      commands: Seq[Command],
      requestType: DSIRequestProto.Type,
      clientSessionCtx: ClientSessionContext,
      redirectionInfo: Option[VersioningRedirectionInfo] = None,
      separateWriteBatcher: Boolean = true): Response
  private[optimus] def sendBatch(batch: BatchContext): Unit
  def start(): Unit
  def shutdown(cause: Option[DalBrokerClient.ShutdownCause]): Unit
  def isRunning: Boolean

  def brokerVirtualHostname: Option[String]
}

abstract class BatchedDalBrokerClient(
    reqLimiter: ClientRequestLimiter,
    val clientCtx: ClientContext,
    val batchRetryManager: Option[BatchRetryManager],
    writeServingPartitionOpt: Option[Partition])
    extends DalClientBatchTracker(reqLimiter)
    with DalBrokerClient { outer =>
  import BatchedDalBrokerClient.log

  private val rwShutdownLock: ReadWriteLock = new ReentrantReadWriteLock
  private val rShutdownLock = rwShutdownLock.readLock
  private val wShutdownLock = rwShutdownLock.writeLock
  /* private val state = new BeanState()
  state.initializeIfNotInitialized() */

  // Needs to be lazy because creating the RequestSender might be side-effecting, for instance by spawning threads etc.
  // As such initialization of the requestSender should not happen until we call start()
  protected lazy val requestSender: RequestSender = createNewRequestSender

  // Needs to be lazy because requestSender is lazy
  protected lazy val requestBatcher = createNewRequestBatcher(requestSender)

  protected lazy val optionalWriteRequestBatcher: Option[ClientRequestBatcher] =
    createOptionalNewRequestBatcherForWrites(requestSender)

  // Needs to be lazy because creating the TimeoutDetector might be side-effecting, for instance by spawning threads
  // etc. As such initialization of the timeoutDetector should not happen until we call start()
  private lazy val timeoutDetector = createNewTimeoutDetector

  protected def createNewRequestSender: RequestSender
  protected def createNewRequestBatcher(requestSender: RequestSender): ClientRequestBatcher
  protected def createOptionalNewRequestBatcherForWrites(requestSender: RequestSender): Option[ClientRequestBatcher] =
    None
  protected def createNewTimeoutDetector: TimeoutDetector

  protected def beforeCorrelationMapClean(): Unit = {
    // test hook
  }

  protected[platform] def withRShutdownLock[T](f: => T): T = {
    rShutdownLock.lock()
    try {
      f
    } finally {
      rShutdownLock.unlock()
    }
  }

  override def start(): Unit = {
    requestSender.start()
    requestBatcher.start()
    optionalWriteRequestBatcher.foreach(_.start())
    timeoutDetector.start()
    // state.startIfNotRunning()
  }

  override final private[optimus] def sendBatch(batch: BatchContext): Unit = {
    requestSender.assertConnected
    // requestSender.sendBatch(batch)
  }

  override final def isRunning: Boolean = ??? // state.isRunning

  @async override def request(
      commands: Seq[Command],
      requestType: DSIRequestProto.Type,
      clientSessionCtx: ClientSessionContext,
      redirectionInfo: Option[VersioningRedirectionInfo] = None,
      separateWriteBatcher: Boolean = true): Response = ??? /* {
    def lastWitnessedTxTimeOpt = EvaluationContext.entityResolver match {
      case hlwt: HasLastWitnessedTime =>
        Some(hlwt.lastWitnessedTime(writeServingPartitionOpt.getOrElse(DefaultPartition)))
      case _ => None
    }
    requestSender.assertConnected
    requestType match {
      case DSIRequestProto.Type.READ_ONLY => ClientRequestBatcher.send(requestBatcher, commands, clientSessionCtx)
      case DSIRequestProto.Type.WRITE =>
        ClientRequestBatcher.sendWrites(
          if (separateWriteBatcher) optionalWriteRequestBatcher.getOrElse(requestBatcher) else requestBatcher,
          commands,
          clientSessionCtx,
          redirectionInfo,
          lastWitnessedTxTimeOpt
        )
      case DSIRequestProto.Type.SERVICE_DISCOVERY =>
        ClientRequestBatcher.send(requestBatcher, commands, clientSessionCtx)
      case DSIRequestProto.Type.PUBSUB =>
        throw new IllegalArgumentException("Do not expect pub sub command type here.")
      case DSIRequestProto.Type.MESSAGES =>
        throw new IllegalArgumentException("Do not expect messages command type here.")
    }
  } */

  def shutdown(cause: Option[DalBrokerClient.ShutdownCause]): Unit = {
    if (isRunning) {
      wShutdownLock.lock()
      try {
        if (isRunning) {
          // to make sure we do not execute the shutdown logic multiple times
          // we do this stopIfRunning and have this whole method body wrapped in isRunning check
          // state.stopIfRunning()
          log.info(s"Shutting down $clientName")
          requestSender.shutdown()
          optionalWriteRequestBatcher.foreach(_.stop())
          requestBatcher.stop()
          timeoutDetector.stop()

          val outstandingBatches = collectInFlightBatches { case (_, bctx) => bctx }
          if (outstandingBatches.nonEmpty)
            log.debug(s"Requests failed due to shutdown are: ${outstandingBatches.map(_.requestUuid).mkString(",")}")
          outstandingBatches.foreach { batch =>
            try {
              batch.completeDueToShutdown(Some(outer))
              ClientRequestTracker.markComplete(batch.requestUuid)
            } catch {
              case NonFatal(ex) =>
                log.error(s"Error when completing batch ${batch.requestUuid}", ex)
            }
          }
          beforeCorrelationMapClean()
          clearInFlightBatches() // Important call to release locks
          log.info(s"$clientName shut down, set to failed ${outstandingBatches.size} in-flight requests")
        }
      } finally {
        wShutdownLock.unlock()
        // state.destroyIfNotDestroyed()
      }
    }
  }
  override def brokerVirtualHostname: Option[String] = clientCtx.brokerVirtualHostname
}

object BatchedDalBrokerClient {
  private val log = getLogger(BatchedDalBrokerClient)
  val defaultConnectTimeout: Long = java.lang.Long.getLong("optimus.dsi.connectTimeout", 30 * 1000L)
}
