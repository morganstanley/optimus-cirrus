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

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import msjava.protobufutils.server.BackendException
import optimus.platform.dal.DSIClientCommon
import optimus.platform.dal.DalTimedoutException
import optimus.platform.dsi.bitemporal.DALNonRetryableActionException
import optimus.platform.dsi.bitemporal.DALRetryableActionException
import optimus.platform.dsi.bitemporal.DsiSpecificTransientError
import optimus.platform.dsi.bitemporal.proto.Dsi._
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import optimus.platform.dsi.bitemporal.NonLeaderBrokerException

import scala.jdk.CollectionConverters._

/**
 * Retries full batch which failed.
 */
trait BatchRetryManager {
  import DSIClientCommon._

  // Necessary to track these batches in order to complete them when BRM is shutdown,
  // as these tasks may have been scheduled but not yet executed
  private[this] val pendingRetryables = new ConcurrentHashMap[String, BatchContext.RetryBatchContextImpl]()

  final def retry(
      client: Option[DalBrokerClient],
      batch: BatchContext.RetryBatchContextImpl,
      exception: Throwable): Unit = ??? /* {
    exception match {
      case ex: Exception => {
        log.warn(s"DAL client got exception: ${ex.getClass.getName} - ${ex.getMessage}")
        log.debug("Stack trace:", ex)
        if (shouldRetry(ex, batch.requestType)) {
          val attempts = batch.attempts
          val backoff = batch.backoff
          if (attempts > 0) {
            log.warn(s"DAL client retry, attempts left = $attempts, retrying in: ${backoff}ms")
            raiseDALRetryEvent(ex)
            val newUuid = UUID.randomUUID() // we always use new uuid even when we retry
            val newBatch = batch.copy(newUuid, attempts - 1, boundedBackOffTime(backoff * 2))
            scheduleRetry(client, batch, ex, backoff, newBatch)
          } else {
            log.warn("Out of retries", ex)
            finallyCompleteBatchWithException(client, batch, ex)
          }
        } else {
          ex match {
            case be @ (_: BackendException | _: DalTimedoutException)
                if batch.requestType == DSIRequestProto.Type.WRITE =>
              log.warn(
                "This exception is not retryable at this point. " +
                  "It might be retried later for PutApplicationEvents ",
                ex)
            case _ => log.warn("This exception is not retryable", ex)
          }
          finallyCompleteBatchWithException(client, batch, ex)
        }
      }
      case t =>
        finallyCompleteBatchWithException(client, batch, t)
    }
  } */

  private def finallyCompleteBatchWithException(
      client: Option[DalBrokerClient],
      batch: BatchContext.RetryBatchContextImpl,
      exception: Throwable): Unit = {
    val wrappedEx = exception match {
      case rt @ (_: DALRetryableActionException | _: NonLeaderBrokerException | _: DsiSpecificTransientError) =>
        new DALNonRetryableActionException("Retry failed", rt, client)
      case t: Throwable => t
    }
    batch.finallyCompleteAllWithException(client, wrappedEx)
  }

  private val retryExecutor = {
    val threadFactory = new CustomizableThreadFactory("DSIClient-Batch-Retry")
    threadFactory.setDaemon(true)
    val result = new ScheduledThreadPoolExecutor(1, threadFactory, new ThreadPoolExecutor.AbortPolicy())
    result.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
    result
  }

  private class BatchRetryTask(
      oldClient: Option[DalBrokerClient],
      oldBatch: BatchContext.RetryBatchContextImpl,
      batch: BatchContext.RetryBatchContextImpl,
      oldException: Throwable)
      extends Runnable {
    override def run(): Unit =
      try {
        pendingRetryables.remove(batch.requestUuid) // untrack as no longer pending
        close(oldClient, oldBatch)
        sendRetryBatch(batch)
      } catch {
        case i: InterruptedException =>
          finallyCompleteBatchWithException(None, batch, oldException)
          Thread.currentThread.interrupt() // reset the interrupt flag
        case e: Exception => retry(None, batch, e)
        case t: Throwable => finallyCompleteBatchWithException(None, batch, t)
      }
  }

  protected def scheduleRetry(
      oldClient: Option[DalBrokerClient],
      oldBatch: BatchContext.RetryBatchContextImpl,
      oldException: Throwable,
      backoff: Long,
      newBatch: BatchContext.RetryBatchContextImpl): Unit =
    try {
      pendingRetryables.put(newBatch.requestUuid, newBatch) // track
      retryExecutor.schedule(
        new BatchRetryTask(oldClient, oldBatch, newBatch, oldException),
        backoff,
        TimeUnit.MILLISECONDS)
    } catch {
      case t: Throwable =>
        pendingRetryables.remove(newBatch.requestUuid) // untrack as not scheduled successfully
        log.error(s"BatchRetryManager failed to schedule the retry for batch: ${newBatch.requestUuid}", t)
        finallyCompleteBatchWithException(oldClient, newBatch, oldException)
    }

  def shutdown(): Unit = {
    retryExecutor.shutdownNow() // shutdown executor before clearing pendingRetryables
    val ex = new BackendException("Connection shutdown")
    pendingRetryables.asScala foreach { case (k, v) =>
      finallyCompleteBatchWithException(None, v, ex)
    }
    pendingRetryables.clear()
  }

  def maxRetryAttempts: Int
  def retryBackoffTime: Long
  protected def shouldRetry(t: Throwable, requestType: DSIRequestProto.Type): Boolean
  protected def sendRetryBatch(newBatch: BatchContext): Unit
  def raiseDALRetryEvent(ex: Exception): Unit
  def raiseDALRecoveryEvent(client: Option[DalBrokerClient]): Unit
  protected def close(client: Option[DalBrokerClient], batch: BatchContext): Unit
}
