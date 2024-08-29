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

import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.crumbs.EventCrumb
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.platform.dal.DalTimedoutException
import optimus.platform.internal.SimpleStateHolder
import optimus.platform.DALEventMulticaster
import optimus.platform.DALTimeoutEvent
import optimus.platform.dal.client.DalClientBatchTracker
import optimus.platform.dsi.DalClientCrumbSource
import optimus.platform.dsi.DalEvents

import scala.annotation.tailrec
import scala.util.control.NonFatal

object TimeoutDetector {
  private val log = getLogger[TimeoutDetector]

  private final val requestIdProperty = "reqId"
  private final val typeProperty = "type"
}

class TimeoutDetector(
    client: DalClientBatchTracker with DalBrokerClient,
    clientName: String,
    config: TimeoutDetectorConfig) {

  import TimeoutDetector._

  private val name = s"Timeout detector $clientName"

  def start(): Unit = {
    if (config.timeout == Integer.MAX_VALUE) {
      log.info(s"Timeout disabled, timeout detector not required")
    } else {
      log.info(s"Starting $name thread")
      timeoutThread.start()
    }
  }

  def stop(): Unit = {
    if (config.timeout == Integer.MAX_VALUE) {
      log.info(s"Timeout disabled, nothing to stop")
    } else {
      stopThread(timeoutThread)
    }
  }

  private val timeoutThread = {
    val res = new Thread {
      override def run(): Unit = {

        log.info(s"$name thread started, will look for timed out requests every ${config.checkInterval} msecs")

        case class TimeoutEntry(batchContext: BatchContext, requestSeqId: Int)

        try {

          @tailrec
          def checkTimedOutRequests(alreadyNotified: Set[TimeoutEntry]): Unit = {

            val timedOut = client.collectInFlightBatches {
              case (seqId, bc) if bc.expired(config.timeout) => TimeoutEntry(bc, seqId)
            }

            try {
              timedOut.foreach {
                case entry @ TimeoutEntry(bc, seqId) if !alreadyNotified.contains(entry) =>
                  log.warn(s"${logPrefix(bc.requestUuid, seqId)} batch timed out (${timedOut.size} in total currently)")
                  Breadcrumbs.trace(bc.chainedId, EventCrumb(_, DalClientCrumbSource, DalEvents.Client.RequestTimedOut))
                  Breadcrumbs.warn(
                    bc.chainedId,
                    PropertiesCrumb(
                      _,
                      DalClientCrumbSource,
                      Map(requestIdProperty -> bc.requestUuid, typeProperty -> "RequestTimedOut")))
                  if (DALEventMulticaster.getListeners.nonEmpty)
                    DALEventMulticaster.notifyListeners(_.timeoutCallback(DALTimeoutEvent(config.timeout)))
                  else {
                    // Complete the batch with timeout exception, so that client can handle them at source..
                    client.onBatchComplete(seqId)
                    bc.completeAllWithException(Some(client), new DalTimedoutException(config.timeout))
                    ClientRequestTracker.markComplete(bc.requestUuid)
                  }
                case _ => // still no response, nothing to do
              }
            } catch {
              case NonFatal(t) =>
                log.error("Caught exception, trying to move on ...", t)
            }

            if (!Thread.currentThread().isInterrupted) {
              Thread.sleep(config.checkInterval)
              checkTimedOutRequests(timedOut.toSet)
            }

          }

          checkTimedOutRequests(Set())

        } catch {
          case _: InterruptedException =>
            ()
        } finally {
          log.info(s"$name thread is now stopped")
        }

      }
    }
    res.setDaemon(true)
    res.setName(name)
    res
  }

}

final case class TimeoutDetectorConfig(
    checkInterval: Long = TimeoutDetectorConfig.timeoutCheckInterval,
    timeout: Int = TimeoutDetectorConfig.maxWaitingTimeDALRequest)

object TimeoutDetectorConfig extends SimpleStateHolder(() => new TimeoutDetectorConfigValues) {
  private val defaultTimeoutCheckInterval: Long = java.lang.Long.getLong("optimus.dsi.timeoutCheckInterval", 1 * 1000L)
  private val defaultMaxWaitingTimeDalRequest: Int = Integer.getInteger("optimus.dsi.maxWaitingTime", Int.MaxValue)

  def maxWaitingTimeDALRequest: Int =
    synchronized { getState.maxWaitingTimeDALRequest }.getOrElse(defaultMaxWaitingTimeDalRequest)

  def setMaxWaitingTimeDalRequest(value: Int): Unit =
    synchronized { getState.maxWaitingTimeDALRequest = Some(value) }

  def resetMaxWaitingTimeDalRequest(): Unit =
    synchronized { getState.maxWaitingTimeDALRequest = None }

  def timeoutCheckInterval: Long =
    synchronized { getState.timeoutCheckInterval }.getOrElse(defaultTimeoutCheckInterval)

  def setTimeoutCheckInterval(value: Long) =
    synchronized { getState.timeoutCheckInterval = Some(value) }

  def resetTimeoutCheckInterval(): Unit =
    synchronized { getState.timeoutCheckInterval = None }
}

private[protobufutils] class TimeoutDetectorConfigValues {
  private[protobufutils] var timeoutCheckInterval: Option[Long] = None
  private[protobufutils] var maxWaitingTimeDALRequest: Option[Int] = None
}
