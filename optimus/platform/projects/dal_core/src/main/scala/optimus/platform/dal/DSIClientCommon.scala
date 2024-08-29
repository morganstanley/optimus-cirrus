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
package optimus.platform.dal

import msjava.slf4jutils.scalalog.Logger
import optimus.core.CoreAPI
import optimus.graph.DiagnosticSettings
import optimus.platform.dsi.bitemporal._
import optimus.platform.internal.SimpleStateHolder
import optimus.platform.storable.PersistentEntity
import optimus.platform.storable.SerializedBusinessEvent

import scala.util.Random

final class DSIClientCommon {
  import DSIClientCommon._
  private[DSIClientCommon] var retryAttempts = OperationRetryAttempts
  private[DSIClientCommon] var backOffTimeInterval = OperationRetryBackoffTime
  private[DSIClientCommon] var maxBackoffTime = OperationRetryMaxBackoffTime
  private[DSIClientCommon] var retryRandomized = OperationRetryRandomized
}

object DSIClientCommon extends SimpleStateHolder(() => new DSIClientCommon()) with CoreAPI {
  private val rand = new Random(System.currentTimeMillis())
  private val OperationRetryAttempts: Int =
    DiagnosticSettings.getIntProperty("optimus.dsi.retryAttempts", 5)
  private val OperationRetryBackoffTime: Long =
    DiagnosticSettings.getLongProperty("optimus.dsi.retryBackoffTimeMs", 1000L)
  private val OperationRetryMaxBackoffTime: Long =
    DiagnosticSettings.getLongProperty("optimus.dsi.retryMaxBackoffTimeMs", 30000L)
  private val OperationRetryRandomized: Boolean =
    DiagnosticSettings.getBoolProperty("optimus.dsi.retryRandomized", true)
  val ServerSideVersioning: Boolean = DiagnosticSettings.getBoolProperty("optimus.dsi.serverSideVersioning", false)
  val DisableBatchRetry: Boolean = DiagnosticSettings.getBoolProperty("optimus.dsi.disableBatchRetry", false)
  def toleranceToNowMinutes: Integer = Integer.getInteger("optimus.dsi.toleranceToNow", -1)
  def failReadsTooCloseToAtNow: Boolean =
    DiagnosticSettings.getBoolProperty("optimus.dsi.failReadsTooCloseToAtNow", false)
  def getRetryCheckSessionState: Boolean =
    DiagnosticSettings.getBoolProperty("optimus.dsi.retryCheckSessionState", true)

  def setMaxRetryAttempts(r: Int): Unit = getState.synchronized { getState.retryAttempts = r }
  def setBackoffTimeInterval(r: Long): Unit = getState.synchronized { getState.backOffTimeInterval = r }
  def setMaxBackoffTime(r: Long): Unit = getState.synchronized { getState.maxBackoffTime = r }
  def setRetryRandomized(r: Boolean): Unit = getState.synchronized { getState.retryRandomized = r }

  def getMaxRetryAttempts: Int = getState.synchronized { getState.retryAttempts }
  def getBackoffTimeInterval: Long = getState.synchronized { getState.backOffTimeInterval }
  def getMaxBackoffTime: Long = getState.synchronized { getState.maxBackoffTime }
  def getRetryRandomized: Boolean = getState.synchronized { getState.retryRandomized }

  def resetMaxRetryAttempts(): Unit = getState.synchronized { getState.retryAttempts = OperationRetryAttempts }
  def resetBackoffTimeInterval(): Unit = getState.synchronized {
    getState.backOffTimeInterval = OperationRetryBackoffTime
  }
  def resetMaxBackoffTime(): Unit = getState.synchronized {
    getState.maxBackoffTime = OperationRetryMaxBackoffTime
  }

  def resetRetryRandomized(): Unit = getState.synchronized {
    getState.retryRandomized = OperationRetryRandomized
  }

  def boundedBackOffTime(calculated: Long): Long = {
    val max = getMaxBackoffTime
    if (calculated < max) calculated else max
  }

  def randomizedBackOffTime(calculated: Long): Long = {
    if (getRetryRandomized) {
      (calculated * (0.5 + rand.nextDouble() / 2)).toLong
    } else calculated
  }

  private[this] def getFullResultFromPartialResult[T](partialResults: Seq[PartialResult[T]]): Option[FullResult[T]] = {
    if (partialResults.isEmpty) None
    else Some(AbstractPartialResult.getFullResult[T, Result, PartialResult[T]](partialResults))
  }

  def collate(results: Seq[Result]): Seq[Result] = {
    val resultBuffer = Seq.newBuilder[Result]
    val entitiesBuffer = Seq.newBuilder[PersistentEntity]
    val eventsBuffer = Seq.newBuilder[SerializedBusinessEvent]
    val partialResultsBuffer = Seq.newBuilder[PartialResult[Any]]
    val valuesBuffer = Seq.newBuilder[Array[Any]]

    results foreach {
      case PartialSelectResult(entities, false) =>
        entities foreach { entitiesBuffer += _ }

      case pr: PartialSelectResult if pr.isLast =>
        pr.value foreach { entitiesBuffer += _ }
        resultBuffer += SelectResult(entitiesBuffer.result()).withResultStats(pr.getResultStats())
        entitiesBuffer.clear()

      case PartialGetBusinessEventResult(events, false) =>
        events foreach { eventsBuffer += _ }

      case pr: PartialGetBusinessEventResult if pr.isLast =>
        pr.values foreach { eventsBuffer += _ }
        resultBuffer += GetBusinessEventResult(eventsBuffer.result()).withResultStats(pr.getResultStats())
        eventsBuffer.clear()

      case IntermediatePartialQueryResult(values) =>
        values foreach { valuesBuffer += _ }

      case result @ FinalPartialQueryResult(values, metaDataOption) =>
        metaDataOption
          .map(metaData => {
            values foreach { valuesBuffer += _ }
            resultBuffer += QueryResult(valuesBuffer.result(), metaData)
            entitiesBuffer.clear()
          })
          .getOrElse(
            throw new IllegalStateException(s"Could not find QueryResultMetaData in PartialQueryResult $result."))

      case result: PartialResult[_] =>
        val pResult = result.asInstanceOf[PartialResult[Any]]
        if (pResult.isLast) {
          partialResultsBuffer += pResult
          getFullResultFromPartialResult(partialResultsBuffer.result()).foreach(resultBuffer +=)
          partialResultsBuffer.clear()
        } else partialResultsBuffer += pResult

      case r => resultBuffer += r
    }
    resultBuffer.result()
  }

  // utilized by subclass
  val log: Logger = msjava.slf4jutils.scalalog.getLogger(this)
}
