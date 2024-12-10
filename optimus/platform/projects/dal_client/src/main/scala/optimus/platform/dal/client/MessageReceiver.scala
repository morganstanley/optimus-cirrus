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
package optimus.platform.dal.client

import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.BreadcrumbsSendLimit.OnceByCrumbEquality
import optimus.breadcrumbs.crumbs.EventCrumb
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.graph.DiagnosticSettings
import optimus.platform.dal.config.DalEnv
import optimus.platform.dsi.DalClientCrumbSource
import optimus.platform.dsi.DalEvents
import optimus.platform.dsi.bitemporal.AbstractPartialResult
import optimus.platform.dsi.bitemporal.PartialResult
import optimus.platform.dsi.bitemporal.Result
import optimus.platform.dsi.bitemporal.proto.CommandProtoSerialization
import optimus.platform.dsi.bitemporal.proto.Dsi
import optimus.platform.dsi.bitemporal.proto.Dsi.EstablishSessionResultProto
import optimus.platform.dsi.bitemporal.proto.Dsi.ResultProto
import optimus.platform.dsi.bitemporal.proto.Dsi.TimingsProto
import optimus.platform.dsi.bitemporal.proto.Dsi.VersioningResponseProto
import optimus.platform.dsi.protobufutils.BatchedDalBrokerClient
import optimus.platform.dsi.protobufutils.BatchContext
import optimus.platform.dsi.protobufutils.ClientRequestTracker
import optimus.platform.dsi.protobufutils.DebugZeroResult
import optimus.platform.dsi.protobufutils.NodeWithScheduler
import optimus.platform.dsi.protobufutils.PreCalc
import optimus.platform.dsi.protobufutils.logPrefix
import optimus.testsupport.shouldLogDalResultWeight
import optimus.utils.PropertyUtils

import scala.jdk.CollectionConverters._
import scala.collection.compat._

private[platform] trait DalServiceResponse extends Any {
  def isPartial: Boolean
  def results: Seq[ResultProto]
  def commandIndices: Seq[Int]
  def establishSessionResult: Option[EstablishSessionResultProto]
  def timings: Option[TimingsProto]
}

private[platform] trait DalServiceResponseMessage extends Any {
  def hasError: Boolean
  def hasPayload: Boolean
  def seqId: Int
}

private[platform] abstract class MessageReceiver[Message <: DalServiceResponseMessage](
    val dalEnv: DalEnv,
    client: BatchedDalBrokerClient,
    connectionDescription: String,
    enableOutOfOrderCompletion: Boolean)
    extends CommandProtoSerialization {
  import MessageReceiver._

  // Subtypes should implement this depending on the kinds of payloads they expect to handle
  protected def unsafeParsePayload(
      requestUuid: String,
      seqId: Int,
      batchContext: BatchContext,
      now: Long,
      elapsed: => Long,
      message: Message): Unit

  protected def unsafeParseError(response: Message): Unit

  protected final def unsafeParseResponse(
      requestUuid: String,
      seqId: Int,
      now: Long,
      elapsed: => Long,
      batchContext: BatchContext,
      response: DalServiceResponse): Unit = {
    val isPartial = response.isPartial
    def isPartialResult(result: Result) = result match {
      case p: PartialResult[_] => !p.isLast
      case _                   => false
    }

    // after the broker changes are out we can remove this workaround and accept new style response only
    // after the client changes are out we can remove the old "results" field from the proto
    val resultsWithCommandIndices: Seq[(Int, (Result, ResultProto))] = {

      val resultProtos: Seq[ResultProto] = response.results
      val resultIndices: Seq[Int] = response.commandIndices

      require(resultProtos.nonEmpty, logPrefix(requestUuid, seqId) + "this response contains no results")
      require(
        resultProtos.size == resultIndices.size,
        s"${logPrefix(requestUuid, seqId)} number of results (${resultProtos.size}) does not match number of " +
          s"indices (${resultIndices.size})"
      )

      if (DebugZeroResult.value) {
        val isPartialPretty = s"${if (isPartial) "" else "non-"}partial"
        log.debug(
          s"debug 0 result-1 ${logPrefix(requestUuid, seqId)} new format $isPartialPretty response received " +
            s"containing a total of ${resultIndices.size} results for the following command(s): " +
            resultIndices.mkString(","))
      }

      val results: Seq[Result] = resultProtos.map(fromProto)

      if (shouldLogDalResultWeight) {
        val weight: Int = results.map(_.weight).sum
        // stdout usage intended, works with CI stdout plugin
        if (weight > 0) println(s"DAL result weight for $requestUuid: $weight")
      }

      resultIndices.zip(results.zip(resultProtos))

    }

    val resultsWithCommandIndicesGrouped: Map[Int, Seq[(Result, ResultProto)]] =
      resultsWithCommandIndices.groupMap(_._1)(_._2)

    // This logic should be encapsulated in batchContext itself once legacy message format no longer supported
    log.trace(s"${logPrefix(requestUuid, seqId)} obtaining BatchContext monitor")
    batchContext.synchronized {
      log.trace(s"${logPrefix(requestUuid, seqId)} obtained BatchContext monitor")
      resultsWithCommandIndicesGrouped.foreach { case (commandIndex, zipped) =>
        val (results, resultsProto) = zipped.unzip
        if (DebugZeroResult.value) {
          log.debug(
            s"debug 0 result-2 ${logPrefix(requestUuid, seqId)} commandIndex $commandIndex results.size " +
              s"${results.size} type ${results.map { _.getClass.getCanonicalName }.toSet.mkString(":")}")
        }
        val clientRequestContext = batchContext.commandIdToClientRequestContext(commandIndex)
        response.establishSessionResult.foreach { esrProto =>
          // check before set because setting involves deserializing proto
          if (clientRequestContext.establishSessionResult.isEmpty)
            clientRequestContext.establishSessionResult = Some(fromProto(esrProto))
        }

        val (saveResults: Seq[Result], saveProto: Seq[ResultProto]) =
          (results, clientRequestContext.completable) match {
            case (Seq(pqr: AbstractPartialResult[_, _]), NodeWithScheduler(_, _, _, PreCalc(pre))) =>
              pre.launch(pqr)
              (pre.mungeResults(results), Seq.empty[Dsi.ResultProto])
            case _ =>
              (results, resultsProto)
          }

        // If we have tc and times: Extract from clientRequestContext.completable if completable is NodeWithScheduler
        // FixedTemporalContextCache#loadEntity(tc, pe, si)
        clientRequestContext
          .addCommandResults( // This will add the results to seq in commandContext(commandIndex); note still under batch lock
            requestUuid,
            seqId,
            commandIndex,
            saveResults,
            saveProto,
            isPartialResult(results.last)
          )
      }
    }
    log.trace(s"${logPrefix(requestUuid, seqId)} released BatchContext monitor")

    completeBatch(requestUuid, seqId, now, elapsed, batchContext, response.isPartial, Some(response))
  }

  protected final def unsafeParseVersioningResponse(
      requestUuid: String,
      seqId: Int,
      now: Long,
      elapsed: => Long,
      batchContext: BatchContext,
      response: VersioningResponseProto): Unit = ??? /* {
    val versioningResult = response.getResult

    batchContext.synchronized {
      val commandIndex = 0
      val clientRequestContext = batchContext.commandIdToClientRequestContext(commandIndex)
      clientRequestContext.addCommandResults(
        requestUuid,
        seqId,
        commandIndex,
        Seq(fromProto(versioningResult)),
        Seq(versioningResult),
        isPartial = false)
    }

    completeBatch(requestUuid, seqId, now, elapsed, batchContext, isPartial = false, response = None)
  } */

  private def completeBatch(
      requestUuid: String,
      seqId: Int,
      now: Long,
      elapsed: => Long,
      batchContext: BatchContext,
      isPartial: Boolean,
      response: Option[DalServiceResponse]): Unit = ??? /* {
    // lazy to avoid deserializing if not used
    lazy val timings =
      response.flatMap(_.timings).map(_.getEntriesList.asScala.map(entry => (entry.getKey, entry.getValue)).toMap)

    if (batchContext.completeResponses(client, seqId, now, isPartial, enableOutOfOrderCompletion)) {
      log.trace(s"${logPrefix(requestUuid, seqId)} completing batch")
      client.onBatchComplete(seqId)
      if (Breadcrumbs.collecting) {
        timings.flatMap(_.get("tot")).map { totalTime =>
          val clientLatency = elapsed - totalTime
          if (clientLatency >= clientStatCrumbClientLatencyThreshold) {
            Breadcrumbs.info(
              batchContext.chainedId,
              PropertiesCrumb(
                _,
                DalClientCrumbSource,
                Properties.reqId -> requestUuid,
                Properties.clientLatency -> clientLatency.toString
              )
            )
          }
        }
      }

      val timingLogs = timings
        .map(generateTimingLog)
        .map(logLine => s" (detailed timings: $logLine)")
        .getOrElse("")
      val nRemaining = ClientRequestTracker.markComplete(requestUuid)
      log.info(s"${logPrefix(requestUuid, seqId)} batch completed in $elapsed ms, got ${batchContext.numResults} " +
        s"results, on connection $connectionDescription$timingLogs, env=${dalEnv}, appId=${client.clientCtx.appId}, zoneId=${client.clientCtx.zoneId}, remaining=$nRemaining")
    }
  } */

  // Entry-point when a message is received
  def messageCallback(response: Message): Unit = {
    require(response.hasPayload || response.hasError, "invalid response: no error, no payload")

    val seqId = response.seqId

    client.getBatch(seqId) match {
      case Some(batchContext @ BatchContext(requestUuid, chainedId, started)) =>
        val now = System.currentTimeMillis
        lazy val elapsed = now - started

        log.trace(
          s"Parsed envelope for batch ${logPrefix(requestUuid, seqId)}: ${batchContext.clientRequests.size} " +
            s"requests with ${batchContext.clientRequests.flatMap(_.commands).size} commands - " +
            s"${batchContext.numResults} results so far")

        Breadcrumbs.trace(
          OnceByCrumbEquality,
          chainedId,
          EventCrumb(_, DalClientCrumbSource, DalEvents.Client.ResponseReceived))

        def failBatch(throwable: Throwable): Unit = {
          log.error(s"${logPrefix(requestUuid, seqId)} batch failed in ${elapsed}ms", throwable)
          client.onBatchComplete(seqId)
          batchContext.completeAllWithException(Some(client), throwable)
          ClientRequestTracker.markComplete(requestUuid)
        }

        try {
          if (response.hasError) {
            log.error(s"${logPrefix(requestUuid, seqId)} response indicated error")
            unsafeParseError(response)
          } else {
            unsafeParsePayload(requestUuid, seqId, batchContext, now, elapsed, response)
          }
        } catch {
          case t: Throwable => failBatch(t)
        }

      case None =>
        // can't do any better, we found no request that we could fail, server does not want an answer back so all we
        // can do is log
        log.error(
          s"Received but not expected responses to request with sequence id $seqId, perhaps it's completed already?")
    }
  }

  private def generateTimingLog(timings: Map[String, Long]): String = {
    timings.map { case (key, value) => s"$key -> $value ms" }.mkString(", ")
  }
}

object MessageReceiver {
  private val log = getLogger(MessageReceiver)
  private val clientStatCrumbClientLatencyThresholdProperty = "optimus.dsi.client.breadcrumbThreshold"
  private val clientStatCrumbClientLatencyThreshold =
    DiagnosticSettings.getIntProperty(clientStatCrumbClientLatencyThresholdProperty, 20)
}
