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
package optimus.graph.tracking

import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb.ProfilerSource
import optimus.breadcrumbs.crumbs.ProfiledEventCause
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.graph.Settings
import optimus.graph.diagnostics.JsonMapper
import optimus.platform.util.Log
import optimus.scalacompat.collection._
import optimus.ui.HandlerResult
import optimus.ui.UiWorkerPerformanceRecorder
import org.apache.commons.lang3.StringUtils
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.time.Instant
import scala.collection.mutable
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.Using

object ProfilingEventStatistics extends Log {
  private final case class ConfigJson(
      threshold: Option[Int],
      interval: Option[Int],
      dropUntaggedChildren: Option[Boolean]
  )

  private final case class Config(
      recording: Option[Recording],
      dropUntaggedChildren: Boolean
  ) {
    def postProcess(pec: ProfiledEventCause): ProfiledEventCause =
      if (dropUntaggedChildren) {
        val updatedChildEvents = pec.childEvents.filter(_.profilingData.contains(HandlerResult.eventTagKey))
        log.debug(
          s"[${pec.eventName}] Dropping Untagged Children Events: ${pec.childEvents.size} => ${updatedChildEvents.size}")
        pec.copy(childEvents = pec.childEvents.filter(_.profilingData.contains(HandlerResult.eventTagKey)))
      } else pec
  }

  private val processors: Map[String, Config] = {
    val configs: Map[String, ConfigJson] = if (Settings.publishThrottledHandlerProfilingStats) {
      val configFile = Settings.throttledHandlerProfilingConfigFile
      if (configFile == "") {
        log.warn(
          "Missing profiling throttle config file. Please configure it using -Doptimus.handler.publishProfilingDataConfigFile")
        Map.empty
      } else {
        Using(Source.fromResource(configFile)("utf-8").bufferedReader()) { reader =>
          Try(JsonMapper.mapper.readValue[Map[String, ConfigJson]](reader)) match {
            case Success(result) => result
            case Failure(e) =>
              log.error(s"Failed to parse config file $configFile, no performance breadcrumbs will be sent!", e)
              Map.empty[String, ConfigJson]
          }
        }.getOrElse {
          log.error(s"Failed to read config file $configFile, no performance breadcrumbs will be sent!")
          Map.empty
        }
      }
    } else Map.empty

    configs.map { case (key, config) =>
      key -> Config(
        (for {
          threshold <- config.threshold
          interval <- config.interval
        } yield new Recording(key, threshold, interval)),
        config.dropUntaggedChildren.getOrElse(false)
      )
    }
  }

  private[optimus] def resetForTesting(now: Instant = Instant.now()): Unit = {
    processors.valuesIterator.foreach(_.recording.foreach(_.resetInterval(now)))
  }

  private def sendSingleBreadcrumb(profiledEvent: ProfiledEventCause): Unit = {
    if (Settings.profilingStatsDumpToFile) {
      ProfilingEventStatsFileUtil.dumpSingleEventData(profiledEvent)
    } else {
      Breadcrumbs.info(
        ChainedID.root,
        PropertiesCrumb(
          _,
          ProfilerSource,
          Properties.profiledEvent -> profiledEvent,
          Properties.appDir -> Option(System.getenv("APP_DIR"))
            .getOrElse(sys.props.getOrElse("user.dir", "unknown")),
          Properties.user -> sys.props.getOrElse("user.name", "unknown"),
          Properties.appId -> Option(System.getProperty("APP_NAME"))
            .getOrElse(System.getenv().asScala.getOrElse("APP_NAME", "unknown"))
        )
      )
      log.debug(
        s"${profiledEvent.eventName} completed in ${profiledEvent.totalDurationMs} ms, actionSelfTime: ${profiledEvent.actionSelfTimeMs} ms with meta ${profiledEvent.profilingData}, childEventsSize: ${profiledEvent.childEvents.size} (${ChainedID.root})")
    }
  }

  /**
   * Publish profiled events but aggregated to reduce crumb volume.
   *
   *  1. Only profiled events are published
   *  1. The tree of event is aggregated such that there is only two layers (root + profiled children)
   *  1. Data is accumulated over multiple calls
   */
  private[graph] def processProfiledEvent(event: EventCause): Unit = {
    val publishThisEvent = Settings.handlerProfilingDataPublish && event.root.rootProfiler.hasProfiledChildren
    if (Settings.publishThrottledHandlerProfilingStats) {
      val raw = event.profile.getSummaryProfilingData
      val globalTag = raw.profilingData.getOrElse(EventCause.globalTagKey, "unknown")
      // the globalEventTag looks like this: "SnapCycle-xyz layout", the string before "-" is the global tag, the rest is a suffix which can be information from user's config/UI/.etc.
      val globalTagPrefix = globalTag.split('-').head.trim
      val proc = processors.get(globalTagPrefix)

      val profiledEventCause = proc.fold(raw)(_.postProcess(raw))
      val recording = proc.flatMap(_.recording)

      recording match {
        case Some(processor) =>
          if (processor.interval >= 0) {
            processor.movingProfStatistic(profiledEventCause, globalTag)
          }
        case _ =>
          val doSend =
            if (publishThisEvent) true
            else if (Settings.profilingStatsSendNonConfigEvents) {
              log.warn(
                s"Remove -Doptimus.handler.profilingStatsSendNonConfigEvents=true in PROD to avoid excessive breadcrumbs!")
              true
            } else false

          if (doSend) sendSingleBreadcrumb(profiledEventCause)
      }
    } else if (publishThisEvent) {
      sendSingleBreadcrumb(event.profile.getSummaryProfilingData)
    }
  }

  // Mutable state, only touch when synchronized
  class Recording(targetGlobalTag: String, threshold: Int, val interval: Int) {
    private var allGlobalTags = mutable.Set.empty[String]
    private var profDurationBinaryHeap = List.empty[Long]
    private var metaDataList = List.empty[Map[String, String]]
    private var intervalStartTime: Instant = Instant.now()
    private var intervalExpectedEndTime: Instant = intervalStartTime.plusMillis(interval)
    private var totalCount = 0
    private var exceededCount = 0

    // This is not synchronized as it's only called in the synchronized block in movingProfStatistic
    private def toRecord: Option[Record] = {
      if (totalCount > 0)
        Some(
          Record(
            profDurationBinaryHeap.sorted,
            metaDataList,
            intervalStartTime,
            allGlobalTags.toSeq,
            totalCount,
            exceededCount,
            threshold,
            interval,
            targetGlobalTag))
      else None
    }

    // This is not synchronized as it's only called in the synchronized block in movingProfStatistic
    private def addEvent(globalTag: String, profDuration: Long, profiledEvent: ProfiledEventCause): Unit = {
      totalCount += 1
      allGlobalTags += globalTag
      profDurationBinaryHeap +:= profDuration
      if (metaDataList.size < 10) {
        val childMetaData = profiledEvent.childEvents
          .foldLeft(mutable.Map.empty[String, String]) { (map, childEvent) => map ++= childEvent.profilingData }
          .toMap
        if (childMetaData.nonEmpty) metaDataList :+= childMetaData
      }
      if (profDuration > threshold) {
        exceededCount += 1
        if (Settings.profilingStatsSendThrottledEvents) ProfilingEventStatistics.sendSingleBreadcrumb(profiledEvent)
      }
    }

    private[ProfilingEventStatistics] def resetInterval(currentTime: Instant): Unit = synchronized {
      allGlobalTags = mutable.Set.empty[String]
      profDurationBinaryHeap = Nil
      metaDataList = Nil
      intervalStartTime = currentTime
      intervalExpectedEndTime = currentTime.plusMillis(interval)
      totalCount = 0
      exceededCount = 0
    }

    private[graph] def movingProfStatistic(profiledEvent: ProfiledEventCause, globalTag: String): Unit = {
      val currentTime = Instant.now()
      val toSend = synchronized {
        val record = if (!currentTime.isBefore(intervalExpectedEndTime)) {
          val r = toRecord // Step 1
          resetInterval(currentTime) // Step 2
          r
        } else None
        addEvent(globalTag, profiledEvent.totalDurationMs, profiledEvent) // Step 3
        record
      }
      toSend.foreach { r => r.computeStatsAndSendBreadcrumb(currentTime) }
    }
  }

  val percentileKeys: Seq[(String, Double)] = Seq(
    "p50" -> 50,
    "p60" -> 60,
    "p70" -> 70,
    "p80" -> 80,
    "p90" -> 90,
    "p95" -> 95,
    "p99" -> 99,
    "p99.9" -> 99.9
  )

  val statsKeys = Seq("avg") ++ percentileKeys.map(_._1)

  // not mutated
  final case class Record(
      profDurationList: Seq[Long],
      metaDataList: Seq[Map[String, String]],
      intervalStartTime: Instant,
      allGlobalTags: Seq[String],
      totalCount: Int,
      exceededCount: Int,
      threshold: Int,
      interval: Int,
      targetGlobalTag: String) {
    private def calculatePercentile(percentile: Double, sortedArray: Seq[Long]): Long = {
      val index = math.floor(percentile / 100 * sortedArray.length)
      sortedArray(index.toInt)
    }

    private def calculateAverage(sortedArray: Seq[Long]): Long = (sortedArray.sum.toDouble / sortedArray.length).toLong

    private def sendBreadcrumb(
        data: Map[String, JsValue],
        intervalStartTime: Instant,
        intervalEndTime: Instant,
        uiWorkerStats: Map[String, JsValue],
        metaData: Seq[Map[String, JsValue]]): Unit = {
      val actualInterval = (intervalEndTime.toEpochMilli - intervalStartTime.toEpochMilli).toInt
      val profiledEventStats: Map[String, JsValue] = Map(
        "globalTag" -> JsString(targetGlobalTag),
        "allGlobalTags" -> allGlobalTags.toJson,
        "threshold" -> JsNumber(threshold),
        "interval" -> JsNumber(interval),
        "totalNoOfEvents" -> JsNumber(totalCount),
        "exceededNoOfEvents" -> JsNumber(exceededCount),
        "exceedanceRate" -> JsNumber(exceededCount.toDouble / totalCount.toDouble),
        "intervalStartTime" -> JsString(intervalStartTime.toString),
        "intervalEndTime" -> JsString(intervalEndTime.toString),
        "actualInterval" -> JsNumber(actualInterval),
        "durationStatistics" -> data.toJson,
        "uiWorkerStats" -> uiWorkerStats.toJson,
        "metaData" -> metaData.toJson
      )
      if (Settings.profilingStatsDumpToFile) {
        ProfilingEventStatsFileUtil.dumpStatsBreadcrumbsData(profiledEventStats, data, uiWorkerStats, metaData)
      } else {
        Breadcrumbs.info(
          ChainedID.root,
          PropertiesCrumb(
            _,
            ProfilerSource,
            Properties.profiledEventStatistics -> profiledEventStats,
            Properties.appDir -> Option(System.getenv("APP_DIR")).getOrElse(sys.props.getOrElse("user.dir", "unknown")),
            Properties.user -> sys.props.getOrElse("user.name", "unknown"),
            Properties.appId -> Option(System.getProperty("APP_NAME"))
              .getOrElse(Option(System.getenv("APP_NAME")).getOrElse("unknown"))
          )
        )
      }
    }

    private[graph] def computeStatsAndSendBreadcrumb(currentTime: Instant): Unit = {
      if (profDurationList.nonEmpty) {
        // calculate percentiles and send breadcrumb
        val percentiles = Map("avg" -> JsNumber(calculateAverage(profDurationList))) ++
          percentileKeys.map { case (key, d) => key -> JsNumber(calculatePercentile(d, profDurationList)) }

        val metaData: Seq[Map[String, JsValue]] = metaDataList.map(_.mapValuesNow { value =>
          if (StringUtils.isNumeric(value)) JsNumber(value) else JsString(value)
        })

        sendBreadcrumb(
          percentiles,
          intervalStartTime,
          currentTime,
          UiWorkerPerformanceRecorder.getBreadcrumbs,
          metaData)
      }
    }
  }
}
