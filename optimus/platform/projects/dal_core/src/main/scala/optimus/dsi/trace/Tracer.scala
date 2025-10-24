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
package optimus.dsi.trace

import com.google.common.util.concurrent.AtomicDouble
import com.google.common.util.concurrent.AtomicLongMap
import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.crumbs.CrumbNodeType
import optimus.breadcrumbs.crumbs.EdgeType
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.breadcrumbs.filter.CrumbFilter.DEFAULT_MAX_CRUMB_SIZE
import optimus.dsi.trace.TraceBackEnd.BackEnd
import optimus.dsi.trace.TraceIdentity.CacheIdentity
import optimus.graph.DiagnosticSettings
import optimus.graph.Edges
import optimus.platform.dal.config.DalConfigurationContext
import optimus.platform.dal.config.Host
import optimus.platform.dsi.DalTraceCrumbSource
import optimus.platform.storable.SerializedEntity
import optimus.scalacompat.collection._
import optimus.utils.datetime.ZoneIds
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.time.Duration
import java.time.Instant
import java.time.ZonedDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.regex.Pattern
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.reflect.macros.blackbox.Context
import scala.util.control.NonFatal

/**
 * The traceId should be unique for each incoming request, even for the requests which have been retried by the client.
 */
object Tracer {
  import RequestTracer._
  import TraceEvent._
  import TraceRelevance._

  private[optimus] val log = getLogger(Tracer.getClass.getName)
  private val enabled = new AtomicBoolean(DiagnosticSettings.getBoolProperty("optimus.dsi.trace.enabled", true))
  private val logThreshold = new AtomicInteger(DiagnosticSettings.getIntProperty("optimus.dsi.trace.logThreshold", 500))
  // trace has to last at least this long (in milliseconds) to get published
  private val breadcrumbThreshold = new AtomicInteger(
    DiagnosticSettings.getIntProperty("optimus.dsi.trace.breadcrumbThreshold", 50))
  // scaling power factor for the fixed relevance table
  // the current default means the least relevant trace (TraceRelevance.One) has to last at least 30 seconds to register
  private val breadcrumbRelevanceThresholdPower = new AtomicDouble(
    DiagnosticSettings.getDoubleProperty("optimus.dsi.trace.breadcrumbRelevanceThresholdPower", 1.21035))
  private val breadcrumbTraceThreadFilter = new AtomicReference(
    BreadcrumbTraceFilterConfig(
      DiagnosticSettings.getStringProperty(
        "optimus.dsi.trace.breadcrumbTraceThreadFilter",
        "(dsiExecutor.*|cacheExecutor.*|DdcResponseExecutor.*)")))
  final case class RequestState(layer: Tracing.Phase, event: TraceEvent.Event, threadName: String)
  final case class Interval(startTime: Instant, endTime: Instant) {
    lazy val durationMillis = endTime.toEpochMilli - startTime.toEpochMilli
  }
  final case class BreadcrumbTraceFilterConfig(threadFilter: String) {
    lazy val threadFilterPattern: Pattern = Pattern.compile(threadFilter)
  }
  private final case class TraceCrumbAccumulator(accum: Seq[JsValue], size: Int)

  private final case class GroupedCrumbTrace(
      id: String,
      event: String,
      start: String,
      dur: String,
      timing: String
  ) {
    def toJson(): JsValue = {
      Map[String, String]("id" -> id, "event" -> event, "start" -> start, "dur" -> dur, "timing" -> timing).toJson
    }
  }

  /**
   * For a given traceId, records the beginning and end of an event.
   */
  def recordEvent[A](id: TraceId, event: TraceEvent.Event)(f: A): A = macro TracerMacros.recordEventImpl[A]

  /**
   * For a given bunch of traceId, records the beginning and end of an event.
   */
  def recordBatchEvent[A](ids: Set[TraceId], event: TraceEvent.Event)(f: A): A =
    macro TracerMacros.recordBatchEventImpl[A]

  def recordEventsBatch[A](events: Seq[(TraceId, TraceEvent.Event)])(f: A): A =
    macro TracerMacros.recordEventsBatchImpl[A]

  /**
   * For a given traceId, records the beginning of an event. It should be called exactly once for a given pair of
   * traceId and event.
   */
  def recordStartEvent(id: TraceId, event: TraceEvent.Event with TraceEvent.ImpliesWallclockTiming): Unit = {
    if (enabled.get()) {
      runSafe {
        val tr = event match {
          case _: TraceEvent.ImpliesFirstTrace => TracerRegistry.create(id)
          case _                               => TracerRegistry.get(id)
        }
        if (tr != null) {
          tr.startTrace(event)
        }
      }
    }
  }

  private[trace] def getStoredTracesSize(id: TraceId): Int = TracerRegistry.get(id).getStoredTracesSize

  private final val tracerName = "DALTracer"
  private final val requestIdProperty = "reqId"
  private final val timingProperty = "timing"
  private final val typeProperty = "type"
  private[dsi] final val threadIdProperty = "threadId"
  private final val startProperty = "start"
  private final val durationProperty = "duration"
  private final val wallclockPropertyValue = Map(timingProperty -> "wc")
  private final val aggregatePropertyValue = Map(timingProperty -> "agg")

  private val relevanceToRequestDurationThreshold = Map(
    One -> 5000.0,
    Two -> 2500.0,
    Three -> 1000.0,
    Four -> 500.0,
    Five -> 250.0,
    Six -> 175.0,
    Seven -> 100.0,
    Eight -> 50.0,
    Nine -> 10.0,
    Ten -> 1.0
  )

  private def flushTrace(id: TraceId, tr: RequestTracer): Unit = {
    val relevanceThresholdPower = breadcrumbRelevanceThresholdPower.get
    val tracesSeen = new scala.collection.mutable.HashSet[TraceData]()

    def batchTraceCrumbs(groupedTraces: Iterator[GroupedCrumbTrace]): Unit = {
      val remainingTraces: TraceCrumbAccumulator =
        groupedTraces.foldLeft(TraceCrumbAccumulator(Seq.empty[JsValue], 0))(
          (left: TraceCrumbAccumulator, right: GroupedCrumbTrace) => {
            val nextTrace: JsValue = right.toJson()
            val nextTraceSize: Int = nextTrace.toString.length
            // To send each trace as an individual crumb, replace '(DEFAULT_MAX_CRUMB_SIZE - 200)' with 0
            if (left.size + nextTraceSize < (DEFAULT_MAX_CRUMB_SIZE - 200) || left.size == 0) {
              TraceCrumbAccumulator(left.accum :+ nextTrace, left.size + nextTraceSize)
            } else {
              Breadcrumbs.info(
                id.childId,
                PropertiesCrumb(
                  _,
                  DalTraceCrumbSource,
                  DalConfigurationContext.wrap(Map[String, String](requestIdProperty -> id.requestId)),
                  Map[String, JsValue]("traces" -> left.accum.toJson)
                )
              )
              TraceCrumbAccumulator(Seq[JsValue](nextTrace), nextTraceSize)
            }
          })
      if (remainingTraces.accum.nonEmpty) {
        Breadcrumbs.info(
          id.childId,
          PropertiesCrumb(
            _,
            DalTraceCrumbSource,
            DalConfigurationContext.wrap(Map[String, String](requestIdProperty -> id.requestId)),
            Map[String, JsValue]("traces" -> remainingTraces.accum.toJson)
          )
        )
      }
    }

    def prepareRelevantBreadcrumbs(
        trace: TraceData,
        durations: Map[TraceEvent.Event, Long],
        bcThreshold: Long): Option[GroupedCrumbTrace] = {
      def traceRelevant(trace: TraceData, durations: Map[TraceEvent.Event, Long], threshold: Long) = {
        val relevantDuration =
          durations.getOrElse(TraceEvent.DsiRequest, durations.getOrElse(TraceEvent.SKRequest, 0L)) - durations
            .filter { case (ev, dur) => ev.isInstanceOf[ImpliesStallTiming] }
            .values
            .sum
        trace.durationMillis >= threshold && relevantDuration >= scala.math
          .pow(relevanceToRequestDurationThreshold(trace.event.relevance), relevanceThresholdPower)
      }

      if (Breadcrumbs.collecting && traceRelevant(trace, durations, bcThreshold) && !tracesSeen.contains(trace)) {
        val childId = id.childId
        Edges.ensureTracked(childId, id.chainedId, tracerName, CrumbNodeType.Trace, EdgeType.InvokedBy)
        val values: Map[String, String] = trace.event match {
          case _: ImpliesWallclockTiming => trace.values ++ wallclockPropertyValue
          case _                         => trace.values ++ aggregatePropertyValue
        }
        tracesSeen.add(trace)
        Some(
          GroupedCrumbTrace(
            childId.toString.replaceFirst("^[^#]+#", "#"),
            values("event"),
            values("startTimeUtc"),
            values("durationMillis"),
            values("timing")
          ))
      } else {
        None
      }
    }

    def writeTraceLog(trace: TraceData, duration: Long, threshold: Long): Unit = {
      val traceStr = s"""{\"uuid\": \"${id.requestId}\", ${trace.toString}}"""
      trace.level match {
        case TraceLevel.Info if (duration >= threshold)  => log.info(traceStr)
        case TraceLevel.Debug if (duration >= threshold) => log.debug(traceStr)
        case _                                           => log.trace(traceStr)
      }
    }

    try {
      val traces = tr.getTraces()
      val durations =
        tr.getLongestTraceDurations(t =>
          t match {
            case _: ImpliesFirstTrace | _: ImpliesStallTiming => true
            case _                                            => false
          })
      val threshold = logThreshold.get.toLong
      val bcThreshold = breadcrumbThreshold.get.toLong
      if (Breadcrumbs.collecting) {
        val groupedTraces = traces.iterator.flatMap { t =>
          writeTraceLog(
            t,
            durations.getOrElse(
              TraceEvent.DsiRequest,
              durations.getOrElse(TraceEvent.SKRequest, durations.getOrElse(TraceEvent.SKForwardRequest, 0L))),
            threshold)
          prepareRelevantBreadcrumbs(t, durations, bcThreshold)
        }
        batchTraceCrumbs(groupedTraces)
      } else {
        traces.foreach { t =>
          writeTraceLog(
            t,
            durations.getOrElse(
              TraceEvent.DsiRequest,
              durations.getOrElse(TraceEvent.SKRequest, durations.getOrElse(TraceEvent.SKForwardRequest, 0L))),
            threshold)
        }
      }
    } finally {
      TracerRegistry.remove(id)
      tr.clear()
    }
  }

  /**
   * For a given traceId, records the end of an event. It should be called exactly once for a given pair of traceId and
   * event.
   */
  def recordStopEvent(id: TraceId, event: TraceEvent.Event with TraceEvent.ImpliesWallclockTiming): Unit = {
    if (enabled.get()) {
      runSafe {
        val tr = TracerRegistry.get(id)
        if (tr != null) {
          tr.endTrace(event)
          event match {
            case _: TraceEvent.ImpliesFirstTrace => flushTrace(id, tr)
            case _                               =>
          }
        }
      }
    }
  }

  /**
   * Gets current start time of inflight event (if there exists such an event).
   */
  def inflightTraceStartTime(
      id: TraceId,
      event: TraceEvent.Event with TraceEvent.ImpliesWallclockTiming): Option[Long] = {
    inflightTraceStartTime(Seq(id), event)
  }

  /**
   * Gets the earliest start time of inflight event (if there exists such an event) for the given sub-request traceIds
   */
  def inflightTraceStartTime(
      ids: Seq[TraceId],
      event: TraceEvent.Event with TraceEvent.ImpliesWallclockTiming): Option[Long] = {
    if (enabled.get()) {
      val ts = ids flatMap { id =>
        Option(TracerRegistry.get(id)) flatMap { _.inflightTraceStartTime(event) }
      }
      if (ts.nonEmpty) Some(ts.min) else None
    } else {
      None
    }
  }

  /*
   * Records the startTime and duration of the passed event for the given traceid.
   */
  def recordEventDuration(id: TraceId, event: TraceEvent.Event, startTime: Instant, durationMillis: Long): Unit = {
    if (enabled.get()) {
      runSafe {
        val tr = TracerRegistry.get(id)
        if (tr != null) {
          tr.recordTrace(event, startTime, durationMillis)
        }
      }
    }
  }

  /*
   * Records any data in Long format for the given event
   * If data already exists for the given event then it
   * will add to that
   */
  def recordEventData(id: TraceId, event: TraceEvent.Event, data: Long): Unit = {
    if (enabled.get()) {
      runSafe {
        val tr = TracerRegistry.get(id)
        if (tr != null) {
          tr.aggregateTraces.addAndGet(event, data)
        }
      }
    }
  }

  def recordEventData[T](id: TraceId, event: TraceEvent.Event, data: T): Unit = {
    if (enabled.get()) {
      runSafe {
        val tr = TracerRegistry.get(id)
        if (tr != null) tr.recordTrace[T](event, data)
      }
    }
  }

  /*
   * Return aggregate of all Long data for the given event
   */
  def getEventAggregateData(id: TraceId, event: TraceEvent.Event): Long = {
    if (enabled.get()) {
      val agg = Option(TracerRegistry.get(id)).map(_.getAggregateTrace(event))
      agg.getOrElse(0L)
    } else 0L
  }

  def getAllEventData[T](id: TraceId, event: TraceEvent.Event): Seq[T] = {
    if (enabled.get()) {
      val agg =
        Option(TracerRegistry.get(id))
          .map(_.getCustomDataTraces[T](traceEvent => traceEvent == event))
          .getOrElse(Seq.empty[TraceCustomData[T]])
      agg.map(_.data)
    } else Seq.empty[T]
  }

  private[trace] def difference(interval: Interval, exclude: Seq[Interval]): Seq[Interval] = {
    def getInterval(start: Instant, end: Instant): Option[Interval] = {
      if (start.isBefore(end)) Some(Interval(start, end)) else None
    }

    val resBld = Seq.newBuilder[Interval]
    var remaining = Option(interval)
    exclude.sortBy(_.startTime) foreach { case Interval(start, end) =>
      remaining match {
        case Some(Interval(startTime, endTime)) =>
          getInterval(startTime, Seq(start, endTime).min).foreach(resBld += _)
          remaining = getInterval(Seq(end, startTime).max, endTime)
        case _ =>
      }
    }
    remaining foreach (resBld += _)
    resBld.result()
  }

  private[dsi] def merge(intervals: Seq[Interval]): Seq[Interval] = {
    // get the minimal startTime and maximal endTime to construct an interval
    val startEnd =
      intervals.foldLeft(Interval(Instant.ofEpochMilli(Long.MaxValue), Instant.ofEpochMilli(Long.MinValue))) {
        case (Interval(res_s, res_e), Interval(start, end)) =>
          Interval(Seq(res_s, start).min, Seq(res_e, end).max)
      }
    // trick: difference(startEnd, intervals) = startEnd - merge(intervals)
    difference(startEnd, difference(startEnd, intervals))
  }

  /*
   * Records the intervals of the passed event for the given traceid, excluding the given intevals
   */
  def recordEventDuration(
      id: TraceId,
      event: TraceEvent.Event,
      startTime: Instant,
      endTime: Instant,
      exclude: Seq[Interval]): Unit = {
    if (enabled.get()) {
      runSafe {
        val tr = TracerRegistry.get(id)
        if (tr != null) {
          val intervals = difference(Interval(startTime, endTime), exclude)
          intervals foreach { case i @ Interval(start, end) =>
            tr.recordTrace(event, start, i.durationMillis)
          }
        }
      }
    }
  }

  def recordBatchEventDuration(
      ids: Set[TraceId],
      event: TraceEvent.Event,
      startTime: Instant,
      durationMillis: Long): Unit = {
    if (enabled.get()) {
      ids foreach { id =>
        recordEventDuration(id, event, startTime, durationMillis)
      }
    }
  }

  /*
   * For a given traceId, groups the BackEndAware events by the backEnd value and aggregates the durationMillis for each group.
   */
  def getBackendTraceAggregateTime(id: TraceId): Map[TraceBackEnd.BackEnd, Long] = {
    getBackendTraceAggregateTime(Seq(id))
  }

  /*
   * For a bunch of traceIds, groups the BackEndAware events by the backEnd value and aggregates the durationMillis for each group.
   */
  def getBackendTraceAggregateTime(ids: Seq[TraceId]): Map[TraceBackEnd.BackEnd, Long] = {
    if (enabled.get()) {
      val traces = ids flatMap { id =>
        Option(TracerRegistry.get(id)) map { _.getTraces() } getOrElse Nil
      }
      traces collect { case TraceData(event: TraceEvent.Event with TraceEvent.BackEndAware, _, l, _) =>
        (event, l)
      } groupBy { case (t, _) => t.backEnd } map { case (b, evts) =>
        b -> { evts map (_._2) sum }
      }
    } else Map.empty
  }

  def getAllBackendTraceWallclockTime(ids: Seq[TraceId]): Map[TraceBackEnd.BackEnd, Long] =
    getBackendTraceWallclockTime[TraceEvent.BackEndAware](ids)

  /*
   * For a bunch of traceIds, groups the BackEndAware events by the backEnd value and aggregates the MERGED intervals for each group.
   */
  def getBackendTraceWallclockTime[T <: TraceEvent.BackEndAware](
      ids: Seq[TraceId]
  )(implicit tag: ClassTag[T]): Map[TraceBackEnd.BackEnd, Long] = {
    if (enabled.get()) {
      val traces = ids flatMap { id =>
        Option(TracerRegistry.get(id)) map { _.getTraces() } getOrElse Nil
      }
      traces collect { case t @ TraceData(event: T, _, _, _) =>
        (event, t)
      } groupBy { case (t, _) =>
        t.backEnd
      } map { case (b, datas) =>
        b -> {
          merge(datas map { case (_, t @ TraceData(_, startTime, _, _)) =>
            Interval(startTime, t.endTime)
          }).foldLeft(0L) { case (r, interval) => r + interval.durationMillis }
        }
      }
    } else Map.empty
  }

  /*
   * For a bunch of traceIds, get the MERGED interval for QStall wallclock time.
   */
  def getStallWallclockTime(ids: Seq[TraceId]): Long = {
    if (enabled.get()) {
      val traces = ids flatMap { id =>
        Option(TracerRegistry.get(id)) map { _.getTraces() } getOrElse Nil
      }
      merge(traces collect { case t @ TraceData(_: ImpliesStallTiming, startTime, _, _) =>
        Interval(startTime, t.endTime)
      }).foldLeft(0L) { case (r, interval) => r + interval.durationMillis }
    } else 0L
  }

  def getLsqtCheckAggregateTime(id: TraceId): Long = getLsqtCheckAggregateTime(Seq(id))
  def getLsqtCheckAggregateTime(ids: Seq[TraceId]): Long = {
    getAggregateTime(ids, TraceEvent.LsqtWait)
  }

  def getPinningAggregateTime(id: TraceId): Long = getPinningAggregateTime(Seq(id))
  def getPinningAggregateTime(ids: Seq[TraceId]): Long = {
    getAggregateTime(ids, TraceEvent.MongoPinning)
  }

  def getExecQAggregateTime(id: TraceId): Long = getExecQAggregateTime(Seq(id))
  def getExecQAggregateTime(ids: Seq[TraceId]): Long = {
    getAggregateTime(ids, TraceEvent.ExecQ)
  }

  def getPreExecQAggregateTime(id: TraceId): Long = getPreExecQAggregateTime(Seq(id))
  def getPreExecQAggregateTime(ids: Seq[TraceId]): Long = {
    getAggregateTime(ids, TraceEvent.PreExecQ)
  }

  def getEntitlementAggregateTime(id: TraceId): Long = getEntitlementAggregateTime(Seq(id))
  def getEntitlementAggregateTime(ids: Seq[TraceId]): Long = {
    getAggregateTime(
      ids,
      (t: TraceEvent.Event) =>
        t match {
          case _: TraceEvent.EntitlementCheck => true
          case _                              => false
        })
  }

  def getEntitlementLoadAggregateTime(id: TraceId): Long = getEntitlementLoadAggregateTime(Seq(id))
  def getEntitlementLoadAggregateTime(ids: Seq[TraceId]): Long = {
    getAggregateTime(
      ids,
      (t: TraceEvent.Event) =>
        t match {
          case TraceEvent.EntitlementLoad => true
          case _                          => false
        })
  }

  def getEntitlementLoadLoadEntitlementsAggregateTime(id: TraceId): Long =
    getEntitlementLoadLoadEntitlementsAggregateTime(Seq(id))
  def getEntitlementLoadLoadEntitlementsAggregateTime(ids: Seq[TraceId]): Long = {
    getAggregateTime(
      ids,
      (t: TraceEvent.Event) =>
        t match {
          case TraceEvent.EntitlementLoadLoadEntitlements => true
          case _                                          => false
        })
  }

  def getEntitlementLoadLoadFeatureEntitlementsAggregateTime(id: TraceId): Long =
    getEntitlementLoadLoadFeatureEntitlementsAggregateTime(Seq(id))
  def getEntitlementLoadLoadFeatureEntitlementsAggregateTime(ids: Seq[TraceId]): Long = {
    getAggregateTime(
      ids,
      (t: TraceEvent.Event) =>
        t match {
          case TraceEvent.EntitlementLoadLoadFeatureEntitlements => true
          case _                                                 => false
        })
  }

  def getEntitlementLoadLoadLinkagesAggregateTime(id: TraceId): Long =
    getEntitlementLoadLoadLinkagesAggregateTime(Seq(id))
  def getEntitlementLoadLoadLinkagesAggregateTime(ids: Seq[TraceId]): Long = {
    getAggregateTime(
      ids,
      (t: TraceEvent.Event) =>
        t match {
          case TraceEvent.EntitlementLoadLoadLinkages => true
          case _                                      => false
        })
  }

  def getEntitlementLoadLoadDalEntitlementsAggregateTime(id: TraceId): Long =
    getEntitlementLoadLoadDalEntitlementsAggregateTime(Seq(id))
  def getEntitlementLoadLoadDalEntitlementsAggregateTime(ids: Seq[TraceId]): Long = {
    getAggregateTime(
      ids,
      (t: TraceEvent.Event) =>
        t match {
          case TraceEvent.EntitlementLoadLoadDalEntitlements => true
          case _                                             => false
        })
  }

  def getEntitlementLoadGetPayloadsAggregateTime(id: TraceId): Long =
    getEntitlementLoadGetPayloadsAggregateTime(Seq(id))
  def getEntitlementLoadGetPayloadsAggregateTime(ids: Seq[TraceId]): Long = {
    getAggregateTime(
      ids,
      (t: TraceEvent.Event) =>
        t match {
          case TraceEvent.EntitlementLoadGetPayloads => true
          case _                                     => false
        })
  }

  def getEntitlementLoadPreloadPermissionsAggregateTime(id: TraceId): Long =
    getEntitlementLoadPreloadPermissionsAggregateTime(Seq(id))
  def getEntitlementLoadPreloadPermissionsAggregateTime(ids: Seq[TraceId]): Long = {
    getAggregateTime(
      ids,
      (t: TraceEvent.Event) =>
        t match {
          case TraceEvent.EntitlementLoadPreloadPermissions => true
          case _                                            => false
        })
  }

  def getEntitlementLoadEntitlementTransformAggregateTime(id: TraceId): Long =
    getEntitlementLoadEntitlementTransformAggregateTime(Seq(id))
  def getEntitlementLoadEntitlementTransformAggregateTime(ids: Seq[TraceId]): Long = {
    getAggregateTime(
      ids,
      (t: TraceEvent.Event) =>
        t match {
          case TraceEvent.EntitlementLoadEntitlementTransform => true
          case _                                              => false
        })
  }

  def getEntitlementLoadCacheMissAggregateTime(id: TraceId): Long = getEntitlementLoadCacheMissAggregateTime(Seq(id))
  def getEntitlementLoadCacheMissAggregateTime(ids: Seq[TraceId]): Long = {
    getAggregateTime(
      ids,
      (t: TraceEvent.Event) =>
        t match {
          case TraceEvent.EntitlementLoadCacheMiss => true
          case _                                   => false
        })
  }

  def getEntitlementLinkagesCheckWallClockTime(id: TraceId): Long = getEntitlementLinkagesCheckWallClockTime(Seq(id))
  def getEntitlementLinkagesCheckWallClockTime(ids: Seq[TraceId]): Long = {
    getWallClockTime(
      ids,
      (t: TraceEvent.Event) =>
        t match {
          case TraceEvent.EntitlementLinkagesCheck => true
          case _                                   => false
        })
  }

  def getEntitlementWallclockTime(id: TraceId): Long = getEntitlementWallclockTime(Seq(id))
  def getEntitlementWallclockTime(ids: Seq[TraceId]): Long = {
    getWallClockTime(
      ids,
      (t: TraceEvent.Event) =>
        t match {
          case _: TraceEvent.EntitlementCheck => true
          case _                              => false
        })
  }

  def getEntitlementLoadWallclockTime(id: TraceId): Long = getEntitlementLoadWallclockTime(Seq(id))
  def getEntitlementLoadWallclockTime(ids: Seq[TraceId]): Long = {
    getWallClockTime(
      ids,
      (t: TraceEvent.Event) =>
        t match {
          case _: TraceEvent.EntitlementLoadEventBase => true
          case _                                      => false
        })
  }

  def getSlowPayloadReadCount(id: TraceId): Long = getSlowPayloadReadCount(Seq(id))
  def getSlowPayloadReadCount(ids: Seq[TraceId]): Long = {
    getAggregate[Long](
      ids,
      0L,
      (a: Long, event: TraceEvent.Event, _: Long) =>
        event match {
          case e: TraceEvent.PayloadRead => a + e.slowReadCount
          case _                         => a
        })
  }

  def getEntitlementCheckStageCount(id: TraceId): (Long, Long) = getEntitlementCheckStageCount(Seq(id))
  def getEntitlementCheckStageCount(ids: Seq[TraceId]): (Long, Long) = {
    val pre = getPreEntitlementCheckCount(ids)
    val post = getPostEntitlementCheckCount(ids)
    (pre, post)
  }

  def getPreEntitlementCheckCount(ids: Seq[TraceId]): Long = {
    getAggregate[Long](
      ids,
      0L,
      (a: Long, event: TraceEvent.Event, _: Long) =>
        event match {
          case e: TraceEvent.PreEntitlementCheck => a + e.count
          case _                                 => a
        })
  }
  def getPostEntitlementCheckCount(ids: Seq[TraceId]): Long = {
    getAggregate[Long](
      ids,
      0L,
      (a: Long, event: TraceEvent.Event, _: Long) =>
        event match {
          case e: TraceEvent.PostEntitlementCheck => a + e.count
          case _                                  => a
        })
  }

  def getTxnRetryAggregateTimeAndCount(id: TraceId): (Long, Int) = getTxnRetryAggregateTimeAndCount(Seq(id))
  def getTxnRetryAggregateTimeAndCount(ids: Seq[TraceId]): (Long, Int) = {
    var count: Int = 0
    val time = getAggregateTime(
      ids,
      (t: TraceEvent.Event, l: Long) =>
        t match {
          case TraceEvent.TxnRetry =>
            count = count + 1
            l
          case _ => 0L
        })
    (time, count)
  }

  def getChannelIntervals(id: TraceId, ddcId: CacheIdentity): Seq[Interval] = {
    if (enabled.get()) {
      val tr = TracerRegistry.get(id)
      if (tr != null) {
        tr.getTraces(_ match {
          case TraceEvent.ChannelEx(ddcIds) if ddcIds.contains(ddcId) => true
          case TraceEvent.ChannelQ(id) if id == ddcId                 => true
          case _                                                      => false
        }) map { case t @ TraceData(_, startTime, durationMillis, _) => Interval(startTime, t.endTime) }
      } else Seq.empty
    } else Seq.empty
  }

  def getSessionCacheMiss(id: TraceId): Int = {
    if (enabled.get()) {
      val tr = Option(TracerRegistry.get(id))
      tr.map(_.getTraces({
        case TraceEvent.SessionCacheMiss => true
        case _                           => false
      }).size)
        .getOrElse(0)
    } else 0
  }

  def getDHTMiss(id: TraceId): Int = {
    if (enabled.get()) {
      val tr = Option(TracerRegistry.get(id))
      tr.map(_.getTraces({
        case TraceEvent.DHTMiss => true
        case _                  => false
      }).size)
        .getOrElse(0)
    } else 0
  }

  def getWallClockTime(ids: Seq[TraceId], event: TraceEvent.Event): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == event)

  def getWallClockTime(id: TraceId, filter: (TraceEvent.Event) => Boolean): Long = getWallClockTime(Seq(id), filter)
  def getWallClockTime(ids: Seq[TraceId], filter: (TraceEvent.Event) => Boolean): Long = {
    if (enabled.get()) {
      merge(ids flatMap { id =>
        Option(TracerRegistry.get(id)) map { _.getMergedTrace(filter) } getOrElse Nil
      }).foldLeft(0L) { case (r, a) =>
        r + a.durationMillis
      }
    } else 0L
  }

  def getThreadTraces(
      ids: Seq[TraceId],
      prefilter: TraceEvent.Event => Boolean = (_: TraceEvent.Event) => true,
      postfilter: TraceData => Boolean = (_: TraceData) => true): Seq[TraceData] = {
    getTraces(ids, prefilter)
      .filter(postfilter)
      .groupBy(_.threadName)
      .flatMap { case (_, traces) => coalesce(traces) }
      .toSeq
  }

  def getTraces(ids: Seq[TraceId], filterCriteria: TraceEvent.Event => Boolean): Seq[TraceData] = {
    if (enabled.get()) {
      ids.flatMap { id =>
        Option(TracerRegistry.get(id)) map (_.getTraces(filterCriteria)) getOrElse Nil
      }
    } else Seq.empty
  }

  private[trace] def coalesce(traces: Seq[TraceData]): Seq[TraceData] = {
    // This projects a set of potentially overlapping thread traces onto the time axis,
    // thereby generating a set of non-overlapping maximal traces for a single thread
    // (the source trace timing data may not be wallclock-based)
    // We can use the output for calculating accurate thread occupancy and capacity utilization
    // NOTE: this method assumes that "threadName" is the same in every element of the "traces" sequence
    if (traces.nonEmpty) {
      val threadName = traces.head.threadName
      val source = traces.sortBy(_.startTime)
      var currentStart = source.head.startTime
      var currentEnd = source.head.endTime
      val resultBuffer = new mutable.ListBuffer[TraceData]

      def closeCurrentTrace =
        resultBuffer += TraceData(
          TraceEvent.All,
          currentStart,
          Duration.between(currentStart, currentEnd).toMillis,
          threadName)

      source.sliding(2).foreach {
        case Seq(_) =>
        case Seq(prev, next) =>
          if (next.startTime.isAfter(currentEnd)) {
            closeCurrentTrace
            currentStart = next.startTime
            currentEnd = next.endTime
          } else if (next.endTime.isAfter(currentEnd)) {
            currentEnd = next.endTime
          }
      }
      closeCurrentTrace
      resultBuffer.toSeq
    } else Seq.empty
  }

  def getThreadTraceProperties(ids: Seq[TraceId], onlyExecutorThreads: Boolean = true): Seq[Map[String, String]] = {
    def traceTypePreFilter(traceEvent: TraceEvent.Event) = traceEvent match {
      case TraceEvent.Ex | TraceEvent.CacheEx | TraceEvent.ChannelEx(_) | TraceEvent.Snd | TraceEvent.DdcSerialization |
          TraceEvent.DdcSnd | TraceEvent.DdcResponseEx | TraceEvent.LsqtWait | TraceEvent.TxnRetry |
          TraceEvent.ReadDemuxer | TraceEvent.WriteDemuxer | TraceEvent.TxnManagerFinalizeCommit |
          TraceEvent.TxnManagerLogBlobs | TraceEvent.TxnManagerLogMetadata | TraceEvent.TxnManagerRecovery |
          TraceEvent.PublishWrite =>
        true
      case _ => false
    }

    def threadNamePostFilter(trace: TraceData) =
      breadcrumbTraceThreadFilter.get.threadFilterPattern.matcher(trace.threadName).matches

    getThreadTraces(
      ids,
      prefilter = traceTypePreFilter,
      postfilter = if (onlyExecutorThreads) threadNamePostFilter else (_: TraceData) => true).map { td =>
      Map(
        typeProperty -> "threadTrace",
        threadIdProperty -> td.threadName,
        startProperty -> s"${td.startTime}",
        durationProperty -> s"${td.durationMillis}"
      )
    }
  }

  def getChannelWallClock(id: TraceId, ddcId: CacheIdentity): Long = {
    getWallClockTime(
      id,
      _ match {
        case TraceEvent.ChannelEx(ddcIds) if ddcIds.contains(ddcId) => true
        case TraceEvent.ChannelQ(id) if id == ddcId                 => true
        case _                                                      => false
      })
  }

  def getAggregateTime(id: TraceId, event: TraceEvent.Event): Long = {
    getAggregateTime(Seq(id), event)
  }

  def getAggregateTime(ids: Seq[TraceId], event: TraceEvent.Event): Long = {
    if (enabled.get()) {
      val agg = ids flatMap { id =>
        Option(TracerRegistry.get(id))
          .map(_.getAggregateTrace(event))
      }
      if (agg.nonEmpty) agg.sum else 0L
    } else 0L
  }

  def getAggregateTime(id: TraceId, filter: (TraceEvent.Event) => Boolean): Long =
    getAggregateTime(Seq(id), filter)

  def getAggregateTime(ids: Seq[TraceId], filter: (TraceEvent.Event) => Boolean): Long = {
    getAggregateTime(ids, (event: TraceEvent.Event, time: Long) => if (filter(event)) time else 0L)
  }

  def getAggregateTime(id: TraceId, f: (TraceEvent.Event, Long) => Long): Long =
    getAggregateTime(Seq(id), f)

  def getAggregateTime(ids: Seq[TraceId], f: (TraceEvent.Event, Long) => Long): Long =
    getAggregate[Long](ids, 0L, (a: Long, event: TraceEvent.Event, time: Long) => a + f(event, time))

  def getAggregate[T](id: TraceId, default: T, op: (T, TraceEvent.Event, Long) => T): T =
    getAggregate(Seq(id), default, op)

  def getAggregate[T](ids: Seq[TraceId], default: T, op: (T, TraceEvent.Event, Long) => T): T = {
    if (enabled.get()) {
      val traces = ids flatMap { id =>
        Option(TracerRegistry.get(id)) map { _.getTraces() } getOrElse Nil
      }
      traces.foldLeft(default) { (a, t) =>
        op(a, t.event, t.durationMillis)
      }
    } else default
  }

  def getCacheAggregateStatus(id: TraceId, f: BackEnd => Boolean): (Long, Long, Long) = {
    getCacheAggregateStatus(Seq(id), f)
  }

  def getCacheAggregateStatus(ids: Seq[TraceId], f: BackEnd => Boolean): (Long, Long, Long) = {
    getAggregate[(Long, Long, Long)](
      ids,
      (0L, 0L, 0L),
      (a: (Long, Long, Long), event: TraceEvent.Event, _: Long) =>
        event match {
          case TraceEvent.CacheRead(_, hitCount, missCount, computeHitCount, backEnd) if f(backEnd) =>
            (a._1 + hitCount, a._2 + missCount, a._3 + computeHitCount)
          case _ => a
        }
    )
  }

  def getExAggregateTime(ids: Seq[TraceId]): Long = getAggregateTime(ids, TraceEvent.Ex)
  def getExWallclockTime(ids: Seq[TraceId]): Long = getWallClockTime(ids, TraceEvent.Ex)
  def getQAggregateTime(ids: Seq[TraceId]): Long = getAggregateTime(ids, TraceEvent.Q)
  def getQWallclockTime(id: TraceId): Long = getQWallclockTime(Seq(id))
  def getQWallclockTime(ids: Seq[TraceId]): Long = getWallClockTime(ids, TraceEvent.Q)
  def getSndAggregateTime(id: TraceId): Long = getSndAggregateTime(Seq(id))
  def getSndAggregateTime(ids: Seq[TraceId]): Long = getAggregateTime(ids, TraceEvent.Snd)
  def getAllAggregateTime(id: TraceId): Long = getAllAggregateTime(Seq(id))
  def getAllAggregateTime(ids: Seq[TraceId]): Long =
    getAggregateTime(ids, TraceEvent.All) + getEntitlementLoadAggregateTime(ids)
  def getAllWallclockTime(ids: Seq[TraceId]): Long = {
    getWallClockTime(ids, TraceEvent.All)
  }
  def getGpbAggregateTime(id: TraceId): Long = getGpbAggregateTime(Seq(id))
  def getGpbAggregateTime(ids: Seq[TraceId]): Long = getAggregateTime(ids, TraceEvent.GpbSerialization)
  def getBackEndSerializationTime(id: TraceId): Long = getBackEndSerializationTime(Seq(id))
  def getBackEndSerializationTime(ids: Seq[TraceId]): Long = getAggregateTime(ids, TraceEvent.BackEndSerialization)
  def getPayloadAggregateTime(id: TraceId): Long = getPayloadAggregateTime(Seq(id))
  def getPayloadAggregateTime(ids: Seq[TraceId]): Long = getAggregateTime(ids, TraceEvent.PayloadAggregate)
  def getPayloadWallclockTime(id: TraceId): Long = getPayloadWallclockTime(Seq(id))
  def getPayloadWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.PayloadAggregate)
  def getGpbSize(id: TraceId): Long = getEventAggregateData(id, TraceEvent.GpbSize)
  def getGpbSize(ids: Seq[TraceId]): Long =
    ids.map { id =>
      getEventAggregateData(id, TraceEvent.GpbSize)
    }.sum

  def getIgnoreListCount(ids: Seq[TraceId]): Long =
    ids.map { id =>
      getEventAggregateData(id, TraceEvent.ReferenceResolverIgnoreListCount)
    }.sum
  def getArchiveReadStats(ids: Seq[TraceId]): Map[SerializedEntity.TypeRef, Long] = {
    val gpfsStats =
      ids.flatMap { id =>
        getAllEventData[Map[SerializedEntity.TypeRef, Long]](id, TraceEvent.GpfsRead).map(_.toSeq)
      }.flatten
    val s3Stats =
      ids.flatMap { id =>
        getAllEventData[Map[SerializedEntity.TypeRef, Long]](id, TraceEvent.S3Read).map(_.toSeq)
      }.flatten
    (gpfsStats ++ s3Stats)
      .groupBy(_._1)
      .mapValuesNow(_.map(_._2).sum)
  }

  def getPrcEnrichWithPayloadWallclockTime(id: TraceId): Long =
    getPrcEnrichWithPayloadWallclockTime(Seq(id))
  def getPrcEnrichWithPayloadWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.PrcEnrichWithPayload)
  def getPrcPayloadFetchWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.PrcPayloadFetch)
  def getPrcPayloadPostProcessingWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.PrcPayloadPostProcessing)
  def getPrcEntitlementCheckCmdWithResultWallclockTime(id: TraceId): Long =
    getPrcEntitlementCheckCmdWithResultWallclockTime(Seq(id))
  def getPrcEntitlementCheckCmdWithResultWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.PrcEntitlementCheckCmdWithResult)
  def getPrcEntitlementPostCheckWallclockTime(id: TraceId): Long =
    getPrcEntitlementPostCheckWallclockTime(Seq(id))
  def getPrcEntitlementPostCheckWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.PrcEntitlementPostCheck)
  def getPrcEntitlementPreCheckWallclockTime(id: TraceId): Long =
    getPrcEntitlementPreCheckWallclockTime(Seq(id))
  def getPrcEntitlementPreCheckWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.PrcEntitlementPreCheck)
  def getPrcLocalSpaceRetrievalWallclockTime(id: TraceId): Long =
    getPrcLocalSpaceRetrievalWallclockTime(Seq(id))
  def getPrcLocalSpaceRetrievalWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.PrcLocalSpaceRetrieval)
  def getPrcLockReadLockWallclockTime(id: TraceId): Long =
    getPrcLockReadLockWallclockTime(Seq(id))
  def getPrcLockReadLockWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.PrcLockReadLock)
  def getPrcLockWriteLockWallclockTime(id: TraceId): Long =
    getPrcLockWriteLockWallclockTime(Seq(id))
  def getPrcLockWriteLockWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.PrcLockWriteLock)
  def getPrcLsqtWaitWallclockTime(id: TraceId): Long =
    getPrcLsqtWaitWallclockTime(Seq(id))
  def getPrcLsqtWaitWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.PrcLsqtWait)
  def getPrcPutWallclockTime(id: TraceId): Long =
    getPrcPutWallclockTime(Seq(id))
  def getPrcPutWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.PrcPut)
  def getPrcRemoteSpaceRetrievalWallclockTime(id: TraceId): Long =
    getPrcRemoteSpaceRetrievalWallclockTime(Seq(id))
  def getPrcRemoteSpaceRetrievalWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.PrcRemoteSpaceRetrieval)
  def getPrcRetrievalIdxProcessingWallclockTime(id: TraceId): Long =
    getPrcRetrievalIdxProcessingWallclockTime(Seq(id))
  def getPrcRetrievalIdxProcessingWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.PrcRetrievalIndexProcessing)
  def getPrcSessionEstablishmentWallclockTime(id: TraceId): Long =
    getPrcSessionEstablishmentWallclockTime(Seq(id))
  def getPrcSessionEstablishmentWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.PrcSessionEstablishment)
  def getPrcSKMarkKeyWallclockTime(id: TraceId): Long =
    getPrcSKMarkKeyWallclockTime(Seq(id))
  def getPrcSKMarkKeyWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.PrcSKMarkKey)
  def getPrcSKSpaceRetrievalWallclockTime(id: TraceId): Long =
    getPrcSKSpaceRetrievalWallclockTime(Seq(id))
  def getPrcSKSpaceRetrievalWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.PrcSKSpaceRetrieval)
  def getPrcTriggerUserOptsDeserdeWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.PrcTriggerUserOptsDeserde)
  def getPrcRemoteSpaceProcessingWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.PrcRemoteSpaceProcessing)
  def getPrcSerdeResultWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.PrcSerdeResult)
  def getSKReceiveGQWallclockTime(id: TraceId): Long =
    getSKReceiveGQWallclockTime(Seq(id))
  def getSKReceiveGQWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.SkReceiveGlobalQ)
  def getSKReceiveQWallclockTime(id: TraceId): Long =
    getSKReceiveQWallclockTime(Seq(id))
  def getSKReceiveQWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.SkReceiveQ)
  def getSKStorageModuleRetrievalWallclockTime(id: TraceId): Long =
    getSKStorageModuleRetrievalWallclockTime(Seq(id))
  def getSKStorageModuleRetrievalWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.SkStorageModuleRetrieval)
  def getSKTriggerRetrievalWallclockTime(id: TraceId): Long =
    getSKTriggerRetrievalWallclockTime(Seq(id))
  def getSKTriggerRetrievalWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.SkTriggerRetrieval)
  def getSKResultQWallclockTime(id: TraceId): Long =
    getSKResultQWallclockTime(Seq(id))
  def getSKResultQWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.SkResultQ)
  def getSKSendResultWallclockTime(id: TraceId): Long =
    getSKSendResultWallclockTime(Seq(id))
  def getSKSendResultWallclockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t == TraceEvent.SkSendResult)
  def getConflictCheckWallClockTime(ids: Seq[TraceId]): Long =
    getWallClockTime(ids, (t: TraceEvent.Event) => t.isInstanceOf[TraceEvent.LockManagerConflictCheck])

  def getBackendHosts(id: TraceId): Set[Host] = getBackendHosts(Seq(id))
  def getBackendHosts(ids: Seq[TraceId]): Set[Host] = {
    if (enabled.get()) {
      val traces = ids flatMap { id =>
        Option(TracerRegistry.get(id)) map { _.getTraces() } getOrElse Nil
      }
      traces map { _.event } flatMap {
        case t: TraceEvent.Event with TraceEvent.HasHost => t.host
        case _                                           => None
      } toSet
    } else Set.empty
  }

  def getTxnActionsMetrics(ids: Seq[TraceId]): Map[String, String] = {
    val traces: Seq[TraceData] = ids flatMap { id =>
      Option(TracerRegistry.get(id)) map { _.getTraces() } getOrElse Nil
    }

    ((traces map { _.event } collect { case e: TraceEvent.MongoPrep => e.info })
      .foldLeft(Map.empty[String, Long]) { (acc, map) =>
        map.foldLeft(acc) { case (iAcc, (k, v)) =>
          iAcc.updated(k, iAcc.getOrElse(k, 0L) + v)
        }
      })
      .mapValuesNow(_.toString)
  }

  def getWindowDelay(ids: Seq[TraceId]): Int = {
    val traces: Seq[TraceData] = ids flatMap { id =>
      Option(TracerRegistry.get(id)) map { _.getTraces() } getOrElse Nil
    }

    (traces map { _.event } collect { case e: TraceEvent.WindowDelay => e.delay }).sum
  }

  private def cnfEventMatcher(event: TraceEvent.Event): Int = {
    event match {
      case TraceEvent.LockManagerConflictCheck(cnt) => cnt
      case _                                        => 0
    }
  }
  def getConflictCheckCount(allTraceIds: Seq[TraceId]): Long =
    Tracer.getAggregate[Int](allTraceIds, 0, (a: Int, event: TraceEvent.Event, _: Long) => a + cnfEventMatcher(event))

  def getTracerRegistrySize(): Int = {
    TracerRegistry.size()
  }

  def dumpAllTraces(): String = {
    TracerRegistry.dump()
  }

  def getCursorOpenHosts(ids: Seq[TraceId]): Set[String] = {
    if (enabled.get()) {
      val traces = ids flatMap { id =>
        Option(TracerRegistry.get(id)) map { _.getTraces() } getOrElse Nil
      }
      traces.collect { case TraceData(CursorOpen(_, host), _, _, _) => host.toString }.toSet
    } else Set.empty
  }

  def getReadSessionCreationHosts(ids: Seq[TraceId]): Set[String] = {
    if (enabled.get()) {
      val traces = ids flatMap { id =>
        Option(TracerRegistry.get(id)) map { _.getTraces() } getOrElse Nil
      }
      traces
        .collect { case TraceData(ReadSessionCreation(hosts), _, _, _) => hosts.map(_.toString) }
        .toSet
        .flatten
    } else Set.empty
  }

  // For testing purpose.
  private[optimus] def removeTrace(id: TraceId): Unit = runSafe { TracerRegistry.remove(id) }

  /**
   * Clears the tracer registry
   */
  def clearTracerRegistry(): Unit = {
    TracerRegistry.clear()
  }

  def enable(): Unit = {
    enabled.getAndSet(true)
  }

  def disable(): Unit = {
    enabled.set(false)
  }

  /*
   * used to change the log threshold via JMX
   */
  def setLogThreshold(t: Int): Integer = {
    logThreshold.getAndSet(t)
  }

  def getLogThreshold(): Integer = {
    logThreshold.get()
  }

  def setBreadcrumbThreshold(t: Int): Integer = {
    breadcrumbThreshold.getAndSet(t)
  }

  def getBreadcrumbThreshold(): Integer = {
    breadcrumbThreshold.get()
  }

  def setBreadcrumbRelevanceThresholdMultiplier(t: Double): Double = {
    breadcrumbRelevanceThresholdPower.getAndSet(t)
  }

  def getBreadcrumbRelevanceThresholdMultiplier(): Double = {
    breadcrumbRelevanceThresholdPower.get()
  }

  def setBreadcrumbTraceThreadFilter(f: String): String = {
    breadcrumbTraceThreadFilter.getAndSet(BreadcrumbTraceFilterConfig(f)).threadFilter
  }

  def getBreadcrumbTraceThreadFilter(): String = {
    breadcrumbTraceThreadFilter.get.threadFilter
  }

  def getCurrentState(id: TraceId): Option[RequestState] = {
    Option(TracerRegistry.get(id)) map { tr =>
      tr.inferCurrentStateFromTraces()
    }
  }

  /**
   * Do not call this method directly. It should only be called from the macro above.
   */
  private[optimus] def doBatchRecordTrace[A](
      ids: Set[TraceId],
      event: TraceEvent.Event,
      startTime: Instant,
      res: A): A = {
    runSafe {
      ids foreach (doRecordTrace(_, event, startTime, res))
    }
    res
  }

  private[optimus] def doRecordEventsBatchTrace[A](
      events: Seq[(TraceId, TraceEvent.Event)],
      startTime: Instant,
      res: A): A = {
    runSafe {
      val duration = patch.MilliInstant.now().toEpochMilli - startTime.toEpochMilli
      events foreach { case (traceId, event) => recordEventDuration(traceId, event, startTime, duration) }
    }
    res
  }

  /**
   * Do not call this method directly. It should only be called from the macro above.
   */
  private[optimus] def doRecordTrace[A](id: TraceId, event: TraceEvent.Event, startTime: Instant, res: A): A = {
    if (enabled.get()) {
      runSafe {
        val tr = TracerRegistry.get(id)
        if (tr != null) {
          val d = patch.MilliInstant.now().toEpochMilli - startTime.toEpochMilli()
          tr.recordTrace(event, startTime, d)
        }
      }
    }
    res
  }

  private def runSafe[A](f: => A): Unit = {
    try {
      f
    } catch {
      // catch all non-fatal throwables as we do not want tracing error to affect broker's functioning
      // if needed, on seeing the Tracing error logs, the production mgmt team can bounce the broker
      case NonFatal(e) => log.error("Tracing error", e)
    }
  }

  object RequestTracer {
    private[trace] final case class TraceData(
        event: TraceEvent.Event,
        startTime: Instant,
        durationMillis: Long,
        threadName: String) {
      lazy val endTime: Instant = startTime.plusMillis(durationMillis)
      private lazy val startTimeUtc = ZonedDateTime.ofInstant(startTime, ZoneIds.UTC).toOffsetDateTime()
      lazy val values = event.values ++ Map[String, String](
        "startTimeUtc" -> startTimeUtc.toString,
        "durationMillis" -> durationMillis.toString)
      override def toString(): String = {
        "\"event\": \"" + event
          .toString() + "\", \"startTimeUtc\": \"" + startTimeUtc.toString + "\", \"durationMillis\": " + durationMillis
      }
      def level = event.level
    }

    private[trace] final case class TraceCustomData[T](event: TraceEvent.Event, data: T)
  }
  private class RequestTracer {
    import RequestTracer._

    private val inFlightTraces = new ConcurrentHashMap[TraceEvent.Event, TraceData]

    private val storedTraces = mutable.ListBuffer.empty[TraceData]
    private val storedCustomDataTraces = mutable.ListBuffer.empty[TraceCustomData[_]]
    private val storedTracesRwLock = new ReentrantReadWriteLock
    private val storedTracesReadLock = storedTracesRwLock.readLock()
    private val storedTracesWriteLock = storedTracesRwLock.writeLock()

    private[trace] val aggregateTraces: AtomicLongMap[TraceEvent.Event] =
      AtomicLongMap.create(new ConcurrentHashMap[TraceEvent.Event, java.lang.Long](32))

    private def withStoredTracesReadLock[T](f: => T): T = {
      try {
        storedTracesReadLock.lock()
        f
      } finally {
        storedTracesReadLock.unlock()
      }
    }

    private def withStoredTracesWriteLock[T](f: => T): T = {
      try {
        storedTracesWriteLock.lock()
        f
      } finally {
        storedTracesWriteLock.unlock()
      }
    }

    // Testing hook
    private[trace] def getStoredTracesSize: Int = withStoredTracesReadLock { storedTraces.size }

    def getAggregateTrace(event: TraceEvent.Event): Long = aggregateTraces.get(event)

    def getMergedTrace(f: TraceEvent.Event => Boolean): Seq[Interval] = {
      val traces = withStoredTracesReadLock {
        storedTraces.toList.filter(data => f(data.event))
      }
      merge(traces map { case t @ TraceData(_, startTime, _, _) =>
        Interval(startTime, t.endTime)
      })
    }
    def getMergedTraceTime(filter: TraceEvent.Event => Boolean): Long = {
      getMergedTrace(filter).foldLeft(0L) { case (r, a) =>
        r + a.durationMillis
      }
    }

    def startTrace(event: TraceEvent.Event) = {
      val threadName = Thread.currentThread().getName
      inFlightTraces.put(event, TraceData(event, patch.MilliInstant.now(), -1, threadName))
    }

    def endTrace(event: TraceEvent.Event) = {
      val tr = inFlightTraces.get(event)
      if (tr != null && tr.durationMillis == -1) {
        val d = patch.MilliInstant.now().toEpochMilli() - tr.startTime.toEpochMilli()
        doRecordTrace(event, tr.startTime, d, tr.threadName)
      }
    }

    def inflightTraceStartTime(event: TraceEvent.Event): Option[Long] = {
      Option(inFlightTraces.get(event))
        .filter(_.durationMillis == -1)
        .map(_.startTime.toEpochMilli)
    }

    private[trace] def doRecordTrace(
        event: TraceEvent.Event,
        startTime: Instant,
        durationMillis: Long,
        threadName: String): Unit = {
      aggregateTraces.addAndGet(event, durationMillis)
      event match {
        case TraceEvent.GpbSerialization | TraceEvent.BackEndSerialization =>
        case _ =>
          withStoredTracesWriteLock {
            storedTraces += TraceData(event, startTime, durationMillis, threadName)
          }
      }
    }

    def recordTrace(event: TraceEvent.Event, startTime: Instant, durationMillis: Long) = {
      doRecordTrace(event, startTime, durationMillis, Thread.currentThread().getName)
    }

    def clear(): Unit = withStoredTracesReadLock {
      inFlightTraces.clear()
      storedTraces.clear()
      aggregateTraces.clear()
      storedCustomDataTraces.clear()
    }

    def getTraces(): Seq[TraceData] = withStoredTracesReadLock {
      val st = storedTraces
      st.result()
    }

    def getTraces(filter: TraceEvent.Event => Boolean): Seq[TraceData] = withStoredTracesReadLock {
      storedTraces.toList filter { t =>
        filter(t.event)
      }
    }

    // note that this will return only one mapping per trace event type, the longest one captured with the stored traces
    private[trace] def getLongestTraceDurations(
        filter: TraceEvent.Event => Boolean = _ => true): Map[TraceEvent.Event, Long] = {
      val buffer = mutable.Map.empty[TraceEvent.Event, Long]
      withStoredTracesReadLock {
        storedTraces filter { trace =>
          filter(trace.event)
        } foreach { trace =>
          buffer.get(trace.event) map { durationMillis =>
            if (trace.durationMillis > durationMillis) buffer.put(trace.event, trace.durationMillis)
          } orElse { buffer.put(trace.event, trace.durationMillis) }
        }
      }
      buffer.toMap
    }

    override def toString: String = withStoredTracesReadLock {
      val s = storedTraces map { t =>
        s"{$t}"
      } mkString ", "
      s
    }

    def inferCurrentStateFromTraces(): RequestState = {
      // make a local copy of traces so that the following code doesn't get affected by the changes to the traces
      val trackingAwareInflightTraces = inFlightTraces.values().asScala.toSeq
      val trackingAwareStoredTraces = withStoredTracesReadLock { storedTraces.result() }
      /*
       * If inflight traces are empty,
       *  return the State as just received.
       * else
       *  find the last started inflight trace, get its phase information,
       *  check for any stored traces with same layer info,
       *  if found,
       *    return the event from stored traces
       *  else
       *    return the last started inflight event
       */
      trackingAwareInflightTraces.sortBy(_.startTime).lastOption map { lastInflightTrace =>
        val relevantEvent = trackingAwareStoredTraces
          .filter(_.event.phase == lastInflightTrace.event.phase)
          .sortBy(_.startTime)
          .lastOption
          .getOrElse(lastInflightTrace)
          .event
        RequestState(relevantEvent.phase, relevantEvent, lastInflightTrace.threadName)
      } getOrElse {
        RequestState(Tracing.Received, TraceEvent.DsiRequest, "")
      }
    }

    def recordTrace[T](event: TraceEvent.Event, data: T): Unit = withStoredTracesWriteLock {
      storedCustomDataTraces += TraceCustomData(event, data)
    }

    def getCustomDataTraces[T](filter: TraceEvent.Event => Boolean): Seq[TraceCustomData[T]] =
      withStoredTracesReadLock {
        storedCustomDataTraces.toList.filter(t => filter(t.event)).map(_.asInstanceOf[TraceCustomData[T]])
      }
  }

  private object TracerRegistry {
    private val tracerMap = new ConcurrentHashMap[TraceId, RequestTracer]

    def create(name: TraceId) = {
      val ntr = new RequestTracer()
      tracerMap.put(name, ntr)
      ntr
    }

    def get(name: TraceId) = {
      tracerMap.get(name)
    }

    def remove(name: TraceId): Unit = {
      tracerMap.remove(name)
    }

    def size() = {
      tracerMap.size()
    }

    def clear(): Unit = {
      tracerMap.clear()
    }

    def dump(): String = {
      tracerMap.asScala.map { case (id, tracer) =>
        s"$id -> ${tracer.toString}"
      } mkString " # "
    }
  }
}

class TracerMacros(val c: Context) {
  import Tracer._
  import c.universe._

  def recordEventImpl[A: c.WeakTypeTag](id: c.Expr[TraceId], event: c.Expr[TraceEvent.Event])(
      f: c.Expr[A]): c.Expr[A] = {

    reify {
      val st = patch.MilliInstant.now()
      // We can't use f.splice directly because it can be re-ordered to run
      // after doRecordTrace, so the recorded time would be bogus - almost
      // certainly 0. So we pass result of f.splice to doRecordTrace to inhibit reordering.
      doRecordTrace(id.splice, event.splice, st, f.splice)
    }
  }

  def recordBatchEventImpl[A: c.WeakTypeTag](ids: c.Expr[Set[TraceId]], event: c.Expr[TraceEvent.Event])(
      f: c.Expr[A]): c.Expr[A] = {
    reify {
      val st = patch.MilliInstant.now()
      // We can't use f.splice directly because it can be re-ordered to run
      // after doRecordTrace, so the recorded time would be bogus - almost
      // certainly 0. So we pass result of f.splice to doBatchRecordTrace to inhibit reordering.
      doBatchRecordTrace(ids.splice, event.splice, st, f.splice)
    }
  }

  def recordEventsBatchImpl[A: c.WeakTypeTag](events: c.Expr[Seq[(TraceId, TraceEvent.Event)]])(
      f: c.Expr[A]): c.Expr[A] = {
    reify {
      val st = patch.MilliInstant.now()
      // We can't use f.splice directly because it can be re-ordered to run
      // after doRecordTrace, so the recorded time would be bogus - almost
      // certainly 0. So we pass result of f.splice to doBatchRecordTrace to inhibit reordering.
      doRecordEventsBatchTrace(events.splice, st, f.splice)
    }
  }

}
