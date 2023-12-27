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
package optimus.breadcrumbs.crumbs

import java.time.ZonedDateTime
import java.{util => ju}

import spray.json._
import Properties.JsonImplicits._
import optimus.breadcrumbs.crumbs.Properties.Key
import spray.json.JsValue

import scala.jdk.CollectionConverters._
import scala.collection.immutable

// Base property type for events
private[breadcrumbs] trait Event extends Key[ZonedDateTime] {
  override def source = "optimus"
  override def parse(js: JsValue): ZonedDateTime = js.convertTo[ZonedDateTime]
  override def toJson(t: ZonedDateTime): JsValue = t.toJson
  override def equals(obj: Any): Boolean = obj match {
    case ek: Event => ek.name == this.name && ek.source == this.source
    case _         => false
  }
  override def hashCode: Int = name.hashCode ^ source.hashCode
  val name: String
  override def toString: String = name
}

final case class SourcedEvent(override val source: String, override val name: String) extends Event {
  override def toString = s"$source:$name"
}

abstract class KnownEvents private[crumbs] extends Enumeration {
  def event(name: String): Events.EventVal = new EventValImpl(name)
  private[this] class EventValImpl(override val name: String) extends Val(nextId, name) with Events.EventVal {
    override val source = "optimus"
  }

  private[crumbs] def set = values.asInstanceOf[Set[Events.EventVal]]
}

object KnownEvents {
  abstract class Set private[crumbs] (val kps: KnownEvents*)
  private[crumbs] lazy val allKnownEvents: immutable.Set[Events.EventVal] = {
    Events.values.iterator.map(_.asInstanceOf[Events.EventVal]).toSet | {
      ju.ServiceLoader.load(classOf[Set]).asScala.flatMap(_.kps).toSet.flatMap((_: KnownEvents).set)
    }
  }
}

//noinspection TypeAnnotation
// "Official" events
object Events extends KnownEvents {
  sealed trait EventVal extends Event { this: KnownEvents#Value =>
    val name: String
  }

  def parseEvent(name: String, source: Option[String] = None): Event =
    // First try canonical enumeration
    values
      .find(_.toString == name)
      .map(_.asInstanceOf[EventVal])
      .orElse {
        // Or if source is specified, just make a SourcedEvent
        source.map(SourcedEvent(_, name))
      }
      .getOrElse {
        name.split(":").toList match {
          case Nil                   => SourcedEvent("unknown", "unknown")
          case name :: Nil           => SourcedEvent("optimus", name)
          case source :: name +: Nil => SourcedEvent(source, name)
          case source :: rest        => SourcedEvent(source, rest.mkString("e"))
        }
      }

  def apply(name: String, source: Option[String] = None): Event = parseEvent(name, source)

  val InternalTestEvent = event("InternalTestEvent")

  val ClientLog = event("ClientLog")

  val Completed = event("Completed")
  val AppStarted = event("AppStarted")
  val AppCompleted = event("AppCompleted")
  val RuntimeCreated = event("RuntimeCreated")
  val RuntimeShutDown = event("RuntimeShutDown")

  val AppStartPerf = event("AppStartPerf")
  val AppRestorePerf = event("AppRestorePerf")
  val AppEventPerf = event("AppEventPerf")
  val AppClientResourceUsage = event("AppClientResourceUsage")
  val AppClientStartPref = event("AppClientStartPref")

  val GraphStalling = event("GraphStalling")
  val GraphStalled = event("GraphStalled")
  val StallDetected = event("StallDetected")

  val DistSend = event("DistSend")
  val DistReceived = event("DistReceived")
  val DistReturn = event("DistReturn")
  val DistCollected = event("CDist")
  val OptimusAppCollected = event("COptimus")

  val DMCRecovery = event("DMCRecovery")
  val DMCCacheHit = event("DMCCacheHit")
  val DMCCacheMiss = event("DMCCacheMiss")
  val DMCFailure = event("DMCFailure")
  val DMCLocalCompute = event("DMCLocalCompute")
  val DMCGridCompute = event("DMCGridCompute")
  val DMCSuccess = event("DMCSuccess")
  val DMCCancel = event("DMCCancel")

  val DalServerReqRcvd = event("DalServerReqRcvd")
  val DalServerReqHndld = event("DalServerReqHndld")
  val DalServerLRR = event("DalServerLRR")
  val DalServerGC = event("DalServerGC")
  val DalServerParBrkrCacheHit = event("DalServerParBrkrCacheHit")
  val DalServerFullBrkrCacheHit = event("DalServerFullBrkrCacheHit")
  val DalServerExecInDDC = event("DalServerExecInDDC")
  val DalServerExecLocally = event("DalServerExecLocally")
  val DalServerDdcReq = event("DalServerDdcReq")
  val DalServerDdcRes = event("DalServerDdcRes")
  val DalServerPrcReq = event("DalServerPrcReq")
  val DalServerChanReq = event("DalServerChanReq")
  val DalServerChanRes = event("DalServerChanRes")
  val DalServerZnConfig = event("DalServerZnConfig")
  val DalServerZnLmtBrch = event("DalServerZnLmtBrch")
  val DalServerWriteVisited = event("DalServerWriteVisited")
  val DalServerTxRangeViolation = event("DalServerTxRangeThresholdViolation")

  val DalWorkMgrRecoveredEntry = event("DalWorkMgrRecoveredEntry")

  val DalSatReqRecvd = event("DalSatReqRecvd")
  val DalSatReqHndld = event("DalSatReqHndld")

  val DalClientReqSent = event("DalClientReqSent")
  val DalClientRespRcvd = event("DalClientRespRcvd")
  val DalClientReqTimedOut = event("DalClientReqTimedOut")

  val ProfilingMetricsStarted = event("ProfilingMetricsStarted")
  val ProfilingMetricsCollected = event("ProfilingMetricsCollected")

  val TemporalSurfaceTrace = event("TemporalSurfaceTrace")

  val PublicationQueueDepth = event("PublicationQueueDepth")
  val DalBanWriteEntity = event("DalBanWriteEntity")

}
