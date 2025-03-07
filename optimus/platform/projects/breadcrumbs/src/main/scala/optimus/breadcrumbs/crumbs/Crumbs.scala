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

import optimus.breadcrumbs.ChainedID

import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import java.net.InetAddress
import java.time.Instant
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import optimus.breadcrumbs.Breadcrumbs
import org.slf4j.LoggerFactory
import spray.json._
import DefaultJsonProtocol._
import optimus.breadcrumbs.BreadcrumbsPublisher
import optimus.breadcrumbs.crumbs
import optimus.breadcrumbs.crumbs.Crumb.CrumbFlag
import optimus.breadcrumbs.crumbs.Crumb.Headers
import optimus.breadcrumbs.crumbs.Crumb.OptimusSource
import optimus.breadcrumbs.crumbs.Properties.Elem
import optimus.breadcrumbs.crumbs.Properties.Elems
import optimus.logging.LoggingInfo
import optimus.utils.datetime.ZoneIds

import scala.annotation.varargs
import scala.collection.immutable.HashMap
import optimus.scalacompat.collection._

import java.util.Objects
import java.util.concurrent.atomic.AtomicInteger

// Keep explicit serial version IDs to coordinate reading from WT.
// This is an argument for converting this entire hierarchy to straight string maps.
@SerialVersionUID(2020060801L)
sealed abstract class Crumb(
    val uuid: ChainedID,
    val source: Crumb.Source = Crumb.OptimusSource,
    val hints: Set[CrumbHint] = Set.empty[CrumbHint])
    extends Serializable {
  import Crumb._
  val t: Long = System.currentTimeMillis()
  def zdt: ZonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(t), ZoneIds.UTC)
  val now: String = t.toString
  val locator: String = Crumb.newLocator()
  def prettyNow: String = Crumb.prettyNow(t)
  private[breadcrumbs] def clazz: String = this.getClass.getName match {
    case Crumb.extractClass(c) => c
    case c                     => c
  }
  protected def baseString = s"$clazz($prettyNow, $uuid"
  protected def J[T: JsonWriter: JsonFormat](k: String, v: T): (String, JsValue) = (k, v.toJson)
  protected def J[T: JsonWriter: JsonFormat](kv: (String, T)): (String, JsValue) = (kv._1, kv._2.toJson)
  private[breadcrumbs] final val hintString: String = hints.toSeq.sorted.mkString(",")
  def flags = source.flags
  final val header =
    Map(
      Headers.Crumb -> clazz,
      Headers.Source -> source.toString,
      Headers.Uuid -> uuid.toString,
      Headers.VertexId -> uuid.vertexId,
      Headers.Time -> t.toString,
      Headers.CrumbLocator -> locator
    )
  final protected def cleanProperties: Map[String, String] =
    (stringProperties.mapValuesNow { v =>
      if (v eq null) "<null>" else v
    } ++ jsonProperties.mapValuesNow(_.toString)).toMap
  final def asMap: Map[String, String] = header ++ cleanProperties ++ Map[String, String](Headers.Hints -> hintString)
  def asJMap: Map[String, JsValue] = {
    val builder = HashMap.newBuilder[String, JsValue]
    builder += J(Headers.Crumb, clazz.toJson)
    builder += J(Headers.Source, source.toString)
    builder += J(Headers.Uuid, uuid.toString)
    builder += J(Headers.CrumbLocator, locator)
    builder += J(Headers.VertexId, uuid.vertexId)
    builder += J(Headers.Time, t)
    if (hints.nonEmpty) {
      builder += J(Headers.Hints, hintString)
    }

    builder ++= stringProperties.mapValuesNow(_.toJson)
    builder ++= jsonProperties

    builder.result()
  }

  private[breadcrumbs] def debugString = s"$clazz($prettyNow, $uuid, ${asJMap}"

  private[breadcrumbs] def stringProperties: Map[String, String]
  private[breadcrumbs] def jsonProperties: Map[String, JsValue] = Map.empty[String, JsValue]

  // Making this a lazy val to avoid multiple conversions, but this means the caller must be sure by the time this
  // is called, the Crumb is at its final shape to be sent, i.e., no more property fields would be added.
  private[optimus] final lazy val asJSON: JsValue = asJMap.toJson

  private[breadcrumbs] def withProperties(elems: Elem[_]*) = new WithProperties(this, Elems(elems: _*))
}

private[breadcrumbs] class WithSource(crumb: Crumb, source: Crumb.Source)
    extends Crumb(crumb.uuid, source, crumb.hints) {
  private[breadcrumbs] override def clazz = crumb.clazz
  override val t: Long = crumb.t
  private[breadcrumbs] override def stringProperties: Map[String, String] = crumb.stringProperties
  private[breadcrumbs] override def jsonProperties: Map[String, JsValue] = crumb.jsonProperties
}

private[breadcrumbs] class WithReplicaFrom(uuid: ChainedID, crumb: Crumb)
    extends Crumb(uuid, crumb.source, crumb.hints) {
  private[breadcrumbs] override def clazz = crumb.clazz
  override val t: Long = crumb.t
  private[breadcrumbs] override def stringProperties: Map[String, String] = crumb.stringProperties
  private[breadcrumbs] override def jsonProperties: Map[String, JsValue] =
    crumb.jsonProperties + (Properties.replicaFrom -> crumb.uuid).toTuple
}

private[breadcrumbs] class WithCurrentlyRunning(crumb: Crumb, uuids: Seq[ChainedID])
    extends Crumb(crumb.uuid, crumb.source, crumb.hints) {
  private[breadcrumbs] override def clazz = crumb.clazz
  override val t: Long = crumb.t
  private[breadcrumbs] override def stringProperties: Map[String, String] = crumb.stringProperties
  private[breadcrumbs] override def jsonProperties: Map[String, JsValue] =
    crumb.jsonProperties + (Properties.currentlyRunning -> uuids).toTuple
}

private[breadcrumbs] class WithProperties(crumb: Crumb, elems: Elems)
    extends Crumb(crumb.uuid, crumb.source, crumb.hints) {
  private[breadcrumbs] override def clazz = crumb.clazz
  override val t: Long = crumb.t
  private[breadcrumbs] override def stringProperties: Map[String, String] = crumb.stringProperties
  private[breadcrumbs] override def jsonProperties: Map[String, JsValue] =
    crumb.jsonProperties ++ elems.toMap
}

private[breadcrumbs] final case class FlushMarker(close: Boolean = false) extends Crumb(ChainedID.root) {
  val latch = new CountDownLatch(1)
  def await(to: Long): Boolean = latch.await(to, TimeUnit.MILLISECONDS)
  def flushed(): Unit = latch.countDown()
  private[breadcrumbs] override def stringProperties: Map[String, String] = Map.empty
}

object CrumbNodeType extends Enumeration {
  type CrumbNodeType = Value
  val GenericNode, NullNode, UserAction, CalculationUnit, DataRequest, DistributedCalculation, DMCCalculation, Trace,
      JobNode =
    Value
}

class NameCrumb(uuid: ChainedID, source: Crumb.Source, val name: String, val tpe: CrumbNodeType.CrumbNodeType)
    extends Crumb(uuid, source) {
  def this(uuid: ChainedID, name: String, tpe: CrumbNodeType.CrumbNodeType) = this(uuid, Crumb.OptimusSource, name, tpe)
  // Equality ignores time
  override def toString = s"$baseString, name=$name, type=$tpe)"
  override def equals(that: Any): Boolean =
    that match {
      case that: NameCrumb => uuid == that.uuid && name == that.name && tpe == that.tpe
      case _               => false
    }
  override def hashCode: Int = uuid.hashCode ^ name.hashCode ^ tpe.hashCode()
  override def stringProperties: Map[String, String] = Map("name" -> name, "nodeType" -> tpe.toString)
}

@SerialVersionUID(2016010700L)
object EdgeType extends Enumeration {
  type EdgeType = Value
  val InvokedBy, // Parent is invoker
  RequestedBy, // Parent is requester
  HostedBy, // Parent is physical or virtual machine host
  UsingEng, // Parent is Engine ID
  ReplicatedTo, // Parent is node we registered interest in (possibly same as engine ID)
  Batching = Value // Parent is part of the batch represented by the child
}

class EdgeCrumb(val child: ChainedID, val parent: ChainedID, val edge: EdgeType.EdgeType, source: Crumb.Source)
    extends Crumb(child, source) {
  def this(child: ChainedID, parent: ChainedID, edge: EdgeType.EdgeType) =
    this(child, parent, edge, Crumb.OptimusSource)
  override def toString = s"$baseString, parent=$parent, edge=$edge)"
  override def stringProperties: Map[String, String] =
    Map(Headers.Parent -> parent.toString, Headers.ParentVertexId -> parent.vertexId, "edgeType" -> edge.toString)
  // Equality method ignores time
  override def equals(that: Any): Boolean =
    that match {
      case that: EdgeCrumb => uuid == that.uuid && parent == that.parent && edge == that.edge
      case _               => false
    }
  override def hashCode: Int = uuid.hashCode ^ parent.hashCode ^ edge.hashCode
}

class HostCrumb(child: ChainedID, host: InetAddress, source: Crumb.Source = OptimusSource)
    extends EdgeCrumb(child, new ChainedID(host), EdgeType.HostedBy, source) {
  // Caching "InetAddress.getLocalHost" avoids unnecessary DNS lookups and should be fine as hostname is not
  // expected to change during app runtime.
  def this(child: ChainedID) = this(child, LoggingInfo.getHostInetAddr)
}
object HostCrumb {
  def apply(child: ChainedID, host: InetAddress) = new HostCrumb(child, host)
  def apply(child: ChainedID) = new HostCrumb(child, InetAddress.getLocalHost)
  def apply(child: ChainedID, source: Crumb.Source) = new HostCrumb(child, InetAddress.getLocalHost, source)
}

class EventCrumb(uuid: ChainedID, source: Crumb.Source, val event: Event) extends Crumb(uuid, source) {
  def this(uuid: ChainedID, event: Event) = this(uuid, Crumb.OptimusSource, event)
  override def toString = s"$baseString, $event)"
  override def stringProperties: Map[String, String] = Map("event" -> event.name)
  override def equals(that: Any): Boolean = that match {
    case that: EventCrumb => uuid == that.uuid && event == that.event
    case _                => false
  }
  override def hashCode: Int = uuid.hashCode ^ event.hashCode
}

object EventCrumb {

  // This is here temporarily until we move all events under a sealed Event trait.
  type EventDEPRECATED = optimus.breadcrumbs.crumbs.Event

  def apply(uuid: ChainedID, event: Event) = new EventCrumb(uuid, event)
  def apply(source: Crumb.Source, event: Event) = new EventCrumb(ChainedID.root, source, event)
  def apply(uuid: ChainedID, source: Crumb.Source, event: Event) = new EventCrumb(uuid, source, event)
}

class PropertiesCrumb(
    uuid: ChainedID,
    source: Crumb.Source,
    private val m: Map[String, String],
    override val jsonProperties: Map[String, JsValue] = Map.empty,
    hints: Set[CrumbHint] = Set.empty[CrumbHint])
    extends Crumb(uuid, source, hints) {
  def this(uuid: ChainedID, elems: (String, String)*) = this(uuid, Crumb.OptimusSource, Map(elems: _*))
  def this(uuid: ChainedID, hints: Set[CrumbHint], elems: (String, String)*) =
    this(uuid, Crumb.OptimusSource, Map(elems: _*), hints = hints)
  def this(uuid: ChainedID, source: Crumb.Source, elems: (String, String)*) = this(uuid, source, Map(elems: _*))
  def this(uuid: ChainedID, source: Crumb.Source, hints: Set[CrumbHint], elems: (String, String)*) =
    this(uuid, source, Map(elems: _*), hints = hints)
  def this(uuid: ChainedID, m: Map[String, String]) = this(uuid, Crumb.OptimusSource, m, Map.empty[String, JsValue])
  def this(uuid: ChainedID, m: Map[String, String], hints: Set[CrumbHint]) =
    this(uuid, Crumb.OptimusSource, m, Map.empty[String, JsValue], hints)
  def this(uuid: ChainedID, m: Map[String, String], jsonProperties: Map[String, JsValue]) =
    this(uuid, Crumb.OptimusSource, m, jsonProperties)
  def this(uuid: ChainedID, m: Map[String, String], jsonProperties: Map[String, JsValue], hints: Set[CrumbHint]) =
    this(uuid, Crumb.OptimusSource, m, jsonProperties, hints)

  private def mapString =
    cleanProperties.filter(!_._1.startsWith("_")).toSeq.sortBy(_._1).map(e => s"${e._1} -> ${e._2}").mkString(", ")
  override def toString = s"$baseString, $mapString)"
  override def stringProperties: Map[String, String] = m.mapValuesNow(v => if (Objects.isNull(v)) "null" else v)
  override def equals(that: Any): Boolean = that match {
    case that: PropertiesCrumb =>
      uuid == that.uuid && m == that.m && jsonProperties == that.jsonProperties && hints == that.hints
    case _ => false
  }
  override def hashCode: Int = uuid.hashCode() ^ stringProperties.hashCode ^ jsonProperties.hashCode
}

object PropertiesCrumb {

  // Preferred construction from typed pairs
  def apply(uuid: ChainedID, elems: Properties.Elem[_]*) =
    new PropertiesCrumb(uuid, Map.empty[String, String], Map[String, JsValue](elems.map(_.toTuple): _*))
  def apply(uuid: ChainedID, hints: Set[CrumbHint], elems: List[Properties.Elem[_]]) =
    new PropertiesCrumb(uuid, Map.empty[String, String], Map[String, JsValue](elems.map(_.toTuple): _*), hints)
  @varargs def apply(uuid: ChainedID, source: Crumb.Source, elems: Properties.Elem[_]*) =
    new PropertiesCrumb(
      uuid,
      source,
      Map.empty[String, String],
      Map[String, JsValue](elems.map(_.toTuple): _*),
      Set.empty[CrumbHint])
  def apply(uuid: ChainedID, source: Crumb.Source, elems: Properties.Elems) =
    new PropertiesCrumb(uuid, source, Map.empty[String, String], elems.toMap, Set.empty[CrumbHint])
  def apply(uuid: ChainedID, source: Crumb.Source, elems: Properties.Elems, hints: Set[CrumbHint]) =
    new PropertiesCrumb(uuid, source, Map.empty[String, String], elems.toMap, hints)

  @varargs def apply(uuid: ChainedID, source: Crumb.Source, hints: Set[CrumbHint], elems: Properties.Elem[_]*) =
    new PropertiesCrumb(uuid, source, Map.empty[String, String], Map[String, JsValue](elems.map(_.toTuple): _*), hints)
  def apply(uuid: ChainedID, m: => Map[String, String]) = new PropertiesCrumb(uuid, Crumb.OptimusSource, m)
  def apply(uuid: ChainedID, m: => Map[String, String], hints: Set[CrumbHint]) =
    new PropertiesCrumb(uuid, Crumb.OptimusSource, m, hints = hints)
  def apply(uuid: ChainedID, m: => Map[String, String], jsonProperties: Map[String, JsValue]) =
    new PropertiesCrumb(uuid, Crumb.OptimusSource, m, jsonProperties)
  def apply(uuid: ChainedID, m: => Map[String, String], jsonProperties: Map[String, JsValue], hints: Set[CrumbHint]) =
    new PropertiesCrumb(uuid, Crumb.OptimusSource, m, jsonProperties, hints)
  def apply(uuid: ChainedID, source: Crumb.Source, m: => Map[String, String], elems: Properties.Elem[_]*) =
    new PropertiesCrumb(uuid, source, m, Map[String, JsValue](elems.map(_.toTuple): _*))
  def apply(
      uuid: ChainedID,
      source: Crumb.Source,
      m: => Map[String, String],
      hints: Set[CrumbHint],
      elems: Properties.Elem[_]*) =
    new PropertiesCrumb(uuid, source, m, Map[String, JsValue](elems.map(_.toTuple): _*), hints)
  def apply(
      uuid: ChainedID,
      source: Crumb.Source,
      m: => Map[String, String],
      jsonProperties: Map[String, JsValue] = Map.empty,
      hints: Set[CrumbHint] = Set.empty[CrumbHint]) = new PropertiesCrumb(uuid, source, m, jsonProperties, hints)
}

class DiagPropertiesCrumb(uuid: ChainedID, source: Crumb.Source, jsonProperties: Map[String, JsValue])
    extends PropertiesCrumb(
      uuid,
      source,
      Map.empty[String, String],
      jsonProperties ++ Properties.jsMap(Properties._mappend -> "append"),
      CrumbHints.Diagnostics) {
  override def flags: Set[CrumbFlag] = source.flags + CrumbFlag.DoNotReplicate
}

object DiagPropertiesCrumb {
  def apply(uuid: ChainedID, source: Crumb.Source, elems: Properties.Elems) =
    new DiagPropertiesCrumb(uuid, source, elems.toMap)
}

class EventPropertiesCrumb(
    uuid: ChainedID,
    source: Crumb.Source,
    m: Map[String, String],
    jsonProperties: Map[String, JsValue] = Map.empty,
    hints: Set[CrumbHint] = Set.empty[CrumbHint]
) extends PropertiesCrumb(uuid, source, m, jsonProperties ++ Properties.jsMap(Properties._mappend -> "append"), hints)

object EventPropertiesCrumb {
  def apply(uuid: ChainedID, source: Crumb.Source, elems: Elems) =
    new EventPropertiesCrumb(uuid, source, Map.empty[String, String], elems.toMap)
  def apply(uuid: ChainedID, source: Crumb.Source, elems: Properties.Elem[_]*) =
    new EventPropertiesCrumb(uuid, source, Map.empty[String, String], Properties.jsMap(elems: _*))
  def apply(uuid: ChainedID, source: Crumb.Source, hints: Set[CrumbHint], elems: Properties.Elem[_]*) =
    new EventPropertiesCrumb(uuid, source, Map.empty[String, String], Properties.jsMap(elems: _*), hints)
  def apply(uuid: ChainedID, elems: Properties.Elem[_]*) =
    new EventPropertiesCrumb(uuid, Crumb.OptimusSource, Map.empty[String, String], Properties.jsMap(elems: _*))
  def apply(uuid: ChainedID, hints: Set[CrumbHint], elems: Properties.Elem[_]*) =
    new EventPropertiesCrumb(uuid, Crumb.OptimusSource, Map.empty[String, String], Properties.jsMap(elems: _*), hints)
}

private[breadcrumbs] object MetaDataType extends Enumeration {
  type MetaDataType = Value
  val App: crumbs.MetaDataType.Value = Value
}

private[optimus] class AppMetadataPropertiesCrumb(
    uuid: ChainedID,
    source: Crumb.Source,
    m: Map[String, String],
    jsonProperties: Map[String, JsValue] = Map.empty,
    hints: Set[CrumbHint] = Set.empty[CrumbHint])
    extends PropertiesCrumb(
      uuid,
      source,
      m,
      jsonProperties ++ Properties.jsMap(Properties._meta -> MetaDataType.App),
      hints)

class LogPropertiesCrumb(
    uuid: ChainedID,
    source: Crumb.Source,
    m: Map[String, String],
    jsonProperties: Map[String, JsValue] = Map.empty,
    hints: Set[CrumbHint] = Set.empty[CrumbHint])
    extends PropertiesCrumb(
      uuid,
      source,
      m,
      jsonProperties ++ Properties.jsMap(Properties._mappend -> "append"),
      hints) {
  def this(uuid: ChainedID, m: Map[String, String]) = this(uuid, Crumb.OptimusSource, m)
  def this(uuid: ChainedID, m: Map[String, String], hints: Set[CrumbHint]) =
    this(uuid, Crumb.OptimusSource, m, hints = hints)
  def this(uuid: ChainedID, m: Map[String, String], jsonProperties: Map[String, JsValue]) =
    this(uuid, Crumb.OptimusSource, m, jsonProperties)
  def this(uuid: ChainedID, m: Map[String, String], jsonProperties: Map[String, JsValue], hints: Set[CrumbHint]) =
    this(uuid, Crumb.OptimusSource, m, jsonProperties, hints)
  def this(uuid: ChainedID, elems: Array[String]) =
    this(uuid, (elems.indices by 2).map(i => (elems(i), elems(i + 1))).toMap)
  def this(uuid: ChainedID, elems: Array[String], hints: Set[CrumbHint]) =
    this(uuid, (elems.indices by 2).map(i => (elems(i), elems(i + 1))).toMap, hints)
}

object LogPropertiesCrumb {
  private[breadcrumbs] def loginfo =
    Properties.jsMap(Properties.host -> LoggingInfo.getHost, Properties.logFile -> LoggingInfo.getLogFile)
  final case class Location(m: Map[String, JsValue])
  def location(drop: Int): Location = {
    val st = Thread.currentThread().getStackTrace.toSeq.tail
    Location(
      st.dropWhile(_.getClassName.contains(".breadcrumbs."))
        .drop(drop)
        .headOption
        .fold[Map[String, JsValue]] {
          Map.empty
        } { elem =>
          Properties.jsMap(
            Properties.file -> elem.getFileName,
            Properties.line -> elem.getLineNumber,
            Properties.clazz -> elem.getClassName)
        })
  }

  def apply(uuid: ChainedID, source: Crumb.Source, elems: Properties.Elem[_]*) =
    new LogPropertiesCrumb(uuid, source, Map.empty[String, String], Properties.jsMap(elems: _*) ++ location(0).m)
  def apply(uuid: ChainedID, source: Crumb.Source, hints: Set[CrumbHint], elems: Properties.Elem[_]*) =
    new LogPropertiesCrumb(uuid, source, Map.empty[String, String], Properties.jsMap(elems: _*) ++ location(0).m, hints)
  def apply(uuid: ChainedID, elems: Properties.Elem[_]*) =
    new LogPropertiesCrumb(uuid, Map.empty[String, String], Properties.jsMap(elems: _*) ++ location(0).m)
  def apply(uuid: ChainedID, hints: Set[CrumbHint], elems: Properties.Elem[_]*) =
    new LogPropertiesCrumb(uuid, Map.empty[String, String], Properties.jsMap(elems: _*) ++ location(0).m, hints)
  def apply(uuid: ChainedID, source: Crumb.Source, props: Map[String, String], elems: Properties.Elem[_]*) =
    new LogPropertiesCrumb(uuid, source, props, Properties.jsMap(elems: _*) ++ location(0).m)
  def apply(uuid: ChainedID, source: Crumb.Source, elems: Properties.Elems) =
    new LogPropertiesCrumb(uuid, source, Map.empty[String, String], elems.toMap, Set.empty[CrumbHint])
  def apply(
      uuid: ChainedID,
      source: Crumb.Source,
      props: Map[String, String],
      hints: Set[CrumbHint],
      elems: Properties.Elem[_]*) =
    new LogPropertiesCrumb(uuid, source, props, Properties.jsMap(elems: _*) ++ location(0).m, hints)
  def apply(uuid: ChainedID, location: Location, elems: Properties.Elem[_]*) =
    new LogPropertiesCrumb(uuid, Map.empty[String, String], Properties.jsMap(elems: _*) ++ location.m)
  def apply(uuid: ChainedID, location: Location, hints: Set[CrumbHint], elems: Properties.Elem[_]*) =
    new LogPropertiesCrumb(uuid, Map.empty[String, String], Properties.jsMap(elems: _*) ++ location.m, hints)
}

final class LogCrumb(
    val level: String,
    uuid: ChainedID,
    source: Crumb.Source,
    val msg: String,
    hints: Set[CrumbHint] = Set.empty[CrumbHint])
    extends LogPropertiesCrumb(
      uuid,
      source,
      Map.empty,
      Properties.jsMap(Properties.logLevel -> level, Properties.logMsg -> msg),
      hints) {

  def this(level: String, uuid: ChainedID, msg: String) = this(level, uuid, Crumb.OptimusSource, msg)
  def this(level: String, uuid: ChainedID, msg: String, hints: Set[CrumbHint]) =
    this(level, uuid, Crumb.OptimusSource, msg, hints)
  override def toString = s"LogCrumb($prettyNow, $level, $uuid: $msg)"
}

object LogCrumb {
  def info(uuid: ChainedID, source: Crumb.Source, msg: => String): Unit =
    Breadcrumbs.info(uuid, new LogCrumb("INFO", _, source, msg))
  def info(uuid: ChainedID, source: Crumb.Source, msg: => String, hints: Set[CrumbHint]): Unit =
    Breadcrumbs.info(uuid, new LogCrumb("INFO", _, source, msg, hints))
  def error(uuid: ChainedID, source: Crumb.Source, msg: => String): Unit =
    Breadcrumbs.error(uuid, new LogCrumb("INFO", _, source, msg))
  def error(uuid: ChainedID, source: Crumb.Source, msg: => String, hints: Set[CrumbHint]): Unit =
    Breadcrumbs.error(uuid, new LogCrumb("INFO", _, source, msg, hints))
  def warn(uuid: ChainedID, source: Crumb.Source, msg: => String): Unit =
    Breadcrumbs.warn(uuid, new LogCrumb("WARN", _, source, msg))
  def warn(uuid: ChainedID, source: Crumb.Source, msg: => String, hints: Set[CrumbHint]): Unit =
    Breadcrumbs.warn(uuid, new LogCrumb("WARN", _, source, msg, hints))
  def debug(uuid: ChainedID, source: Crumb.Source, msg: => String): Unit =
    Breadcrumbs.debug(uuid, new LogCrumb("DEBUG", _, source, msg))
  def debug(uuid: ChainedID, source: Crumb.Source, msg: => String, hints: Set[CrumbHint]): Unit =
    Breadcrumbs.debug(uuid, new LogCrumb("DEBUG", _, source, msg, hints))
  def trace(uuid: ChainedID, source: Crumb.Source, msg: => String): Unit =
    Breadcrumbs.trace(uuid, new LogCrumb("TRACE", _, source, msg))
  def trace(uuid: ChainedID, source: Crumb.Source, msg: => String, hints: Set[CrumbHint]): Unit =
    Breadcrumbs.trace(uuid, new LogCrumb("TRACE", _, source, msg, hints))

  def info(uuid: ChainedID, msg: => String, hints: Set[CrumbHint] = Set.empty[CrumbHint]): Unit =
    info(uuid, OptimusSource, msg, hints)
  def error(uuid: ChainedID, msg: => String, hints: Set[CrumbHint] = Set.empty[CrumbHint]): Unit =
    error(uuid, OptimusSource, msg, hints)
  def warn(uuid: ChainedID, msg: => String, hints: Set[CrumbHint] = Set.empty[CrumbHint]): Unit =
    warn(uuid, OptimusSource, msg, hints)
  def debug(uuid: ChainedID, msg: => String, hints: Set[CrumbHint] = Set.empty[CrumbHint]): Unit =
    debug(uuid, OptimusSource, msg, hints)
  def trace(uuid: ChainedID, msg: => String, hints: Set[CrumbHint] = Set.empty[CrumbHint]): Unit =
    trace(uuid, OptimusSource, msg, hints)
}

object Crumb {

  object Headers {
    val Crumb = "crumb"
    val Uuid = "uuid" // toString component of ChainedID, probably unique, subject to grid retries
    val VertexId = "vuid" // truly unique component of ChainedID
    val CrumbLocator = "loc" // unique identifier of an individual crumb (multiple crumbs may be issued with the above)
    val Parent = "parent"
    val ParentVertexId = "pvuid"
    val Source = "src"
    val Time = "t"
    val TimeUtc = "tUTC"
    val DefaultSource = "optimus"
    val Hints = "hints"
  }

  private val count = new AtomicInteger(0)
  private def newLocator(): String = ChainedID.root.base + "L" + count.incrementAndGet()
  private val log = LoggerFactory.getLogger("Crumbs")

  import net.iharder.base64.Base64

  sealed trait CrumbFlag
  object CrumbFlag {
    case object DoNotReplicate extends CrumbFlag
    case object DoNotReplicateOrAnnotate extends CrumbFlag
  }

  type CrumbFlags = Set[CrumbFlag]
  object CrumbFlags {
    val None = Set.empty[CrumbFlag]

    def apply(flags: CrumbFlag*) = flags.toSet
  }

  trait Source {
    def name: String
    override def toString: String = name
    val flags: CrumbFlags = CrumbFlags.None
    // If positive, specifies the hard limit for the number of crumbs sent with this source
    val maxCrumbs: Int = -1
    @volatile final private var _shutdown = false
    final def shutdown(): Unit = _shutdown = true
    final def isShutdown: Boolean = _shutdown
    final private[breadcrumbs] val kafkaCount = new AtomicInteger(0)
    final private[breadcrumbs] val kafkaFailures = new AtomicInteger(0)
    final private[breadcrumbs] val enqueueFailures = new AtomicInteger()
    final private[breadcrumbs] val sendCount = new AtomicInteger(0)
    final def getCount: Int = sendCount.get()
    def sentCount: Int = kafkaCount.get()
    final def getKafkaFailures: Int = kafkaFailures.get()
    final def getEnqueueFailures: Int = enqueueFailures.get()
    final override def equals(obj: Any): Boolean = obj match {
      case s: AnyRef => s eq this
      case _         => false
    }
    private[breadcrumbs] def sources: Seq[Source] = Seq(this)
    final override def hashCode(): Int = System.identityHashCode(this)
    def +(that: Source): MultiSource = new MultiSource(sources ++ that.sources)
    def publisherOverride: Option[BreadcrumbsPublisher] = None
  }

  class MultiSource private[Crumb] (override private[breadcrumbs] val sources: Seq[Source]) extends Source {
    override val name: String = sources.map(_.name).mkString("+")
    override val flags: CrumbFlags = sources.map(_.flags).reduce(_ ++ _)
    override val maxCrumbs: Int = sources.map(_.maxCrumbs).min
  }

  trait DalSource extends Source { require(name.startsWith("DAL")) }
  object OptimusSource extends Source { override val name: String = Headers.DefaultSource }
  object RuntimeSource extends Source { override val name = "RT" }
  object GCSource extends Source { override val name = "GC" }
  object ObservableSource extends Source { override val name = "OBS" }
  object ProfilerSource extends Crumb.Source {
    override val name: String = "PROF"
    override val flags = Set(CrumbFlag.DoNotReplicate)
  }

  def newSource(sourceName: String) = new Source {
    override def name = sourceName
  }
  def rtSource(): RuntimeSource.type = RuntimeSource
  def gcSource(): GCSource.type = GCSource

  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

  def prettyNow(t: Long): String =
    ZonedDateTime
      .ofInstant(Instant.ofEpochMilli(t), ZoneIds.UTC)
      .toOffsetDateTime
      .format(formatter)
  def prettyNow(t: String): String = prettyNow(t.toLong)
  def prettyNow: String = prettyNow(now)
  def now: String = System.currentTimeMillis().toString

  def jsonString(as: collection.Seq[String]): String = as.toJson.toString()

  private[crumbs] val extractClass = """.*\b(\w+)Crumb$""".r

  /**
   * Convert a sequence of strings into a sequence of crumbs
   */
  def iterator(payloads: Iterable[String]): Iterator[Crumb] = new Iterator[Crumb] {
    val payloadIterator: Iterator[String] = payloads.iterator
    var crumbIterator: Iterator[Crumb] = new Iterator[Crumb] { def hasNext = false; def next(): Crumb = null }
    override def hasNext: Boolean = crumbIterator.hasNext || payloadIterator.hasNext
    override def next(): Crumb =
      if (crumbIterator.hasNext) crumbIterator.next()
      else {
        val payload = payloadIterator.next()
        log.trace(payload)
        val bytes = Base64.decode(payload, Base64.GZIP)
        val bis = new ByteArrayInputStream(bytes)
        val ois = new ObjectInputStream(bis)
        crumbIterator = new Iterator[Crumb] {
          override def hasNext: Boolean = bis.available() > 0
          override def next(): Crumb = {
            val c = ois.readObject().asInstanceOf[Crumb]
            log.trace(c.toString)
            c
          }
        }
        crumbIterator.next()
      }
  }
  def seq(payloads: Iterable[String]): Seq[Crumb] = iterator(payloads).toSeq

}
