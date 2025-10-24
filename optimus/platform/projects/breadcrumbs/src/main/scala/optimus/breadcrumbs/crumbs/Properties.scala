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

import com.github.benmanes.caffeine.cache.Caffeine
import msjava.base.util.uuid.MSUuid
import optimus.breadcrumbs.ChainedID
import optimus.platform.util.Log
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Objects
import java.{util => ju}
import scala.annotation.varargs
import scala.collection.immutable.SortedSet
import scala.collection.immutable.TreeSet
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

abstract class KnownProperties extends Enumeration {
  import KnownProperties._
  import Properties.Elem
  import Properties.Key

  // copied out of Enumeration.scala
  private def nextNameOrNull = if (nextName != null && nextName.hasNext) nextName.next() else null
  abstract class EnumeratedKey[A: JsonReader: JsonWriter](nme: String) extends Val(nextId, nme) with Key[A] {
    def this() = this(nextNameOrNull)
    override lazy val name: String = toString
    override def source: String = Crumb.Headers.DefaultSource
    def parse(s: String): A = s.parseJson.convertTo[A]
    def parse(js: JsValue): A = js.convertTo[A]
    def toJson(a: A): JsValue = a.toJson
    private[crumbs] def withMeta(md: MetaData): this.type = {
      _meta = md.copy(owner = this)
      this
    }
    private[crumbs] def withMeta(md: MetaData, description: String): this.type = {
      _meta = md.copy(descriptionOverride = description)
      this
    }
    private[crumbs] def withMeta(flags: Int, units: PropertyUnits.Units, description: String = ""): this.type = {
      _meta = MetaData(flags, units, description, this)
      this
    }
  }

  def fromString(s: String): Option[EnumeratedKey[_]] = Try(withName(s)) match {
    case Success(k: EnumeratedKey[_])       => Some(k)
    case Success(_)                         => None
    case Failure(_: NoSuchElementException) => None
    case Failure(e)                         => throw e
  }
  def metaFromString(s: String): MetaData = fromString(s).map(_.meta).getOrElse(NullMeta)

  // This nonsense is necessary to ensure that .elem will take only the correct type, including
  // basic types, when called from Java.
  class EnumeratedKeyRef[A <: AnyRef: JsonReader: JsonWriter](nme: String) extends EnumeratedKey[A](nme) {
    def this() = this(nextNameOrNull)
    final def elem(a: A): Elem[A] = Elem(this, a)
    def apply(a: A): Elem[A] = Elem(this, a)
  }
  class EnumeratedKeyInt(nme: String) extends EnumeratedKey[Int](nme) {
    def this() = this(nextNameOrNull)
    def elem(i: Int): Elem[Int] = Elem(this, i)
    def apply(i: Int): Elem[Int] = Elem(this, i)
  }
  class EnumeratedKeyLong(nme: String) extends EnumeratedKey[Long](nme) {
    def this() = this(nextNameOrNull)
    def elem(i: Long): Elem[Long] = Elem(this, i)
    def apply(i: Long): Elem[Long] = Elem(this, i)
  }
  class EnumeratedKeyDouble(nme: String) extends EnumeratedKey[Double](nme) {
    def this() = this(nextNameOrNull)
    def elem(d: Double): Elem[Double] = Elem(this, d)
    def apply(d: Double): Elem[Double] = Elem(this, d)
  }
  class EnumeratedKeyFloat(nme: String) extends EnumeratedKey[Float](nme) {
    def this() = this(nextNameOrNull)
    def elem(d: Float): Elem[Float] = Elem(this, d)
    def apply(d: Float): Elem[Float] = Elem(this, d)
  }
  class EnumeratedKeyBoolean(nme: String) extends EnumeratedKey[Boolean](nme) {
    def this() = this(nextNameOrNull)
    def apply(b: Boolean): Elem[Boolean] = Elem(this, b)
    def elem(b: Boolean): Elem[Boolean] = Elem(this, b)
  }

  protected[this] def prop[A <: AnyRef: JsonReader: JsonWriter] = new EnumeratedKeyRef[A]
  protected[this] def prop[A <: AnyRef: JsonReader: JsonWriter](nme: String) = new EnumeratedKeyRef[A](nme)
  protected[this] def propI = new EnumeratedKeyInt
  protected[this] def propI(nme: String) = new EnumeratedKeyInt(nme)
  protected[this] def propL = new EnumeratedKeyLong
  protected[this] def propL(nme: String) = new EnumeratedKeyLong(nme)
  protected[this] def propD = new EnumeratedKeyDouble
  protected[this] def propD(nme: String) = new EnumeratedKeyDouble(nme)
  protected[this] def propB = new EnumeratedKeyBoolean
  protected[this] def propB(nme: String) = new EnumeratedKeyBoolean(nme)
  protected[this] def propF = new EnumeratedKeyFloat
  protected[this] def propF(nme: String) = new EnumeratedKeyFloat(nme)

  private[crumbs] def set = values.asInstanceOf[collection.Set[KnownProperties#EnumeratedKey[_]]]
}

object PropertyUnits {
  sealed class Units(nme: String) {
    override def toString: String = nme
    def fromStackCount(count: Long): Long = count
  }
  case object Millis extends Units("ms") {
    override def fromStackCount(count: Long): Long = count / (1000L * 1000L)
  }
  case object Nanoseconds extends Units("ns")
  case object MegaBytes extends Units("MB") {
    override def fromStackCount(count: Long): Long = count / (1024L * 1024L)
  }
  case object Count extends Units("")
  case object BareNumber extends Units("")
  case object Percentage extends Units("%")
  case object Bytes extends Units("B")

}

object KnownProperties {
  val DefaultDescription = "Property"
  abstract class Set private[crumbs] (val kps: KnownProperties*)
  private[optimus] lazy val allKnownProperties = {
    Properties.values.map(_.asInstanceOf[KnownProperties#EnumeratedKey[_]]) | {
      ju.ServiceLoader.load(classOf[Set]).asScala.flatMap(_.kps).toSet.flatMap((_: KnownProperties).set)
    }
  }
  implicit val EnumeratedKeyOrdering: Ordering[KnownProperties#EnumeratedKey[_]] = Ordering.by(_.name)

  import PropertyUnits._

  final case class MetaData(
      flags: Int,
      units: Units,
      descriptionOverride: String = "",
      owner: KnownProperties#EnumeratedKey[_] = null) {
    def sumOverTime: Boolean = (flags & AGG_OVER_TIME) != 0
    def sumOverEngines: Boolean = (flags & AGG_OVER_ENGINES) != 0
    def avgOverTime: Boolean = !sumOverTime
    def avgOverEngines: Boolean = !sumOverEngines
    def graphable: Boolean = (flags & UNGRAPHABLE) == 0
    def isFlame: Boolean = (flags & FLAME) != 0
    def isTime: Boolean = units == Millis || units == Nanoseconds
    def internal: Boolean = (flags & INTERNAL) != 0

    def description =
      if (descriptionOverride.nonEmpty) descriptionOverride
      else if (Objects.nonNull(owner)) owner.name
      else DefaultDescription

    def apply(description: String): MetaData = copy(descriptionOverride = description)

    def withFlags(add: Int, remove: Int = 0): MetaData =
      copy((flags & ~remove) | add)
  }

  val AVG_OVER = 0
  val AGG_OVER_TIME = 1
  val AGG_OVER_ENGINES = 2
  val AGG_ALL = AGG_OVER_TIME | AGG_OVER_ENGINES
  val AGG_HYPER = 4
  val UNGRAPHABLE = 8
  val FLAME = 16
  val INTERNAL = 32

  val NullMeta = MetaData(0, BareNumber)
  val InternalNullMeta = MetaData(INTERNAL, BareNumber)
  val TimeSpent = MetaData(AGG_ALL, Millis)
  val InternalTimeSpent = MetaData(AGG_ALL | INTERNAL, Millis)
  val TimeSpentNs = MetaData(AGG_ALL, Nanoseconds)
  val AllocSamples = MetaData(AGG_ALL, MegaBytes)
  val MemoryInUse = MetaData(AGG_OVER_ENGINES, MegaBytes)
  val UnintegrableCount = MetaData(AGG_OVER_ENGINES, Count)
  val IntegrableCount = MetaData(AGG_ALL, Count)
  val InternalIntegrableCount = MetaData(AGG_ALL | INTERNAL, Count)
  val CumulativeEventCount = MetaData(AGG_OVER_ENGINES, Count)
  val GaugeLevel = MetaData(0, BareNumber)
  val InternalGaugeLevel = MetaData(INTERNAL, BareNumber)

}

//noinspection TypeAnnotation
object Properties extends KnownProperties {

  import KnownProperties._

  implicit class MapStringToJsonOps(m: Map[String, JsValue]) {
    def getAs[T: JsonReader](k: String): Option[T] = m.get(k).flatMap(x => Try(x.convertTo[T]).toOption)
    def getOrElseAs[T: JsonReader](k: String, default: T): T =
      m.get(k).flatMap(x => Try(x.convertTo[T]).toOption).getOrElse(default)
    def getp[A: JsonReader](k: Properties.Key[A]): Option[A] =
      m.get(k.toString).flatMap(x => Try(x.convertTo[A]).toOption)
    def get[T](k: EnumeratedKey[T]): Option[T] = m.get(k.name).map(k.parse)
    def getKey[T](k: EnumeratedKey[T]): Option[T] = m.get(k.name).map(k.parse)
    // For use in iterable-dominated for-comprehensions
    def geti[T](k: EnumeratedKey[T]): Iterable[T] = m.get(k.name).map(k.parse).toIterable
    def getOrElse[T](k: EnumeratedKey[T], default: => T): T = get(k).getOrElse(default)
    // Same thing, if scala gets confused
    def getKeyOrElse[T](k: EnumeratedKey[T], default: => T): T = get(k).getOrElse(default)

    // These two methods avoid converting fully parsing the Elems entry
    def getAsMap(k: Key[Elems]): Option[Map[String, JsValue]] =
      m.get(k.name).map(_.convertTo[Map[String, JsValue]])

    def getAsSeqMap(k: Key[Seq[Elems]]): Option[Seq[Map[String, JsValue]]] =
      m.get(k.name).map(_.convertTo[Seq[Map[String, JsValue]]])
  }

  implicit class MapPropToJsonOps(properties: Map[Properties.Key[_], JsValue]) {

    /**
     * Get a bona-fide typed parameter
     */
    def getp[A](k: Properties.Key[A]): Option[A] =
      properties.get(k).flatMap { v =>
        Try(k.parse(v)).toOption
      }

    def getAs[A: JsonReader](k: String): Option[A] =
      properties.get(Properties.stringToKey(k, None)).flatMap { v =>
        Try(v.convertTo[A]).toOption
      }

    /**
     * Retrieve possibly nested string maps
     */
    def gets(ks: String*): Option[String] = {
      def get(mj: JsValue, ks: List[String]): Option[String] = {
        val m = mj.convertTo[Map[String, JsValue]]
        ks match {
          case Nil => None
          case k :: Nil =>
            m.get(k).map { v =>
              Try(v.convertTo[String]).getOrElse(v.toString)
            }
          case k :: krest =>
            m.get(k).flatMap(v => get(v, krest))
        }
      }

      if (ks.size == 1)
        properties.get(Properties.stringToKey(ks(0), None)).map { v =>
          Try(v.convertTo[String]).getOrElse(v.toString)
        }
      else {
        val k :: krest = ks.toList
        properties.get(Properties.stringToKey(k, None)).flatMap(get(_, krest))
      }
    }

    def has(k: Properties.Key[_]): Boolean = properties.contains(k)
    def has(ks: String*): Boolean = properties.gets(ks: _*).isDefined

  }
  object JsonImplicits {

    class EnumJsonConverter[T <: scala.Enumeration](enu: T) extends RootJsonFormat[T#Value] {
      override def write(obj: T#Value): JsValue = JsString(obj.toString)
      override def read(json: JsValue): T#Value = {
        json match {
          case JsString(txt) => enu.withName(txt)
          case somethingElse =>
            throw DeserializationException(s"Expected a value from enum $enu instead of $somethingElse")
        }
      }
    }

    implicit object NestedElemsJsonConverter extends RootJsonFormat[Elems] {
      override def write(obj: Elems): JsValue = {
        obj.toMap.toJson
      }
      override def read(json: JsValue): Elems = {
        val kvs = json.convertTo[Map[String, JsValue]]
        val elems = kvs.map {
          case (stringKey, jsValue) => {
            val prop = stringToKey(stringKey, None).asInstanceOf[Key[Any]]
            val value = prop.parse(jsValue)
            Elem(prop, value)
          }
        }.toSeq
        Elems(elems: _*)
      }
    }

    implicit def toZonedDateTime(t: Instant): ZonedDateTime =
      ZonedDateTime.ofInstant(t, ZoneId.of("UTC"))

    implicit object ZDTJsonFormat extends RootJsonFormat[ZonedDateTime] {
      override def read(json: JsValue): ZonedDateTime = {
        Try[ZonedDateTime] {
          ZonedDateTime.ofInstant(Instant.ofEpochMilli(json.convertTo[Long]), ZoneId.of("UTC"))
        }.recover { case _ =>
          ZonedDateTime.parse(json.convertTo[String], DateTimeFormatter.ISO_ZONED_DATE_TIME)
        }.get
      }
      override def write(obj: ZonedDateTime): JsValue = obj.toInstant.toEpochMilli.toJson
    }

    implicit object InstantJsonFormat extends RootJsonFormat[Instant] {
      override def write(obj: Instant): JsValue = obj.toEpochMilli.toJson
      override def read(json: JsValue): Instant = Instant.ofEpochMilli(json.convertTo[Long])
    }

    implicit object DurationJsonFormat extends RootJsonFormat[Duration] {
      override def write(obj: Duration): JsValue = obj.toMillis.toJson
      override def read(json: JsValue): Duration = Duration.ofMillis(json.convertTo[Long])
    }

    implicit object ExceptionJsonFormat extends RootJsonFormat[Throwable] {
      override def read(json: JsValue): Throwable = {
        val (className, msg) = json.convertTo[(String, String)]
        try {
          val clazz = Class.forName(className)
          val cs = clazz.getConstructor(classOf[String])
          cs.newInstance(msg).asInstanceOf[Throwable]
        } catch {
          case _: Throwable => UnclassifiedException(className, msg)
        }
      }
      override def write(obj: Throwable): JsValue = {
        val msg = if (obj.getMessage != null) obj.getMessage else ""

        (obj.getClass.getCanonicalName, msg).toJson
      }
    }

    implicit def SortedSetJsonFormat[T: Ordering: JsonFormat]: RootJsonFormat[SortedSet[T]] =
      new RootJsonFormat[SortedSet[T]] {
        override def write(obj: SortedSet[T]): JsValue = obj.toSeq.toJson
        override def read(json: JsValue): SortedSet[T] = TreeSet.empty[T] ++ json.convertTo[Seq[T]]
      }

    implicit val crumbNodeTypeJsonFormat: EnumJsonConverter[CrumbNodeType.type] = new EnumJsonConverter(CrumbNodeType)

    implicit val metaDataTypeJsonFormat: EnumJsonConverter[MetaDataType.type] = new EnumJsonConverter(MetaDataType)

    implicit object EventJsonFormat extends RootJsonFormat[Event] {
      override def read(json: JsValue): Event = Events.parseEvent(json.convertTo[String])
      override def write(obj: Event): JsValue = obj.toString.toJson
    }

    implicit object MSUuidJsonFormat extends RootJsonFormat[MSUuid] {
      override def read(json: JsValue): MSUuid = new MSUuid(json.convertTo[String])
      override def write(obj: MSUuid): JsValue = obj.toString.toJson
    }

    implicit object ChainedIDJsonFormat extends RootJsonFormat[ChainedID] {
      // identity cache - we're generally sending the same chained id over and over again
      private val cache = Caffeine.newBuilder().weakKeys().maximumSize(10).build[ChainedID, JsValue]

      override def read(json: JsValue): ChainedID = {
        val (repr, depth, level, vuid) = json.convertTo[(String, Int, Int, String)]
        new ChainedID(repr, depth, level, vuid)
      }
      override def write(obj: ChainedID): JsValue =
        cache.get(obj, obj => (obj.repr, obj.depth, obj.crumbLevel, obj.vertexId).toJson)
    }

    implicit object KeyJsonFormat extends RootJsonFormat[Key[_]] {
      override def read(json: JsValue): Key[_] = stringToKey(json.convertTo[String], None)
      override def write(obj: Key[_]): JsValue = obj.toString.toJson
    }

    implicit val requestsStallInfoJsonFormat: RootJsonFormat[RequestsStallInfo] = jsonFormat3(RequestsStallInfo.apply)

    implicit val reasonRequestsStallInfoJsonFormat: RootJsonFormat[ReasonRequestsStallInfo] = jsonFormat2(
      ReasonRequestsStallInfo.apply)

    implicit val brokerJsonFormat: RootJsonFormat[Broker] = jsonFormat2(Broker.apply)

    implicit val requestSummaryJsonFormat: RootJsonFormat[RequestSummary] = jsonFormat8(RequestSummary.apply)

    implicit object EventCauseJsonProtocol extends RootJsonFormat[ProfiledEventCause] {
      override def write(event: ProfiledEventCause): JsValue = {
        JsObject(
          "eventName" -> JsString(event.eventName),
          "profilingData" -> event.profilingData.toJson,
          "startTimeMs" -> JsNumber(event.startTimeMs),
          "totalDurationMs" -> JsNumber(event.totalDurationMs),
          "actionSelfTimeMs" -> JsNumber(event.actionSelfTimeMs),
          "childEvents" -> JsArray(event.childEvents.map(child => write(child).toJson).toVector)
        )
      }

      override def read(value: JsValue): ProfiledEventCause = {
        value.asJsObject.getFields(
          "eventName",
          "profilingData",
          "startTimeMs",
          "totalDurationMs",
          "actionSelfTimeMs",
          "childEvents") match {
          case Seq(eventName, profilingData, startTimeMs, totalDuration, actionSelfTimeMs, children) =>
            ProfiledEventCause(
              eventName = eventName.convertTo[String],
              profilingData = profilingData.convertTo[Map[String, String]],
              startTimeMs = startTimeMs.convertTo[Long],
              totalDurationMs = totalDuration.convertTo[Long],
              actionSelfTimeMs = actionSelfTimeMs.convertTo[Long],
              childEvents = children.convertTo[Seq[JsValue]].map(c => read(c))
            )
          case _ => throw deserializationError(s"ProfiledEventCause wrong format")
        }
      }
    }

    /*
  Alternate writer for doubles with limited precision, e.g.:
    import DefaultJsonProtocol.{DoubleJsonFormat => _, _}
    private implicit val rounder: RootJsonFormat[Double] = JsonImplicits.rounder(2)
     */
    def rounder(d: Int): RootJsonFormat[Double] = new RootJsonFormat[Double] {
      override def write(v: Double): JsValue = {
        val a = Math.pow(10, d)
        DoubleJsonFormat.write(Math.round(v * a) / a)
      }
      override def read(json: JsValue): Double = DoubleJsonFormat.read(json)
    }

    /*
      Add extra output fields to a json object on write, and ignore them on read
     */
    implicit class JsonWriterAugmenter[A](val origFormat: RootJsonFormat[A]) extends AnyVal {
      def augmentWriter(fs: (String, A => JsValue)*) = new RootJsonFormat[A] {
        override def write(obj: A): JsValue = {
          val newFields: Map[String, JsValue] = fs.iterator.map { case (k, f) =>
            k -> f(obj)
          }.toMap
          origFormat.write(obj) match {
            case o: JsObject => o.copy(fields = o.fields ++ newFields)
            case j           => new JsObject(newFields + ("_original" -> j))
          }
        }
        override def read(json: JsValue): A = json match {
          case o: JsObject =>
            origFormat.read(o.fields.get("_original") match {
              case Some(jv) => jv
              case None     => new JsObject(o.fields -- fs.map(_._1))
            })
          case x =>
            throw new IllegalArgumentException(s"Can't unaugment $x")
        }
      }
    }

  }
  import JsonImplicits._
  import spray.json._
  import DefaultJsonProtocol._

  sealed trait ElemOrElems

  final case class Elem[A](k: Key[A], v: A) extends ElemOrElems with Log {
    // "null" is not likely to be meaningful, but it's better than throwing an NPE now
    def toTuple: (String, JsValue) = (k.toString, if (null == v) "null".toJson else k.toJson(v))

    private def nonEmptyValue(v: Any) = v match {
      case null            => false
      case None            => false
      case m: Map[_, _]    => m.nonEmpty
      case it: Iterable[_] => it.nonEmpty
      case n: Number =>
        val d = n.doubleValue()
        d != 0.0 && !d.isNaN // Profire ignores all zero values
      case _ => true
    }

    def nonEmpty: Boolean = nonEmptyValue(v)

    private def filterEmptyValues(value: Any): Any = value match {
      case null | None => None
      case m: Map[_, _] =>
        m.flatMap { case (k, v) =>
          val filtered = filterEmptyValues(v)
          if (nonEmptyValue(filtered)) Some(k -> filtered) else None
        }
      case it: Iterable[_] =>
        it.flatMap { elem =>
          val filtered = filterEmptyValues(elem)
          Some(filtered).filter(nonEmptyValue)
        }
      case n: Number if n.doubleValue() == 0.0 => None
      case other                               => other
    }

    def filterEmptyValuesInList: Elem[A] = {
      val value: A =
        try { filterEmptyValues(v).asInstanceOf[A] }
        catch {
          case NonFatal(e) =>
            log.debug(s"Filter empty values failed! $k -> $v", e)
            v
        }
      Elem[A](k, value)
    }
  }
  object Elem {
    def apply[A](tuple: (Key[A], A)): Elem[A] = Elem(tuple._1, tuple._2)
  }

  // Utility wrapper for accumulating property lists - mostly useful because varargs and Seq are equivalent under erasure.
  // See GridProfilerUtils for an example.
  final class Elems(val m: List[Elem[_]]) extends ElemOrElems {
    def ++(o: TraversableOnce[Elem[_]]) = new Elems(m ++ o)
    def +(es: Elems) = new Elems(m ++ es.m)
    def :::(es: Elems) = new Elems(m ++ es.m)
    def ::(es: Elems) = new Elems(m ++ es.m)
    def :::(eso: Option[Elems]) = eso.fold(this)(es => new Elems(m ++ es.m))
    def ::(e: Elem[_]) = new Elems(e :: m)
    def +(e: Elem[_]) = new Elems(e :: m)
    def ::(eo: Option[Elem[_]]) = eo.fold(this)(e => new Elems(e :: m))
    def +(eo: Option[Elem[_]]) = eo.fold(this)(e => new Elems(e :: m))
    def toMap: Map[String, JsValue] = m.map(_.toTuple).toMap
    def toTuples: Seq[(String, JsValue)] = m.map(_.toTuple)
    override def toString = toMap.toString

    // Extract a single element from the Elems list. If you want to extract many elements, use
    // .toMap and then the implicit methods in MapStringToJsonOps.
    def expensiveGet[T](key: Key[T]): Option[T] = m.find(_.k.name == key.name).map(_.v.asInstanceOf[T])
  }

  object Elems {
    val Nil = new Elems(scala.Nil)

    @varargs def apply(ess: ElemOrElems*): Elems =
      new Elems(ess.foldLeft(List.empty[Elem[_]]) {
        case (acc, e: Elem[_]) => e :: acc
        case (acc, Elems.Nil)  => acc
        case (acc, es: Elems)  => es.m ::: acc
      })

    @varargs def nonEmptyElems(ess: ElemOrElems*): Elems = {
      def filtered(e: Elem[_]): Option[Elem[_]] = {
        val f = e.filterEmptyValuesInList
        if (f.nonEmpty) Some(f) else None
      }

      new Elems(
        ess.flatMap {
          case e: Elem[_] => filtered(e)
          case es: Elems  => es.m.flatMap(filtered)
          case _          => None
        }.toList
      )
    }
  }

  trait Key[A] extends HasMetaData {
    def name: String
    @inline def ->(a: A) = Elem(this, a)
    @inline def :=(a: A) = Elem(this, a)
    def source: String
    def parse(js: JsValue): A
    def toJson(a: A): JsValue
    def maybe(b: Boolean, a: => A): Elems = if (b) Elems(this -> a) else Elems.Nil
    def maybe(p: A => Boolean, a: A): Elems = if (p(a)) Elems(this -> a) else Elems.Nil
    def maybe(o: Option[A]): Elems = o.fold(Elems.Nil)(a => Elems(this -> a))
    def nonNull(o: A): Elems = if (Objects.nonNull(o)) Elems(this -> o) else Elems.Nil
  }

  trait HasMetaData {
    protected var _meta: MetaData = NullMeta
    def name: String
    def meta: MetaData = _meta
  }

  class UntypedProperty(val name: String, src: Option[String]) extends Key[String] {
    val source: String = src.getOrElse("optimus")
    override def toString: String = src.fold(name)(_ + s":$name")
    override def parse(js: JsValue): String =
      try {
        js.convertTo[String]
      } catch {
        case NonFatal(_) => js.toString
      }
    override def toJson(a: String): JsValue = a.toJson
    override def equals(obj: Any): Boolean = obj match {
      case utp: UntypedProperty => utp.name == this.name
      case _                    => false
    }
    override def hashCode(): Int = name.hashCode ^ source.hashCode()

  }

  def stringToKey(s: String, src: Option[String]): Key[_] =
    KnownProperties.allKnownProperties.find(_.toString == s) getOrElse {
      s.split(":").toList match {
        case Nil                   => new UntypedProperty("unknown", src.orElse(Some("optimus")))
        case _ :: Nil              => new UntypedProperty(s, src.orElse(Some("optimus")))
        case source :: name :: Nil => new UntypedProperty(name, Some(source))
        case source :: rest        => new UntypedProperty(rest.mkString(":"), Some(source))
      }
    }

  def jsMap(elems: Elem[_]*): Map[String, JsValue] = elems.map(_.toTuple).toMap

  // For adding properties outside the central registry that use the mechanics of crumb
  // property serialization but of course won't be deserializable without the projects that
  // define them on the classpath.
  sealed class AdHocKey[A](nme: String, writer: JsonWriter[A]) extends Key[A] {
    override def name: String = nme
    override def toString = name
    override def source: String = Crumb.Headers.DefaultSource
    def toJson(a: A): JsValue = writer.write(a)
    override def parse(js: JsValue): A = throw new NotImplementedError("This is a rogue property")
  }

  def adhocProperty[A: JsonWriter](name: String)(implicit writer: JsonWriter[A]): Key[A] =
    new AdHocKey[A](name, writer)

  def adhocElems[A: JsonWriter](m: Map[String, A]): Elems =
    Elems(m.map { case (k, v) =>
      adhocProperty[A](k) -> v
    }.toSeq: _*)
  import java.util
  def adhocStringElems(m: util.Map[String, String]): Elems = adhocElems(m.asScala.toMap)
  def adhocStringElems(m: Map[String, String]): Elems = adhocElems(m)

  final case class UnclassifiedException(exceptionName: String, msg: String) extends Exception(msg)

  private[breadcrumbs] val _mappend = prop[String]
  private[optimus] val _meta = prop[MetaDataType.Value]

  val breadcrumbsSentSoFar = propL
  val crumbsSent = prop[Map[String, Int]].withMeta(InternalIntegrableCount)
  val enqueueFailures = prop[Map[String, Int]].withMeta(INTERNAL | AGG_ALL, PropertyUnits.Count)
  val crumbCountAnalysis = prop[Map[String, Map[String, Double]]].withMeta(INTERNAL | AGG_ALL, PropertyUnits.Count)
  val crumbSnapAnalysis =
    prop[Map[String, Map[String, Double]]].withMeta(INTERNAL | AGG_OVER_ENGINES, PropertyUnits.Count)
  val kafkaMetrics = prop[Map[String, Double]].withMeta(INTERNAL, PropertyUnits.BareNumber)
  val uuidLevel = prop[String]

  val `type` = prop[String]
  val subtype = prop[String]
  val description = prop[String]
  val crumbType = prop[String]
  val proid = prop[String]
  val node = prop[String]
  val logFile = prop[String]
  val distedTo = prop[String]
  val distedFrom = prop[String]
  val engineId = prop[ChainedID]
  val engineRoot = prop[String].withMeta(
    NullMeta,
    "'Engine Root' refers to the sampled root UUID of a specific JVM, which can be either local or remote.\n" +
      "For example, if there are 5 engines, it typically means that this execution involved 5 JVMs:\n" +
      "1 JVM for main process, while the other 4 JVMs execute distributed tasks remotely(or locally)."
  )
  val engineRootsCount =
    propI.withMeta(UnintegrableCount, "The number of engine roots in the sampling interval")
  val engine = prop[String]
  private[breadcrumbs] val replicaFrom = prop[ChainedID]
  private[breadcrumbs] val limitCount = propL
  val currentlyRunning = prop[Seq[ChainedID]]
  val distActive = propI
  val distLostTasks = propI
  val distActiveTime = propL
  val activeScopes = prop[Seq[String]]
  val activeRoots = prop[Seq[String]]
  val dedupKey = prop[String]
  val requestId = prop[ChainedID]
  val exception = prop[Throwable]
  val stackTrace = prop[Seq[String]]
  val remoteException = prop[Throwable]
  val batchSize = propI
  val pricingDate = prop[String]
  val dalReqUuid = prop[String]
  val tStarted = prop[ZonedDateTime]
  val tEnded = prop[ZonedDateTime]
  val nodeType = prop[CrumbNodeType.Value]
  val name = prop[String]
  val agents = prop[Seq[String]]
  val debug = prop[String]
  val priority = prop[String]
  val tasksExecutedOnEngine = propL
  val xsLockContention = propL
  val throttleCycleBreaking = propL
  val throttleIgnored = propL

  val logLevel = prop[String]
  val logMsg = prop[String]
  val file = prop[String]
  val line = propI
  val clazz = prop[String]

  val gcCause = prop[String]
  val gcRatio = propD.withMeta(GaugeLevel)
  val gcUsedHeapBeforeGC = propD.withMeta(MemoryInUse)
  val gcUsedOldGenBeforeGC = propD.withMeta(MemoryInUse)
  val gcUsedHeapAfterGC = propD.withMeta(MemoryInUse)
  val gcUsedOldGenAfterCleanup = propD.withMeta(MemoryInUse)
  val gcMaxOldGen = propD.withMeta(MemoryInUse)
  val gcMaxHeap = propD.withMeta(MemoryInUse)
  val gcPools = prop[(Map[String, String], Map[String, String])]
  val gcHeapOkRatio = propD.withMeta(GaugeLevel, "For unit tests only")
  val gcFinalizerCount =
    propI.withMeta(UnintegrableCount)
  val gcFinalizerCountAfter =
    propI.withMeta(UnintegrableCount)
  val gcFinalizerQueue = prop[String]
  val gcNative = prop[String]
  val gcNativeIndex = propI
  val gcNativePath = prop[String]
  val gcNativeEvicted = propL.withMeta(MemoryInUse)
  val gcNativeAllocation = propL
  val gcNativeAllocator = prop[String]
  val gcNativeWatermark = propL
  val gcNativeHighWatermark = propL
  val gcNativeEmergencyWatermark = propL
  val gcNativeCacheClearCount = propI
  val gcRSSLimit = propL
  val gcNativeInvocations = propL
  val gcNativeAllocAfter = propL.withMeta(MemoryInUse)
  val gcNativeHeapChange = propL.withMeta(MemoryInUse) // note, this is not the difference of the previous two
  val gcNativeJVMFootprint = propI.withMeta(MemoryInUse)
  val gcNativeJVMHeap = propI.withMeta(MemoryInUse)
  val gcNativeAlloc = propI.withMeta(MemoryInUse)
  val gcNativeManagedAlloc = propI.withMeta(MemoryInUse)
  val gcNativeRSS = propI.withMeta(MemoryInUse)
  val gcNativeSurrogateRSS = propI.withMeta(MemoryInUse)
  val cacheClearCount = propL.withMeta(IntegrableCount)
  val gcNativeCacheClearCountGC = propL.withMeta(IntegrableCount)
  val gcNativeCacheClearCountMain = propL.withMeta(IntegrableCount)
  val gcNativeCacheClearCountGlobal = propL.withMeta(IntegrableCount)
  val gcNativeCacheClearCountGlobalCallbacks = propL.withMeta(IntegrableCount)
  val gcNativeStats = prop[Map[String, Int]].withMeta(MemoryInUse)
  val gcAction = prop[String]
  val gcName = prop[String]
  val gcDuration =
    propL.withMeta(TimeSpent, "Total of GC times that were reported to GCMonitor during this sampling interval")
  val gcTotalPausesInCycle = propL.withMeta(IntegrableCount)
  val gcMaxPauseInCycle = propL.withMeta(GaugeLevel)
  val gcCacheRemoved = propI.withMeta(IntegrableCount)
  val gcCacheRemaining = propI.withMeta(UnintegrableCount)
  val gcCleanupsFired = propL.withMeta(IntegrableCount)
  val gcMinorsSinceMajor = propL.withMeta(IntegrableCount)
  val gcCacheMemoryUsage = propL.withMeta(MemoryInUse)
  val gcNonCacheMemoryUsage = propL.withMeta(MemoryInUse)
  val gcMemoryLimit = propL.withMeta(MemoryInUse)

  val allocationName = prop[String]
  val allocationInfo = prop[String]
  val allocationEntriesCount = propL.withMeta(UnintegrableCount)
  val allocationMemoryUsage = propL.withMeta(MemoryInUse)
  val allocationKeyspacesCount = propL.withMeta(IntegrableCount)

  val evictionTime = propL.withMeta(TimeSpentNs) // eviction time in ns
  val expiredCount = propL
  val expiredMemory = propL.withMeta(MemoryInUse)
  val evictedCount = propL
  val evictedMemory = propL.withMeta(MemoryInUse)
  val youngestEvictedAccessTime = prop[Instant]
  val youngestEvictedCreationTime = prop[Instant]

  val gcMinUsedHeapAfterGC = propD.withMeta(MemoryInUse)
  val gcMinUsedHeapBeforeGC = propD.withMeta(MemoryInUse)
  val gcMaxUsedHeapAfterGC = propD.withMeta(MemoryInUse)
  val gcMaxUsedHeapBeforeGC = propD.withMeta(MemoryInUse)
  val gcUsedOldGenAfterGC = propD.withMeta(MemoryInUse)
  val gcUsedHeapAfterCleanup = propD.withMeta(MemoryInUse)
  val gcNumMinor = propI
  val gcNumMajor = propI

  val clearCacheLevel = propL
  val triggerRatio = propD.withMeta(GaugeLevel)
  val includeSI = propB

  val profStarts = propL.withMeta(IntegrableCount)
  val profTimes = propL.withMeta(TimeSpent)
  val profWallTime = propL.withMeta(
    TimeSpent,
    "This is the sum of system clock times from when nodes enters the graph to when it completes."
  )
  val profGraphTime = propL.withMeta(
    TimeSpent,
    "Sum of clock time spent in body of node/async methods, does not include explicit wait time." +
      "\nIf there are other threads consuming cpu, this includes time when we're swapped out." +
      "\nNote: might be different from graphCpuTime, that none of these prof times include overhead e.g. OG*sth*.java"
  )
  val profSelfTime = propL.withMeta(
    TimeSpent,
    "Sum of all the nodes self times" +
      "\nSelf time for a node = time spent directly in a node, ignoring any other nodes/async calls it makes" +
      "\nNote: probably the same as profGraphTime "
  )
  val profGraphSpinTime = propL.withMeta(TimeSpent, "Total time spent spinning(read for work) in graph execution")
  val profGraphCpuTime = propL.withMeta(
    TimeSpent,
    "This is explicit cpu time from ThreadMXBean#getCurrentThreadCpuTime spent inside graph (node/async) methods" +
      "\nNote: this one doesn't include either explicit wait (e.g. DAL)," +
      " or slowdown due to other threads (like GC or compiler) consuming cpu."
  )
  val profGraphWaitTime =
    propL.withMeta(
      TimeSpent,
      "Aggregated wall time excludes spin that any graph scheduler threads spent waiting for work to do")
  val profGraphIdleTime = propL.withMeta(TimeSpent, "Total time spent by scheduler threads off-graph, parked")
  val profDalReads = propL.withMeta(IntegrableCount, "Total requests sent to DAL")
  val profDalEntities = propL.withMeta(IntegrableCount, "Total number of entities returned by those DAL read commands")
  val profDalWrites = propL.withMeta(IntegrableCount, "Total number of DAL write commands")
  val profUnderUtilizedTime = propL.withMeta(TimeSpent)
  val profCacheTime = propL.withMeta(TimeSpent, "Total time spent looking up nodes in cache")
  val profDalRequestsDist = propI.withMeta(IntegrableCount)
  val profDalWritesDist = propI.withMeta(IntegrableCount)
  val profDalResultsDist = propI.withMeta(IntegrableCount)
  val profMaxHeap = propL.withMeta(MemoryInUse, "Current maximum heap memory that can be used (e.g. -Xmx limit)")
  val profCurrHeap = propL.withMeta(MemoryInUse, "Current heap memory used")
  val profMaxNonHeap = propL.withMeta(MemoryInUse, "Current maximum non-heap memory that can be used (e.g. -Xmx limit)")
  val profCurrNonHeap = propL.withMeta(MemoryInUse, "Current non-heap memory used")
  val profJvmCPUTime = propL.withMeta(TimeSpent, "JVM CPU time used (from OperatingSystemMXBean.getProcessCpuTime)")
  val profJvmCPULoad = propD.withMeta(
    GaugeLevel,
    "Percentage of system CPU (0 - 100%) used by this application (from OperatingSystemMXBean.getProcessCpuLoad)")
  val profSysCPULoad =
    propD.withMeta(
      GaugeLevel,
      "Percentage of system CPU (0 - 100%) that was used by all processes on the machine (from OperatingSystemMXBean.getSystemCpuLoad)")
  val profSysFreeMem = propL.withMeta(MemoryInUse, "Free system memory")
  val profSysTotalMem = propL.withMeta(MemoryInUse, "Total system memory")
  val profCgroupMem = prop[Map[String, Long]].withMeta(MemoryInUse, "Cgroup memory")
  val profLoadAvg =
    propD.withMeta(
      UnintegrableCount,
      "Average number of busy logical CPUs on the system over the last minute (from OperatingSystemMXBean.getSystemLoadAverage)")
  val profGcStopTheWorld = propL.withMeta(TimeSpent)
  val profGcTimeAll = propL.withMeta(
    TimeSpent,
    "Total time spent in GC, " +
      "reported by ManagementFactory.getGarbageCollectorMXBeans during the sampling interval")
  val profGcCount = propL.withMeta(IntegrableCount, "Number of GC events")
  val profJitTime = propL.withMeta(TimeSpent, "JIT compiler time (always parallel, this contributes to the CPU time)")
  val profClTime = propL.withMeta(TimeSpent, "The accumulated elapsed time spent by class loading")
  val profDistOverheadAtEngine = propL.withMeta(TimeSpent)
  val profDistTasks = propI.withMeta(IntegrableCount)
  val profNodeExecutionTime = propL.withMeta(TimeSpent)
  val profThreads = prop[Map[String, Map[String, Long]]]
  val profCaches = prop[Map[String, Map[String, Long]]]
  val profMetricsDiff = prop[Map[String, Map[String, Array[Double]]]]
  val profSS = prop[Map[String, Array[String]]]
  val profEvictions =
    prop[Map[String, Long]].withMeta(IntegrableCount, "Total number of nodes evicted from cache, for various reasons")

  // Events about DependencyTracker queue sizes
  // Technically, Queued = Added - Processed but in practice it might differ slightly depending on when snaps are
  // published.

  // Totals are always published
  val profDepTrackerTaskTotalAdded = propL.withMeta(IntegrableCount)
  val profDepTrackerTaskTotalProcessed = propL.withMeta(IntegrableCount)
  val profDepTrackerTaskTotalQueued = propI.withMeta(UnintegrableCount)

  // Breakdown per queue is only published for the first -Doptimus.sampling.deptracker.maxqueues queues (defaults to 0)
  val profDepTrackerTaskAdded = prop[Map[String, Long]].withMeta(IntegrableCount)
  val profDepTrackerTaskProcessed = prop[Map[String, Long]].withMeta(IntegrableCount)
  val profDepTrackerTaskQueued = prop[Map[String, Int]].withMeta(UnintegrableCount)

  val cardEstimated = prop[Map[String, Int]].withMeta(MetaData(AGG_HYPER | UNGRAPHABLE, PropertyUnits.Count))
  val cardRaw = prop[Map[String, Long]]
  val cardNumerator = prop[Map[String, Double]]
  val cardEstimators =
    prop[Map[String, Map[String, Int]]].withMeta(MetaData(AGG_HYPER | UNGRAPHABLE, PropertyUnits.Count))

  val profStallTime = propL.withMeta(TimeSpent)
  val pluginCounts = prop[Map[String, Map[String, Long]]].withMeta(IntegrableCount)
  val pluginSnaps = prop[Map[String, Map[String, Long]]].withMeta(UnintegrableCount)
  val pluginStateTimes = prop[Map[String, Map[String, Long]]].withMeta(TimeSpent)
  val pluginFullWaitTimes = prop[Map[String, Long]].withMeta(TimeSpent)
  val pluginStallTimes = prop[Map[String, Long]].withMeta(TimeSpent)
  val pluginNodeOverflows = prop[Map[String, Long]].withMeta(IntegrableCount)
  val profDALBatches = propI.withMeta(IntegrableCount)
  val profDALCommands = propI.withMeta(IntegrableCount)
  val profGCDuration = propL.withMeta(TimeSpent)
  val profGCMajorDuration = propL.withMeta(TimeSpent)
  val profGCMinorDuration = propL.withMeta(TimeSpent)
  val profGCMajors = propI.withMeta(IntegrableCount)
  val profGCMinors = propI.withMeta(IntegrableCount)
  val profCommitedMB = propL.withMeta(MemoryInUse)
  val profHeapMB = propL.withMeta(MemoryInUse)
  val profNonAuxBlockingTime = propL.withMeta(TimeSpent)
  val profStartupEventTime = propL.withMeta(TimeSpent)
  val profHandlerEventTime = propL.withMeta(TimeSpent)

  val snapTimeMs = propL.withMeta(TimeSpent)
  val snapTimeUTC = prop[String]
  val snapEpochId = propL.withMeta(InternalNullMeta)
  val snapPeriod = propL.withMeta(InternalTimeSpent)
  val snapDuration = propL.withMeta(InternalTimeSpent)
  val snapBatch = propI
  val canonicalPub = propB

  val pulse = prop[Elems]
  val pulseMeta = prop[Map[String, Int]]

  val profiledEvent = prop[ProfiledEventCause]
  val profiledEventStatistics = prop[Map[String, JsValue]]
  val profilingMode = prop[String]
  val withConsole = propB
  val entityAgentVersion = prop[String]

  val env = prop[String]
  val sysEnv = prop[Map[String, String]]
  val appId = prop[String]
  val appIds = prop[Seq[String]]
  val timeout = propL
  val pid = propL
  val ppid = propL
  val cpid = propL
  val host = prop[String]
  val port = prop[String]
  val user = prop[String]
  val clusterName = prop[String]
  val server = prop[Map[String, JsValue]]
  val args = prop[Seq[String]].withMeta(NullMeta, "for risk related usages")
  val argsMap = prop[Map[String, String]].withMeta(NullMeta, "for profiling related usages")
  val argsSeq = prop[Seq[(String, String)]].withMeta(NullMeta, "for profiling related usages")
  val argsType = prop[String]
  val tmInstance = prop[String]
  val config = prop[Map[String, String]]
  val event = prop[String]
  val severity = prop[String]
  val duration = propL // event duration in milliseconds
  val commands = prop[Seq[String]]
  val className = prop[String]
  val cmdLine = prop[String]
  val appDir = prop[String]
  val javaVersion = prop[String]
  val javaHome = prop[String]
  val javaOpts = prop[String]
  val scalaHome = prop[String]
  val osName = prop[String]
  val osVersion = prop[String]
  val sysLoc = prop[String]
  val logname = prop[String]
  val time = prop[String]
  val splunkTime = prop[String]
  val rootUuid = prop[String]
  val publisherId = prop[String]
  val invocationStyle = prop[String]
  val gsfControllerId = prop[String]
  val gsfEngineId = prop[String]
  val kubeNodeName = prop[String]
  val state = prop[String]
  val enabled = propB

  val appLaunchContextType = prop[String]
  val appLaunchContextEnv = prop[String]
  val appLaunchContextName = prop[String]

  val autosysJobName = prop[String]
  val autosysInstance = prop[String]

  // DAL
  val broker = prop[Broker]("broker")
  val reqId = prop[String]
  val requestSummary = prop[RequestSummary]("req")
  val requestCommandLocations = prop[String]("RequestCommandLocations")
  val clientLatency = prop[String]("cltLat")

  val reason = prop[String]
  val reasonRequestsStallInfo = prop[Seq[ReasonRequestsStallInfo]]

  val clearCacheIncludeSSPrivate = propB
  val clearCacheIncludeSiGlobal = propB
  val clearCacheIncludeGlobal = propB
  val clearCacheIncludeNamedCaches = propB
  val clearCacheIncludePerPropertyCaches = propB
  val clearCacheFilterDescription = prop[String]
  val numRemoved = propL

  // Collection method statistics
  val methodName = prop[String]
  val countNum = propL

  // Notification/Reactive warning/errors
  val reactiveError = prop[String]
  val reactiveTickingScenario = prop[String]
  val reactiveTargetScenario = prop[String]
  val reactiveBindId = prop[String]

  val scope = prop[String]

  val obtScope = prop[String]
  val obtCommit = prop[String]
  val obtWorkspace = prop[String]
  val obtProgresses = prop[Seq[(Instant, String, Double)]]
  val obtStart = prop[Instant]
  val obtEnd = prop[Instant]
  val obtWallTime = prop[Duration]
  val obtCategory = prop[String]
  val obtBenchmarkScenario = prop[String]
  val obtBuildId = prop[String]
  val obtRunDetails = prop[Map[String, String]]
  val obtDurationByPhase = prop[Map[String, Map[String, Long]]]
  val obtDurationCentilesByPhase = prop[Map[String, Seq[Long]]]
  val obtDurationByCategory = prop[Map[String, Map[String, Long]]]
  val obtDurationByScenario = prop[Map[String, Map[String, Long]]]
  val obtDurationCentilesByCategory = prop[Map[String, Seq[Long]]]
  val obtFailuresByCategory = prop[Map[String, Int]]
  val obtErrorsByCategory = prop[Map[String, Long]]
  val obtWarningsByCategory = prop[Map[String, Long]]
  val obtStats = prop[Map[String, Long]]
  val obtStatsByCategory = prop[Map[String, Map[String, Long]]]
  val obtStressTestIterations = propI
  val obtStartCentiles = prop[Seq[Instant]]
  val obtEndCentiles = prop[Seq[Instant]]
  val obtWallTimes = prop[Map[String, Long]]
  val obtWallTimeCentiles = prop[Seq[Duration]]

  val obtTaskStart = prop[Instant]
  val obtTaskEnd = prop[Instant]
  val obtTaskDuration = prop[Duration]
  val obtTaskCmd = prop[String]
  val obtTaskCategory = prop[String]
  val obtTaskLog = prop[String]
  val obtTaskExitCode = propI
  val obtTaskState = prop[String]
  val obtTaskTryCount = propI
  val obtTaskMaxTryCount = propI
  val obtUploadHost = prop[String]
  val obtUploadTargetDir = prop[String]

  val obtRegexRuleInputSize = propI
  val obtRegexPatternInputSize = propI
  val obtRegexTotalScopesScanned = propI
  val obtRegexScan = propL
  val obtRegexRules = prop[Map[String, Long]]

  // pgo group validation properties
  val pgoDiff = prop[Map[String, String]]
  val optconfPath = prop[String]
  val optconfAction = prop[String]
  val optconfApplyTimeElapsed = propL
  val artifacts = prop[String]

  val hotspotSource = prop[String]
  val hotspotEngine = prop[String]
  val hotspotStart: Properties.EnumeratedKeyLong = propL.withMeta(IntegrableCount, "Number of node starts")
  val hotspotEvicted = propL.withMeta(IntegrableCount, "Nodes evicted from cache")
  val hotspotInvalidated = propI.withMeta(IntegrableCount, "Number of invalidations for NodeTasks")
  val hotspotCacheHit = propL.withMeta(IntegrableCount, "Number of cache hits")
  val hotspotCacheHitFromDifferentTasks =
    propL.withMeta(NullMeta, "Number of cache hits from different tasks")
  val hotspotCacheMiss = propL.withMeta(IntegrableCount, "Number of cache misses")
  val hotspotCacheTime = propL.withMeta(TimeSpent, "Time spent on cache lookup logic")
  val hotspotCacheCycle =
    propL.withMeta(GaugeLevel, "The maximum depth in the LRU cache at which we re-used (re-cycled) the node")
  val hotspotCacheCycleStats =
    prop[Seq[Long]]
      .withMeta(NullMeta, "Histogram Y-axis of the LRU depths(log_2 of the count that occurred at this depth)")
  val hotspotCacheHisto = prop[String].withMeta(NullMeta, "Histogram of LRU depths")
  val hotspotXsLookupTime = propL.withMeta(TimeSpent, "Time spent in cross scenario lookups")
  val hotspotNodeReusedTime = propL.withMeta(
    TimeSpent,
    "Total time saved by using result from cache, rather than recalculating" +
      "<br>This can be inaccurate because first evaluation of a @node can trigger classloading, (lazy) val evaluation (such as one-time hashCode calculation), JIT compilation etc" +
      "<br>So benefit can be over-inflated, and vary from run to run, as node evaluation order is non-deterministic"
  )
  val hotspotCacheBenefit = propL.withMeta(
    TimeSpent,
    "Derived Definition: Reused Node Time - Cache Time" +
      "<br>Calculated cache benefit of this node" +
      "<br>Note warnings on Reused Node Time tooltip"
  )
  val hotspotAvgCacheTime = propL.withMeta(TimeSpent, "Average cache time: time/(hit+miss)")
  val hotspotAncSelfTime = propL.withMeta(
    TimeSpent,
    "Ancillary Self Time<br>Defined as sum of self times of all non-cached child nodes<br><br>" +
      "<div style='font-family:consolas;font-size:12pt'><span style='color:blue'>def</span> g = given(tweaks) { <span style='color:green'>some_code</span> }</div>" +
      "<span style='color:green'>some_code</span> is represented by a node (<i>g_given_12</i> where 12 is the line number in the source file)" +
      "<br>ANC Time for node <i>g</i> will be the selfTime of the <i>g_given_12</i> node"
  )
  val hotspotPostCompleteTime =
    propL.withMeta(
      TimeSpent,
      "Time between node completing/suspending and stopping, across multiple invocations<br><br>" +
        "<i>Most of this time is taken notifying the waiters of this node.<br>" +
        "It's a graph internal time, not related to the user code in this node</i>"
    )
  val hotspotWallTime =
    propL.withMeta(TimeSpent, "Total Wall Time (Warning not-additive and therefore could be useless)")
  val hotspotSelfTime = propL.withMeta(
    TimeSpent,
    "Total CPU time, across multiple invocations<br><br>" +
      "<i>Note - includes: <br>" +
      "Cache lookup time of its children<br>" +
      "Tweak lookup time<br>" +
      "Some args hash time for tweakable nodes in XS scope</i><br>" +
      "See docs for more detail"
  )
  val hotspotTweakLookupTime = propL.withMeta(TimeSpent, "Time spent looking up tweak of a given property")
  val hotspotIsScenarioIndependent =
    prop[Option[Boolean]]
  val hotspotFavorReuse = prop[Option[Boolean]]
  val hotspotCacheable = prop[Option[Boolean]]
  val hotspotPropertyName = prop[String]
  val spPropertyName = prop[String].withMeta(NullMeta, "Shortened Property/Node Name")
  val hotspotPropertyFlagsString = prop[String]
  val hotspotPropertyFlags = propL
  val hotspotRunningNode = prop[String]
  val hotspotPropertySource = prop[String]
  val hotspotCollisionCount =
    propI.withMeta(IntegrableCount, "Number of (hash) collisions encountered when looking up NodeTasks in NodeCache")
  val hotspotChildNodeLookupCount = propI.withMeta(IntegrableCount, "Total number of child nodes looked up")
  val hotspotChildNodeLookupTime = propL.withMeta(
    TimeSpent,
    "Time spent looking up child nodes in cache" +
      "<br>This is included in the Self Time of node" +
      "<br>If Self Time and Child Node Lookup Time are close, it implies the node does little work" +
      "<br>Child Node Lookup Time is the Cache Time"
  )

  val mergedAppName = prop[String].withMeta(NullMeta, "The merged application name for optimus app")
  val smplTimes = prop[Map[String, Long]].withMeta(InternalTimeSpent)
  val flameTimes = prop[Map[String, Long]]
    .withMeta(TimeSpent.withFlags(FLAME), "root total time for a sample that is published as a flame graph")
  val flameLive = prop[Map[String, Long]]
    .withMeta(
      MemoryInUse.withFlags(FLAME),
      "root total live memory used for a sample that is published as a flame graph")
  val flameAlloc =
    prop[Map[String, Long]].withMeta(
      AllocSamples.withFlags(FLAME),
      "root total memory allocated for a sample that is published as a flame graph")
  val flameCounts = prop[Map[String, Long]]
    .withMeta(IntegrableCount.withFlags(FLAME), "root total value for a sample that is published as a flame graph")
  val samplingPauseTime = propL.withMeta(InternalTimeSpent)
  val childProcessCount = propI.withMeta(UnintegrableCount, "Number of child processes")
  val childProcessCPU = propD.withMeta(GaugeLevel, "Total CPU load of all child processes")
  val childProcessRSS = propL.withMeta(MemoryInUse, "Total resident memory size of all child processes")
  val spInternalStats = prop[Map[String, Map[String, Double]]].withMeta(InternalGaugeLevel)
  val apInternalStats = prop[Map[String, Double]].withMeta(InternalGaugeLevel)

  val crumbplexerIgnore = prop[String] // tells crumbplexer to ignore the whole crumb; value is the reason
  val plexerCountPrinted = propL.withMeta(IntegrableCount)
  val plexerCountDeduped = propL.withMeta(IntegrableCount)
  val plexerCountDropped = propL.withMeta(IntegrableCount)
  val plexerCountParseError = propL.withMeta(IntegrableCount)
  val plexerCountPause = propL.withMeta(TimeSpent)
  val plexerSnapFreeDisk = propL.withMeta(MemoryInUse)
  val plexerSnapOldestHours = propL.withMeta(GaugeLevel)
  val plexerSnapRootCount = propL.withMeta(UnintegrableCount)
  val plexerSnapLagClient = propL.withMeta(MetaData(0, PropertyUnits.Millis, "Print lag since crumb time"))
  val plexerSnapLagKafka = propL.withMeta(MetaData(0, PropertyUnits.Millis, "Print lag since kafka publish"))
  val plexerSnapLagQueue = propL.withMeta(MetaData(AVG_OVER, PropertyUnits.Millis, "Print lag since enqueued"))
  val plexerSnapQueueSize = propL.withMeta(MetaData(AVG_OVER, PropertyUnits.Count), "Queue length")
  val plexerCountExceptions = propL.withMeta(IntegrableCount, "Exceptions in main poll loop")

  val profDmcCacheAttemptCount = propL.withMeta(IntegrableCount)
  val profDmcCacheMissCount = propL.withMeta(IntegrableCount)
  val profDmcCacheHitCount = propL.withMeta(IntegrableCount)
  val profDmcCacheComputeCount = propL.withMeta(IntegrableCount)
  val profDmcCacheDeduplicationCount = propL.withMeta(IntegrableCount)
  val profStats = prop[Map[String, String]]
  val profStatsType = prop[String]
  val profSummary = prop[Map[String, JsObject]]
  val profOpenedFiles = prop[Seq[String]]
  val miniGridMeta = prop[JsObject]
  val profStacks = prop[Seq[Elems]]
  val numStacksPublished = propL.withMeta(InternalIntegrableCount)
  val numStacksReferenced = propL.withMeta(InternalIntegrableCount)
  val numSamplesPublished =
    propL.withMeta(
      InternalIntegrableCount,
      "Number of stacks published from sampling profiler during this sampling interval")
  val numCrumbFailures = propI.withMeta(InternalIntegrableCount)
  val numStackProblems = propL.withMeta(InternalIntegrableCount)
  val numSamplersDisabled = propL.withMeta(InternalIntegrableCount)
  val crumbQueueLength = propI.withMeta(InternalGaugeLevel)
  val crumbConfig = prop[Seq[String]]
  val stackElem = prop[String]
  val pTpe = prop[String]
  val jfrSize = propL
  val pSlf = propL
  val pTot = propL
  val pSID = prop[String]
  val miniGridCalc = prop[String]
  val stepName = prop[String]
  val stepProfile = prop[Seq[Elems]]
  val profCollapsed = prop[String]
  val profMS = prop[String]
  val stackThumb = prop[Map[String, Int]]
  val profPreOptimusStartup = propL.withMeta(TimeSpent, "Time spent from JVM startup to invocation of withOptimus")
  val profOptimusStartup = propL.withMeta(
    TimeSpent,
    "Time spent from invocation of withOptimus to execution of withOptimus closure  - includes DAL init")

  val distWallTime = propL.withMeta(TimeSpent)
  val distTaskDuration = propL.withMeta(TimeSpent)
  val distTasksComplete = propI.withMeta(IntegrableCount)
  val distTasksRcvd = propI.withMeta(IntegrableCount)
  val distBytesRcvd = propL.withMeta(AllocSamples)
  val distBytesSent = propL.withMeta(AllocSamples)

  // Temporal surface tracing
  val tsQueryClassName = prop[String]
  val tsQueryType = prop[String]
  val tsTag = prop[String]

  val vt = prop[Instant]
  val tt = prop[Instant]
  val dmcClientSummary = prop[Map[String, Long]]

  val plugin = prop[String]
  val totalPriceables = propI
  val usdAverageSwaps = propI
  val usdAverageSwapsPercentage = propD

  /** givenOverlay use cases */
  val currentScenario = prop[String]
  val overlayScenario = prop[String]
  val trivialOverlay = propB

  /** Cycle recovery node and stack */
  val recoveredCycleNode = prop[String]
  val recoveredCycleStack = prop[String]

  /** Cache time misreported task name */
  val cacheTimeMisreported = prop[String]

  /** OGTrace exception */
  val ogtraceBackingStoreProblem = prop[String]

  /** RT verifier violations */
  val rtvViolation = prop[String]
  val rtvOwner = prop[String]
  val rtvLocation = prop[String]
  val rtvStack = prop[String]

  /** Instrumented node + all concrete implementations */
  val baseClass = prop[String]
  val concreteClass = prop[String]

  val targetNode = prop[String]
  val requestingNode = prop[String]
  val exceptionTrace = prop[String]

  /** Used to detect whether a certain feature is being used */
  val feature = prop[String]

  /** EmailSender tracking */
  val recipientDomains = prop[Seq[String]]
  val senderAPI = prop[String]
  val sender = prop[String]
  val jobName = prop[String]

  val fileContents = prop[String]

  val cpuInfo = prop[Map[String, String]]
  val cpuCores = propI
  val idealThreads = propI
  val graphUtil = propD
  val memInfo = prop[Map[String, String]]

  /** Catch production uses of -Doptimus.runtime.allowIllegalOverrideOfInitialRuntimeEnvironment=true */
  val overrideInitRuntimeEnv = propB

  val schedulerState = prop[String]
  val profWorkThreads = propI
  val profWaitThreads = propI
  val profBlockedThreads = propI
  val scalaFallbackModule = prop[String] // object that was looked up
  val scalaFallbackCls = prop[String] // class it was looked up from

  val scenarioExpected = prop[String]
  val scenarioFound = prop[String]

  /** Graph Stress Test history and timings */
  val stressTestInjector = prop[String]
  val stressTestSuite = prop[String]
  val stressTestGraph = prop[String]
  val stressTestTest = prop[String]
  // measured in seconds
  val stressTestAvgGraphTimeS = propD
  val stressTestFailure = propB

  /** startup time on Minigrid */
  val jvmUptime = propL

  val userWarningsAdded = prop[Map[String, Int]]
  val userWarningsRemoved = prop[Map[String, Int]]
  val totalWarningsAdded = propI
  val totalWarningsRemoved = propI
  val numBadBaseline = propI
  val numFileReadErrors = propI
  val buildNumber = propI

  val oldCacheSize = propI
  val newCacheSize = propI

  val auxSchedulerTimedOutNodes = prop[Seq[String]]
  val auxSchedulerTotalFlushDurationNs = propL
  val auxSchedulerFlushDurationNs = propL

  val kvProcessorStats = prop[NestedMapOrValue[Double]]

  val artifactId = prop[String]
  val artifactCacheZkPath = prop[String]
  val artifactDownloadStats = prop[NestedMapOrValue[Long]]
  val artifactDownloadTempArchivePath = prop[String]
  val artifactDownloadExtractPath = prop[String]

  val jenkinsTestCase = prop[String]
  val jenkinsAppName = prop[String]
  val jenkinsModuleName = prop[String]
  val jenkinsModuleGroupName = prop[String]
  val jenkinsRegressionName = prop[String]
  val jenkinsBuildNumber = prop[String]
  val jenkinsTestPhase = prop[String]
  val prId = prop[String]
  val installVersion = prop[String]
  val stratoVersion = prop[String]
  val idKvm = prop[String]
  val buildCommit = prop[String]
  val customId = prop[String]

  val appExpandedPath = prop[String]
  val appOriginalPath = prop[String]
  val appMeta = prop[String]
  val appProject = prop[String]
  val appRelease = prop[String]
  val appReleaseLink = prop[String]
  val appGroup = prop[String]
  val appGridEngineName = prop[String]

  val snapshotType = prop[String]
  val snapshotName = prop[String]
  val snapshotFullTime = prop[String]
  val snapshotIncrementalTime = prop[String]
  val snapshotProcessingTimeMs = propL
  val snapshotEntries = propL
  val snapshotBytes = propL
  val snapshotRawBytes = propL
  val snapshotBlockingWaitMs = propL
  val snapshotRetries = propI

  // splunk result properties
  val result = prop[Map[String, JsValue]]
  val results = prop[JsArray]
  val message = prop[String]

  val pricingMachineId = prop[String]
  val pricingPortId = prop[String]
  val pricingTrade = prop[String]
  val pricingHugeNumber = prop[String]
  val pricingNumbers = prop[Seq[String]]

  val treadmillEnv = prop[Map[String, String]]

  val source = prop[String]
}

final case class RequestsStallInfo(pluginType: StallPlugin.Value, reqCount: Int, req: Seq[String]) {
  def take(n: Int): RequestsStallInfo = RequestsStallInfo(pluginType, reqCount, req.take(n))
}

final case class ReasonRequestsStallInfo(reason: String, requestsStallInfo: RequestsStallInfo)

object StallPlugin {
  type Value = String

  val None = "None"
  val DAL = "DAL"
  val Dist = "DIST"
  val DMC = "DMC"
  val JOB = "JOB"
}

object RequestsStallInfo {
  val empty: RequestsStallInfo = RequestsStallInfo(StallPlugin.None, 0, Seq.empty)
}

final case class Broker(host: String, port: String)

final case class RequestSummary(
    uuid: String,
    user: String,
    clientMachine: String,
    appId: String,
    zoneId: String,
    pid: Int,
    clientPath: String,
    cmdSummary: String)

final case class ProfiledEventCause(
    eventName: String,
    profilingData: Map[String, String],
    startTimeMs: Long,
    totalDurationMs: Long,
    actionSelfTimeMs: Long,
    childEvents: Seq[ProfiledEventCause]) {
  def prettyPrint(level: Int = 0, path: String = "0"): String = {
    val stringBuilder = new StringBuilder
    stringBuilder.append("Profile Details:\n")
    prettyPrintImpl(stringBuilder, level, path)
    stringBuilder.toString
  }

  def prettyPrintImpl(sb: StringBuilder, level: Int = 0, path: String = ""): Unit = {
    sb.append(s"${" " * level}$path. $eventName\t$totalDurationMs\n")
    childEvents.zipWithIndex.foreach { case (s, i) =>
      s.prettyPrintImpl(sb, level + 1, s"$path-$i")
    }
  }
}
