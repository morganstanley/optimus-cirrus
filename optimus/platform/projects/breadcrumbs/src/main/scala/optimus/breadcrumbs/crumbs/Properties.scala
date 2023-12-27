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

import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.{util => ju}
import msjava.base.util.uuid.MSUuid
import optimus.breadcrumbs.ChainedID
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.Objects
import scala.annotation.varargs
import scala.collection.immutable.SortedSet
import scala.collection.immutable.TreeSet
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.util.Try

abstract class KnownProperties extends Enumeration {
  import Properties.Elem
  import Properties.Key

  // copied out of Enumeration.scala
  private def nextNameOrNull = if (nextName != null && nextName.hasNext) nextName.next() else null
  abstract class EnumeratedKey[A: JsonReader: JsonWriter](nme: String) extends Val(nextId, nme) with Key[A] {
    def this() = this(nextNameOrNull)
    override def name: String = toString
    override def source: String = Crumb.Headers.DefaultSource
    def parse(s: String): A = s.parseJson.convertTo[A]
    def parse(js: JsValue): A = js.convertTo[A]
    def toJson(a: A): JsValue = a.toJson
  }
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

  private[crumbs] def set = values.asInstanceOf[Set[KnownProperties#EnumeratedKey[_]]]
}

object KnownProperties {
  abstract class Set private[crumbs] (val kps: KnownProperties*)
  private[crumbs] lazy val allKnownProperties = {
    Properties.values.map(_.asInstanceOf[KnownProperties#EnumeratedKey[_]]) | {
      ju.ServiceLoader.load(classOf[Set]).asScala.flatMap(_.kps).toSet.flatMap((_: KnownProperties).set)
    }
  }
  implicit val EnumeratedKeyOrdering: Ordering[KnownProperties#EnumeratedKey[_]] = Ordering.by(_.name)
}

//noinspection TypeAnnotation
object Properties extends KnownProperties {

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
      override def read(json: JsValue): ChainedID = {
        val (repr, depth, level, vuid) = json.convertTo[(String, Int, Int, String)]
        new ChainedID(repr, depth, level, vuid)
      }
      override def write(obj: ChainedID): JsValue = (obj.repr, obj.depth, obj.crumbLevel, obj.vertexId).toJson
    }

    implicit object KeyJsonFormat extends RootJsonFormat[Key[_]] {
      override def read(json: JsValue): Key[_] = stringToKey(json.convertTo[String], None)
      override def write(obj: Key[_]): JsValue = obj.toString.toJson
    }

    implicit val requestsStallInfoJsonFormat: RootJsonFormat[RequestsStallInfo] = jsonFormat3(RequestsStallInfo.apply)

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

  final case class Elem[A](k: Key[A], v: A) extends ElemOrElems {
    // "null" is not likely to be meaningful, but it's better than throwing an NPE now
    def toTuple: (String, JsValue) = (k.toString, if (null == v) "null".toJson else k.toJson(v))
  }

  // Utility wrapper for accumulating property lists - mostly useful because varargs and Seq are equivalent under erasure.
  // See GridProfilerUtils for an example.
  final class Elems(val m: List[Elem[_]]) extends ElemOrElems {
    def ++(o: TraversableOnce[Elem[_]]) = new Elems(m ++ o)
    def +(es: Elems) = new Elems(m ++ es.m)
    def :::(es: Elems) = new Elems(m ++ es.m)
    def :::(eso: Option[Elems]) = eso.fold(this)(es => new Elems(m ++ es.m))
    def ::(e: Elem[_]) = new Elems(e :: m)
    def +(e: Elem[_]) = new Elems(e :: m)
    def ::(eo: Option[Elem[_]]) = eo.fold(this)(e => new Elems(e :: m))
    def +(eo: Option[Elem[_]]) = eo.fold(this)(e => new Elems(e :: m))
    def toMap: Map[String, JsValue] = m.map(_.toTuple).toMap
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
        case (acc, es: Elems)  => es.m ::: acc
      })
  }

  trait Key[A] {
    def name: String
    @inline def ->(a: A) = Elem(this, a)
    @inline def :=(a: A) = Elem(this, a)
    def source: String
    def parse(js: JsValue): A
    def toJson(a: A): JsValue
    def maybe(b: Boolean, a: => A): Option[Elem[A]] = if (b) Some(Elem(this, a)) else None
    def maybe(p: A => Boolean, a: A): Option[Elem[A]] = if (p(a)) Some(Elem(this, a)) else None
  }

  class UntypedProperty(val name: String, src: Option[String]) extends Key[String] {
    val source: String = src.getOrElse("optimus")
    override def toString: String = src.fold(name)(_ + s":$name")
    override def parse(js: JsValue): String =
      try {
        js.convertTo[String]
      } catch {
        case _: Throwable => js.toString
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
  val engineRoot = prop[String]
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
  val dalReqUuid = prop[String]
  val tStarted = prop[ZonedDateTime]
  val tEnded = prop[ZonedDateTime]
  val nodeType = prop[CrumbNodeType.Value]
  val name = prop[String]
  val agents = prop[Seq[String]]
  val debug = prop[String]
  val priority = prop[String]
  val tasksExecutedOnEngine = propL

  val logLevel = prop[String]
  val logMsg = prop[String]
  val file = prop[String]
  val line = propI
  val clazz = prop[String]

  val gcCause = prop[String]
  val gcRatio = propD
  val gcTimeWindow = propI
  val gcUsedHeapBeforeGC = propD
  val gcUsedOldGenBeforeGC = propD
  val gcUsedHeapAfterGC = propD
  val gcUsedOldGenAfterCleanup = propD
  val gcMaxOldGen = propD
  val gcMaxHeap = propD
  val gcPools = prop[(Map[String, String], Map[String, String])]
  val gcBackOffAfter = propI
  val gcHeapOkRatio = propD
  val gcFinalizerCount = propI
  val gcFinalizerCountAfter = propI
  val gcFinalizerQueue = prop[String]
  val gcNative = prop[String]
  val gcNativeIndex = propI
  val gcNativeExplicitlyRequested = propB
  val gcNativeVersion = propI
  val gcNativePath = prop[String]
  val gcNativeEvicted = propL
  val gcNativeAllocation = propL
  val gcNativeAllocator = prop[String]
  val gcNativeWatermark = propL
  val gcNativeHighWatermark = propL
  val gcNativeEmergencyWatermark = propL
  val gcRSSLimit = propL
  val gcNativeInvocations = propL
  val gcNativeAllocAfter = propL
  val gcNativeHeapChange = propL // note, this is not the difference of the previous two
  val gcNativeJVMFootprint = propI
  val gcNativeJVMHeap = propI // different from GCMonitor properties in that it's not correlated with a GC
  val gcNativeAlloc = propI
  val gcNativeManagedAlloc = propI
  val gcNativeRSS = propI
  val gcNativeSurrogateRSS = propI
  val cacheClearCount = propL
  val gcNativeCacheClearCountGC = propL
  val gcNativeCacheClearCountMain = propL
  val gcNativeCacheClearCountGlobal = propL
  val gcNativeCacheClearCountGlobalCallbacks = propL
  val gcAction = prop[String]
  val gcName = prop[String]
  val gcDuration = propL
  val gcCacheRemoved = propI
  val gcCacheRemaining = propI
  val gcCleanupsFired = propL
  val gcMinorsSinceMajor = propL
  val gcCacheMemoryUsage = propL
  val gcNonCacheMemoryUsage = propL
  val gcMemoryLimit = propL

  val allocationName = prop[String]
  val allocationInfo = prop[String]
  val allocationEntriesCount = propL
  val allocationMemoryUsage = propL
  val allocationKeyspacesCount = propL

  val evictionTime = propL // eviction time in ns
  val expiredCount = propL
  val expiredMemory = propL
  val evictedCount = propL
  val evictedMemory = propL

  //
  val gcMinUsedHeapAfterGC = propD
  val gcMinUsedHeapBeforeGC = propD
  val gcMaxUsedHeapAfterGC = propD
  val gcMaxUsedHeapBeforeGC = propD
  val gcUsedOldGenAfterGC = propD
  val gcUsedHeapAfterCleanup = propD
  val gcNumMinor = propI
  val gcNumMajor = propI

  val clearCacheLevel = propL
  val triggerRatio = propD
  val includeSI = propB

  val profStarts = propL
  val profTimes = propL
  val profWallTime = propL
  val profGraphTime = propL
  val profSelfTime = propL
  val profGraphSpinTime = propL
  val profGraphCpuTime = propL
  val profGraphWaitTime = propL
  val profGraphIdleTime = propL
  val profDalReads = propL
  val profDalEntities = propL
  val profDalWrites = propL
  val profUnderUtilizedTime = propL
  val profCacheTime = propL
  val profDalRequestsDist = propI
  val profDalWritesDist = propI
  val profDalResultsDist = propI
  val profMaxHeap = propL
  val profCurrHeap = propL
  val profMaxNonHeap = propL
  val profCurrNonHeap = propL
  val profJvmCPUTime = propL
  val profJvmCPULoad = propD
  val profSysCPULoad = propD
  val profLoadAvg = propD
  val profGcStopTheWorld = propL
  val profGcTimeAll = propL
  val profGcCount = propL
  val profJitTime = propL
  val profClTime = propL
  val profEngineReuse = propI
  val profDistOverhead = propL
  val profDistOverheadAtEngine = propL
  val profDistTasks = propI
  val profNodeExecutionTime = propL
  val profThreads = prop[Map[String, Map[String, Long]]]
  val profMetricsDiff = prop[Map[String, Map[String, Array[Double]]]]
  val profSS = prop[Map[String, Array[String]]]
  val profEvictions = prop[Map[String, Long]]

  val profStallTime = propL
  val pluginInFlight = prop[Map[String, Long]]
  val pluginStarts = prop[Map[String, Long]]
  val pluginFullWaitTimes = prop[Map[String, Long]]
  val pluginStallTimes = prop[Map[String, Long]]
  val profDALBatches = propI
  val profDALCommands = propI
  val profGCDuration = propL
  val profGCMajorDuration = propL
  val profGCMinorDuration = propL
  val profGCMajors = propI
  val profGCMinors = propI
  val profCommitedMB = propL
  val profHeapMB = propL
  val profNonAuxBlockingTime = propL
  val profStartupEventTime = propL
  val profHandlerEventTime = propL

  val snapTimeMs = propL
  val snapTimeUTC = prop[String]
  val snapEpochId = propL
  val snapPeriod = propL
  val snapDuration = propL
  val snapBatch = propI
  val canonicalPub = propB

  val pulse = prop[Elems]

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
  val host = prop[String]
  val port = prop[String]
  val user = prop[String]
  val clusterName = prop[String]
  val server = prop[Map[String, JsValue]]
  val args = prop[Seq[String]]
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
  val appPrint = prop[String]
  val engPrint = prop[String]
  val invocationStyle = prop[String]
  val gsfControllerId = prop[String]
  val gsfEngineId = prop[String]
  val kubeNodeName = prop[String]
  val state = prop[String]

  val appLaunchContextType = prop[String]
  val appLaunchContextEnv = prop[String]
  val appLaunchContextName = prop[String]

  // DAL
  val broker = prop[Broker]("broker")
  val reqId = prop[String]
  val requestSummary = prop[RequestSummary]("req")
  val requestCommandLocations = prop[String]("RequestCommandLocations")
  val clientLatency = prop[String]("cltLat")

  val reason = prop[String]
  val requestsStallInfo = prop[RequestsStallInfo]

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
  val obtTaskExitCode = propI
  val obtTaskState = prop[String]
  val obtTaskTryCount = propI
  val obtTaskMaxTryCount = propI
  val obtUploadHost = prop[String]
  val obtUploadTargetDir = prop[String]

  // pgo group validation properties
  val pgoDiff = prop[Seq[Map[String, String]]]
  val optconfPath = prop[String]
  val optconfAction = prop[String]
  val optconfApplyTimeElapsed = propL
  val artifacts = prop[String]

  val hotspotEngine = prop[String]
  val hotspotStart = propL
  val hotspotEvicted = propL
  val hotspotInvalidated = propI
  val hotspotCacheHit = propL
  val hotspotCacheHitFromDifferentTasks = propL
  val hotspotCacheMiss = propL
  val hotspotCacheTime = propL
  val hotspotXsLookupTime = propL
  val hotspotNodeReusedTime = propL
  val hotspotCacheBenefit = propL
  val hotspotAvgCacheTime = propL
  val hotspotAncSelfTime = propL
  val hotspotPostCompleteTime = propL
  val hotspotWallTime = propL
  val hotspotSelfTime = propL
  val hotspotTweakLookupTime = propL
  val hotspotIsScenarioIndependent = prop[Option[Boolean]]
  val hotspotFavorReuse = prop[Option[Boolean]]
  val hotspotCacheable = prop[Option[Boolean]]
  val hotspotPropertyName = prop[String]
  val hotspotPropertyFlagsString = prop[String]
  val hotspotPropertyFlags = propL
  val hotspotRunningNode = prop[String]
  val hotspotPropertySource = prop[String]
  val hotspotCollisionCount = propI
  val hotspotChildNodeLookupCount = propI
  val hotspotChildNodeLookupTime = propL

  val smplEverything = propL
  val smplGraphTime = propL
  val smplSyncStackTime = propL
  val smplTweakLookupATime = propL
  val smplTweakLookupSTime = propL
  val smplCacheTime = propL
  val smplOHSampling = propL
  val smplOHInstrum = propL

  val profStats = prop[Map[String, String]]
  val profStatsType = prop[String]
  val profSummary = prop[Map[String, JsObject]]
  val profOpenedFiles = prop[Seq[String]]
  val miniGridMeta = prop[JsObject]
  val profStacks = prop[Seq[Elems]]
  val numStacksPublished = propL
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
  val stackThumb = prop[Map[String, Int]]
  val profPreOptimusStartup = propL

  val distWallTime = propL
  val distTaskDuration = propL
  val distTasksComplete = propI
  val distTasksRcvd = propI
  val distBytesRcvd = propL
  val distBytesSent = propL

  // Temporal surface tracing
  val tsQueryClassName = prop[String]
  val tsQueryType = prop[String]
  val tsTag = prop[String]

  val vt = prop[Instant]
  val tt = prop[Instant]
  val dmcClientSummary = prop[Map[String, Long]]

  val plugin = prop[String]

  /** givenOverlay use cases */
  val currentScenario = prop[String]
  val overlayScenario = prop[String]
  val trivialOverlay = propB

  /** XSFT cycle recovery (NodeTaskInfo name) */
  val xsftCycle = prop[String]
  val xsftStack = prop[String]

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

}

final case class RequestsStallInfo(pluginType: StallPlugin.Value, reqCount: Int, req: Seq[String]) {
  def take(n: Int): RequestsStallInfo = RequestsStallInfo(pluginType, reqCount, req.take(n))
}

object StallPlugin {
  type Value = String

  val None = "None"
  val DAL = "DAL"
  val Dist = "DIST"
  val DMC = "DMC"
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
