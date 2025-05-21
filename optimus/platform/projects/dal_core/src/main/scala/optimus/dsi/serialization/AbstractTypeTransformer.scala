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
package optimus.dsi.serialization

import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetTime
import java.time.Period
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.zone.ZoneRulesException

import optimus.dsi.util.HashMap7
import optimus.platform.pickling.ImmutableByteArray
import optimus.platform.storable.BusinessEventReference
import optimus.platform.storable.EntityReference
import optimus.platform.storable.FinalReference
import optimus.platform.storable.ModuleEntityToken
import optimus.platform.storable.TemporaryReference
import optimus.platform.storable.VersionedReference
import optimus.platform.dsi.bitemporal.DateTimeSerialization
import optimus.utils.datetime.LocalDateOps
import optimus.utils.datetime.ZoneIds

import scala.jdk.CollectionConverters._
import scala.collection.immutable
import scala.collection.mutable
import scala.collection.{Map => ScalaMap}
import scala.reflect.ClassTag

/*
 * Type Markers for Serialization
 */
abstract class TypeMarkers[T] {
  val Tuple: T
  val FinalRef: T
  val VersionedRef: T
  val BusinessEventRef: T
  val LocalDate: T
  val LocalTime: T
  val ModuleEntityToken: T
  val OffsetTime: T
  val ZonedDateTime: T
  val ZoneId: T
  val Period: T
  val ByteArray: T
}

object JavaCollectionUtils {
  import java.{util => ju}

  def mapToArrayList[A, B](f: A => B, l: ju.Collection[A]): ju.ArrayList[B] = {
    val length = l.size
    val output = new ju.ArrayList[B](length)
    val it = l.iterator
    while (it.hasNext) {
      output.add(f(it.next))
    }
    output
  }

  def mapToArrayList[A, B](f: A => B, l: Iterable[A]): ju.ArrayList[B] = {
    val length = l.size
    val output = new ju.ArrayList[B](length)
    val it = l.iterator
    while (it.hasNext) {
      output.add(f(it.next()))
    }
    output
  }

  def mkArrayList(xs: Any*) = {
    val output = new ju.ArrayList[Any](xs.size)
    xs foreach output.add
    output
  }

}

object BinaryConversions {
  def byteBufferToReferenceArray(bb: ByteBuffer) = {
    val refBuf = new Array[Byte](16)
    bb.get(refBuf)
    refBuf
  }
}

// This is needed because of the AnyRef ObjectIds in IndexEntrys
trait ObjectIdSerializer {
  def serialize(ref: AnyRef): String
  def deserialize(x: String): AnyRef
}

/*
 * The actual TypeTransformer
 */

trait AbstractTypeTransformerOps[T] {
  import java.{util => ju}

  import JavaCollectionUtils._
  protected implicit val tTag: ClassTag[T]

  /*
   * Abstract class members
   */
  val TypeMarkers: TypeMarkers[T]
  protected def isMarker(expected: T, actual: Any): Boolean

  private[optimus] def toFinalEntityReference(o: T, t: Option[Int]): FinalReference
  private[optimus] def toBusinessEventReference(o: T, t: Option[Int]): BusinessEventReference
  private[optimus] def toVersionedReference(o: T): VersionedReference

  private[optimus] def fromFinalEntityReference(e: FinalReference): ju.ArrayList[Any]
  private[optimus] def fromBusinessEventReference(be: BusinessEventReference): ju.ArrayList[Any]

  protected def toByteArray(s: Iterator[Any]): ImmutableByteArray
  protected def fromByteArray(b: Array[Byte]): AnyRef

  protected def fromLong(l: Long): AnyRef
  protected def toLong(a: Any): Long

  def serialize(o: Any): AnyRef = {
    o match {
      case x: ImmutableByteArray        => serialize(x.data)
      case x: Option[_]                 => x.map(serialize(_)).getOrElse(null)
      case x: collection.Seq[_]         => mapToArrayList(serialize _, x)
      case x: immutable.SortedMap[_, _] => toJavaOrderedMap(x.asInstanceOf[immutable.SortedMap[String, Any]])
      case x: Map[_, _]                 => toJavaMap(x.asInstanceOf[Map[String, Any]])
      case x: Set[_]                    => toJavaSet(x)
      case ModuleEntityToken(className) => mkArrayList(TypeMarkers.ModuleEntityToken, className)
      case x: Product => {
        val ct = x.productArity
        val output = new ju.ArrayList[Any](ct + 1)
        output.add(TypeMarkers.Tuple)

        var i = 0
        while (i < ct) {
          output.add(serialize(x.productElement(i)))
          i += 1
        }
        output
      }
      case x: TemporaryReference     => throw new TypeTransformException("Temporary references are not allowed here.")
      case x: FinalReference         => fromFinalEntityReference(x)
      case x: BusinessEventReference => fromBusinessEventReference(x)
      case x: LocalDate              => mkArrayList(TypeMarkers.LocalDate, fromLocalDate(x))
      case x: LocalTime              => mkArrayList(TypeMarkers.LocalTime, fromLocalTime(x))
      case x: OffsetTime => mkArrayList(TypeMarkers.OffsetTime, x.toLocalTime.toNanoOfDay, x.getOffset.getId)
      case x: ZonedDateTime => {
        val asUtc = x.withZoneSameInstant(ZoneIds.UTC)
        val utcLocal = DateTimeSerialization.fromInstant(asUtc.toInstant)
        val zone = x.getZone.getId
        mkArrayList(TypeMarkers.ZonedDateTime, utcLocal, zone)
      }
      case x: ZoneId => mkArrayList(TypeMarkers.ZoneId, x.getId)
      case x: Period =>
        mkArrayList(
          TypeMarkers.Period,
          x.getYears,
          x.getMonths,
          x.getDays,
          // java.time.Period only supports day fields
          // below is vestiges of us starting with JSR310
          // classes, back in the day.
          0,
          0,
          0,
          0
        )
      case b: Array[Byte] => fromByteArray(b)
      case l: Long        => fromLong(l)
      case _              => o.asInstanceOf[AnyRef]
    }
  }

  def deserialize(o: Any): Any = o match {
    case jl: java.util.List[_]   => fromSeq(jl.iterator.asScala)
    case l: List[_]              => fromSeq(l.iterator)
    case b: mutable.Buffer[_]    => fromSeq(b.iterator)
    case jm: java.util.Map[_, _] => fromJavaMap(jm.asInstanceOf[java.util.Map[String, _]])
    case sm: ScalaMap[_, _]      => fromScalaMap(sm.asInstanceOf[ScalaMap[String, _]])
    case x                       => x
  }

  protected def fromScalaMap(sm: ScalaMap[String, _]): ScalaMap[String, Any] = {
    val out = immutable.HashMap.newBuilder[String, Any]
    sm.foreach { k =>
      out += (k._1 -> deserialize(k._2))
    }
    out.result()

  }

  protected def fromJavaMap(jm: java.util.Map[String, _]): immutable.Map[String, Any] = {
    val it = jm.entrySet.iterator
    val bld = Map.newBuilder[String, Any]
    while (it.hasNext) {
      val n = it.next
      bld += (n.getKey -> deserialize(n.getValue))
    }
    bld.result()
  }

  private def fromSeq(s: Iterator[Any]): Any = {
    if (s.isEmpty)
      return Nil
    val marker = s.next()

    if (isMarker(TypeMarkers.Tuple, marker))
      s.map(deserialize).toSeq
    else if (isMarker(TypeMarkers.FinalRef, marker))
      toFinalEntityRef(s)
    else if (isMarker(TypeMarkers.VersionedRef, marker))
      toVersionedRef(s)
    else if (isMarker(TypeMarkers.BusinessEventRef, marker))
      toBusinessEventRef(s)
    else if (isMarker(TypeMarkers.ModuleEntityToken, marker))
      ModuleEntityToken(s.next().asInstanceOf[String])
    else if (isMarker(TypeMarkers.LocalDate, marker))
      toLocalDate(s)
    else if (isMarker(TypeMarkers.LocalTime, marker))
      toLocalTime(s)
    else if (isMarker(TypeMarkers.OffsetTime, marker))
      toOffsetTime(s)
    else if (isMarker(TypeMarkers.ZonedDateTime, marker))
      toZonedDateTime(s)
    else if (isMarker(TypeMarkers.ZoneId, marker))
      toZoneId(s)
    else if (isMarker(TypeMarkers.Period, marker))
      toPeriod(s)
    else if (isMarker(TypeMarkers.ByteArray, marker))
      toByteArray(s)
    else
      deserialize(marker) :: (s map deserialize).toList

  }

  protected def toJavaMap(m: Map[String, Any]): java.util.Map[String, Any] = {
    // We have to use Java 7's HashMap7 implementation as hash calculation of entity non-unique index
    // (and others) depends upon it, and Java 8's implementation changes the iteration order of the map
    // as compared to Java 7.
    //
    // TODO (OPTIMUS-14952): Revert this patch once we solve the ordering issue while
    // calculating hash for non-unique indexes.
    val output = new HashMap7[String, Any](m.size)
    m foreach { ent =>
      output.put(ent._1.asInstanceOf[String], serialize(ent._2))
    }
    output
  }

  protected def toJavaOrderedMap(m: immutable.SortedMap[String, Any]): java.util.SortedMap[String, Any] = {
    val output = new ju.TreeMap[String, Any]()
    m foreach { ent =>
      output.put(ent._1.asInstanceOf[String], serialize(ent._2))
    }
    output
  }

  protected def toJavaSet[T](s: Set[T]): java.util.Set[T] =
    s.asJava

  // Pickled entity refs may be new format: Array(typeId: Int, data: Array[Byte]), majority format Array(data: Array[Byte]) or really old format: Array(typeName: String, data: Array[Byte])
  protected def toFinalEntityRef(s: Iterator[Any]): FinalReference =
    s.toList match {
      case (data: T) :: Nil                       => toFinalEntityReference(data, None)
      case (typeId: Int) :: (data: T) :: Nil      => toFinalEntityReference(data, Some(typeId))
      case (typeName: String) :: (data: T) :: Nil => toFinalEntityReference(data, None)
      case other => throw new TypeTransformException(s"Cannot get EntityReference from ${other.mkString(",")}")
    }

  protected def toVersionedRef(s: Iterator[Any]): VersionedReference =
    s.toList match {
      case (data: T) :: Nil => toVersionedReference(data)
      case other => throw new TypeTransformException(s"Cannot get VersionedReference from ${other.mkString(",")}")
    }

  protected def toBusinessEventRef(s: Iterator[Any]): BusinessEventReference =
    s.toList match {
      case (data: T) :: Nil                       => toBusinessEventReference(data, None)
      case (typeId: Int) :: (data: T) :: Nil      => toBusinessEventReference(data, Some(typeId))
      case (typeName: String) :: (data: T) :: Nil => toBusinessEventReference(data, None)
      case other => throw new TypeTransformException(s"Cannot get BusinessEventReference from ${other.mkString(",")}")
    }

  private[optimus] def fromLocalDate(date: LocalDate): Long = LocalDateOps.toModifiedJulianDay(date)
  private def toLocalDate(s: Iterator[Any]): LocalDate = {
    val ret = LocalDateOps.ofModifiedJulianDay(toLong(s.next()))

    if (s.hasNext)
      throw new TypeTransformException(s"Cannot get LocalDate from $s")

    ret
  }

  private[optimus] def fromLocalTime(time: LocalTime): Long = time.toNanoOfDay
  private def toLocalTime(s: Iterator[Any]): LocalTime = {
    val ret = LocalTime.ofNanoOfDay(toLong(s.next()))
    if (s.hasNext)
      throw new TypeTransformException(s"Cannot get LocalTime from $s")

    ret
  }

  private def toOffsetTime(s: Iterator[Any]): OffsetTime = {
    val ret =
      OffsetTime.of(LocalTime.ofNanoOfDay(s.next().asInstanceOf[Long]), ZoneOffset.of(s.next().asInstanceOf[String]))

    if (s.hasNext)
      throw new TypeTransformException(s"Cannot get OffsetTime from $s")

    ret
  }

  private def toZonedDateTime(s: Iterator[Any]): ZonedDateTime = {
    val local = DateTimeSerialization.toInstant(s.next().asInstanceOf[Long])
    val zone = toZoneId(s.next())
    val ret = ZonedDateTime.ofInstant(local, zone)

    if (s.hasNext)
      throw new TypeTransformException(s"Cannot get ZonedDateTime from $s")

    ret
  }

  protected def toZoneId(s: Iterator[Any]): ZoneId = {
    val ret = ZoneId.of(s.next().asInstanceOf[String])

    if (s.hasNext)
      throw new TypeTransformException(s"Cannot get ZonedId from $s")

    ret
  }

  private def toPeriod(s: Iterator[Any]): Period = {

    val years = s.next().asInstanceOf[Int]
    val months = s.next().asInstanceOf[Int]
    val days = s.next().asInstanceOf[Int]

    // java.time.Period only supports day fields
    // but stored format was based on JSR310 Period
    // which supported the time fields.
    // We need skip 4 fields
    s.next() // hours
    s.next() // mins
    s.next() // secs
    s.next() // nanos

    val ret = Period.of(years, months, days)

    if (s.hasNext)
      throw new TypeTransformException(s"Cannot get Period from $s")

    ret
  }

  private def toZoneId(o: Any): ZoneId = {
    try {
      val zone = o.asInstanceOf[String]
      ZoneId.of(zone)
    } catch {
      case _: ZoneRulesException => ZoneId.of(o.asInstanceOf[String], ZoneId.SHORT_IDS)
      case _: ClassCastException => throw new TypeTransformException(s"Cannot get ZoneId from $o")
    }
  }

}

class TypeTransformException(error: String) extends Exception(error)

trait TypedReferenceAwareTransformer[T] {
  self: AbstractTypeTransformerOps[T] =>
  import java.{util => ju}

  import JavaCollectionUtils._
  private[optimus] def fromFinalEntityReference(e: FinalReference): ju.ArrayList[Any] = {
    e.getTypeId
      .map(id =>
        // Place the type id in front of the data, so that older versions of the code can still read the blobs
        mkArrayList(TypeMarkers.FinalRef, id, e.data))
      .getOrElse(mkArrayList(TypeMarkers.FinalRef, e.data))
  }
  private[optimus] def fromBusinessEventReference(be: BusinessEventReference): ju.ArrayList[Any] = {
    be.getTypeId
      .map(id =>
        // Place the type id in front of the data, so that older versions of the code can still read the blobs
        mkArrayList(TypeMarkers.BusinessEventRef, id, be.data))
      .getOrElse(mkArrayList(TypeMarkers.BusinessEventRef, be.data))
  }
}

trait ReferenceTransformer[T] {
  self: AbstractTypeTransformerOps[T] =>
  import java.{util => ju}

  import JavaCollectionUtils._
  private[optimus] def fromFinalEntityReference(e: FinalReference): ju.ArrayList[Any] = {
    mkArrayList(TypeMarkers.FinalRef, e.data)
  }
  private[optimus] def fromBusinessEventReference(be: BusinessEventReference): ju.ArrayList[Any] = {
    mkArrayList(TypeMarkers.BusinessEventRef, be.data)
  }
}
