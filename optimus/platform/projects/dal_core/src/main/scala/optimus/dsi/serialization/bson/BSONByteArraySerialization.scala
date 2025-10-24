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
package optimus.dsi.serialization.bson

import java.time.Instant
import java.time._
import msjava.slf4jutils.scalalog._
import optimus.platform.pickling.ImmutableByteArray
import optimus.platform.dsi.bitemporal.DateTimeSerialization
import optimus.platform.pickling.PickledProperties
import optimus.utils.datetime.LocalDateOps
import optimus.utils.datetime.ZoneIds

import scala.collection.mutable

object BsonByteArraySerialization {

  private val logger = getLogger[BsonByteArraySerialization.type]
  private val debug = false

  val indexStrArray = (0 to 500).map(_.toString)

  def getIndexString(index: Int) = {
    if (index < indexStrArray.length) {
      indexStrArray(index)
    } else {
      index.toString
    }
  }

  def propertiesMapToBytes(properties: PickledProperties): Array[Byte] = {
    val ser = new BsonByteArraySerialization
    ser.writeData(properties.asMap)
    ser.out.toByteArray
  }

  def bytesToPropertiesMap(bytes: Array[Byte]): PickledProperties = {
    val handlerStackCallback = new HandlerStackCallback()
    val decode = new ThriftyBSONDecoder()
    decode.decode(bytes, handlerStackCallback)
    handlerStackCallback.properties
  }
}

class BsonByteArraySerialization {
  import BsonByteArraySerialization._

  val out = new ThriftyOutputBuffer()

  def writeData(properties: Map[String, Any]): Unit = {
    val marker = writeStartObj()
    val tor = properties.iterator
    while (tor.hasNext) {
      val (key, value) = tor.next()
      writeAny(key, value)
    }
    writeEndObject(marker)
  }

  def writeName(objType: Byte, name: String): Unit = {
    if (debug) logger.debug(s"write name $name of type $objType at ${out.getPosition}")
    out.write(objType)
    out.writeCString(name)
  }

  def writeBinaryProp(key: String, value: Array[Byte]): Unit = {
    writeName(BSONTypeIds.Binary, key)
    writeBinary(value)
  }

  def writeStringProp(key: String, value: String): Unit = {
    if (value == null)
      writeNull(key)
    else {
      writeName(BSONTypeIds.String, key)
      out.writeString(value)
    }
  }

  def writeBooleanProp(key: String, value: Boolean): Unit = {
    writeName(BSONTypeIds.Boolean, key)
    out.write(if (value) 0x1.asInstanceOf[Byte] else 0x0.asInstanceOf[Byte])
  }

  def writeLongProp(key: String, value: Long): Unit = {
    writeName(BSONTypeIds.NumberLong, key)
    out.writeLong(value)
  }

  def writeIntProp(key: String, value: Int): Unit = {
    writeName(BSONTypeIds.NumberInt, key)
    out.writeInt(value)
  }

  def writeDoubleProp(key: String, value: Double): Unit = {
    writeName(BSONTypeIds.Number, key)
    out.writeDouble(value)
  }

  def writeInstantProp(key: String, value: Instant): Unit = {
    writeLongProp(key, DateTimeSerialization.fromInstant(value))
  }

  def writeBinary(value: Array[Byte]): Unit = {
    out.writeInt(value.length)
    out.write(BSONTypeIds.BinGeneral)
    out.write(value)
  }

  def writeStartNamedObj(name: String): Int = {
    writeName(BSONTypeIds.Object, name)
    writeStartObj()
  }

  def writeStartObj(): Int = {
    val sizeMarker = out.getPosition
    if (debug) logger.debug(s"start object at $sizeMarker")
    out.writeInt(0) // will overwrite when done
    sizeMarker
  }

  def writeEndObject(sizeMarker: Int): Unit = {
    out.write(BSONTypeIds.Eoo)
    val size: Int = out.getPosition - sizeMarker
    out.writeInt(sizeMarker, size)
    if (debug) logger.debug(s"close object started at $sizeMarker of size $size at ${out.getPosition}")
  }

  def writeNull(key: String): Unit = {
    writeName(BSONTypeIds.Null, key)
  }

  def writeIterable(key: String, iter: Iterable[Any]): Unit = {
    if (debug) logger.debug(s"write iterable $key at ${out.getPosition}")
    writeName(BSONTypeIds.Array, key)
    val marker = writeStartObj()
    var index = 0
    val tor = iter.iterator
    while (tor.hasNext) {
      writeAny(getIndexString(index), tor.next())
      index += 1
    }
    writeEndObject(marker)
  }

  def writeDotDotDot(key: String, someStuff: Any*): Unit = {
    writeIterable(key, someStuff)
  }

  def writeMap(key: String, map: Map[String, _]): Unit = {
    out.write(BSONTypeIds.Object)
    out.writeCString(key)
    val marker = writeStartObj()
    val tor = map.iterator
    while (tor.hasNext) {
      val (subKey, subVal) = tor.next()
      writeAny(subKey, subVal)
    }
    writeEndObject(marker)
  }

  protected def throwForInvalidType(any: Any) =
    throw new RuntimeException(s"Cannot map type ${any.getClass.getName}: $any")

  def writeAny(key: String, any: Any): Unit = {
    any match {
      case x: Number =>
        any match {
          case x: Double => writeDoubleProp(key, x)
          case x: Int    => writeIntProp(key, x)
          case x: Long   => writeLongProp(key, x)
          case x: Byte   => writeIntProp(key, x.toInt)
          case x: Float  => writeDoubleProp(key, x.toDouble)
          case x: Short  => writeIntProp(key, x.toInt)
        }
      case x: String      => writeStringProp(key, x)
      case x: Boolean     => writeBooleanProp(key, x)
      case x: Array[Byte] => writeBinaryProp(key, x)

      case x: ImmutableByteArray => writeBinaryProp(key, x.data)
      case x: Option[_]          => if (x.isDefined) writeAny(key, x.get) else writeNull(key)
      case x: collection.Seq[_]  => writeIterable(key, x)
      case x: Map[_, _]          => writeMap(key, x.asInstanceOf[Map[String, Any]])
      case x: Set[_]             => writeIterable(key, x)
      case x: Product =>
        val buff = mutable.ListBuffer.empty[Any]
        buff.append(BinaryMarkers.Tuple)
        val iterator = x.productIterator
        while (iterator.hasNext) buff.append(iterator.next())
        writeIterable(key, buff)
      case x: LocalDate  => writeDotDotDot(key, BinaryMarkers.LocalDate, LocalDateOps.toModifiedJulianDay(x))
      case x: LocalTime  => writeDotDotDot(key, BinaryMarkers.LocalTime, x.toNanoOfDay)
      case x: OffsetTime => writeDotDotDot(key, BinaryMarkers.OffsetTime, x.toLocalTime.toNanoOfDay, x.getOffset.getId)
      case x: ZonedDateTime =>
        val asUtc = x.withZoneSameInstant(ZoneIds.UTC)
        val utcLocal = DateTimeSerialization.fromInstant(asUtc.toInstant)
        val zone = x.getZone.getId
        writeDotDotDot(key, BinaryMarkers.ZonedDateTime, utcLocal, zone)
      case x: ZoneId => writeDotDotDot(key, BinaryMarkers.ZoneId, x.getId)
      case x: Period =>
        writeDotDotDot(
          key,
          BinaryMarkers.Period,
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
      case _ => throwForInvalidType(any)
    }
  }

}
