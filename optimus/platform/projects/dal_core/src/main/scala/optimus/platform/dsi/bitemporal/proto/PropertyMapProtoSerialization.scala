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
package optimus.platform.dsi.bitemporal.proto

import optimus.utils.datetime.ZoneIds

import scala.jdk.CollectionConverters._
import scala.collection.Map
import scala.collection.Seq
import com.google.protobuf.ByteString

import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetTime
import java.time.Period
import java.time.ZoneId
import java.time.ZonedDateTime
import optimus.utils.datetime._
import optimus.platform.pickling.ImmutableByteArray
import optimus.platform.dsi.bitemporal.proto.Dsi.FieldProto
import optimus.platform.dsi.bitemporal.DateTimeSerialization
import optimus.platform.storable._

import java.time.ZoneOffset
import net.iharder.Base64
import optimus.core.CoreHelpers

object ProtoPickleSerializer {
  // The original implementation serializes Map[String, Any] to a list of values and a list of keys.
  // In this way, the serialization of value is independent to the keys and serialization becomes recursive.
  // However, this cut off the connection between keys and the associated value, and it becomes impossible
  // to filter proto messages on CPS based on the properties map.
  // To fix this, the better solution would be serializing Map[String, Any] to a list of (String, Any)
  // tuples. However, the result format will be incompatible with the old one, and we're not able to migrate
  // to new implementation (unless we can update all clients/servers to the same code base at the same time).
  //
  // Here's a fix so that the serialization of Map values will carry the associatedKey. This way, the new
  // format is compatible with old ones, however, we need to change the method signature and apply different
  // arguments depending on the calling context:
  //     serialize normal values <=> associatedKey == None
  //     serialize Map values    <=> associatedKey == the associated key of that value
  def propertiesToProto(o: Any, associatedKey: Option[String] = None): FieldProto = ??? /* {
    val builder = FieldProto.newBuilder
    if (associatedKey.isDefined) {
      builder.setAssociatedKey(associatedKey.get)
    }

    o match {
      case x: Int                => builder.setType(FieldProto.Type.INT).setIntValue(x)
      case x: String             => builder.setType(FieldProto.Type.STRING).setStringValue(x)
      case x: Double             => builder.setType(FieldProto.Type.DOUBLE).setDoubleValue(x)
      case x: Boolean            => builder.setType(FieldProto.Type.BOOLEAN).setBoolValue(x)
      case x: Char               => builder.setType(FieldProto.Type.CHAR).setIntValue(x.toInt)
      case x: Long               => builder.setType(FieldProto.Type.LONG).setLongValue(x)
      case x: Short              => builder.setType(FieldProto.Type.SHORT).setIntValue(x.toInt)
      case x: Byte               => builder.setType(FieldProto.Type.BYTE).setIntValue(x)
      case x: Array[Byte]        => builder.setType(FieldProto.Type.BLOB).setBlobValue(ByteString.copyFrom(x))
      case x: ImmutableByteArray => builder.setType(FieldProto.Type.BLOB).setBlobValue(ByteString.copyFrom(x.data))
      case x: Seq[_] =>
        val seq = (x map (propertiesToProto(_, None))).asJava
        builder.setType(FieldProto.Type.SEQ).addAllChildren(seq)
      case x: Map[_, _] =>
        val values = (x.map(v => propertiesToProto(v._2, Some(v._1.asInstanceOf[String]))).toSeq).asJava
        val keys = x.map(k => k._1.asInstanceOf[String]).toSeq.asJava
        // TODO (OPTIMUS-13435): When we serialize map values, we serialize the associated key, too.
        // So the value serialization contains sufficient information about the map. For backward compatibility, we still serialize the
        // keys with addAllFields. When all the clients have upgraded to the new code base, we need to remove the redundant key fields.
        builder.setType(FieldProto.Type.MAP).addAllChildren(values).addAllFields(keys)
      case (k: String, v: Any) =>
        builder.setType(FieldProto.Type.TUPLE2).setStringValue(k).addChildren(propertiesToProto(v))
      case x: FinalTypedReference =>
        builder
          .setType(FieldProto.Type.ENTITY_REF)
          .setStringValue(Base64.encodeBytes(x.data))
          .setBlobValue(ByteString.copyFrom(x.data))
          .setIntValue(x.typeId)
      case x: FinalReference =>
        // TODO (OPTIMUS-13436): string value is used for filtering on CPS, when all clients have
        // upgraded to support string entity ref, we need to remove blob value from the representation
        builder
          .setType(FieldProto.Type.ENTITY_REF)
          .setStringValue(Base64.encodeBytes(x.data))
          .setBlobValue(ByteString.copyFrom(x.data))
      case x: TemporaryReference =>
        // NB TemporaryReference should never be used for filtering on CPS so we should only ever need to serialize to a blob value here
        builder.setType(FieldProto.Type.TEMPORARY_ENTITY_REF).setBlobValue(ByteString.copyFrom(x.data))
      case x: TypedBusinessEventReference =>
        builder
          .setType(FieldProto.Type.BUSINESS_EVENT_REF)
          .setBlobValue(ByteString.copyFrom(x.data))
          .setIntValue(x.typeId)
      case x: BusinessEventReference =>
        builder.setType(FieldProto.Type.BUSINESS_EVENT_REF).setBlobValue(ByteString.copyFrom(x.data))
      case x: VersionedReference =>
        builder.setType(FieldProto.Type.VERSIONED_REF).setBlobValue(ByteString.copyFrom(x.data))
      case ModuleEntityToken(className) =>
        builder.setType(FieldProto.Type.MODULE_ENTITY_TOKEN).setStringValue(className)
      case x: LocalDate => builder.setType(FieldProto.Type.LOCAL_DATE).setLongValue(LocalDateOps.toModifiedJulianDay(x))
      case x: LocalTime => builder.setType(FieldProto.Type.LOCAL_TIME).setLongValue(x.toNanoOfDay)
      case x: OffsetTime =>
        builder
          .setType(FieldProto.Type.OFFSET_TIME)
          .setStringValue(x.getOffset.getId)
          .setLongValue(x.toLocalTime.toNanoOfDay)
      case x: ZonedDateTime =>
        val asUtc = x.withZoneSameInstant(ZoneIds.UTC)
        val utcLocal = DateTimeSerialization.fromInstant(asUtc.toInstant)
        val zone = x.getZone.getId
        builder.setType(FieldProto.Type.ZONE_DATE_TIME).setLongValue(utcLocal).setStringValue(zone)
      case x: ZoneId => builder.setType(FieldProto.Type.ZONE_ID).setStringValue(x.getId)
      case x: Period =>
        builder
          .setType(FieldProto.Type.PERIOD)
          .setPeriodValue(
            PeriodProto.newBuilder
              .setYears(x.getYears)
              .setMonths(x.getMonths)
              .setDays(x.getDays)
              // java.time.Period only supports day fields
              // below is vestiges of us starting with JSR310
              // classes, back in the day.
              .setHours(0)
              .setMins(0)
              .setSecs(0)
              .setNanos(0)
              .build)
      case null  => builder.setType(FieldProto.Type.NULL)
      case other => throw new IllegalArgumentException(s"Cannot serialize ${other.getClass}")
    }

    builder.build
  } */

  private final case class MapEntry(key: String, value: Any)

  def protoToProperties(proto: FieldProto): Any = ??? /* {
    val value = proto.getType match {
      case FieldProto.Type.INT     => proto.getIntValue
      case FieldProto.Type.STRING  => proto.getStringValue
      case FieldProto.Type.DOUBLE  => proto.getDoubleValue
      case FieldProto.Type.BOOLEAN => proto.getBoolValue
      case FieldProto.Type.CHAR    => proto.getIntValue.toChar
      case FieldProto.Type.LONG    => proto.getLongValue
      case FieldProto.Type.SHORT   => proto.getIntValue.toShort
      case FieldProto.Type.BYTE    => proto.getIntValue.toByte
      case FieldProto.Type.BLOB    => ImmutableByteArray(proto.getBlobValue.toByteArray)
      case FieldProto.Type.SEQ     => proto.getChildrenList.asScala.iterator.map { protoToProperties }.toList: List[Any]
      case FieldProto.Type.MAP => {
        // updated to keep the collection as a Java collection rather than tupling and mapping to avoid intermediate collections
        // this is a hot method wrt allocations.

        // TODO (OPTIMUS-13435): The original serialization of Map serializes the values and keys
        // independently. The new implementation serialize the key into the associated value. When all clients have upgraded to new
        // implementation, we should remove the legacy support.
        val rawValues = proto.getChildrenList
        if (rawValues.isEmpty) {
          Map.empty
        } else
          protoToProperties(rawValues.get(0)) match {
            // new protocol format
            case newFormat: MapEntry =>
              var result = collection.immutable.Map.empty[String, Any]
              result = result.updated(newFormat.key.intern, newFormat.value)
              // and then the rest of the list
              rawValues.subList(1, rawValues.size()).forEach { raw =>
                val converted = protoToProperties(raw).asInstanceOf[MapEntry]
                result = result.updated(converted.key.intern, converted.value)
              }

              result
            case oldFormat => // legacy protocol format
              val values = rawValues.subList(1, rawValues.size()).asScala map protoToProperties
              values.+=:(oldFormat)
              val keys = proto.getFieldsList.asScala.map(_.intern)
              (keys.iterator zip values.iterator).toIndexedSeq
          }
      }
      case FieldProto.Type.TUPLE2     => (proto.getStringValue, protoToProperties(proto.getChildren(0)))
      case FieldProto.Type.ENTITY_REF =>
        // TODO (OPTIMUS-13436): string representation is used for filtering on CPS in DAL notification
        // when all the clients have upgraded to support string entity reference, we need to remove the redundant blob support
        val data =
          if (proto.hasStringValue) RawReference.stringToBytes(proto.getStringValue) else proto.getBlobValue.toByteArray
        val typeOpt = if (proto.hasIntValue) Some(proto.getIntValue) else None
        EntityReference(data, typeOpt)
      case FieldProto.Type.VERSIONED_REF =>
        VersionedReference(proto.getBlobValue.toByteArray)
      case FieldProto.Type.TEMPORARY_ENTITY_REF =>
        require(proto.hasBlobValue)
        EntityReference.temporary(proto.getBlobValue.toByteArray)

      case FieldProto.Type.BUSINESS_EVENT_REF =>
        val data = proto.getBlobValue.toByteArray
        val typeOpt = if (proto.hasIntValue) Some(proto.getIntValue) else None
        BusinessEventReference(data, typeOpt)
      case FieldProto.Type.MODULE_ENTITY_TOKEN => ModuleEntityToken(proto.getStringValue)
      case FieldProto.Type.LOCAL_DATE          => LocalDateOps.ofModifiedJulianDay(proto.getLongValue)
      case FieldProto.Type.LOCAL_TIME          => LocalTime.ofNanoOfDay(proto.getLongValue)
      case FieldProto.Type.OFFSET_TIME =>
        OffsetTime.of(LocalTime.ofNanoOfDay(proto.getLongValue), ZoneOffset.of(proto.getStringValue))
      case FieldProto.Type.ZONE_DATE_TIME =>
        val local = DateTimeSerialization.toInstant(proto.getLongValue)
        val zone = CoreHelpers.getZoneID(proto.getStringValue)
        ZonedDateTime.ofInstant(local, zone)
      case FieldProto.Type.ZONE_ID => CoreHelpers.getZoneID(proto.getStringValue)
      case FieldProto.Type.PERIOD =>
        val p = proto.getPeriodValue
        Period.of(p.getYears, p.getMonths, p.getDays)
      case FieldProto.Type.NULL => null
      case _                    => throw new IllegalArgumentException(s"Cannot deserialize ${proto.getType}")
    }

    if (proto.hasAssociatedKey)
      MapEntry(proto.getAssociatedKey, value)
    else
      value
  } */
}
