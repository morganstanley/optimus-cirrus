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
import optimus.platform.dsi.bitemporal.proto.Dsi.PeriodProto
import optimus.platform.dsi.bitemporal.DateTimeSerialization
import optimus.platform.storable._

import java.time.ZoneOffset
import net.iharder.base64.Base64
import optimus.core.CoreHelpers
import optimus.graph.Settings
import optimus.platform.pickling.PicklingConstants
import optimus.platform.pickling.Shape
import optimus.platform.pickling.SlottedBufferAsEmptySeq

import java.util
import scala.collection.immutable.ArraySeq

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
  def propertiesToProto(o: Any, associatedKey: Option[String] = None): FieldProto = {
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
      case x: collection.Seq[_] =>
        val seq = (x map (propertiesToProto(_, None))).asJava
        builder.setType(FieldProto.Type.SEQ).addAllChildren(seq)
      case x: Map[_, _] =>
        val values = x.map(v => propertiesToProto(v._2, Some(v._1.asInstanceOf[String]))).toSeq.asJava
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
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////
  // ProtoBuf -> Proto Section
  //
  // The code int his section deliberately uses while loops, avoid boxing and uses mutation to
  // avoid allocations as this is a hot path for client processes.
  //
  // The main complexities in this area are as follows:
  //
  // [1]- We want to handle SEQ and MAP protobuf types rather specifically:
  //   - For MAP, we use SlottedBufferAsMap as the pickled form (instead of a Map)
  //   - For SEQs that have low number of entries (below Settings.slottedBufferForSeqThreshold) we
  //   use SlottedBufferAsSeq which holds the members as individual fields rather than inside of an
  //   array.
  //   - For SEQs that have higher number of entries, we use ArraySeq instances. Here, we are careful
  //   to use ArraySeq.ofInt, ArraySeq.ofDouble, etc. to avoid boxing of the primitive types where we can.
  //
  // [2]- We want to intern the values of fields of SlottedBufferAsMap. The interning behaviour is driven by
  // per-property stats maintained in the Shape class. Based on these stats we may deem interning unfruitful for a
  // given field and turn it off.
  //
  private final case class MapEntry(key: String, value: Any)

  private def compareTypes(o1: FieldProto.Type, o2: FieldProto.Type) = {
    def mappedType(t: FieldProto.Type) = {
      t match {
        case FieldProto.Type.INT     => 0
        case FieldProto.Type.DOUBLE  => 1
        case FieldProto.Type.BOOLEAN => 2
        case FieldProto.Type.CHAR    => 3
        case FieldProto.Type.LONG    => 4
        case FieldProto.Type.SHORT   => 5
        case FieldProto.Type.BYTE    => 6
        case _                       => 7
      }
    }
    mappedType(o1).compare(mappedType(o2))
  }

  // Takes a MAP or SEQ FieldProto instance and creates a SlottedBufferAsMap or SlottedBufferAsSeq
  // with it.
  private def protoToSlottedBuffer(proto: FieldProto, isMap: Boolean): Any = {
    val rawValues: util.List[FieldProto] = proto.getChildrenList
    val sortedList = new util.ArrayList(rawValues)
    var tag: String = Shape.NoTag
    if (sortedList.isEmpty) {
      if (isMap) Map.empty else Seq.empty
    } else {
      var i = 0
      var tagIndex = -1
      var keys: Array[String] = null
      var valuesSize = sortedList.size()
      if (isMap) {
        // For maps, we need to sort the FieldProtos by type and also find the tag if there is one
        // (embeddables have a tag property)
        sortedList.sort((o1: FieldProto, o2: FieldProto) => compareTypes(o1.getType, o2.getType))
        while (i < sortedList.size() && tagIndex == -1) {
          val currField = sortedList.get(i)
          if (
            currField.getType == FieldProto.Type.STRING &&
            currField.getAssociatedKey == PicklingConstants.embeddableTag
          ) {
            tagIndex = i
            tag = currField.getStringValue
            valuesSize = sortedList.size() - 1
          }
          i += 1
        }
        // For MAP, we have keys. This will hold them:
        keys = new Array[String](valuesSize)
      }

      // Fill in keys (if applicable), types to create the shape
      def getValueFor(field: FieldProto, tpe: Class[_]): Any = {
        if (tpe.isPrimitive) {
          // [SEE_FIELDPROTO_FOR_PRIMITIVES]
          // The generated SlottedBuffer classes take an Object[] as we need a somewhat generic ctor signature
          // across SlottedBuffers of all shapes. On the other-hand we want to avoid boxing of primitive types
          // so what we do here is to just use the FieldProto as the item in that array and we let the generated
          // code call the appropriate type-specific accessor of the FieldProto class to get the value without
          // boxing.
          field
        } else {
          // For object types, we get the actual values and
          // set the value into the array directly. The SlottedBuffers
          // will just use the value directly.
          protoToProperties(field) match {
            case MapEntry(_, v) => v
            case other          => other
          }
        }
      }

      val types = new Array[Class[_]](valuesSize)
      val values = new Array[Any](valuesSize)
      var srcI = 0
      var destI = 0
      while (srcI < sortedList.size) {
        if (srcI != tagIndex) {
          val rv = sortedList.get(srcI)
          if (isMap) keys(destI) = rv.getAssociatedKey.intern
          types(destI) = rv.getType match {
            case FieldProto.Type.INT     => classOf[Int]
            case FieldProto.Type.DOUBLE  => classOf[Double]
            case FieldProto.Type.BOOLEAN => classOf[Boolean]
            case FieldProto.Type.CHAR    => classOf[Char]
            case FieldProto.Type.LONG    => classOf[Long]
            case FieldProto.Type.SHORT   => classOf[Short]
            case FieldProto.Type.BYTE    => classOf[Byte]
            case _                       => classOf[AnyRef]
          }
          values(destI) = getValueFor(rv, types(destI))
          destI += 1
        }
        srcI += 1
      }

      val shape = Shape(keys, types, tag)

      if (!isMap)
        shape.createInstanceAsSeq(values)
      else
        shape.createInstanceAsMap(values)
    }
  }

  // Takes a SEQ FieldProto and creates a ArraySeq with it. Implementation is quite
  // repetitive because we want to use the ArraySeq.of* classes to avoid boxing
  // of primitive types and uses while loops to avoid closure allocations in hot code
  // like this.
  private def protoToArraySeq(proto: FieldProto): ArraySeq[_] = {
    // We'll walk the values once to see if they are homogenous primitive types...
    var fType: FieldProto.Type = null
    var i = 0
    while (i < proto.getChildrenCount && fType != FieldProto.Type.NULL) {
      val currType = proto.getChildren(i).getType
      if (fType eq null)
        fType = currType
      else if (currType != fType) {
        fType = FieldProto.Type.NULL // We'll use this to mean mixed-type.
      }
      i += 1
    }
    // ... and a second time to create the correct type of array and set the values
    val seq = fType match {
      case FieldProto.Type.INT =>
        val values = new Array[Int](proto.getChildrenCount)
        i = 0
        while (i < proto.getChildrenCount) {
          values(i) = getIntValue(proto.getChildren(i))
          i += 1
        }
        new ArraySeq.ofInt(values)
      case FieldProto.Type.DOUBLE =>
        val values = new Array[Double](proto.getChildrenCount)
        i = 0
        while (i < proto.getChildrenCount) {
          values(i) = getDoubleValue(proto.getChildren(i))
          i += 1
        }
        new ArraySeq.ofDouble(values)
      case FieldProto.Type.BOOLEAN =>
        val values = new Array[Boolean](proto.getChildrenCount)
        i = 0
        while (i < proto.getChildrenCount) {
          values(i) = getBoolValue(proto.getChildren(i))
          i += 1
        }
        new ArraySeq.ofBoolean(values)
      case FieldProto.Type.CHAR =>
        getCharValue(proto)
        val values = new Array[Char](proto.getChildrenCount)
        i = 0
        while (i < proto.getChildrenCount) {
          values(i) = getCharValue(proto.getChildren(i))
          i += 1
        }
        new ArraySeq.ofChar(values)
      case FieldProto.Type.LONG =>
        val values = new Array[Long](proto.getChildrenCount)
        i = 0
        while (i < proto.getChildrenCount) {
          values(i) = getLongValue(proto.getChildren(i))
          i += 1
        }
        new ArraySeq.ofLong(values)
      case FieldProto.Type.SHORT =>
        val values = new Array[Short](proto.getChildrenCount)
        i = 0
        while (i < proto.getChildrenCount) {
          values(i) = getShortValue(proto.getChildren(i))
          i += 1
        }
        new ArraySeq.ofShort(values)
      case FieldProto.Type.BYTE =>
        val values = new Array[Byte](proto.getChildrenCount)
        i = 0
        while (i < proto.getChildrenCount) {
          values(i) = getByteValue(proto.getChildren(i))
          i += 1
        }
        new ArraySeq.ofByte(values)
      case _ =>
        val values = new Array[AnyRef](proto.getChildrenCount)
        i = 0
        while (i < proto.getChildrenCount) {
          values(i) = protoToProperties(proto.getChildren(i)).asInstanceOf[AnyRef]
          i += 1
        }
        new ArraySeq.ofRef(values)
    }
    seq
  }

  // The following methods are used to get values of primitive types from the proto
  // These methods are called from generated SlottedBuffer classes and deal with the
  // type conversions for the primitive types that require it.
  // The need to remain public so that the generated code can access them.
  // noinspection ScalaWeakerAccess - used from generated code
  def getIntValue(proto: FieldProto): Int = proto.getIntValue
  // noinspection ScalaWeakerAccess - used from generated code
  def getDoubleValue(proto: FieldProto): Double = proto.getDoubleValue
  // noinspection ScalaWeakerAccess - used from generated code
  def getBoolValue(proto: FieldProto): Boolean = proto.getBoolValue
  // noinspection ScalaWeakerAccess - used from generated code
  def getCharValue(proto: FieldProto): Char = proto.getIntValue.toChar
  // noinspection ScalaWeakerAccess - used from generated code
  def getLongValue(proto: FieldProto): Long = proto.getLongValue
  // noinspection ScalaWeakerAccess - used from generated code
  def getShortValue(proto: FieldProto): Short = proto.getIntValue.toShort
  // noinspection ScalaWeakerAccess - used from generated code
  def getByteValue(proto: FieldProto): Byte = proto.getIntValue.toByte

  def protoToProperties(proto: FieldProto): Any = {
    val value = proto.getType match {
      case FieldProto.Type.INT     => getIntValue(proto)
      case FieldProto.Type.STRING  => proto.getStringValue
      case FieldProto.Type.DOUBLE  => getDoubleValue(proto)
      case FieldProto.Type.BOOLEAN => getBoolValue(proto)
      case FieldProto.Type.CHAR    => getCharValue(proto)
      case FieldProto.Type.LONG    => getLongValue(proto)
      case FieldProto.Type.SHORT   => getShortValue(proto)
      case FieldProto.Type.BYTE    => getByteValue(proto)
      case FieldProto.Type.BLOB    => ImmutableByteArray(proto.getBlobValue.toByteArray)
      case FieldProto.Type.SEQ     =>
        // For small collections use SlottedBuffer classes as long as we didn't already create
        // a large number of such classes
        if (proto.getChildrenCount == 0) SlottedBufferAsEmptySeq
        else if (
          proto.getChildrenCount <= Settings.slottedBufferForSeqThreshold &&
          !Shape.reachedUniqueShapeThreshold
        ) {
          // We're below the thresholds so we'll go with SlottedBuffers
          protoToSlottedBuffer(proto, isMap = false)
        } else {
          protoToArraySeq(proto)
        }
      case FieldProto.Type.MAP        => protoToSlottedBuffer(proto, isMap = true)
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
  }
}
