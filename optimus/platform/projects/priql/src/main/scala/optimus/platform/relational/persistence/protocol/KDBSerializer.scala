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
package optimus.platform.relational.persistence.protocol

import optimus.utils.datetime.ZoneIds
import java.io.{InputStream, OutputStream}
import optimus.platform.storable._
import KDBSerializerImpl._
import KDBType._
import scala.collection.immutable.Range
import scala.collection.mutable.HashMap
import java.time._
import java.lang.reflect.{Modifier, Field, Proxy, Method, Array => ReflectArray}
import optimus.entity.EntityInfoRegistry
import optimus.platform.util.ProxyMarker
import optimus.platform.relational.internal.PriqlPickledMapWrapper

/**
 * The KDBSerializer class
 *   1. circle references are not supported.
 *
 * 2. object with non String/Primitive types might not be loaded by KDB
 *
 * 3. can only serialize Iterable/Array and String/Primitive value now
 *
 * 4. inner class is not supported
 */
object KDBSerializer {

  /**
   * Serialize the value to a output stream
   *
   * @param value
   *   The value.
   * @param output
   *   The output stream.
   * @param typeCollector
   *   The type collector.
   *
   * The type collector is used to collect java types which can be used with a type provider when deserialize the value.
   */
  def serialize(value: Any, output: OutputStream, typeCollector: TypeCollector): Unit = {
    require(output != null, "output cannot be null.")

    val tc = if (typeCollector != null) typeCollector else NullTypeCollector

    tc.setType(value match {
      case v: Iterable[_] => classOf[Iterable[_]]
      case v: Array[_]    => v.getClass
      case _              => classOf[SingleObject]
    })

    writeHeader(output)
    new KDBSerializerImpl(tc, null, null).serialize(value, output)
  }

  private def writeHeader(output: OutputStream): Unit = {
    output.write(0xff)
    output.write(0x01)
  }

  /**
   * Deserialize the value from an input stream
   *
   * @param input
   *   The input stream.
   * @param typeProvider
   *   The type provider.
   * @return
   *   The value.
   *
   * The type provider should provide types in the same order when type collector collects those types during
   * serialization.
   */
  def deserialize(input: InputStream, typeProvider: TypeProvider): Any =
    deserialize(input, typeProvider, new DefaultInstanceFactory)

  /**
   * Deserialize the value from an input stream
   *
   * @param input
   *   The input stream.
   * @param typeProvider
   *   The type provider.
   * @param instanceFactory
   *   The instance factory.
   * @return
   *   The value.
   *
   * The type provider should provide types in the same order when type collector collects those types during
   * serialization.
   */
  def deserialize(input: InputStream, typeProvider: TypeProvider, instanceFactory: InstanceFactory): Any = {
    require(input != null, "input cannot be null.")
    require(typeProvider != null, "typeProvider cannot be null.")
    require(instanceFactory != null, "instanceFactory cannot be null.")

    readHeader(input)
    val clazz = typeProvider.getType()
    val value = new KDBSerializerImpl(null, typeProvider, instanceFactory).deserialize(input)
    changeType(clazz, value)
  }

  private def readHeader(input: InputStream): Unit =
    if (input.read() != 0xff || input.read() != 0x01)
      throw new UnsupportedOperationException("Invalid kdb stream header! Expect 0xff01")
}

private[persistence] class KDBSerializerImpl(
    val typeCollector: TypeCollector,
    val typeProvider: TypeProvider,
    val instanceFactory: InstanceFactory) {

  def serialize(value: Any, output: OutputStream): Unit = {
    val kdbType = getKDBType(value)
    if (kdbType == KDBType.Object) serialize(List(value), output)
    else {
      encode(kdbType.id.toByte, output)
      if (kdbType.id < 0) serializePrimitive(kdbType, value, output)
      else if (kdbType == KDBType.Null) serializeNull(output)
      else if (kdbType == KDBType.Dict) serializeDictionary(value, output)
      else {
        encode(0.toByte, output) // required by kdb format
        val iter = asIterable(value)
        if (kdbType == KDBType.Flip) {
          serializeFlip(iter, output)
        } else {
          serializeList(kdbType, iter, output)
        }
      }
    }
  }

  private def serializeNull(output: OutputStream): Unit = encode(0.toByte, output)

  private def serializePrimitive(kdbType: KDBType, value: Any, output: OutputStream): Unit = {
    kdbType match {
      case KDBType.Bool     => encode(value.asInstanceOf[Boolean], output)
      case KDBType.Byte     => encode(value.asInstanceOf[Byte], output)
      case KDBType.Char     => encode(value.asInstanceOf[Char], output)
      case KDBType.Short    => encode(value.asInstanceOf[Short], output)
      case KDBType.Int      => encode(value.asInstanceOf[Int], output)
      case KDBType.Long     => encode(value.asInstanceOf[Long], output)
      case KDBType.Float    => encode(value.asInstanceOf[Float], output)
      case KDBType.Double   => encode(value.asInstanceOf[Double], output)
      case KDBType.String   => encode(value.asInstanceOf[String], output)
      case KDBType.Date     => encode(value.asInstanceOf[LocalDate], output)
      case KDBType.DateTime => encode(value.asInstanceOf[ZonedDateTime], output)
      case _                => throw new UnsupportedOperationException("Not implemented.")
    }
  }

  private def serializeDictionary(value: Any, output: OutputStream): Unit = {
    val dict = value.asInstanceOf[Map[_, _]]
    if (dict.isEmpty) {
      serialize(List.empty, output)
      serialize(List.empty, output)
    } else {
      serialize(dict.keys, output)
      serialize(dict.values, output)
    }
  }

  private def serializeFlip(iter: Iterable[_], output: OutputStream): Unit = {
    encode(KDBType.Dict.id.toByte, output)
    val cls = iter.head.getClass // empty iter will be a general list
    if (classOf[Entity].isAssignableFrom(cls)) serializeEntityList(cls, iter, output)
    else if (cls.isEnum) serializeNativeEnumList(cls, iter, output)
    else if (cls == Constants.EnumerationValClass) serializeScalaEnumList(cls, iter, output)
    else serializeObjectList(cls, iter, output)
  }

  private def serializeNativeEnumList(clazz: Class[_], iter: Iterable[Any], output: OutputStream): Unit = {
    this.typeCollector.setType(clazz)
    serialize(List("#"), output)
    serialize(List(iter.map(i => i.asInstanceOf[Enum[_]].ordinal)), output)
  }

  private def serializeScalaEnumList(clazz: Class[_], iter: Iterable[Any], output: OutputStream): Unit = {
    val outer = clazz.getSuperclass.getDeclaredFields().filter(f => f.getName == "$outer").apply(0)
    outer.setAccessible(true)
    val clazz1 = outer.get(iter.head).getClass
    this.typeCollector.setType(clazz1)
    serialize(List("#"), output)
    serialize(List(iter.map(i => i.asInstanceOf[Enumeration#Value].id)), output)
  }

  private def serializeEntityList(clazz: Class[_], iter: Iterable[Any], output: OutputStream): Unit = {
    this.typeCollector.setType(clazz)
    val entityCompBase = entityCompanion(clazz)
    val keys = entityCompBase.info.properties.withFilter(_.isStored).map(_.name)
    val vals = keys.map(clazz.getMethod(_)).map(m => iter.map(m.invoke(_)))
    serialize(keys, output)
    serialize(vals, output)
  }

  private def serializeObjectList(clazz: Class[_], iter: Iterable[Any], output: OutputStream): Unit = {
    this.typeCollector.setType(clazz)
    if (Proxy.isProxyClass(clazz) || ProxyMarker.isProxyClass(clazz)) {
      val methods = ProxyMarker
        .getInterfaces(clazz)
        .iterator
        .flatMap(i => i.getMethods())
        .filter(m => m.getParameterTypes().isEmpty)
        .map(m => (m.getDeclaringClass().getName + "." + m.getName, m))
        .toMap
      serialize(methods.map { case (name, method) => name }, output)
      serialize(methods.map { case (name, method) => iter.map(method.invoke(_)) }, output)
    } else {
      val fieldBuilder = List.newBuilder[Field]
      var tempclazz: Class[_] = clazz
      while (tempclazz != null) {
        for (f <- tempclazz.getDeclaredFields) {
          if (f.getName.equals("this$0") || f.getName.equals("$outer")) {
            throw new UnsupportedOperationException(
              "Inner class " + clazz.getName + " is not supported, please declare it at top level!")
          } else if (
            !Modifier.isStatic(f.getModifiers) && !(Modifier.isTransient(f.getModifiers) && f.getName
              .startsWith("optimus$"))
          ) {
            fieldBuilder += f
          }
        }
        tempclazz = tempclazz.getSuperclass
      }
      val fields = fieldBuilder.result()
      if (fields.isEmpty) {
        // put in an id field
        serialize(List("#"), output)
        serialize(List(Range(0, iter.size).toList), output)
      } else {
        fields.foreach(_.setAccessible(true))
        serialize(fields.map(_.getName), output)
        serialize(fields.map(f => iter.map(f.get(_))), output)
      }
    }
  }

  private def serializeList(kdbType: KDBType, iter: Iterable[Any], output: OutputStream): Unit = {
    encode(iter.size, output) // length
    kdbType match {
      case KDBType.GeneralList =>
        for (item <- iter) {
          setType(item)
          serialize(item, output)
        }
      case KDBType.BoolList     => for (item <- iter) encode(item.asInstanceOf[Boolean], output)
      case KDBType.ByteList     => for (item <- iter) encode(item.asInstanceOf[Byte], output)
      case KDBType.CharList     => for (item <- iter) encode(item.asInstanceOf[Char], output)
      case KDBType.ShortList    => for (item <- iter) encode(item.asInstanceOf[Short], output)
      case KDBType.IntList      => for (item <- iter) encode(item.asInstanceOf[Int], output)
      case KDBType.LongList     => for (item <- iter) encode(item.asInstanceOf[Long], output)
      case KDBType.FloatList    => for (item <- iter) encode(item.asInstanceOf[Float], output)
      case KDBType.DoubleList   => for (item <- iter) encode(item.asInstanceOf[Double], output)
      case KDBType.StringList   => for (item <- iter) encode(item.asInstanceOf[String], output)
      case KDBType.DateList     => for (item <- iter) encode(item.asInstanceOf[LocalDate], output)
      case KDBType.DateTimeList => for (item <- iter) encode(item.asInstanceOf[ZonedDateTime], output)
      case _                    => throw new UnsupportedOperationException("Not implemented.")
    }
  }

  def deserialize(input: InputStream): Any = {
    val kdbType = KDBType.apply(decodeByte(input))
    if (kdbType.id < 0) deserializePrimitive(kdbType, input)
    else if (kdbType == KDBType.Null) deserializeNull(kdbType, input)
    else if (kdbType == KDBType.Dict) deserializeDictionary(input)
    // required by kdb format
    else if (decodeByte(input) != 0) throw new UnsupportedOperationException("Unexpected byte, should be zero!")
    else if (kdbType == KDBType.Flip) deserializeFlip(input)
    else deserializeList(kdbType, input)
  }

  private def deserializeNull(kdbType: KDBType, input: InputStream): Any =
    if (decodeByte(input) != 0) throw new UnsupportedOperationException("Expect zero for generic null.")
    else null

  private def deserializePrimitive(kdbType: KDBType, input: InputStream): Any = {
    kdbType match {
      case KDBType.Bool     => decodeBoolean(input)
      case KDBType.Byte     => decodeByte(input)
      case KDBType.Char     => decodeChar(input)
      case KDBType.Short    => decodeShort(input)
      case KDBType.Int      => decodeInt(input)
      case KDBType.Long     => decodeLong(input)
      case KDBType.Float    => decodeFloat(input)
      case KDBType.Double   => decodeDouble(input)
      case KDBType.String   => decodeString(input)
      case KDBType.Date     => decodeDate(input)
      case KDBType.DateTime => decodeDateTime(input)
      case _                => throw new UnsupportedOperationException("Unsupported kdb type: " + kdbType)
    }
  }

  private def deserializeList(kdbType: KDBType, input: InputStream): Iterable[_] = {
    val n = decodeInt(input) // length
    kdbType match {
      case KDBType.GeneralList  => Range(0, n, 1).map(_ => changeType(typeProvider.getType(), deserialize(input)))
      case KDBType.BoolList     => Range(0, n, 1).map(_ => decodeBoolean(input))
      case KDBType.ByteList     => Range(0, n, 1).map(_ => decodeByte(input))
      case KDBType.CharList     => Range(0, n, 1).map(_ => decodeChar(input))
      case KDBType.ShortList    => Range(0, n, 1).map(_ => decodeShort(input))
      case KDBType.IntList      => Range(0, n, 1).map(_ => decodeInt(input))
      case KDBType.LongList     => Range(0, n, 1).map(_ => decodeLong(input))
      case KDBType.FloatList    => Range(0, n, 1).map(_ => decodeFloat(input))
      case KDBType.DoubleList   => Range(0, n, 1).map(_ => decodeDouble(input))
      case KDBType.StringList   => Range(0, n, 1).map(_ => decodeString(input))
      case KDBType.DateList     => Range(0, n, 1).map(_ => decodeDate(input))
      case KDBType.DateTimeList => Range(0, n, 1).map(_ => decodeDateTime(input))
      case _                    => throw new UnsupportedOperationException("Unsupported kdb type: " + kdbType)
    }
  }

  private def deserializeDictionary(input: InputStream): Map[_, _] = {
    val keys = deserialize(input).asInstanceOf[Iterable[Any]].toArray
    val values = deserialize(input).asInstanceOf[Iterable[Any]].toArray
    val hashMap = new HashMap[Any, Any]
    for (i <- 0 until keys.size) hashMap.put(keys.apply(i), values.apply(i))
    hashMap.toMap
  }

  private def deserializeFlip(input: InputStream): Iterable[_] = {
    if (decodeByte(input) != KDBType.Dict.id)
      throw new UnsupportedOperationException("Unexpected KDBType for flip content!")

    val itemClass = this.typeProvider.getType()

    if (classOf[Entity].isAssignableFrom(itemClass)) deserializeEntityList(itemClass, input)
    else if (itemClass.isEnum || classOf[Enumeration].isAssignableFrom(itemClass)) deserializeEnumList(itemClass, input)
    else deserializeObjectList(itemClass, input)
  }

  private def deserializeEntityList(clazz: Class[_], input: InputStream): Iterable[_] = {
    val fieldNameToValueIter = createFieldNameToValueIter(input)
    val entityCompBase = entityCompanion(clazz)
    val properties = entityCompBase.info.properties.filter(_.isStored)

    val listBuilder = List.newBuilder[Any]
    var shouldBreak = false
    val itemMap = new HashMap[String, Any]
    while (!shouldBreak) {
      for (p <- properties) {
        val valueIter = fieldNameToValueIter(p.name)
        if (valueIter.hasNext) itemMap.put(p.name, changeType(p.propertyClass, valueIter.next()))
        else shouldBreak = true
      }
      if (!shouldBreak)
        listBuilder += entityCompBase.info.createUnpickled(new PriqlPickledMapWrapper(itemMap.toMap), true)
      itemMap.clear()
    }
    listBuilder.result()
  }

  private def deserializeEnumList(clazz: Class[_], input: InputStream): Iterable[_] = {
    val fieldNameToValueIter = createFieldNameToValueIter(input)
    if (fieldNameToValueIter.isEmpty)
      throw new UnsupportedOperationException("We do not support object with no fields!")

    val listBuilder = List.newBuilder[Any]
    fieldNameToValueIter("#").foreach(i => listBuilder += instanceFactory.createEnum(clazz, i.asInstanceOf[Int]))
    listBuilder.result()
  }

  private def deserializeObjectList(clazz: Class[_], input: InputStream): Iterable[_] = {
    val nameToValueIter = createFieldNameToValueIter(input)
    if (nameToValueIter.isEmpty)
      throw new UnsupportedOperationException("We do not support object with no fields/methods!")
    val listBuilder = List.newBuilder[Any]
    if (Proxy.isProxyClass(clazz) || ProxyMarker.isProxyClass(clazz)) {
      val interfaces = ProxyMarker.getInterfaces(clazz)
      val nameToMethod: Map[String, Method] = interfaces.iterator
        .flatMap(i => i.getMethods())
        .filter(m => m.getParameterTypes().isEmpty)
        .map(m => (m.getDeclaringClass().getName + "." + m.getName, m))
        .toMap
      var shouldBreak = false
      while (!shouldBreak) {
        val methodToValue = new HashMap[Method, Any]
        for (mn <- nameToValueIter.keys) {
          val valueIter = nameToValueIter(mn)
          if (valueIter.hasNext) methodToValue.put(nameToMethod(mn), valueIter.next())
          else shouldBreak = true
        }
        if (!shouldBreak) {
          listBuilder += Proxy.newProxyInstance(clazz.getClassLoader, interfaces, new DictionaryProxy(methodToValue))
        }
      }
    } else {
      val nameToField = createFieldNameToField(clazz)
      val shouldNotSetField = nameToField.isEmpty || (nameToValueIter.size == 1 && nameToValueIter.contains("#"))
      var shouldBreak = false
      while (!shouldBreak) {
        val instance = instanceFactory.createInstance(clazz)
        for (fn <- nameToValueIter.keys) {
          val valueIter = nameToValueIter(fn)
          if (valueIter.hasNext) {
            val value = valueIter.next()
            if (!shouldNotSetField) {
              val field = nameToField(fn)
              field.set(instance, changeType(field.getType, value))
            }
          } else shouldBreak = true
        }
        if (!shouldBreak) listBuilder += instance
      }
    }
    listBuilder.result()
  }

  private def createFieldNameToValueIter(input: InputStream): HashMap[String, Iterator[_]] = {
    val fieldNames = deserialize(input).asInstanceOf[Iterable[String]].toArray
    val values = deserialize(input).asInstanceOf[Iterable[Iterable[_]]].toArray
    val fieldNameToValueIter = new HashMap[String, Iterator[_]]
    for (i <- 0 until fieldNames.size) fieldNameToValueIter.put(fieldNames.apply(i), values.apply(i).iterator)
    fieldNameToValueIter
  }

  private def createFieldNameToField(clazz: Class[_]): HashMap[String, Field] = {
    val nameToField = new HashMap[String, Field]
    var tempclazz: Class[_] = clazz
    while (tempclazz != null) {
      for (field <- tempclazz.getDeclaredFields.filter(f => !Modifier.isStatic(f.getModifiers))) {
        field.setAccessible(true)
        nameToField.put(field.getName, field)
      }
      tempclazz = tempclazz.getSuperclass
    }
    nameToField
  }

  private def setType(value: Any): Unit =
    typeCollector.setType(value match {
      case _: Iterable[_] | _: Array[_] => classOf[Iterable[_]]
      case _                            => classOf[SingleObject]
    })
}

private[persistence] object KDBSerializerImpl {

  def encode(value: Boolean, output: OutputStream): Unit = output.write(if (value) 1 else 0)

  def decodeBoolean(input: InputStream): Boolean = input.read == 1

  def encode(value: Byte, output: OutputStream): Unit = output.write(0xff & value)

  def decodeByte(input: InputStream): Byte = input.read.toByte

  def encode(value: Char, output: OutputStream): Unit = output.write(0xff & value)

  def decodeChar(input: InputStream): Char = input.read.toChar

  def encode(value: Short, output: OutputStream): Unit = {
    output.write(0xff & value)
    output.write(0xff & (value >> 8))
  }

  def decodeShort(input: InputStream): Short = {
    val a = input.read
    val b = input.read
    (a | (b << 8)).toShort
  }

  def encode(value: Int, output: OutputStream): Unit = {
    output.write(0xff & value)
    output.write(0xff & (value >> 8))
    output.write(0xff & (value >> 16))
    output.write(0xff & (value >> 24))
  }

  def decodeInt(input: InputStream): Int = {
    val a, b, c, d = input.read
    a | (b << 8) | (c << 16) | (d << 24)
  }

  def encode(value: Long, output: OutputStream): Unit = {
    encode(value.toInt, output)
    encode((value >> 32).toInt, output)
  }

  def decodeLong(input: InputStream): Long = {
    val a, b = decodeInt(input).toLong
    (a & 0xffffffffL) | (b << 32)
  }

  def encode(value: Float, output: OutputStream): Unit =
    encode(java.lang.Float.floatToRawIntBits(value), output)

  def decodeFloat(input: InputStream): Float =
    java.lang.Float.intBitsToFloat(decodeInt(input))

  def encode(value: Double, output: OutputStream): Unit =
    encode(java.lang.Double.doubleToRawLongBits(value), output)

  def decodeDouble(input: InputStream): Double =
    java.lang.Double.longBitsToDouble(decodeLong(input))

  def encode(value: String, output: OutputStream): Unit = {
    output.write(value.getBytes("US-ASCII"))
    output.write(0)
  }

  def decodeString(input: InputStream): String = {
    val sb = new StringBuffer()
    var b = input.read
    while (b != 0) {
      sb.append(b.toChar)
      b = input.read
    }
    sb.toString
  }

  def encode(value: LocalDate, output: OutputStream): Unit =
    encode((value.toEpochDay() - dateOffset).toInt, output)

  def decodeDate(input: InputStream): LocalDate =
    LocalDate.ofEpochDay(dateOffset + decodeInt(input))

  def encode(value: ZonedDateTime, output: OutputStream): Unit = {
    val utc = value.withZoneSameInstant(ZoneIds.UTC)
    val time = utc.toLocalTime().toNanoOfDay().toDouble / nanosPerDay
    val date = utc.toLocalDate().toEpochDay() - dateOffset
    encode(time + date, output)
  }

  def decodeDateTime(input: InputStream): ZonedDateTime = {
    val dateTime = decodeDouble(input)
    val days = dateTime.toInt + (if (dateTime >= 0) 0 else -1)
    val localDate = LocalDate.ofEpochDay(dateOffset + days)
    val localTime = LocalTime.ofNanoOfDay(((dateTime - days) * nanosPerDay).toLong)
    ZonedDateTime.of(localDate, localTime, ZoneIds.UTC)
  }

  def getSimpleKDBType(value: Any) = value match {
    case _: Boolean       => KDBType.Bool
    case _: Byte          => KDBType.Byte
    case _: Char          => KDBType.Char
    case _: Short         => KDBType.Short
    case _: Int           => KDBType.Int
    case _: Long          => KDBType.Long
    case _: Float         => KDBType.Float
    case _: Double        => KDBType.Double
    case _: String        => KDBType.String
    case _: LocalDate     => KDBType.Date
    case _: ZonedDateTime => KDBType.DateTime
    case _                => KDBType.Object
  }

  def asIterable(value: Any): Iterable[_] = value match {
    case v: Array[_]    => v
    case v: Iterable[_] => v
    case _              => throw new RuntimeException("Invalid value, expect Array[_] or Iterable[_].")
  }

  /**
   * Copied from Query.scala
   */
  def entityCompanion[T <: Entity](entityClass: Class[_]): EntityCompanionBase[T] =
    EntityInfoRegistry.getCompanion(entityClass).asInstanceOf[EntityCompanionBase[T]]

  def changeType(target: Class[_], value: Any): Any = value match {
    case v: Iterable[_] =>
      if (target.isArray) {
        val valueArray = value.asInstanceOf[Iterable[Any]].toArray
        val componentType = target.getComponentType
        val arr = ReflectArray.newInstance(componentType, valueArray.length)
        for (i <- 0 until valueArray.length) ReflectArray.set(arr, i, valueArray.apply(i))
        arr
      } else if (target == classOf[SingleObject]) value.asInstanceOf[Iterable[_]].headOption.getOrElse(null)
      else if (!target.isAssignableFrom(value.getClass)) {
        throw new UnsupportedOperationException("Not implemented yet, try to use Iterable[_] as field type.")
      } else value
    case _ => value
  }

  def checkItemAreSameType(iter: Iterable[Any]): Boolean = iter.toIndexedSeq match {
    case (_: Iterable[_]) +: _ | (_: Array[_]) +: _ | null +: _ => false
    case h +: _ =>
      val cls = h.getClass
      !iter.exists(i => i == null || i.getClass != cls)
  }

  def getKDBType(value: Any): KDBType = value match {
    case null         => KDBType.Null
    case v: Map[_, _] => KDBType.Dict
    case _: Iterable[_] | _: Array[_] =>
      val iter = asIterable(value)
      val itemsAreSameType = checkItemAreSameType(iter)
      if (itemsAreSameType) {
        val kdbType = getSimpleKDBType(iter.head)
        if (kdbType == KDBType.Object) KDBType.Flip
        else KDBType.apply(kdbType.id * -1)
      } else KDBType.GeneralList
    case _ => getSimpleKDBType(value)
  }

  val dateOffset = LocalDate.of(2000, 1, 1).toEpochDay
  val nanosPerDay = 24L * 60 * 60 * 1000000000

}

/**
 * Used internally by KDBSerializer
 */
final class SingleObject()
