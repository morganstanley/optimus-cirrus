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
package optimus.platform.relational.internal

import java.lang.annotation.Annotation
import optimus.utils.datetime.ZoneIds

import java.lang.reflect.Method
import java.lang.reflect.Modifier
import java.text.ParseException
import java.util.Date
import java.util.concurrent.atomic.AtomicReference
import java.time._
import java.time.format.DateTimeFormatterBuilder
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import msjava.slf4jutils.scalalog._
import optimus.entity.IndexInfo
import optimus.platform._
import optimus.platform.AsyncImplicits._
import optimus.platform.relational._
import optimus.platform.relational.tree._
import optimus.platform.storable._
import optimus.utils.datetime.ZonedDateTimeOps
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.util.Try
import optimus.scalacompat.collection._

object RelationalUtils {
  import TypeInfoUtils._
  val log = getLogger(this)

  def isImmutableArrayOfByte(shape: TypeInfo[_]): Boolean = {
    val underlying = TypeInfo.underlying(shape)
    underlying <:< classOf[ImmutableArray[_]] && underlying.typeParams.exists(t => t.clazz == classOf[Byte])
  }

  def extractEntityDefaultKey(entityInfo: optimus.entity.StorableInfo): Option[IndexInfo[Storable, _]] = {
    entityInfo.keys.find(f => f.default).asInstanceOf[Option[IndexInfo[Storable, _]]]
  }

  def extractConstructorValNames[T](info: TypeInfo[T]): Seq[String] = {
    info.primaryConstructorParams.map(_._1).filter(info.propertyNames.toSet)
  }

  def convertItemValue(item: String, itemType: TypeInfo[_], timeZone: ZoneId): AnyRef = {
    convertItemValue(
      item,
      itemType.clazz,
      itemType.typeParams.headOption match {
        case Some(arg) => arg.clazz
        case None      => null
      },
      timeZone)
  }

  private val valueFormatterLookup = new AtomicReference(Map.empty[Class[_], StringValueFormatter])

  /**
   * Convert item from string to its type. the date time format in csv should be YYYY-MM-DD or YYYY-MM-DDDD
   * HH:MM:SS,millisec or YYYY-MM-DDDDTHH:MM:SS,millisec+08:00[ZoneId]
   *
   * Does not handle nested types.
   */
  def convertItemValue(item: String, itemClass: Class[_], typeArgument: Class[_], timeZone: ZoneId): AnyRef = {
    itemClass match {
      case c if (c == classOf[String])               => item
      case c if (c == classOf[Int])                  => int2Integer(item.toInt)
      case c if (c == classOf[Long])                 => long2Long(item.toLong)
      case c if (c == classOf[Double])               => double2Double(item.toDouble)
      case c if (c == classOf[Boolean])              => boolean2Boolean(item.toBoolean)
      case c if (c == classOf[Float])                => float2Float(item.toFloat)
      case c if (c == classOf[Byte])                 => byte2Byte(item.toByte)
      case c if (c == classOf[Short])                => short2Short(item.toShort)
      case c if (c == classOf[java.math.BigDecimal]) => BigDecimal(item)
      case c if (c == classOf[Char]) =>
        if (item.length == 1) char2Character(item.charAt(0))
        else throw new IllegalArgumentException("Cannot convert " + item + "to Char")
      case c if (c == classOf[LocalTime]) =>
        try {
          LocalTime.parse(item)
        } catch {
          case e: ParseException =>
            throw new RelationalException(
              s"cannot convert $item in csv to LocalTime, since it is not the format of hh-mm-ss or hh-mm-ss.nano",
              e)
        }
      case c if (c == classOf[LocalDate]) =>
        try {
          LocalDate.parse(item)
        } catch {
          case e: ParseException =>
            throw new RelationalException(
              s"cannot convert $item in csv to LocalDate, since it is not the format of yyyy-MM-dd",
              e)
        }

      case c if (c == classOf[ZonedDateTime]) => {
        (item.indexOf("[") == -1 && item.indexOf("]") == -1) match {
          case true => convertFormattedZdt(item)
          case _    => convertCompletedZdt(item, timeZone)
        }
      }
      case c if (c == classOf[Option[_]]) =>
        if (item == null || item == "None") None
        else {
          val value = if (item.indexOf("Some") != -1) item.substring(5, item.lastIndexOf(")")) else item
          Some(convertItemValue(value, typeArgument, null, timeZone))
        }
      case c =>
        var lookup = valueFormatterLookup.get
        val formatter = lookup.get(c) match {
          case Some(f) => f
          case None =>
            val annon = c.getAnnotation(classOf[stringFormatter])
            if (annon eq null)
              throw new IllegalArgumentException(s"Cannot convert '$item' to $c with type parameter $typeArgument")
            val f = annon.formatter.getDeclaredConstructor().newInstance()
            while (!valueFormatterLookup.compareAndSet(lookup, lookup + (c -> f))) {
              lookup = valueFormatterLookup.get
            }
            f
        }
        formatter.fromString(item)
    }
  }

  /**
   * the format is YYYY-MM-DDDD HH:MM:SS.millisec just convert to default time zone
   */
  private def convertFormattedZdt(item: String): ZonedDateTime = {
    Try {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
      val localDateTime = LocalDateTime.parse(item, formatter)
      ZonedDateTime.of(localDateTime, ZoneIds.UTC)
    } orElse Try {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      val localDateTime = LocalDateTime.parse(item, formatter)
      ZonedDateTime.of(localDateTime, ZoneIds.UTC)
    } getOrElse {
      throw new RelationalException(
        "cannot convert value in csv to ZonedDateTime, since it is not the format of yyyy-MM-dd HH:mm:ss.SSS or yyyy-MM-dd HH:mm:ss")
    }
  }

  /**
   * the format is YYYY-MM-DDTHH:MM:SS.millisec+08:00[ZoneId] can 1 convert to user specified time zone in
   * csvRelation.source.timeZone 2 create a new one with the time zone in csv
   */
  private def convertCompletedZdt(item: String, timeZone: ZoneId): ZonedDateTime = {
    try {
      val zdt = ZonedDateTimeOps.parseTreatingOffsetAndZoneIdLikeJSR310(item)

      if (timeZone == null)
        zdt
      else
        convertZonedDateTimeToZonedDateTime(zdt, timeZone)
    } catch {
      case e: ParseException =>
        throw new RelationalException(
          s"cannot convert ${item} in csv to ZonedDateTime, since it is not the format of yyyy-MM-dd HH:mm:ss.SSS+offset[ZoneId]",
          e)
    }
  }

  private[optimus] def convertZonedDateTime2JUDate(date: ZonedDateTime): Date = {
    // should this instead use java.sql.Timestamp and set the number of nanoseconds?
    val ts = new Date(date.toEpochSecond * 1000)
    ts
  }

  // ------------------------------------------------------------------------------
  //  Utility of DateTime
  // ------------------------------------------------------------------------------
  private[optimus] def convertToZonedDateTime(a: Any, timeZone: ZoneId): Any = {
    a match {
      case zdt: ZonedDateTime => convertZonedDateTimeToZonedDateTime(zdt, timeZone)
      case _                  => a
    }
  }

  private[optimus] def convertZonedDateTimeToZonedDateTime(
      originalZdt: ZonedDateTime,
      destTimeZone: ZoneId): ZonedDateTime = {
    originalZdt.withZoneSameInstant(destTimeZone)

  }

  private[optimus] val zonedDateTimeFormatter = {
    val dtfb = new DateTimeFormatterBuilder
    dtfb
      .appendPattern("yyyy-MM-dd HH:mm:ss")
      .appendFraction(ChronoField.NANO_OF_SECOND, 9, 9, true)
      .appendPattern("XXXXX")
      .appendZoneId()
      .toFormatter()
  }

  def convertZonedDateTimeToString(zdt: ZonedDateTime): String = zonedDateTimeFormatter.format(zdt)
  def convertLocalDateToString(dt: LocalDate): String = dt.format(DateTimeFormatter.ISO_LOCAL_DATE)

  private[optimus] def convertConstToString(a: Any): String = a match {
    case zdt: ZonedDateTime => convertZonedDateTimeToString(zdt)
    case dt: LocalDate      => convertLocalDateToString(dt)
    case _                  => if (a == null) "" else a.toString
  }

  // ------------------------------------------------------------------------------
  //  Utility of Reflection
  // ------------------------------------------------------------------------------
  @node private[optimus] def distinctDataWithKeys[T](data: Iterable[T], key: RelationKey[T]): Iterable[T] = {
    if (!hasKeys(key))
      throw new IllegalArgumentException("Does not provide keys")

    if ((data eq null) || data.isEmpty)
      data
    else {
      distinctWithKeys(data, key)
    }
  }

  @node private[optimus] def distinctWithKeys[T](data: Iterable[T], key: RelationKey[T]): Iterable[T] = {
    val hashCodeArray: Array[Int] = if (key.isSyncSafe) {
      // handcoded so we avoid boxing the ints (we have have many millions of them)
      val hashes = new Array[Int](data.size)
      var pos = 0
      val it = data.iterator
      while (it.hasNext) {
        hashes(pos) = key.ofSync(it.next()).hashCode
        pos += 1
      }
      hashes
    } else
      data.apar(PriqlSettings.concurrencyLevel, runtimeChecks = false).map(r => key.of(r).hashCode)(Array.breakOut)

    val dataArray = data.toArray[Any].asInstanceOf[Array[T]]
    val resultBuffer = new ArrayBuffer[T](data.size)

    sort(hashCodeArray, dataArray)

    val length = dataArray.length
    var from = 0
    var ranges: ListBuffer[(Int, Int)] = null
    while (from < length) {
      var to = from
      val hashCode = hashCodeArray(from)
      while (to < length - 1 && hashCodeArray(to + 1) == hashCode) to += 1
      if (to == from) {
        resultBuffer += dataArray(from)
      } else {
        if (ranges eq null)
          ranges = new ListBuffer[(Int, Int)]
        ranges += from -> (to + 1)
      }
      from = to + 1
    }
    if (ranges ne null)
      resultBuffer ++= ranges.apar(PriqlSettings.concurrencyLevel, runtimeChecks = false).flatMap {
        case (from, until) => extractUniqueItemsWithIdenticalHashCode(dataArray, from, until, key)
      }
    resultBuffer.toIndexedSeq
  }

  @async private def extractUniqueItemsWithIdenticalHashCode[T](
      data: Array[T],
      from: Int,
      until: Int,
      key: RelationKey[T]): Iterable[T] = {
    val dataView = data.view
    def checkUniqueness(v: Vector[Any]): Unit = {
      val len = v.length
      var i = 0
      while (i < len) {
        val k = v(i)
        var j = i + 1
        while (j < len) {
          if (v(j) == k)
            throw new IllegalArgumentException(s"Duplicate key: Two unequal rows have the same key. Row 1: ${data(
                from + i)}; Row 2: ${data(from + j)}; Key: $k; Key fields: ${key.fields.toList}")
          j += 1
        }
        i += 1
      }
    }

    var i = from + 1
    var u = until
    while (i < u) {
      val item = data(i)
      if (dataView.slice(from, i).exists(t => equal(t, item))) {
        u -= 1
        val temp = data(u)
        data(u) = data(i)
        data(i) = temp
      } else {
        i += 1
      }
    }

    if (u - from > 1) {
      val keys: Vector[Any] =
        if (key.isSyncSafe)
          dataView.slice(from, u).iterator.map(v => key.ofSync(v)).toVector
        else
          dataView
            .slice(from, u)
            .apar(PriqlSettings.concurrencyLevel, runtimeChecks = false)
            .map(v => key.of(v))(Vector.breakOut)

      checkUniqueness(keys)
    }

    dataView.slice(from, u)
  }

  private def sort[T](values: Array[Int], adjoint: Array[T]): Unit = {

    def exchange(i: Int, j: Int): Unit = {
      val temp1 = values(i)
      values(i) = values(j)
      values(j) = temp1

      val temp2 = adjoint(i)
      adjoint(i) = adjoint(j)
      adjoint(j) = temp2
    }

    def quicksort(low: Int, high: Int): Unit = {
      var i = low
      var j = high
      val pivot = values(low + (high - low) / 2)

      while (i <= j) {
        while (values(i) < pivot) i += 1
        while (values(j) > pivot) j -= 1
        if (i <= j) {
          exchange(i, j)
          i += 1
          j -= 1
        }
      }
      if (low < j)
        quicksort(low, j)
      if (i < high)
        quicksort(i, high)
    }

    if (adjoint.length != 0)
      quicksort(0, adjoint.length - 1)
  }

  def equal(a: Any, b: Any): Boolean = a == b || structurallyEqual(a, b)

  // we need this to support equality on anon types (e.g. "new { def x = 7 }")
  def structurallyEqual(a: Any, b: Any): Boolean =
    if (a != null && b != null) {
      val aClass = a.getClass
      val bClass = b.getClass
      if (aClass.getName.contains("$anon$") && bClass.getName.contains("$anon$")) {
        val aMethods = aClass.getMethods
          .withFilter(m =>
            !predefMethods(m.getName) && (m.getModifiers & Modifier.PUBLIC) != 0 && m.getParameterTypes.length == 0)
          .map(m => m.getName -> m)
          .toMap
        val bMethods = bClass.getMethods
          .withFilter(m =>
            !predefMethods(m.getName) && (m.getModifiers & Modifier.PUBLIC) != 0 && m.getParameterTypes.length == 0)
          .map(m => m.getName -> m)
          .toMap
        if (aMethods.keys == bMethods.keys) {
          aMethods.forall { case (name, method) =>
            method.invoke(a) == bMethods(name).invoke(b)
          }
        } else false
      } else false

    } else false

  private[optimus] def hasKeys(key: RelationKey[_]): Boolean =
    key != null && key.fields != null && !(key.fields.isEmpty)

  def findAllGetterMethodFromJava(c: Class[_]): Map[String, Method] = {
    c.getMethods.iterator
      .filter(m =>
        m.getParameterTypes.isEmpty
          && Modifier.isPublic(m.getModifiers)
          && !(Modifier.isStatic(m.getModifiers))
          && !(Void.TYPE.equals(m.getReturnType))
          && isGetMethod(m.getName))
      .map(m => (getPropertyName(m.getName), m))
      .toMap
  }

  private[optimus] def isGetMethod(name: String): Boolean = {
    name.startsWith("get") && (!name.equals("getClass")) && (!name.equals(
      "getAll"
    )) // getAll comes from JavaDynamicObjectProxy
  }

  private[optimus] def getPropertyName(name: String): String =
    if (isGetMethod(name)) StringUtils.uncapitalize(name.substring(3, name.length)) else name

  def box(b: Any): AnyRef = b match {
    case null       => null
    case a: AnyRef  => a
    case a: Byte    => Byte.box(a)
    case a: Short   => Short.box(a)
    case a: Int     => Int.box(a)
    case a: Long    => Long.box(a)
    case a: Float   => Float.box(a)
    case a: Double  => Double.box(a)
    case a: Boolean => Boolean.box(a)
    case a: Char    => Char.box(a)
    case _          => throw new RelationalException("unexpected type: " + b.getClass)
  }

  private[optimus] def getPrimitiveColumnFromAnyArray(
      returnType: Class[_],
      name: String,
      anyArray: Array[Any]): RelationColumn[_] = {
    returnType match {
      case c if (c == classOf[Int]) =>
        val intA = new ListBuffer[Int]
        anyArray.foreach(f => intA += f.asInstanceOf[java.lang.Integer])
        new RelationColumn(returnType, name, intA.toArray)
      case c if (c == classOf[Double]) =>
        val doubleA = new ListBuffer[Double]
        anyArray.foreach(f => doubleA += f.asInstanceOf[java.lang.Double])
        new RelationColumn(returnType, name, doubleA.toArray)
      case c if (c == classOf[Long]) =>
        val longA = new ListBuffer[Long]
        anyArray.foreach(f => longA += f.asInstanceOf[java.lang.Long])
        new RelationColumn(returnType, name, longA.toArray)
      case c if (c == classOf[Float]) =>
        val floatA = new ListBuffer[Float]
        anyArray.foreach(f => floatA += f.asInstanceOf[java.lang.Float])
        new RelationColumn(returnType, name, floatA.toArray)
      case c if (c == classOf[Short]) =>
        val shortA = new ListBuffer[Short]
        anyArray.foreach(f => shortA += f.asInstanceOf[java.lang.Short])
        new RelationColumn(returnType, name, shortA.toArray)
      case c if (c == classOf[Byte]) =>
        val byteA = new ListBuffer[Byte]
        anyArray.foreach(f => byteA += f.asInstanceOf[java.lang.Byte])
        new RelationColumn(returnType, name, byteA.toArray)
      case c if (c == classOf[Char]) =>
        val charA = new ListBuffer[Char]
        anyArray.foreach(f => charA += f.asInstanceOf[java.lang.Character])
        new RelationColumn(returnType, name, charA.toArray)
      case c if (c == classOf[String]) =>
        val strA = new ListBuffer[String]
        anyArray.foreach(f => strA += f.asInstanceOf[java.lang.String])
        new RelationColumn(returnType, name, strA.toArray)
      case _ =>
        new RelationColumn(returnType, name, anyArray)
    }
  }
  private[optimus] def convertEntityRowDataToColumnData[T <: Entity, RowType: TypeInfo](
      rows: Iterable[RowType]): RelationColumnView = {
    if (rows != null) {

      val properties = typeInfo[RowType].asInstanceOf[TypeInfo[(_, _)]].primaryConstructorParams
      val columns = new ListBuffer[RelationColumn[_]]
      for (propertyMap <- properties) {
        val columnValues = new ArrayBuffer[Any]
        rows.foreach(f => {
          val propertyV = typeInfo.propertyValue(propertyMap._1, f)
          columnValues += propertyV
        })
        val column = getPrimitiveColumnFromAnyArray(propertyMap._2, propertyMap._1, columnValues.toArray)
        columns += column
      }
      new RelationColumnView(List.range(0, rows.size), new RelationColumnSource(typeInfo[RowType], columns.toList))
    } else
      throw new RelationalException(
        "rows generated from createIterable shouldn't be null, but empty or with some elements")
  }

  private[optimus] def distinctColumnBasedDataWithKeys(
      columnView: RelationColumnView,
      key: RelationKey[_]): RelationColumnView = {
    val keyColumns = new ListBuffer[RelationColumn[_]]
    val keyNames = new ListBuffer[String]
    val shapeType = columnView.src.shapeType
    key.fields.foreach(f => keyNames += f)

    columnView.src.columns.foreach(f => {
      if (keyNames.contains(f.columnName))
        keyColumns += f
    })

    if (keyColumns.isEmpty) columnView
    else {
      val indicies = columnView.indicies
      val key2Indicies = new HashMap[ListBuffer[Any], Int] // index is the first one who has the same keys
      for (i <- 0 until indicies.size) {
        val index = indicies(i)
        val keys = new ListBuffer[Any]
        keyColumns foreach (keys += _.columnValues(i))
        if (!key2Indicies.contains(keys))
          key2Indicies.put(keys, index)
      }
      val newIndicies = new ListBuffer[Int]
      val values = key2Indicies.values.toIndexedSeq
      indicies.foreach(f => if (values contains f) newIndicies += f)
      new RelationColumnView(newIndicies.toList, columnView.src)
    }
  }

  def getMethodsMap[T <: Annotation](clz: Class[_]): Map[String, Method] = {
    clz.getMethods.iterator
      .filter(_.getParameterCount() == 0)
      .map { m =>
        m.getName -> m
      }
      .toMap
  }

  def getMethodAnnotation[T <: Annotation](
      methods: Map[String, Method],
      fieldName: String,
      anno: Class[T]): Option[T] = {
    methods.get(fieldName).map(_.getAnnotation(anno))
  }
}
