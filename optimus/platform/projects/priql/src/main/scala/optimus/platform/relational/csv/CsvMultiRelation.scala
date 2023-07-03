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
package optimus.platform.relational.csv

import java.time.ZoneId
import java.lang.reflect._
import optimus.platform._
import optimus.platform.pickling.PickledInputStream
import optimus.platform.relational.{MapBasedDynamicObject, RelationalException}
import optimus.platform.relational.inmemory.{ArrangedSource, IterableSource}
import optimus.platform.relational.internal.{PriqlPickledMapWrapper, RelationalUtils}
import optimus.platform.relational.tree._
import optimus.platform.storable.Entity
import optimus.platform.util.CsvParser
import org.apache.commons.io.LineIterator
import org.apache.commons.lang3.StringUtils
import scala.collection.mutable.ListBuffer

/**
 * CSV provider
 */
class CsvMultiRelation[T](
    val source: AbstractCsvSource[T],
    typeInfo: TypeInfo[T],
    key: RelationKey[_],
    pos: MethodPosition)
    extends ProviderRelation(typeInfo, key, pos)
    with IterableSource[T]
    with ArrangedSource {
  override def getProviderName = "CSVProvider"

  override def prettyPrint(indent: Int, out: StringBuilder): Unit = {
    indentAndStar(indent, out)
    out ++= serial + " Provider:" + " '" + getProviderName + "' " + "of" + " type " + typeInfo.name + "\n"
  }

  override def fillQueryExplainItem(level_id: Integer, table: ListBuffer[QueryExplainItem]): Unit = {
    table += new QueryExplainItem(level_id, getProviderName, typeInfo.name, "CsvFileScan", 0)
  }

  def isSyncSafe = true

  def getSync(): Iterable[T] = source match {
    case t: CsvSource[_] => new CsvEntityReaderIterable[Entity](this)(typeInfo.cast[Entity]).asInstanceOf[Iterable[T]]
    case t: DynamicCsvSource =>
      new CsvDynamicReaderIterable(this.asInstanceOf[CsvMultiRelation[DynamicObject]]).asInstanceOf[Iterable[T]]
  }

  @async def get() = getSync()

  override def makeKey(newKey: RelationKey[_]) = new CsvMultiRelation[T](source, typeInfo, newKey, pos)
}

object CsvMultiRelation {
  def apply[T <: Entity: TypeInfo, KeyType <: RelationKey[T]](
      source: CsvSource[T],
      key: KeyType,
      pos: MethodPosition): CsvMultiRelation[T] = new CsvMultiRelation[T](source, typeInfo[T], key, pos)

  def apply[T <: DynamicObject](
      source: DynamicCsvSource,
      key: RelationKey[T],
      pos: MethodPosition): CsvMultiRelation[DynamicObject] =
    new CsvMultiRelation[DynamicObject](source, typeInfo[DynamicObject], key, pos)
}

trait CsvFieldConvertor {
  def convert(fieldName: String, fieldValue: String): Any
}

object CsvFieldConvertor {
  def apply(propertyMap: Map[String, TypeInfo[_]] = Map.empty, timeZone: ZoneId = null): CsvFieldConvertor = {
    new CsvFieldConvertorImpl(propertyMap, timeZone)
  }
}

private class CsvFieldConvertorImpl(val propertyMap: Map[String, TypeInfo[_]], val timeZone: ZoneId)
    extends CsvFieldConvertor {

  private val TYPE_NAME_PREFIX = "class "

  private def getClass(cType: Type) = {
    def getClassName(cType: Type): String = {
      cType match {
        case null => ""
        case t =>
          val className = t.toString
          if (className.startsWith(TYPE_NAME_PREFIX)) className.substring(TYPE_NAME_PREFIX.length())
          else ""
      }
    }

    if (cType.isInstanceOf[Class[_]]) cType.asInstanceOf[Class[_]]
    else {
      val className = getClassName(cType)
      if (className == null || className.isEmpty) null
      else Class.forName(className)
    }
  }

  override def convert(fieldName: String, fieldValue: String): Any = {
    propertyMap.get(fieldName).getOrElse(null) match {
      case TypeInfo(classes, _, _, typeParams) if (classes.length == 1 && typeParams.length == 1) =>
        RelationalUtils.convertItemValue(fieldValue, classes.head, typeParams.head.classes.head, timeZone)
      case TypeInfo(classes, _, _, typeParams) if (classes.length == 1 && typeParams.length == 0) =>
        RelationalUtils.convertItemValue(fieldValue, classes.head, null, timeZone)
      case null => fieldValue
      case t =>
        throw new RelationalException(s"CsvFieldConvertor cannot convert ${fieldName} whose target type is ${t.name}")
    }
  }
}

sealed abstract class CsvReaderIterable[T](val csvRelation: CsvMultiRelation[_]) extends Iterable[T] {
  // The line iterator to read all csv data from file or string. And LineIterator will close reader
  val lineIter = new LineIterator(csvRelation.source.reader) {
    // we should ignore empty line of csv file.
    override def isValidLine(line: String): Boolean = StringUtils.isNotBlank(line)
  }

  // Read 1st line if we have header.
  val headers: List[String] = if (csvRelation.source.hasHeader) CsvParser.parseLine(lineIter.nextLine()) else List.empty

  lazy val loadedData: List[T] = {
    val tmpData = ListBuffer[T]()
    while (lineIter.hasNext) {
      try {
        val items: List[String] = CsvParser.parseLine(lineIter.nextLine())
        tmpData += createObject(items)
      } catch {
        case e: Exception => throw new RelationalException("Cannot parse csv row to object.", e)
      }
    }
    tmpData.toList
  }

  def iterator: Iterator[T] = loadedData.iterator

  def createObject(items: List[String]): T
}

class CsvDynamicReaderIterable(csvRelation: CsvMultiRelation[DynamicObject])
    extends CsvReaderIterable[DynamicObject](csvRelation) {
  override def createObject(items: List[String]): DynamicObject = {
    if (csvRelation.source.hasHeader) {
      if (headers.size != items.size) throw new IllegalArgumentException("CSV header and column size should be same.")
      val customConvertor = csvRelation.source match {
        case s: DynamicCsvSource => s.convertor
        case other =>
          throw new RelationalException(
            s"CsvDynamicReaderIterable only accept DynamicCsvSource but here is ${other.getClass}")
      }
      val result =
        if (customConvertor != null)
          headers.zip(items).map { case (name, value) => name -> customConvertor.convert(name, value) }
        else
          headers.zip(items)
      new MapBasedDynamicObject(result.toMap)
    } else
      throw new RelationalException(
        "the first line in csv file should be header if yo want to create dynamic object from csv")
  }
}

/**
 * CSV Iterable will read csv file and return Entity instance.
 */
class CsvEntityReaderIterable[T <: Entity: TypeInfo](csvRelation: CsvMultiRelation[_])
    extends CsvReaderIterable[T](csvRelation) {

  lazy val metadata = csvRelation.source match {
    case source: CsvSource[_] => source.entity.info.propertyMetadata
    case other =>
      throw new RelationalException(s"CsvEntityReaderIterable only accepts CsvSource but here is ${other.getClass}")
  }

  private def propertyType(name: String): Class[_] = {
    if (metadata.contains(name)) {
      metadata(name).propertyClass
    } else
      throw new RuntimeException(
        "Cannot find " + name + " property in Entity " + csvRelation.source.asInstanceOf[CsvSource[_]].entity.info)
  }

  // we can only create it when we first read the file, because the input stream may not allow multiple read.
  private var defaultCtor: Option[Constructor[_]] = null

  /**
   * Map column data to Entity ctor according to the column name
   */
  override def createObject(items: List[String]): T = {
    if (csvRelation.source.hasHeader) {
      // Map column data to Entity ctor according to the column name

      // this is an alternate way of reading .csvs into entity, where if you have a constructor with names matching
      // the .csv headers, it will find that constructor and use it. Note this does NOT work with Option[ type ],
      // because we can parse the option, but due to type erasure, we don't know what we need to convert the contained
      // value into (i.e. in scala, this evaluates to TRUE: classOf[Option[Int]] == classOf[Option[String]] )
      // so if we don't match the conditions: (all members match), (no Option[]), then we fall back to the old way
      // where the data members of the entity have to match the .csv headers
      val headersSet = headers.toSet
      val matchingConstList = csvRelation.source.rowTypeClass.getConstructors
        .filter(aConstructor => headersSet == aConstructor.getParameters.map(_.getName).toSet)
        .toList

      // if isEmpty == true, it means it does NOT contain Option
      val matchingConstListWithoutOption =
        matchingConstList.filter(_.getParameterTypes.filter(paraType => paraType == classOf[Option[_]]).isEmpty)

      matchingConstListWithoutOption match {
        case singleConstructor :: Nil => {
          // this assumes that the header is in the same order as the constructor args, so need to set them that way
          val headerItemsMap = (headers zip items).toMap
          val orderedItems = singleConstructor.getParameters
            .map(_.getName)
            .map(k => headerItemsMap.getOrElse(k, throw new IllegalArgumentException(s"cannot find key $k in row")))
          val values = singleConstructor.getParameterTypes.zip(orderedItems).map { case (paraType, item) =>
            RelationalUtils.convertItemValue(item, paraType, null, csvRelation.source.timeZone)
          }
          singleConstructor.newInstance(values: _*).asInstanceOf[T]
        }
        case _ =>
          if (headers.size != items.size)
            throw new IllegalArgumentException("CSV header and column size should be same.")
          val itemMap: Map[String, AnyRef] = headers
            .zip(items)
            .iterator
            .collect {
              case (header, item) if metadata.contains(header) =>
                val proType = propertyType(header)
                val typeArgu =
                  if (proType == classOf[Option[_]]) {
                    val headerProp = metadata(header)
                    headerProp.typeTag.mirror.runtimeClass(headerProp.typeTag.tpe.typeArgs.head)
                  } else null
                header -> RelationalUtils.convertItemValue(item, proType, typeArgu, csvRelation.source.timeZone)
            }
            .toMap

          csvRelation.source
            .asInstanceOf[CsvSource[_]]
            .entity
            .info
            .createUnpickled(new PriqlPickledMapWrapper(itemMap))
            .asInstanceOf[T]
      }
    } else {
      // Map column data to Entity ctor according to the position
      if (defaultCtor == null) defaultCtor = csvRelation.source.rowTypeClass.getConstructors.find(c => {
        // The entityplugin will generate a constructor for pickling entities, which takes a PickledInputStream and a Boolean
        // as first two parameters, and StorageInfo and EntityReference as the following two parameters.
        // Since PickedInputStream is not storable, we can skip a constructor when its first parameter is a PickledInputStream
        val parameterTypes = c.getParameterTypes
        parameterTypes.length == items.length && !(parameterTypes.length >= 1 && parameterTypes(0) == classOf[
          PickledInputStream])
      })

      if (defaultCtor.isEmpty)
        throw new IllegalArgumentException(
          "Cannot find Entity ctor that has the same size of input parameters as CSV columns.")

      val values = defaultCtor.get.getParameterTypes.zip(items).map { case (paraType, item) =>
        RelationalUtils.convertItemValue(item, paraType, null, csvRelation.source.timeZone)
      }
      defaultCtor.get.newInstance(values: _*).asInstanceOf[T]
    }
  }
}
