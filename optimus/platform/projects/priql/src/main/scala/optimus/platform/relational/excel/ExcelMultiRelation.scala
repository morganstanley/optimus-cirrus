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
package optimus.platform.relational.excel

import java.lang.reflect.Constructor
import java.util.{Calendar, GregorianCalendar}
import java.time.{LocalDate, ZonedDateTime}

import msjava.slf4jutils.scalalog._
import optimus.platform._
import optimus.platform.relational.RelationalException
import optimus.platform.relational.inmemory.{ArrangedSource, IterableSource}
import optimus.platform.relational.internal.{ExcelHeader, PriqlPickledMapWrapper}
import optimus.platform.relational.tree.{MethodPosition, ProviderRelation, QueryExplainItem, TypeInfo, typeInfo}
import optimus.platform.storable.Entity
import org.apache.poi.ss.usermodel.{CellType, CellStyle, DateUtil, Sheet, Workbook}
import org.apache.poi.ss.util.CellReference

import scala.collection.mutable.{HashMap, ListBuffer}

class ExcelMultiRelation[T <: Entity: TypeInfo](
    val source: ExcelSource[T],
    typeInfo: TypeInfo[T],
    key: RelationKey[_],
    pos: MethodPosition)
    extends ProviderRelation(typeInfo, key, pos)
    with IterableSource[T]
    with ArrangedSource {
  override def getProviderName = "ExcelProvider"

  override def prettyPrint(indent: Int, out: StringBuilder): Unit = {
    indentAndStar(indent, out)
    out ++= serial + " Provider:" + " '" + getProviderName + "' " + "of" + " type " + typeInfo.toString + "\n"
  }

  override def fillQueryExplainItem(level_id: Integer, table: ListBuffer[QueryExplainItem]) = {
    table += new QueryExplainItem(level_id, getProviderName, typeInfo.name, "ExcelFileScan", 0)
  }

  def isSyncSafe = true

  def getSync(): Iterable[T] = new ExcelReaderIterable[T](this)

  @async def get() = getSync()

  override def makeKey(newKey: RelationKey[_]) = new ExcelMultiRelation[T](source, typeInfo, newKey, pos)
}

object ExcelMultiRelation {
  def apply[T <: Entity: TypeInfo](
      source: ExcelSource[T],
      key: RelationKey[_],
      pos: MethodPosition): ExcelMultiRelation[T] = {
    new ExcelMultiRelation[T](source, typeInfo[T], key, pos)
  }

}

/**
 * when read from excel file there are two situations need to convert cell value to ZonedDateTime with default time zone
 * or LocalDate: 1 cell is Date formated(read directly from a cell) 2 cell type is numeric(through function call) do we
 * need to consider String type date e.g. YYYY-MM-DDDDTHH:MM:SS,millisec+08:00[ZoneId] and convert it to a specified
 * time zone ??
 */
class ExcelReaderIterable[T <: Entity: TypeInfo](val excelRelation: ExcelMultiRelation[T]) extends Iterable[T] {
  val log = getLogger[ExcelReaderIterable[_]]

  lazy val loadedData: List[T] = {
    val excelReader = new POIReader(
      excelRelation.source.sheet.getWorkbook(),
      excelRelation.source.sheet,
      excelRelation.source.startPoint,
      excelRelation.source.rows,
      excelRelation.source.cols
    )
    var defaultCtor: Option[Constructor[_]] = null

    val headers: AnyRef = {
      (excelRelation.source.hasHeader, excelReader.hasNext) match {
        case (ExcelHeader.FIRSTLINE, true) if excelRelation.source.entityToExcelFieldMap == null =>
          excelReader.next.asInstanceOf[List[String]]

        case (ExcelHeader.FIRSTLINE, true) =>
          val excelColumns = excelReader.next.asInstanceOf[List[String]]
          val entityToExcelIndexMap = new HashMap[String, Int]
          excelRelation.source.entityToExcelFieldMap.foreach {
            case (entityField, excelField) => {
              val index = excelColumns.indexOf(excelField)
              if (index != -1) {
                entityToExcelIndexMap.put(entityField, index)
              } else {
                throw new RelationalException(
                  "the columns filed you specify in right of map isn't in the excel column filed names: " + excelField)
              }
            }
          }
          entityToExcelIndexMap

        case (ExcelHeader.FIRSTLINE, false) =>
          throw new RelationalException("the first line in excel table should be header whose cell type is String")

        case (ExcelHeader.NO_HEADER, _) => List.empty

        case o => throw new MatchError(o) // exhaustivity doesn't support scala.Enumeration
      }
    }

    def propertyType(name: String): Class[_] = {
      if (excelRelation.source.entity.info.propertyMetadata.contains(name))
        excelRelation.source.entity.info.propertyMetadata(name).propertyClass
      else throw new RuntimeException("Cannot find " + name + " property in Entity " + excelRelation.source.entity.info)
    }

    def createEntity(items: List[AnyRef]): T = {
      var colIndex = 0
      if (excelRelation.source.hasHeader == ExcelHeader.FIRSTLINE) {
        val temp1 = if (excelRelation.source.entityToExcelFieldMap == null) {
          val line_headers = headers.asInstanceOf[List[String]]
          // Map column data to Entity ctor according to the column name
          if (line_headers.size != items.size)
            throw new IllegalArgumentException("excel header and column size should be same.")
          line_headers.zip(items)

        } else {
          val map_headers = headers.asInstanceOf[HashMap[String, Int]]
          val hash_temp1 = new HashMap[String, AnyRef]
          map_headers.foreach { case (field, index) => hash_temp1.put(field, items(index)) }
          hash_temp1.toMap
        }

        val itemMap: Map[String, AnyRef] = temp1.iterator.map {
          case (key, item) => {
            colIndex += 1;
            val value = convertItemValue(item, propertyType(key), key, colIndex)
            (key, value);
          }
        }.toMap
        excelRelation.source.entity.info.createUnpickled(new PriqlPickledMapWrapper(itemMap)).asInstanceOf[T]
      } else {
        // Map column data to Entity ctor according to the position
        if (defaultCtor == null)
          defaultCtor =
            excelRelation.source.rowTypeClass.getConstructors.find(c => c.getParameterTypes.length == items.length)

        if (defaultCtor.isEmpty)
          throw new IllegalArgumentException(
            "Cannot find Entity ctor that has the same size of input parameters as excel columns.")

        val values: List[AnyRef] = defaultCtor.get.getParameterTypes
          .zip(items)
          .iterator
          .map {
            case (paraType, item) => {
              colIndex += 1
              convertItemValue(item, paraType, colIndex.toString, colIndex)
            }
          }
          .toList
        defaultCtor.get.newInstance(values: _*).asInstanceOf[T]
      }
    }

    val tmpData = ListBuffer[T]()
    while (excelReader.hasNext) {
      try {
        val items = excelReader.next
        tmpData += createEntity(items)
      } catch {
        case e: Exception => throw new RelationalException("Cannot parse excel row to object.", e)
      }
    }
    tmpData.toList
  }

  def iterator: Iterator[T] = loadedData.iterator

  private def convertItemValue(item: AnyRef, itemClass: Class[_], fieldName: String, colIndex: Int): AnyRef = {
    if (itemClass == classOf[String]) {
      item match {
        case i: String => i
        case _ =>
          throw new RelationalException(
            "cannot convert: type for " + fieldName + "is String which is different from column " + colIndex + " type in excel")
      }
    } else if (itemClass == classOf[Int]) {
      item match {
        case i: java.lang.Double if (i.toInt == i) => int2Integer(i.toInt)
        case _ =>
          throw new RelationalException(
            "cannot convert: type for " + fieldName + "is Int which is different from column " + colIndex + " type in excel")
      }
    } else if (itemClass == classOf[Long]) {
      item match {
        case i: java.lang.Double if (i.toLong == i) => long2Long(i.toLong)
        case _ =>
          throw new RelationalException(
            "cannot convert: type for " + fieldName + "is Long which is different from column " + colIndex + " type in excel")
      }
    } else if (itemClass == classOf[Double]) {
      item match {
        case i: java.lang.Double => double2Double(i)
        case _ =>
          throw new RelationalException(
            "cannot convert: type for " + fieldName + "is Double which is different from column " + colIndex + " type in excel")
      }
    } else if (itemClass == classOf[Boolean]) {
      item match {
        case i: java.lang.Boolean => boolean2Boolean(i)
        case _ =>
          throw new RelationalException(
            "cannot convert: type for " + fieldName + "is Boolean which is different from column " + colIndex + " type in excel")
      }
    } else if (itemClass == classOf[Float]) {
      item match {
        case i: java.lang.Double => double2Double(i)
        case _ =>
          throw new RelationalException(
            "cannot convert: type for " + fieldName + "is Float which is different from column " + colIndex + " type in excel")
      }
    } else if (itemClass == classOf[Byte]) {
      item match {
        case i: java.lang.Double if (i.toByte == i) => byte2Byte(i.toByte)
        case _ =>
          throw new RelationalException(
            "cannot convert: type for " + fieldName + "is Byte which is different from column " + colIndex + " type in excel")
      }
    } else if (itemClass == classOf[Short]) {
      item match {
        case i: java.lang.Double if (i.toShort == i) => short2Short(i.toShort)
        case _ =>
          throw new RelationalException(
            "cannot convert: type for " + fieldName + "is Short which is different from column " + colIndex + " type in excel")
      }
    } else if (itemClass == classOf[BigDecimal]) {
      item match {
        case i: String           => BigDecimal(i)
        case i: java.lang.Double => BigDecimal(i)
        case _ =>
          throw new RelationalException(
            "cannot convert: type for " + fieldName + "is BigDecimal so column " + colIndex + " type in excel should be String or numeric")
      }
    } else if (itemClass == classOf[Char]) {
      item match {
        case i: String if (i.length() == 1) => char2Character(i.charAt(0))
        case _ =>
          throw new IllegalArgumentException(
            "cannot convert: type for " + fieldName + "is char so column " + colIndex + " type in excel should be String whose length is 1")
      }
    } else if (itemClass == classOf[LocalDate]) {
      val date = item match {
        case i: java.lang.Double => // it comes when we don't do the date format in excel or get from formula
          log.warn("a numeric field in excel is being converted to a LocalDate")
          DateUtil.getJavaDate(i)
        case i: java.util.Date =>
          i
        case _ =>
          throw new RelationalException(
            "cannot convert: type for " + fieldName + "is LocalDate so column " + colIndex + " type in excel should be numeric with or without date formatted")
      }
      val c = new GregorianCalendar()
      c.setTime(date)
      LocalDate.of(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH))
    } else if (itemClass == classOf[ZonedDateTime]) {
      val date = item match {
        case i: java.lang.Double => // it comes when we don't do the date format in excel or get from formula
          log.warn("a numeric field in excel is being converted to a ZonedDateTime")
          DateUtil.getJavaDate(i)
        case i: java.util.Date =>
          i
        case _ =>
          throw new RelationalException(
            "cannot convert: type for " + fieldName + "is ZonedDateTime so column " + colIndex + " type in excel should be numeric with or without date formatted")
      }
      val cd = new GregorianCalendar()
      cd.setTime(date)
      ZonedDateTime.of(
        cd.get(Calendar.YEAR),
        cd.get(Calendar.MONTH) + 1,
        cd.get(Calendar.DAY_OF_MONTH),
        cd.get(Calendar.HOUR_OF_DAY),
        cd.get(Calendar.MINUTE),
        cd.get(Calendar.SECOND),
        cd.get(Calendar.MILLISECOND) * 1000000,
        excelRelation.source.timeZone
      )
    } else throw new IllegalArgumentException("Cannot convert " + item + " to " + itemClass)
  }

}

class POIReader(val wb: Workbook, val sheet: Sheet, val startPoint: String, val rows: Int, val cols: Short) {

  val evaluator = wb.getCreationHelper().createFormulaEvaluator()
  val sRow = new CellReference(startPoint).getRow
  val sCol = new CellReference(startPoint).getCol
  var rowHaveRead = 0

  def hasNext: Boolean = rowHaveRead != rows

  /**
   * Get cell values of next row in the sheet
   */
  def next: List[AnyRef] = {
    val cellValues = new ListBuffer[AnyRef]
    val currentRow = sheet.getRow(sRow + rowHaveRead)
    for (i <- 0 until cols) {
      val cell = currentRow.getCell(sCol + i)
      cell.getCellType match {
        case CellType.STRING => cellValues += cell.getStringCellValue()
        case CellType.NUMERIC =>
          if (DateUtil.isCellDateFormatted(cell)) cellValues += cell.getDateCellValue()
          else cellValues += double2Double(cell.getNumericCellValue())
        case CellType.BOOLEAN => cellValues += boolean2Boolean(cell.getBooleanCellValue())
        case CellType.FORMULA => {
          val cellValue = evaluator.evaluate(cell)
          cellValue.getCellType() match {
            case CellType.STRING => cellValues += cellValue.getStringValue()
            case CellType.NUMERIC =>
              cellValues += double2Double(
                cellValue.getNumberValue()
              ) // it might be a Date but cannot check whether it is date formated
            case CellType.BOOLEAN => cellValues += boolean2Boolean(cellValue.getBooleanValue())
            case _ => throw new RelationalException("this type should not be the result type of a formula")
          }
        }
        case _ =>
          throw new RelationalException(
            "this type in the excel cell isn't supported to convert to an entity object " + cell.getCellType)
      }

    }
    rowHaveRead += 1
    cellValues.toList
  }

  /**
   * Get header values of the sheet
   */
  def findHeaders(): Map[String, (Int, CellStyle)] = {
    import scala.collection.mutable.Map

    val map = Map.empty[String, (Int, CellStyle)]

    val headerrow = sheet.getRow(sRow)
    if (headerrow == null) {
      map.toMap
    } else {
      val defaultCellStyle = sheet.getWorkbook().createCellStyle()
      val datarow = sheet.getRow(sRow + 1)
      val minColIx: Int = sCol
      val maxColIx: Int = sCol + cols

      for (colIx <- minColIx until maxColIx) {
        val headerCell = headerrow.getCell(colIx)
        if (headerCell != null) {
          val headerName = headerCell.getStringCellValue()
          val dataCell = datarow.getCell(colIx)
          val dataCellStyle = if (dataCell != null) dataCell.getCellStyle() else defaultCellStyle
          map(headerName) = (colIx, dataCellStyle)
        }
      }
      rowHaveRead += 1
      map.toMap
    }
  }

}
