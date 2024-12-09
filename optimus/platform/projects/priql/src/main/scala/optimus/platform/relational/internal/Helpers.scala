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

import java.nio.charset.StandardCharsets

import java.time.{LocalDate, ZoneId, ZonedDateTime}
import optimus.platform._
import optimus.platform.relational.tree._
import org.apache.commons.io.IOUtils
import org.apache.poi.ss.usermodel.{Cell, CellStyle, Row, Sheet, Workbook}

trait CommonHelper {

  /**
   * it is the same with RelationOutputter
   */
  protected def prettifyName(name: String): String = {
    // remove the _n. prefix for tuples
    val fieldName = name.split("\\.").last

    // remove get and fix the case for java-style methods
    if (fieldName.startsWith("get")) {
      val propName = fieldName.substring(3)
      propName.head.toLower + propName.tail
    } else fieldName
  }
}

/**
 * A helper object to fill Relation data to Excel sheet, and convert LocalDate and ZonedDateTime to targetTimeZone
 */
object ExcelHelper extends CommonHelper {

  def fillSheetWithTupleResult[LeftType: TypeInfo, RightType: TypeInfo](
      results: Iterable[(LeftType, RightType)],
      sheet: Sheet,
      hasHeader: ExcelHeader.ExcelHeader,
      timeZone: ZoneId): Unit = {
    val headerIndex = 0
    val leftValueMap =
      typeInfo[LeftType].propertyNames.map(p => (p, typeInfo[LeftType].propertyValue(p, results.head._1)))
    val rightValueMap =
      typeInfo[RightType].propertyNames.map(p => (p, typeInfo[RightType].propertyValue(p, results.head._2)))
    val columnHeaders: Map[String, (Int, CellStyle)] = findColumnHeaderAndStyle(
      sheet,
      headerIndex,
      hasHeader,
      leftValueMap.toMap ++ rightValueMap.toMap,
      typeInfo[LeftType].propertyNames ++ typeInfo[RightType].propertyNames)

    // set cell style as the default column style
    columnHeaders.foreach {
      case (name, (idx, style)) => {
        if (sheet.getColumnWidth(idx) == 2048) sheet.setColumnWidth(idx, 16 * 256)
      }
    }

    var rowIndex = headerIndex + 1
    for ((r1, r2) <- results) {
      val leftValueMap = typeInfo[LeftType].propertyNames.map(p =>
        (
          p,
          if (timeZone == null) typeInfo[LeftType].propertyValue(p, r1)
          else RelationalUtils.convertToZonedDateTime(typeInfo[LeftType].propertyValue(p, r1), timeZone)))
      val rightValueMap = typeInfo[RightType].propertyNames.map(p =>
        (
          p,
          if (timeZone == null) typeInfo[RightType].propertyValue(p, r2)
          else RelationalUtils.convertToZonedDateTime(typeInfo[RightType].propertyValue(p, r2), timeZone)))

      fillRow(columnHeaders, sheet.createRow(rowIndex), leftValueMap.toMap ++ rightValueMap.toMap, timeZone)
      rowIndex += 1
    }
  }

  def fillSheetWithGroupResult[RowType: TypeInfo, Key](
      results: Map[Key, Iterable[RowType]],
      sheet: Sheet,
      hasHeader: ExcelHeader.ExcelHeader,
      timeZone: ZoneId): Unit = {
    val headerIndex = 0
    val firstRowValueMap =
      typeInfo[RowType].propertyNames.map(p => (p, typeInfo.propertyValue(p, results.head._2.head)))
    val columnHeaders: Map[String, (Int, CellStyle)] =
      findColumnHeaderAndStyle(sheet, headerIndex, hasHeader, firstRowValueMap.toMap, typeInfo[RowType].propertyNames)

    // set cell style as the default column style
    columnHeaders.foreach {
      case (name, (idx, style)) => {
        if (sheet.getColumnWidth(idx) == 2048) sheet.setColumnWidth(idx, 16 * 256)
      }
    }

    var rowIndex = headerIndex + 1
    for (rs <- results) {
      val rows = rs._2
      for (r <- rows) {
        val rowValueMap = typeInfo[RowType].propertyNames.map(p =>
          (
            p,
            if (timeZone == null) typeInfo[RowType].propertyValue(p, r)
            else RelationalUtils.convertToZonedDateTime(typeInfo[RowType].propertyValue(p, r), timeZone)))

        fillRow(columnHeaders, sheet.createRow(rowIndex), rowValueMap.toMap, timeZone)
        rowIndex += 1
      }
    }
  }

  def fillSheetWithNormalResult[RowType: TypeInfo](
      results: Iterable[RowType],
      sheet: Sheet,
      hasHeader: ExcelHeader.ExcelHeader,
      timeZone: ZoneId): Unit = {
    if (results.head.isInstanceOf[DynamicObject])
      fillSheetWithDynamicResult(results.asInstanceOf[Iterable[DynamicObject]], sheet, hasHeader, timeZone)
    else {
      val headerIndex = 0
      val firstRowValueMap = implicitly[TypeInfo[RowType]].propertyNames.map(p =>
        (prettifyName(p), implicitly[TypeInfo[RowType]].propertyValue(p, results.head)))
      val columnHeaders: Map[String, (Int, CellStyle)] = findColumnHeaderAndStyle(
        sheet,
        headerIndex,
        hasHeader,
        firstRowValueMap.toMap,
        implicitly[TypeInfo[RowType]].propertyNames)

      // set cell style as the default column style
      columnHeaders.foreach {
        case (name, (idx, style)) => {
          if (sheet.getColumnWidth(idx) == 2048) sheet.setColumnWidth(idx, 16 * 256)
        }
      }

      var rowIndex = headerIndex + 1
      for (r <- results) {
        val rowValueMap = implicitly[TypeInfo[RowType]].propertyNames.map(p =>
          (
            prettifyName(p),
            if (timeZone == null) implicitly[TypeInfo[RowType]].propertyValue(p, r)
            else RelationalUtils.convertToZonedDateTime(implicitly[TypeInfo[RowType]].propertyValue(p, r), timeZone)))

        fillRow(columnHeaders, sheet.createRow(rowIndex), rowValueMap.toMap, timeZone)
        rowIndex += 1
      }
    }
  }

  def fillSheetWithDynamicResult(
      results: Iterable[DynamicObject],
      sheet: Sheet,
      hasHeader: ExcelHeader.ExcelHeader,
      timeZone: ZoneId): Unit = {
    val headerIndex = 0 // default to the first row
    val firstRowValueMap = results.head.getAll

    val columnHeaders: Map[String, (Int, CellStyle)] =
      findColumnHeaderAndStyle(sheet, headerIndex, hasHeader, firstRowValueMap, firstRowValueMap.keys.toSeq.sorted)

    // set cell style as the default column style
    columnHeaders.foreach {
      case (name, (idx, style)) => {
        if (sheet.getColumnWidth(idx) == 2048) sheet.setColumnWidth(idx, 16 * 256)
      }
    }

    var rowIndex = headerIndex + 1
    for (r <- results) {
      val rowValueMap = r.getAll
      val newMap =
        if (timeZone != null)
          rowValueMap.map { case (n, v) => (n, RelationalUtils.convertToZonedDateTime(v, timeZone)) }
        else rowValueMap

      fillRow(columnHeaders, sheet.createRow(rowIndex), newMap, timeZone)
      rowIndex += 1
    }
  }

  private def fillRow(
      headerMap: Map[String, (Int, CellStyle)],
      row: Row,
      valueMap: Map[String, Any],
      timeZone: ZoneId): Unit = {
    headerMap foreach {
      case (name, (idx, style)) => {
        val cell = row.createCell(idx)
        fillCell(cell, valueMap(name), timeZone)
        cell.setCellStyle(style)
      }
    }
  }

  private def fillCell(cell: Cell, item: Any, timeZone: ZoneId): Unit = {
    item match {
      case null                 =>
      case i: String            => cell.setCellValue(i)
      case i: Int               => cell.setCellValue(i)
      case i: java.lang.Integer => cell.setCellValue(i.doubleValue)
      case i: Long              => cell.setCellValue(i.toDouble)
      case i: java.lang.Long    => cell.setCellValue(i.doubleValue)
      case i: Double            => cell.setCellValue(i)
      case i: java.lang.Double  => cell.setCellValue(i.doubleValue)
      case i: Boolean           => cell.setCellValue(i)
      case i: java.lang.Boolean => cell.setCellValue(i.booleanValue)
      case i: Float             => cell.setCellValue(i)
      case i: java.lang.Float   => cell.setCellValue(i.floatValue)
      case i: Byte              => cell.setCellValue(i)
      case i: java.lang.Byte    => cell.setCellValue(i.doubleValue)
      case i: Short             => cell.setCellValue(i)
      case i: java.lang.Short   => cell.setCellValue(i.doubleValue)
      case i: Char              => cell.setCellValue(i.toString)
      case i: BigDecimal        => cell.setCellValue(i.toString)
      case i: BigInt            => cell.setCellValue(i.toString)
      case i: LocalDate =>
        cell.setCellValue(new java.util.GregorianCalendar(i.getYear, i.getMonthValue - 1, i.getDayOfMonth).getTime)
      case i: ZonedDateTime =>
        val zd = RelationalUtils.convertZonedDateTimeToZonedDateTime(i, timeZone)
        cell.setCellValue(RelationalUtils.convertZonedDateTime2JUDate(zd))
      case i =>
        throw new IllegalArgumentException("Cannot convert " + item.toString + " to " + i.getClass())
    }
  }

  private def createDefaultCellStyles(wb: Workbook, itemClass: Class[_]): CellStyle = {
    val style = wb.createCellStyle()
    itemClass match {
      case c if (c == classOf[String] || c == classOf[Char]) =>
        style.setDataFormat(wb.createDataFormat().getFormat("text"))
      case c
          if (c == classOf[Int] || c == classOf[Long] || c == classOf[Byte] || c == classOf[Short] ||
            c == classOf[java.lang.Integer] || c == classOf[java.lang.Long] || c == classOf[
              java.lang.Byte] || c == classOf[java.lang.Short]) =>
        style.setDataFormat(wb.createDataFormat().getFormat("0"))
      case c
          if (c == classOf[Double] || c == classOf[Float] || c == classOf[BigDecimal] ||
            c == classOf[java.lang.Double] || c == classOf[java.lang.Float]) =>
        style.setDataFormat(wb.createDataFormat().getFormat("0.0000"))
      case c if (c == classOf[LocalDate])     => style.setDataFormat(wb.createDataFormat().getFormat("m/d/yyyy"))
      case c if (c == classOf[ZonedDateTime]) => style.setDataFormat(wb.createDataFormat().getFormat("m/d/yyyy h:mm"))
      case _                                  =>
    }
    style
  }

  private def findColumnHeaderAndStyle(
      sheet: Sheet,
      headerIndex: Int,
      hasHeader: ExcelHeader.ExcelHeader,
      firstRowValueMap: Map[String, Any],
      fieldOrdering: Seq[String]): Map[String, (Int, CellStyle)] = {
    if (hasHeader == ExcelHeader.FIRSTLINE) {
      findColumnHeaders(sheet, headerIndex, firstRowValueMap) // Rows are 0 based
    } else {
      // create new headers, and add headers to excel
      val newheaders = fieldOrdering.zipWithIndex map { case (name, idx) =>
        (name, (idx, createDefaultCellStyles(sheet.getWorkbook(), firstRowValueMap(name).getClass())))
      }
      val headerRow = sheet.createRow(headerIndex)
      newheaders foreach {
        case (n, (idx, s)) => {
          val cell = headerRow.createCell(idx)
          cell.setCellStyle(createDefaultCellStyles(sheet.getWorkbook(), classOf[String]))
          cell.setCellValue(n)
        }
      }

      newheaders.toMap
    }
  }

  /**
   * it's better to get fieldName->fieldValue mapping and use fieldValue to get return type for DynamicObject and
   * Anonymous Type
   */
  private def findColumnHeaders(
      sheet: Sheet,
      headRowIndex: Int,
      firstRowValueMap: Map[String, Any]): Map[String, (Int, CellStyle)] = {
    val map = scala.collection.mutable.Map.empty[String, (Int, CellStyle)]

    val headerrow = sheet.getRow(headRowIndex)
    if (headerrow == null)
      throw new optimus.platform.relational.RelationalException(
        "Excel's first line is not header, please use ExcelHeader.NO_HEADER")

    val datarow = sheet.getRow(headRowIndex + 1)
    val minColIx: Int = headerrow.getFirstCellNum()
    val maxColIx: Int = headerrow.getLastCellNum()

    for (colIx <- minColIx until maxColIx) {
      val headerCell = headerrow.getCell(colIx)
      if (headerCell != null) {
        val headerName = headerCell.getStringCellValue()
        val dataCell = if (datarow != null) datarow.getCell(colIx) else null
        val dataCellStyle =
          if (dataCell != null) dataCell.getCellStyle()
          else createDefaultCellStyles(sheet.getWorkbook(), firstRowValueMap(headerName).getClass)

        map(headerName) = (colIx, dataCellStyle)
      }
    }
    map.toMap
  }
}

/**
 * A helper object to write data to Csv file
 */
// Remove this object if it is never used.
object CsvHelper extends CommonHelper {

  def writeNoramlDynamicResult(
      results: Iterable[DynamicObject],
      typeInfo: TypeInfo[_],
      out: java.io.OutputStream,
      timeZone: ZoneId): Unit = {
    val fieldNames = results.head.getAll.keys
    val headerLine: String = fieldNames.map(f => quoteCsvData(f, null)).mkString(",")
    IOUtils.write(headerLine + System.lineSeparator(), out, StandardCharsets.UTF_8)

    results.foreach(f => {
      val valueMap = f.getAll
      var bodyLine: String = null
      bodyLine = fieldNames
        .map(d => {
          val a = valueMap.get(d).get
          quoteCsvData(a, timeZone)
        })
        .mkString(",")
      IOUtils.write(bodyLine + System.lineSeparator(), out, StandardCharsets.UTF_8)
    })
  }

  def writeGroupResult[RowType: TypeInfo, Key](
      results: Map[Key, Iterable[RowType]],
      out: java.io.OutputStream,
      timeZone: ZoneId): Unit = {
    val properties = typeInfo[RowType].propertyNames
    val headerLine: String = properties.map(f => quoteCsvData(f, null)).mkString(",")
    IOUtils.write(headerLine + System.lineSeparator(), out, StandardCharsets.UTF_8)

    for ((_, rows) <- results; r <- rows) {
      val bodyLine = properties
        .map(p => {
          quoteCsvData(typeInfo.propertyValue(p, r), timeZone)
        })
        .mkString(",")
      IOUtils.write(bodyLine + System.lineSeparator(), out, StandardCharsets.UTF_8)
    }
  }

  /**
   * Dynamic, Join, Untype, Entity, Normal class
   */
  def writeNormalResult[RowType](
      results: Iterable[RowType],
      typeInfo: TypeInfo[_],
      out: java.io.OutputStream,
      timeZone: ZoneId): Unit = {
    if (results.head.isInstanceOf[DynamicObject])
      writeNoramlDynamicResult(results.asInstanceOf[Iterable[DynamicObject]], typeInfo, out, timeZone)
    else {
      val methodNames = typeInfo.propertyNames
      val headerLine: String = methodNames.map(f => quoteCsvData(prettifyName(f), null)).mkString(",")
      IOUtils.write(headerLine + System.lineSeparator(), out, StandardCharsets.UTF_8)

      results.foreach(r => {
        val bodyLine = methodNames.map(m => {
          val fieldValue = typeInfo.propertyValue(m, r)

          quoteCsvData(fieldValue, timeZone)
        })
        val line = bodyLine.mkString(",")
        IOUtils.write(line + System.lineSeparator(), out, StandardCharsets.UTF_8)
      })
    }
  }

  private def quoteCsvData(fieldValue: Any, timeZone: ZoneId): String = {
    val a = if (timeZone != null) RelationalUtils.convertToZonedDateTime(fieldValue, timeZone) else fieldValue
    if (a == null) ""
    else
      a match {
        case i: java.lang.Number => i.toString()
        case i: ZonedDateTime if (timeZone != null) =>
          "\"" + i.toOffsetDateTime.toLocalDateTime.toString.replace("T", " ") + "\""
        case _ => "\"" + a.toString.replace("\"", "\"\"") + "\""
      }
  }
}

object ExcelHeader extends Enumeration {
  type ExcelHeader = Value

  val FIRSTLINE = Value(0)
  val NO_HEADER = Value(1)
}
