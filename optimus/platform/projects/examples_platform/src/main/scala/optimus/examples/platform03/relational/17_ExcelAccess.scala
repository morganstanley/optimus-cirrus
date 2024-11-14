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
package optimus.examples.platform03.relational

import optimus.utils.datetime.ZoneIds
import optimus.platform._
import optimus.platform.relational._
import optimus.examples.platform03.relational.Data.RichEntity
import optimus.examples.platform03.relational.Data.PoorEntity
import java.time.LocalDate
import java.time.ZonedDateTime
import java.time.ZoneId
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import java.io.FileInputStream
import org.apache.poi.ss.usermodel.Workbook
import java.io.InputStream
import org.apache.poi.poifs.filesystem.POIFSFileSystem
import org.apache.poi.poifs.crypt.EncryptionInfo
import org.apache.poi.poifs.crypt.Decryptor
import optimus.platform.relational.RelationalException
import java.security.GeneralSecurityException
import optimus.platform.relational.excel.ExcelSource
import optimus.platform.relational.internal.ExcelHeader

object ExcelAccess extends LegacyOptimusApp {

  val data = Set(
    RichEntity(
      "david",
      LocalDate.of(2000, 1, 2),
      ZonedDateTime.of(2000, 11, 12, 13, 14, 15, 0, ZoneIds.UTC),
      20,
      true,
      123.55,
      200.888),
    RichEntity(
      "jessica",
      LocalDate.of(1990, 3, 3),
      ZonedDateTime.of(2010, 1, 2, 3, 4, 5, 0, ZoneIds.UTC),
      18,
      false,
      3344.5,
      600.1)
  )

  excelReaderWithHeader
  excelReaderWithoutHeader
  excelWriterWithHeaderAndCustomStyle
  excelWriterFillAnonClassAndWithoutHeader
  excelReaderWithColumnMapping
  // excelReaderWithPasswordProtected

  private def decryptionExcelFile(fileToOpen: InputStream, passwd: String): Workbook = {
    val poifs = new POIFSFileSystem(fileToOpen)
    val info = new EncryptionInfo(poifs)
    val d = Decryptor.getInstance(info)
    try {
      if (!d.verifyPassword(passwd)) {
        throw new RelationalException("Unable to process: password is not correct to " + fileToOpen)
      }

      val dataStream = d.getDataStream(poifs)
      val wb = new XSSFWorkbook(dataStream)
      wb
    } catch {
      case ex: GeneralSecurityException =>
        throw new RelationalException("Unable to process encrypted document: " + fileToOpen, ex);
    }
  }

  /*
  def excelReaderWithPasswordProtected = {
    val fileToOpen = new FileInputStream("YOUR_FILE_PATH")
    val wb = decryptionExcelFile(fileToOpen, "YOUR_PASSWD")
    val excelSource1 = ExcelSource(RichEntity, wb.getSheet("SHEET_NAME"), ExcelHeader.FIRSTLINE, ZoneIds.UTC, null)
    val query = from(excelSource1).filter(t => t.name == "James")
    val actual = Query.execute(query)
    actual.foreach(f => println("name: " + f.name + " age: " + f.age + " birthday: " + f.birthday + " onboardday: " + f.onboardday))
  }
   *
   */

  def excelReaderWithColumnMapping = {
    val entityToExcelMap = new scala.collection.mutable.HashMap[String, String]
    entityToExcelMap.put("entityName", "name")
    entityToExcelMap.put("entityAge", "age")
    entityToExcelMap.put("entityBirthday", "onboardday")

    val excelSource1 =
      ExcelSource(PoorEntity, Data.createExampleSheetWithHeader, ExcelHeader.FIRSTLINE, ZoneIds.UTC, entityToExcelMap)
    val query = from(excelSource1).filter(t => t.entityName == "David")
    val actual = Query.execute(query)
    actual.foreach(f => println("name: " + f.entityName + " age: " + f.entityAge + " birthday: " + f.entityBirthday))
  }

  def excelReaderWithHeader = {

    val excelSource1 = ExcelSource(RichEntity, Data.createExampleSheetWithHeader, ExcelHeader.FIRSTLINE, ZoneIds.UTC)
    val query = from(excelSource1).filter(t => t.name == "James")
    val actual = Query.execute(query)
    actual.foreach(f =>
      println("name: " + f.name + " age: " + f.age + " birthday: " + f.birthday + " onboardday: " + f.onboardday))
  }

  def excelReaderWithoutHeader = {
    val excelSource2 = ExcelSource(RichEntity, Data.createExampleSheetWithoutHeader, ExcelHeader.NO_HEADER, ZoneIds.UTC)
    val query = from(excelSource2).filter(t => t.name == "James")
    val actual = Query.execute(query)
    actual.foreach(f =>
      println("name: " + f.name + " age: " + f.age + " birthday: " + f.birthday + " onboardday: " + f.onboardday))
  }

  def excelWriterWithHeaderAndCustomStyle = {
    val wb = new XSSFWorkbook()

    // Define templates
    val stringTemplateStyle = wb.createCellStyle()
    stringTemplateStyle.setDataFormat(wb.createDataFormat().getFormat("text"))
    val dateTimeTemplateStyle = wb.createCellStyle()
    dateTimeTemplateStyle.setDataFormat(wb.createDataFormat().getFormat("yyyy-m-d h:mm:ss"))
    val numTemplateStyle = wb.createCellStyle()
    numTemplateStyle.setDataFormat(wb.createDataFormat().getFormat("0.0000"))
    val intTemplateStyle = wb.createCellStyle()
    intTemplateStyle.setDataFormat(wb.createDataFormat().getFormat("0"))

    val sheet = wb.createSheet("mynewsheet")
    val columNameRow = sheet.createRow(0)
    val columFormatRow = sheet.createRow(1)

    columNameRow.createCell(0).setCellValue("name")
    columFormatRow.createCell(0) setCellStyle (stringTemplateStyle)

    columNameRow.createCell(1).setCellValue("birthday")
    columFormatRow.createCell(1) setCellStyle (dateTimeTemplateStyle)

    columNameRow.createCell(2).setCellValue("age")
    columFormatRow.createCell(2) setCellStyle (intTemplateStyle)
    columNameRow.createCell(3).setCellValue("onboardday")
    columFormatRow.createCell(3) setCellStyle (dateTimeTemplateStyle)
    columNameRow.createCell(4).setCellValue("price")
    columFormatRow.createCell(4) setCellStyle (numTemplateStyle)

    val q1 = for (t <- from(data)) yield t
    Query.fillExcelSheet(q1, sheet, ExcelHeader.FIRSTLINE)
  }

  def excelWriterFillAnonClassAndWithoutHeader = {
    val q1 =
      for (t <- from(data))
        yield new {
          val name = t.name; val birthday = t.birthday; val onboardday = t.onboardday; val age = t.age;
          val isMale = t.isMale; val price = t.price; val sum = t.sum
        }
    val wb = new XSSFWorkbook()
    val sheet = wb.createSheet("mynewsheet")
    Query.fillExcelSheet(q1, sheet, ExcelHeader.NO_HEADER)
  }
}
