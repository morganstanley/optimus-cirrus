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

import optimus.platform._
import optimus.platform.annotations.dimension
import optimus.platform.relational._
import java.time.ZonedDateTime
import java.time.LocalDate
import _root_.java.io._
import org.apache.poi.ss.usermodel.Sheet
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.poi.xssf.usermodel.XSSFCellStyle
import org.apache.poi.xssf.usermodel.XSSFDataFormat
import java.time.LocalDateTime
import optimus.platform.relational.dal.persistence._

trait PositionBase {
  @dimension def portfolio: String
  @dimension def symbol: String
  def quantity: Int
}

final case class Position(val portfolio: String, val symbol: String, val quantity: Int) extends PositionBase

final case class Price(@dimension val symbol: String, val price: Float) {}

final case class Portfolio(val id: String, val clientName: String) {}

final case class PositionValue(
    val portfolio: String,
    val symbol: String,
    val quantity: Int,
    val unitValue: Float,
    val positionValue: Float)

final case class Bond(val portfolio: String, val price: Float, val subject: String, val age: Float) {}

final case class DealProposal(
    val maker: String,
    val subject: String,
    val amount: _root_.java.lang.Float,
    val estimatedCommission: _root_.java.lang.Float) {}

trait Person {
  def getName: String
}

final case class PersonImpl(@scala.beans.BeanProperty val name: String) extends Person {}

@stored @entity class Employee(
    @key val employeeID: String,
    val firstName: String,
    val lastName: String,
    val birthday: ZonedDateTime = ZonedDateTime.now,
    val onboard: LocalDate = LocalDate.now,
    val age: Int = 20) {
  @entersGraph def birthday$init(): ZonedDateTime = ZonedDateTime.now
  @entersGraph def onboard$init(): LocalDate = LocalDate.now
  @entersGraph def age$init(): Int = 20

  def company(): String = "MS"
}

@entity class SqlTrde(val id: Int, val amount: Double, val symbol: String)

//@entity class  Trade(val tradeID: String, val symbol: String, vasployee) {}

object Data {

  def getPositionsFromJava(): java.util.Set[IPosition] = {
    val pos = new java.util.HashSet[IPosition]
    pos.add(new PositionImpl("James", "goog", 100))
    pos.add(new PositionImpl("James", "ibm", 400))
    pos.add(new PositionImpl("James", "tsco", 300))
    pos.add(new PositionImpl("James", "ibm", 400))
    pos.add(new PositionImpl("Kostya", "ibm", 4000))
    pos.add(new PositionImpl("Kostya", "ba", 100))
    pos.add(new PositionImpl("David", "tsco", 1000))
    pos.add(new PositionImpl("David", "ba", 200))
    pos
  }

  def getPositions(): Set[Position] = {
    Set(
      Position("James", "goog", 110),
      Position("James", "ibm", 400),
      Position("James", "msft", 500),
      Position("James", "ms", 2200),
      Position("James", "vod", 400),
      Position("James", "tsco", 300),
      Position("James", "gs", 1000),
      Position("James", "ba", 200),
      Position("Kostya", "ibm", 4000),
      Position("Kostya", "msft", 600),
      Position("Kostya", "ms", 2500),
      Position("Kostya", "vod", 200),
      Position("Kostya", "tsco", 100),
      Position("Kostya", "gs", 1500),
      Position("Kostya", "ba", 100),
      Position("David", "goog", 200),
      Position("David", "ibm", 2200),
      Position("David", "msft", 500),
      Position("David", "ms", 7000),
      Position("David", "vod", 200),
      Position("David", "tsco", 1000),
      Position("David", "gs", 1600),
      Position("David", "ba", 200)
    )
  }

  def getYesterdayPositions(): Set[Position] = {
    Set(
      Position("James", "yhoo", 100),
      Position("James", "ibm", 400),
      Position("James", "msft", 500),
      Position("James", "ms", 2200),
      Position("James", "vod", 400),
      Position("James", "tsco", 300),
      Position("James", "gs", 1000),
      Position("James", "ba", 200),
      Position("Kostya", "ibm", 4000),
      Position("Kostya", "msft", 600),
      Position("Kostya", "ms", 2500),
      Position("Kostya", "tsco", 100),
      Position("Kostya", "gs", 1500),
      Position("Kostya", "ba", 100),
      Position("Kostya", "hmc", 100),
      Position("David", "goog", 200),
      Position("David", "ibm", 2200),
      Position("David", "msft", 500),
      Position("David", "vod", 200),
      Position("David", "hmc", 1000),
      Position("David", "gs", 1600),
      Position("David", "ba", 200)
    )
  }

  def getPortfolio(): Set[Portfolio] = {
    Set(
      Portfolio("James", "James & Co."),
      Portfolio("David", "Shanghai Inc."),
      Portfolio("Kostya", "Roga i Kopyta Ltd."),
      Portfolio("NoPos", "Lazy Trader Gmbh."))
  }

  def getPrices(): Set[Price] = {
    Set(
      Price("goog", 485.7f),
      Price("ibm", 125.47f),
      Price("msft", 25.71f),
      Price("ms", 27.16f),
      Price("vod", 20.13f),
      Price("tsco", 412.45f),
      Price("gs", 144.40f),
      Price("ba", 64.6f)
    )
  }

  def getYesterdaysClose(): Set[Price] = {
    Set(
      Price("goog", 486.8f),
      Price("ibm", 115.47f),
      Price("msft", 31.73f),
      Price("ms", 30.46f),
      Price("vod", 33.13f),
      Price("tsco", 423.45f),
      Price("gs", 142.39f),
      Price("ba", 60.6f)
    )
  }

  def getPositionValues() =
    for (
      (pos, price) <- from(getPositions())
        .innerJoin(from(getPrices()))
        .on((pos, p) => pos.symbol == p.symbol)
    )
      yield PositionValue(
        pos.symbol,
        pos.portfolio,
        pos.quantity,
        price.price,
        positionValue = price.price * pos.quantity)

  def getBonds(): Set[Bond] = {
    Set(
      Bond("James", 39, "MMo Inc.", 3),
      Bond("James", 45, "CosPi Plc.", 5),
      Bond("Kostya", 37, "Piffo", 4),
      Bond("Kostya", 37, "Piffo", age = 8),
      Bond("David", 73, "S2k", 6))

  }

  def getPropDeals(): Set[DealProposal] = {
    Set(
      DealProposal("Trubach", "IPO", null, null),
      DealProposal("Barabanchik", "IPO", 1965, null),
      DealProposal("Trubach", "Issue", null, 123),
      DealProposal("Trubach", "IPO", 1200, 99)
    )
  }

  @stored @entity class RichEntity(
      @key val name: String,
      val birthday: LocalDate,
      val onboardday: ZonedDateTime,
      val age: Int,
      val isMale: Boolean,
      val price: Double,
      val sum: Double) {}

  @stored @entity class PoorEntity(@key val entityName: String, val entityAge: Int, val entityBirthday: LocalDate) {}

  def createSheetWithHeader: Sheet = {
    val wb = new XSSFWorkbook()
    val sheet = wb.createSheet("mynewsheet")
    val heads = List("name", "birthday", "age", "onboardday", "isMale", "price").zipWithIndex // no sum
    val row = sheet.createRow(0)
    heads foreach { case (n, i) => { row.createCell(i); row.getCell(i).setCellValue(n) } }
    sheet
  }

  def createExampleSheetWithHeader: Sheet = {
    val wb = new XSSFWorkbook()
    val sheet = wb.createSheet("Sheet1")
    val row0 = sheet.createRow(0);
    row0.createCell(0).setCellValue("name");
    row0.createCell(1).setCellValue("birthday");
    row0.createCell(2).setCellValue("onboardday");
    row0.createCell(3).setCellValue("age");
    row0.createCell(4).setCellValue("isMale");
    row0.createCell(5).setCellValue("price");
    row0.createCell(6).setCellValue("sum");
    val row1 = sheet.createRow(1);
    val style = wb.createCellStyle().asInstanceOf[XSSFCellStyle]
    val df = wb.createDataFormat().asInstanceOf[XSSFDataFormat]
    style.setDataFormat(df.getFormat("m/d/yy"));
    row1.createCell(0).setCellValue("James");
    row1.createCell(1).setCellValue(41233.62347);
    var tempcell = row1.createCell(2)
    tempcell.setCellValue(41233.62347)
    tempcell.setCellStyle(style)
    row1.createCell(3).setCellValue(44);
    row1.createCell(4).setCellValue(true);
    row1.createCell(5).setCellValue(234.89);
    row1.createCell(6).setCellValue(18219.49605);

    val row2 = sheet.createRow(2);
    row2.createCell(0).setCellValue("David");
    row2.createCell(1).setCellValue(41233.62347);
    tempcell = row2.createCell(2)
    tempcell.setCellValue(41233.62347)
    tempcell.setCellStyle(style)
    row2.createCell(3).setCellValue(62);
    row2.createCell(4).setCellValue(true);
    row2.createCell(5).setCellValue(233.65);
    row2.createCell(6).setCellValue(2344.49605);

    sheet
  }

  def createExampleSheetWithoutHeader: Sheet = {
    val wb = new XSSFWorkbook()
    val sheet = wb.createSheet("Sheet1")

    val row1 = sheet.createRow(0);
    val style = wb.createCellStyle().asInstanceOf[XSSFCellStyle]
    val df = wb.createDataFormat().asInstanceOf[XSSFDataFormat]
    style.setDataFormat(df.getFormat("m/d/yy"));
    row1.createCell(0).setCellValue("James");
    row1.createCell(1).setCellValue(41233.62347);
    var tempcell = row1.createCell(2)
    tempcell.setCellValue(41233.62347)
    tempcell.setCellStyle(style)
    row1.createCell(3).setCellValue(44);
    row1.createCell(4).setCellValue(true);
    row1.createCell(5).setCellValue(234.89);
    row1.createCell(6).setCellValue(18219.49605);

    val row2 = sheet.createRow(1);
    row2.createCell(0).setCellValue("David");
    row2.createCell(1).setCellValue(41233.62347);
    tempcell = row2.createCell(2)
    tempcell.setCellValue(41233.62347)
    tempcell.setCellStyle(style)
    row2.createCell(3).setCellValue(62);
    row2.createCell(4).setCellValue(true);
    row2.createCell(5).setCellValue(233.65);
    row2.createCell(6).setCellValue(2344.49605);

    sheet
  }
}
