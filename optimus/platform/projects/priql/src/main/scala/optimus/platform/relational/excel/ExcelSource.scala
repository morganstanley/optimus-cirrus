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

import optimus.utils.datetime.ZoneIds
import java.time.ZoneId

import optimus.platform.relational.RelationalException
import optimus.platform.relational.internal.ExcelHeader
import optimus.platform.relational.tree._
import optimus.platform.storable.{Entity, EntityCompanionBase}
import org.apache.poi.ss.usermodel.Sheet
import org.apache.poi.ss.util.CellReference

import scala.collection.mutable

class ExcelSource[RowType <: Entity: TypeInfo](
    val entity: EntityCompanionBase[RowType],
    val timeZone: ZoneId,
    val sheet: Sheet,
    val startPoint: String,
    val rows: Int,
    val cols: Short,
    val hasHeader: ExcelHeader.ExcelHeader,
    val entityToExcelFieldMap: mutable.Map[String, String]) {
  if (entity == null) throw new IllegalArgumentException("Does not provide entity")
  private[optimus] val rowType = typeInfo[RowType]
  private[optimus] val rowTypeClass: Class[RowType] = rowType.runtimeClass
}

object ExcelSource {

  def apply[RowType <: Entity: TypeInfo](
      entity: EntityCompanionBase[RowType],
      sheet: Sheet,
      hasHeader: ExcelHeader.ExcelHeader): ExcelSource[RowType] = apply(entity, sheet, hasHeader, ZoneIds.UTC)

  def apply[RowType <: Entity: TypeInfo](
      entity: EntityCompanionBase[RowType],
      sheet: Sheet,
      hasHeader: ExcelHeader.ExcelHeader,
      columnMap: mutable.Map[String, String]): ExcelSource[RowType] =
    apply(entity, sheet, hasHeader, ZoneIds.UTC, columnMap)

  def apply[RowType <: Entity: TypeInfo](
      entity: EntityCompanionBase[RowType],
      sheet: Sheet,
      hasHeader: ExcelHeader.ExcelHeader,
      timeZone: ZoneId,
      columnMap: mutable.Map[String, String]): ExcelSource[RowType] = {
    if (entity == null || sheet == null) throw new RelationalException("entity and sheet shouldn't be null")
    val sRow = sheet.getFirstRowNum
    val eRow = sheet.getLastRowNum
    val sCol = sheet.getRow(sRow).getFirstCellNum()
    val eCol = sheet.getRow(eRow).getLastCellNum()
    if (sRow == -1 || eRow == -1 || sCol == -1 || eCol == -1)
      throw new RelationalException("there isn't any table in this sheet")
    val cellRef = new CellReference(sRow, sCol)
    new ExcelSource(
      entity,
      timeZone,
      sheet,
      cellRef.formatAsString(),
      eRow - sRow + 1,
      (eCol - sCol).toShort,
      hasHeader,
      columnMap)
  }

  def apply[RowType <: Entity: TypeInfo](
      entity: EntityCompanionBase[RowType],
      sheet: Sheet,
      hasHeader: ExcelHeader.ExcelHeader,
      timeZone: ZoneId): ExcelSource[RowType] = {
    if (entity == null || sheet == null) throw new RelationalException("entity and sheet shouldn't be null")
    val sRow = sheet.getFirstRowNum
    val eRow = sheet.getLastRowNum
    val sCol = sheet.getRow(sRow).getFirstCellNum()
    val eCol = sheet.getRow(eRow).getLastCellNum()
    if (sRow == -1 || eRow == -1 || sCol == -1 || eCol == -1)
      throw new RelationalException("there isn't any table in this sheet")
    val cellRef = new CellReference(sRow, sCol)
    new ExcelSource(
      entity,
      timeZone,
      sheet,
      cellRef.formatAsString(),
      eRow - sRow + 1,
      (eCol - sCol).toShort,
      hasHeader,
      null)
  }

  def apply[RowType <: Entity: TypeInfo](
      entity: EntityCompanionBase[RowType],
      sheet: Sheet,
      leftTop: String,
      rightBottom: String,
      hasHeader: ExcelHeader.ExcelHeader): ExcelSource[RowType] =
    apply(entity, sheet, leftTop, rightBottom, hasHeader, ZoneIds.UTC)

  def apply[RowType <: Entity: TypeInfo](
      entity: EntityCompanionBase[RowType],
      sheet: Sheet,
      leftTop: String,
      rightBottom: String,
      hasHeader: ExcelHeader.ExcelHeader,
      timeZone: ZoneId): ExcelSource[RowType] = {
    if (entity == null || sheet == null) throw new RelationalException("entity and sheet shouldn't be null")

    val sRow = new CellReference(leftTop).getRow
    val sCol = new CellReference(leftTop).getCol
    val eRow = new CellReference(rightBottom).getRow
    val eCol = new CellReference(rightBottom).getCol
    new ExcelSource(entity, timeZone, sheet, leftTop, eRow - sRow + 1, (eCol - sCol + 1).toShort, hasHeader, null)
  }

  def apply[RowType <: Entity: TypeInfo](
      entity: EntityCompanionBase[RowType],
      sheet: Sheet,
      leftTop: String,
      rightBottom: String,
      hasHeader: ExcelHeader.ExcelHeader,
      columnMap: mutable.Map[String, String]): ExcelSource[RowType] =
    apply(entity, sheet, leftTop, rightBottom, hasHeader, ZoneIds.UTC, columnMap)

  def apply[RowType <: Entity: TypeInfo](
      entity: EntityCompanionBase[RowType],
      sheet: Sheet,
      leftTop: String,
      rightBottom: String,
      hasHeader: ExcelHeader.ExcelHeader,
      timeZone: ZoneId,
      columnMap: mutable.Map[String, String]): ExcelSource[RowType] = {
    if (entity == null || sheet == null) throw new RelationalException("entity and sheet shouldn't be null")

    val sRow = new CellReference(leftTop).getRow
    val sCol = new CellReference(leftTop).getCol
    val eRow = new CellReference(rightBottom).getRow
    val eCol = new CellReference(rightBottom).getCol
    new ExcelSource(entity, timeZone, sheet, leftTop, eRow - sRow + 1, (eCol - sCol + 1).toShort, hasHeader, columnMap)
  }
}
