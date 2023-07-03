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

import java.io.OutputStream
import java.time._

import optimus.platform._
import optimus.platform._
import optimus.platform.AsyncImplicits._
import optimus.platform.relational.KeyPropagationPolicy
import optimus.platform.relational.{MapBasedDynamicObject, ScalaBasedDynamicObjectFactory}
import optimus.platform.relational.formatter._
import optimus.platform.relational.inmemory.ScalaTypeMultiRelation
import optimus.platform.relational.pivot.{DataField, PivotField, PivotTable}
import optimus.platform.relational.tree.TypeInfo

import scala.collection.mutable
import optimus.scalacompat.collection._

object PivotHelper extends CommonHelper {

  @async def executePivotTable[LeftType, RowType <: DynamicObject, SrcType](
      pivot: PivotTable[LeftType, RowType, SrcType]): Iterable[RowType] = {
    // step one: get query result items
    val resultItems = Query.execute(pivot.data)
    // step two: create a map of <DynamicObject[Item], Item>
    val srcTypeInfo = pivot.data.elementType.asInstanceOf[TypeInfo[SrcType]]
    val dynamicIterable = ScalaBasedDynamicObjectFactory(srcTypeInfo) match {
      case Right(f) =>
        resultItems.map(t => (f(t), t))
      case Left(lf) =>
        val nf = asNode.apply$withNode(lf)
        resultItems.apar.map(t => (nf(t), t))
    }
    // step three: group all items based on RowFields
    val rowFieldList = pivot.rowFields.toArray
    val rowLevelGroup = dynamicIterable.groupBy(t => new MultiKey(rowFieldList.map(f => t._1(f.name))))
    // step four: group items in every sub group in rowLevelGroup based on PivotFields
    val result = rowLevelGroup.toSeq.apar.map {
      case (rowValue, rowGroup) => {
        val keyColumnMap: Map[String, Any] = rowFieldList.map(_.name).iterator.zip(rowValue.keys.iterator).toMap
        val provider = pivot.data.provider
        val pivotResultMap = calculatePivotGroup(rowGroup, pivot.pivotFields, pivot.dataFields, provider, srcTypeInfo)

        val innerPivotRow = pivot.rowFactory.createPivotRow(keyColumnMap ++ pivotResultMap)
        innerPivotRow
      }
    }
    result.asInstanceOf[Iterable[RowType]]
  }

  @async def calculatePivotGroup[T](
      group: Iterable[(DynamicObject, T)],
      pivotFields: Seq[PivotField],
      dataFields: Seq[DataField[T]],
      provider: QueryProvider,
      srcTypeInfo: TypeInfo[T]): Map[String, DynamicObject] = {

    val pivotField = pivotFields.head
    val restPivotFields = pivotFields.tail

    val pivotResultMap: Map[String, DynamicObject] = group
      .groupBy(t => t._1(pivotField.name))
      .apar
      .map {
        case (pivotKey, pivotInnerGroup) => {
          val valueMap = if (restPivotFields.isEmpty) {
            // Calculate the group by value for lowest pivot field, e.g. sum, avg.
            dataFields.apar.map { t =>
              val items = pivotInnerGroup.map(_._2)
              val query =
                provider.createQuery(
                  ScalaTypeMultiRelation(
                    iter = items,
                    key = NoKey,
                    typeInfo = srcTypeInfo,
                    keyPolicy = KeyPropagationPolicy.NoKey))
              (t.name, t.calculate(query))
            }(Map.breakOut): Map[String, Any]
          } else {
            // we have more pivot field, so do group by again on the lower level pivot field
            calculatePivotGroup(pivotInnerGroup, restPivotFields, dataFields, provider, srcTypeInfo)
          }

          val innerDynamicObject = new MapBasedDynamicObject(valueMap) // aggregation data items
          (pivotKey.toString, innerDynamicObject)
        }
      }(Map.breakOut)
    pivotResultMap
  }

  // ----------------------------------------------------------------------------------------------------------------------
  // ---------------------------------------- PivotTable Output API            --------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------------

  @async def displayPivotTable[LeftType, RowType <: DynamicObject, SrcType](
      pivot: PivotTable[LeftType, RowType, SrcType],
      os: OutputStream,
      timeZone: Option[ZoneId]): Unit = {
    val pivotRows = executePivotTable(pivot)
    val pivotTableHeaders = extractPivotHeader(pivotRows)
    val headers = flattenHeaders(pivotTableHeaders, null)
    val values =
      flattenPivotRow(pivotRows, pivotTableHeaders, null).map(f => f.map(i => if (i == null) "" else i.toString).toSeq)

    TableDisplay.format(TableDisplay.tabulateIterable(headers.toSeq, values), os, timeZone)
  }

  private def extractPivotHeader(rows: Iterable[DynamicObject]): Map[String, Map[String, Any]] = {
    // iterate all items to find all headers, (Header, SubHeaders)
    rows.foldLeft(Map.empty[String, Map[String, Any]])((headers, item) => extractHeader(item, headers))
  }

  private def extractHeader(o: DynamicObject, headers: Map[String, Map[String, Any]]): Map[String, Map[String, Any]] = {
    val mutableHeaders = mutable.Map(headers.toSeq: _*)

    val columns = o.getAll
    columns.foreach {
      case (name, value) => {
        if (!mutableHeaders.contains(name)) mutableHeaders.put(name, Map.empty)

        value match {
          case dynamic: DynamicObject =>
            val existingSubHeader = mutableHeaders.get(name).get.asInstanceOf[Map[String, Map[String, Any]]]
            val newSubHeaders = extractHeader(dynamic, existingSubHeader)
            mutableHeaders.put(name, newSubHeaders)
          case _ =>
        }
      }
    }

    mutableHeaders.toMap
  }

  private def flattenHeaders(headers: Map[String, Map[String, Any]], prefix: String): Iterable[String] = {
    headers.flatMap {
      case (h, subHeaders) => {
        val columnName = if (prefix == null) h else prefix + ":" + h

        if (subHeaders.isEmpty) Seq(columnName)
        else flattenHeaders(subHeaders.asInstanceOf[Map[String, Map[String, Any]]], columnName)
      }
    }
  }

  private def flattenPivotRow(
      rows: Iterable[DynamicObject],
      headers: Map[String, Map[String, Any]],
      defaultValue: Any = null): Iterable[Iterable[Any]] = {
    rows.map(r => flattenPivotCell(r, headers, defaultValue))
  }

  private def flattenPivotCell(
      pivotCell: Any,
      subheaders: Map[String, Map[String, Any]],
      defaultValue: Any = null): Iterable[Any] = {
    if (!subheaders.isEmpty) {
      // subHeaders means this pivotCell is still a pivot row contains many data, so its value is DynamicObject
      subheaders.flatMap {
        case (head, subHeaders) => {
          val cellValue = if (pivotCell != null) {
            val cellDynamicObject = pivotCell.asInstanceOf[DynamicObject]
            if (cellDynamicObject.contains(head)) cellDynamicObject.get(head) else defaultValue
          } else defaultValue

          flattenPivotCell(cellValue, subHeaders.asInstanceOf[Map[String, Map[String, Any]]], defaultValue)
        }
      }
    } else {
      if (pivotCell == null) Seq(null) else Seq(pivotCell)
    }
  }
}
