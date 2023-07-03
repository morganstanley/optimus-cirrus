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
package optimus.platform.relational.pivot

import optimus.platform._
import optimus.platform._
import optimus.platform.relational.tree.TypeInfo

trait PivotTableField {
  def name: String
}
class PivotField(val name: String) extends PivotTableField
class RowField(val name: String) extends PivotTableField
trait DataField[T] extends PivotTableField {
  @async def calculate(items: Query[T]): Any
}
object DataField {
  def apply[T, R](name: String, aggregationLambda: Lambda1[Query[T], R]): DataField[T] =
    AggrLambdaDataField(name, aggregationLambda)

  private final case class AggrLambdaDataField[T, R](val name: String, val aggregationLambda: Lambda1[Query[T], R])
      extends DataField[T] {
    @async override def calculate(items: Query[T]): R = {
      aggregationLambda match {
        case Lambda1(Some(f), _, _)  => f(items)
        case Lambda1(_, Some(nf), _) => nf(items).get
        case Lambda1(None, None, _) =>
          throw new IllegalArgumentException("expected either a Function or a NodeFunction but both were None")
      }
    }
  }
}

class PivotTable[RemainingType, RowType <: DynamicObject, SrcType](
    val data: Query[SrcType],
    val pivotFields: Seq[PivotField],
    val rowFields: Seq[RowField],
    val dataFields: Seq[DataField[SrcType]],
    val rowFactory: PivotRowFactory) {
  val rowClass = rowFactory.pivotRowClass

  def addRowField[U](lambda: RemainingType => U): Any =
    macro PivotTableMacros.addRowField[SrcType, U, RowType, RemainingType]
  def addRowFieldRaw(r: RowField): PivotTable[Nothing, Nothing, SrcType] =
    new PivotTable(data, pivotFields, rowFields :+ r, dataFields, rowFactory)

  def addPivotField[U](lambda: RemainingType => U): Any =
    macro PivotTableMacros.addPivotField[SrcType, U, RowType, RemainingType]
  def addPivotFieldRaw(p: PivotField): PivotTable[Nothing, Nothing, SrcType] =
    new PivotTable(data, pivotFields ++ Seq(p), rowFields, dataFields, rowFactory)

  def addDataField(columnName: String, lambda: Query[SrcType] => Any): Any =
    macro PivotTableMacros.addDataField[SrcType, RowType, RemainingType]
  def addDataFieldList(d: Seq[DataField[SrcType]]): PivotTable[RemainingType, RowType, SrcType] =
    new PivotTable[RemainingType, RowType, SrcType](data, pivotFields, rowFields, dataFields ++ d, rowFactory)

  def groupOn[U](lambda: RemainingType => U): Any =
    macro PivotTableMacros
      .groupOn[
        SrcType,
        U,
        RowType,
        RemainingType
      ] // e.g. translate to addRowField, but can add more than one row field, just like pivotOn

  def map[U](lambda: Query[SrcType] => U)(implicit uType: TypeInfo[U]): Any =
    macro PivotTableMacros.pivotMap[SrcType, U, RowType, RemainingType]

  def groupOnTyped[GroupOnType >: SrcType]: Any =
    macro PivotTableMacros.groupOnTyped[SrcType, GroupOnType, RowType, RemainingType]

}
