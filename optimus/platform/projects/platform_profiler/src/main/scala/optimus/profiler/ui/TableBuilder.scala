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
package optimus.profiler.ui
import scala.collection.mutable

class TableBuilder private (n: Int) {
  import TableBuilder._
  private val map = mutable.HashMap.empty[RowID, mutable.IndexedSeq[mutable.Builder[Any, IndexedSeq[Any]]]]

  private def newRow = mutable.IndexedSeq.fill(n)(IndexedSeq.newBuilder[Any])

  private class ColumnBuilderImpl(private val index: Int) extends ColumnBuilder {
    def put(row: RowID, value: Any): Unit = {
      map.updateWith(row) {
        _.orElse(Some(newRow)) // create a new row at that rowID
          .map { is =>
            // add our new element to the column elements at the row id
            is(index).addOne(value)
            is
          }
      }
    }
  }

  /**
   * Get the builder for column `i` of this table.
   */
  def column(i: Int): ColumnBuilder = {
    if (i < 0 || i > n) throw new IndexOutOfBoundsException(s"column $i is out of bound for a table of $n columns")
    new ColumnBuilderImpl(i)
  }

  /**
   * Converts a “ragged” rows to full rows of Option.
   */
  private def fillRagged[T](rows: IndexedSeq[IndexedSeq[T]]): Iterator[IndexedSeq[ValueMaybe]] = {
    rows.map(_.size).maxOption match {
      case None => Iterator.empty
      case Some(n) =>
        (0 until n).iterator.map { i =>
          rows.map(r =>
            r.lift(i) match {
              case Some(v) => ValuePresent(v)
              case None    => ValueMissing
            })
        }
    }
  }

  /**
   * Transform this builder to rows by applying the function f to data in the builder.
   */
  def result[T](f: TableRow => T): Seq[T] = {
    map.iterator
      .map { case (row, builder) => row -> builder.map(_.result()) }
      .flatMap { case (id, rows) =>
        fillRagged(rows.to(IndexedSeq))
          .map { values => TableRowImpl(id, values) }
          .map { tr => f(tr) }
      }
      .to(Seq)
  }
}

object TableBuilder {

  private final case class TableRowImpl(id: RowID, values: IndexedSeq[ValueMaybe]) extends TableRow

  /**
   * Create a mutable TableBuilder.
   *
   * Columns are created using [[TableBuilder.column]] which returns a [[ColumnBuilder]] that supports inserting data
   * for the column. The builder is converted into a table using `result`.
   */
  def create(numColumns: Int): TableBuilder = new TableBuilder(numColumns)
}

/**
 * Container for a single column on a TableBuilder.
 */
trait ColumnBuilder {

  /**
   * Add a value to this column.
   *
   * The `row` argument will determine where the value goes in the final built table. Multiple calls to `put` with the
   * same value of `row` will add multiple rows in sequence with the same ID.
   */
  def put(row: RowID, value: Any): Unit
}

/**
 * An Option[Any] newtype to ensure that we don't mix Option value from the profiled code from missing / present values
 * inside the table columns
 */
sealed trait ValueMaybe
final case class ValuePresent(value: Any) extends ValueMaybe
case object ValueMissing extends ValueMaybe

trait TableRow {

  /**
   * The current [[RowID]] for this row.
   */
  def id: RowID

  /**
   * Column values for this row.
   *
   * For each column, the value is None if that entry doesn't contain any entries. This can happen either because only
   * certain columns in the table have an entry for [[id]], or because some columns have multiple entries for the same
   * [[id]].
   */
  val values: Seq[ValueMaybe]
}
